//! Offline measurement for early-stop point decoding: recompresses sampled bottom
//! segments exactly as the run builder does (magicless frames, level 1, the
//! segment's raw dictionary and block-size hint), with Zstandard's
//! `targetCBlockSize` and, for comparison, `maxBlockSize`. For each record it
//! finds the end of the Zstandard block holding the record's last byte by
//! streaming decompression, which bounds what an early-stopping reader decodes.
use std::time::Instant;
use tessera::diagnostics::SegmentSample;
use zstd_safe::{
    CCtx, CParameter, DCtx, DParameter, FrameFormat, InBuffer, OutBuffer, ResetDirective,
};

#[derive(Clone, Copy)]
pub enum Setting {
    Off,
    Target(u32),
    MaxBlock(u32),
}
impl Setting {
    fn label(self) -> String {
        match self {
            Self::Off => "off".into(),
            Self::Target(n) => format!("targetCBlockSize={}K", n >> 10),
            Self::MaxBlock(n) => format!("maxBlockSize={}K", n >> 10),
        }
    }
}

fn varint(bytes: &[u8], at: &mut usize) -> u64 {
    let mut out = 0u64;
    let mut shift = 0;
    loop {
        let b = bytes[*at];
        *at += 1;
        out |= u64::from(b & 127) << shift;
        if b < 128 {
            return out;
        }
        shift += 7;
    }
}

/// End offsets of each record in a raw block, by walking it.
pub fn record_ends(block: &[u8]) -> Vec<usize> {
    let n = block.len();
    let restarts = u32::from_le_bytes(block[n - 4..].try_into().unwrap()) as usize;
    let records_end = n - 4 * (restarts + 1);
    let mut at = 0;
    let mut ends = Vec::new();
    while at < records_end {
        let _shared = varint(block, &mut at);
        let suffix = varint(block, &mut at) as usize;
        let tag = varint(block, &mut at) as usize;
        let _seq = varint(block, &mut at);
        at += suffix + tag.saturating_sub(1);
        ends.push(at);
    }
    assert_eq!(at, records_end);
    ends
}

/// Stored ranges of each Zstandard block of a magicless frame without a
/// content size, dictionary ID or checksum.
fn zstd_blocks(frame: &[u8]) -> Vec<usize> {
    assert_eq!(frame[0], 0, "descriptor without optional fields");
    let mut at = 2;
    let mut ends = Vec::new();
    loop {
        let h = u32::from_le_bytes([frame[at], frame[at + 1], frame[at + 2], 0]);
        let size = (h >> 3) as usize;
        let stored = match (h >> 1) & 3 {
            0 | 2 => size,
            1 => 1,
            _ => panic!("reserved block type"),
        };
        at += 3 + stored;
        ends.push(at);
        if h & 1 != 0 {
            assert_eq!(at, frame.len());
            return ends;
        }
    }
}

pub fn cctx(setting: Setting) -> CCtx<'static> {
    let mut ctx = CCtx::create();
    for p in [
        CParameter::Format(FrameFormat::Magicless),
        CParameter::ContentSizeFlag(false),
        CParameter::ChecksumFlag(false),
        CParameter::DictIdFlag(false),
    ] {
        ctx.set_parameter(p).unwrap();
    }
    match setting {
        Setting::Off => {}
        Setting::Target(n) => {
            ctx.set_parameter(CParameter::TargetCBlockSize(n))
                .expect("targetCBlockSize");
        }
        Setting::MaxBlock(n) => {
            ctx.set_parameter(CParameter::MaxBlockSize(n))
                .expect("maxBlockSize");
        }
    }
    ctx
}

struct Compressed {
    /// Per segment, per block: the frame.
    frames: Vec<Vec<Vec<u8>>>,
    stored: u64,
}

fn compress(samples: &[SegmentSample], setting: Setting) -> Compressed {
    let mut ctx = cctx(setting);
    let mut frames = Vec::with_capacity(samples.len());
    let mut stored = 0u64;
    for s in samples {
        // As Compressor::begin_segment: level 1, the hint, the raw dictionary.
        ctx.set_parameter(CParameter::CompressionLevel(1)).unwrap();
        ctx.set_parameter(CParameter::SrcSizeHint(s.hint as u32))
            .unwrap();
        ctx.load_dictionary(&s.dictionary).unwrap();
        let mut out = Vec::with_capacity(s.blocks.len());
        for block in &s.blocks {
            let mut frame = vec![0u8; zstd_safe::compress_bound(block.len())];
            let n = ctx.compress2(&mut frame[..], block).unwrap();
            frame.truncate(n);
            stored += n as u64;
            out.push(frame);
        }
        frames.push(out);
    }
    Compressed { frames, stored }
}

pub fn dctx() -> DCtx<'static> {
    let mut ctx = DCtx::create();
    ctx.set_parameter(DParameter::Format(FrameFormat::Magicless))
        .unwrap();
    ctx
}

/// Decoded bytes at the end of each Zstandard block, by streaming the frame
/// one Zstandard block at a time.
fn decoded_at_block_ends(ctx: &mut DCtx<'_>, frame: &[u8], raw_len: usize) -> Vec<usize> {
    ctx.reset(ResetDirective::SessionOnly).unwrap();
    let mut out = vec![0u8; raw_len];
    let mut output = OutBuffer::around(&mut out[..]);
    let mut fed = 0;
    let mut ends = Vec::new();
    for end in zstd_blocks(frame) {
        let mut input = InBuffer::around(&frame[fed..end]);
        while input.pos() < end - fed {
            ctx.decompress_stream(&mut output, &mut input).unwrap();
        }
        // Drain whatever the decoder still holds for this block.
        loop {
            let before = output.pos();
            let mut empty = InBuffer::around(&[]);
            ctx.decompress_stream(&mut output, &mut empty).unwrap();
            if output.pos() == before {
                break;
            }
        }
        fed = end;
        ends.push(output.pos());
    }
    assert_eq!(output.pos(), raw_len);
    ends
}

pub fn run(name: &str, samples: &[SegmentSample]) {
    let raw: usize = samples.iter().flat_map(|s| &s.blocks).map(Vec::len).sum();
    let blocks: usize = samples.iter().map(|s| s.blocks.len()).sum();
    let hints: usize = samples.iter().map(|s| s.hint).sum::<usize>() / samples.len().max(1);
    eprintln!(
        "c2_sample,{name},segments={},blocks={blocks},raw={raw},mean_hint={hints}",
        samples.len()
    );
    let ends: Vec<Vec<Vec<usize>>> = samples
        .iter()
        .map(|s| s.blocks.iter().map(|b| record_ends(b)).collect())
        .collect();
    let records: usize = ends.iter().flatten().map(Vec::len).sum();
    let settings = [
        Setting::Off,
        Setting::Target(16 << 10),
        Setting::Target(8 << 10),
        Setting::Target(4 << 10),
        Setting::MaxBlock(16 << 10),
        Setting::MaxBlock(8 << 10),
        Setting::MaxBlock(4 << 10),
    ];
    let mut off_stored = 0;
    let mut off_seconds = 0.0;
    let mut ctx = dctx();
    for setting in settings {
        let c = compress(samples, setting);
        // Early stop: decoded bytes per uniformly chosen record, and how
        // many Zstandard blocks frames hold.
        let mut decoded = 0u64;
        let mut zblocks = 0u64;
        for ((s, frames), ends) in samples.iter().zip(&c.frames).zip(&ends) {
            ctx.reset(ResetDirective::SessionOnly).unwrap();
            ctx.load_dictionary(&s.dictionary).unwrap();
            for ((block, frame), ends) in s.blocks.iter().zip(frames).zip(ends) {
                let at = decoded_at_block_ends(&mut ctx, frame, block.len());
                zblocks += at.len() as u64;
                for &end in ends {
                    let reach = match setting {
                        Setting::Off => block.len(),
                        _ => *at.iter().find(|&&d| d >= end).unwrap(),
                    };
                    decoded += reach as u64;
                }
            }
        }
        // Whole-frame decompression time, best of three.
        let mut best = f64::MAX;
        let mut scratch = vec![0u8; 64 << 10];
        let mut timed = dctx();
        for _ in 0..3 {
            let started = Instant::now();
            for (s, frames) in samples.iter().zip(&c.frames) {
                for (block, frame) in s.blocks.iter().zip(frames) {
                    if scratch.len() < block.len() {
                        scratch.resize(block.len(), 0);
                    }
                    let n = timed
                        .decompress_using_dict(&mut scratch[..block.len()], frame, &s.dictionary)
                        .unwrap();
                    assert_eq!(n, block.len());
                }
            }
            best = best.min(started.elapsed().as_secs_f64());
        }
        if let Setting::Off = setting {
            off_stored = c.stored;
            off_seconds = best;
        }
        println!(
            "c2,{name},{},{},{:.4},{:.1},{:.1},{:.3},{:.3}",
            setting.label(),
            c.stored,
            c.stored as f64 / off_stored as f64,
            decoded as f64 / records as f64,
            zblocks as f64 / blocks as f64,
            best / off_seconds,
            raw as f64 / records as f64,
        );
    }
}

const FRAME_HEADER_LEN: usize = 12;

#[derive(Clone, Copy)]
struct ScopeRule {
    label: &'static str,
    stored_mult: Option<usize>,
    raw_max_segs: Option<usize>,
    drift_pct: Option<usize>,
}

/// Evaluates consecutive bottom segments when a segment's dictionary is reused
/// across subsequent segments under fixed scope multipliers and stopping rules.
pub fn run_dict_scope(name: &str, samples: &[SegmentSample]) {
    if samples.is_empty() {
        return;
    }
    let seg_count = samples.len();
    let seg_raw: Vec<u64> = samples
        .iter()
        .map(|s| s.blocks.iter().map(|b| b.len() as u64).sum())
        .collect();
    let seg_blocks: Vec<u64> = samples.iter().map(|s| s.blocks.len() as u64).collect();
    let seg_records: Vec<u64> = samples
        .iter()
        .map(|s| s.blocks.iter().map(|b| record_ends(b).len() as u64).sum())
        .collect();
    let total_raw: u64 = seg_raw.iter().sum();
    let total_blocks: u64 = seg_blocks.iter().sum();
    let total_records: u64 = seg_records.iter().sum();

    // Verify exact byte parity with the stored frames at scope 1.
    let mut ctx = cctx(Setting::Off);
    let mut scratch_out = Vec::new();
    let mut base_seg_payload = Vec::with_capacity(seg_count);
    for (idx, s) in samples.iter().enumerate() {
        ctx.set_parameter(CParameter::CompressionLevel(1)).unwrap();
        ctx.set_parameter(CParameter::SrcSizeHint(s.hint as u32))
            .unwrap();
        ctx.load_dictionary(&s.dictionary).unwrap();
        let mut payload = 0u64;
        for (bi, (block, orig)) in s.blocks.iter().zip(&s.frames).enumerate() {
            let bound = zstd_safe::compress_bound(block.len());
            if scratch_out.len() < bound {
                scratch_out.resize(bound, 0);
            }
            let n = ctx.compress2(&mut scratch_out[..bound], block).unwrap();
            assert_eq!(
                &scratch_out[..n],
                orig.as_slice(),
                "frame parity mismatch at segment {idx} block {bi}"
            );
            payload += n as u64;
        }
        base_seg_payload.push(payload);
    }

    let base_payload: u64 = base_seg_payload.iter().sum();
    let base_hdr: u64 = base_payload + total_blocks * FRAME_HEADER_LEN as u64;
    let base_dicts: u64 = samples.iter().map(|s| s.dictionary.len() as u64).sum();
    let base_total = base_hdr + base_dicts;

    // Offset drift curve: recompress segment i + offset with segment i's dictionary.
    let max_offset = 15.min(seg_count.saturating_sub(1));
    let mut off_base = vec![0u64; max_offset + 1];
    let mut off_recomp = vec![0u64; max_offset + 1];
    let mut off_count = vec![0usize; max_offset + 1];
    for i in 0..seg_count {
        let dict = &samples[i].dictionary;
        let hint = samples[i].hint;
        ctx.set_parameter(CParameter::CompressionLevel(1)).unwrap();
        ctx.set_parameter(CParameter::SrcSizeHint(hint as u32))
            .unwrap();
        ctx.load_dictionary(dict).unwrap();
        off_base[0] += base_seg_payload[i];
        off_recomp[0] += base_seg_payload[i];
        off_count[0] += 1;
        let limit = (i + max_offset).min(seg_count - 1);
        for j in (i + 1)..=limit {
            let offset = j - i;
            let mut recomp = 0u64;
            for block in &samples[j].blocks {
                let bound = zstd_safe::compress_bound(block.len());
                if scratch_out.len() < bound {
                    scratch_out.resize(bound, 0);
                }
                let n = ctx.compress2(&mut scratch_out[..bound], block).unwrap();
                recomp += n as u64;
            }
            off_base[offset] += base_seg_payload[j];
            off_recomp[offset] += recomp;
            off_count[offset] += 1;
        }
    }
    for offset in 0..=max_offset {
        let drift = if off_base[offset] == 0 {
            0.0
        } else {
            (off_recomp[offset] as f64 - off_base[offset] as f64) / off_base[offset] as f64 * 100.0
        };
        println!(
            "scope_offset,{name},{offset},{},{},{},{drift:.3}",
            off_count[offset], off_base[offset], off_recomp[offset]
        );
    }

    let rules = [
        ScopeRule {
            label: "fixed_n1",
            stored_mult: None,
            raw_max_segs: Some(1),
            drift_pct: None,
        },
        ScopeRule {
            label: "fixed_n2",
            stored_mult: None,
            raw_max_segs: Some(2),
            drift_pct: None,
        },
        ScopeRule {
            label: "fixed_n4",
            stored_mult: None,
            raw_max_segs: Some(4),
            drift_pct: None,
        },
        ScopeRule {
            label: "fixed_n8",
            stored_mult: None,
            raw_max_segs: Some(8),
            drift_pct: None,
        },
        ScopeRule {
            label: "fixed_n16",
            stored_mult: None,
            raw_max_segs: Some(16),
            drift_pct: None,
        },
        ScopeRule {
            label: "stored_k8",
            stored_mult: Some(8),
            raw_max_segs: None,
            drift_pct: None,
        },
        ScopeRule {
            label: "stored_k12",
            stored_mult: Some(12),
            raw_max_segs: None,
            drift_pct: None,
        },
        ScopeRule {
            label: "stored_k16",
            stored_mult: Some(16),
            raw_max_segs: None,
            drift_pct: None,
        },
        ScopeRule {
            label: "stored_k24",
            stored_mult: Some(24),
            raw_max_segs: None,
            drift_pct: None,
        },
        ScopeRule {
            label: "stored_k32",
            stored_mult: Some(32),
            raw_max_segs: None,
            drift_pct: None,
        },
        ScopeRule {
            label: "k8_m8",
            stored_mult: Some(8),
            raw_max_segs: Some(8),
            drift_pct: None,
        },
        ScopeRule {
            label: "k12_m8",
            stored_mult: Some(12),
            raw_max_segs: Some(8),
            drift_pct: None,
        },
        ScopeRule {
            label: "k16_m8",
            stored_mult: Some(16),
            raw_max_segs: Some(8),
            drift_pct: None,
        },
        ScopeRule {
            label: "k16_m16",
            stored_mult: Some(16),
            raw_max_segs: Some(16),
            drift_pct: None,
        },
        ScopeRule {
            label: "k24_m16",
            stored_mult: Some(24),
            raw_max_segs: Some(16),
            drift_pct: None,
        },
        ScopeRule {
            label: "k32_m16",
            stored_mult: Some(32),
            raw_max_segs: Some(16),
            drift_pct: None,
        },
        ScopeRule {
            label: "k12_m8_d25",
            stored_mult: Some(12),
            raw_max_segs: Some(8),
            drift_pct: Some(25),
        },
        ScopeRule {
            label: "k16_m8_d20",
            stored_mult: Some(16),
            raw_max_segs: Some(8),
            drift_pct: Some(20),
        },
        ScopeRule {
            label: "k16_m8_d25",
            stored_mult: Some(16),
            raw_max_segs: Some(8),
            drift_pct: Some(25),
        },
        ScopeRule {
            label: "k16_m8_d35",
            stored_mult: Some(16),
            raw_max_segs: Some(8),
            drift_pct: Some(35),
        },
        ScopeRule {
            label: "k16_m16_d20",
            stored_mult: Some(16),
            raw_max_segs: Some(16),
            drift_pct: Some(20),
        },
        ScopeRule {
            label: "k16_m16_d25",
            stored_mult: Some(16),
            raw_max_segs: Some(16),
            drift_pct: Some(25),
        },
        ScopeRule {
            label: "k16_m16_d35",
            stored_mult: Some(16),
            raw_max_segs: Some(16),
            drift_pct: Some(35),
        },
        ScopeRule {
            label: "k24_m16_d25",
            stored_mult: Some(24),
            raw_max_segs: Some(16),
            drift_pct: Some(25),
        },
    ];

    let timed_segs = seg_count.min(32);
    let mut decode_ctx = dctx();
    let mut decode_buf = vec![0u8; 64 << 10];

    for rule in rules {
        let mut scopes = 0usize;
        let mut max_segs = 0usize;
        let mut total_payload = 0u64;
        let mut total_dicts = 0u64;
        // Decoded timing fixtures for the first `timed_segs` segments.
        let mut timed_frames: Vec<(usize, Vec<Vec<u8>>)> = Vec::with_capacity(timed_segs);

        let mut i = 0;
        while i < seg_count {
            scopes += 1;
            let dict = &samples[i].dictionary;
            let dict_len = dict.len() as u64;
            total_dicts += dict_len;
            let hint = samples[i].hint;
            ctx.set_parameter(CParameter::CompressionLevel(1)).unwrap();
            ctx.set_parameter(CParameter::SrcSizeHint(hint as u32))
                .unwrap();
            ctx.load_dictionary(dict).unwrap();

            let prefix_raw = seg_raw[i];
            let prefix_payload = base_seg_payload[i];
            let mut scope_stored_hdr = prefix_payload + seg_blocks[i] * FRAME_HEADER_LEN as u64;
            let mut scope_segs = 1usize;
            total_payload += prefix_payload;
            if i < timed_segs {
                timed_frames.push((i, samples[i].frames.clone()));
            }

            let mut j = i + 1;
            let mut last_drifted = false;
            while j < seg_count {
                if rule.raw_max_segs.is_some_and(|max| scope_segs >= max) {
                    break;
                }
                if rule
                    .stored_mult
                    .is_some_and(|k| scope_stored_hdr >= k as u64 * dict_len)
                {
                    break;
                }
                if last_drifted {
                    break;
                }
                let mut seg_payload = 0u64;
                let mut frames_j = if j < timed_segs {
                    Vec::with_capacity(samples[j].blocks.len())
                } else {
                    Vec::new()
                };
                for block in &samples[j].blocks {
                    let bound = zstd_safe::compress_bound(block.len());
                    if scratch_out.len() < bound {
                        scratch_out.resize(bound, 0);
                    }
                    let n = ctx.compress2(&mut scratch_out[..bound], block).unwrap();
                    seg_payload += n as u64;
                    if j < timed_segs {
                        frames_j.push(scratch_out[..n].to_vec());
                    }
                }
                if j < timed_segs {
                    timed_frames.push((i, frames_j));
                }
                total_payload += seg_payload;
                scope_stored_hdr += seg_payload + seg_blocks[j] * FRAME_HEADER_LEN as u64;
                scope_segs += 1;
                if let Some(drift_limit) = rule.drift_pct {
                    // seg_payload / seg_raw[j] > prefix_payload / prefix_raw * (100 + drift_limit) / 100
                    let lhs = u128::from(seg_payload) * u128::from(prefix_raw) * 100;
                    let rhs = u128::from(prefix_payload)
                        * u128::from(seg_raw[j])
                        * u128::from(100 + drift_limit as u64);
                    if lhs > rhs {
                        last_drifted = true;
                    }
                }
                j += 1;
            }
            max_segs = max_segs.max(scope_segs);
            i = j;
        }

        // Warmup + 5 timed decode passes on `timed_frames`.
        let timed_block_count: usize = timed_frames.iter().map(|(_, fs)| fs.len()).sum();
        let mut ns_samples = [0.0f64; 5];
        for pass in 0..6 {
            let started = Instant::now();
            for (seg_idx, (dict_seg, frames)) in timed_frames.iter().enumerate() {
                let dict = &samples[*dict_seg].dictionary;
                for (block, frame) in samples[seg_idx].blocks.iter().zip(frames) {
                    if decode_buf.len() < block.len() {
                        decode_buf.resize(block.len(), 0);
                    }
                    let n = decode_ctx
                        .decompress_using_dict(&mut decode_buf[..block.len()], frame, dict)
                        .unwrap();
                    assert_eq!(n, block.len());
                    if pass == 0 {
                        assert_eq!(&decode_buf[..n], block.as_slice());
                    }
                }
            }
            if pass > 0 {
                ns_samples[pass - 1] =
                    started.elapsed().as_nanos() as f64 / timed_block_count.max(1) as f64;
            }
        }
        ns_samples.sort_by(f64::total_cmp);
        let median_ns = ns_samples[2];

        let total_hdr = total_payload + total_blocks * FRAME_HEADER_LEN as u64;
        let total_bytes = total_hdr + total_dicts;
        let b_per_rec = total_bytes as f64 / total_records as f64;
        let delta_pct = (total_bytes as f64 - base_total as f64) / base_total as f64 * 100.0;
        let frame_drift_pct =
            (total_payload as f64 - base_payload as f64) / base_payload as f64 * 100.0;
        println!(
            "scope_spike,{name},{},{seg_count},{total_blocks},{total_records},{total_raw},{scopes},{:.2},{max_segs},{total_payload},{total_hdr},{total_dicts},{total_bytes},{b_per_rec:.4},{delta_pct:+.3},{frame_drift_pct:+.3},{median_ns:.1}",
            rule.label,
            seg_count as f64 / scopes as f64,
        );
    }
}
