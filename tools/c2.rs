//! Offline measurement for early-stop point decoding: recompresses sampled bottom
//! segments exactly as the run builder does (magicless frames, level 1, the
//! segment's raw dictionary and block-size hint), with Zstandard's
//! `targetCBlockSize` and, for comparison, `maxBlockSize`. For each record it
//! finds the end of the Zstandard block holding the record's last byte by
//! streaming decompression, which bounds what an early-stopping reader decodes.
use std::time::Instant;
use tessera::engine::diagnostics::SegmentSample;
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
fn record_ends(block: &[u8]) -> Vec<usize> {
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

fn cctx(setting: Setting) -> CCtx<'static> {
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

fn dctx() -> DCtx<'static> {
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
