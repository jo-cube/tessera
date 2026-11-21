use crate::counters::c2::{Setting, cctx, dctx, record_ends};
use crate::frame::{self, Block, Sections};
use std::hint::black_box;
use std::time::Instant;
use tessera::diagnostics::SegmentSample;
use zstd_safe::{CCtx, CDict, CParameter, DCtx, DDict, ParamSwitch};
use zstd_sys::{ZDICT_finalizeDictionary, ZDICT_getErrorName, ZDICT_isError, ZDICT_params_t};

const REPEATS: usize = 10;
const SAMPLE_BYTES: usize = 64 << 10;
const RAW_CAP: usize = 64 << 10;
const MAGIC: u32 = 0xec30_a437;
const DICT_ID: u32 = 32_768;
const VARIANTS: [&str; 5] = ["V0", "V1", "V2a", "V2b", "V3"];

/// Thread CPU time, excluding time descheduled during a build or load.
fn cpu_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: time points to writable storage of the type clock_gettime expects.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) },
        0
    );
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

fn finalize(
    segment: &SegmentSample,
    samples: &mut Vec<u8>,
    sizes: &mut Vec<usize>,
    dict: &mut Vec<u8>,
) {
    samples.clear();
    sizes.clear();
    // Select whole encoded blocks across the segment. The finalizer collects
    // literal/sequence frequencies while matching against the retained content.
    let step = segment
        .blocks
        .iter()
        .map(Vec::len)
        .sum::<usize>()
        .div_ceil(SAMPLE_BYTES);
    for block in segment.blocks.iter().step_by(step.max(1)) {
        samples.extend_from_slice(block);
        sizes.push(block.len());
    }
    let content = &segment.dictionary;
    dict.resize(content.len() + 256, 0);
    // SAFETY: sizes partitions the initialized sample buffer, all input
    // buffers stay live and disjoint from the writable dictionary buffer,
    // and the C call retains no pointers.
    let len = unsafe {
        ZDICT_finalizeDictionary(
            dict.as_mut_ptr().cast(),
            dict.len(),
            content.as_ptr().cast(),
            content.len(),
            samples.as_ptr().cast(),
            sizes.as_ptr(),
            sizes.len().try_into().unwrap(),
            ZDICT_params_t {
                compressionLevel: 1,
                notificationLevel: 0,
                dictID: DICT_ID,
            },
        )
    };
    // SAFETY: testing the result is valid for every size_t. The error name,
    // when needed, points to a static NUL-terminated zstd string.
    unsafe {
        if ZDICT_isError(len) != 0 {
            panic!(
                "dictionary finalization: {}",
                std::ffi::CStr::from_ptr(ZDICT_getErrorName(len)).to_string_lossy()
            );
        }
    }
    assert!(len <= dict.len());
    dict.truncate(len);
    assert!(dict.ends_with(content));
}

#[derive(Default)]
struct Encoded {
    dictionary: Vec<u8>,
    bytes: Vec<u8>,
    ends: Vec<usize>,
}

impl Encoded {
    fn frames(&self) -> impl Iterator<Item = &[u8]> {
        self.ends.iter().scan(0, |start, &end| {
            let frame = &self.bytes[*start..end];
            *start = end;
            Some(frame)
        })
    }
}

#[derive(Clone, Copy)]
enum BuildKind {
    Raw,
    Entropy,
    RawLiterals,
}

struct Build {
    kind: BuildKind,
    ctx: CCtx<'static>,
    segments: Vec<Encoded>,
    scratch: Vec<u8>,
    samples: Vec<u8>,
    sizes: Vec<usize>,
    cdict_bytes: usize,
    cdict_peak: usize,
}

impl Build {
    fn new(kind: BuildKind, segments: &[SegmentSample]) -> Self {
        let mut ctx = cctx(Setting::Off);
        if matches!(kind, BuildKind::RawLiterals) {
            ctx.set_parameter(CParameter::LiteralCompressionMode(ParamSwitch::Disable))
                .unwrap();
        }
        Self {
            kind,
            ctx,
            segments: segments.iter().map(|_| Encoded::default()).collect(),
            scratch: Vec::with_capacity(zstd_safe::compress_bound(RAW_CAP)),
            samples: Vec::with_capacity(SAMPLE_BYTES),
            sizes: Vec::new(),
            cdict_bytes: 0,
            cdict_peak: 0,
        }
    }

    fn run(&mut self, segments: &[SegmentSample]) -> (u64, u64) {
        self.cdict_bytes = 0;
        self.cdict_peak = 0;
        let started = cpu_ns();
        let mut finalize_ns = 0;
        for (segment, encoded) in segments.iter().zip(&mut self.segments) {
            encoded.bytes.clear();
            encoded.ends.clear();
            let dict = if matches!(self.kind, BuildKind::Entropy) {
                let begin = cpu_ns();
                finalize(
                    segment,
                    &mut self.samples,
                    &mut self.sizes,
                    &mut encoded.dictionary,
                );
                finalize_ns += cpu_ns() - begin;
                &encoded.dictionary
            } else {
                &segment.dictionary
            };
            self.ctx
                .set_parameter(CParameter::CompressionLevel(1))
                .unwrap();
            self.ctx
                .set_parameter(CParameter::SrcSizeHint(segment.hint as u32))
                .unwrap();
            // As in the builder, the first frame digests the dictionary into
            // a local CDict sized with the block hint; later frames reuse it.
            self.ctx.load_dictionary(dict).unwrap();
            for block in &segment.blocks {
                assert!(block.len() <= RAW_CAP);
                self.scratch.clear();
                self.ctx.compress2(&mut self.scratch, block).unwrap();
                encoded.bytes.extend_from_slice(&self.scratch);
                encoded.ends.push(encoded.bytes.len());
            }
            let with_dict = self.ctx.sizeof();
            self.ctx.disable_dictionary().unwrap();
            let cost = with_dict - self.ctx.sizeof();
            self.cdict_bytes += cost;
            self.cdict_peak = self.cdict_peak.max(cost);
        }
        (cpu_ns() - started, finalize_ns)
    }
}

#[derive(Default)]
struct Layout {
    frames: usize,
    blocks: usize,
    total: usize,
    raw: usize,
    rle: usize,
    literals: usize,
    sequences: usize,
    literal_modes: [usize; 4],
    huffman_bytes: [usize; 4],
    fse_modes: [[usize; 4]; 3],
    fse_bytes: [[usize; 4]; 3],
    huffman_fresh_frames: usize,
    fse_fresh_frames: usize,
    fresh_frames: usize,
    huffman_dict_frames: usize,
    fse_dict_frames: usize,
}

impl Layout {
    fn add(&mut self, frame: &[u8], entropy_dict: bool) {
        self.frames += 1;
        self.total += frame.len();
        let (mut huffman_fresh, mut fse_fresh) = (false, false);
        let (mut huffman_dict, mut fse_dict) = (false, false);
        let mut huffman_from_dict = entropy_dict;
        let mut fse_from_dict = [entropy_dict; 3];
        frame::walk(frame, |block| {
            self.blocks += 1;
            match block {
                Block::Raw(len) => self.raw += len,
                Block::Rle => self.rle += 1,
                Block::Compressed(s) => {
                    self.literals += s.literals;
                    self.sequences += s.sequences;
                    self.literal_modes[s.literal_mode] += 1;
                    self.huffman_bytes[s.literal_mode] += s.huffman.len();
                    if s.literal_mode == 2 {
                        huffman_fresh = true;
                        huffman_from_dict = false;
                    }
                    huffman_dict |= s.literal_mode == 3 && huffman_from_dict;
                    if let Some(tables) = s.fse {
                        for (i, table) in tables.iter().enumerate() {
                            self.fse_modes[i][table.mode] += 1;
                            self.fse_bytes[i][table.mode] += table.description.len();
                            fse_fresh |= table.mode == 2;
                            fse_from_dict[i] &= table.mode == 3;
                            fse_dict |= fse_from_dict[i];
                        }
                    }
                }
            }
        })
        .expect("supported Zstandard frame");
        self.huffman_fresh_frames += usize::from(huffman_fresh);
        self.fse_fresh_frames += usize::from(fse_fresh);
        self.fresh_frames += usize::from(huffman_fresh || fse_fresh);
        self.huffman_dict_frames += usize::from(huffman_dict);
        self.fse_dict_frames += usize::from(fse_dict);
    }

    fn report(&self, name: &str, variant: &str) {
        let overhead = 2 * self.frames + 3 * self.blocks;
        assert_eq!(
            self.total,
            overhead + self.raw + self.rle + self.literals + self.sequences
        );
        println!(
            "layout,{name},{variant},{},{},{},{},{},{},{},{}",
            self.frames,
            self.blocks,
            self.total,
            overhead,
            self.raw,
            self.rle,
            self.literals,
            self.sequences
        );
        println!(
            "reuse,{name},{variant},{},{},{},{},{}",
            self.fresh_frames,
            self.huffman_fresh_frames,
            self.fse_fresh_frames,
            self.huffman_dict_frames,
            self.fse_dict_frames
        );
        for (mode, label) in ["raw", "rle", "compressed", "repeat"].iter().enumerate() {
            println!(
                "mode,{name},{variant},literals,{label},{},{}",
                self.literal_modes[mode], self.huffman_bytes[mode]
            );
        }
        for (i, table) in ["literal_lengths", "offsets", "match_lengths"]
            .iter()
            .enumerate()
        {
            for (mode, label) in ["predefined", "rle", "compressed", "repeat"]
                .iter()
                .enumerate()
            {
                println!(
                    "mode,{name},{variant},{table},{label},{},{}",
                    self.fse_modes[i][mode], self.fse_bytes[i][mode]
                );
            }
        }
    }
}

fn digests<'a>(dictionaries: impl Iterator<Item = &'a [u8]>) -> (Vec<DDict<'a>>, u64) {
    let start = cpu_ns();
    let tables = dictionaries.map(DDict::create_by_reference).collect();
    (tables, cpu_ns() - start)
}

struct Decoder<'a> {
    ctx: DCtx<'static>,
    output: Vec<u8>,
    segments: &'a [SegmentSample],
    built: Option<&'a [Encoded]>,
    digests: Option<&'a [DDict<'a>]>,
    entropy: bool,
}

impl<'a> Decoder<'a> {
    fn new(
        segments: &'a [SegmentSample],
        built: Option<&'a [Encoded]>,
        digests: Option<&'a [DDict<'a>]>,
        entropy: bool,
    ) -> Self {
        Self {
            ctx: dctx(),
            output: vec![0; RAW_CAP],
            segments,
            built,
            digests,
            entropy,
        }
    }

    fn pass(&mut self, verify: bool) -> u128 {
        let started = Instant::now();
        for (i, segment) in self.segments.iter().enumerate() {
            let built = self.built.map(|built| &built[i]);
            let dict = if self.entropy {
                &built.unwrap().dictionary
            } else {
                &segment.dictionary
            };
            let ddict = self.digests.map(|digests| &digests[i]);
            let decode = |(block, frame): (&Vec<u8>, &[u8])| {
                let out = &mut self.output[..block.len()];
                let count = if let Some(ddict) = ddict {
                    self.ctx.decompress_using_ddict(out, frame, ddict)
                } else {
                    self.ctx.decompress_using_dict(out, frame, dict)
                }
                .unwrap();
                assert_eq!(count, block.len());
                if verify {
                    assert_eq!(out, block);
                }
                black_box(out);
            };
            if let Some(built) = built {
                segment.blocks.iter().zip(built.frames()).for_each(decode);
            } else {
                segment
                    .blocks
                    .iter()
                    .zip(segment.frames.iter().map(Vec::as_slice))
                    .for_each(decode);
            }
        }
        started.elapsed().as_nanos()
    }
}

fn proof_dict(sections: Sections<'_>, content: &[u8]) -> Option<Vec<u8>> {
    let tables = sections.fse?;
    if sections.huffman.is_empty()
        || tables.iter().any(|table| table.mode != 2)
        || content.len() < 8
    {
        return None;
    }
    let mut dict = Vec::new();
    dict.extend(MAGIC.to_le_bytes());
    dict.extend(DICT_ID.to_le_bytes());
    dict.extend(sections.huffman);
    for i in [1, 2, 0] {
        dict.extend(tables[i].description);
    }
    for offset in [1u32, 4, 8] {
        dict.extend(offset.to_le_bytes());
    }
    dict.extend(content);
    Some(dict)
}

fn prove(name: &str, segments: &[SegmentSample]) {
    let (mut extracted, mut loaded, mut verified) = (0, 0, 0);
    let mut layout = Layout::default();
    for segment in segments {
        let mut dict = None;
        let mut first = true;
        frame::walk(&segment.probe, |block| {
            if first && let Block::Compressed(sections) = block {
                dict = proof_dict(sections, &segment.dictionary);
            }
            first = false;
        })
        .expect("supported probe frame");
        let Some(dict) = dict else {
            continue;
        };
        extracted += 1;
        let (Some(ddict), Some(cdict)) = (
            DDict::try_create(&dict),
            CDict::try_create_by_reference(&dict, 1),
        ) else {
            continue;
        };
        assert_eq!(ddict.get_dict_id().unwrap().get(), DICT_ID);
        loaded += 1;
        let mut encoder = cctx(Setting::Off);
        encoder.ref_cdict(&cdict).unwrap();
        let mut decoder = dctx();
        let mut stored = Vec::with_capacity(zstd_safe::compress_bound(RAW_CAP));
        let mut output = vec![0; RAW_CAP];
        for block in &segment.blocks {
            stored.clear();
            encoder.compress2(&mut stored, block).unwrap();
            let n = decoder
                .decompress_using_ddict(&mut output[..block.len()], &stored, &ddict)
                .unwrap();
            assert_eq!(&output[..n], block);
            layout.add(&stored, true);
            verified += 1;
        }
    }
    println!(
        "proof,{name},{},{extracted},{loaded},{verified},{},{}",
        segments.len(),
        layout.huffman_dict_frames,
        layout.fse_dict_frames
    );
}

fn order<const N: usize>(repeat: usize) -> [usize; N] {
    std::array::from_fn(|i| {
        let i = if (repeat / N).is_multiple_of(2) {
            i
        } else {
            N - 1 - i
        };
        (i + repeat) % N
    })
}

pub fn run(name: &str, segments: &[SegmentSample]) {
    assert!(!segments.is_empty(), "no sampled Zstandard segments");
    let raw: usize = segments.iter().flat_map(|s| &s.blocks).map(Vec::len).sum();
    let frames: usize = segments.iter().map(|s| s.frames.len()).sum();
    let records: usize = segments
        .iter()
        .flat_map(|s| &s.blocks)
        .map(|b| record_ends(b).len())
        .sum();
    println!("sample,{name},{},{frames},{raw},{records}", segments.len());

    let mut builds = [BuildKind::Raw, BuildKind::Entropy, BuildKind::RawLiterals]
        .map(|kind| Build::new(kind, segments));
    let mut build_ns = [[0; REPEATS]; 3];
    let mut finalization = [0; REPEATS];
    for build in &mut builds {
        build.run(segments);
    }
    for repeat in 0..REPEATS {
        for i in order::<3>(repeat) {
            let (cpu, finalized) = builds[i].run(segments);
            build_ns[i][repeat] = cpu;
            if i == 1 {
                finalization[repeat] = finalized;
            }
        }
    }

    let [v0, v2, v3] = &builds;
    let parity = segments
        .iter()
        .zip(&v0.segments)
        .flat_map(|(s, b)| s.frames.iter().zip(b.frames()))
        .filter(|(stored, rebuilt)| stored.as_slice() != *rebuilt)
        .count();
    println!("parity,{name},{frames},{parity}");

    let mut original = Layout::default();
    let mut entropy = Layout::default();
    let mut raw_literals = Layout::default();
    for s in segments {
        for frame in &s.frames {
            original.add(frame, false);
        }
    }
    for (build, layout, full_dict) in [(v2, &mut entropy, true), (v3, &mut raw_literals, false)] {
        for frame in build.segments.iter().flat_map(Encoded::frames) {
            layout.add(frame, full_dict);
        }
    }
    assert_eq!(
        raw_literals.literal_modes[2] + raw_literals.literal_modes[3],
        0
    );
    original.report(name, "V0");
    entropy.report(name, "V2");
    raw_literals.report(name, "V3");

    let mut load_ns = [[0; 2]; REPEATS];
    let (mut raw_dd, mut entropy_dd) = (Vec::new(), Vec::new());
    for (repeat, load) in load_ns.iter_mut().enumerate() {
        for i in order::<2>(repeat) {
            if i == 0 {
                let (digests, elapsed) = digests(segments.iter().map(|s| s.dictionary.as_slice()));
                raw_dd = digests;
                load[0] = elapsed;
            } else {
                let (digests, elapsed) =
                    digests(v2.segments.iter().map(|s| s.dictionary.as_slice()));
                entropy_dd = digests;
                load[1] = elapsed;
            }
        }
    }
    let raw_dicts: usize = segments.iter().map(|s| s.dictionary.len()).sum();
    let full_dicts: usize = v2.segments.iter().map(|s| s.dictionary.len()).sum();
    let mut decoders = [
        Decoder::new(segments, None, None, false),
        Decoder::new(segments, None, Some(&raw_dd), false),
        Decoder::new(segments, Some(&v2.segments), None, true),
        Decoder::new(segments, Some(&v2.segments), Some(&entropy_dd), true),
        Decoder::new(segments, Some(&v3.segments), None, false),
    ];
    for decoder in &mut decoders {
        decoder.pass(true);
    }
    for repeat in 0..REPEATS {
        for i in order::<5>(repeat) {
            let elapsed = decoders[i].pass(false);
            let label = VARIANTS[i];
            let build = [0, 0, 1, 1, 2][i];
            let load = match i {
                1 => load_ns[repeat][0],
                3 => load_ns[repeat][1],
                _ => 0,
            };
            let finalized = if build == 1 { finalization[repeat] } else { 0 };
            println!(
                "timing,{name},{label},{repeat},{elapsed},{},{finalized},{load}",
                build_ns[build][repeat] + load
            );
        }
    }
    for (i, (layout, dicts, ddicts, build)) in [
        (&original, raw_dicts, 0, v0),
        (
            &original,
            raw_dicts,
            raw_dd.iter().map(DDict::sizeof).sum(),
            v0,
        ),
        (&entropy, full_dicts, 0, v2),
        (
            &entropy,
            full_dicts,
            entropy_dd.iter().map(DDict::sizeof).sum(),
            v2,
        ),
        (&raw_literals, raw_dicts, 0, v3),
    ]
    .into_iter()
    .enumerate()
    {
        let label = VARIANTS[i];
        println!(
            "storage,{name},{label},{},{dicts},{ddicts},{},{},{}",
            layout.total, build.cdict_bytes, build.cdict_peak, layout.fresh_frames
        );
    }
    prove(name, segments);
}
