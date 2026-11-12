//! Research (5AB): whether another immutable-run layout moves the run's
//! density–latency frontier. Nothing here is used by the engine; the run
//! format is unchanged. Beside the production run, at its own block sizes,
//! two candidate layouts:
//!
//! - [`Blocks`]: the production block format behind the same kind of sparse
//!   index, each block compressed against a Zstandard dictionary trained on
//!   the run's records, so small blocks can keep the density of large ones.
//! - [`Split`]: each record's key, sequence number and a reference to its
//!   value in uncompressed production-format key blocks, searched without
//!   decoding; the values of each key block in one independently
//!   compressed group, decoded only to read a value.
//!
//! Both keep sequence numbers, tombstones, version order, the run's Bloom
//! filter and a CRC-32 over every compressed frame, verified on each
//! decode, as the production run does. `layouts_agree_with_the_production_run`
//! checks them against the production run; `layout_tradeoffs` measures them.
//!
//! Research (5AD): `decode_costs` breaks a block read into its parts, and
//! `dictionary_runs` builds [`Blocks`] at 4 KiB with a dictionary trained on
//! samples of the input runs' blocks, beside production Zstandard and LZ4
//! runs, recording training, construction and read costs:
//!
//! ```text
//! cargo test --release --lib run::tests::layouts -- --ignored --nocapture
//! ```

use std::hint::black_box;
use std::time::Instant;

use zstd_safe::{CCtx, CDict, DCtx, DDict};

use super::costs::{Rng, compare, document};
use crate::block::{BlockBuilder, BlockPosition, BlockView, Record, Value, Version};
use crate::run::filter::{self as key_filter, KeyFilter, KeyHashes};
use crate::run::{Codec, ReadWorkspace, Run, RunBuilder};

const LEVEL: i32 = 3;
/// A frame header: CRC-32 of the stored bytes, then the decoded length.
const HEADER: usize = 8;
/// The production frame header and footer, for its accounting.
const RUN_HEADER: usize = 12;
const RUN_FOOTER: usize = 32;

/// A record: key, sequence number, and value or `None` for a tombstone.
type Owned = (Vec<u8>, u64, Option<Vec<u8>>);

fn value_of(value: &Option<Vec<u8>>) -> Value<'_> {
    value.as_deref().map_or(Value::Tombstone, Value::Bytes)
}

/// What a layout holds, by role.
#[derive(Default)]
struct Bytes {
    /// Compressed blocks, or compressed value groups.
    payload: usize,
    /// Uncompressed key blocks: keys, sequence numbers, value references.
    keys: usize,
    /// Frame headers, footers and key-block checksums.
    framing: usize,
    /// Block offsets and each block's last key.
    index: usize,
    /// The dictionary in its decoding form, held as long as the run.
    dictionary: usize,
    filter: usize,
}

impl Bytes {
    fn total(&self) -> usize {
        self.payload + self.keys + self.framing + self.index + self.dictionary + self.filter
    }
}

/// A reader's state: the production workspace, or a decompression context
/// and a buffer for the candidates.
struct Workspace {
    read: ReadWorkspace,
    dctx: DCtx<'static>,
    buf: Vec<u8>,
    position: BlockPosition,
}

impl Workspace {
    fn new() -> Self {
        Self {
            read: ReadWorkspace::new(),
            dctx: DCtx::create(),
            buf: Vec::new(),
            position: BlockPosition::new(),
        }
    }
}

trait Layout {
    /// The newest version of `key`, as [`Run::get`] returns it.
    fn get<T>(
        &self,
        key: &[u8],
        workspace: &mut Workspace,
        found: impl FnOnce(Option<Version<'_>>) -> T,
    ) -> T;
    /// Every record whose key starts with `prefix`, in order, every version.
    fn scan(&self, prefix: &[u8], workspace: &mut Workspace, visit: impl FnMut(Record<'_>));
    fn bytes(&self) -> Bytes;
    /// What a reader holds to decode: buffers and contexts.
    fn workspace_bytes(&self, workspace: &Workspace) -> usize;
}

impl Layout for Run<Vec<u8>> {
    fn get<T>(
        &self,
        key: &[u8],
        workspace: &mut Workspace,
        found: impl FnOnce(Option<Version<'_>>) -> T,
    ) -> T {
        found(Run::get(self, key, &mut workspace.read).unwrap())
    }

    fn scan(&self, prefix: &[u8], workspace: &mut Workspace, mut visit: impl FnMut(Record<'_>)) {
        let mut cursor = self.cursor(&mut workspace.read);
        cursor.seek_prefix(prefix).unwrap();
        while let Some(record) = cursor.next_record().unwrap() {
            visit(record);
        }
    }

    fn bytes(&self) -> Bytes {
        let index = self.view().index_len();
        let framing = self.block_count() * RUN_HEADER + RUN_FOOTER;
        Bytes {
            payload: self.as_bytes().len() - index - framing,
            framing,
            index,
            filter: self.filter_bytes(),
            ..Bytes::default()
        }
    }

    fn workspace_bytes(&self, workspace: &Workspace) -> usize {
        workspace.read.capacity() + workspace.read.context_bytes()
    }
}

// Compression shared by the candidates.

#[derive(Clone, Copy, Debug, PartialEq)]
enum Pack {
    Zstd,
    Lz4,
}

/// Trains a dictionary of at most `size` bytes on `samples`.
fn train<'a>(size: usize, samples: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let (mut buffer, mut sizes) = (Vec::new(), Vec::new());
    for sample in samples {
        buffer.extend_from_slice(sample);
        sizes.push(sample.len());
    }
    let mut dictionary = Vec::with_capacity(size);
    zstd_safe::train_from_buffer(&mut dictionary, &buffer, &sizes).unwrap();
    dictionary
}

/// About `budget` bytes of samples, taken evenly across `records`.
fn sampled<'a>(
    records: &'a [Owned],
    budget: usize,
    sample: impl Fn(&'a Owned) -> Option<&'a [u8]> + 'a,
) -> impl Iterator<Item = &'a [u8]> {
    let total: usize = records
        .iter()
        .map(|record| record.0.len() + record.2.as_ref().map_or(0, Vec::len))
        .sum();
    let step = (total / budget.max(1)).max(1);
    records.iter().step_by(step).filter_map(sample)
}

struct Compressor {
    pack: Pack,
    cctx: CCtx<'static>,
    cdict: Option<CDict<'static>>,
    scratch: Vec<u8>,
}

impl Compressor {
    fn new(pack: Pack, dictionary: Option<&[u8]>) -> Self {
        Self {
            pack,
            cctx: CCtx::create(),
            cdict: dictionary.map(|dictionary| CDict::create(dictionary, LEVEL)),
            scratch: Vec::new(),
        }
    }

    /// Appends `raw` to `out` as a frame: header, then the stored bytes.
    fn frame(&mut self, raw: &[u8], out: &mut Vec<u8>) {
        self.scratch.clear();
        match self.pack {
            Pack::Zstd => {
                self.scratch.reserve(zstd_safe::compress_bound(raw.len()));
                match &self.cdict {
                    Some(cdict) => self
                        .cctx
                        .compress_using_cdict(&mut self.scratch, raw, cdict),
                    None => self.cctx.compress(&mut self.scratch, raw, LEVEL),
                }
                .unwrap();
            }
            Pack::Lz4 => {
                self.scratch
                    .resize(lz4_flex::block::get_maximum_output_size(raw.len()), 0);
                let written = lz4_flex::block::compress_into(raw, &mut self.scratch).unwrap();
                self.scratch.truncate(written);
            }
        }
        out.extend_from_slice(&crc32fast::hash(&self.scratch).to_le_bytes());
        out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.scratch);
    }
}

/// Verifies and decodes one frame into `buf`.
fn unframe<'w>(
    pack: Pack,
    ddict: Option<&DDict<'_>>,
    frame: &[u8],
    dctx: &mut DCtx<'_>,
    buf: &'w mut Vec<u8>,
) -> &'w [u8] {
    let (header, stored) = frame.split_at(HEADER);
    let checksum = u32::from_le_bytes(header[..4].try_into().unwrap());
    assert_eq!(crc32fast::hash(stored), checksum, "damaged frame");
    let len = u32::from_le_bytes(header[4..].try_into().unwrap()) as usize;
    buf.clear();
    match pack {
        Pack::Zstd => {
            buf.reserve(len);
            match ddict {
                Some(ddict) => dctx.decompress_using_ddict(buf, stored, ddict),
                None => dctx.decompress(buf, stored),
            }
            .unwrap();
        }
        Pack::Lz4 => {
            buf.resize(len, 0);
            lz4_flex::block::decompress_into(stored, buf).unwrap();
        }
    }
    assert_eq!(buf.len(), len);
    buf
}

/// Each block's last key, for a binary search.
#[derive(Default)]
struct Index {
    keys: Vec<u8>,
    ends: Vec<u32>,
}

impl Index {
    fn push(&mut self, key: &[u8]) {
        self.keys.extend_from_slice(key);
        self.ends.push(self.keys.len() as u32);
    }

    fn len(&self) -> usize {
        self.ends.len()
    }

    fn key(&self, at: usize) -> &[u8] {
        let start = at
            .checked_sub(1)
            .map_or(0, |before| self.ends[before] as usize);
        &self.keys[start..self.ends[at] as usize]
    }

    /// The first block whose last key is at least `key`, or `len()`.
    fn find(&self, key: &[u8]) -> usize {
        let (mut low, mut high) = (0, self.len());
        while low < high {
            let mid = (low + high) / 2;
            if self.key(mid) < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }

    fn bytes(&self) -> usize {
        self.keys.len() + self.ends.len() * size_of::<u32>()
    }
}

/// The production rule for ending a block before `key`: at the target, or
/// at twice the target within one key's versions.
fn ends_block(block: &BlockBuilder, key: &[u8], value: &Option<Vec<u8>>, target: usize) -> bool {
    if block.is_empty() {
        return false;
    }
    let len = key.len() + value.as_ref().map_or(0, Vec::len);
    let limit = if block.last_key() == key {
        2 * target
    } else {
        target
    };
    len >= target || block.encoded_len() + len / 2 >= limit
}

fn filter_of(records: &[Owned]) -> Option<KeyFilter> {
    let mut hashes = KeyHashes::new();
    for (key, ..) in records {
        hashes.add(key);
    }
    hashes.finish()
}

fn excluded(filter: Option<&KeyFilter>, key: &[u8]) -> bool {
    filter.is_some_and(|filter| !filter.may_contain(key_filter::hash(key)))
}

// Candidate: dictionary-compressed blocks.

struct Blocks {
    pack: Pack,
    ddict: Option<DDict<'static>>,
    frames: Vec<u8>,
    offsets: Vec<u64>,
    index: Index,
    filter: Option<KeyFilter>,
}

impl Blocks {
    fn build(records: &[Owned], pack: Pack, target: usize, dictionary: Option<&[u8]>) -> Self {
        let mut compressor = Compressor::new(pack, dictionary);
        let mut layout = Self {
            pack,
            ddict: dictionary.map(DDict::create),
            frames: Vec::new(),
            offsets: Vec::new(),
            index: Index::default(),
            filter: filter_of(records),
        };
        let (mut block, mut raw) = (BlockBuilder::new(), Vec::new());
        let mut cut = |block: &mut BlockBuilder, layout: &mut Self| {
            layout.index.push(block.last_key());
            layout.offsets.push(layout.frames.len() as u64);
            raw.clear();
            block.flush_into(&mut raw);
            compressor.frame(&raw, &mut layout.frames);
        };
        for (key, seq, value) in records {
            if ends_block(&block, key, value, target) {
                cut(&mut block, &mut layout);
            }
            block.add(key, *seq, value_of(value)).unwrap();
        }
        if !block.is_empty() {
            cut(&mut block, &mut layout);
        }
        layout.offsets.push(layout.frames.len() as u64);
        layout.frames.shrink_to_fit();
        layout
    }

    fn load<'w>(&self, at: usize, dctx: &mut DCtx<'_>, buf: &'w mut Vec<u8>) -> BlockView<'w> {
        let frame = &self.frames[self.offsets[at] as usize..self.offsets[at + 1] as usize];
        BlockView::open(unframe(self.pack, self.ddict.as_ref(), frame, dctx, buf)).unwrap()
    }
}

impl Layout for Blocks {
    fn get<T>(
        &self,
        key: &[u8],
        workspace: &mut Workspace,
        found: impl FnOnce(Option<Version<'_>>) -> T,
    ) -> T {
        if excluded(self.filter.as_ref(), key) {
            return found(None);
        }
        let at = self.index.find(key);
        if at == self.index.len() {
            return found(None);
        }
        let block = self.load(at, &mut workspace.dctx, &mut workspace.buf);
        found(block.get(key).unwrap())
    }

    fn scan(&self, prefix: &[u8], workspace: &mut Workspace, mut visit: impl FnMut(Record<'_>)) {
        let Workspace {
            dctx,
            buf,
            position,
            ..
        } = workspace;
        let start = self.index.find(prefix);
        for at in start..self.index.len() {
            let block = self.load(at, dctx, &mut *buf);
            if at == start {
                position.seek(block, prefix).unwrap();
            } else {
                position.reset();
            }
            while let Some(record) = position.next_record(block).unwrap() {
                if !record.key.starts_with(prefix) {
                    return;
                }
                visit(record);
            }
        }
    }

    fn bytes(&self) -> Bytes {
        let blocks = self.index.len();
        Bytes {
            payload: self.frames.len() - blocks * HEADER,
            framing: blocks * HEADER,
            index: self.index.bytes() + self.offsets.len() * size_of::<u64>(),
            dictionary: self.ddict.as_ref().map_or(0, DDict::sizeof),
            filter: self.filter.as_ref().map_or(0, KeyFilter::bytes),
            ..Bytes::default()
        }
    }

    fn workspace_bytes(&self, workspace: &Workspace) -> usize {
        workspace.buf.capacity() + workspace.dctx.sizeof()
    }
}

// Candidate: searchable keys, separately compressed values.

fn write_varint(out: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_varint(input: &mut &[u8]) -> usize {
    let mut value = 0;
    for shift in (0..).step_by(7) {
        let (&byte, rest) = input.split_first().unwrap();
        *input = rest;
        value |= usize::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            break;
        }
    }
    value
}

/// A value reference: offset and length within the group.
fn value_range(reference: &[u8]) -> std::ops::Range<usize> {
    let mut reference = reference;
    let start = read_varint(&mut reference);
    start..start + read_varint(&mut reference)
}

struct Split {
    pack: Pack,
    ddict: Option<DDict<'static>>,
    /// Key blocks: production blocks whose values are value references.
    keys: Vec<u8>,
    key_offsets: Vec<u64>,
    /// A CRC-32 per key block, checked when the layout is built, as an
    /// uncompressed production block is checked when its run is loaded.
    key_checksums: Vec<u32>,
    groups: Vec<u8>,
    group_offsets: Vec<u64>,
    index: Index,
    filter: Option<KeyFilter>,
}

/// Key blocks are cut at this size whatever their values, so that runs of
/// tombstones keep them searchable.
const MAX_KEY_BLOCK: usize = 16 << 10;

impl Split {
    fn build(records: &[Owned], pack: Pack, target: usize, dictionary: Option<&[u8]>) -> Self {
        let mut compressor = Compressor::new(pack, dictionary);
        let mut layout = Self {
            pack,
            ddict: dictionary.map(DDict::create),
            keys: Vec::new(),
            key_offsets: Vec::new(),
            key_checksums: Vec::new(),
            groups: Vec::new(),
            group_offsets: Vec::new(),
            index: Index::default(),
            filter: filter_of(records),
        };
        let (mut block, mut values, mut reference) = (BlockBuilder::new(), Vec::new(), Vec::new());
        let mut cut = |block: &mut BlockBuilder, values: &mut Vec<u8>, layout: &mut Self| {
            layout.index.push(block.last_key());
            let start = layout.keys.len();
            layout.key_offsets.push(start as u64);
            block.flush_into(&mut layout.keys);
            layout
                .key_checksums
                .push(crc32fast::hash(&layout.keys[start..]));
            layout.group_offsets.push(layout.groups.len() as u64);
            compressor.frame(values, &mut layout.groups);
            values.clear();
        };
        for (key, seq, value) in records {
            let full = values.len() + value.as_ref().map_or(0, Vec::len) / 2 >= target
                || block.encoded_len() >= MAX_KEY_BLOCK;
            if !block.is_empty() && block.last_key() != key.as_slice() && full {
                cut(&mut block, &mut values, &mut layout);
            }
            match value {
                Some(value) => {
                    reference.clear();
                    write_varint(&mut reference, values.len());
                    write_varint(&mut reference, value.len());
                    values.extend_from_slice(value);
                    block.add(key, *seq, Value::Bytes(&reference)).unwrap();
                }
                None => block.add(key, *seq, Value::Tombstone).unwrap(),
            }
        }
        if !block.is_empty() {
            cut(&mut block, &mut values, &mut layout);
        }
        layout.key_offsets.push(layout.keys.len() as u64);
        layout.group_offsets.push(layout.groups.len() as u64);
        for at in 0..layout.index.len() {
            assert_eq!(
                crc32fast::hash(layout.key_block(at)),
                layout.key_checksums[at]
            );
        }
        layout.keys.shrink_to_fit();
        layout.groups.shrink_to_fit();
        layout
    }

    fn key_block(&self, at: usize) -> &[u8] {
        &self.keys[self.key_offsets[at] as usize..self.key_offsets[at + 1] as usize]
    }

    fn group<'w>(&self, at: usize, dctx: &mut DCtx<'_>, buf: &'w mut Vec<u8>) -> &'w [u8] {
        let frame =
            &self.groups[self.group_offsets[at] as usize..self.group_offsets[at + 1] as usize];
        unframe(self.pack, self.ddict.as_ref(), frame, dctx, buf)
    }
}

impl Layout for Split {
    fn get<T>(
        &self,
        key: &[u8],
        workspace: &mut Workspace,
        found: impl FnOnce(Option<Version<'_>>) -> T,
    ) -> T {
        if excluded(self.filter.as_ref(), key) {
            return found(None);
        }
        let at = self.index.find(key);
        if at == self.index.len() {
            return found(None);
        }
        let keys = BlockView::open(self.key_block(at)).unwrap();
        match keys.get(key).unwrap() {
            Some(Version {
                seq,
                value: Value::Bytes(reference),
            }) => {
                let group = self.group(at, &mut workspace.dctx, &mut workspace.buf);
                found(Some(Version {
                    seq,
                    value: Value::Bytes(&group[value_range(reference)]),
                }))
            }
            other => found(other),
        }
    }

    fn scan(&self, prefix: &[u8], workspace: &mut Workspace, mut visit: impl FnMut(Record<'_>)) {
        let Workspace {
            dctx,
            buf,
            position,
            ..
        } = workspace;
        let start = self.index.find(prefix);
        for at in start..self.index.len() {
            let keys = BlockView::open(self.key_block(at)).unwrap();
            if at == start {
                position.seek(keys, prefix).unwrap();
            } else {
                position.reset();
            }
            // A scan reads most of the group, so it is decoded up front.
            let group = self.group(at, dctx, &mut *buf);
            while let Some(record) = position.next_record(keys).unwrap() {
                if !record.key.starts_with(prefix) {
                    return;
                }
                let value = match record.value {
                    Value::Tombstone => Value::Tombstone,
                    Value::Bytes(reference) => Value::Bytes(&group[value_range(reference)]),
                };
                visit(Record {
                    key: record.key,
                    seq: record.seq,
                    value,
                });
            }
        }
    }

    fn bytes(&self) -> Bytes {
        let groups = self.index.len();
        Bytes {
            payload: self.groups.len() - groups * HEADER,
            keys: self.keys.len(),
            framing: groups * HEADER + self.key_checksums.len() * size_of::<u32>(),
            index: self.index.bytes()
                + (self.key_offsets.len() + self.group_offsets.len()) * size_of::<u64>(),
            dictionary: self.ddict.as_ref().map_or(0, DDict::sizeof),
            filter: self.filter.as_ref().map_or(0, KeyFilter::bytes),
        }
    }

    fn workspace_bytes(&self, workspace: &Workspace) -> usize {
        workspace.buf.capacity() + workspace.dctx.sizeof()
    }
}

// The candidates under test.

enum Built {
    Run(Run<Vec<u8>>),
    Blocks(Blocks),
    Split(Split),
}

/// A layout to build: its name and how.
#[derive(Clone, Copy)]
enum Spec {
    Run(Codec, usize),
    Blocks(Pack, usize, Option<usize>),
    Split(Pack, usize, Option<usize>),
}

impl Spec {
    fn name(self) -> String {
        let kib = |bytes: usize| {
            if bytes >= 1024 {
                format!("{} KiB", bytes / 1024)
            } else {
                format!("{bytes} B")
            }
        };
        let dict = |size: Option<usize>| {
            size.map_or(String::new(), |size| format!(" + {} dict", kib(size)))
        };
        match self {
            Self::Run(codec, target) => format!("run, {codec:?} {} blocks", kib(target)),
            Self::Blocks(pack, target, size) => {
                format!("blocks, {pack:?} {}{}", kib(target), dict(size))
            }
            Self::Split(pack, target, size) => {
                format!("split, {pack:?} {} value groups{}", kib(target), dict(size))
            }
        }
    }

    /// Builds the layout, returning it and the seconds spent training.
    fn build(self, records: &[Owned]) -> (Built, f64) {
        let start = Instant::now();
        let dictionary = |size: Option<usize>, values_only: bool| {
            size.map(|size| {
                train(
                    size,
                    sampled(records, 100 * size, move |(key, _, value)| {
                        if values_only {
                            value.as_deref()
                        } else {
                            value.as_deref().or(Some(key.as_slice()))
                        }
                    }),
                )
            })
        };
        match self {
            Self::Run(codec, target) => {
                let mut builder = RunBuilder::with_target_block_len(codec, target);
                for (key, seq, value) in records {
                    builder.add(key, *seq, value_of(value)).unwrap();
                }
                (Built::Run(builder.finish()), 0.0)
            }
            Self::Blocks(pack, target, size) => {
                let dictionary = dictionary(size, false);
                let trained = start.elapsed().as_secs_f64();
                let layout = Blocks::build(records, pack, target, dictionary.as_deref());
                (Built::Blocks(layout), trained)
            }
            Self::Split(pack, target, size) => {
                let dictionary = dictionary(size, true);
                let trained = start.elapsed().as_secs_f64();
                let layout = Split::build(records, pack, target, dictionary.as_deref());
                (Built::Split(layout), trained)
            }
        }
    }
}

/// Calls `$body` with `$layout` bound to the built layout's concrete type.
macro_rules! with_layout {
    ($built:expr, $layout:ident => $body:expr) => {
        match $built {
            Built::Run($layout) => $body,
            Built::Blocks($layout) => $body,
            Built::Split($layout) => $body,
        }
    };
}

// Correctness.

/// Every record, every newest version, every probed absence and every
/// prefix scan of `layout` against the records it was built from.
fn check<L: Layout>(layout: &L, records: &[Owned], probes: &[Vec<u8>], prefixes: &[Vec<u8>]) {
    let mut workspace = Workspace::new();
    let owned = |record: Record<'_>| -> Owned {
        let value = match record.value {
            Value::Bytes(bytes) => Some(bytes.to_vec()),
            Value::Tombstone => None,
        };
        (record.key.to_vec(), record.seq, value)
    };
    for prefix in prefixes {
        let mut seen = Vec::new();
        layout.scan(prefix, &mut workspace, |record| seen.push(owned(record)));
        let expected: Vec<&Owned> = records.iter().filter(|r| r.0.starts_with(prefix)).collect();
        assert_eq!(
            seen.iter().collect::<Vec<_>>(),
            expected,
            "prefix {prefix:x?}"
        );
    }
    for key in probes {
        let newest = records
            .iter()
            .find(|record| record.0 == *key)
            .map(|(_, seq, value)| (*seq, value.clone()));
        let got = layout.get(key, &mut workspace, |version| {
            version.map(|version| {
                let value = match version.value {
                    Value::Bytes(bytes) => Some(bytes.to_vec()),
                    Value::Tombstone => None,
                };
                (version.seq, value)
            })
        });
        assert_eq!(got, newest, "key {key:x?}");
    }
}

/// Records with binary and shared-prefix keys, version chains (one longer
/// than a block), tombstones between versions, and empty values.
fn tricky_records() -> Vec<Owned> {
    let mut keys: Vec<Vec<u8>> = (0..2_000u64)
        .map(|i| format!("k/{:05}/{}", i * 7 % 2_000, "x".repeat((i % 5) as usize)).into_bytes())
        .collect();
    keys.extend([
        vec![0x00],
        vec![0x00, 0x00],
        vec![0x00, 0xFF],
        vec![0xFF],
        vec![0xFF, 0xFF],
        vec![0xFF, 0xFF, 0x00],
        b"long chain".to_vec(),
    ]);
    keys.sort();
    keys.dedup();
    let mut records = Vec::new();
    for (i, key) in keys.into_iter().enumerate() {
        let i = i as u64;
        let base = 1_000 * (i + 1);
        let versions = if key == b"long chain" {
            300
        } else if i.is_multiple_of(7) {
            3
        } else {
            1
        };
        for version in (0..versions).rev() {
            let value = if i.is_multiple_of(11) && version + 1 == versions
                || version == 1 && i.is_multiple_of(7)
            {
                None
            } else if i.is_multiple_of(13) {
                Some(Vec::new())
            } else {
                Some(compare::json(i, version))
            };
            records.push((key.clone(), base + version, value));
        }
    }
    records
}

#[test]
fn layouts_agree_with_the_production_run() {
    let records = tricky_records();
    let mut probes: Vec<Vec<u8>> = records.iter().map(|r| r.0.clone()).collect();
    probes.dedup();
    let absent: Vec<Vec<u8>> = probes
        .iter()
        .step_by(3)
        .flat_map(|key| {
            [
                [key.as_slice(), &[0x7F]].concat(),
                key[..key.len() - 1].to_vec(),
            ]
        })
        .chain([b"".to_vec(), b"j".to_vec(), b"z".to_vec(), vec![0xFF; 4]])
        .collect();
    let mut prefixes: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"k".to_vec(),
        b"k/0".to_vec(),
        b"k/01".to_vec(),
        b"j".to_vec(),
        b"z".to_vec(),
        vec![0x00],
        vec![0xFF],
        vec![0xFF, 0xFF],
        b"long chain".to_vec(),
    ];
    prefixes.extend(probes.iter().step_by(97).cloned());
    probes.extend(absent);

    // Duplicate copies of one version are refused alike: every layout
    // builds its blocks with the production block builder.
    let mut block = BlockBuilder::new();
    block.add(b"k", 2, Value::Tombstone).unwrap();
    assert!(block.add(b"k", 2, Value::Tombstone).is_err());
    let mut builder = RunBuilder::default();
    builder.add(b"k", 2, Value::Tombstone).unwrap();
    assert!(builder.add(b"k", 2, Value::Tombstone).is_err());

    for spec in [
        Spec::Run(Codec::Zstd, 256),
        Spec::Blocks(Pack::Zstd, 256, None),
        Spec::Blocks(Pack::Zstd, 256, Some(4 << 10)),
        Spec::Blocks(Pack::Lz4, 256, None),
        Spec::Split(Pack::Zstd, 128, Some(4 << 10)),
        Spec::Split(Pack::Lz4, 128, None),
        Spec::Split(Pack::Zstd, 1 << 20, None),
    ] {
        let (built, _) = spec.build(&records);
        with_layout!(&built, layout => check(layout, &records, &probes, &prefixes));
    }

    // 5AD: a dictionary trained on decoded blocks of overlapping inputs, as
    // a compaction would train it before merging them.
    let inputs: Vec<Blocks> = (0..3)
        .map(|part| {
            let part: Vec<Owned> = records.iter().skip(part).step_by(3).cloned().collect();
            Blocks::build(&part, Pack::Zstd, 1 << 10, None)
        })
        .collect();
    let (samples, sizes) = input_samples(&inputs, 64 << 10);
    assert!(sizes.len() > 8, "{} samples", sizes.len());
    let dictionary = train_on(4 << 10, &samples, &sizes);
    let layout = Blocks::build(&records, Pack::Zstd, 256, Some(&dictionary));
    check(&layout, &records, &probes, &prefixes);
}

/// A dictionary frame names its dictionary, and decodes with that one only:
/// without it, or with another, Zstandard refuses the frame rather than
/// produce other bytes. A format would still record the codec explicitly;
/// the identifier is a second check.
#[test]
fn dictionary_frames_name_their_dictionary_and_need_it() {
    let records = tricky_records();
    let samples: Vec<&[u8]> = records.iter().filter_map(|r| r.2.as_deref()).collect();
    let dictionary = train(4 << 10, samples.iter().step_by(2).copied());
    let other = train(4 << 10, samples.iter().skip(1).step_by(2).copied());
    let id = zstd_safe::get_dict_id(&dictionary).unwrap();
    assert_ne!(Some(id), zstd_safe::get_dict_id(&other));

    let layout = Blocks::build(&records, Pack::Zstd, 1 << 10, Some(&dictionary));
    let (mut dctx, mut buf) = (DCtx::create(), Vec::with_capacity(8 << 10));
    for at in 0..layout.index.len() {
        let stored = layout.stored(at);
        assert_eq!(zstd_safe::get_dict_id_from_frame(stored), Some(id));
        buf.clear();
        assert!(dctx.decompress(&mut buf, stored).is_err());
        buf.clear();
        let wrong = DDict::create(&other);
        assert!(
            dctx.decompress_using_ddict(&mut buf, stored, &wrong)
                .is_err()
        );
        buf.clear();
        let right = layout.ddict.as_ref().unwrap();
        assert!(dctx.decompress_using_ddict(&mut buf, stored, right).is_ok());
    }
}

// Measurements.

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

/// Half the range as a share of the median.
fn spread(samples: &[f64]) -> f64 {
    let (low, high) = samples
        .iter()
        .fold((f64::MAX, f64::MIN), |(low, high), &x| {
            (low.min(x), high.max(x))
        });
    (high - low) / 2.0 / median(samples.to_vec()) * 100.0
}

/// Kibibytes of this process's `field` in `/proc/self/status`.
fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}

/// Mean ns per get over three timed rounds (median and spread), and the
/// p99 of one round timed per get.
fn time_gets<L: Layout>(layout: &L, workspace: &mut Workspace, keys: &[Vec<u8>]) -> TimedGets {
    let get = |workspace: &mut Workspace, key: &[u8]| {
        layout.get(key, workspace, |version| {
            black_box(version.map(|version| match version.value {
                Value::Bytes(bytes) => bytes.len(),
                Value::Tombstone => 0,
            }))
        });
    };
    for key in &keys[..keys.len() / 4] {
        get(workspace, key);
    }
    let rounds: Vec<f64> = (0..3)
        .map(|_| {
            let start = Instant::now();
            for key in keys {
                get(workspace, key);
            }
            start.elapsed().as_nanos() as f64 / keys.len() as f64
        })
        .collect();
    let mut each: Vec<u64> = keys
        .iter()
        .map(|key| {
            let start = Instant::now();
            get(workspace, key);
            start.elapsed().as_nanos() as u64
        })
        .collect();
    each.sort_unstable();
    let p99 = each[each.len() * 99 / 100] as f64;
    TimedGets {
        mean: median(rounds.clone()),
        spread: spread(&rounds),
        p50: each[each.len() / 2] as f64,
        p99,
    }
}

/// Lookup timings: the median of three rounds' mean, the spread of those
/// rounds, and percentiles of one round timed per lookup.
struct TimedGets {
    mean: f64,
    spread: f64,
    p50: f64,
    p99: f64,
}

/// Mean µs to the first record and for the whole scan, over `prefixes`,
/// median of three rounds.
fn time_scans<L: Layout>(
    layout: &L,
    workspace: &mut Workspace,
    prefixes: &[Vec<u8>],
) -> (f64, f64, f64) {
    let mut firsts = Vec::new();
    let mut totals = Vec::new();
    for _ in 0..3 {
        let (mut first_ns, mut total_ns) = (0u128, 0u128);
        for prefix in prefixes {
            let start = Instant::now();
            let mut first = None;
            layout.scan(prefix, workspace, |record| {
                first.get_or_insert_with(|| start.elapsed());
                black_box(record);
            });
            total_ns += start.elapsed().as_nanos();
            first_ns += first.unwrap().as_nanos();
        }
        firsts.push(first_ns as f64 / prefixes.len() as f64 / 1e3);
        totals.push(total_ns as f64 / prefixes.len() as f64 / 1e3);
    }
    let spread = spread(&totals);
    (median(firsts), median(totals), spread)
}

struct Dataset {
    name: &'static str,
    records: Vec<Owned>,
}

/// The datasets, each built only when reached, so one is held at a time.
fn datasets() -> impl Iterator<Item = Dataset> {
    type Payload = fn(u64, u64) -> Vec<u8>;
    let specs: [(&'static str, u64, Payload); 3] = [
        ("sensor, 4.19M × 8 + 14–32 B", 256, compare::sensor),
        ("JSON, 1.05M × 8 + ~545 B", 64, compare::json),
        ("documents, 65.5k × 8 + 1–6 KiB", 4, |index, version| {
            document(index + (version << 32))
        }),
    ];
    specs.into_iter().map(|(name, groups, value)| Dataset {
        name,
        records: compare::history(groups, value)
            .map(|(key, seq, value)| (key.to_vec(), seq, value))
            .collect(),
    })
}

/// Point lookups, prefix scans and full scans of each layout over the
/// comparison harness's data with its tombstones (one record in 16 deleted,
/// one in 16 at a later version), each layout first checked against the
/// records it was built from.
#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn layout_tradeoffs() {
    const PROBES: usize = 30_000;
    let mut specs = vec![
        Spec::Run(Codec::Zstd, 16 << 10),
        Spec::Run(Codec::Zstd, 4 << 10),
        Spec::Run(Codec::Zstd, 2 << 10),
        Spec::Run(Codec::Lz4, 16 << 10),
        Spec::Run(Codec::Lz4, 4 << 10),
        Spec::Blocks(Pack::Zstd, 4 << 10, None),
        Spec::Blocks(Pack::Zstd, 4 << 10, Some(64 << 10)),
        Spec::Blocks(Pack::Zstd, 2 << 10, Some(64 << 10)),
        Spec::Split(Pack::Zstd, 4 << 10, Some(64 << 10)),
        Spec::Split(Pack::Zstd, 1 << 10, Some(64 << 10)),
        Spec::Split(Pack::Lz4, 4 << 10, None),
    ];
    // A second pass in reverse order, against effects of position.
    if std::env::var_os("LAYOUTS_REVERSED").is_some() {
        specs.reverse();
    }
    let mib = |bytes: usize| bytes as f64 / f64::from(1 << 20);
    println!(
        "\n| dataset | layout | total MiB | B/record | payload | keys | index | framing | dictionary | filter | build s (train) | build peak MiB | present ns (±%) | p99 µs | deleted ns | missing ns | seek µs, 32 keys | scan µs, 32 keys (±%) | scan µs, 512 keys | full scan ns/record | reader KiB |"
    );
    println!("|{}", "---|".repeat(21));
    for dataset in datasets() {
        let records = &dataset.records;
        let count = records.len() as u64;
        let mut rng = Rng::new(5);
        let pick = |rng: &mut Rng, wanted: fn(u64) -> bool| loop {
            let index = rng.below(count);
            if wanted(index) {
                break index as usize;
            }
        };
        let present: Vec<Vec<u8>> = (0..PROBES)
            .map(|_| records[pick(&mut rng, |i| i % 16 != 8)].0.clone())
            .collect();
        let deleted: Vec<Vec<u8>> = (0..PROBES)
            .map(|_| records[pick(&mut rng, |i| i % 16 == 8)].0.clone())
            .collect();
        let missing: Vec<Vec<u8>> = (0..PROBES)
            .map(|_| {
                let mut key = records[pick(&mut rng, |_| true)].0.clone();
                key[7] = 0x7F;
                key
            })
            .collect();
        let short: Vec<Vec<u8>> = (0..2_000)
            .map(|_| records[pick(&mut rng, |_| true)].0[..6].to_vec())
            .collect();
        let medium: Vec<Vec<u8>> = (0..200)
            .map(|_| records[pick(&mut rng, |_| true)].0[..4].to_vec())
            .collect();
        let probes: Vec<Vec<u8>> = [&present, &deleted, &missing]
            .iter()
            .flat_map(|keys| keys[..200].iter().cloned())
            .collect();
        let check_prefixes = &short[..20];

        for &spec in &specs {
            let rss = status_kib("VmRSS:");
            let _ = std::fs::write("/proc/self/clear_refs", "5");
            let start = Instant::now();
            let (built, trained) = spec.build(records);
            let build_s = start.elapsed().as_secs_f64();
            let peak = status_kib("VmHWM:").saturating_sub(rss);
            let row = with_layout!(&built, layout => {
                // Exhaustive against the records, which are in key order.
                let mut workspace = Workspace::new();
                let mut at = 0;
                layout.scan(&[], &mut workspace, |record| {
                    let (key, seq, value) = &records[at];
                    assert_eq!((record.key, record.seq, record.value), (key.as_slice(), *seq, value_of(value)));
                    at += 1;
                });
                assert_eq!(at, records.len());
                spot_check(layout, records, &probes, check_prefixes);

                let TimedGets { mean: present_ns, spread: present_spread, p99, .. } =
                    time_gets(layout, &mut workspace, &present);
                let deleted_ns = time_gets(layout, &mut workspace, &deleted).mean;
                let missing_ns = time_gets(layout, &mut workspace, &missing).mean;
                let (seek, short_us, short_spread) = time_scans(layout, &mut workspace, &short);
                let (_, medium_us, _) = time_scans(layout, &mut workspace, &medium);
                let full: Vec<f64> = (0..2)
                    .map(|_| {
                        let start = Instant::now();
                        let mut seen = 0usize;
                        layout.scan(&[], &mut workspace, |record| {
                            black_box(record);
                            seen += 1;
                        });
                        start.elapsed().as_nanos() as f64 / seen as f64
                    })
                    .collect();
                let bytes = layout.bytes();
                format!(
                    "| {:.2} | {:.2} | {:.2} | {:.2} | {:.3} | {:.3} | {:.3} | {:.2} | {build_s:.2} ({trained:.2}) | {:.0} | {present_ns:.0} (±{present_spread:.0}) | {:.2} | {deleted_ns:.0} | {missing_ns:.0} | {seek:.2} | {short_us:.2} (±{short_spread:.0}) | {medium_us:.1} | {:.1} | {:.0} |",
                    mib(bytes.total()),
                    bytes.total() as f64 / records.len() as f64,
                    mib(bytes.payload),
                    mib(bytes.keys),
                    mib(bytes.index),
                    mib(bytes.framing),
                    mib(bytes.dictionary),
                    mib(bytes.filter),
                    peak as f64 / 1024.0,
                    p99 / 1e3,
                    median(full),
                    layout.workspace_bytes(&workspace) as f64 / 1024.0,
                )
            });
            println!("| {} | {} {row}", dataset.name, spec.name());
            drop(built);
        }
    }
}

/// Newest versions of the probes and the records of each prefix, found in
/// the key-ordered records by position: record `i` holds key `i`.
fn spot_check<L: Layout>(layout: &L, records: &[Owned], probes: &[Vec<u8>], prefixes: &[Vec<u8>]) {
    let mut workspace = Workspace::new();
    for key in probes {
        let expected = records
            .binary_search_by(|record| record.0.as_slice().cmp(key))
            .ok()
            .map(|at| (records[at].1, records[at].2.clone()));
        let got = layout.get(key, &mut workspace, |version| {
            version.map(|version| {
                let value = match version.value {
                    Value::Bytes(bytes) => Some(bytes.to_vec()),
                    Value::Tombstone => None,
                };
                (version.seq, value)
            })
        });
        assert_eq!(got, expected);
    }
    for prefix in prefixes {
        let start = records.partition_point(|record| record.0.as_slice() < prefix.as_slice());
        let mut at = start;
        layout.scan(prefix, &mut workspace, |record| {
            let (key, seq, value) = &records[at];
            assert_eq!(
                (record.key, record.seq, record.value),
                (key.as_slice(), *seq, value_of(value))
            );
            at += 1;
        });
        assert!(at > start && !records.get(at).is_some_and(|r| r.0.starts_with(prefix)));
    }
}

// 5AD: where a block's decode time goes, and dictionary runs trained the way
// a compaction could train them.

/// The 5AD datasets, smaller than the 5AB ones: each holds at least 64 MiB
/// of input, which is all the 5AD measurements read.
fn research_datasets() -> impl Iterator<Item = Dataset> {
    type Payload = fn(u64, u64) -> Vec<u8>;
    let specs: [(&'static str, u64, Payload); 3] = [
        ("sensor", 128, compare::sensor),
        ("JSON", 16, compare::json),
        ("documents", 4, |index, version| {
            document(index + (version << 32))
        }),
    ];
    specs.into_iter().map(|(name, groups, value)| Dataset {
        name,
        records: compare::history(groups, value)
            .map(|(key, seq, value)| (key.to_vec(), seq, value))
            .collect(),
    })
}

/// The first records of `records` holding about `bytes` of input, counted
/// as a flush or compaction counts it (key, value and 16 bytes a record).
fn first_bytes(records: &[Owned], bytes: usize) -> &[Owned] {
    let mut total = 0;
    let count = records
        .iter()
        .take_while(|(key, _, value)| {
            let more = total < bytes;
            total += key.len() + value.as_ref().map_or(0, Vec::len) + 16;
            more
        })
        .count();
    &records[..count]
}

/// Mean ns per item of `f` over `items`, the median of five rounds.
fn ns_each<T>(items: &[T], mut f: impl FnMut(&T)) -> f64 {
    let rounds = (0..5)
        .map(|_| {
            let start = Instant::now();
            items.iter().for_each(&mut f);
            start.elapsed().as_nanos() as f64 / items.len() as f64
        })
        .collect();
    median(rounds)
}

impl Blocks {
    /// The stored bytes of frame `at`, after its header.
    fn stored(&self, at: usize) -> &[u8] {
        &self.frames[self.offsets[at] as usize + HEADER..self.offsets[at + 1] as usize]
    }

    /// About `count` frames, spread evenly over the run.
    fn picks(&self, count: usize) -> Vec<usize> {
        (0..self.index.len())
            .step_by((self.index.len() / count).max(1))
            .collect()
    }
}

/// Training samples taken the way a compaction could take them before it
/// merges: whole decoded blocks of its input runs, spread evenly across them,
/// about `budget` bytes in all.
fn input_samples(inputs: &[Blocks], budget: usize) -> (Vec<u8>, Vec<usize>) {
    let blocks: usize = inputs.iter().map(|input| input.index.len()).sum();
    // Each frame header records its decoded length.
    let decoded: usize = inputs
        .iter()
        .flat_map(|input| {
            input.offsets[..input.index.len()].iter().map(|&offset| {
                let at = offset as usize + 4;
                u32::from_le_bytes(input.frames[at..at + 4].try_into().unwrap()) as usize
            })
        })
        .sum::<usize>()
        .max(1);
    let step = (decoded / budget.max(1)).max(1).min(blocks.max(1));
    let (mut dctx, mut buf) = (DCtx::create(), Vec::new());
    let (mut samples, mut sizes) = (Vec::new(), Vec::new());
    let mut at = 0;
    for input in inputs {
        while at < input.index.len() {
            let frame = &input.frames[input.offsets[at] as usize..input.offsets[at + 1] as usize];
            let block = unframe(Pack::Zstd, None, frame, &mut dctx, &mut buf);
            samples.extend_from_slice(block);
            sizes.push(block.len());
            at += step;
        }
        at -= input.index.len();
    }
    (samples, sizes)
}

/// Raw blocks of the output itself, every `every`-th, cut as the builder
/// cuts them: what a second pass over the merge could sample.
fn output_samples(records: &[Owned], target: usize, every: usize) -> (Vec<u8>, Vec<usize>) {
    let (mut block, mut raw) = (BlockBuilder::new(), Vec::new());
    let (mut samples, mut sizes, mut count) = (Vec::new(), Vec::new(), 0);
    let mut cut = |block: &mut BlockBuilder| {
        raw.clear();
        block.flush_into(&mut raw);
        if count % every == 0 {
            samples.extend_from_slice(&raw);
            sizes.push(raw.len());
        }
        count += 1;
    };
    for (key, seq, value) in records {
        if ends_block(&block, key, value, target) {
            cut(&mut block);
        }
        block.add(key, *seq, value_of(value)).unwrap();
    }
    if !block.is_empty() {
        cut(&mut block);
    }
    (samples, sizes)
}

fn train_on(size: usize, samples: &[u8], sizes: &[usize]) -> Vec<u8> {
    let mut dictionary = Vec::with_capacity(size);
    zstd_safe::train_from_buffer(&mut dictionary, samples, sizes).unwrap();
    dictionary
}

/// Resident memory added while `f` runs, above the level before it: a lower
/// bound, since freed heap is reused first.
fn peak_during<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let rss = status_kib("VmRSS:");
    let _ = std::fs::write("/proc/self/clear_refs", "5");
    let out = f();
    let peak = status_kib("VmHWM:").saturating_sub(rss);
    (out, peak as f64 / 1024.0)
}

/// Where a Zstandard block's decode time goes, by block length: each step of
/// the production read path timed alone on the same frames, with LZ4 and a
/// plain copy for reference, and at 4 KiB the dictionary paths.
#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn decode_costs() {
    println!(
        "\n| dataset | block | records/block | decoded B | stored B | CRC ns | header ns | decompress ns | fresh context ns | copy ns | open ns | walk ns | lookup in block ns | production get ns | LZ4 decompress ns | compress ns |"
    );
    println!("|{}", "---|".repeat(16));
    let mut dictionary_rows = Vec::new();
    for dataset in research_datasets() {
        let records = first_bytes(&dataset.records, 64 << 20);
        for target in [1 << 10, 2 << 10, 4 << 10, 8 << 10, 16 << 10] {
            let blocks = Blocks::build(records, Pack::Zstd, target, None);
            let lz4 = Blocks::build(records, Pack::Lz4, target, None);
            let picks = blocks.picks(2_000);
            let (mut dctx, mut buf) = (DCtx::create(), Vec::with_capacity(4 * target + (64 << 10)));
            let decoded: Vec<Vec<u8>> = picks
                .iter()
                .map(|&at| {
                    buf.clear();
                    dctx.decompress(&mut buf, blocks.stored(at)).unwrap();
                    buf.clone()
                })
                .collect();
            let keys: Vec<&[u8]> = picks.iter().map(|&at| blocks.index.key(at)).collect();
            let at: Vec<usize> = (0..picks.len()).collect();
            let mean = |f: &dyn Fn(usize) -> usize| {
                at.iter().map(|&i| f(i)).sum::<usize>() as f64 / at.len() as f64
            };
            let records_per_block = mean(&|i| {
                let view = BlockView::open(&decoded[i]).unwrap();
                let (mut position, mut count) = (BlockPosition::new(), 0);
                while position.next_record(view).unwrap().is_some() {
                    count += 1;
                }
                count
            });

            let crc = ns_each(&picks, |&at| {
                black_box(crc32fast::hash(blocks.stored(at)));
            });
            let header = ns_each(&picks, |&at| {
                black_box(zstd_safe::get_frame_content_size(blocks.stored(at)).unwrap());
            });
            let decompress = ns_each(&picks, |&at| {
                buf.clear();
                dctx.decompress(&mut buf, blocks.stored(at)).unwrap();
            });
            let fresh = ns_each(&picks, |&at| {
                let mut dctx = DCtx::create();
                buf.clear();
                dctx.decompress(&mut buf, blocks.stored(at)).unwrap();
            });
            let copy = ns_each(&at, |&i| {
                buf.clear();
                buf.extend_from_slice(&decoded[i]);
                black_box(&buf);
            });
            let open = ns_each(&at, |&i| {
                black_box(BlockView::open(&decoded[i]).unwrap());
            });
            let walk = ns_each(&at, |&i| {
                let view = BlockView::open(&decoded[i]).unwrap();
                let mut position = BlockPosition::new();
                while let Some(record) = position.next_record(view).unwrap() {
                    black_box(record);
                }
            });
            let lookup = ns_each(&at, |&i| {
                let view = BlockView::open(&decoded[i]).unwrap();
                black_box(view.get(keys[i]).unwrap());
            });
            let mut builder = RunBuilder::with_target_block_len(Codec::Zstd, target);
            for (key, seq, value) in records {
                builder.add(key, *seq, value_of(value)).unwrap();
            }
            let run = builder.finish();
            let mut read = ReadWorkspace::new();
            let production = ns_each(&keys, |key| {
                black_box(Run::get(&run, key, &mut read).unwrap());
            });
            let lz4_decode = ns_each(&lz4.picks(2_000), |&at| {
                let frame = &lz4.frames[lz4.offsets[at] as usize..lz4.offsets[at + 1] as usize];
                let len = u32::from_le_bytes(frame[4..HEADER].try_into().unwrap()) as usize;
                buf.resize(len, 0);
                lz4_flex::block::decompress_into(&frame[HEADER..], &mut buf).unwrap();
            });
            let mut cctx = CCtx::create();
            let mut out = Vec::with_capacity(zstd_safe::compress_bound(4 * target + (64 << 10)));
            let compress = ns_each(&at, |&i| {
                out.clear();
                cctx.compress(&mut out, &decoded[i], LEVEL).unwrap();
            });
            println!(
                "| {} | {} KiB | {records_per_block:.0} | {:.0} | {:.0} | {crc:.0} | {header:.0} | {decompress:.0} | {fresh:.0} | {copy:.0} | {open:.0} | {walk:.0} | {lookup:.0} | {production:.0} | {lz4_decode:.0} | {compress:.0} |",
                dataset.name,
                target >> 10,
                mean(&|i| decoded[i].len()),
                mean(&|i| blocks.stored(picks[i]).len()),
            );

            if target == 4 << 10 {
                // A dictionary trained on the inputs a compaction would merge.
                let inputs: Vec<Blocks> = (0..4)
                    .map(|part| {
                        let part: Vec<Owned> =
                            records.iter().skip(part).step_by(4).cloned().collect();
                        Blocks::build(&part, Pack::Zstd, 16 << 10, None)
                    })
                    .collect();
                let (samples, sizes) = input_samples(&inputs, 100 * (64 << 10));
                let dictionary = train_on(64 << 10, &samples, &sizes);
                let with = Blocks::build(records, Pack::Zstd, target, Some(&dictionary));
                let ddict = with.ddict.as_ref().unwrap();
                let by_reference = DDict::create_by_reference(&dictionary);
                let picks = with.picks(2_000);
                let using_ddict = ns_each(&picks, |&at| {
                    buf.clear();
                    dctx.decompress_using_ddict(&mut buf, with.stored(at), ddict)
                        .unwrap();
                });
                let using_reference = ns_each(&picks, |&at| {
                    buf.clear();
                    dctx.decompress_using_ddict(&mut buf, with.stored(at), &by_reference)
                        .unwrap();
                });
                let using_raw = ns_each(&picks, |&at| {
                    buf.clear();
                    dctx.decompress_using_dict(&mut buf, with.stored(at), &dictionary)
                        .unwrap();
                });
                let prepare = ns_each(&[(); 20], |()| {
                    black_box(DDict::create(&dictionary));
                });
                let prepare_reference = ns_each(&[(); 20], |()| {
                    black_box(DDict::create_by_reference(&dictionary));
                });
                let cdict = CDict::create(&dictionary, LEVEL);
                let compress_dict = ns_each(&at, |&i| {
                    out.clear();
                    cctx.compress_using_cdict(&mut out, &decoded[i], &cdict)
                        .unwrap();
                });
                let stored = picks.iter().map(|&at| with.stored(at).len()).sum::<usize>() as f64
                    / picks.len() as f64;
                dictionary_rows.push(format!(
                    "| {} | {} | {stored:.0} | {decompress:.0} | {using_ddict:.0} | {using_reference:.0} | {using_raw:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {compress_dict:.0} | {} | {} |",
                    dataset.name,
                    dictionary.len(),
                    prepare / 1e3,
                    prepare_reference / 1e3,
                    ddict.sizeof() as f64 / 1024.0,
                    by_reference.sizeof() as f64 / 1024.0,
                    compress,
                    dctx.sizeof(),
                    cdict.sizeof(),
                ));
            }
        }
    }
    println!(
        "\n| dataset (4 KiB blocks) | dictionary B | stored B | plain decompress ns | with DDict ns | with DDict by reference ns | with raw dictionary ns | prepare DDict µs | prepare by reference µs | DDict KiB | by reference KiB | plain compress ns | compress with CDict ns | DCtx B | CDict B |"
    );
    println!("|{}", "---|".repeat(15));
    for row in dictionary_rows {
        println!("{row}");
    }
}

/// Dictionary runs at 4 KiB blocks beside production runs of the same
/// records, at three run sizes, with the dictionary trained three ways:
/// on decoded blocks of the inputs a compaction would merge (one pass), on
/// the output's own raw blocks (a second pass), and on values (5AB).
/// Memory counts everything a persisted dictionary run would hold: blocks,
/// headers, index, filter, the raw dictionary stored in the run and its
/// decoding form.
#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn dictionary_runs() {
    const DICTIONARY: usize = 64 << 10;
    const BUDGET: usize = 100 * DICTIONARY;
    println!(
        "\n| dataset | input MiB | layout | total MiB | vs 16 KiB | blocks | samples MiB | sample s | train s | train peak MiB | build s | build MB/s | build peak MiB | estimated saving | present p50 µs | p99 µs | seek µs | 32 keys µs | 512 keys µs | full scan ns/record |"
    );
    println!("|{}", "---|".repeat(20));
    let mib = |bytes: usize| bytes as f64 / f64::from(1 << 20);
    for dataset in research_datasets() {
        for input_mib in [4, 16, 64] {
            let records = first_bytes(&dataset.records, input_mib << 20);
            let input_bytes: usize = records
                .iter()
                .map(|(key, _, value)| key.len() + value.as_ref().map_or(0, Vec::len) + 16)
                .sum();
            let reads = input_mib == 64;
            let mut rng = Rng::new(7);
            let count = records.len() as u64;
            let present: Vec<Vec<u8>> = std::iter::repeat_with(|| rng.below(count) as usize)
                .filter(|&at| records[at].2.is_some())
                .take(30_000)
                .map(|at| records[at].0.clone())
                .collect();
            let short: Vec<Vec<u8>> = (0..2_000)
                .map(|_| records[rng.below(count) as usize].0[..6].to_vec())
                .collect();
            let medium: Vec<Vec<u8>> = (0..200)
                .map(|_| records[rng.below(count) as usize].0[..4].to_vec())
                .collect();
            // The inputs a compaction would merge: four overlapping runs.
            let inputs: Vec<Blocks> = (0..4)
                .map(|part| {
                    let part: Vec<Owned> = records.iter().skip(part).step_by(4).cloned().collect();
                    Blocks::build(&part, Pack::Zstd, 16 << 10, None)
                })
                .collect();

            let mut base = None;
            let mut plain_payload = None;
            enum Source {
                Inputs(usize),
                Output,
                Values,
            }
            let layouts: Vec<(String, Option<Source>, Spec)> = vec![
                (
                    "production, Zstd 16 KiB".into(),
                    None,
                    Spec::Run(Codec::Zstd, 16 << 10),
                ),
                (
                    "production, Zstd 4 KiB".into(),
                    None,
                    Spec::Run(Codec::Zstd, 4 << 10),
                ),
                (
                    "production, LZ4 4 KiB".into(),
                    None,
                    Spec::Run(Codec::Lz4, 4 << 10),
                ),
                (
                    "4 KiB + 64 KiB dict from inputs".into(),
                    Some(Source::Inputs(DICTIONARY)),
                    Spec::Blocks(Pack::Zstd, 4 << 10, None),
                ),
                (
                    "4 KiB + 16 KiB dict from inputs".into(),
                    Some(Source::Inputs(16 << 10)),
                    Spec::Blocks(Pack::Zstd, 4 << 10, None),
                ),
                (
                    "4 KiB + 64 KiB dict from output blocks".into(),
                    Some(Source::Output),
                    Spec::Blocks(Pack::Zstd, 4 << 10, None),
                ),
                (
                    "4 KiB + 64 KiB dict from values".into(),
                    Some(Source::Values),
                    Spec::Blocks(Pack::Zstd, 4 << 10, None),
                ),
            ];
            for (name, source, spec) in layouts {
                if matches!(source, Some(Source::Output | Source::Values)) && !reads {
                    continue;
                }
                // Samples and training, timed apart from the build.
                let start = Instant::now();
                let ((dictionary, samples_len, sample_s, train_s, estimate), train_peak) =
                    peak_during(|| match &source {
                        None => (None, 0, 0.0, 0.0, None),
                        Some(source) => {
                            let size = match source {
                                Source::Inputs(size) => *size,
                                _ => DICTIONARY,
                            };
                            let (samples, sizes) = match source {
                                Source::Inputs(_) => input_samples(&inputs, 100 * size),
                                Source::Output => {
                                    let blocks = input_bytes.div_ceil(4 << 10);
                                    output_samples(
                                        records,
                                        4 << 10,
                                        (blocks * 4096 / BUDGET).max(1),
                                    )
                                }
                                Source::Values => {
                                    let (mut samples, mut sizes) = (Vec::new(), Vec::new());
                                    for value in sampled(records, BUDGET, |(key, _, value)| {
                                        value.as_deref().or(Some(key.as_slice()))
                                    }) {
                                        samples.extend_from_slice(value);
                                        sizes.push(value.len());
                                    }
                                    (samples, sizes)
                                }
                            };
                            let sample_s = start.elapsed().as_secs_f64();
                            // Held out: every eighth sample, in 4 KiB pieces.
                            let (mut train, mut train_sizes, mut held) =
                                (Vec::new(), Vec::new(), Vec::new());
                            let mut at = 0;
                            for (i, &len) in sizes.iter().enumerate() {
                                let sample = &samples[at..at + len];
                                at += len;
                                if i % 8 == 7 {
                                    held.extend(sample.chunks(4 << 10).map(<[u8]>::to_vec));
                                } else {
                                    train.extend_from_slice(sample);
                                    train_sizes.push(len);
                                }
                            }
                            let trained = Instant::now();
                            let full = train_on(size, &samples, &sizes);
                            let train_s = trained.elapsed().as_secs_f64();
                            let trial = train_on(size, &train, &train_sizes);
                            // A builder would train once; the trial dictionary
                            // only estimates the saving on unseen blocks.
                            let cdict = CDict::create(&trial, LEVEL);
                            let mut cctx = CCtx::create();
                            let (mut plain, mut with) = (0, 0);
                            let mut out = Vec::new();
                            for piece in &held {
                                out.clear();
                                out.reserve(zstd_safe::compress_bound(piece.len()));
                                plain += cctx.compress(&mut out, piece, LEVEL).unwrap();
                                out.clear();
                                with += cctx.compress_using_cdict(&mut out, piece, &cdict).unwrap();
                            }
                            let estimate = (plain > 0).then(|| 1.0 - with as f64 / plain as f64);
                            (Some(full), samples.len(), sample_s, train_s, estimate)
                        }
                    });
                let start = Instant::now();
                let (built, build_peak) = peak_during(|| match spec {
                    Spec::Blocks(pack, target, _) => {
                        Built::Blocks(Blocks::build(records, pack, target, dictionary.as_deref()))
                    }
                    spec => spec.build(records).0,
                });
                let build_s = start.elapsed().as_secs_f64();
                let row = with_layout!(&built, layout => {
                    let bytes = layout.bytes();
                    let total = bytes.total() + dictionary.as_ref().map_or(0, Vec::len);
                    let base = *base.get_or_insert(total);
                    let blocks = match &built {
                        Built::Run(run) => run.block_count(),
                        Built::Blocks(blocks) => blocks.index.len(),
                        Built::Split(split) => split.index.len(),
                    };
                    if name == "production, Zstd 4 KiB" {
                        plain_payload = Some(bytes.payload);
                    }
                    let actual = match (&dictionary, plain_payload) {
                        (Some(_), Some(plain)) => {
                            format!(" (actual {:.0}%)", 100.0 * (1.0 - bytes.payload as f64 / plain as f64))
                        }
                        _ => String::new(),
                    };
                    let estimate = estimate.map_or("–".into(), |e| format!("{:.0}%{actual}", 100.0 * e));
                    let read_cells = if reads {
                        spot_check(layout, records, &present[..200], &short[..20]);
                        let mut workspace = Workspace::new();
                        let gets = time_gets(layout, &mut workspace, &present);
                        let (seek, short_us, _) = time_scans(layout, &mut workspace, &short);
                        let (_, medium_us, _) = time_scans(layout, &mut workspace, &medium);
                        let full: Vec<f64> = (0..2)
                            .map(|_| {
                                let start = Instant::now();
                                let mut seen = 0usize;
                                layout.scan(&[], &mut workspace, |record| {
                                    black_box(record);
                                    seen += 1;
                                });
                                assert_eq!(seen, records.len());
                                start.elapsed().as_nanos() as f64 / seen as f64
                            })
                            .collect();
                        format!(
                            "{:.2} | {:.2} | {seek:.2} | {short_us:.2} | {medium_us:.1} | {:.0}",
                            gets.p50 / 1e3,
                            gets.p99 / 1e3,
                            median(full)
                        )
                    } else {
                        "– | – | – | – | – | –".into()
                    };
                    format!(
                        "{:.2} | {:+.1}% | {blocks} | {:.1} | {sample_s:.3} | {train_s:.2} | {train_peak:.0} | {build_s:.2} | {:.0} | {build_peak:.0} | {estimate} | {read_cells}",
                        mib(total),
                        100.0 * (total as f64 / base as f64 - 1.0),
                        mib(samples_len),
                        input_bytes as f64 / 1e6 / build_s,
                    )
                });
                println!("| {} | {input_mib} | {name} | {row} |", dataset.name);
            }
        }
    }
}
