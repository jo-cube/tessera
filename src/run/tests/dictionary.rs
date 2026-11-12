//! Runs that store a dictionary: the version 4 format, its validation, and
//! compaction's choice of a dictionary. Engine contracts (recovery,
//! snapshots, mixed settings) are tested in `tests/checkpoint.rs`.

use std::sync::Arc;

use crate::block::Value;
use crate::run::codec::Codec;
use crate::run::dictionary::{self, DICTIONARY_WORKSPACE, MAX_DICTIONARY_LEN, Trained};
use crate::run::{
    CompactLimits, Compacted, CorruptRun, DictionaryChoice, MergeWorkspace, ReadWorkspace, Run,
    RunBuilder, RunCorruption, RunView, compact,
};

use super::costs::compare;

type Owned = (Vec<u8>, u64, Option<Vec<u8>>);

const FOOTER_V3: usize = 32;
const FOOTER_V4: usize = 40;
const SMALL_BLOCK: usize = 4 << 10;

/// `count` records of JSON values from the comparison dataset, one in
/// sixteen a tombstone, as a run of an overlapping history holds them.
fn json_records(groups: u64) -> Vec<Owned> {
    compare::history(groups, compare::json)
        .map(|(key, seq, value)| (key.to_vec(), seq, value))
        .collect()
}

fn sensor_records(groups: u64) -> Vec<Owned> {
    compare::history(groups, compare::sensor)
        .map(|(key, seq, value)| (key.to_vec(), seq, value))
        .collect()
}

fn add_all(builder: &mut RunBuilder, records: &[Owned]) {
    for (key, seq, value) in records {
        let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
        builder.add(key, *seq, value).unwrap();
    }
}

fn plain(records: &[Owned], target: usize) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_target_block_len(Codec::Zstd, target);
    add_all(&mut builder, records);
    builder.finish()
}

fn with_dictionary(records: &[Owned], dictionary: &Arc<Trained>) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_dictionary(Arc::clone(dictionary), SMALL_BLOCK);
    add_all(&mut builder, records);
    builder.finish()
}

/// Trains on every decoded block of `run`, as compaction trains on samples.
fn train_on(run: &Run<Vec<u8>>) -> Arc<Trained> {
    let view = run.view();
    let mut workspace = ReadWorkspace::new();
    let (mut samples, mut sizes) = (Vec::new(), Vec::new());
    for block in 0..view.block_count() {
        let frame = view.frame(block).unwrap();
        let decoded = workspace.load(&frame, view.dictionary()).unwrap();
        samples.extend_from_slice(decoded);
        sizes.push(decoded.len());
    }
    Arc::new(dictionary::train(&samples, &sizes).expect("enough samples to train"))
}

fn walk(view: RunView<'_>) -> Result<Vec<Owned>, CorruptRun> {
    let mut workspace = ReadWorkspace::new();
    let mut cursor = view.cursor(&mut workspace);
    let mut out = Vec::new();
    while let Some(record) = cursor.next_record()? {
        let value = match record.value {
            Value::Bytes(bytes) => Some(bytes.to_vec()),
            Value::Tombstone => None,
        };
        out.push((record.key.to_vec(), record.seq, value));
        assert!(out.len() <= 1 << 20, "cursor did not terminate");
    }
    Ok(out)
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}

/// A version 4 run's regions, read without the crate: frames, dictionary,
/// index (table and keys), footer.
struct Regions<'a> {
    frames: &'a [u8],
    dictionary: &'a [u8],
    index: &'a [u8],
    /// The footer fields both versions share.
    shared: &'a [u8],
}

fn regions(bytes: &[u8]) -> Regions<'_> {
    let footer = bytes.len() - FOOTER_V4;
    assert_eq!(u32_at(bytes, footer + 32), 4, "a version 4 run");
    let index_offset = u64_at(bytes, footer) as usize;
    let dict_len = u32_at(bytes, footer + 24) as usize;
    Regions {
        frames: &bytes[..index_offset - dict_len],
        dictionary: &bytes[index_offset - dict_len..index_offset],
        index: &bytes[index_offset..footer],
        shared: &bytes[footer..footer + 24],
    }
}

/// Reassembles a version 4 run around another dictionary, with a correct
/// footer and checksum for it.
fn with_stored_dictionary(bytes: &[u8], dictionary: &[u8]) -> Vec<u8> {
    let r = regions(bytes);
    let mut out = [r.frames, dictionary, r.index].concat();
    let index_offset = (r.frames.len() + dictionary.len()) as u64;
    out.extend_from_slice(&index_offset.to_le_bytes());
    out.extend_from_slice(&r.shared[8..24]);
    out.extend_from_slice(&(dictionary.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(dictionary).to_le_bytes());
    out.extend_from_slice(&4u32.to_le_bytes());
    out.extend_from_slice(&bytes[bytes.len() - 4..]);
    out
}

/// The same frames as a version 3 run, which stores no dictionary.
fn without_dictionary(bytes: &[u8]) -> Vec<u8> {
    let r = regions(bytes);
    let mut out = [r.frames, r.index].concat();
    out.extend_from_slice(&(r.frames.len() as u64).to_le_bytes());
    out.extend_from_slice(&r.shared[8..24]);
    out.extend_from_slice(&3u32.to_le_bytes());
    out.extend_from_slice(&bytes[bytes.len() - 4..]);
    out
}

fn rejection(bytes: Vec<u8>) -> CorruptRun {
    Run::new(bytes).expect_err("damaged run accepted")
}

fn fixture() -> (Vec<Owned>, Arc<Trained>) {
    let records: Vec<Owned> = json_records(1).into_iter().step_by(16).collect();
    let dictionary = train_on(&plain(&records, 1 << 10));
    (records, dictionary)
}

/// A dictionary run reads exactly as a plain run of the same records:
/// lookups of every key, including tombstones and absent keys, prefix scans
/// and full iteration, through the built run, a validated copy and a
/// recovered one.
#[test]
fn dictionary_runs_read_like_plain_runs() {
    let (records, dictionary) = fixture();
    let reference = plain(&records, SMALL_BLOCK);
    let built = with_dictionary(&records, &dictionary);
    assert!(built.has_dictionary() && !reference.has_dictionary());
    let validated = Run::new(built.as_bytes().to_vec()).unwrap();
    let (recovered, max_seq) = Run::recover(built.as_bytes().to_vec()).unwrap();
    assert_eq!(max_seq, reference.max_seq());
    assert!(recovered.filter_bytes() > 0);

    let expected = walk(reference.view()).unwrap();
    for run in [&built, &validated, &recovered] {
        assert!(run.dictionary_bytes() > dictionary.bytes().len());
        assert_eq!(
            run.held_bytes(),
            run.filter_bytes() + run.dictionary_bytes()
        );
        assert_eq!(walk(run.view()).unwrap(), expected);
        let mut workspace = ReadWorkspace::new();
        for (key, seq, value) in records.iter().step_by(7) {
            let found = run.get(key, &mut workspace).unwrap().unwrap();
            let found_value = match found.value {
                Value::Bytes(bytes) => Some(bytes.to_vec()),
                Value::Tombstone => None,
            };
            assert_eq!((found.seq, found_value), (*seq, value.clone()));
        }
        let mut absent = records[100].0.clone();
        absent.push(0);
        assert!(run.get(&absent, &mut workspace).unwrap().is_none());

        let prefix = &records[300].0[..3];
        let mut scan = run.cursor(&mut workspace);
        scan.seek_prefix(prefix).unwrap();
        let mut count = 0;
        while let Some(record) = scan.next_record().unwrap() {
            assert!(record.key.starts_with(prefix));
            count += 1;
        }
        let in_prefix = records.iter().filter(|r| r.0.starts_with(prefix)).count();
        assert_eq!(count, in_prefix);
    }
}

/// Every frame names the dictionary, decodes on its own in any order, and
/// the run is written in version 4; plain runs stay in version 3.
#[test]
fn dictionary_frames_are_independent_and_marked() {
    let (records, dictionary) = fixture();
    let run = with_dictionary(&records, &dictionary);
    let bytes = run.as_bytes();
    assert_eq!(u32_at(bytes, bytes.len() - 8), 4);
    let plain_bytes = plain(&records, SMALL_BLOCK);
    let plain_bytes = plain_bytes.as_bytes();
    assert_eq!(u32_at(plain_bytes, plain_bytes.len() - 8), 3);

    let r = regions(bytes);
    assert_eq!(r.dictionary, dictionary.bytes());
    let id = zstd_safe::get_dict_id_from_dict(r.dictionary).unwrap();
    let view = run.view();
    assert!(view.block_count() > 8);
    for block in (0..view.block_count()).rev() {
        let frame = view.frame(block).unwrap();
        assert_eq!(frame.codec, Codec::ZstdDictionary);
        assert_eq!(zstd_safe::get_dict_id_from_frame(frame.stored), Some(id));
        // A fresh workspace per block: nothing carries over between frames.
        let mut workspace = ReadWorkspace::new();
        let decoded = workspace.load(&frame, view.dictionary()).unwrap();
        assert_eq!(decoded.len(), frame.decoded_len);
    }
    // Construction is deterministic for the same records and dictionary.
    assert_eq!(with_dictionary(&records, &dictionary).as_bytes(), bytes);
}

/// Damage to the dictionary or its footer fields is reported before any
/// frame is read, with the kind and offset of what is wrong.
#[test]
fn damaged_dictionaries_are_rejected() {
    let (records, dictionary) = fixture();
    let run = with_dictionary(&records, &dictionary);
    let bytes = run.as_bytes().to_vec();
    let r = regions(&bytes);
    let (dict_at, footer) = (r.frames.len(), bytes.len() - FOOTER_V4);

    // A changed byte fails the checksum.
    let mut flipped = bytes.clone();
    flipped[dict_at + 100] ^= 1;
    assert_eq!(
        rejection(flipped),
        CorruptRun {
            offset: dict_at,
            kind: RunCorruption::ChecksumMismatch
        }
    );

    // Bytes that match their checksum but are not a dictionary.
    let garbage = vec![0x5A; dictionary.bytes().len()];
    let error = rejection(with_stored_dictionary(&bytes, &garbage));
    assert_eq!(error.kind, RunCorruption::InvalidDictionary);
    assert_eq!(error.offset, dict_at);
    // The magic number but a damaged header: Zstandard refuses to prepare it.
    let mut header = dictionary.bytes().to_vec();
    header[8..64].fill(0xFF);
    let error = rejection(with_stored_dictionary(&bytes, &header));
    assert_eq!(error.kind, RunCorruption::InvalidDictionary);

    // Declared lengths out of bounds are refused from the footer alone,
    // before anything is allocated or checksummed.
    for len in [0, MAX_DICTIONARY_LEN as u32 + 1, u32::MAX] {
        let mut changed = bytes.clone();
        changed[footer + 24..footer + 28].copy_from_slice(&len.to_le_bytes());
        assert_eq!(
            rejection(changed),
            CorruptRun {
                offset: footer,
                kind: RunCorruption::InvalidDictionary
            },
            "{len}"
        );
    }
    // A length within bounds that reaches before the start of the run: the
    // dictionary region is incomplete.
    let tiny: Vec<Owned> = records[..3].to_vec();
    let small = with_dictionary(&tiny, &dictionary).as_bytes().to_vec();
    let small_footer = small.len() - FOOTER_V4;
    let index_offset = u64_at(&small, small_footer) as u32;
    assert!(index_offset < MAX_DICTIONARY_LEN as u32);
    let mut reaching = small.clone();
    reaching[small_footer + 24..small_footer + 28]
        .copy_from_slice(&(index_offset + 1).to_le_bytes());
    assert_eq!(
        rejection(reaching),
        CorruptRun {
            offset: small_footer,
            kind: RunCorruption::InvalidFooter
        }
    );

    // A footer that claims version 4 but is too short for one.
    let mut short = 4u32.to_le_bytes().to_vec();
    short.extend_from_slice(&bytes[bytes.len() - 4..]);
    let mut short = [vec![0; FOOTER_V3 - 8], short].concat();
    assert_eq!(rejection(short.clone()).kind, RunCorruption::Truncated);
    // And an unknown version.
    let at = short.len() - 8;
    short[at..at + 4].copy_from_slice(&5u32.to_le_bytes());
    assert_eq!(rejection(short).kind, RunCorruption::UnsupportedVersion);
}

/// A frame that needs a dictionary fails in a run without one, and in a
/// run whose dictionary is another: never decoded with the wrong one.
#[test]
fn missing_and_mismatched_dictionaries_are_rejected() {
    let (records, dictionary) = fixture();
    let bytes = with_dictionary(&records, &dictionary).as_bytes().to_vec();

    let error = rejection(without_dictionary(&bytes));
    assert_eq!(
        error,
        CorruptRun {
            offset: 0,
            kind: RunCorruption::MissingDictionary
        }
    );

    // A valid dictionary trained on other data.
    let other: Vec<Owned> = sensor_records(1).into_iter().step_by(4).collect();
    let other = train_on(&plain(&other, 1 << 10));
    assert_ne!(
        zstd_safe::get_dict_id_from_dict(other.bytes()),
        zstd_safe::get_dict_id_from_dict(dictionary.bytes())
    );
    let error = rejection(with_stored_dictionary(&bytes, other.bytes()));
    assert_eq!(
        error,
        CorruptRun {
            offset: 0,
            kind: RunCorruption::DictionaryMismatch
        }
    );

    // The view of a dictionary run reads nothing until its run attaches the
    // dictionary.
    let run = Run::new(bytes.clone()).unwrap();
    let detached = RunView::open(&bytes).unwrap();
    let error = detached
        .get(&records[0].0, &mut ReadWorkspace::new())
        .unwrap_err();
    assert_eq!(error.kind, RunCorruption::MissingDictionary);
    assert!(run.get(&records[0].0, &mut ReadWorkspace::new()).is_ok());
}

/// Input runs, interleaved so that their key ranges overlap.
fn inputs(records: &[Owned], count: usize) -> Vec<Run<Vec<u8>>> {
    (0..count)
        .map(|input| {
            let part: Vec<Owned> = records.iter().skip(input).step_by(count).cloned().collect();
            plain(&part, SMALL_BLOCK)
        })
        .collect()
}

fn limits(dictionary: bool) -> CompactLimits {
    CompactLimits {
        target_run_len: 4 << 20,
        max_output_bytes: usize::MAX,
        max_workspace_bytes: 64 << 20,
        codec: Codec::Zstd,
        target_block_len: SMALL_BLOCK,
        dictionary,
    }
}

fn merge(inputs: &[Run<Vec<u8>>], limits: CompactLimits) -> Compacted {
    let views: Vec<_> = inputs.iter().map(Run::view).collect();
    compact(&views, |_| true, limits, &mut MergeWorkspace::new()).unwrap()
}

fn records_of(out: &Compacted) -> Vec<Owned> {
    out.runs
        .iter()
        .flat_map(|run| walk(run.view()).unwrap())
        .collect()
}

/// On JSON-like values, compaction trains one dictionary from samples of
/// its inputs, uses it for every output run and keeps exactly the records a
/// plain compaction keeps, in fewer bytes; the workspace estimate grows by
/// the training allowance.
#[test]
fn compaction_uses_a_dictionary_where_it_saves() {
    let inputs = inputs(&json_records(1), 3);
    let plain_out = merge(&inputs, limits(false));
    assert_eq!(plain_out.stats.dictionary, DictionaryChoice::Off);
    let out = merge(&inputs, limits(true));
    let DictionaryChoice::Used { saving_percent } = out.stats.dictionary else {
        panic!("{:?}", out.stats.dictionary);
    };
    assert!(saving_percent >= 10, "{saving_percent}");
    assert!(out.stats.dictionary_time > std::time::Duration::ZERO);
    assert!(out.runs.len() > 1 && out.runs.iter().all(Run::has_dictionary));
    assert_eq!(records_of(&out), records_of(&plain_out));
    assert_eq!(
        out.workspace_bytes,
        plain_out.workspace_bytes + DICTIONARY_WORKSPACE
    );
    let held: usize = out.runs.iter().map(Run::held_bytes).sum();
    assert_eq!(out.held_bytes, held);
    // Smaller even with every run's dictionary and its prepared form.
    assert!(
        out.output_bytes + out.held_bytes < plain_out.output_bytes + plain_out.held_bytes,
        "{} + {} against {} + {}",
        out.output_bytes,
        out.held_bytes,
        plain_out.output_bytes,
        plain_out.held_bytes
    );
    // A dictionary input is sampled and merged like any other.
    let again = merge(&out.runs, limits(true));
    assert!(matches!(
        again.stats.dictionary,
        DictionaryChoice::Used { .. }
    ));
    assert_eq!(records_of(&again), records_of(&plain_out));
}

/// Every fallback builds the same records as plain Zstandard, decided
/// before the merge: small, repetitive sensor records save too little; a
/// small input is not sampled; and a workspace limit without room for
/// training leaves the output plain rather than failing.
#[test]
fn compaction_falls_back_to_plain_runs() {
    let sensor = inputs(&sensor_records(20), 2);
    let out = merge(&sensor, limits(true));
    let DictionaryChoice::NotBeneficial { saving_percent } = out.stats.dictionary else {
        panic!("{:?}", out.stats.dictionary);
    };
    assert!(saving_percent < 10);
    assert!(out.runs.iter().all(|run| !run.has_dictionary()));
    let plain_out = merge(&sensor, limits(false));
    assert_eq!(out.output_bytes, plain_out.output_bytes);
    assert_eq!(
        out.workspace_bytes,
        plain_out.workspace_bytes + DICTIONARY_WORKSPACE
    );

    let small = inputs(&json_records(1)[..2_000], 2);
    let out = merge(&small, limits(true));
    assert_eq!(out.stats.dictionary, DictionaryChoice::InputTooSmall);
    assert_eq!(
        out.workspace_bytes,
        merge(&small, limits(false)).workspace_bytes
    );

    let json = inputs(&json_records(1), 2);
    let needed = merge(&json, limits(false)).workspace_bytes;
    let tight = CompactLimits {
        max_workspace_bytes: needed + DICTIONARY_WORKSPACE - 1,
        ..limits(true)
    };
    let out = merge(&json, tight);
    assert_eq!(out.stats.dictionary, DictionaryChoice::WorkspaceLimit);
    assert!(out.runs.iter().all(|run| !run.has_dictionary()));
    assert_eq!(records_of(&out), records_of(&merge(&json, limits(false))));
}

/// Production lookups in dictionary runs against plain 4 KiB runs on the
/// same records (`LOOKUP_GROUPS` groups of the comparison dataset, 8 by
/// default): within one run, and spread over the runs one compaction
/// writes, through one workspace or one per run as the engine keeps them.
#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn lookup_costs() {
    use std::time::Instant;

    let groups = std::env::var("LOOKUP_GROUPS").map_or(8, |g| g.parse().unwrap());
    let records = json_records(groups);
    let inputs = inputs(&records, 2);
    let mut rng = super::costs::Rng::new(7);
    let keys: Vec<&[u8]> = (0..20_000)
        .map(|_| {
            records[rng.below(records.len() as u64) as usize]
                .0
                .as_slice()
        })
        .collect();
    for (name, dictionary, run_len) in [
        ("plain, one run", false, usize::MAX),
        ("dictionary, one run", true, usize::MAX),
        ("plain, 64 MiB runs", false, 64 << 20),
        ("dictionary, 64 MiB runs", true, 64 << 20),
    ] {
        let out = merge(
            &inputs,
            CompactLimits {
                target_run_len: run_len,
                ..limits(dictionary)
            },
        );
        // One workspace for every run, or one per run as the engine's
        // lookups keep them.
        for per_run in [false, true] {
            let mut workspaces: Vec<ReadWorkspace> =
                (0..out.runs.len()).map(|_| ReadWorkspace::new()).collect();
            let mut samples = Vec::with_capacity(keys.len());
            for _ in 0..2 {
                samples.clear();
                for key in &keys {
                    let start = Instant::now();
                    let index = out
                        .runs
                        .iter()
                        .position(|run| run.key_range().unwrap().unwrap().max >= *key)
                        .unwrap();
                    let workspace = &mut workspaces[if per_run { index } else { 0 }];
                    std::hint::black_box(out.runs[index].get(key, workspace).unwrap());
                    samples.push(start.elapsed().as_nanos() as u64);
                }
            }
            samples.sort_unstable();
            println!(
                "| {name} | runs {} | per-run workspaces {per_run} | {:?} | {:.2} MiB | p50 {} ns | p99 {} ns |",
                out.runs.len(),
                out.stats.dictionary,
                (out.output_bytes + out.held_bytes) as f64 / f64::from(1 << 20),
                samples[samples.len() / 2],
                samples[samples.len() * 99 / 100],
            );
        }
    }
}

/// The same lookups through the engine: records loaded in shuffled order,
/// flushed and consolidated by compaction (with the background worker
/// during the load if `LOOKUP_BACKGROUND` is set), at 4 KiB blocks with and
/// without dictionaries.
#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn engine_lookup_costs() {
    use std::time::Instant;

    use crate::engine::{CompactionOptions, Compression, Engine, EngineOptions, EngineWorkspace};

    let groups = std::env::var("LOOKUP_GROUPS").map_or(8, |g| g.parse().unwrap());
    let records: Vec<Owned> = json_records(groups)
        .into_iter()
        .filter(|record| record.2.is_some())
        .collect();
    let mut rng = super::costs::Rng::new(7);
    let keys: Vec<&[u8]> = (0..20_000)
        .map(|_| {
            records[rng.below(records.len() as u64) as usize]
                .0
                .as_slice()
        })
        .collect();
    let background = std::env::var("LOOKUP_BACKGROUND").is_ok();
    for compression in [Compression::Zstd, Compression::ZstdDictionary] {
        let engine = Engine::new(EngineOptions {
            target_block_len: SMALL_BLOCK,
            compression,
            max_runs: usize::MAX,
            max_run_bytes: usize::MAX,
            background_compaction: background.then(CompactionOptions::default),
            ..EngineOptions::default()
        });
        // Shuffled, as the comparison harness loads, so that flushes overlap.
        let mut order: Vec<usize> = (0..records.len()).collect();
        let mut shuffle = super::costs::Rng::new(11);
        for at in (1..order.len()).rev() {
            order.swap(at, shuffle.below(at as u64 + 1) as usize);
        }
        for &index in &order {
            let (key, _, value) = &records[index];
            engine.put(key, value.as_deref().unwrap()).unwrap();
        }
        engine.flush().unwrap();
        if background {
            engine.wait_for_compaction().unwrap();
        }
        let options = CompactionOptions {
            max_overlap: 1,
            max_inputs: usize::MAX,
            max_input_bytes: usize::MAX,
            max_output_bytes: usize::MAX,
            ..CompactionOptions::default()
        };
        let mut merge = MergeWorkspace::new();
        let mut choices = Vec::new();
        while let Some(report) = engine.compact(options, &mut merge).unwrap() {
            choices.push(report.stats.dictionary);
        }
        let mut workspace = EngineWorkspace::new();
        let mut samples = Vec::with_capacity(keys.len());
        for _ in 0..2 {
            samples.clear();
            for key in &keys {
                let start = Instant::now();
                std::hint::black_box(engine.get(key, &mut workspace).unwrap());
                samples.push(start.elapsed().as_nanos() as u64);
            }
        }
        samples.sort_unstable();
        let stats = engine.stats();
        println!(
            "| engine {compression:?} | runs {} | {choices:?} | {:.2} MiB + {:.2} | p50 {} ns | p99 {} ns |",
            stats.runs,
            stats.run_bytes as f64 / f64::from(1 << 20),
            stats.held_bytes as f64 / f64::from(1 << 20),
            samples[samples.len() / 2],
            samples[samples.len() * 99 / 100],
        );
    }
}
