//! The directory against a model and against the ungrouped readers: its
//! invariants, lookups and scans over generated histories, conflicts, damage,
//! and its publication with the engine's runs.

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

use super::*;
use crate::block::Value;
use crate::engine::{
    CompactionPolicy, Engine, MergeWorkspace, Options, Published, Snapshot, deepest_overlap,
};
use crate::run::{
    Codec, ConflictingCopies, LiveValue, LogicalCursor, LogicalWorkspace, ReadError, RunBuilder,
    latest_from, latest_in,
};

/// A key, its sequence number and its value, `None` for a tombstone.
type Record = (Vec<u8>, u64, Option<Vec<u8>>);

/// A live key, its sequence number and value, as a scan returns it.
type Live = (Vec<u8>, u64, Vec<u8>);

/// The newest version of each key: its sequence number and value.
type Model = BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)>;

fn run(records: &[Record]) -> SharedRun {
    let mut sorted = records.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    // Small blocks, so that runs span several and scans cross them.
    let mut builder = RunBuilder::with_raw_block_target(Codec::Zstd, 64);
    for (key, seq, value) in &sorted {
        let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
        builder.add(key, *seq, value).unwrap();
    }
    Arc::new(Published {
        id: crate::engine::checkpoint::new_run_id(),
        checkpoint_crc: std::sync::OnceLock::new(),
        run: builder.finish(),
        largest_block: 0,
    })
}

fn put(key: &[u8], seq: u64, value: &[u8]) -> Record {
    (key.to_vec(), seq, Some(value.to_vec()))
}

fn model_of(runs: &[Vec<Record>]) -> Model {
    let mut model = Model::new();
    for (key, seq, value) in runs.iter().flatten() {
        let newer = model.get(key).is_none_or(|&(newest, _)| *seq > newest);
        if newer {
            model.insert(key.clone(), (*seq, value.clone()));
        }
    }
    model
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

/// Binary keys around the edges of the byte order, and a run of ordinary
/// ones, sorted.
fn alphabet() -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 0xFF],
        vec![1],
        vec![0x7F],
        vec![0x80],
        vec![0xFE],
        vec![0xFE, 0xFF],
        vec![0xFF],
        vec![0xFF, 0],
        vec![0xFF, 0xFF],
        vec![0xFF, 0xFF, 0xFF],
    ];
    keys.extend((0..48).map(|i| format!("k{i:03}").into_bytes()));
    keys.sort();
    keys
}

/// Generates `count` runs, each over a window of the alphabet whose width is
/// up to `width` keys and that holds both of its ends, so that windows sharing
/// an end overlap at exactly one key. Sequence numbers are a shuffled pool, so
/// runs' bounds interleave. A run may hold two versions of a key, a
/// tombstone, an empty value, or an identical copy of a record another run
/// holds.
fn history(rng: &mut Rng, count: usize, width: usize) -> Vec<Vec<Record>> {
    let keys = alphabet();
    // Enough sequence numbers for every record, shuffled.
    let mut seqs: Vec<u64> = (1..=(count * (2 * width + 4)) as u64).collect();
    for at in (1..seqs.len()).rev() {
        seqs.swap(at, rng.below(at + 1));
    }
    let mut seqs = seqs.into_iter();
    let mut runs: Vec<Vec<Record>> = Vec::new();
    for _ in 0..count {
        let low = rng.below(keys.len());
        let high = (low + rng.below(width)).min(keys.len() - 1);
        let mut chosen = vec![low, high];
        chosen.extend((low..=high).filter(|_| rng.below(3) == 0));
        chosen.sort_unstable();
        chosen.dedup();
        let mut records: Vec<Record> = Vec::new();
        for index in chosen {
            let versions = if rng.below(6) == 0 { 2 } else { 1 };
            for _ in 0..versions {
                let value = match rng.below(8) {
                    0 | 1 => None,
                    2 => Some(Vec::new()),
                    _ => Some(vec![rng.below(256) as u8; 1 + rng.below(40)]),
                };
                records.push((keys[index].clone(), seqs.next().unwrap(), value));
            }
        }
        // An identical copy of a record an earlier run holds, for a key this
        // run does not hold yet.
        if !runs.is_empty() && rng.below(3) == 0 {
            let earlier = &runs[rng.below(runs.len())];
            let record = &earlier[rng.below(earlier.len())];
            if !records.iter().any(|(key, _, _)| *key == record.0) {
                records.push(record.clone());
            }
        }
        runs.push(records);
    }
    runs
}

fn owned(
    found: Result<Option<LiveValue<'_>>, ReadError>,
) -> Result<Option<(u64, Vec<u8>)>, ReadError> {
    found.map(|found| found.map(|found| (found.seq, found.value.to_vec())))
}

fn live(model: &Model, key: &[u8]) -> Option<(u64, Vec<u8>)> {
    let (seq, value) = model.get(key)?;
    value.as_ref().map(|value| (*seq, value.clone()))
}

/// Every run once; each group key-disjoint and ascending, with its bound the
/// largest of its runs'; groups by descending bound; as few groups as the
/// deepest overlap; and the same directory from the same runs.
fn check_invariants(runs: &[SharedRun], directory: &RunDirectory) {
    assert_eq!(directory.len(), runs.len());
    for run in runs {
        let held = directory
            .runs()
            .iter()
            .filter(|held| Arc::ptr_eq(held, run));
        assert_eq!(held.count(), 1, "every run once");
    }
    let mut previous_bound = u64::MAX;
    let mut covered = 0;
    for group in &directory.groups {
        assert_eq!(group.start, covered, "groups tile the runs");
        assert!(group.end > group.start, "no empty group");
        covered = group.end;
        let members = &directory.runs[group.start..group.end];
        for pair in members.windows(2) {
            let (left, right) = (pair[0].run.key_range(), pair[1].run.key_range());
            assert!(left.unwrap().max < right.unwrap().min, "strictly disjoint");
        }
        let bound = members.iter().map(|run| run.run.max_seq()).max().unwrap();
        assert_eq!(group.max_seq, bound);
        assert!(bound <= previous_bound, "newest group first");
        previous_bound = bound;
    }
    assert_eq!(covered, runs.len());
    assert_eq!(directory.group_count(), deepest_overlap(runs));

    let again = RunDirectory::build(runs);
    let same = again
        .runs()
        .iter()
        .zip(directory.runs())
        .all(|(a, b)| Arc::ptr_eq(a, b));
    assert!(same, "deterministic");
    assert_eq!(again.groups.len(), directory.groups.len());
}

/// Collects a prefix scan through the directory's grouped merge.
fn grouped_scan(
    directory: &RunDirectory,
    prefix: &[u8],
    workspace: &mut LogicalWorkspace,
) -> Result<Vec<Live>, ReadError> {
    let mut cursor = LogicalCursor::grouped(directory, directory.groups(), workspace);
    cursor.seek_prefix(prefix)?;
    let mut out = Vec::new();
    while let Some(record) = cursor.next_record()? {
        out.push((record.key.to_vec(), record.seq, record.value.to_vec()));
    }
    Ok(out)
}

/// The same scan through the merge of every run as its own input.
fn flat_scan(
    runs: &[SharedRun],
    prefix: &[u8],
    workspace: &mut LogicalWorkspace,
) -> Result<Vec<Live>, ReadError> {
    let mut cursor = LogicalCursor::over(runs.iter().map(|run| run.run.view()), workspace);
    cursor.seek_prefix(prefix)?;
    let mut out = Vec::new();
    while let Some(record) = cursor.next_record()? {
        out.push((record.key.to_vec(), record.seq, record.value.to_vec()));
    }
    Ok(out)
}

const PREFIXES: [&[u8]; 14] = [
    b"",
    &[0],
    &[0, 0],
    &[0x7F],
    &[0x80],
    &[0xFE],
    &[0xFF],
    &[0xFF, 0xFF],
    &[0xFF, 0xFF, 0xFF, 0xFF],
    b"j",
    b"k",
    b"k0",
    b"k02",
    b"k04",
];

/// Generated histories from disjoint to overlap far above any compaction
/// allowance: every lookup and prefix scan through the directory agrees with
/// the model and with the flat readers.
#[test]
fn directory_reads_agree_with_the_model_and_the_flat_readers() {
    let mut rng = Rng(0x5eed_05a9);
    let mut probes = alphabet();
    probes.extend(alphabet().into_iter().map(|mut key| {
        key.push(1);
        key
    }));
    probes.push(b"zzz".to_vec());
    let mut logical = LogicalWorkspace::new();
    let mut flat_ws = LogicalWorkspace::new();
    let mut deepest = 0;
    for (count, width) in [
        (1, 4),
        (2, 3),
        (5, 2),
        (8, 70),
        (20, 3),
        (20, 12),
        (60, 6),
        (60, 70),
    ] {
        for _ in 0..6 {
            let records = history(&mut rng, count, width);
            let model = model_of(&records);
            let runs: Vec<SharedRun> = records.iter().map(|records| run(records)).collect();
            let directory = RunDirectory::build(&runs);
            check_invariants(&runs, &directory);
            deepest = deepest.max(directory.group_count());
            let flat = flat_order(&runs);
            for probe in &probes {
                let want = live(&model, probe);
                let found = owned(latest_from(directory.lookup(), probe, &mut logical));
                assert_eq!(found, Ok(want.clone()), "lookup {probe:02x?}");
                let flat = owned(latest_in(
                    flat.iter().map(|run| &run.run),
                    probe,
                    &mut flat_ws,
                ));
                assert_eq!(flat, Ok(want), "flat lookup {probe:02x?}");
            }
            for prefix in PREFIXES {
                let want: Vec<_> = model
                    .iter()
                    .filter(|(key, _)| key.starts_with(prefix))
                    .filter_map(|(key, (seq, value))| Some((key.clone(), *seq, value.clone()?)))
                    .collect();
                let grouped = grouped_scan(&directory, prefix, &mut logical).unwrap();
                assert_eq!(grouped, want, "prefix {prefix:02x?}");
                assert_eq!(flat_scan(&runs, prefix, &mut flat_ws).unwrap(), want);
            }
            // A seek, then reading on past several runs of a group.
            for target in alphabet().iter().step_by(7) {
                let mut cursor =
                    LogicalCursor::grouped(&directory, directory.groups(), &mut logical);
                cursor.seek(target).unwrap();
                let mut keys = Vec::new();
                while let Some(record) = cursor.next_record().unwrap() {
                    keys.push(record.key.to_vec());
                }
                let want: Vec<_> = model
                    .range(target.clone()..)
                    .filter(|(_, (_, value))| value.is_some())
                    .map(|(key, _)| key.clone())
                    .collect();
                assert_eq!(keys, want, "seek {target:02x?}");
            }
        }
    }
    assert!(deepest >= 30, "overlap reached {deepest} groups");
}

fn flat_order(runs: &[SharedRun]) -> Vec<SharedRun> {
    let mut order = runs.to_vec();
    order.sort_by_key(|run| std::cmp::Reverse(run.run.max_seq()));
    order
}

/// Ranges that share an end key overlap, so they cannot share a group.
#[test]
fn runs_sharing_an_end_key_are_in_different_groups() {
    let runs = vec![
        run(&[put(b"a", 1, b"1"), put(b"c", 2, b"2")]),
        run(&[put(b"c", 3, b"3"), put(b"e", 4, b"4")]),
        run(&[put(b"f", 5, b"5"), put(b"g", 6, b"6")]),
    ];
    let directory = RunDirectory::build(&runs);
    check_invariants(&runs, &directory);
    assert_eq!(directory.group_count(), 2);
    let mut logical = LogicalWorkspace::new();
    let found = owned(latest_from(directory.lookup(), b"c", &mut logical));
    assert_eq!(found, Ok(Some((3, b"3".to_vec()))));
    let all = grouped_scan(&directory, b"", &mut logical).unwrap();
    let keys: Vec<&[u8]> = all.iter().map(|(key, _, _)| key.as_slice()).collect();
    assert_eq!(keys, [&b"a"[..], b"c", b"e", b"f", b"g"]);
}

/// A copy with the sequence number already found can be in a group whose
/// bound equals it, so that group is still read: a differing copy is a
/// conflict naming both runs' directory positions, and an identical one
/// counts once.
#[test]
fn equal_sequence_copies_in_other_groups_are_still_read() {
    for (other, conflict) in [(b"x", false), (b"y", true)] {
        let runs = vec![
            run(&[put(b"a", 1, b"-"), put(b"b", 10, b"x"), put(b"c", 2, b"-")]),
            run(&[put(b"b", 10, other), put(b"d", 3, b"-")]),
        ];
        let directory = RunDirectory::build(&runs);
        assert_eq!(directory.group_count(), 2);
        assert_eq!(directory.groups[0].max_seq, directory.groups[1].max_seq);
        let mut logical = LogicalWorkspace::new();
        let found = owned(latest_from(directory.lookup(), b"b", &mut logical));
        let scanned = grouped_scan(&directory, b"b", &mut logical);
        if conflict {
            let expected = ReadError::ConflictingCopies(ConflictingCopies {
                key: b"b".to_vec(),
                seq: 10,
                inputs: (0, 1),
            });
            assert_eq!(found, Err(expected.clone()));
            assert_eq!(scanned, Err(expected));
        } else {
            assert_eq!(found, Ok(Some((10, b"x".to_vec()))));
            assert_eq!(scanned.unwrap(), [(b"b".to_vec(), 10, b"x".to_vec())]);
        }
    }
}

/// Versions of a key in several groups, the newest a tombstone in one and an
/// older value in another: lookups and scans see the key deleted, and a later
/// value in a third group revives it.
#[test]
fn versions_across_groups_resolve_by_sequence() {
    let base = vec![
        run(&[put(b"k1", 5, b"old"), put(b"k5", 6, b"keep")]),
        run(&[(b"k1".to_vec(), 9, None), put(b"k3", 2, b"-")]),
        run(&[
            put(b"k0", 1, b"-"),
            put(b"k1", 7, b"mid"),
            put(b"k2", 3, b"-"),
        ]),
    ];
    let directory = RunDirectory::build(&base);
    assert_eq!(directory.group_count(), 3);
    let mut logical = LogicalWorkspace::new();
    assert_eq!(
        owned(latest_from(directory.lookup(), b"k1", &mut logical)),
        Ok(None)
    );
    let keys: Vec<Vec<u8>> = grouped_scan(&directory, b"k", &mut logical)
        .unwrap()
        .into_iter()
        .map(|(key, _, _)| key)
        .collect();
    assert_eq!(
        keys,
        [
            b"k0".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k5".to_vec()
        ]
    );

    let mut revived = base;
    revived.push(run(&[put(b"k1", 11, b"new"), put(b"k9", 12, b"-")]));
    let directory = RunDirectory::build(&revived);
    let found = owned(latest_from(directory.lookup(), b"k1", &mut logical));
    assert_eq!(found, Ok(Some((11, b"new".to_vec()))));
}

/// The first frame's payload, which every lookup or scan of the run's first
/// block decodes.
const DAMAGE_AT: usize = 16;

/// Damage in a run is reported by a lookup or scan that must read it, and
/// cannot be noticed by one that rules the run out: by its range within a
/// group, or by its group's bound once a newer version is found.
#[test]
fn damage_is_reported_only_where_a_read_must_open_the_run() {
    let newest = run(&[
        put(b"a", 50, b"new"),
        put(b"m", 51, b"new"),
        put(b"z", 52, b"-"),
    ]);
    let left = run(&[put(b"b", 1, b"-"), put(b"c", 2, b"-")]);
    let damaged_source = run(&[put(b"m", 3, b"old"), put(b"n", 4, b"old")]);
    let damaged = Arc::new(Published {
        id: crate::engine::checkpoint::new_run_id(),
        checkpoint_crc: std::sync::OnceLock::new(),
        run: damaged_source.run.damaged_at(DAMAGE_AT),
        largest_block: 0,
    });
    let right = run(&[put(b"p", 5, b"-"), put(b"q", 6, b"-")]);
    let runs = vec![newest, left, Arc::clone(&damaged), right];
    let directory = RunDirectory::build(&runs);
    assert_eq!(directory.group_count(), 2);
    assert_eq!(directory.groups[1].end - directory.groups[1].start, 3);
    let mut logical = LogicalWorkspace::new();

    // The newest group finds `m`; the older group's bound is below it.
    let found = owned(latest_from(directory.lookup(), b"m", &mut logical));
    assert_eq!(found, Ok(Some((51, b"new".to_vec()))));
    // `n` is only in the damaged run, which must be opened.
    let found = owned(latest_from(directory.lookup(), b"n", &mut logical));
    assert!(matches!(found, Err(ReadError::Corrupt(_))), "{found:?}");
    // Its neighbours in the group are read without touching it.
    for key in [&b"c"[..], b"p"] {
        assert!(owned(latest_from(directory.lookup(), key, &mut logical)).is_ok());
    }

    // A scan that reaches the damaged run fails, and keeps failing.
    let mut cursor = LogicalCursor::grouped(&directory, directory.groups(), &mut logical);
    cursor.seek_prefix(b"").unwrap();
    let mut failed = None;
    for _ in 0..10 {
        if let Err(error) = cursor.next_record() {
            failed = Some(error);
            break;
        }
    }
    let failed = failed.expect("the scan reaches the damaged run");
    assert!(matches!(failed, ReadError::Corrupt(_)));
    assert_eq!(cursor.next_record().unwrap_err(), failed, "repeated");
    // Prefixes on either side of it, in the same group, read cleanly.
    for prefix in [&b"b"[..], b"c", b"p", b"q"] {
        assert_eq!(
            grouped_scan(&directory, prefix, &mut logical)
                .unwrap()
                .len(),
            1
        );
    }
}

/// A scan reads a group of many runs through one decoding buffer and one
/// decompression context, where the flat merge holds one per run.
#[test]
fn a_group_reuses_one_decoder_across_its_runs() {
    let runs: Vec<SharedRun> = (0..24u64)
        .map(|index| {
            let records: Vec<Record> = (0..20u64)
                .map(|offset| {
                    let key = format!("key{:06}", index * 100 + offset).into_bytes();
                    (key, index * 100 + offset + 1, Some(vec![b'v'; 50]))
                })
                .collect();
            run(&records)
        })
        .collect();
    let directory = RunDirectory::build(&runs);
    assert_eq!(directory.group_count(), 1);
    let mut grouped = LogicalWorkspace::new();
    let mut flat = LogicalWorkspace::new();
    let all = grouped_scan(&directory, b"", &mut grouped).unwrap();
    assert_eq!(all.len(), 24 * 20);
    assert_eq!(all, flat_scan(&runs, b"", &mut flat).unwrap());
    let one_context = grouped.context_bytes();
    assert!(one_context > 0);
    assert_eq!(flat.context_bytes(), 24 * one_context);
    assert!(grouped.capacity() < flat.capacity());
    // A prefix ending exactly at a run boundary, and one spanning two runs.
    let keys = |records: Vec<Live>| records.len();
    assert_eq!(
        keys(grouped_scan(&directory, b"key0001", &mut grouped).unwrap()),
        20
    );
    assert_eq!(
        keys(grouped_scan(&directory, b"key000", &mut grouped).unwrap()),
        200
    );
}

/// Every run heavily overlapping: as many groups as runs, read in the flat
/// order, and lookups decode no more than the flat lookup does.
#[test]
fn heavy_overlap_reads_like_the_flat_order() {
    let runs: Vec<SharedRun> = (0..40u64)
        .map(|index| {
            run(&[
                put(b"a", index * 3 + 1, b"-"),
                put(format!("m{index:02}").as_bytes(), index * 3 + 2, b"v"),
                put(b"z", index * 3 + 3, b"-"),
            ])
        })
        .collect();
    let directory = RunDirectory::build(&runs);
    check_invariants(&runs, &directory);
    assert_eq!(directory.group_count(), 40);
    let flat = flat_order(&runs);
    let mut grouped_ws = LogicalWorkspace::new();
    let mut flat_ws = LogicalWorkspace::new();
    for key in [&b"a"[..], b"m07", b"m39", b"m", b"z"] {
        let (before_grouped, before_flat) = (grouped_ws.blocks_loaded(), flat_ws.blocks_loaded());
        let grouped = owned(latest_from(directory.lookup(), key, &mut grouped_ws));
        let flat = owned(latest_in(
            flat.iter().map(|run| &run.run),
            key,
            &mut flat_ws,
        ));
        assert_eq!(grouped, flat);
        assert_eq!(
            grouped_ws.blocks_loaded() - before_grouped,
            flat_ws.blocks_loaded() - before_flat,
            "{key:02x?}"
        );
    }
}

// The engine.

fn small() -> Options {
    Options {
        memtable_size: 16 << 10,
        target_run_len: 16 << 10,

        background_compaction: None,
        ..Options::default()
    }
}

fn key(index: u64) -> Vec<u8> {
    format!("key/{index:06}").into_bytes()
}

/// The directory holds exactly the state's runs.
fn assert_published(engine: &Engine) {
    let state = engine.core.read_state();
    check_invariants(&state.runs, &state.directory);
}

/// Flushes, compactions and recovery each publish a directory of exactly the
/// runs they leave; a snapshot keeps the directory it was taken with.
#[test]
fn every_publication_rebuilds_the_directory_and_snapshots_keep_theirs() {
    let engine = Engine::in_memory(small()).unwrap();
    let value = vec![7u8; 100];
    let mut merge = MergeWorkspace::new();
    for round in 0..6u64 {
        for index in 0..300 {
            engine
                .ks()
                .put(&key((index * 7 + round * 13) % 1000), &value)
                .unwrap();
        }
        engine.flush().unwrap();
        assert_published(&engine);
    }
    let snapshot = engine.snapshot();
    let pinned = Arc::clone(&snapshot.view.runs);
    assert!(Arc::ptr_eq(&pinned, &engine.core.read_state().directory));
    let ks = engine.ks();
    let keys = |snapshot: &Snapshot| {
        let mut reader = snapshot.reader();
        let mut scan = reader.range(&ks, ..);
        let mut keys = Vec::new();
        while let Some((key, _)) = scan.next().unwrap() {
            keys.push(key.to_vec());
        }
        keys
    };
    let before = keys(&snapshot);
    loop {
        match engine.core.compact(CompactionPolicy::default(), &mut merge) {
            Ok(Some(_)) => assert_published(&engine),
            Ok(None) => break,
            Err(error) => panic!("{error:?}"),
        }
    }
    engine.ks().delete(&key(1)).unwrap();
    engine.flush().unwrap();
    assert_published(&engine);
    assert!(!Arc::ptr_eq(&pinned, &engine.core.read_state().directory));
    // The snapshot still reads the runs it was taken with.
    assert!(Arc::ptr_eq(&pinned, &snapshot.view.runs));
    assert_eq!(keys(&snapshot), before);
    drop(snapshot);
    drop(pinned);

    let dir = std::env::temp_dir().join(format!("tessera-directory-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    engine.checkpoint_to(&dir).unwrap();
    let recovered = Engine::open(&dir, small()).unwrap();
    assert_published(&recovered);
    for index in 0..1000 {
        let want = engine.ks().get(&key(index)).unwrap();
        let found = recovered.ks().get(&key(index)).unwrap();
        assert_eq!(found, want);
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Readers racing flushes and compactions each read one published
/// directory: every key written before the readers started is found with a
/// value at least as new as the last one they saw, and every scan is sorted
/// and complete.
#[test]
fn readers_see_whole_directories_while_runs_are_published() {
    const KEYS: u64 = 400;
    let engine = Arc::new(Engine::in_memory(small()).unwrap());
    for index in 0..KEYS {
        engine.ks().put(&key(index), &0u64.to_le_bytes()).unwrap();
    }
    engine.flush().unwrap();
    let start = Arc::new(Barrier::new(3));
    let readers: Vec<_> = (0..2)
        .map(|reader| {
            let engine = Arc::clone(&engine);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                let ks = engine.ks();
                let mut reader_handle = engine.reader();
                let mut seen = vec![0u64; KEYS as usize];
                start.wait();
                for pass in 0..40 {
                    for index in (0..KEYS).filter(|index| (index + pass) % 5 == reader) {
                        let found = reader_handle.get(&ks, &key(index)).unwrap();
                        let value = u64::from_le_bytes(found.unwrap().try_into().unwrap());
                        assert!(value >= seen[index as usize], "went back");
                        seen[index as usize] = value;
                    }
                    let mut scan = reader_handle.prefix(&ks, b"key/");
                    let mut count = 0;
                    let mut last: Option<Vec<u8>> = None;
                    while let Some((key, _)) = scan.next().unwrap() {
                        assert!(last.as_deref() < Some(key));
                        last = Some(key.to_vec());
                        count += 1;
                    }
                    assert_eq!(count, KEYS);
                }
            })
        })
        .collect();
    start.wait();
    let mut merge = MergeWorkspace::new();
    for round in 1..=30u64 {
        for index in (0..KEYS).filter(|index| index % 3 == round % 3) {
            engine.ks().put(&key(index), &round.to_le_bytes()).unwrap();
        }
        engine.flush().unwrap();
        if round % 4 == 0 {
            while let Ok(Some(_)) = engine.core.compact(CompactionPolicy::default(), &mut merge) {}
        }
    }
    for reader in readers {
        reader.join().unwrap();
    }
    assert_published(&engine);
}

/// The directory's own memory: one pointer per run and a few words per
/// group.
#[test]
fn the_directory_holds_a_pointer_per_run() {
    let runs: Vec<SharedRun> = (0..10u64)
        .map(|index| run(&[put(format!("{index}").as_bytes(), index + 1, b"-")]))
        .collect();
    let directory = RunDirectory::build(&runs);
    assert_eq!(directory.group_count(), 1);
    assert_eq!(
        directory.heap_bytes(),
        10 * size_of::<SharedRun>() + size_of::<Group>()
    );
}
