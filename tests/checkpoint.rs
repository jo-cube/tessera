//! Checkpoints and recovery through the public API: what a checkpoint holds,
//! what recovery accepts and rejects, and what a failed checkpoint leaves.
//! Interrupted publication is tested beside the code, where crashes can be
//! injected. Files go to a fresh directory under the system temporary
//! directory, which the bounded runner makes a small tmpfs.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::engine::checkpoint::{Damage, OpenError, recover};
use crate::engine::{CompactionPolicy, Compression, Engine, ErrorKind, MergeWorkspace, Options};

/// The defaults without the background worker: these tests flush and
/// compact by hand and count what each step leaves.
fn manual() -> Options {
    Options {
        background_compaction: None,
        ..Options::default()
    }
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("tessera-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn contents(engine: &Engine, prefix: &[u8]) -> Model {
    snapshot_contents(&engine.snapshot(), &engine.ks(), prefix)
}

fn snapshot_contents(snapshot: &crate::Snapshot, ks: &crate::Keyspace, prefix: &[u8]) -> Model {
    let mut reader = snapshot.reader();
    let mut scan = reader.prefix(ks, prefix);
    let mut out = Model::new();
    while let Some((key, value)) = scan.next().unwrap() {
        out.insert(key.to_vec(), value.to_vec());
    }
    out
}

fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    engine.ks().get(key).unwrap()
}

/// The damage recovery finds in `dir`, which [`Engine::open`] refuses as
/// corrupt at `path`.
fn damage_at(dir: &Path, path: &Path) -> Damage {
    let error = Engine::open(dir, manual()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Corruption, "{error}");
    assert_eq!(error.path(), Some(path), "{error}");
    match recover(dir, &manual()) {
        Err(OpenError::Corrupt { path: at, damage }) => {
            assert_eq!(at, path);
            damage
        }
        Err(error) => panic!("{error:?}"),
        Ok(_) => panic!("damage at {} was accepted", path.display()),
    }
}

fn checkpoint_dirs(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name != "LOCK")
        .collect();
    names.sort();
    names
}

fn compact_all(engine: &Engine) {
    let options = CompactionPolicy {
        max_overlap: 1,
        ..CompactionPolicy::default()
    };
    let mut workspace = MergeWorkspace::new();
    while engine
        .core
        .compact(options, &mut workspace)
        .unwrap()
        .is_some()
    {}
}

/// State in runs and the active memtable, including deletes in the
/// active table, with binary and empty keys, a large value, and deletes that
/// hide older values in runs.
fn varied_engine() -> (Engine, Model) {
    let engine = Engine::in_memory(Options {
        target_run_len: 4096,
        ..manual()
    })
    .unwrap();
    let mut model = Model::new();
    let mut put = |key: Vec<u8>, value: Vec<u8>| {
        engine.ks().put(&key, &value).unwrap();
        model.insert(key, value);
    };
    for i in 0..2_000u32 {
        put(i.to_be_bytes().to_vec(), format!("v{i}").into_bytes());
    }
    put(Vec::new(), b"empty key".to_vec());
    put(vec![0xFF; 40], vec![0xAB; 100_000]);
    put(vec![0], Vec::new());
    engine.flush().unwrap();
    for i in (0..2_000u32).step_by(3) {
        engine.ks().delete(&i.to_be_bytes()).unwrap();
        model.remove(&i.to_be_bytes()[..]);
    }
    engine.flush().unwrap();
    for i in (1..2_000u32).step_by(7) {
        let (key, value) = (i.to_be_bytes().to_vec(), b"newer".to_vec());
        engine.ks().put(&key, &value).unwrap();
        model.insert(key, value);
    }
    engine.ks().delete(&[0xFF; 40]).unwrap();
    model.remove(&[0xFF; 40][..]);
    (engine, model)
}

#[test]
fn recovery_reproduces_the_checkpointed_state_and_continues_its_sequence() {
    let scratch = Scratch::new("round-trip");
    let (engine, model) = varied_engine();
    let report = engine.checkpoint_to(&scratch.0).unwrap();
    assert_eq!(report.last_seq, engine.engine_stats().last_seq);
    assert!(report.records as usize >= model.len());
    assert!(report.runs > 1, "split at the target run length");
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000001", "runs"]
    );

    let recovered = Engine::open(&scratch.0, manual()).unwrap();
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(
        contents(&recovered, &[0, 0, 3]),
        contents(&engine, &[0, 0, 3])
    );
    for key in [
        &b""[..],
        &[0],
        &[0xFF; 40],
        &3u32.to_be_bytes(),
        &4u32.to_be_bytes(),
    ] {
        assert_eq!(get(&recovered, key), model.get(key).cloned(), "{key:?}");
    }
    let keys: Vec<_> = (0..2_100u32).map(|i| i.to_be_bytes().to_vec()).collect();
    let mut reader = recovered.reader();
    let actual: Vec<_> = reader
        .multi_get(&recovered.ks(), &keys)
        .unwrap()
        .iter()
        .map(|value| value.map(<[u8]>::to_vec))
        .collect();
    drop(reader);
    assert_eq!(
        actual,
        keys.iter()
            .map(|key| model.get(key).cloned())
            .collect::<Vec<_>>()
    );
    let stats = recovered.engine_stats();
    assert_eq!(stats.last_seq, report.last_seq);
    assert!(stats.held_bytes > 0, "filters are restored");
    assert_eq!(
        recovered.ks().put_seq(b"next", b"1").unwrap(),
        report.last_seq + 1
    );

    // The recovered engine flushes, compacts and checkpoints like any other.
    recovered.ks().delete(&4u32.to_be_bytes()).unwrap();
    recovered.flush().unwrap();
    compact_all(&recovered);
    let second = recovered.checkpoint_to(&scratch.0).unwrap();
    assert_eq!((second.generation, second.reclaimed), (2, 0));
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        [
            "CURRENT",
            "ckpt-00000000000000000001",
            "ckpt-00000000000000000002",
            "runs"
        ]
    );
    let expected = contents(&recovered, b"");
    drop(recovered);
    let again = Engine::open(&scratch.0, manual()).unwrap();
    assert_eq!(contents(&again, b""), expected);
    assert_eq!(get(&again, &4u32.to_be_bytes()), None);
}

/// Each block records its codec, so the configured compression decides only
/// what new runs use: a checkpoint written under one setting recovers under
/// any other, and compaction rewrites what it merges.
#[test]
fn checkpoints_recover_under_any_compression() {
    const ALL: [Compression; 2] = [Compression::Zstd, Compression::Uncompressed];
    let with = |compression| Options {
        target_run_len: 1 << 20,
        compression,
        ..manual()
    };
    let mut run_bytes = Vec::new();
    for written in ALL {
        let scratch = Scratch::new(&format!("compression-{written:?}"));
        let engine = Engine::in_memory(with(written)).unwrap();
        let mut model = Model::new();
        for i in 0..3_000u32 {
            let (key, value) = (i.to_be_bytes().to_vec(), format!("value {} of {i}", i % 10));
            engine.ks().put(&key, value.as_bytes()).unwrap();
            model.insert(key, value.into_bytes());
        }
        engine.flush().unwrap();
        for i in (0..3_000u32).step_by(3) {
            engine.ks().delete(&i.to_be_bytes()).unwrap();
            model.remove(&i.to_be_bytes()[..]);
        }
        engine.flush().unwrap();
        assert_eq!(contents(&engine, b""), model);
        run_bytes.push(engine.engine_stats().run_bytes);
        engine.checkpoint_to(&scratch.0).unwrap();

        for read in ALL {
            let recovered = Engine::open(&scratch.0, with(read)).unwrap();
            assert_eq!(contents(&recovered, b""), model, "{written:?} as {read:?}");
            assert_eq!(
                get(&recovered, &4u32.to_be_bytes()),
                model.get(&4u32.to_be_bytes()[..]).cloned()
            );
            assert_eq!(get(&recovered, &3u32.to_be_bytes()), None);
            // A newer run in the configured codec beside the recovered ones:
            // overwrites, a delete and a key a delete had hidden.
            let mut newer = model.clone();
            for i in [1u32, 3, 4, 2_999] {
                let (key, value) = (i.to_be_bytes().to_vec(), format!("newer {i}"));
                recovered.ks().put(&key, value.as_bytes()).unwrap();
                newer.insert(key, value.into_bytes());
            }
            recovered.ks().delete(&2u32.to_be_bytes()).unwrap();
            newer.remove(&2u32.to_be_bytes()[..]);
            recovered.flush().unwrap();
            assert!(recovered.engine_stats().runs > 1);
            // The newer run decides overwritten and deleted keys alone: the
            // recovered run holding the older version is not decoded.
            for (key, expected) in [(4u32, newer.get(&4u32.to_be_bytes()[..])), (2, None)] {
                let mut reader = recovered.reader();
                let found = reader.get(&recovered.ks(), &key.to_be_bytes()).unwrap();
                assert_eq!(
                    found.map(<[u8]>::to_vec).as_ref(),
                    expected,
                    "{written:?} as {read:?}"
                );
                assert_eq!(reader.counters().blocks_loaded, 1, "key {key}");
            }
            let snapshot = recovered.snapshot();
            // Compaction writes the merged runs with the configured codec;
            // the snapshot keeps reading the runs it replaced.
            compact_all(&recovered);
            assert_eq!(contents(&recovered, b""), newer, "{written:?} as {read:?}");
            let seen = snapshot_contents(&snapshot, &recovered.ks(), b"");
            assert_eq!(seen, newer, "snapshot, {written:?} as {read:?}");
        }
    }
    assert!(run_bytes[0] < run_bytes[1], "{run_bytes:?}");
}

/// Like the codec, the block length decides only what new runs use: each
/// block records its length, so runs of every length are read side by side
/// and recover under any setting, and compactions rewrite
/// records in the current setting. Checkpoints preserve existing bytes.
#[test]
fn runs_of_any_block_length_recover_mix_and_are_rewritten() {
    const LENGTHS: [usize; 3] = [16 << 10, 2 << 10, 256];
    let with = |block_size| Options {
        target_run_len: 1 << 20,
        block_size,
        ..manual()
    };
    // The decoded block one lookup leaves in a fresh reader.
    let block_of = |engine: &Engine, key: u32| {
        let mut reader = engine.reader().unpooled();
        reader.get(&engine.ks(), &key.to_be_bytes()).unwrap();
        assert_eq!(reader.counters().blocks_loaded, 1);
        reader.counters().capacity
    };
    // Adaptive compressed targets may reach the raw cap on repetitive records.
    let within = |block: usize, _target: usize| block <= 64 << 10;
    let mut blocks = Vec::new();
    for written in LENGTHS {
        let scratch = Scratch::new(&format!("block-length-{written}"));
        let engine = Engine::in_memory(with(written)).unwrap();
        let mut model = Model::new();
        for i in 0..3_000u32 {
            let (key, value) = (i.to_be_bytes().to_vec(), format!("value {} of {i}", i % 10));
            engine.ks().put(&key, value.as_bytes()).unwrap();
            model.insert(key, value.into_bytes());
        }
        engine.flush().unwrap();
        compact_all(&engine);
        let block = block_of(&engine, 4);
        assert!(within(block, written), "{block} for {written}");
        blocks.push(block);
        engine.checkpoint_to(&scratch.0).unwrap();

        for read in LENGTHS {
            let recovered = Engine::open(&scratch.0, with(read)).unwrap();
            assert_eq!(contents(&recovered, b""), model, "{written} as {read}");
            // Recovered runs keep their blocks.
            assert_eq!(block_of(&recovered, 4), block, "{written} as {read}");
            // A newer run in the configured length beside them: overwrites,
            // deletes, and keys only the older run holds.
            let mut newer = model.clone();
            for i in (0..3_000u32).step_by(7) {
                let (key, value) = (i.to_be_bytes().to_vec(), format!("newer {i}"));
                recovered.ks().put(&key, value.as_bytes()).unwrap();
                newer.insert(key, value.into_bytes());
            }
            for i in (1..3_000u32).step_by(11) {
                recovered.ks().delete(&i.to_be_bytes()).unwrap();
                newer.remove(&i.to_be_bytes()[..]);
            }
            recovered.flush().unwrap();
            assert!(recovered.engine_stats().max_overlap > 1);
            assert_eq!(contents(&recovered, b""), newer, "{written} as {read}");
            assert_eq!(
                get(&recovered, &7u32.to_be_bytes()),
                Some(b"newer 7".to_vec())
            );
            assert_eq!(get(&recovered, &12u32.to_be_bytes()), None);
            assert_eq!(block_of(&recovered, 4), block, "{written} as {read}");

            // A checkpoint preserves all existing block lengths, including the
            // older run under the newer runs.
            let again = Scratch::new(&format!("block-length-{written}-{read}"));
            recovered.checkpoint_to(&again.0).unwrap();
            let reopened = Engine::open(&again.0, with(written)).unwrap();
            assert_eq!(contents(&reopened, b""), newer, "{written} as {read}");
            assert!(within(block_of(&reopened, 4), read), "{written} as {read}");

            // Compaction rewrites the merged runs in the configured length;
            // a snapshot keeps reading the runs it replaced.
            let snapshot = recovered.snapshot();
            compact_all(&recovered);
            assert_eq!(recovered.engine_stats().max_overlap, 1);
            assert_eq!(contents(&recovered, b""), newer, "{written} as {read}");
            assert!(within(block_of(&recovered, 4), read), "{written} as {read}");
            let seen = snapshot_contents(&snapshot, &recovered.ks(), b"");
            assert_eq!(seen, newer, "snapshot, {written} as {read}");
        }
    }
    assert!(blocks[0] > blocks[1] && blocks[1] > blocks[2], "{blocks:?}");

    // Lengths above the largest the engine builds are refused, not
    // adjusted; the largest target still caps ordinary raw blocks at 64 KiB.
    let refused = Engine::in_memory(with((64 << 10) + 1)).unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::Config);
    assert!(
        refused
            .to_string()
            .starts_with("Options::block_size is 65537 but must be from 1 to 65536"),
        "{refused}"
    );
    let engine = Engine::in_memory(with(64 << 10)).unwrap();
    for i in 0..3_000u32 {
        let value = format!("value {} of {i}", i % 10);
        engine.ks().put(&i.to_be_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();
    let block = block_of(&engine, 2_999);
    assert!(within(block, 64 << 10) && block > 32 << 10, "{block}");
}

/// A JSON-like value of about 300 to 900 bytes, the same for the same
/// `(index, version)`.
fn profile(index: u32, version: u32) -> Vec<u8> {
    const WORDS: [&str; 16] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
    ];
    let mut state = (u64::from(index) + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ u64::from(version);
    let mut next = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let bio: Vec<&str> = (0..20 + next(100))
        .map(|_| WORDS[next(16) as usize])
        .collect();
    format!(
        r#"{{"id":{index},"version":{version},"name":"{} {}","active":{},"score":{},"plan":"{}","created":"2026-{:02}-{:02}","bio":"{}"}}"#,
        WORDS[next(16) as usize],
        WORDS[next(16) as usize],
        next(2) == 0,
        next(100_000),
        ["free", "pro", "team"][next(3) as usize],
        1 + next(12),
        1 + next(28),
        bio.join(" "),
    )
    .into_bytes()
}

/// Compacts until one run covers each key, returning the report of every
/// job that rewrote runs (moves rewrite nothing and are left out).
fn compact_reports(engine: &Engine) -> Vec<crate::engine::CompactionReport> {
    let options = CompactionPolicy {
        max_overlap: 1,
        ..CompactionPolicy::default()
    };
    let mut workspace = MergeWorkspace::new();
    let mut reports = Vec::new();
    while let Some(report) = engine.core.compact(options, &mut workspace).unwrap() {
        if report.kind != crate::engine::CompactionKind::Move {
            reports.push(report);
        }
    }
    reports
}

/// The manifest's exact run payload boundaries, excluding persisted
/// filters: the run entries follow the 88-byte header and the keyspace table.
fn run_payloads(dir: &Path) -> Vec<(PathBuf, usize)> {
    let current = fs::read(dir.join("CURRENT")).unwrap();
    let generation = u64::from_le_bytes(current[8..16].try_into().unwrap());
    let manifest = fs::read(dir.join(format!("ckpt-{generation:020}/MANIFEST"))).unwrap();
    let u32_at = |at: usize| u32::from_le_bytes(manifest[at..at + 4].try_into().unwrap()) as usize;
    let (table, count) = (u32_at(64), u32_at(68));
    (0..count)
        .map(|i| {
            let at = 88 + table + i * 72;
            let id = u128::from_le_bytes(manifest[at..at + 16].try_into().unwrap());
            let len = u64::from_le_bytes(manifest[at + 52..at + 60].try_into().unwrap()) as usize;
            (dir.join("runs").join(format!("{id:032x}")), len)
        })
        .collect()
}

fn run_versions(dir: &Path) -> Vec<u32> {
    run_payloads(dir)
        .into_iter()
        .map(|(path, len)| {
            let bytes = fs::read(path).unwrap();
            u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap())
        })
        .collect()
}

/// Dictionary runs through the engine: compaction builds them on
/// structured values and reads equal the model; a checkpoint writes them
/// (version 1) and they recover under every compression setting, beside
/// newer plain runs; a snapshot keeps reading dictionary runs that a later
/// compaction replaced.
#[test]
fn dictionary_runs_recover_mix_and_are_rewritten() {
    let with = |compression| Options {
        block_size: 4 << 10,
        compression,
        ..manual()
    };
    let engine = Engine::in_memory(with(Compression::Zstd)).unwrap();
    let mut model = Model::new();
    let put = |engine: &Engine, model: &mut Model, i: u32, version: u32| {
        let (key, value) = (format!("user/{i:06}").into_bytes(), profile(i, version));
        engine.ks().put(&key, &value).unwrap();
        model.insert(key, value);
    };
    for i in 0..14_000 {
        put(&engine, &mut model, i, 0);
    }
    engine.flush().unwrap();
    // A later flush adds another overlapping dictionary run.
    for i in (0..14_000).step_by(3) {
        put(&engine, &mut model, i, 1);
    }
    for i in (1..14_000).step_by(29) {
        let key = format!("user/{i:06}").into_bytes();
        engine.ks().delete(&key).unwrap();
        model.remove(&key);
    }
    engine.flush().unwrap();
    let plain_stats = engine.engine_stats();
    let reports = compact_reports(&engine);
    assert_eq!(reports.len(), 1);
    assert_eq!(contents(&engine, b""), model);
    assert_eq!(get(&engine, b"user/000003"), Some(profile(3, 1)));
    assert_eq!(get(&engine, b"user/000001"), None);
    let stats = engine.engine_stats();
    // Filters and physical segment metadata are held beside stored bytes.
    assert!(stats.held_bytes > reports[0].output_held_bytes / 2);
    assert_eq!(stats.held_bytes, reports[0].output_held_bytes);
    assert!(stats.run_bytes < plain_stats.run_bytes);

    // Oversized records remain readable and can be compacted.
    for i in (0..14_000).step_by(5) {
        put(&engine, &mut model, i, 2);
    }
    let large = vec![b'x'; 768 << 10];
    engine.ks().put(b"user/large", &large).unwrap();
    model.insert(b"user/large".to_vec(), large);
    engine.flush().unwrap();
    compact_reports(&engine);
    assert_eq!(contents(&engine, b""), model);

    // A snapshot holds the dictionary runs a compaction replaces.
    let snapshot = engine.snapshot();
    let held = model.clone();
    for i in (0..14_000).step_by(7) {
        put(&engine, &mut model, i, 3);
    }
    engine.flush().unwrap();
    let _reports = compact_reports(&engine);
    assert!(engine.engine_stats().retained_held_bytes > 0);
    assert_eq!(snapshot_contents(&snapshot, &engine.ks(), b""), held);
    drop(snapshot);
    assert_eq!(contents(&engine, b""), model);

    // A checkpoint writes the existing version-6 runs unchanged.
    let scratch = Scratch::new("dictionary-runs");
    engine.checkpoint_to(&scratch.0).unwrap();
    let versions = run_versions(&scratch.0);
    assert!(
        !versions.is_empty() && versions.iter().all(|&v| v == 1),
        "{versions:?}"
    );

    for read in [Compression::Zstd, Compression::Uncompressed] {
        let recovered = Engine::open(&scratch.0, with(read)).unwrap();
        assert_eq!(contents(&recovered, b""), model, "{read:?}");
        assert!(recovered.engine_stats().held_bytes > 0);
        // Newer plain runs beside the recovered dictionary runs, including
        // keys whose newest version is only in the older run.
        let mut newer = model.clone();
        for i in (0..14_000).step_by(11) {
            put(&recovered, &mut newer, i, 4);
        }
        recovered.ks().delete(b"user/000002").unwrap();
        newer.remove(&b"user/000002"[..]);
        recovered.flush().unwrap();
        assert!(recovered.engine_stats().max_overlap > 1);
        assert_eq!(contents(&recovered, b""), newer, "{read:?}");
        assert_eq!(get(&recovered, b"user/000011"), Some(profile(11, 4)));
        assert_eq!(
            get(&recovered, b"user/000013"),
            newer.get(&b"user/000013"[..]).cloned()
        );
        assert_eq!(get(&recovered, b"user/000002"), None);
        // Compaction rewrites the mix in the current setting.
        compact_reports(&recovered);
        assert_eq!(contents(&recovered, b""), newer, "{read:?}");
    }
}

/// Small repetitive sensor records also use segment dictionaries correctly.
#[test]
fn segment_dictionaries_round_trip_sensor_records() {
    let engine = Engine::in_memory(Options {
        block_size: 4 << 10,
        compression: Compression::Zstd,
        ..manual()
    })
    .unwrap();
    let reading = |i: u32, version: u32| {
        let mut value = (1_700_000_000_000u64 + u64::from(i) * 250 + u64::from(version))
            .to_be_bytes()
            .to_vec();
        value.extend_from_slice(&(i % 1_000).to_be_bytes());
        value.extend_from_slice(&u64::from(i / 64).to_be_bytes());
        value.extend_from_slice(&[
            (i % 4) as u8,
            version as u8,
            (i.wrapping_mul(2_654_435_761) >> 24) as u8,
        ]);
        value
    };
    for version in 0..2 {
        for i in 0..160_000u32 {
            engine
                .ks()
                .put(&i.to_be_bytes(), &reading(i, version))
                .unwrap();
        }
        engine.flush().unwrap();
    }
    let reports = compact_reports(&engine);
    assert_eq!(
        engine.engine_stats().held_bytes,
        reports[0].output_held_bytes
    );
    assert_eq!(get(&engine, &7u32.to_be_bytes()), Some(reading(7, 1)));
}

#[test]
fn opening_a_directory_without_a_checkpoint_starts_empty_and_an_empty_engine_recovers() {
    let scratch = Scratch::new("empty");
    // A missing directory is created; nothing is written until a checkpoint.
    let engine = Engine::open(&scratch.0, manual()).unwrap();
    assert!(engine.keyspaces().is_empty());
    assert_eq!(engine.metadata(), None);
    engine.ks().put(b"a", b"1").unwrap();
    engine.close().unwrap();
    assert_eq!(checkpoint_dirs(&scratch.0), Vec::<String>::new());
    let engine = Engine::open(&scratch.0, manual()).unwrap();
    assert!(engine.keyspaces().is_empty());
    engine.close().unwrap();
    let engine = Engine::in_memory(Options {
        last_seq: 41,
        ..manual()
    })
    .unwrap();
    let report = engine.checkpoint_to(&scratch.0).unwrap();
    assert_eq!((report.records, report.runs, report.last_seq), (0, 0, 41));
    let recovered = Engine::open(&scratch.0, manual()).unwrap();
    assert!(recovered.keyspaces().is_empty());
    assert_eq!(recovered.ks().put_seq(b"a", b"1").unwrap(), 42);
}

/// Publishes all pending memtables.
fn flush(engine: &Engine) {
    engine.flush().unwrap();
}

/// Writes, flushes and compactions continue during the checkpoint, which
/// holds exactly the writes up to its ceiling.
#[test]
fn a_checkpoint_beside_writes_flushes_and_compactions_holds_one_state() {
    let scratch = Scratch::new("concurrent");
    let engine = Engine::in_memory(Options {
        memtable_size: 256 << 10,
        target_run_len: 64 << 10,
        background_compaction: Some(CompactionPolicy::default()),
        ..manual()
    })
    .unwrap();
    let done = AtomicBool::new(false);
    let mut log: Vec<(u64, Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    let mut reports = Vec::new();
    thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut log = Vec::new();
            let mut i = 0u64;
            while !done.load(Ordering::Relaxed) || i < 50_000 {
                let key = (i.wrapping_mul(0x9E37_79B9) % 5_000).to_be_bytes().to_vec();
                let seq = if i % 5 == 4 {
                    let seq = engine.ks().delete_seq(&key);
                    seq.map(|seq| (seq, None))
                } else {
                    let value = i.to_le_bytes().to_vec();
                    engine
                        .ks()
                        .put_seq(&key, &value)
                        .map(|seq| (seq, Some(value)))
                };
                match seq {
                    Ok((seq, value)) => log.push((seq, key, value)),
                    Err(_) => {
                        flush(&engine);
                        continue;
                    }
                }
                if i.is_multiple_of(2_000) {
                    flush(&engine);
                }
                i += 1;
            }
            log
        });
        for _ in 0..3 {
            reports.push(engine.checkpoint_to(&scratch.0).unwrap());
        }
        done.store(true, Ordering::Relaxed);
        log = writer.join().unwrap();
    });
    let last = reports.last().unwrap();
    let mut model = Model::new();
    for (seq, key, value) in &log {
        if *seq > last.last_seq {
            continue;
        }
        match value {
            Some(value) => model.insert(key.clone(), value.clone()),
            None => model.remove(key),
        };
    }
    let recovered = Engine::open(&scratch.0, manual()).unwrap();
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(recovered.engine_stats().last_seq, last.last_seq);
    assert!(
        reports
            .windows(2)
            .all(|pair| pair[0].last_seq <= pair[1].last_seq)
    );
    assert_eq!(checkpoint_dirs(&scratch.0).len(), 4);
}

#[test]
fn damaged_or_missing_files_are_rejected_and_never_replaced_by_an_empty_engine() {
    let scratch = Scratch::new("damage");
    let (engine, _) = varied_engine();
    engine.checkpoint_to(&scratch.0).unwrap();
    let ckpt = scratch.0.join("ckpt-00000000000000000001");
    let pristine: Vec<(PathBuf, Vec<u8>)> = fs::read_dir(&ckpt)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .chain(run_payloads(&scratch.0).into_iter().map(|(path, _)| {
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        }))
        .chain([(
            scratch.0.join("CURRENT"),
            fs::read(scratch.0.join("CURRENT")).unwrap(),
        )])
        .collect();
    let restore = || {
        for (path, bytes) in &pristine {
            fs::write(path, bytes).unwrap();
        }
    };
    let damage = |path: &Path| damage_at(&scratch.0, path);
    let run = run_payloads(&scratch.0)[1].0.clone();
    let manifest = ckpt.join("MANIFEST");
    let current = scratch.0.join("CURRENT");

    let mut bytes = fs::read(&run).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x10;
    fs::write(&run, &bytes).unwrap();
    assert_eq!(damage(&run), Damage::Checksum);
    restore();

    // A filter has its own checksum, checked before it is trusted.
    let filter_at = run_payloads(&scratch.0)[1].1;
    let mut filter_damage = fs::read(&run).unwrap();
    filter_damage[filter_at] ^= 1;
    fs::write(&run, &filter_damage).unwrap();
    assert_eq!(damage(&run), Damage::FilterChecksum);
    restore();

    fs::write(&run, &bytes[..middle]).unwrap();
    assert!(matches!(damage(&run), Damage::Length { .. }));
    restore();

    fs::remove_file(&run).unwrap();
    assert_eq!(damage(&run), Damage::Missing);
    restore();

    let bytes = fs::read(&manifest).unwrap();
    fs::write(&manifest, &bytes[..bytes.len() - 3]).unwrap();
    assert!(matches!(damage(&manifest), Damage::Length { .. }));
    let mut flipped = bytes.clone();
    flipped[30] ^= 1;
    fs::write(&manifest, &flipped).unwrap();
    assert_eq!(damage(&manifest), Damage::Checksum);
    fs::remove_file(&manifest).unwrap();
    assert_eq!(damage(&manifest), Damage::Missing);
    restore();

    fs::write(&current, b"not a checkpoint").unwrap();
    assert!(matches!(damage(&current), Damage::Length { .. }));
    let mut bytes = pristine.last().unwrap().1.clone();
    bytes[0] ^= 1;
    fs::write(&current, &bytes).unwrap();
    assert_eq!(damage(&current), Damage::BadMagic);
    restore();

    // Recovery changed nothing, and the restored checkpoint recovers.
    Engine::open(&scratch.0, manual()).unwrap();
}

#[test]
fn a_failed_checkpoint_leaves_the_engine_and_the_previous_checkpoint() {
    let scratch = Scratch::new("failed");
    let (engine, model) = varied_engine();
    let first = engine.checkpoint_to(&scratch.0).unwrap();
    engine.ks().put(b"later", b"x").unwrap();
    let before = engine.engine_stats();

    // A file where the run directory belongs fails the next checkpoint.
    let runs = scratch.0.join("runs");
    let aside = scratch.0.join("runs-aside");
    fs::rename(&runs, &aside).unwrap();
    fs::write(&runs, b"not a directory").unwrap();
    let error = engine.checkpoint_to(&scratch.0).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io, "{error}");
    assert!(!error.checkpoint_committed());
    fs::remove_file(&runs).unwrap();
    fs::rename(&aside, &runs).unwrap();
    assert_eq!(engine.engine_stats().last_seq, before.last_seq);
    assert_eq!(get(&engine, b"later"), Some(b"x".to_vec()));
    let recovered = Engine::open(&scratch.0, manual()).unwrap();
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(recovered.engine_stats().last_seq, first.last_seq);
    drop(recovered);

    // The next checkpoint takes a new generation and succeeds.
    let next = engine.checkpoint_to(&scratch.0).unwrap();
    assert!(next.generation > first.generation);
    assert_eq!(
        get(&Engine::open(&scratch.0, manual()).unwrap(), b"later"),
        Some(b"x".to_vec())
    );
}

/// A file longer than its manifest entry or format allows is refused before
/// it is read into memory: here sparse files far larger than the memory the
/// tests run in.
#[test]
fn oversized_files_are_refused_before_they_are_read() {
    let scratch = Scratch::new("oversized");
    let (engine, _) = varied_engine();
    engine.checkpoint_to(&scratch.0).unwrap();
    let generation = scratch.0.join(format!("ckpt-{:020}", 1));
    for path in [
        scratch.0.join("CURRENT"),
        generation.join("MANIFEST"),
        run_payloads(&scratch.0)[0].0.clone(),
    ] {
        let original = fs::read(&path).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(64 << 30)
            .unwrap();
        let damage = damage_at(&scratch.0, &path);
        assert!(
            matches!(
                damage,
                Damage::Length {
                    found: 68_719_476_736,
                    ..
                }
            ),
            "{}: {damage:?}",
            path.display()
        );
        fs::write(&path, original).unwrap();
    }
    Engine::open(&scratch.0, manual()).unwrap();
}

/// Closing an engine recovered from a checkpoint writes no new one: what
/// was written after recovery is gone at the next recovery, and the lock
/// is free.
#[test]
fn closing_a_recovered_engine_takes_no_checkpoint() {
    let scratch = Scratch::new("close-recovered");
    let engine = Engine::in_memory(Options::default()).unwrap();
    engine.ks().put(b"kept", b"1").unwrap();
    engine.checkpoint_to(&scratch.0).unwrap();
    engine.close().unwrap();

    let engine = Engine::open(&scratch.0, Options::default()).unwrap();
    engine.ks().put(b"lost", b"2").unwrap();
    engine.close().unwrap();
    let engine = Engine::open(&scratch.0, Options::default()).unwrap();
    assert!(engine.ks().get(b"kept").unwrap().is_some());
    assert!(engine.ks().get(b"lost").unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn multi_segment_checkpoint_streams_one_run_and_recovers() {
    for compression in [Compression::Zstd, Compression::Uncompressed] {
        let options = Options {
            compression,
            ..manual()
        };
        let scratch = Scratch::new(&format!("multi-segment-{compression:?}"));
        let engine = Engine::in_memory(options).unwrap();
        for i in 0..9_000u32 {
            engine
                .ks()
                .put(&i.to_be_bytes(), &vec![(i % 19) as u8; 1024])
                .unwrap();
        }
        let report = engine.checkpoint_to(&scratch.0).unwrap();
        assert_eq!((report.runs, report.records), (1, 9_000));
        assert_eq!(run_versions(&scratch.0), vec![1]);
        let recovered = Engine::open(&scratch.0, options).unwrap();
        let stats = recovered.engine_stats();
        assert_eq!(stats.runs, 1);
        if compression == Compression::Uncompressed {
            assert!(stats.pool_in_use_bytes >= 1 << 20);
        }
        assert!(stats.pool_reserve_bytes <= stats.pool_reserve_limit);
        assert_eq!(stats.pool_reserve_limit, 32 << 20);
        let mut reader = recovered.reader();
        let ks = recovered.ks();
        let mut scan = reader.range(&ks, ..);
        for i in 0..9_000u32 {
            let (key, value) = scan.next().unwrap().unwrap();
            assert_eq!(key, i.to_be_bytes());
            assert_eq!(value, vec![(i % 19) as u8; 1024]);
        }
        assert!(scan.next().unwrap().is_none());
        drop(scan);
        drop(reader);
        for i in [0u32, 4_100, 8_999] {
            assert_eq!(
                get(&recovered, &i.to_be_bytes()),
                Some(vec![(i % 19) as u8; 1024])
            );
        }
    }
}

#[test]
fn checkpoints_preserve_bottom_runs_without_filters() {
    for compression in [Compression::Uncompressed, Compression::Zstd] {
        let dir = Scratch::new(&format!("bottom-no-filter-{compression:?}"));
        let options = Options {
            compression,
            ..manual()
        };
        let engine = Engine::in_memory(options).unwrap();
        for version in [b"old", b"new"] {
            for key in [b"a", b"c", b"e"] {
                engine.ks().put(key, version).unwrap();
            }
            engine.flush().unwrap();
        }
        assert!(engine.engine_stats().filter_bytes > 0);
        compact_all(&engine);
        assert_eq!(engine.engine_stats().filter_bytes, 0);
        assert!(
            engine
                .core
                .read_state()
                .only()
                .runs
                .iter()
                .all(|run| run.run.filter().is_none())
        );
        let ks = engine.ks();
        let mut reader = engine.reader();
        assert_eq!(reader.get(&ks, b"c").unwrap().unwrap(), b"new");
        assert!(reader.get(&ks, b"b").unwrap().is_none());
        assert_eq!(reader.counters().blocks_loaded, 2);
        assert!(reader.counters().decoded_bytes > 0);
        drop(reader);
        engine.checkpoint_to(&dir.0).unwrap();
        let recovered = Engine::open(&dir.0, options).unwrap();
        assert_eq!(recovered.engine_stats().filter_bytes, 0);
        assert!(
            recovered
                .core
                .read_state()
                .only()
                .runs
                .iter()
                .all(|run| run.run.filter().is_none())
        );
        assert_eq!(get(&recovered, b"c").unwrap(), b"new");
        assert_eq!(get(&recovered, b"b"), None);
        // A new level-0 flush still receives a filter beside an unfiltered bottom.
        recovered.ks().put(b"c", b"latest").unwrap();
        recovered.flush().unwrap();
        assert!(recovered.engine_stats().filter_bytes > 0);
    }
}
