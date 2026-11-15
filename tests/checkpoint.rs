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

use tessera::engine::{
    CheckpointError, CheckpointOptions, CompactionError, CompactionOptions, Compression, Damage,
    Engine, EngineOptions, EngineWorkspace, MergeWorkspace, OpenError, Snapshot, SnapshotLimits,
};

/// The defaults without the background worker: these tests flush and
/// compact by hand and count what each step leaves.
fn manual() -> EngineOptions {
    EngineOptions {
        background_compaction: None,
        ..EngineOptions::default()
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
    let mut workspace = EngineWorkspace::new();
    let guard = engine.read();
    let mut scan = guard.scan_prefix(prefix, &mut workspace);
    let mut out = Model::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.insert(record.key.to_vec(), record.value.to_vec());
    }
    out
}

fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    let mut workspace = EngineWorkspace::new();
    engine
        .get(key, &mut workspace)
        .unwrap()
        .map(|found| found.value.to_vec())
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
    let options = CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    while engine.compact(options, &mut workspace).unwrap().is_some() {}
}

/// State in runs, in a frozen table left by a refused flush, and in the
/// active table, with binary and empty keys, a large value, and deletes that
/// hide older values in runs.
fn varied_engine() -> (Engine, Model) {
    let engine = Engine::new(EngineOptions {
        target_run_len: 4096,
        ..manual()
    })
    .unwrap();
    let mut model = Model::new();
    let mut put = |key: Vec<u8>, value: Vec<u8>| {
        engine.put(&key, &value).unwrap();
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
        engine.delete(&i.to_be_bytes()).unwrap();
        model.remove(&i.to_be_bytes()[..]);
    }
    engine.flush().unwrap();
    for i in (1..2_000u32).step_by(7) {
        let (key, value) = (i.to_be_bytes().to_vec(), b"newer".to_vec());
        engine.put(&key, &value).unwrap();
        model.insert(key, value);
    }
    engine.delete(&[0xFF; 40]).unwrap();
    model.remove(&[0xFF; 40][..]);
    (engine, model)
}

#[test]
fn recovery_reproduces_the_checkpointed_state_and_continues_its_sequence() {
    let scratch = Scratch::new("round-trip");
    let (engine, model) = varied_engine();
    let report = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert_eq!(report.last_seq, engine.stats().last_seq);
    assert_eq!(report.records as usize, model.len());
    assert!(report.runs > 1, "split at the target run length");
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000001"]
    );

    let recovered = Engine::recover(&scratch.0, manual()).unwrap();
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
    let stats = recovered.stats();
    assert_eq!(stats.last_seq, report.last_seq);
    assert!(stats.held_bytes > 0, "filters are rebuilt");
    assert_eq!(recovered.put(b"next", b"1").unwrap(), report.last_seq + 1);

    // A larger configured sequence number wins; a smaller one never does.
    let ahead = Engine::recover(
        &scratch.0,
        EngineOptions {
            last_seq: report.last_seq + 100,
            ..manual()
        },
    )
    .unwrap();
    assert_eq!(ahead.put(b"k", b"v").unwrap(), report.last_seq + 101);

    // The recovered engine flushes, compacts and checkpoints like any other.
    recovered.delete(&4u32.to_be_bytes()).unwrap();
    recovered.flush().unwrap();
    compact_all(&recovered);
    let second = recovered
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert_eq!((second.generation, second.reclaimed), (2, 1));
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000002"]
    );
    let again = Engine::recover(&scratch.0, manual()).unwrap();
    assert_eq!(contents(&again, b""), contents(&recovered, b""));
    assert_eq!(get(&again, &4u32.to_be_bytes()), None);
}

/// Each block records its codec, so the configured compression decides only
/// what new runs use: a checkpoint written under one setting recovers under
/// any other, and compaction rewrites what it merges.
#[test]
fn checkpoints_recover_under_any_compression() {
    const ALL: [Compression; 2] = [Compression::Zstd, Compression::Uncompressed];
    let with = |compression| EngineOptions {
        target_run_len: 1 << 20,
        compression,
        ..manual()
    };
    let mut run_bytes = Vec::new();
    for written in ALL {
        let scratch = Scratch::new(&format!("compression-{written:?}"));
        let engine = Engine::new(with(written)).unwrap();
        let mut model = Model::new();
        for i in 0..3_000u32 {
            let (key, value) = (i.to_be_bytes().to_vec(), format!("value {} of {i}", i % 10));
            engine.put(&key, value.as_bytes()).unwrap();
            model.insert(key, value.into_bytes());
        }
        engine.flush().unwrap();
        for i in (0..3_000u32).step_by(3) {
            engine.delete(&i.to_be_bytes()).unwrap();
            model.remove(&i.to_be_bytes()[..]);
        }
        engine.flush().unwrap();
        assert_eq!(contents(&engine, b""), model);
        run_bytes.push(engine.stats().run_bytes);
        engine
            .checkpoint(&scratch.0, CheckpointOptions::default())
            .unwrap();

        for read in ALL {
            let recovered = Engine::recover(&scratch.0, with(read)).unwrap();
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
                recovered.put(&key, value.as_bytes()).unwrap();
                newer.insert(key, value.into_bytes());
            }
            recovered.delete(&2u32.to_be_bytes()).unwrap();
            newer.remove(&2u32.to_be_bytes()[..]);
            recovered.flush().unwrap();
            assert!(recovered.stats().runs > 1);
            // The newer run decides overwritten and deleted keys alone: the
            // recovered run holding the older version is not decoded.
            for (key, expected) in [(4u32, newer.get(&4u32.to_be_bytes()[..])), (2, None)] {
                let mut workspace = EngineWorkspace::new();
                let found = recovered
                    .get(&key.to_be_bytes(), &mut workspace)
                    .unwrap()
                    .map(|found| found.value.to_vec());
                assert_eq!(found.as_ref(), expected, "{written:?} as {read:?}");
                assert_eq!(workspace.blocks_loaded(), 1, "key {key}");
            }
            let snapshot = recovered.snapshot().unwrap();
            // Compaction writes the merged runs with the configured codec;
            // the snapshot keeps reading the runs it replaced.
            compact_all(&recovered);
            assert_eq!(contents(&recovered, b""), newer, "{written:?} as {read:?}");
            let mut workspace = EngineWorkspace::new();
            let mut scan = snapshot.scan_prefix(b"", &mut workspace);
            let mut seen = Model::new();
            while let Some(record) = scan.next_record().unwrap() {
                seen.insert(record.key.to_vec(), record.value.to_vec());
            }
            assert_eq!(seen, newer, "snapshot, {written:?} as {read:?}");
        }
    }
    assert!(run_bytes[0] < run_bytes[1], "{run_bytes:?}");
}

/// Like the codec, the block length decides only what new runs use: each
/// block records its length, so runs of every length are read side by side
/// and recover under any setting, and compactions and checkpoints rewrite
/// what they write in the current one.
#[test]
fn runs_of_any_block_length_recover_mix_and_are_rewritten() {
    const LENGTHS: [usize; 3] = [16 << 10, 2 << 10, 256];
    let with = |target_block_compressed| EngineOptions {
        target_run_len: 1 << 20,
        target_block_compressed,
        ..manual()
    };
    // The decoded block one lookup leaves in a fresh workspace.
    let block_of = |engine: &Engine, key: u32| {
        let mut workspace = EngineWorkspace::new();
        engine.get(&key.to_be_bytes(), &mut workspace).unwrap();
        assert_eq!(workspace.blocks_loaded(), 1);
        workspace.capacity()
    };
    // Records here are under 64 bytes, so a block passes its target by less.
    let within = |block: usize, _target: usize| block <= 256 << 10;
    let mut blocks = Vec::new();
    for written in LENGTHS {
        let scratch = Scratch::new(&format!("block-length-{written}"));
        let engine = Engine::new(with(written)).unwrap();
        let mut model = Model::new();
        for i in 0..3_000u32 {
            let (key, value) = (i.to_be_bytes().to_vec(), format!("value {} of {i}", i % 10));
            engine.put(&key, value.as_bytes()).unwrap();
            model.insert(key, value.into_bytes());
        }
        engine.flush().unwrap();
        compact_all(&engine);
        let block = block_of(&engine, 4);
        assert!(within(block, written), "{block} for {written}");
        blocks.push(block);
        engine
            .checkpoint(&scratch.0, CheckpointOptions::default())
            .unwrap();

        for read in LENGTHS {
            let recovered = Engine::recover(&scratch.0, with(read)).unwrap();
            assert_eq!(contents(&recovered, b""), model, "{written} as {read}");
            // Recovered runs keep their blocks.
            assert_eq!(block_of(&recovered, 4), block, "{written} as {read}");
            // A newer run in the configured length beside them: overwrites,
            // deletes, and keys only the older run holds.
            let mut newer = model.clone();
            for i in (0..3_000u32).step_by(7) {
                let (key, value) = (i.to_be_bytes().to_vec(), format!("newer {i}"));
                recovered.put(&key, value.as_bytes()).unwrap();
                newer.insert(key, value.into_bytes());
            }
            for i in (1..3_000u32).step_by(11) {
                recovered.delete(&i.to_be_bytes()).unwrap();
                newer.remove(&i.to_be_bytes()[..]);
            }
            recovered.flush().unwrap();
            assert!(recovered.stats().max_overlap > 1);
            assert_eq!(contents(&recovered, b""), newer, "{written} as {read}");
            assert_eq!(
                get(&recovered, &7u32.to_be_bytes()),
                Some(b"newer 7".to_vec())
            );
            assert_eq!(get(&recovered, &12u32.to_be_bytes()), None);
            assert_eq!(block_of(&recovered, 4), block, "{written} as {read}");

            // A checkpoint of the mixed runs writes every run in the current
            // length, whatever the engine it is recovered into is set to.
            let again = Scratch::new(&format!("block-length-{written}-{read}"));
            recovered
                .checkpoint(&again.0, CheckpointOptions::default())
                .unwrap();
            let reopened = Engine::recover(&again.0, with(written)).unwrap();
            assert_eq!(contents(&reopened, b""), newer, "{written} as {read}");
            assert!(within(block_of(&reopened, 4), read), "{written} as {read}");

            // Compaction rewrites the merged runs in the configured length;
            // a snapshot keeps reading the runs it replaced.
            let snapshot = recovered.snapshot().unwrap();
            compact_all(&recovered);
            assert_eq!(recovered.stats().max_overlap, 1);
            assert_eq!(contents(&recovered, b""), newer, "{written} as {read}");
            assert!(within(block_of(&recovered, 4), read), "{written} as {read}");
            let mut workspace = EngineWorkspace::new();
            let mut scan = snapshot.scan_prefix(b"", &mut workspace);
            let mut seen = Model::new();
            while let Some(record) = scan.next_record().unwrap() {
                seen.insert(record.key.to_vec(), record.value.to_vec());
            }
            assert_eq!(seen, newer, "snapshot, {written} as {read}");
        }
    }
    assert!(blocks[0] > blocks[1] && blocks[1] > blocks[2], "{blocks:?}");

    // Lengths above the largest the engine builds are refused, not
    // adjusted; at the largest, these records of about 100 KB are cut into
    // blocks capped at 256 KiB raw.
    let Err(OpenError::Config(refused)) = Engine::new(with((256 << 10) + 1)).map(drop) else {
        panic!("an oversized block length was accepted");
    };
    assert_eq!(
        (refused.setting, refused.max),
        ("EngineOptions::target_block_compressed", 256 << 10)
    );
    let engine = Engine::new(with(256 << 10)).unwrap();
    for i in 0..3_000u32 {
        let value = format!("value {} of {i}", i % 10);
        engine.put(&i.to_be_bytes(), value.as_bytes()).unwrap();
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
fn compact_reports(engine: &Engine) -> Vec<tessera::engine::CompactionReport> {
    let options = CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    let mut reports = Vec::new();
    while let Some(report) = engine.compact(options, &mut workspace).unwrap() {
        if report.kind != tessera::engine::CompactionKind::Move {
            reports.push(report);
        }
    }
    reports
}

/// The format version of every run file in a checkpoint directory tree.
fn run_versions(dir: &Path) -> Vec<u32> {
    let mut versions = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            versions.extend(run_versions(&path));
        } else if path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("run-")
        {
            let bytes = fs::read(&path).unwrap();
            let at = bytes.len() - 8;
            versions.push(u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()));
        }
    }
    versions
}

/// Dictionary runs through the engine: compaction builds them on
/// structured values and reads equal the model; a checkpoint writes them
/// (version 5) and they recover under every compression setting, beside
/// newer plain runs; a snapshot keeps reading dictionary runs that a later
/// compaction replaced; and a compaction refused by its job limits
/// publishes nothing.
#[test]
fn dictionary_runs_recover_mix_and_are_rewritten() {
    let with = |compression| EngineOptions {
        target_block_compressed: 4 << 10,
        compression,
        ..manual()
    };
    let engine = Engine::new(with(Compression::Zstd)).unwrap();
    let mut model = Model::new();
    let put = |engine: &Engine, model: &mut Model, i: u32, version: u32| {
        let (key, value) = (format!("user/{i:06}").into_bytes(), profile(i, version));
        engine.put(&key, &value).unwrap();
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
        engine.delete(&key).unwrap();
        model.remove(&key);
    }
    engine.flush().unwrap();
    let plain_stats = engine.stats();
    let reports = compact_reports(&engine);
    assert_eq!(reports.len(), 1);
    assert_eq!(contents(&engine, b""), model);
    assert_eq!(get(&engine, b"user/000003"), Some(profile(3, 1)));
    assert_eq!(get(&engine, b"user/000001"), None);
    let stats = engine.stats();
    // Filters and physical segment metadata are held beside stored bytes.
    assert!(stats.held_bytes > reports[0].output_held_bytes / 2);
    assert_eq!(stats.held_bytes, reports[0].output_held_bytes);
    assert!(stats.run_bytes < plain_stats.run_bytes);

    // A refused compaction publishes nothing and keeps every read. A record
    // eight blocks long gets a block of its own, so the job over its flush
    // needs more workspace than the smallest limit allowed, which admits
    // two raw-cap input blocks.
    for i in (0..14_000).step_by(5) {
        put(&engine, &mut model, i, 2);
    }
    let large = vec![b'x'; 768 << 10];
    engine.put(b"user/large", &large).unwrap();
    model.insert(b"user/large".to_vec(), large);
    engine.flush().unwrap();
    let before = engine.stats();
    let narrowest = CompactionOptions {
        max_overlap: 1,
        max_workspace_bytes: 0,
        ..CompactionOptions::default()
    };
    let Err(CompactionError::Config(config)) =
        engine.compact(narrowest, &mut MergeWorkspace::new())
    else {
        panic!("a workspace limit of 0 is refused");
    };
    let refused = CompactionOptions {
        max_workspace_bytes: config.min as usize,
        ..narrowest
    };
    let error = engine
        .compact(refused, &mut MergeWorkspace::new())
        .unwrap_err();
    assert!(
        matches!(error, CompactionError::JobLimit { inputs: 2, .. }),
        "{error:?}"
    );
    let after = engine.stats();
    assert_eq!(
        (after.runs, after.run_bytes),
        (before.runs, before.run_bytes)
    );
    assert_eq!(contents(&engine, b""), model);

    // A snapshot holds the dictionary runs a compaction replaces.
    let snapshot = engine.snapshot().unwrap();
    let held = model.clone();
    for i in (0..14_000).step_by(7) {
        put(&engine, &mut model, i, 3);
    }
    engine.flush().unwrap();
    let _reports = compact_reports(&engine);
    assert!(engine.stats().retained_held_bytes > 0);
    let mut workspace = EngineWorkspace::new();
    let mut scan = snapshot.scan_prefix(b"", &mut workspace);
    let mut seen = Model::new();
    while let Some(record) = scan.next_record().unwrap() {
        seen.insert(record.key.to_vec(), record.value.to_vec());
    }
    drop(scan);
    assert_eq!(seen, held);
    drop(snapshot);
    assert_eq!(contents(&engine, b""), model);

    // A checkpoint samples its output segments and writes version 5.
    let scratch = Scratch::new("dictionary-runs");
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    let versions = run_versions(&scratch.0);
    assert!(
        !versions.is_empty() && versions.iter().all(|&v| v == 5),
        "{versions:?}"
    );

    for read in [Compression::Zstd, Compression::Uncompressed] {
        let recovered = Engine::recover(&scratch.0, with(read)).unwrap();
        assert_eq!(contents(&recovered, b""), model, "{read:?}");
        assert!(recovered.stats().held_bytes > 0);
        // Newer plain runs beside the recovered dictionary runs, including
        // keys whose newest version is only in the older run.
        let mut newer = model.clone();
        for i in (0..14_000).step_by(11) {
            put(&recovered, &mut newer, i, 4);
        }
        recovered.delete(b"user/000002").unwrap();
        newer.remove(&b"user/000002"[..]);
        recovered.flush().unwrap();
        assert!(recovered.stats().max_overlap > 1);
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
    let engine = Engine::new(EngineOptions {
        target_block_compressed: 4 << 10,
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
            engine.put(&i.to_be_bytes(), &reading(i, version)).unwrap();
        }
        engine.flush().unwrap();
    }
    let reports = compact_reports(&engine);
    assert_eq!(engine.stats().held_bytes, reports[0].output_held_bytes);
    assert_eq!(get(&engine, &7u32.to_be_bytes()), Some(reading(7, 1)));
}

/// After a switch to a less compact codec, a compaction that would grow the
/// runs past `max_run_bytes` is refused and leaves the runs, their contents
/// and a snapshot's view as they were; lifting the limit lets it through.
#[test]
fn a_compaction_refused_after_a_codec_switch_changes_nothing() {
    let scratch = Scratch::new("codec-switch-limit");
    let with = |compression| EngineOptions {
        target_run_len: 128 << 10,
        compression,
        ..manual()
    };
    let engine = Engine::new(with(Compression::Zstd)).unwrap();
    let mut model = Model::new();
    for i in 0..3_000u32 {
        let (key, value) = (
            i.to_be_bytes().to_vec(),
            format!("value {} of {i} {}", i % 10, "x".repeat(1024)),
        );
        engine.put(&key, value.as_bytes()).unwrap();
        model.insert(key, value.into_bytes());
    }
    engine.flush().unwrap();
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();

    // Recovered Zstandard runs under a newer uncompressed run of overwrites.
    let recovered = Engine::recover(&scratch.0, with(Compression::Uncompressed)).unwrap();
    for i in (0..3_000u32).step_by(5) {
        let (key, value) = (
            i.to_be_bytes().to_vec(),
            format!("newer {i} {}", "x".repeat(1024)),
        );
        recovered.put(&key, value.as_bytes()).unwrap();
        model.insert(key, value.into_bytes());
    }
    recovered.flush().unwrap();
    let snapshot = recovered.snapshot().unwrap();
    let before = recovered.stats();
    assert!(before.max_overlap > 1, "{before:?}");
    recovered
        .set_run_limits(usize::MAX, before.run_bytes + before.held_bytes)
        .unwrap();
    let options = CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    let (refused, before_refusal) = loop {
        let before_step = recovered.stats();
        let result = recovered.compact(options, &mut MergeWorkspace::new());
        if matches!(&result, Ok(Some(_))) {
            continue;
        }
        break (result, before_step);
    };
    assert!(
        matches!(refused, Err(CompactionError::RunLimit(_))),
        "{refused:?}"
    );
    let after = recovered.stats();
    assert_eq!(
        (
            after.run_bytes,
            after.held_bytes,
            after.runs,
            after.last_seq
        ),
        (
            before_refusal.run_bytes,
            before_refusal.held_bytes,
            before_refusal.runs,
            before_refusal.last_seq
        )
    );
    assert_eq!(contents(&recovered, b""), model);
    let snapshot_contents = |snapshot: &Snapshot| {
        let mut workspace = EngineWorkspace::new();
        let mut scan = snapshot.scan_prefix(b"", &mut workspace);
        let mut seen = Model::new();
        while let Some(record) = scan.next_record().unwrap() {
            seen.insert(record.key.to_vec(), record.value.to_vec());
        }
        seen
    };
    assert_eq!(snapshot_contents(&snapshot), model);

    recovered.set_run_limits(usize::MAX, usize::MAX).unwrap();
    compact_all(&recovered);
    assert!(recovered.stats().run_bytes > before.run_bytes);
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(snapshot_contents(&snapshot), model);
}

#[test]
fn an_empty_engine_checkpoints_and_recovers_empty_but_nothing_is_not_a_checkpoint() {
    let scratch = Scratch::new("empty");
    assert!(matches!(
        Engine::recover(&scratch.0, manual()),
        Err(OpenError::NoCheckpoint(_))
    ));
    fs::create_dir_all(&scratch.0).unwrap();
    assert!(matches!(
        Engine::recover(&scratch.0, manual()),
        Err(OpenError::NoCheckpoint(_))
    ));
    let engine = Engine::new(EngineOptions {
        last_seq: 41,
        ..manual()
    })
    .unwrap();
    let report = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert_eq!((report.records, report.runs, report.last_seq), (0, 0, 41));
    let recovered = Engine::recover(&scratch.0, manual()).unwrap();
    assert!(contents(&recovered, b"").is_empty());
    assert_eq!(recovered.put(b"a", b"1").unwrap(), 42);
}

/// Flushes, waiting for the worker whenever the run limit refuses.
fn flush(engine: &Engine) {
    for _ in 0..1_000 {
        if engine.flush().is_ok() {
            return;
        }
        engine.wait_for_compaction().unwrap();
    }
    panic!("compaction never made room");
}

/// Writes, flushes and compactions continue during the checkpoint, which
/// holds exactly the writes up to its ceiling.
#[test]
fn a_checkpoint_beside_writes_flushes_and_compactions_holds_one_state() {
    let scratch = Scratch::new("concurrent");
    let engine = Engine::new(EngineOptions {
        memtable_budget: 256 << 10,
        target_run_len: 64 << 10,
        background_compaction: Some(CompactionOptions::default()),
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
                    let seq = engine.delete(&key);
                    seq.map(|seq| (seq, None))
                } else {
                    let value = i.to_le_bytes().to_vec();
                    engine.put(&key, &value).map(|seq| (seq, Some(value)))
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
            reports.push(
                engine
                    .checkpoint(&scratch.0, CheckpointOptions::default())
                    .unwrap(),
            );
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
    let recovered = Engine::recover(&scratch.0, manual()).unwrap();
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(recovered.stats().last_seq, last.last_seq);
    assert!(
        reports
            .windows(2)
            .all(|pair| pair[0].last_seq <= pair[1].last_seq)
    );
    assert_eq!(checkpoint_dirs(&scratch.0).len(), 2);
}

#[test]
fn damaged_or_missing_files_are_rejected_and_never_replaced_by_an_empty_engine() {
    let scratch = Scratch::new("damage");
    let (engine, _) = varied_engine();
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    let ckpt = scratch.0.join("ckpt-00000000000000000001");
    let pristine: Vec<(PathBuf, Vec<u8>)> = fs::read_dir(&ckpt)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
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
    let damage = |path: &Path| match Engine::recover(&scratch.0, manual()) {
        Err(OpenError::Corrupt { path: at, damage }) => {
            assert_eq!(at, path);
            damage
        }
        other => panic!("{:?}", other.map(|_| ())),
    };
    let run = ckpt.join("run-000001");
    let manifest = ckpt.join("MANIFEST");
    let current = scratch.0.join("CURRENT");

    let mut bytes = fs::read(&run).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x10;
    fs::write(&run, &bytes).unwrap();
    assert_eq!(damage(&run), Damage::Checksum);
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
    Engine::recover(&scratch.0, manual()).unwrap();
}

#[test]
fn a_refused_or_failed_checkpoint_leaves_the_engine_and_the_previous_checkpoint() {
    let scratch = Scratch::new("refused");
    let (engine, model) = varied_engine();
    let first = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    engine.put(b"later", b"x").unwrap();
    let before = engine.stats();

    let error = engine
        .checkpoint(&scratch.0, CheckpointOptions { max_bytes: 10_000 })
        .unwrap_err();
    assert!(
        matches!(error, CheckpointError::DiskLimit { bytes, max_bytes: 10_000 } if bytes > 10_000)
    );
    assert_eq!(engine.stats(), before);
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000001"]
    );
    let recovered = Engine::recover(&scratch.0, manual()).unwrap();
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(recovered.stats().last_seq, first.last_seq);

    // Without a snapshot to take, nothing is written.
    let limited = Engine::new(EngineOptions {
        snapshots: SnapshotLimits {
            max_live: 0,
            ..SnapshotLimits::default()
        },
        ..manual()
    })
    .unwrap();
    assert!(matches!(
        limited.checkpoint(&scratch.0, CheckpointOptions::default()),
        Err(CheckpointError::Snapshot(_))
    ));
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000001"]
    );

    // The next checkpoint takes a new generation and succeeds.
    let next = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert!(next.generation > first.generation);
    assert_eq!(
        get(&Engine::recover(&scratch.0, manual()).unwrap(), b"later"),
        Some(b"x".to_vec())
    );
}

#[test]
fn recovery_refuses_more_runs_than_the_options_allow() {
    let scratch = Scratch::new("limits");
    let (engine, _) = varied_engine();
    let report = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    let error = Engine::recover(
        &scratch.0,
        EngineOptions {
            max_runs: report.runs - 1,
            ..manual()
        },
    );
    assert!(matches!(error, Err(OpenError::RunLimit(_))));
    // Refused from the manifest's lengths, before reading a run.
    let error = Engine::recover(
        &scratch.0,
        EngineOptions {
            max_run_bytes: report.largest_run_bytes,
            ..manual()
        },
    );
    assert!(matches!(error, Err(OpenError::RunLimit(_))));
}

/// A file longer than its manifest entry or format allows is refused before
/// it is read into memory: here sparse files far larger than the memory the
/// tests run in.
#[test]
fn oversized_files_are_refused_before_they_are_read() {
    let scratch = Scratch::new("oversized");
    let (engine, _) = varied_engine();
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    let generation = scratch.0.join(format!("ckpt-{:020}", 1));
    for path in [
        scratch.0.join("CURRENT"),
        generation.join("MANIFEST"),
        generation.join("run-000000"),
    ] {
        let original = fs::read(&path).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(64 << 30)
            .unwrap();
        let error = Engine::recover(&scratch.0, manual());
        assert!(
            matches!(
                error,
                Err(OpenError::Corrupt {
                    damage: Damage::Length {
                        found: 68_719_476_736,
                        ..
                    },
                    ..
                })
            ),
            "{}: {error:?}",
            path.display()
        );
        fs::write(&path, original).unwrap();
    }
    Engine::recover(&scratch.0, manual()).unwrap();
}

/// Closing an engine recovered from a checkpoint writes no new one: what
/// was written after recovery is gone at the next recovery, and the lock
/// is free.
#[test]
fn closing_a_recovered_engine_takes_no_checkpoint() {
    let scratch = Scratch::new("close-recovered");
    let engine = Engine::new(EngineOptions::default()).unwrap();
    engine.put(b"kept", b"1").unwrap();
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    engine.close().unwrap();

    let engine = Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
    engine.put(b"lost", b"2").unwrap();
    assert_eq!(engine.close(), Ok(()));
    let engine = Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
    let mut workspace = EngineWorkspace::new();
    assert!(engine.get(b"kept", &mut workspace).unwrap().is_some());
    assert!(engine.get(b"lost", &mut workspace).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn multi_segment_checkpoint_streams_one_v5_run_and_recovers() {
    for compression in [Compression::Zstd, Compression::Uncompressed] {
        let options = EngineOptions {
            compression,
            ..manual()
        };
        let scratch = Scratch::new(&format!("multi-segment-{compression:?}"));
        let engine = Engine::new(options).unwrap();
        for i in 0..9_000u32 {
            engine
                .put(&i.to_be_bytes(), &vec![(i % 19) as u8; 1024])
                .unwrap();
        }
        let report = engine
            .checkpoint(&scratch.0, CheckpointOptions::default())
            .unwrap();
        assert_eq!((report.runs, report.records), (1, 9_000));
        assert_eq!(run_versions(&scratch.0), vec![5]);
        let recovered = Engine::recover(&scratch.0, options).unwrap();
        let stats = recovered.stats();
        assert_eq!(stats.runs, 1);
        assert!(stats.pool_in_use_bytes >= 1 << 20);
        assert_eq!(stats.pool_reserve_bytes, 0);
        assert_eq!(stats.pool_reserve_limit, 64 << 20);
        let mut workspace = EngineWorkspace::new();
        let guard = recovered.read();
        let mut scan = guard.scan(&mut workspace);
        for i in 0..9_000u32 {
            let record = scan.next_record().unwrap().unwrap();
            assert_eq!(record.key, i.to_be_bytes());
            assert_eq!(record.value, vec![(i % 19) as u8; 1024]);
        }
        assert!(scan.next_record().unwrap().is_none());
        drop(scan);
        for i in [0u32, 4_100, 8_999] {
            assert_eq!(
                get(&recovered, &i.to_be_bytes()),
                Some(vec![(i % 19) as u8; 1024])
            );
        }
    }
}
