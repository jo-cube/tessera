//! The integrated engine: visibility across memtables and runs, flush stages,
//! limits, and concurrent use.
//!
//! Expected results come from hash maps maintained by the tests. Concurrency
//! tests use barriers and shared flags and never sleep.

use std::collections::HashMap;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use tessera::engine::{
    CompactionError, CompactionOptions, Compression, ENTRY_OVERHEAD, Engine, EngineOptions,
    EngineStats, EngineWorkspace, MAX_KEY_LEN, MergeWorkspace, OpenError, ReadGuard, RunLimitError,
    SnapshotLimits, UpdateError, WriteError,
};

/// The defaults without the background worker: these tests flush and
/// compact by hand and count what each step leaves.
fn manual() -> EngineOptions {
    EngineOptions {
        background_compaction: None,
        ..EngineOptions::default()
    }
}

type Live = (Vec<u8>, u64, Vec<u8>);

fn engine() -> Engine {
    Engine::new(manual()).unwrap()
}

fn value_of(engine: &Engine, key: &[u8]) -> Option<(u64, Vec<u8>)> {
    let mut workspace = EngineWorkspace::new();
    engine
        .get(key, &mut workspace)
        .unwrap()
        .map(|found| (found.seq, found.value.to_vec()))
}

fn scan_prefix(guard: &ReadGuard<'_>, prefix: &[u8]) -> Vec<Live> {
    let mut workspace = EngineWorkspace::new();
    let mut scan = guard.scan_prefix(prefix, &mut workspace);
    let mut out = Vec::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.push((record.key.to_vec(), record.seq, record.value.to_vec()));
    }
    out
}

fn scanned(engine: &Engine, prefix: &[u8]) -> Vec<Live> {
    scan_prefix(&engine.read(), prefix)
}

fn live(key: &[u8], seq: u64, value: &[u8]) -> Live {
    (key.to_vec(), seq, value.to_vec())
}

fn no_frozen(stats: EngineStats) -> bool {
    stats.frozen.is_none()
}

// Writes and reads.

#[test]
fn an_absent_key_reads_as_none() {
    assert_eq!(value_of(&engine(), b"a"), None);
}

#[test]
fn a_write_is_visible_to_the_next_read_with_its_sequence_number() {
    let engine = engine();
    let seq = engine.put(b"a", b"v").unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
}

#[test]
fn overwrite_delete_and_reinsert_each_take_effect_at_once() {
    let engine = engine();
    engine.put(b"a", b"1").unwrap();
    let seq = engine.put(b"a", b"2").unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"2".to_vec())));
    engine.delete(b"a").unwrap();
    assert_eq!(value_of(&engine, b"a"), None);
    let seq = engine.put(b"a", b"3").unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"3".to_vec())));
}

#[test]
fn an_empty_key_and_empty_value_are_ordinary() {
    let engine = engine();
    let seq = engine.put(b"", b"").unwrap();
    assert_eq!(value_of(&engine, b""), Some((seq, Vec::new())));
    engine.flush().unwrap();
    assert_eq!(value_of(&engine, b""), Some((seq, Vec::new())));
}

#[test]
fn a_reused_workspace_returns_each_value_alone() {
    let engine = engine();
    engine.put(b"long", b"a longer value").unwrap();
    engine.put(b"short", b"x").unwrap();
    let mut workspace = EngineWorkspace::new();
    assert_eq!(
        engine.get(b"long", &mut workspace).unwrap().unwrap().value,
        b"a longer value"
    );
    assert_eq!(
        engine.get(b"short", &mut workspace).unwrap().unwrap().value,
        b"x"
    );
}

// Sequence numbers.

#[test]
fn sequence_numbers_start_after_the_configured_value_and_increase() {
    let engine = Engine::new(EngineOptions {
        last_seq: 41,
        ..manual()
    })
    .unwrap();
    assert_eq!(engine.put(b"a", b"v"), Ok(42));
    assert_eq!(engine.delete(b"a"), Ok(43));
    assert_eq!(engine.stats().last_seq, 43);
}

#[test]
fn sequence_numbers_continue_across_flushes() {
    let engine = engine();
    assert_eq!(engine.put(b"a", b"v"), Ok(1));
    engine.flush().unwrap();
    assert_eq!(engine.put(b"a", b"w"), Ok(2));
    engine.flush().unwrap();
    assert_eq!(engine.delete(b"a"), Ok(3));
    engine.flush().unwrap();
    assert_eq!(engine.put(b"b", b"x"), Ok(4));
}

#[test]
fn a_rejected_write_uses_no_sequence_number_and_changes_nothing() {
    let engine = Engine::new(EngineOptions {
        memtable_budget: 200,
        ..manual()
    })
    .unwrap();
    engine.put(b"a", b"v").unwrap();
    let before = engine.stats();

    let long = vec![b'k'; MAX_KEY_LEN + 1];
    assert_eq!(
        engine.put(&long, b"v"),
        Err(UpdateError::Rejected(WriteError::KeyTooLong {
            len: MAX_KEY_LEN + 1
        }))
    );
    // The configured budget is reported, whatever the active table's.
    assert!(matches!(
        engine.put(b"b", &[0; 300]),
        Err(UpdateError::Rejected(WriteError::RecordExceedsBudget {
            charge,
            budget: 200,
        })) if charge > 300
    ));
    assert_eq!(engine.stats(), before);

    // A write with no room freezes the full table and lands in the next even
    // though the run limits refuse the flush, which leaves the table frozen.
    engine.set_run_limits(1, 1).unwrap();
    assert_eq!(engine.put(b"b", &[0; 40]), Ok(2));
    let before = engine.stats();
    assert_eq!((before.frozen_tables, before.runs), (1, 0));
    // Both budgets are now held, so the next write with no room is refused,
    // from the kept refusal rather than by building the flush again.
    let refused = engine.put(b"c", &[0; 40]);
    assert!(
        matches!(refused, Err(UpdateError::FlushRefused(_))),
        "{refused:?}"
    );
    assert_eq!(engine.put(b"c", &[0; 40]), refused);
    assert_eq!(engine.stats(), before);
    assert_eq!(value_of(&engine, b"c"), None);
    // Raising the limits lets the next write flush both tables.
    engine.set_run_limits(64, usize::MAX).unwrap();
    assert_eq!(engine.put(b"c", &[0; 40]), Ok(3));
    let stats = engine.stats();
    assert_eq!((stats.frozen_tables, stats.runs), (0, 1));
}

#[test]
fn the_last_sequence_number_is_used_once_and_then_writes_stop() {
    let engine = Engine::new(EngineOptions {
        last_seq: u64::MAX - 1,
        ..manual()
    })
    .unwrap();
    assert_eq!(engine.put(b"a", b"v"), Ok(u64::MAX));
    let before = engine.stats();
    assert_eq!(engine.put(b"b", b"v"), Err(UpdateError::SequencesExhausted));
    assert_eq!(engine.delete(b"a"), Err(UpdateError::SequencesExhausted));
    assert_eq!(engine.stats(), before);
    assert_eq!(value_of(&engine, b"a"), Some((u64::MAX, b"v".to_vec())));
    engine.flush().unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((u64::MAX, b"v".to_vec())));
}

#[test]
fn an_engine_after_the_largest_sequence_number_accepts_nothing() {
    let engine = Engine::new(EngineOptions {
        last_seq: u64::MAX,
        ..manual()
    })
    .unwrap();
    assert_eq!(engine.put(b"a", b"v"), Err(UpdateError::SequencesExhausted));
}

// Precedence across sources.

#[test]
fn a_read_of_a_flushed_key_decodes_a_block() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    let mut workspace = EngineWorkspace::new();
    engine.get(b"a", &mut workspace).unwrap();
    assert_eq!(workspace.blocks_loaded(), 0);
    engine.flush().unwrap();
    engine.get(b"a", &mut workspace).unwrap();
    assert_eq!(workspace.blocks_loaded(), 1);
}

#[test]
fn a_newer_table_update_hides_older_runs_without_decoding_them() {
    let engine = engine();
    engine.put(b"a", b"old").unwrap();
    engine.flush().unwrap();
    let seq = engine.put(b"a", b"new").unwrap();
    let mut workspace = EngineWorkspace::new();
    let found = engine.get(b"a", &mut workspace).unwrap().unwrap();
    assert_eq!((found.seq, found.value), (seq, &b"new"[..]));
    assert_eq!(workspace.blocks_loaded(), 0);
}

#[test]
fn the_newest_run_decides_a_key_written_across_several_flushes() {
    let engine = engine();
    engine.put(b"a", b"1").unwrap();
    engine.flush().unwrap();
    let seq = engine.put(b"a", b"2").unwrap();
    engine.flush().unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"2".to_vec())));
    assert_eq!(engine.stats().runs, 2);
}

#[test]
fn a_tombstone_in_the_active_table_hides_a_value_in_a_run() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    engine.flush().unwrap();
    engine.delete(b"a").unwrap();
    assert_eq!(value_of(&engine, b"a"), None);
    assert_eq!(scanned(&engine, b""), []);
}

#[test]
fn a_flushed_tombstone_hides_a_value_in_an_older_run() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    engine.flush().unwrap();
    engine.delete(b"a").unwrap();
    engine.flush().unwrap();
    assert_eq!(value_of(&engine, b"a"), None);
    assert_eq!(scanned(&engine, b""), []);
    let seq = engine.put(b"a", b"back").unwrap();
    assert_eq!(scanned(&engine, b""), [live(b"a", seq, b"back")]);
}

// Flush stages.

#[test]
fn an_empty_flush_creates_nothing() {
    let engine = engine();
    assert_eq!(engine.flush().unwrap().tables, 0);
    let stats = engine.stats();
    assert!(no_frozen(stats));
    assert_eq!(stats.runs, 0);
}

#[test]
fn a_repeated_flush_adds_nothing() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    assert_eq!(engine.flush().unwrap().tables, 1);
    assert_eq!(engine.flush().unwrap().tables, 0);
    assert_eq!(engine.stats().runs, 1);
}

#[test]
fn a_flush_splits_output_at_the_configured_run_length() {
    let engine = Engine::new(EngineOptions {
        target_run_len: 1,
        ..manual()
    })
    .unwrap();
    engine.put(b"a", b"v").unwrap();
    engine.put(b"b", b"v").unwrap();
    assert_eq!(engine.flush().unwrap().runs, 2);
    assert_eq!(scanned(&engine, b"").len(), 2);
}

#[test]
fn a_report_describes_what_was_published() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    engine.delete(b"b").unwrap();
    let report = engine.flush().unwrap();
    let stats = engine.stats();
    assert_eq!((report.tables, report.records, report.runs), (1, 2, 1));
    assert_eq!(report.run_bytes, stats.run_bytes);
    assert!(report.run_bytes > 0);
}

// Run limits.

fn limited(max_runs: usize, max_run_bytes: usize) -> Engine {
    Engine::new(EngineOptions {
        max_runs,
        max_run_bytes,
        ..manual()
    })
    .unwrap()
}

/// What one small flush counts against the run byte limit: its stored bytes
/// and its filter.
fn flushed_size() -> usize {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    let report = engine.flush().unwrap();
    assert!(report.held_bytes > 0);
    report.run_bytes + report.held_bytes
}

#[test]
fn a_flush_over_the_run_limits_is_rejected_and_stays_readable() {
    let engine = limited(1, 1);
    let seq = engine.put(b"a", b"v").unwrap();
    let error = engine.flush().unwrap_err();
    assert_eq!((error.runs, error.max_runs, error.max_run_bytes), (1, 1, 1));
    assert!(error.run_bytes > 1);
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
    let stats = engine.stats();
    assert!(stats.frozen.is_some());
    assert_eq!((stats.runs, stats.run_bytes), (0, 0));
}

#[test]
fn a_flush_at_the_run_count_is_accepted() {
    let engine = limited(1, usize::MAX);
    engine.put(b"a", b"v").unwrap();
    engine.flush().unwrap();
    engine.put(b"b", b"v").unwrap();
    assert_eq!(engine.flush().unwrap_err().runs, 2);
    assert_eq!(engine.stats().runs, 1);
}

#[test]
fn run_bytes_are_limited_inclusively() {
    let size = flushed_size();
    let exact = limited(usize::MAX, size);
    exact.put(b"a", b"v").unwrap();
    exact.flush().unwrap();

    let short = limited(usize::MAX, size - 1);
    short.put(b"a", b"v").unwrap();
    assert_eq!(
        short.flush().unwrap_err(),
        RunLimitError {
            runs: 1,
            max_runs: usize::MAX,
            run_bytes: size,
            max_run_bytes: size - 1,
            level0_flushes: 1,
            max_level0_flushes: usize::MAX,
        }
    );
}

#[test]
fn a_rejected_flush_succeeds_once_the_limits_are_raised() {
    let engine = limited(1, 1);
    let seq = engine.put(b"a", b"v").unwrap();
    engine.flush().unwrap_err();
    engine.set_run_limits(1, usize::MAX).unwrap();
    assert_eq!(engine.flush().unwrap().tables, 1);
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
    assert_eq!(engine.stats().runs, 1);
}

#[test]
fn writes_continue_into_the_active_table_after_a_rejected_flush() {
    let engine = limited(1, 1);
    engine.put(b"a", b"v").unwrap();
    engine.flush().unwrap_err();
    let seq = engine.put(b"b", b"w").unwrap();
    assert_eq!(value_of(&engine, b"b"), Some((seq, b"w".to_vec())));
    assert_eq!(scanned(&engine, b"").len(), 2);
}

// Budgets.

#[test]
fn a_write_to_a_full_table_flushes_it_and_lands_in_the_next() {
    let budget = 1 + 1 + ENTRY_OVERHEAD;
    let engine = Engine::new(EngineOptions {
        memtable_budget: budget,
        ..manual()
    })
    .unwrap();
    let a = engine.put(b"a", b"v").unwrap();
    let b = engine.put(b"b", b"v").unwrap();
    assert_eq!(b, a + 1);
    let stats = engine.stats();
    assert_eq!((stats.runs, stats.frozen_tables), (1, 0));
    assert_eq!(stats.active.charged, budget);
    assert_eq!(
        scanned(&engine, b""),
        [live(b"a", a, b"v"), live(b"b", b, b"v")]
    );
}

#[test]
fn at_most_one_table_is_frozen_while_the_new_one_fills() {
    let budget = 1 + 1 + ENTRY_OVERHEAD;
    let engine = Engine::new(EngineOptions {
        memtable_budget: budget,
        ..manual()
    })
    .unwrap();
    engine.put(b"a", b"v").unwrap();
    // A refused flush leaves its table frozen.
    engine.set_run_limits(1, 1).unwrap();
    engine.flush().unwrap_err();
    engine.put(b"b", b"v").unwrap();
    let refused = engine.put(b"c", b"v").unwrap_err();
    assert!(matches!(refused, UpdateError::FlushRefused(_)), "{refused}");
    let stats = engine.stats();
    assert_eq!(stats.active.charged, budget);
    assert_eq!(stats.frozen.unwrap().charged, budget);
    engine.set_run_limits(usize::MAX, usize::MAX).unwrap();
    engine.put(b"c", b"v").unwrap();
    assert_eq!(engine.stats().frozen, None);
}

// Automatic flushing.

/// Writers that fill the table together share its flushes: every write lands
/// exactly once with its own number, without a gap, and the tables flushed
/// are full ones. Meanwhile each snapshot sees exactly the writes up to its
/// sequence number, and each writer's writes in order, across every freeze
/// and publication.
#[test]
fn concurrent_writers_flush_full_tables_once_and_snapshots_stay_coherent() {
    const WRITERS: u8 = 4;
    const EACH: u32 = 1_500;
    const BUDGET: usize = 128 << 10;
    let engine = Engine::new(EngineOptions {
        memtable_budget: BUDGET,
        target_run_len: 1 << 30,
        ..manual()
    })
    .unwrap();
    let key = |writer: u8, at: u32| [&[writer][..], &at.to_be_bytes()].concat();
    let done = AtomicBool::new(false);
    let (seqs, snapshots) = thread::scope(|scope| {
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let engine = &engine;
                scope.spawn(move || {
                    (0..EACH)
                        .map(|at| engine.put(&key(writer, at), &[writer; 100]).unwrap())
                        .collect::<Vec<u64>>()
                })
            })
            .collect();
        let reader = scope.spawn(|| {
            let mut checked = 0;
            while !done.load(Ordering::Acquire) {
                let snapshot = engine.snapshot().unwrap();
                let mut workspace = EngineWorkspace::new();
                let mut seen = 0;
                for writer in 0..WRITERS {
                    let mut scan = snapshot.scan_prefix(&[writer], &mut workspace);
                    let mut next = 0u32;
                    while let Some(record) = scan.next_record().unwrap() {
                        assert_eq!(record.key, key(writer, next), "a gap in writer {writer}");
                        assert!(record.seq <= snapshot.last_seq());
                        next += 1;
                    }
                    seen += u64::from(next);
                }
                assert_eq!(seen, snapshot.last_seq());
                checked += 1;
            }
            checked
        });
        let seqs: Vec<u64> = writers
            .into_iter()
            .flat_map(|writer| writer.join().unwrap())
            .collect();
        done.store(true, Ordering::Release);
        (seqs, reader.join().unwrap())
    });
    assert!(snapshots > 0);
    let total = u64::from(WRITERS) * u64::from(EACH);
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, (1..=total).collect::<Vec<_>>());
    let stats = engine.stats();
    assert_eq!(stats.last_seq, total);
    // Each flush took a full table, at least half of it records (the rest
    // history the snapshots pinned): not one flush per writer that found the
    // table full.
    let charge = (5 + 100 + ENTRY_OVERHEAD) as u64;
    let full_tables = total * charge / BUDGET as u64;
    assert!(stats.flushes >= full_tables - 1, "{stats:?}");
    assert!(stats.flushes <= 2 * full_tables + 1, "{stats:?}");
    assert_eq!(stats.runs as u64, stats.flushes);
    let mut workspace = EngineWorkspace::new();
    for writer in 0..WRITERS {
        for at in [0, EACH / 2, EACH - 1] {
            let found = engine
                .get(&key(writer, at), &mut workspace)
                .unwrap()
                .unwrap();
            assert_eq!(found.value, [writer; 100]);
        }
    }
}

/// While a snapshot keeps compaction from running, the run limits refuse the
/// flushes writes need, and the writes are refused, changing nothing, until
/// the snapshot is dropped and a compaction makes room.
#[test]
fn writes_refused_while_a_snapshot_blocks_compaction_resume_after_it() {
    let engine = Engine::new(EngineOptions {
        memtable_budget: 4 << 10,
        max_runs: 2,
        snapshots: SnapshotLimits {
            max_retained_bytes: 0,
            ..SnapshotLimits::default()
        },
        ..manual()
    })
    .unwrap();
    // Holds the first table once it is flushed.
    engine.put(b"first", b"v").unwrap();
    let snapshot = engine.snapshot().unwrap();
    let mut refused = None;
    for at in 0..1_000u32 {
        // Spread, so that every table and run spans the key range.
        match engine.put(&(at * 37 % 64).to_be_bytes(), &[1; 100]) {
            Ok(_) => {}
            Err(error) => {
                refused = Some(error);
                break;
            }
        }
    }
    let refused = refused.expect("the run limits refused a flush");
    assert!(
        matches!(
            refused,
            UpdateError::FlushRefused(RunLimitError { max_runs: 2, .. })
        ),
        "{refused}"
    );
    let before = engine.stats();
    assert_eq!(before.runs, 2);
    assert_eq!(engine.put(b"late", &[1; 100]), Err(refused));
    assert_eq!(engine.stats(), before);
    let merge_all = CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    let blocked = engine.compact(merge_all, &mut workspace);
    assert!(
        matches!(blocked, Err(CompactionError::Retained { .. })),
        "{blocked:?}"
    );
    assert_eq!(engine.put(b"late", &[1; 100]), Err(refused));

    drop(snapshot);
    // The oldest run moves to level 1, then the other merges with it.
    while engine.compact(merge_all, &mut workspace).unwrap().is_some() {}
    let seq = engine.put(b"late", &[1; 100]).unwrap();
    assert_eq!(seq, before.last_seq + 1);
    let stats = engine.stats();
    assert_eq!(stats.flushes, before.flushes + 1);
    assert_eq!(value_of(&engine, b"late"), Some((seq, vec![1; 100])));
}

// Scans.

#[test]
fn scans_return_each_key_once_in_bytewise_order() {
    let engine = engine();
    let keys: [&[u8]; 7] = [b"\xFF", b"b", b"a\x00", b"ab", b"a", b"\x00", b""];
    for (i, key) in keys.iter().enumerate() {
        engine.put(key, &[i as u8]).unwrap();
        if i % 2 == 0 {
            engine.flush().unwrap();
        }
    }
    let seen: Vec<Vec<u8>> = scanned(&engine, b"").into_iter().map(|r| r.0).collect();
    let wanted: [&[u8]; 7] = [b"", b"\x00", b"a", b"a\x00", b"ab", b"b", b"\xFF"];
    assert_eq!(seen, wanted.map(<[u8]>::to_vec));
}

fn binary_keys_engine() -> Engine {
    let engine = engine();
    for key in [&b"\xFF\xFF\x01"[..], b"\xFF", b"\xFF\xFF"] {
        engine.put(key, b"run").unwrap();
    }
    engine.flush().unwrap();
    for key in [&b"\xFF\xFE"[..], b"\xFF\xFF\x00", b"\xFE\xFF", b"a"] {
        engine.put(key, b"table").unwrap();
    }
    engine
}

fn keys_with_prefix(engine: &Engine, prefix: &[u8]) -> Vec<Vec<u8>> {
    scanned(engine, prefix).into_iter().map(|r| r.0).collect()
}

#[test]
fn a_prefix_of_0xff_bytes_is_bounded_correctly_over_both_sources() {
    let engine = binary_keys_engine();
    assert_eq!(
        keys_with_prefix(&engine, b"\xFF\xFF"),
        [&b"\xFF\xFF"[..], b"\xFF\xFF\x00", b"\xFF\xFF\x01"].map(<[u8]>::to_vec)
    );
    assert_eq!(
        keys_with_prefix(&engine, b"\xFF"),
        [
            &b"\xFF"[..],
            b"\xFF\xFE",
            b"\xFF\xFF",
            b"\xFF\xFF\x00",
            b"\xFF\xFF\x01"
        ]
        .map(<[u8]>::to_vec)
    );
}

#[test]
fn an_empty_prefix_scans_everything() {
    let engine = binary_keys_engine();
    assert_eq!(scanned(&engine, b"").len(), 7);
    let reader = engine.read();
    let mut workspace = EngineWorkspace::new();
    let mut scan = reader.scan(&mut workspace);
    let mut count = 0;
    while scan.next_record().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 7);
}

#[test]
fn a_prefix_matching_nothing_returns_nothing_from_any_source() {
    let engine = binary_keys_engine();
    assert_eq!(
        keys_with_prefix(&engine, b"\xFF\xFF\xFF"),
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(keys_with_prefix(&engine, b"z"), Vec::<Vec<u8>>::new());
}

#[test]
fn a_finished_scan_stays_finished() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    let reader = engine.read();
    let mut workspace = EngineWorkspace::new();
    let mut scan = reader.scan(&mut workspace);
    assert!(scan.next_record().unwrap().is_some());
    assert_eq!(scan.next_record().unwrap(), None);
    assert_eq!(scan.next_record().unwrap(), None);
}

#[test]
fn a_prefix_scan_of_a_table_only_key_decodes_no_unrelated_run() {
    let engine = engine();
    engine.put(b"a1", b"run").unwrap();
    engine.flush().unwrap();
    engine.put(b"b1", b"table").unwrap();
    let reader = engine.read();
    let mut workspace = EngineWorkspace::new();
    let mut scan = reader.scan_prefix(b"b", &mut workspace);
    assert_eq!(scan.next_record().unwrap().unwrap().key, b"b1");
    assert_eq!(scan.next_record().unwrap(), None);
    drop(scan);
    assert_eq!(workspace.blocks_loaded(), 0);
}

#[test]
fn reads_through_a_guard_see_one_state() {
    let engine = engine();
    let seq = engine.put(b"a", b"v").unwrap();
    let reader = engine.read();
    let mut workspace = EngineWorkspace::new();
    let found = reader.get(b"a", &mut workspace).unwrap().unwrap();
    assert_eq!((found.seq, found.value), (seq, &b"v"[..]));
    assert_eq!(reader.get(b"b", &mut workspace).unwrap(), None);
}

// Concurrent use.

#[test]
fn an_engine_can_be_shared_between_threads_by_type() {
    fn shareable<T: Send + Sync>() {}
    shareable::<Engine>();
}

/// Lets a blocked thread run, if it can. Never decides a test's outcome: a
/// thread that has not run yet cannot make these assertions fail.
fn give_others_a_chance() {
    for _ in 0..1000 {
        thread::yield_now();
    }
}

#[test]
fn a_held_view_stays_stable_while_a_writer_waits() {
    let engine = engine();
    let seq = engine.put(b"a", b"v").unwrap();
    let writer_started = Barrier::new(2);
    let written = AtomicBool::new(false);

    thread::scope(|scope| {
        let reader = engine.read();
        scope.spawn(|| {
            writer_started.wait();
            engine.put(b"a", b"replaced").unwrap();
            engine.put(b"new", b"v").unwrap();
            written.store(true, Ordering::SeqCst);
        });

        writer_started.wait();
        give_others_a_chance();
        assert_eq!(scan_prefix(&reader, b""), [live(b"a", seq, b"v")]);
        give_others_a_chance();
        assert_eq!(scan_prefix(&reader, b""), [live(b"a", seq, b"v")]);
        assert!(!written.load(Ordering::SeqCst));
        drop(reader);
    });

    assert!(written.load(Ordering::SeqCst));
    assert_eq!(value_of(&engine, b"a").unwrap().1, b"replaced");
    assert!(value_of(&engine, b"new").is_some());
}

#[test]
fn a_held_view_stays_stable_while_a_flush_waits() {
    let engine = engine();
    let seq = engine.put(b"a", b"v").unwrap();
    let flush_started = Barrier::new(2);
    let flushed = AtomicBool::new(false);

    thread::scope(|scope| {
        let reader = engine.read();
        scope.spawn(|| {
            flush_started.wait();
            engine.flush().unwrap();
            flushed.store(true, Ordering::SeqCst);
        });

        flush_started.wait();
        give_others_a_chance();
        assert_eq!(scan_prefix(&reader, b""), [live(b"a", seq, b"v")]);
        assert!(!flushed.load(Ordering::SeqCst));
        drop(reader);
    });

    assert_eq!(engine.stats().runs, 1);
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
}

#[test]
fn concurrent_flushes_each_complete_and_lose_nothing() {
    let engine = engine();
    let start = Barrier::new(2);
    engine.put(b"a", b"v").unwrap();
    thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| {
                start.wait();
                engine.flush().unwrap();
            });
        }
    });
    assert_eq!(engine.stats().runs, 1);
    assert!(no_frozen(engine.stats()));
    assert_eq!(value_of(&engine, b"a").unwrap().1, b"v");
}

fn pair_key(writer: usize, part: &str) -> Vec<u8> {
    format!("w{writer}/{part}").into_bytes()
}

fn counter(value: &[u8]) -> u64 {
    u64::from_be_bytes(value.try_into().unwrap())
}

/// Each writer sets `a` and then `b` to the same increasing counter. Any one
/// consistent state therefore has `a >= b`, with `a` once seen never going
/// backwards and neither key ever vanishing, whatever flushes do.
#[test]
fn concurrent_writers_readers_and_flushes_keep_every_invariant() {
    const WRITERS: usize = 2;
    const WRITES: u64 = 3000;
    const FLUSHES: usize = 25;

    let engine = Engine::new(EngineOptions {
        max_runs: 1000,
        ..manual()
    })
    .unwrap();
    for writer in 0..WRITERS {
        engine
            .put(&pair_key(writer, "a"), &0u64.to_be_bytes())
            .unwrap();
        engine
            .put(&pair_key(writer, "b"), &0u64.to_be_bytes())
            .unwrap();
    }

    let start = Barrier::new(WRITERS + 4);
    let writers_left = AtomicU64::new(WRITERS as u64);

    let sequences: Vec<u64> = thread::scope(|scope| {
        let mut writers = Vec::new();
        for writer in 0..WRITERS {
            let (engine, start, writers_left) = (&engine, &start, &writers_left);
            writers.push(scope.spawn(move || {
                let (a, b) = (pair_key(writer, "a"), pair_key(writer, "b"));
                let mut workspace = EngineWorkspace::new();
                let mut seen = Vec::new();
                start.wait();
                for n in 1..=WRITES {
                    let first = engine.put(&a, &n.to_be_bytes()).unwrap();
                    let found = engine.get(&a, &mut workspace).unwrap().unwrap();
                    assert_eq!(found.seq, first, "read your own write");
                    assert_eq!(counter(found.value), n);
                    seen.push(first);
                    seen.push(engine.put(&b, &n.to_be_bytes()).unwrap());
                }
                writers_left.fetch_sub(1, Ordering::SeqCst);
                seen
            }));
        }

        scope.spawn(|| {
            let mut workspace = EngineWorkspace::new();
            let mut last = [0u64; WRITERS];
            start.wait();
            while writers_left.load(Ordering::SeqCst) > 0 {
                for (writer, last) in last.iter_mut().enumerate() {
                    let key = pair_key(writer, "a");
                    let found = engine.get(&key, &mut workspace).unwrap();
                    let now = counter(found.expect("a written key never vanishes").value);
                    assert!(now >= *last, "a read went backwards");
                    *last = now;
                }
            }
        });

        scope.spawn(|| {
            start.wait();
            while writers_left.load(Ordering::SeqCst) > 0 {
                let scan = scanned(&engine, b"");
                assert_eq!(scan.len(), WRITERS * 2);
                assert!(scan.windows(2).all(|pair| pair[0].0 < pair[1].0));
                for pair in scan.chunks(2) {
                    assert!(counter(&pair[0].2) >= counter(&pair[1].2));
                }
            }
        });

        scope.spawn(|| {
            start.wait();
            let mut done = 0;
            while done < FLUSHES && writers_left.load(Ordering::SeqCst) > 0 {
                done += usize::from(engine.flush().unwrap().tables > 0);
                thread::yield_now();
            }
        });

        start.wait();
        let mut all = Vec::new();
        for writer in writers {
            let seen = writer.join().unwrap();
            assert!(seen.windows(2).all(|pair| pair[0] < pair[1]));
            all.extend(seen);
        }
        all
    });

    let count = sequences.len();
    let mut sorted = sequences;
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), count, "a sequence number was reused");

    for writer in 0..WRITERS {
        for part in ["a", "b"] {
            let found = value_of(&engine, &pair_key(writer, part)).unwrap();
            assert_eq!(counter(&found.1), WRITES);
        }
    }
}

// Reference model.

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

fn key_pool() -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![0],
        vec![0xFF],
        vec![0xFF, 0xFF],
        b"a".to_vec(),
        b"a\x00".to_vec(),
        b"ab".to_vec(),
    ];
    keys.extend((0..24u8).map(|i| vec![b'k', i, 0xFF - i]));
    keys
}

fn entry_charge(key: &[u8], value_len: usize) -> usize {
    key.len() + value_len + ENTRY_OVERHEAD
}

/// The latest update of a key, across every source.
type Model = HashMap<Vec<u8>, (u64, Option<Vec<u8>>)>;

fn expected_live(model: &Model, prefix: &[u8]) -> Vec<Live> {
    let mut live: Vec<Live> = model
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .filter_map(|(key, (seq, value))| Some((key.clone(), *seq, value.clone()?)))
        .collect();
    live.sort();
    live
}

fn assert_engine_matches(engine: &Engine, model: &Model, pool: &[Vec<u8>], prefix: &[u8]) {
    let mut workspace = EngineWorkspace::new();
    for key in pool {
        let found = engine
            .get(key, &mut workspace)
            .unwrap()
            .map(|found| (found.seq, found.value.to_vec()));
        let wanted = model
            .get(key)
            .and_then(|(seq, value)| Some((*seq, value.clone()?)));
        assert_eq!(found, wanted);
    }
    assert_eq!(scanned(engine, b""), expected_live(model, b""));
    assert_eq!(scanned(engine, prefix), expected_live(model, prefix));
}

/// Updates interleaved with flushes, refused flushes that leave tables
/// frozen, and flushes that writes start for room. Reads must always equal
/// the single latest-update map: a flush never changes what is visible.
#[test]
fn randomized_operations_match_an_independent_model_across_flushes() {
    const SEEDS: u64 = 40;
    const OPERATIONS: usize = 300;

    let pool = key_pool();
    let long_key = vec![b'x'; MAX_KEY_LEN + 1];
    let (mut rejected, mut flushes, mut refused) = (0, 0, 0);
    let mut automatic = 0;

    for seed in 0..SEEDS {
        let mut rng = Rng(seed);
        let budget = [1_500, 6_000, 40_000][rng.below(3)];
        let engine = Engine::new(EngineOptions {
            memtable_budget: budget,
            target_run_len: [1, 512, 1 << 20][rng.below(3)],
            max_runs: 100_000,
            max_run_bytes: usize::MAX,
            last_seq: 0,
            background_compaction: None,
            snapshots: SnapshotLimits::default(),
            compression: [
                Compression::Zstd,
                Compression::Lz4,
                Compression::Uncompressed,
                Compression::ZstdDictionary,
            ][rng.below(4)],
            target_block_len: [64, 2 << 10, 16 << 10][rng.below(3)],
        })
        .unwrap();
        let mut model = Model::new();
        // Charged per key in the active table, and what frozen tables are
        // charged: a refused flush leaves its tables frozen.
        let mut active: HashMap<Vec<u8>, usize> = HashMap::new();
        let (mut has_frozen, mut frozen_charged) = (false, 0);
        let mut last_seq = 0;

        for _ in 0..OPERATIONS {
            let prefix = &pool[rng.below(pool.len())];
            let prefix = &prefix[..rng.below(prefix.len() + 1)];
            match rng.below(100) {
                0 => {
                    assert_eq!(
                        engine.put(&long_key, b"v"),
                        Err(UpdateError::Rejected(WriteError::KeyTooLong {
                            len: long_key.len()
                        }))
                    );
                    rejected += 1;
                }
                1..=74 => {
                    let key = pool[rng.below(pool.len())].clone();
                    let delete = rng.below(4) == 0;
                    let len = if delete {
                        0
                    } else if rng.below(12) == 0 {
                        rng.below(2_400)
                    } else {
                        rng.below(40)
                    };
                    let value: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();

                    let charge = entry_charge(&key, len);
                    let total: usize = active.values().sum();
                    let held = active.get(&key).copied().unwrap_or(0);
                    // Frozen tables lower the active table's budget so that
                    // all of them stay within two budgets.
                    let active_budget = budget.min(2 * budget - frozen_charged);
                    let fits = charge <= budget && total - held + charge <= active_budget;

                    let result = if delete {
                        engine.delete(&key)
                    } else {
                        engine.put(&key, &value)
                    };
                    // A write with no room flushes every table and lands
                    // alone in a new one.
                    let flushes_first = !fits && charge <= budget;
                    if fits || flushes_first {
                        last_seq += 1;
                        assert_eq!(result, Ok(last_seq), "seed {seed}");
                        if flushes_first {
                            active.clear();
                            (has_frozen, frozen_charged) = (false, 0);
                            automatic += 1;
                        }
                        active.insert(key.clone(), charge);
                        model.insert(key, (last_seq, (!delete).then_some(value)));
                    } else {
                        assert!(matches!(result, Err(UpdateError::Rejected(_))));
                        rejected += 1;
                    }
                }
                75..=85 => {
                    let empty = !has_frozen && active.is_empty();
                    if rng.below(5) == 0 {
                        engine.set_run_limits(1, 1).unwrap();
                        let result = engine.flush();
                        engine.set_run_limits(100_000, usize::MAX).unwrap();
                        if empty {
                            assert_eq!(result.unwrap().tables, 0);
                        } else {
                            assert!(result.is_err(), "seed {seed}");
                            frozen_charged += active.values().sum::<usize>();
                            active.clear();
                            has_frozen = true;
                            refused += 1;
                        }
                    } else {
                        assert_eq!(engine.flush().unwrap().tables == 0, empty);
                        active.clear();
                        (has_frozen, frozen_charged) = (false, 0);
                        flushes += usize::from(!empty);
                    }
                }
                _ => {}
            }
            assert_eq!(engine.stats().frozen.is_some(), has_frozen);
            assert_eq!(engine.stats().last_seq, last_seq);
            assert_engine_matches(&engine, &model, &pool, prefix);
        }
    }

    assert!(flushes > 100, "{flushes}");
    assert!(refused > 10, "{refused}");
    assert!(rejected > 20, "{rejected}");
    assert!(automatic > 50, "{automatic}");
}

// Configuration.

/// The defaults are valid, run a worker with the default compaction
/// options, and keep the overlap within its allowance without any call to
/// `compact`.
#[test]
fn the_defaults_compact_in_the_background() {
    let defaults = EngineOptions::default();
    assert_eq!(
        defaults.background_compaction,
        Some(CompactionOptions::default())
    );
    assert_eq!(CompactionOptions::default().max_overlap, 4);
    assert_eq!(
        (defaults.compression, defaults.target_block_len),
        (Compression::Zstd, 16 << 10)
    );
    let engine = Engine::new(EngineOptions {
        memtable_budget: 16 << 10,
        ..defaults
    })
    .unwrap();
    assert!(engine.background_status().unwrap().running);
    // Eight flushes of the same keys: eight overlapping runs, unless
    // compacted.
    for round in 0..8u32 {
        for i in 0..50u32 {
            engine.put(&i.to_be_bytes(), &round.to_be_bytes()).unwrap();
        }
        engine.flush().unwrap();
    }
    engine.wait_for_compaction().unwrap();
    let status = engine.background_status().unwrap();
    assert!(status.compactions >= 1, "{status:?}");
    assert!(engine.stats().max_overlap <= 4, "{:?}", engine.stats());
    let mut workspace = EngineWorkspace::new();
    for i in 0..50u32 {
        let found = engine.get(&i.to_be_bytes(), &mut workspace).unwrap();
        assert_eq!(found.unwrap().value, 7u32.to_be_bytes());
    }
}

/// Every setting outside its documented range is refused with its own name
/// and range, nothing is adjusted, and each bound itself is accepted.
#[test]
fn each_setting_out_of_range_is_refused_by_name() {
    let compaction = |change: fn(&mut CompactionOptions)| {
        let mut options = CompactionOptions::default();
        change(&mut options);
        EngineOptions {
            background_compaction: Some(options),
            ..EngineOptions::default()
        }
    };
    let engine = |change: fn(&mut EngineOptions)| {
        let mut options = EngineOptions::default();
        change(&mut options);
        options
    };
    let half = (usize::MAX / 2) as u64;
    let refused: Vec<(EngineOptions, &str, u64, u64, u64)> = vec![
        (
            engine(|o| o.memtable_budget = 0),
            "EngineOptions::memtable_budget",
            0,
            ENTRY_OVERHEAD as u64,
            half,
        ),
        (
            engine(|o| o.memtable_budget = ENTRY_OVERHEAD - 1),
            "EngineOptions::memtable_budget",
            ENTRY_OVERHEAD as u64 - 1,
            ENTRY_OVERHEAD as u64,
            half,
        ),
        (
            engine(|o| o.memtable_budget = usize::MAX / 2 + 1),
            "EngineOptions::memtable_budget",
            half + 1,
            ENTRY_OVERHEAD as u64,
            half,
        ),
        (
            engine(|o| o.target_run_len = 0),
            "EngineOptions::target_run_len",
            0,
            1,
            1 << 30,
        ),
        (
            engine(|o| o.target_run_len = (1 << 30) + 1),
            "EngineOptions::target_run_len",
            (1 << 30) + 1,
            1,
            1 << 30,
        ),
        (
            engine(|o| o.max_runs = 0),
            "EngineOptions::max_runs",
            0,
            1,
            u64::MAX,
        ),
        (
            engine(|o| o.max_run_bytes = 0),
            "EngineOptions::max_run_bytes",
            0,
            1,
            u64::MAX,
        ),
        (
            engine(|o| o.target_block_len = 0),
            "EngineOptions::target_block_len",
            0,
            1,
            64 << 10,
        ),
        (
            engine(|o| o.target_block_len = (64 << 10) + 1),
            "EngineOptions::target_block_len",
            (64 << 10) + 1,
            1,
            64 << 10,
        ),
        (
            compaction(|c| c.max_overlap = 0),
            "CompactionOptions::max_overlap",
            0,
            1,
            u64::MAX,
        ),
        (
            compaction(|c| c.max_inputs = 1),
            "CompactionOptions::max_inputs",
            1,
            2,
            u64::MAX,
        ),
        (
            compaction(|c| c.max_input_bytes = (128 << 20) - 1),
            "CompactionOptions::max_input_bytes",
            (128 << 20) - 1,
            128 << 20,
            u64::MAX,
        ),
        (
            compaction(|c| c.max_output_bytes = (128 << 20) - 1),
            "CompactionOptions::max_output_bytes",
            (128 << 20) - 1,
            128 << 20,
            u64::MAX,
        ),
        (
            compaction(|c| c.max_workspace_bytes = 3_211_263),
            "CompactionOptions::max_workspace_bytes",
            3_211_263,
            // Two inputs of 16 KiB blocks: a context and a block each, and
            // a context and two blocks for the output.
            3_211_264,
            u64::MAX,
        ),
    ];
    for (options, setting, value, min, max) in refused {
        let Err(OpenError::Config(error)) = Engine::new(options).map(drop) else {
            panic!("{setting} accepted");
        };
        assert_eq!(
            (error.setting, error.value, error.min, error.max),
            (setting, value, min, max)
        );
        let message = error.to_string();
        assert!(message.starts_with(setting), "{message}");
        assert!(message.contains(&value.to_string()), "{message}");
    }

    let accepted = [
        engine(|o| o.memtable_budget = ENTRY_OVERHEAD),
        engine(|o| o.target_run_len = 1),
        engine(|o| {
            o.target_run_len = 1 << 30;
            o.background_compaction = None;
        }),
        engine(|o| o.max_runs = 1),
        engine(|o| o.max_run_bytes = 1),
        engine(|o| o.target_block_len = 1),
        engine(|o| o.target_block_len = 64 << 10),
        engine(|o| o.background_compaction = None),
        engine(|o| {
            o.snapshots = SnapshotLimits {
                max_live: 0,
                max_retained_bytes: 0,
            }
        }),
        compaction(|c| c.max_overlap = 1),
        compaction(|c| c.max_inputs = 2),
        compaction(|c| c.max_input_bytes = 128 << 20),
        compaction(|c| c.max_workspace_bytes = 3_211_264),
        compaction(|c| c.disjoint_runs_above = 0),
    ];
    for options in accepted {
        Engine::new(options).unwrap();
    }
}

/// The smallest budget admits exactly the smallest record: an empty key
/// with no value.
#[test]
fn the_smallest_budget_admits_the_smallest_record() {
    let engine = Engine::new(EngineOptions {
        memtable_budget: ENTRY_OVERHEAD,
        ..manual()
    })
    .unwrap();
    engine.delete(b"").unwrap();
    assert!(matches!(
        engine.put(b"k", b""),
        Err(UpdateError::Rejected(
            WriteError::RecordExceedsBudget { .. }
        ))
    ));
}

/// Zero snapshot limits are valid and mean what they say: no snapshot is
/// ever admitted.
#[test]
fn zero_snapshot_limits_refuse_every_snapshot() {
    let engine = Engine::new(EngineOptions {
        snapshots: SnapshotLimits {
            max_live: 0,
            max_retained_bytes: 0,
        },
        ..EngineOptions::default()
    })
    .unwrap();
    engine.put(b"k", b"v").unwrap();
    assert!(engine.snapshot().is_err());
}

/// Options given to `compact` and `set_run_limits` are checked the same way,
/// and refusing them changes nothing.
#[test]
fn runtime_options_are_checked_like_construction() {
    let engine = Engine::new(manual()).unwrap();
    for round in 0..6u32 {
        engine.put(b"k", &round.to_be_bytes()).unwrap();
        engine.flush().unwrap();
    }
    let before = engine.stats();
    let mut workspace = MergeWorkspace::new();
    for options in [
        CompactionOptions {
            max_overlap: 0,
            ..CompactionOptions::default()
        },
        CompactionOptions {
            max_inputs: 0,
            ..CompactionOptions::default()
        },
    ] {
        let refused = engine.compact(options, &mut workspace).unwrap_err();
        assert!(matches!(refused, CompactionError::Config(_)), "{refused}");
    }
    let refused = engine.set_run_limits(0, usize::MAX).unwrap_err();
    assert_eq!(refused.setting, "EngineOptions::max_runs");
    let refused = engine.set_run_limits(1, 0).unwrap_err();
    assert_eq!(refused.setting, "EngineOptions::max_run_bytes");
    assert_eq!(engine.stats(), before);
    // The runs stay within the limits they had: another flush publishes.
    engine.put(b"k", b"new").unwrap();
    engine.flush().unwrap();
    assert!(
        engine
            .compact(CompactionOptions::default(), &mut workspace)
            .unwrap()
            .is_some()
    );
}

/// Dictionary compression is accepted with any valid block length, as
/// requested: a dictionary is trained and kept only where it saves enough,
/// whatever the length.
#[test]
fn dictionary_compression_accepts_every_block_length() {
    for target_block_len in [1, 4 << 10, 16 << 10, 64 << 10] {
        let engine = Engine::new(EngineOptions {
            compression: Compression::ZstdDictionary,
            target_block_len,
            ..manual()
        })
        .unwrap();
        for round in 0..6u32 {
            for i in 0..200u32 {
                let value = format!(r#"{{"id":{i},"round":{round},"name":"user {i}"}}"#);
                engine.put(&i.to_be_bytes(), value.as_bytes()).unwrap();
            }
            engine.flush().unwrap();
        }
        let mut workspace = MergeWorkspace::new();
        while engine
            .compact(CompactionOptions::default(), &mut workspace)
            .unwrap()
            .is_some()
        {}
        let mut reads = EngineWorkspace::new();
        let found = engine.get(&7u32.to_be_bytes(), &mut reads).unwrap();
        assert_eq!(
            found.unwrap().value,
            br#"{"id":7,"round":5,"name":"user 7"}"#,
            "{target_block_len}"
        );
    }
}

// Lifecycle.

/// Closing an idle engine succeeds, with or without a worker.
#[test]
fn closing_an_idle_engine_succeeds() {
    for options in [EngineOptions::default(), manual()] {
        let engine = Engine::new(options).unwrap();
        engine.put(b"k", b"v").unwrap();
        assert_eq!(engine.close(), Ok(()));
    }
}

/// Snapshots outlive the engine: readers on other threads keep reading
/// what they saw while the engine is closed beside them, including during
/// a background compaction it waits for.
#[test]
fn snapshots_stay_readable_while_and_after_the_engine_closes() {
    let engine = Engine::new(EngineOptions {
        memtable_budget: 16 << 10,
        background_compaction: Some(CompactionOptions {
            max_overlap: 1,
            ..CompactionOptions::default()
        }),
        ..EngineOptions::default()
    })
    .unwrap();
    for round in 0..4u32 {
        for i in 0..50u32 {
            engine.put(&i.to_be_bytes(), &round.to_be_bytes()).unwrap();
        }
        engine.flush().unwrap();
    }
    let snapshots: Vec<_> = (0..3).map(|_| engine.snapshot().unwrap()).collect();
    let start = Barrier::new(snapshots.len() + 1);
    thread::scope(|scope| {
        let readers: Vec<_> = snapshots
            .iter()
            .map(|snapshot| {
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    let mut workspace = EngineWorkspace::new();
                    for _ in 0..20 {
                        let mut scan = snapshot.scan(&mut workspace);
                        let mut seen = 0u32;
                        while let Some(record) = scan.next_record().unwrap() {
                            assert_eq!(record.value, 3u32.to_be_bytes());
                            seen += 1;
                        }
                        assert_eq!(seen, 50);
                    }
                })
            })
            .collect();
        start.wait();
        assert_eq!(engine.close(), Ok(()));
        for reader in readers {
            reader.join().unwrap();
        }
    });
    let mut workspace = EngineWorkspace::new();
    let found = snapshots[0]
        .get(&7u32.to_be_bytes(), &mut workspace)
        .unwrap();
    assert_eq!(found.unwrap().value, 3u32.to_be_bytes());
}
