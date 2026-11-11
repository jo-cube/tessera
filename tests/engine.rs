//! The integrated engine: visibility across memtables and runs, flush stages,
//! limits, and concurrent use.
//!
//! Expected results come from hash maps maintained by the tests. Concurrency
//! tests use barriers and shared flags and never sleep.

use std::collections::HashMap;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use tessera::block::MAX_KEY_LEN;
use tessera::engine::{
    BuiltFlush, Engine, EngineOptions, EngineStats, EngineWorkspace, FlushError, FlushJob,
    ReadGuard, SnapshotLimits, UpdateError,
};
use tessera::memtable::{ENTRY_OVERHEAD, WriteError};

type Live = (Vec<u8>, u64, Vec<u8>);

fn engine() -> Engine {
    Engine::new(EngineOptions::default())
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

fn budget_rejection(error: UpdateError) -> bool {
    matches!(
        error,
        UpdateError::Rejected(WriteError::BudgetExceeded { .. })
    )
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
        ..EngineOptions::default()
    });
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
        ..EngineOptions::default()
    });
    engine.put(b"a", b"v").unwrap();
    let before = engine.stats();

    let long = vec![b'k'; MAX_KEY_LEN + 1];
    assert_eq!(
        engine.put(&long, b"v"),
        Err(UpdateError::Rejected(WriteError::KeyTooLong {
            len: MAX_KEY_LEN + 1
        }))
    );
    assert!(matches!(
        engine.put(b"b", &[0; 300]),
        Err(UpdateError::Rejected(
            WriteError::RecordExceedsBudget { .. }
        ))
    ));
    assert!(budget_rejection(engine.put(b"b", &[0; 40]).unwrap_err()));
    assert_eq!(engine.stats(), before);
    assert_eq!(value_of(&engine, b"b"), None);
    assert_eq!(engine.put(b"a", b"w"), Ok(2));
}

#[test]
fn the_last_sequence_number_is_used_once_and_then_writes_stop() {
    let engine = Engine::new(EngineOptions {
        last_seq: u64::MAX - 1,
        ..EngineOptions::default()
    });
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
        ..EngineOptions::default()
    });
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

#[test]
fn a_tombstone_in_the_frozen_table_hides_a_value_in_a_run() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    engine.flush().unwrap();
    engine.delete(b"a").unwrap();
    let job = engine.begin_flush().unwrap();
    assert_eq!(value_of(&engine, b"a"), None);
    assert_eq!(scanned(&engine, b""), []);
    drop(job);
}

// Flush stages.

#[test]
fn an_empty_flush_creates_nothing() {
    let engine = engine();
    assert!(engine.begin_flush().is_none());
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
        ..EngineOptions::default()
    });
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

#[test]
fn every_flush_stage_shows_the_same_reads() {
    let engine = engine();
    let a = engine.put(b"a", b"1").unwrap();
    engine.flush().unwrap();
    let b = engine.put(b"b", b"2").unwrap();
    let expected = [live(b"a", a, b"1"), live(b"b", b, b"2")];
    assert_eq!(scanned(&engine, b""), expected);

    let job = engine.begin_flush().unwrap();
    assert_eq!(scanned(&engine, b""), expected);
    assert_eq!(value_of(&engine, b"b"), Some((b, b"2".to_vec())));
    assert!(engine.stats().frozen.is_some());

    let built = job.build();
    assert_eq!(built.runs().len(), 1);
    assert_eq!(scanned(&engine, b""), expected);
    assert_eq!(engine.stats().runs, 1, "built runs are not visible yet");

    built.publish().unwrap();
    assert_eq!(scanned(&engine, b""), expected);
    assert_eq!(value_of(&engine, b"b"), Some((b, b"2".to_vec())));
    let stats = engine.stats();
    assert!(no_frozen(stats));
    assert_eq!(stats.runs, 2);
}

#[test]
fn writes_during_a_flush_land_in_the_new_table_and_win() {
    let engine = engine();
    engine.put(b"a", b"frozen").unwrap();
    engine.put(b"b", b"frozen").unwrap();
    let job = engine.begin_flush().unwrap();

    let a = engine.put(b"a", b"during").unwrap();
    engine.delete(b"b").unwrap();
    let c = engine.put(b"c", b"during").unwrap();
    let expected = [live(b"a", a, b"during"), live(b"c", c, b"during")];
    assert_eq!(scanned(&engine, b""), expected);

    let built = job.build();
    assert_eq!(scanned(&engine, b""), expected);
    built.publish().unwrap();
    assert_eq!(scanned(&engine, b""), expected);
    assert_eq!(value_of(&engine, b"b"), None);
    assert_eq!(engine.stats().active.payload, 1 + 6 + 1 + 1 + 6);
}

#[test]
fn a_frozen_table_receives_no_later_writes() {
    let engine = engine();
    engine.put(b"a", b"frozen").unwrap();
    let job = engine.begin_flush().unwrap();
    engine.put(b"a", b"during").unwrap();
    assert_eq!(job.tables().map(|table| table.len()).sum::<usize>(), 1);
    let built = job.build();
    assert_eq!(built.runs()[0].len(), 1);
    built.publish().unwrap();
    assert_eq!(value_of(&engine, b"a").unwrap().1, b"during");
}

#[test]
fn an_abandoned_flush_keeps_the_table_readable_and_is_retried() {
    let engine = engine();
    let seq = engine.put(b"a", b"v").unwrap();
    drop(engine.begin_flush().unwrap());
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
    assert!(engine.stats().frozen.is_some());

    drop(engine.begin_flush().unwrap().build());
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
    assert_eq!(engine.stats().runs, 0);

    engine.begin_flush().unwrap().build().publish().unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
    assert!(no_frozen(engine.stats()));
    assert_eq!(engine.stats().runs, 1);
}

#[test]
fn a_retry_does_not_freeze_the_newer_active_table() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    drop(engine.begin_flush().unwrap());
    engine.put(b"b", b"v").unwrap();
    let before = engine.stats().active;

    let job = engine.begin_flush().unwrap();
    assert_eq!(job.tables().map(|table| table.len()).sum::<usize>(), 1);
    assert_eq!(engine.stats().active, before);
    job.build().publish().unwrap();
    assert_eq!(engine.stats().active, before);
}

#[test]
fn a_full_flush_writes_a_retried_table_and_the_active_one_as_one_set_of_runs() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    drop(engine.begin_flush().unwrap());
    engine.put(b"b", b"v").unwrap();

    let report = engine.flush().unwrap();
    assert_eq!((report.tables, report.records, report.runs), (2, 2, 1));
    let stats = engine.stats();
    assert!(no_frozen(stats));
    assert_eq!(stats.active.payload, 0);
    assert_eq!(scanned(&engine, b"").len(), 2);
}

#[test]
fn a_flush_can_follow_a_panic_during_an_earlier_one() {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    let result = thread::scope(|scope| {
        scope
            .spawn(|| {
                let _job = engine.begin_flush().unwrap();
                panic!("flush interrupted");
            })
            .join()
    });
    assert!(result.is_err());
    assert_eq!(engine.flush().unwrap().tables, 1);
    assert_eq!(value_of(&engine, b"a").unwrap().1, b"v");
}

// Run limits.

fn limited(max_runs: usize, max_run_bytes: usize) -> Engine {
    Engine::new(EngineOptions {
        max_runs,
        max_run_bytes,
        ..EngineOptions::default()
    })
}

/// What one small flush counts against the run byte limit: its stored bytes
/// and its filter.
fn flushed_size() -> usize {
    let engine = engine();
    engine.put(b"a", b"v").unwrap();
    let report = engine.flush().unwrap();
    assert!(report.filter_bytes > 0);
    report.run_bytes + report.filter_bytes
}

#[test]
fn a_flush_over_the_run_count_is_rejected_and_stays_readable() {
    let engine = limited(0, usize::MAX);
    let seq = engine.put(b"a", b"v").unwrap();
    let error = engine.flush().unwrap_err();
    assert_eq!((error.runs, error.max_runs), (1, 0));
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
        FlushError {
            runs: 1,
            max_runs: usize::MAX,
            run_bytes: size,
            max_run_bytes: size - 1
        }
    );
}

#[test]
fn a_rejected_flush_succeeds_once_the_limits_are_raised() {
    let engine = limited(0, 0);
    let seq = engine.put(b"a", b"v").unwrap();
    engine.flush().unwrap_err();
    engine.set_run_limits(1, usize::MAX);
    assert_eq!(engine.flush().unwrap().tables, 1);
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
    assert_eq!(engine.stats().runs, 1);
}

#[test]
fn writes_continue_into_the_active_table_after_a_rejected_flush() {
    let engine = limited(0, 0);
    engine.put(b"a", b"v").unwrap();
    engine.flush().unwrap_err();
    let seq = engine.put(b"b", b"w").unwrap();
    assert_eq!(value_of(&engine, b"b"), Some((seq, b"w".to_vec())));
    assert_eq!(scanned(&engine, b"").len(), 2);
}

// Budgets.

#[test]
fn a_full_table_rejects_writes_until_a_flush_makes_room() {
    let budget = 1 + 1 + ENTRY_OVERHEAD;
    let engine = Engine::new(EngineOptions {
        memtable_budget: budget,
        ..EngineOptions::default()
    });
    engine.put(b"a", b"v").unwrap();
    assert!(budget_rejection(engine.put(b"b", b"v").unwrap_err()));
    engine.flush().unwrap();
    engine.put(b"b", b"v").unwrap();
    assert_eq!(scanned(&engine, b"").len(), 2);
}

#[test]
fn at_most_one_table_is_frozen_while_the_new_one_fills() {
    let budget = 1 + 1 + ENTRY_OVERHEAD;
    let engine = Engine::new(EngineOptions {
        memtable_budget: budget,
        ..EngineOptions::default()
    });
    engine.put(b"a", b"v").unwrap();
    let job = engine.begin_flush().unwrap();
    engine.put(b"b", b"v").unwrap();
    assert!(budget_rejection(engine.put(b"c", b"v").unwrap_err()));
    let stats = engine.stats();
    assert_eq!(stats.active.charged, budget);
    assert_eq!(stats.frozen.unwrap().charged, budget);
    job.build().publish().unwrap();
    assert_eq!(engine.stats().frozen, None);
}

#[test]
fn a_flush_accounts_for_each_table_separately() {
    let engine = engine();
    engine.put(b"a", b"12345").unwrap();
    let job = engine.begin_flush().unwrap();
    engine.put(b"bb", b"1").unwrap();
    let stats = engine.stats();
    assert_eq!(stats.frozen.unwrap().payload, 6);
    assert_eq!(stats.active.payload, 3);
    job.build().publish().unwrap();
    let stats = engine.stats();
    assert_eq!(stats.active.payload, 3);
    assert!(stats.run_bytes > 0);
}

// Scans.

/// Fills runs and an active table that the caller then freezes, and returns
/// the live records.
fn fill_mixed(engine: &Engine) -> Vec<Live> {
    let k1 = engine.put(b"k1", b"run").unwrap();
    engine.put(b"k2", b"run").unwrap();
    engine.put(b"k3", b"run").unwrap();
    engine.put(b"k4", b"run").unwrap();
    engine.flush().unwrap();
    let k2 = engine.put(b"k2", b"frozen").unwrap();
    engine.delete(b"k3").unwrap();
    let k4 = engine.put(b"k4", b"frozen").unwrap();
    let k5 = engine.put(b"k5", b"frozen").unwrap();
    vec![
        live(b"k1", k1, b"run"),
        live(b"k2", k2, b"frozen"),
        live(b"k4", k4, b"frozen"),
        live(b"k5", k5, b"frozen"),
    ]
}

#[test]
fn a_scan_merges_every_source_with_the_newest_update_winning() {
    let engine = engine();
    let mut expected = fill_mixed(&engine);
    let _job = engine.begin_flush().unwrap();
    engine.delete(b"k5").unwrap();
    let k6 = engine.put(b"k6", b"active").unwrap();
    expected.pop();
    expected.push(live(b"k6", k6, b"active"));
    assert_eq!(scanned(&engine, b""), expected);
    assert_eq!(value_of(&engine, b"k3"), None);
    assert_eq!(value_of(&engine, b"k5"), None);
}

#[test]
fn a_key_in_the_active_table_replaces_the_frozen_and_run_versions() {
    let engine = engine();
    fill_mixed(&engine);
    let _job = engine.begin_flush().unwrap();
    let seq = engine.put(b"k2", b"active").unwrap();
    assert_eq!(scanned(&engine, b"k2"), [live(b"k2", seq, b"active")]);
}

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
fn reads_and_writes_proceed_while_a_flush_has_built_its_runs() {
    let engine = engine();
    let seq = engine.put(b"a", b"v").unwrap();
    let built: BuiltFlush<'_> = engine.begin_flush().unwrap().build();

    thread::scope(|scope| {
        scope
            .spawn(|| {
                assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
                engine.put(b"b", b"during").unwrap();
                assert_eq!(value_of(&engine, b"b").unwrap().1, b"during");
            })
            .join()
            .unwrap();
    });
    built.publish().unwrap();
    assert_eq!(scanned(&engine, b"").len(), 2);
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
        ..EngineOptions::default()
    });
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

enum Stage<'e> {
    Idle,
    Begun(FlushJob<'e>),
    Built(BuiltFlush<'e>),
}

/// Updates interleaved with every flush stage, abandonment and rejected
/// publication. Whatever the stage, reads must equal the single latest-update
/// map: a flush never changes what is visible.
#[test]
fn randomized_operations_match_an_independent_model_at_every_flush_stage() {
    const SEEDS: u64 = 40;
    const OPERATIONS: usize = 300;

    let pool = key_pool();
    let long_key = vec![b'x'; MAX_KEY_LEN + 1];
    let (mut rejected, mut flushes, mut abandoned, mut refused) = (0, 0, 0, 0);

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
        });
        let mut model = Model::new();
        // Charged per key in the active table, and whether one table is frozen.
        let mut active: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut has_frozen = false;
        let mut last_seq = 0;
        let mut stage = Stage::Idle;

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
                    let fits = charge <= budget && total - held + charge <= budget;

                    let result = if delete {
                        engine.delete(&key)
                    } else {
                        engine.put(&key, &value)
                    };
                    if fits {
                        last_seq += 1;
                        assert_eq!(result, Ok(last_seq), "seed {seed}");
                        active.insert(key.clone(), charge);
                        model.insert(key, (last_seq, (!delete).then_some(value)));
                    } else {
                        assert!(matches!(result, Err(UpdateError::Rejected(_))));
                        rejected += 1;
                    }
                }
                75..=85 => {
                    stage = match stage {
                        Stage::Idle => {
                            let job = engine.begin_flush();
                            assert_eq!(job.is_none(), !has_frozen && active.is_empty());
                            match job {
                                Some(job) => {
                                    if !has_frozen {
                                        has_frozen = true;
                                        active.clear();
                                    }
                                    Stage::Begun(job)
                                }
                                None => Stage::Idle,
                            }
                        }
                        Stage::Begun(job) => Stage::Built(job.build()),
                        Stage::Built(built) => {
                            if rng.below(5) == 0 {
                                engine.set_run_limits(0, 0);
                                built.publish().unwrap_err();
                                engine.set_run_limits(100_000, usize::MAX);
                                refused += 1;
                            } else {
                                built.publish().unwrap();
                                has_frozen = false;
                                flushes += 1;
                            }
                            Stage::Idle
                        }
                    };
                }
                86..=89 if !matches!(stage, Stage::Idle) => {
                    stage = Stage::Idle;
                    abandoned += 1;
                }
                _ => {}
            }
            assert_eq!(engine.stats().frozen.is_some(), has_frozen);
            assert_eq!(engine.stats().last_seq, last_seq);
            assert_engine_matches(&engine, &model, &pool, prefix);
        }
    }

    assert!(flushes > 100, "{flushes}");
    assert!(abandoned > 50, "{abandoned}");
    assert!(refused > 10, "{refused}");
    assert!(rejected > 100, "{rejected}");
}
