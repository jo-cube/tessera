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
    CompactionPolicy, Compression, ENTRY_OVERHEAD, Engine, EngineOptions, EngineStats,
    EngineWorkspace, MAX_KEY_LEN, MergeWorkspace, ReadGuard, UpdateError, WriteError,
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
        max_frozen_memtables: 1,
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

    assert_eq!(engine.put(b"b", &[0; 40]), Ok(2));
    assert_eq!(engine.put(b"c", &[0; 40]), Ok(3));
    engine.flush().unwrap();
    assert_eq!(engine.stats().frozen_tables, 0);
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
    assert_eq!(stats.runs + stats.frozen_tables, 1);
    assert_eq!(stats.active.charged, budget);
    assert_eq!(
        scanned(&engine, b""),
        [live(b"a", a, b"v"), live(b"b", b, b"v")]
    );
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
                let snapshot = engine.snapshot();
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
    // Drain before counting publications: writers may leave frozen tables,
    // and one background flush can coalesce up to two of them.
    engine.flush().unwrap();
    let stats = engine.stats();
    assert_eq!(stats.last_seq, total);
    assert_eq!(stats.frozen_tables, 0);
    // Full tables hold at least half records (the rest can be pinned history).
    // The last publication may contain a partial table.
    let charge = (5 + 100 + ENTRY_OVERHEAD) as u64;
    let full_tables = total * charge / BUDGET as u64;
    assert!(stats.flushes >= full_tables.div_ceil(2), "{stats:?}");
    assert!(stats.flushes <= 2 * full_tables + 2, "{stats:?}");
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

    let engine = Engine::new(EngineOptions { ..manual() }).unwrap();
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

/// Updates interleaved with explicit and automatic flushes. Reads must always equal
/// the single latest-update map: a flush never changes what is visible.
#[test]
fn randomized_operations_match_an_independent_model_across_flushes() {
    const SEEDS: u64 = 40;
    const OPERATIONS: usize = 300;

    let pool = key_pool();
    let long_key = vec![b'x'; MAX_KEY_LEN + 1];
    let (mut rejected, mut flushes) = (0, 0);
    let mut automatic = 0;

    for seed in 0..SEEDS {
        let mut rng = Rng(seed);
        let budget = [1_500, 6_000, 40_000][rng.below(3)];
        let engine = Engine::new(EngineOptions {
            memtable_budget: budget,
            max_frozen_memtables: 2,
            bulk_ingest_min: None,
            background_threads: 1 + rng.below(4),
            level_ratio: [2, 4, 10][rng.below(3)],
            target_run_len: [1, 512, 1 << 20][rng.below(3)],

            last_seq: 0,
            background_compaction: None,

            compression: [Compression::Zstd, Compression::Uncompressed][rng.below(2)],
            target_block_compressed: [64, 2 << 10, 16 << 10][rng.below(3)],
            memory_limit: None,
        })
        .unwrap();
        let mut model = Model::new();
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
                    let result = if delete {
                        engine.delete(&key)
                    } else {
                        engine.put(&key, &value)
                    };
                    if charge <= budget {
                        last_seq += 1;
                        assert_eq!(result, Ok(last_seq), "seed {seed}");
                        automatic += 1;
                        model.insert(key, (last_seq, (!delete).then_some(value)));
                    } else {
                        assert!(matches!(result, Err(UpdateError::Rejected(_))));
                        rejected += 1;
                    }
                }
                75..=85 => {
                    engine.flush().unwrap();
                    flushes += 1;
                }
                _ => {}
            }
            assert_eq!(engine.stats().last_seq, last_seq);
            assert_engine_matches(&engine, &model, &pool, prefix);
        }
    }

    assert!(flushes > 100, "{flushes}");
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
        Some(CompactionPolicy::default())
    );
    assert_eq!(CompactionPolicy::default().max_overlap, 4);
    assert_eq!(
        (defaults.compression, defaults.target_block_compressed),
        (Compression::Zstd, 2048)
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

/// Dictionary compression is accepted with any valid block length, as
/// requested: segment dictionaries and adaptive cuts remain correct,
/// whatever the length.
#[test]
fn adaptive_compression_accepts_every_compressed_target() {
    for target_block_compressed in [1, 4 << 10, 16 << 10, 64 << 10] {
        let engine = Engine::new(EngineOptions {
            compression: Compression::Zstd,
            target_block_compressed,
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
            .core
            .compact(CompactionPolicy::default(), &mut workspace)
            .unwrap()
            .is_some()
        {}
        let mut reads = EngineWorkspace::new();
        let found = engine.get(&7u32.to_be_bytes(), &mut reads).unwrap();
        assert_eq!(
            found.unwrap().value,
            br#"{"id":7,"round":5,"name":"user 7"}"#,
            "{target_block_compressed}"
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
        background_compaction: Some(CompactionPolicy {
            max_overlap: 1,
            ..CompactionPolicy::default()
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
    let snapshots: Vec<_> = (0..3).map(|_| engine.snapshot()).collect();
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
