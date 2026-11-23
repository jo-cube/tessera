//! The integrated engine: visibility across memtables and runs, flush stages,
//! limits, and concurrent use.
//!
//! Expected results come from hash maps maintained by the tests. Concurrency
//! tests use barriers and shared flags and never sleep.

use std::collections::HashMap;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use crate::engine::{
    CompactionPolicy, Compression, ENTRY_OVERHEAD, Engine, EngineStats, ErrorKind, Keyspace,
    MAX_KEY_LEN, MergeWorkspace, Options, Snapshot,
};

/// The defaults without the background worker: these tests flush and
/// compact by hand and count what each step leaves.
fn manual() -> Options {
    Options {
        background_compaction: None,
        ..Options::default()
    }
}

type Live = (Vec<u8>, u64, Vec<u8>);

fn engine() -> Engine {
    Engine::in_memory(manual()).unwrap()
}

fn value_of(engine: &Engine, key: &[u8]) -> Option<(u64, Vec<u8>)> {
    engine.get_seq(key)
}

/// The live records with `prefix` in `snapshot`, with their sequence numbers.
fn scan_prefix(snapshot: &Snapshot, ks: &Keyspace, prefix: &[u8]) -> Vec<Live> {
    let mut reader = snapshot.reader();
    let mut scan = reader.prefix(ks, prefix);
    let mut out = Vec::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.push((record.key.to_vec(), record.seq, record.value.to_vec()));
    }
    out
}

fn scanned(engine: &Engine, prefix: &[u8]) -> Vec<Live> {
    scan_prefix(&engine.snapshot(), &engine.ks(), prefix)
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
    let seq = engine.ks().put_seq(b"a", b"v").unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
}

#[test]
fn overwrite_delete_and_reinsert_each_take_effect_at_once() {
    let engine = engine();
    engine.ks().put(b"a", b"1").unwrap();
    let seq = engine.ks().put_seq(b"a", b"2").unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"2".to_vec())));
    engine.ks().delete(b"a").unwrap();
    assert_eq!(value_of(&engine, b"a"), None);
    let seq = engine.ks().put_seq(b"a", b"3").unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"3".to_vec())));
}

#[test]
fn an_empty_key_and_empty_value_are_ordinary() {
    let engine = engine();
    let seq = engine.ks().put_seq(b"", b"").unwrap();
    assert_eq!(value_of(&engine, b""), Some((seq, Vec::new())));
    engine.flush().unwrap();
    assert_eq!(value_of(&engine, b""), Some((seq, Vec::new())));
}

#[test]
fn a_reused_reader_returns_each_value_alone() {
    let engine = engine();
    let ks = engine.ks();
    ks.put(b"long", b"a longer value").unwrap();
    ks.put(b"short", b"x").unwrap();
    let mut reader = engine.reader();
    assert_eq!(
        reader.get(&ks, b"long").unwrap(),
        Some(&b"a longer value"[..])
    );
    assert_eq!(reader.get(&ks, b"short").unwrap(), Some(&b"x"[..]));
    assert_eq!(ks.get(b"long").unwrap().unwrap(), b"a longer value");
}

// Sequence numbers.

#[test]
fn sequence_numbers_start_after_the_configured_value_and_increase() {
    let engine = Engine::in_memory(Options {
        last_seq: 41,
        ..manual()
    })
    .unwrap();
    assert_eq!(engine.ks().put_seq(b"a", b"v"), Ok(42));
    assert_eq!(engine.ks().delete_seq(b"a"), Ok(43));
    assert_eq!(engine.engine_stats().last_seq, 43);
}

#[test]
fn sequence_numbers_continue_across_flushes() {
    let engine = engine();
    assert_eq!(engine.ks().put_seq(b"a", b"v"), Ok(1));
    engine.flush().unwrap();
    assert_eq!(engine.ks().put_seq(b"a", b"w"), Ok(2));
    engine.flush().unwrap();
    assert_eq!(engine.ks().delete_seq(b"a"), Ok(3));
    engine.flush().unwrap();
    assert_eq!(engine.ks().put_seq(b"b", b"x"), Ok(4));
}

#[test]
fn a_rejected_write_uses_no_sequence_number_and_changes_nothing() {
    let engine = Engine::in_memory(Options {
        memtable_size: 200,
        max_frozen_memtables: 1,
        ..manual()
    })
    .unwrap();
    engine.ks().put(b"a", b"v").unwrap();
    let before = engine.engine_stats();

    let long = vec![b'k'; MAX_KEY_LEN + 1];
    let error = engine.ks().put(&long, b"v").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("key of 65537 bytes"), "{error}");
    // The configured size is reported, whatever the active table's.
    let error = engine.ks().put(b"b", &[0; 300]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        error.to_string().ends_with("memtable size of 200"),
        "{error}"
    );
    assert_eq!(engine.engine_stats(), before);

    assert_eq!(engine.ks().put_seq(b"b", &[0; 40]), Ok(2));
    assert_eq!(engine.ks().put_seq(b"c", &[0; 40]), Ok(3));
    engine.flush().unwrap();
    assert_eq!(engine.engine_stats().frozen_tables, 0);
}

#[test]
fn the_last_sequence_number_is_used_once() {
    let engine = Engine::in_memory(Options {
        last_seq: u64::MAX - 1,
        ..manual()
    })
    .unwrap();
    assert_eq!(engine.ks().put_seq(b"a", b"v"), Ok(u64::MAX));
    assert_eq!(value_of(&engine, b"a"), Some((u64::MAX, b"v".to_vec())));
    engine.flush().unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((u64::MAX, b"v".to_vec())));
}

#[test]
#[should_panic(expected = "sequence numbers are exhausted")]
fn a_write_after_the_largest_sequence_number_panics() {
    let engine = Engine::in_memory(Options {
        last_seq: u64::MAX,
        ..manual()
    })
    .unwrap();
    let _ = engine.ks().put(b"a", b"v");
}

// Precedence across sources.

#[test]
fn a_read_of_a_flushed_key_decodes_a_block() {
    let engine = engine();
    let ks = engine.ks();
    ks.put(b"a", b"v").unwrap();
    let mut reader = engine.reader();
    reader.get(&ks, b"a").unwrap();
    assert_eq!(reader.counters().blocks_loaded, 0);
    engine.flush().unwrap();
    reader.get(&ks, b"a").unwrap();
    assert_eq!(reader.counters().blocks_loaded, 1);
}

#[test]
fn a_newer_table_update_hides_older_runs_without_decoding_them() {
    let engine = engine();
    engine.ks().put(b"a", b"old").unwrap();
    engine.flush().unwrap();
    let seq = engine.ks().put_seq(b"a", b"new").unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"new".to_vec())));
    let mut reader = engine.reader();
    assert_eq!(reader.get(&engine.ks(), b"a").unwrap(), Some(&b"new"[..]));
    assert_eq!(reader.counters().blocks_loaded, 0);
}

#[test]
fn the_newest_run_decides_a_key_written_across_several_flushes() {
    let engine = engine();
    engine.ks().put(b"a", b"1").unwrap();
    engine.flush().unwrap();
    let seq = engine.ks().put_seq(b"a", b"2").unwrap();
    engine.flush().unwrap();
    assert_eq!(value_of(&engine, b"a"), Some((seq, b"2".to_vec())));
    assert_eq!(engine.engine_stats().runs, 2);
}

#[test]
fn a_tombstone_in_the_active_table_hides_a_value_in_a_run() {
    let engine = engine();
    engine.ks().put(b"a", b"v").unwrap();
    engine.flush().unwrap();
    engine.ks().delete(b"a").unwrap();
    assert_eq!(value_of(&engine, b"a"), None);
    assert_eq!(scanned(&engine, b""), []);
}

#[test]
fn a_flushed_tombstone_hides_a_value_in_an_older_run() {
    let engine = engine();
    engine.ks().put(b"a", b"v").unwrap();
    engine.flush().unwrap();
    engine.ks().delete(b"a").unwrap();
    engine.flush().unwrap();
    assert_eq!(value_of(&engine, b"a"), None);
    assert_eq!(scanned(&engine, b""), []);
    let seq = engine.ks().put_seq(b"a", b"back").unwrap();
    assert_eq!(scanned(&engine, b""), [live(b"a", seq, b"back")]);
}

// Flush stages.

#[test]
fn an_empty_flush_creates_nothing() {
    let engine = engine();
    assert_eq!(engine.flush().unwrap().tables, 0);
    let stats = engine.engine_stats();
    assert!(no_frozen(stats));
    assert_eq!(stats.runs, 0);
}

#[test]
fn a_repeated_flush_adds_nothing() {
    let engine = engine();
    engine.ks().put(b"a", b"v").unwrap();
    assert_eq!(engine.flush().unwrap().tables, 1);
    assert_eq!(engine.flush().unwrap().tables, 0);
    assert_eq!(engine.engine_stats().runs, 1);
}

#[test]
fn a_flush_splits_output_at_the_configured_run_length() {
    let engine = Engine::in_memory(Options {
        target_run_len: 1,
        ..manual()
    })
    .unwrap();
    engine.ks().put(b"a", b"v").unwrap();
    engine.ks().put(b"b", b"v").unwrap();
    assert_eq!(engine.flush().unwrap().runs, 2);
    assert_eq!(scanned(&engine, b"").len(), 2);
}

#[test]
fn a_report_describes_what_was_published() {
    let engine = engine();
    engine.ks().put(b"a", b"v").unwrap();
    engine.ks().delete(b"b").unwrap();
    let report = engine.flush().unwrap();
    let stats = engine.engine_stats();
    assert_eq!((report.tables, report.records, report.runs), (1, 2, 1));
    assert_eq!(report.run_bytes, stats.run_bytes);
    assert!(report.run_bytes > 0);
}

// Budgets.

#[test]
fn a_write_to_a_full_table_flushes_it_and_lands_in_the_next() {
    // One record: its keyspace prefix, key and value bytes.
    let budget = 1 + 1 + 3 + ENTRY_OVERHEAD;
    let engine = Engine::in_memory(Options {
        memtable_size: budget,
        ..manual()
    })
    .unwrap();
    let a = engine.ks().put_seq(b"a", b"vvv").unwrap();
    let b = engine.ks().put_seq(b"b", b"vvv").unwrap();
    assert_eq!(b, a + 1);
    let stats = engine.engine_stats();
    assert_eq!(stats.runs + stats.frozen_tables, 1);
    assert_eq!(stats.active.charged, budget);
    assert_eq!(
        scanned(&engine, b""),
        [live(b"a", a, b"vvv"), live(b"b", b, b"vvv")]
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
    let engine = Engine::in_memory(Options {
        memtable_size: BUDGET,
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
                        .map(|at| {
                            engine
                                .ks()
                                .put_seq(&key(writer, at), &[writer; 100])
                                .unwrap()
                        })
                        .collect::<Vec<u64>>()
                })
            })
            .collect();
        let reader = scope.spawn(|| {
            let mut checked = 0;
            while !done.load(Ordering::Acquire) || checked == 0 {
                let snapshot = engine.snapshot();
                let mut reader = snapshot.reader();
                let ks = engine.ks();
                let mut seen = 0;
                for writer in 0..WRITERS {
                    let mut scan = reader.prefix(&ks, &[writer]);
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
    let stats = engine.engine_stats();
    assert_eq!(stats.last_seq, total);
    assert_eq!(stats.frozen_tables, 0);
    // Full tables hold at least half records (the rest can be pinned history).
    // The last publication may contain a partial table.
    let charge = (5 + 100 + ENTRY_OVERHEAD) as u64;
    let full_tables = total * charge / BUDGET as u64;
    assert!(stats.flushes >= full_tables.div_ceil(2), "{stats:?}");
    assert!(stats.flushes <= 2 * full_tables + 2, "{stats:?}");
    assert_eq!(stats.runs as u64, stats.flushes);
    for writer in 0..WRITERS {
        for at in [0, EACH / 2, EACH - 1] {
            let found = engine.ks().get(&key(writer, at)).unwrap().unwrap();
            assert_eq!(found, [writer; 100]);
        }
    }
}

// Scans.

#[test]
fn scans_return_each_key_once_in_bytewise_order() {
    let engine = engine();
    let keys: [&[u8]; 7] = [b"\xFF", b"b", b"a\x00", b"ab", b"a", b"\x00", b""];
    for (i, key) in keys.iter().enumerate() {
        engine.ks().put(key, &[i as u8]).unwrap();
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
        engine.ks().put(key, b"run").unwrap();
    }
    engine.flush().unwrap();
    for key in [&b"\xFF\xFE"[..], b"\xFF\xFF\x00", b"\xFE\xFF", b"a"] {
        engine.ks().put(key, b"table").unwrap();
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
    let mut reader = engine.reader();
    let mut scan = reader.range(&engine.ks(), ..);
    let mut count = 0;
    while scan.next().unwrap().is_some() {
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
    engine.ks().put(b"a", b"v").unwrap();
    let mut reader = engine.reader();
    let mut scan = reader.prefix(&engine.ks(), b"");
    assert!(scan.next().unwrap().is_some());
    assert_eq!(scan.next().unwrap(), None);
    assert_eq!(scan.next().unwrap(), None);
}

#[test]
fn a_prefix_scan_of_a_table_only_key_decodes_no_unrelated_run() {
    let engine = engine();
    engine.ks().put(b"a1", b"run").unwrap();
    engine.flush().unwrap();
    engine.ks().put(b"b1", b"table").unwrap();
    let mut reader = engine.reader();
    let mut scan = reader.prefix(&engine.ks(), b"b");
    assert_eq!(scan.next().unwrap().unwrap().0, b"b1");
    assert_eq!(scan.next().unwrap(), None);
    drop(scan);
    assert_eq!(reader.counters().blocks_loaded, 0);
}

#[test]
fn reads_through_a_snapshot_see_one_state() {
    let engine = engine();
    let ks = engine.ks();
    let seq = ks.put_seq(b"a", b"v").unwrap();
    let snapshot = engine.snapshot();
    ks.put(b"a", b"later").unwrap();
    ks.put(b"b", b"later").unwrap();
    assert_eq!(snapshot.get_seq(b"a"), Some((seq, b"v".to_vec())));
    let mut reader = snapshot.reader();
    assert_eq!(reader.get(&ks, b"a").unwrap(), Some(&b"v"[..]));
    assert_eq!(reader.get(&ks, b"b").unwrap(), None);
}

// Concurrent use.

#[test]
fn an_engine_can_be_shared_between_threads_by_type() {
    fn shareable<T: Send + Sync>() {}
    shareable::<Engine>();
}

#[test]
fn a_held_snapshot_stays_stable_while_a_writer_and_a_flush_proceed() {
    let engine = engine();
    let ks = engine.ks();
    let seq = ks.put_seq(b"a", b"v").unwrap();
    let snapshot = engine.snapshot();
    thread::scope(|scope| {
        scope.spawn(|| {
            ks.put(b"a", b"replaced").unwrap();
            ks.put(b"new", b"v").unwrap();
            engine.flush().unwrap();
        });
    });
    assert_eq!(scan_prefix(&snapshot, &ks, b""), [live(b"a", seq, b"v")]);
    assert_eq!(engine.engine_stats().runs, 1);
    assert_eq!(value_of(&engine, b"a").unwrap().1, b"replaced");
    assert!(value_of(&engine, b"new").is_some());
}

#[test]
fn concurrent_flushes_each_complete_and_lose_nothing() {
    let engine = engine();
    let start = Barrier::new(2);
    engine.ks().put(b"a", b"v").unwrap();
    thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| {
                start.wait();
                engine.flush().unwrap();
            });
        }
    });
    assert_eq!(engine.engine_stats().runs, 1);
    assert!(no_frozen(engine.engine_stats()));
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

    let engine = Engine::in_memory(Options { ..manual() }).unwrap();
    for writer in 0..WRITERS {
        engine
            .ks()
            .put(&pair_key(writer, "a"), &0u64.to_be_bytes())
            .unwrap();
        engine
            .ks()
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
                let ks = engine.ks();
                let mut seen = Vec::new();
                start.wait();
                for n in 1..=WRITES {
                    let first = ks.put_seq(&a, &n.to_be_bytes()).unwrap();
                    let (seq, value) = engine.get_seq(&a).unwrap();
                    assert_eq!(seq, first, "read your own write");
                    assert_eq!(counter(&value), n);
                    seen.push(first);
                    seen.push(ks.put_seq(&b, &n.to_be_bytes()).unwrap());
                }
                writers_left.fetch_sub(1, Ordering::SeqCst);
                seen
            }));
        }

        scope.spawn(|| {
            let ks = engine.ks();
            let mut reader = engine.reader();
            let mut last = [0u64; WRITERS];
            start.wait();
            while writers_left.load(Ordering::SeqCst) > 0 {
                for (writer, last) in last.iter_mut().enumerate() {
                    let key = pair_key(writer, "a");
                    let found = reader.get(&ks, &key).unwrap();
                    let now = counter(found.expect("a written key never vanishes"));
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
    for key in pool {
        let found = engine.get_seq(key);
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
        let engine = Engine::in_memory(Options {
            memtable_size: budget,
            max_frozen_memtables: 2,
            bulk_ingest_min: None,
            background_threads: 1 + rng.below(4),
            level_ratio: [2, 4, 10][rng.below(3)],
            target_run_len: [1, 512, 1 << 20][rng.below(3)],

            last_seq: 0,
            background_compaction: None,

            compression: [Compression::Zstd, Compression::Uncompressed][rng.below(2)],
            block_size: [64, 2 << 10, 16 << 10][rng.below(3)],
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
                    let error = engine.ks().put(&long_key, b"v").unwrap_err();
                    assert_eq!(error.kind(), ErrorKind::InvalidInput);
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
                        engine.ks().delete_seq(&key)
                    } else {
                        engine.ks().put_seq(&key, &value)
                    };
                    if charge <= budget {
                        last_seq += 1;
                        assert_eq!(result, Ok(last_seq), "seed {seed}");
                        automatic += 1;
                        model.insert(key, (last_seq, (!delete).then_some(value)));
                    } else {
                        assert!(result.is_err());
                        rejected += 1;
                    }
                }
                75..=85 => {
                    engine.flush().unwrap();
                    flushes += 1;
                }
                _ => {}
            }
            assert_eq!(engine.engine_stats().last_seq, last_seq);
            assert_engine_matches(&engine, &model, &pool, prefix);
        }
    }

    assert!(flushes > 100, "{flushes}");
    assert!(rejected > 20, "{rejected}");
    assert!(automatic > 50, "{automatic}");
}

// Configuration.

/// The defaults are valid, run a worker with the default compaction
/// options, and keep the overlap within its allowance with no explicit
/// compaction step.
#[test]
fn the_defaults_compact_in_the_background() {
    let defaults = Options::default();
    assert_eq!(
        defaults.background_compaction,
        Some(CompactionPolicy::default())
    );
    assert_eq!(CompactionPolicy::default().max_overlap, 4);
    assert_eq!(
        (defaults.compression, defaults.block_size),
        (Compression::Zstd, 1024)
    );
    let engine = Engine::in_memory(Options {
        memtable_size: 16 << 10,
        ..defaults
    })
    .unwrap();
    assert!(engine.background_status().unwrap().running);
    // Eight flushes of the same keys: eight overlapping runs, unless
    // compacted.
    for round in 0..8u32 {
        for i in 0..50u32 {
            engine
                .ks()
                .put(&i.to_be_bytes(), &round.to_be_bytes())
                .unwrap();
        }
        engine.flush().unwrap();
    }
    engine.wait_for_compaction().unwrap();
    let status = engine.background_status().unwrap();
    assert!(status.compactions >= 1, "{status:?}");
    assert!(
        engine.engine_stats().max_overlap <= 4,
        "{:?}",
        engine.engine_stats()
    );
    for i in 0..50u32 {
        let found = engine.ks().get(&i.to_be_bytes()).unwrap();
        assert_eq!(found.unwrap(), 7u32.to_be_bytes());
    }
}

/// The smallest budget admits an empty record behind the longest, 5-byte
/// keyspace prefix: in a keyspace with a 1-byte prefix, a 4-byte key.
#[test]
fn the_smallest_budget_admits_the_smallest_record() {
    let engine = Engine::in_memory(Options {
        memtable_size: ENTRY_OVERHEAD + 5,
        ..manual()
    })
    .unwrap();
    engine.ks().delete(b"").unwrap();
    engine.ks().delete(b"kkkk").unwrap();
    let error = engine.ks().put(b"kkkkk", b"").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

/// Dictionary compression is accepted with any valid block length, as
/// requested: segment dictionaries and adaptive cuts remain correct,
/// whatever the length.
#[test]
fn adaptive_compression_accepts_every_compressed_target() {
    for block_size in [1, 4 << 10, 16 << 10, 64 << 10] {
        let engine = Engine::in_memory(Options {
            compression: Compression::Zstd,
            block_size,
            ..manual()
        })
        .unwrap();
        for round in 0..6u32 {
            for i in 0..200u32 {
                let value = format!(r#"{{"id":{i},"round":{round},"name":"user {i}"}}"#);
                engine.ks().put(&i.to_be_bytes(), value.as_bytes()).unwrap();
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
        let found = engine.ks().get(&7u32.to_be_bytes()).unwrap();
        assert_eq!(
            found.unwrap(),
            br#"{"id":7,"round":5,"name":"user 7"}"#,
            "{block_size}"
        );
    }
}

// Lifecycle.

/// Closing an idle engine succeeds, with or without a worker.
#[test]
fn closing_an_idle_engine_succeeds() {
    for options in [Options::default(), manual()] {
        let engine = Engine::in_memory(options).unwrap();
        engine.ks().put(b"k", b"v").unwrap();
        engine.close().unwrap();
    }
}

/// Snapshots outlive the engine: readers on other threads keep reading
/// what they saw while the engine is closed beside them, including during
/// a background compaction it waits for.
#[test]
fn snapshots_stay_readable_while_and_after_the_engine_closes() {
    let engine = Engine::in_memory(Options {
        memtable_size: 16 << 10,
        background_compaction: Some(CompactionPolicy {
            max_overlap: 1,
            ..CompactionPolicy::default()
        }),
        ..Options::default()
    })
    .unwrap();
    for round in 0..4u32 {
        for i in 0..50u32 {
            engine
                .ks()
                .put(&i.to_be_bytes(), &round.to_be_bytes())
                .unwrap();
        }
        engine.flush().unwrap();
    }
    let ks = engine.ks();
    let snapshots: Vec<_> = (0..3).map(|_| engine.snapshot()).collect();
    let start = Barrier::new(snapshots.len() + 1);
    thread::scope(|scope| {
        let readers: Vec<_> = snapshots
            .iter()
            .map(|snapshot| {
                let (start, ks) = (&start, &ks);
                scope.spawn(move || {
                    start.wait();
                    let mut reader = snapshot.reader();
                    for _ in 0..20 {
                        let mut scan = reader.range(ks, ..);
                        let mut seen = 0u32;
                        while let Some((_, value)) = scan.next().unwrap() {
                            assert_eq!(value, 3u32.to_be_bytes());
                            seen += 1;
                        }
                        assert_eq!(seen, 50);
                    }
                })
            })
            .collect();
        start.wait();
        engine.close().unwrap();
        for reader in readers {
            reader.join().unwrap();
        }
    });
    let found = snapshots[0]
        .reader()
        .get(&ks, &7u32.to_be_bytes())
        .unwrap()
        .map(<[u8]>::to_vec);
    assert_eq!(found.unwrap(), 3u32.to_be_bytes());
}
