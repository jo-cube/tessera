//! Engine tests that stage a flush or compaction: they hold it between its
//! steps while reads, writes and snapshots go on, or abandon it part way.
//! Staging is internal, so these tests live in the crate; everything else
//! about the engine is tested through its public API under `tests/`.
//!
//! A held flush blocks every other flush, including a write's that needs
//! room, so these tests write only what fits.

use std::collections::BTreeMap;
use std::thread;

use super::*;

/// The defaults without the background worker: these tests flush and
/// compact by hand and count what each step leaves.
fn manual() -> Options {
    Options {
        background_compaction: None,
        ..Options::default()
    }
}

/// Flush stages: what reads see while tables are frozen, built and published.
mod flush {
    use super::*;

    type Live = (Vec<u8>, u64, Vec<u8>);

    fn engine() -> Engine {
        Engine::in_memory(manual()).unwrap()
    }

    fn value_of(engine: &Engine, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        engine.get_seq(key)
    }

    fn scanned(engine: &Engine, prefix: &[u8]) -> Vec<Live> {
        engine.snapshot().records(prefix)
    }

    fn live(key: &[u8], seq: u64, value: &[u8]) -> Live {
        (key.to_vec(), seq, value.to_vec())
    }

    fn no_frozen(stats: EngineStats) -> bool {
        stats.frozen.is_none()
    }

    #[test]
    fn a_tombstone_in_the_frozen_table_hides_a_value_in_a_run() {
        let engine = engine();
        engine.ks().put(b"a", b"v").unwrap();
        engine.flush().unwrap();
        engine.ks().delete(b"a").unwrap();
        let job = engine.core.begin_flush().unwrap();
        assert_eq!(value_of(&engine, b"a"), None);
        assert_eq!(scanned(&engine, b""), []);
        drop(job);
    }

    #[test]
    fn every_flush_stage_shows_the_same_reads() {
        let engine = engine();
        let a = engine.ks().put_seq(b"a", b"1").unwrap();
        engine.flush().unwrap();
        let b = engine.ks().put_seq(b"b", b"2").unwrap();
        let expected = [live(b"a", a, b"1"), live(b"b", b, b"2")];
        assert_eq!(scanned(&engine, b""), expected);

        let job = engine.core.begin_flush().unwrap();
        assert_eq!(scanned(&engine, b""), expected);
        assert_eq!(value_of(&engine, b"b"), Some((b, b"2".to_vec())));
        assert!(engine.engine_stats().frozen.is_some());

        let built = job.build();
        assert_eq!(built.runs().count(), 1);
        assert_eq!(scanned(&engine, b""), expected);
        assert_eq!(
            engine.engine_stats().runs,
            1,
            "built runs are not visible yet"
        );

        built.publish();
        assert_eq!(scanned(&engine, b""), expected);
        assert_eq!(value_of(&engine, b"b"), Some((b, b"2".to_vec())));
        let stats = engine.engine_stats();
        assert!(no_frozen(stats));
        assert_eq!(stats.runs, 2);
    }

    #[test]
    fn writes_during_a_flush_land_in_the_new_table_and_win() {
        let engine = engine();
        engine.ks().put(b"a", b"frozen").unwrap();
        engine.ks().put(b"b", b"frozen").unwrap();
        let job = engine.core.begin_flush().unwrap();

        let a = engine.ks().put_seq(b"a", b"during").unwrap();
        engine.ks().delete(b"b").unwrap();
        let c = engine.ks().put_seq(b"c", b"during").unwrap();
        let expected = [live(b"a", a, b"during"), live(b"c", c, b"during")];
        assert_eq!(scanned(&engine, b""), expected);

        let built = job.build();
        assert_eq!(scanned(&engine, b""), expected);
        built.publish();
        assert_eq!(scanned(&engine, b""), expected);
        assert_eq!(value_of(&engine, b"b"), None);
        // Each key carries its keyspace's one-byte prefix.
        assert_eq!(engine.engine_stats().active.payload, 2 + 6 + 2 + 2 + 6);
    }

    #[test]
    fn a_frozen_table_receives_no_later_writes() {
        let engine = engine();
        engine.ks().put(b"a", b"frozen").unwrap();
        let job = engine.core.begin_flush().unwrap();
        engine.ks().put(b"a", b"during").unwrap();
        assert_eq!(job.tables.iter().map(|table| table.len()).sum::<usize>(), 1);
        let built = job.build();
        assert_eq!(built.runs().next().unwrap().len(), 1);
        built.publish();
        assert_eq!(value_of(&engine, b"a").unwrap().1, b"during");
    }

    #[test]
    fn an_abandoned_flush_keeps_the_table_readable_and_is_retried() {
        let engine = engine();
        let seq = engine.ks().put_seq(b"a", b"v").unwrap();
        drop(engine.core.begin_flush().unwrap());
        assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
        assert!(engine.engine_stats().frozen.is_some());

        drop(engine.core.begin_flush().unwrap().build());
        assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
        assert_eq!(engine.engine_stats().runs, 0);

        engine.core.begin_flush().unwrap().build().publish();
        assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
        assert!(no_frozen(engine.engine_stats()));
        assert_eq!(engine.engine_stats().runs, 1);
    }

    #[test]
    fn a_full_flush_writes_a_retried_table_and_the_active_one_as_one_set_of_runs() {
        let engine = engine();
        engine.ks().put(b"a", b"v").unwrap();
        drop(engine.core.begin_flush().unwrap());
        engine.ks().put(b"b", b"v").unwrap();

        let report = engine.flush().unwrap();
        assert_eq!((report.tables, report.records, report.runs), (2, 2, 1));
        let stats = engine.engine_stats();
        assert!(no_frozen(stats));
        assert_eq!(stats.active.payload, 0);
        assert_eq!(scanned(&engine, b"").len(), 2);
    }

    #[test]
    fn a_flush_can_follow_a_panic_during_an_earlier_one() {
        let engine = engine();
        engine.ks().put(b"a", b"v").unwrap();
        let result = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _job = engine.core.begin_flush().unwrap();
                    panic!("flush interrupted");
                })
                .join()
        });
        assert!(result.is_err());
        assert_eq!(engine.flush().unwrap().tables, 1);
        assert_eq!(value_of(&engine, b"a").unwrap().1, b"v");
    }

    #[test]
    fn a_flush_accounts_for_each_table_separately() {
        let engine = engine();
        engine.ks().put(b"a", b"12345").unwrap();
        let job = engine.core.begin_flush().unwrap();
        engine.ks().put(b"bb", b"1").unwrap();
        let stats = engine.engine_stats();
        // Each key carries its keyspace's one-byte prefix.
        assert_eq!(stats.frozen.unwrap().payload, 7);
        assert_eq!(stats.active.payload, 4);
        job.build().publish();
        let stats = engine.engine_stats();
        assert_eq!(stats.active.payload, 4);
        assert!(stats.run_bytes > 0);
    }

    /// Fills runs and an active table that the caller then freezes, and returns
    /// the live records.
    fn fill_mixed(engine: &Engine) -> Vec<Live> {
        let k1 = engine.ks().put_seq(b"k1", b"run").unwrap();
        engine.ks().put(b"k2", b"run").unwrap();
        engine.ks().put(b"k3", b"run").unwrap();
        engine.ks().put(b"k4", b"run").unwrap();
        engine.flush().unwrap();
        let k2 = engine.ks().put_seq(b"k2", b"frozen").unwrap();
        engine.ks().delete(b"k3").unwrap();
        let k4 = engine.ks().put_seq(b"k4", b"frozen").unwrap();
        let k5 = engine.ks().put_seq(b"k5", b"frozen").unwrap();
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
        let _job = engine.core.begin_flush().unwrap();
        engine.ks().delete(b"k5").unwrap();
        let k6 = engine.ks().put_seq(b"k6", b"active").unwrap();
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
        let _job = engine.core.begin_flush().unwrap();
        let seq = engine.ks().put_seq(b"k2", b"active").unwrap();
        assert_eq!(scanned(&engine, b"k2"), [live(b"k2", seq, b"active")]);
    }

    #[test]
    fn reads_and_writes_proceed_while_a_flush_has_built_its_runs() {
        let engine = engine();
        let seq = engine.ks().put_seq(b"a", b"v").unwrap();
        let built: BuiltFlush<'_> = engine.core.begin_flush().unwrap().build();

        thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert_eq!(value_of(&engine, b"a"), Some((seq, b"v".to_vec())));
                    engine.ks().put(b"b", b"during").unwrap();
                    assert_eq!(value_of(&engine, b"b").unwrap().1, b"during");
                })
                .join()
                .unwrap();
        });
        built.publish();
        assert_eq!(scanned(&engine, b"").len(), 2);
    }
}

/// Snapshots taken at every stage of a flush and a compaction.
mod snapshot {
    use super::*;

    type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

    fn pairs(items: &[(&[u8], &[u8])]) -> Pairs {
        items
            .iter()
            .map(|&(k, v)| (k.to_vec(), v.to_vec()))
            .collect()
    }

    fn scanned(snapshot: &Snapshot, prefix: &[u8]) -> Pairs {
        let records = snapshot.records(prefix);
        assert!(
            records
                .iter()
                .all(|(_, seq, _)| *seq <= snapshot.last_seq())
        );
        records
            .into_iter()
            .map(|(key, _, value)| (key, value))
            .collect()
    }

    fn value(snapshot: &Snapshot, key: &[u8]) -> Option<Vec<u8>> {
        snapshot.value(key)
    }

    fn merge_all() -> CompactionPolicy {
        CompactionPolicy {
            max_overlap: 1,
            ..CompactionPolicy::default()
        }
    }

    /// A snapshot taken while a flush or compaction is part way through sees
    /// the same state as one taken before or after it.
    #[test]
    fn no_source_transition_leaves_a_gap() {
        let engine = Engine::in_memory(manual()).unwrap();
        let mut expected: Pairs = Vec::new();
        let mut held: Vec<(Snapshot, Pairs)> = Vec::new();
        let mut take = |engine: &Engine, expected: &Pairs| {
            let snapshot = engine.snapshot();
            assert_eq!(&scanned(&snapshot, b""), expected);
            held.push((snapshot, expected.clone()));
        };
        for round in 0..3u8 {
            for key in [&b"a"[..], b"m", b"z"] {
                engine.ks().put(key, &[round]).unwrap();
            }
            engine.ks().delete(b"m").unwrap();
            expected = pairs(&[(b"a", &[round]), (b"z", &[round])]);
            take(&engine, &expected);
            let job = engine.core.begin_flush().unwrap();
            take(&engine, &expected);
            let built = job.build();
            take(&engine, &expected);
            built.publish();
            take(&engine, &expected);
        }
        let job = engine.core.begin_job(merge_all()).unwrap().unwrap();
        take(&engine, &expected);
        let built = job.build(&mut MergeWorkspace::new()).unwrap();
        take(&engine, &expected);
        built.publish().unwrap();
        take(&engine, &expected);
        assert_eq!(engine.engine_stats().runs, 1);
        for (snapshot, then) in &held {
            assert_eq!(&scanned(snapshot, b""), then);
            assert_eq!(value(snapshot, b"m"), None);
        }
    }
}

/// Compactions whose selection, build and publication are separated by
/// writes, flushes and snapshots.
mod compaction {
    use super::*;

    /// Merges any overlap, up to `max_inputs` runs at a time.
    fn options(max_inputs: usize) -> CompactionPolicy {
        CompactionPolicy {
            max_inputs,
            max_overlap: 1,
            ..CompactionPolicy::default()
        }
    }

    fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
        engine.ks().get(key).unwrap()
    }

    fn everything(engine: &Engine) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut reader = engine.reader();
        let mut scan = reader.range(&engine.ks(), ..);
        let mut out = Vec::new();
        while let Some((key, value)) = scan.next().unwrap() {
            out.push((key.to_vec(), value.to_vec()));
        }
        out
    }

    type Model = BTreeMap<Vec<u8>, Vec<u8>>;

    fn snapshot_contents(snapshot: &Snapshot) -> Vec<(Vec<u8>, Vec<u8>)> {
        snapshot
            .records(b"")
            .into_iter()
            .map(|(key, _, value)| (key, value))
            .collect()
    }

    /// A snapshot equals the model as it was when the snapshot was taken.
    fn check_snapshot(snapshot: &Snapshot, then: &Model, pool: &[Vec<u8>]) {
        let expected: Vec<_> = then.clone().into_iter().collect();
        assert_eq!(snapshot_contents(snapshot), expected);
        for key in pool {
            assert_eq!(snapshot.value(key), then.get(key).cloned());
        }
    }

    fn compact_fully(engine: &Engine) {
        let mut workspace = MergeWorkspace::new();
        let options = CompactionPolicy {
            max_inputs: usize::MAX,
            max_input_bytes: usize::MAX,
            max_workspace_bytes: usize::MAX,
            ..options(0)
        };
        while engine
            .core
            .compact(options, &mut workspace)
            .unwrap()
            .is_some()
        {}
    }

    /// Runs flushed between selection and publication, and the memtables, are
    /// newer than every input. They stay published and keep winning.
    #[test]
    fn publication_keeps_what_was_flushed_and_written_meanwhile() {
        let engine = Engine::in_memory(manual()).unwrap();
        for round in 0..3 {
            engine
                .ks()
                .put(b"a", format!("a{round}").as_bytes())
                .unwrap();
            engine.ks().put(b"b", b"b").unwrap();
            engine.flush().unwrap();
        }
        let job = engine.core.begin_job(options(8)).unwrap().unwrap();
        engine.ks().delete(b"a").unwrap();
        engine.flush().unwrap();
        let built = job.build(&mut MergeWorkspace::new()).unwrap();
        engine.ks().put(b"c", b"c").unwrap();
        engine.flush().unwrap();
        engine.ks().put(b"b", b"memtable").unwrap();

        let before = engine.engine_stats();
        let report = built.publish().unwrap();
        assert_eq!((report.inputs, report.outputs), (3, 1));
        let after = engine.engine_stats();
        assert_eq!(after.runs, before.runs - 3 + 1);
        assert_eq!(
            after.run_bytes,
            before.run_bytes - report.input_bytes + report.output_bytes
        );
        assert_eq!(
            everything(&engine),
            [
                (b"b".to_vec(), b"memtable".to_vec()),
                (b"c".to_vec(), b"c".to_vec())
            ]
        );
    }

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

    struct Rng(u64);

    enum Stage<'e> {
        Idle,
        Begun(CompactionJob<'e>),
        Built(BuiltCompaction<'e>),
    }

    /// Writes, flushes, snapshots and partial compactions, staged so that writes
    /// and flushes land between selection, build and publication. After every
    /// step reads equal the latest-update map, every report adds up, and a
    /// retained snapshot still equals the map as it was when it was taken.
    #[test]
    fn randomized_compactions_match_an_independent_model() {
        const SEEDS: u64 = 30;
        const OPERATIONS: usize = 400;
        const BACKGROUND_SEEDS: u64 = 10;

        let pool: Vec<Vec<u8>> = (0..40u8)
            .map(|i| vec![b'k', i / 4, i])
            .chain([Vec::new(), vec![0xFF; 3]])
            .collect();
        let (mut compactions, mut incomplete, mut dropped_tombstones) = (0, 0, 0);
        let (mut snapshots, mut outlived) = (0, 0);
        // Jobs into the bottom level that an unselected run may contest.
        let mut contested = 0;

        // Then a few seeds again with a background worker compacting beside the
        // staged manual compactions. Counts come from the manual seeds only.
        let runs = (0..SEEDS).map(|seed| (seed, false));
        for (seed, background) in runs.chain((0..BACKGROUND_SEEDS).map(|seed| (seed, true))) {
            let mut rng = Rng(seed);
            let engine = Engine::in_memory(Options {
                target_run_len: [64, 1_024, 1 << 20][rng.below(3)],

                background_compaction: background.then(|| CompactionPolicy {
                    max_overlap: 1 + seed as usize % 3,
                    ..CompactionPolicy::default()
                }),
                // Odd seeds use small memtables, which the history pinned by
                // snapshots fills, so writes flush them.
                memtable_size: if seed % 2 == 0 { 64 << 20 } else { 4 << 10 },
                ..manual()
            })
            .unwrap();
            // Snapshots taken before the first write read the keyspace too.
            engine.ks();
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            let mut stage = Stage::Idle;
            let mut workspace = MergeWorkspace::new();
            let mut held: Vec<(Snapshot, Model)> = Vec::new();

            for _ in 0..OPERATIONS {
                match rng.below(100) {
                    55..=59 if held.len() >= 6 || (!held.is_empty() && rng.below(3) == 0) => {
                        held.swap_remove(rng.below(held.len()));
                    }
                    55..=59 => {
                        let snapshot = engine.snapshot();
                        assert_eq!(snapshot.last_seq(), engine.engine_stats().last_seq);
                        held.push((snapshot, model.clone()));
                        snapshots += 1;
                    }
                    0..=54 => {
                        let key = pool[rng.below(pool.len())].clone();
                        let value = (rng.below(4) != 0).then(|| {
                            let len = if rng.below(10) == 0 { 3_000 } else { 12 };
                            (0..rng.below(len))
                                .map(|_| rng.next() as u8)
                                .collect::<Vec<u8>>()
                        });
                        let write = |engine: &Engine| match &value {
                            Some(value) => engine.ks().put(&key, value),
                            None => engine.ks().delete(&key),
                        };
                        // A write with no room flushes first.
                        if let Err(error) = write(&engine) {
                            panic!("seed {seed}: {error}");
                        }
                        match value {
                            Some(value) => model.insert(key, value),
                            None => model.remove(&key),
                        };
                    }
                    60..=74 => {
                        engine.flush().unwrap();
                    }
                    _ => {
                        let options = CompactionPolicy {
                            max_inputs: 2 + rng.below(4),
                            max_overlap: 1 + rng.below(3),
                            ..CompactionPolicy::default()
                        };
                        stage = match stage {
                            Stage::Idle => {
                                match engine.core.begin_compaction(options, None).unwrap() {
                                    Some(Begun::Job(job)) => {
                                        if !background && !job.seq_floors().is_empty() {
                                            contested += 1;
                                        }
                                        Stage::Begun(*job)
                                    }
                                    // A move publishes as it is chosen.
                                    Some(Begun::Done(_)) | None => Stage::Idle,
                                }
                            }
                            Stage::Begun(job) => Stage::Built(job.build(&mut workspace).unwrap()),
                            Stage::Built(built) => {
                                let ranges: Vec<_> = built
                                    .output
                                    .runs
                                    .iter()
                                    .map(|run| run.key_range().unwrap())
                                    .map(|range| (range.min.to_vec(), range.max.to_vec()))
                                    .collect();
                                assert!(ranges.windows(2).all(|pair| pair[0].1 < pair[1].0));
                                // Even manual engines have a flush worker. Keep
                                // its publications outside this accounting pair.
                                let _flush = (!background).then(|| engine.core.serialize_flush());
                                let before = engine.engine_stats();
                                let report = built.publish().unwrap();
                                let after = engine.engine_stats();
                                // The worker may compact as soon as this
                                // compaction releases the lock.
                                if !background {
                                    assert_eq!(
                                        after.runs,
                                        before.runs - report.inputs + report.outputs
                                    );
                                    assert_eq!(
                                        after.run_bytes,
                                        before.run_bytes - report.input_bytes + report.output_bytes
                                    );
                                }
                                let stats = report.stats;
                                assert_eq!(
                                    stats.records_read,
                                    stats.records_written
                                        + stats.versions_dropped
                                        + stats.copies_collapsed
                                        + stats.tombstones_dropped,
                                    "seed {seed}, background {background}"
                                );
                                if !background {
                                    compactions += 1;
                                    incomplete += usize::from(!report.complete);
                                    dropped_tombstones += stats.tombstones_dropped;
                                }
                                Stage::Idle
                            }
                        };
                    }
                }
                let expected: Vec<_> = model.clone().into_iter().collect();
                assert_eq!(
                    everything(&engine),
                    expected,
                    "seed {seed}, background {background}"
                );
                for key in &pool {
                    assert_eq!(
                        get(&engine, key),
                        model.get(key).cloned(),
                        "seed {seed}, background {background}"
                    );
                }
                if !held.is_empty() {
                    let (snapshot, then) = &held[rng.below(held.len())];
                    check_snapshot(snapshot, then, &pool);
                }
            }
            drop(stage);
            engine.flush().unwrap();
            if background {
                engine.wait_for_compaction().unwrap();
                assert!(engine.background_status().unwrap().compactions > 0);
            }
            compact_fully(&engine);
            // Fully compacted: nothing overlaps and no two tails are adjacent.
            let done = options(usize::MAX);
            assert!(engine.core.compact(done, &mut workspace).unwrap().is_none());
            let expected: Vec<_> = model.into_iter().collect();
            assert_eq!(
                everything(&engine),
                expected,
                "seed {seed}, background {background}"
            );
            // Everything a snapshot saw has since been flushed and compacted
            // away, and the snapshot still reads it.
            for (snapshot, then) in &held {
                check_snapshot(snapshot, then, &pool);
            }
            outlived += held.len();
            crate::test_support::wait_until(|| {
                !held.is_empty() || engine.engine_stats().retained_bytes == 0
            });
            let stats = engine.engine_stats();
            assert_eq!(stats.snapshots, held.len());
            assert!(held.is_empty() <= (stats.retained_bytes == 0));
            drop(held);
            crate::test_support::wait_until(|| engine.engine_stats().retained_bytes == 0);
            let stats = engine.engine_stats();
            assert_eq!((stats.snapshots, stats.retained_bytes), (0, 0));
        }
        assert!(snapshots > 300, "{snapshots}");
        assert!(outlived > 20, "{outlived}");
        assert!(compactions > 300, "{compactions}");
        assert!(incomplete > 50, "{incomplete}");
        assert!(dropped_tombstones > 50, "{dropped_tombstones}");
        assert!(contested > 50, "{contested}");
    }
}
