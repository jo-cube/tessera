//! Investigation only: where memtable and engine write time goes, by
//! measuring each component on its own. No profiler is available inside the
//! bounded container (no perf, no hardware counters), so costs are attributed
//! by difference between controlled variants.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use tessera::engine::{Engine, EngineOptions, Snapshot, UpdateError};
use tessera::memtable::{FrozenMemtable, Memtable, TARGET_RUN_LEN};

use super::{
    MEMTABLE_BUDGET, Owned, allocations_during, insert_all, shuffled, snapshot_base,
    snapshot_options, task_cpu_ns, tree_json, tree_small, worker_task,
};

const ROUNDS: usize = 3;

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

/// Median ns per operation of `run` over fresh state from `setup`, and
/// allocations per operation from one more pass.
fn per_op<T>(ops: usize, mut setup: impl FnMut() -> T, mut run: impl FnMut(&mut T)) -> (f64, f64) {
    let ns = median(
        (0..ROUNDS)
            .map(|_| {
                let mut state = setup();
                let start = Instant::now();
                run(&mut state);
                let ns = start.elapsed().as_nanos() as f64 / ops as f64;
                black_box(&state);
                ns
            })
            .collect(),
    );
    let mut state = setup();
    let allocs = allocations_during(|| run(&mut state)) as f64 / ops as f64;
    (ns, allocs)
}

fn filled(records: &[Owned], order: &[usize]) -> (Memtable, u64) {
    let mut table = Memtable::new(MEMTABLE_BUDGET, None);
    let mut seq = 0;
    insert_all(records, order, &mut table, &mut seq);
    (table, seq)
}

/// Overwrites `keys`, taking a snapshot every `every` writes (never if 0)
/// that is kept until the next, or dropped at once if `drop_at_once`.
fn overwrite(
    records: &[Owned],
    keys: &[usize],
    state: &mut (Memtable, u64, Vec<FrozenMemtable>),
    every: usize,
    drop_at_once: bool,
) {
    let (table, seq, held) = state;
    for (done, &index) in keys.iter().enumerate() {
        let record = &records[index];
        *seq += 1;
        table
            .put(&record.key, *seq, record.value.as_deref().unwrap())
            .unwrap();
        if every != 0 && done % every == every - 1 {
            held.clear();
            let snapshot = table.snapshot();
            if !drop_at_once {
                held.push(snapshot);
            }
        }
    }
}

fn memtable_rows(name: &str, records: &[Owned]) {
    let count = records.len();
    let random = shuffled(count, 91);
    let second = shuffled(count, 92);
    let mut sorted_probe: Vec<usize> = second[..count / 2].to_vec();
    sorted_probe.sort_unstable();
    let random_probe = &second[..count / 2];
    let (table, _) = filled(records, &random);
    let row = |what: &str, (ns, allocs): (f64, f64)| {
        println!("| {name} | {what} | {ns:.1} | {allocs:.2} |");
    };

    // Traversal and comparisons: the same lookups in random and ascending
    // order, and on a table small enough to stay in cache.
    fn gets(table: &Memtable, records: &[Owned], probe: &[usize]) {
        for &i in probe {
            black_box(table.get(&records[i].key));
        }
    }
    row(
        "get, random order",
        per_op(
            random_probe.len(),
            || (),
            |_| gets(&table, records, random_probe),
        ),
    );
    row(
        "get, ascending order",
        per_op(
            sorted_probe.len(),
            || (),
            |_| gets(&table, records, &sorted_probe),
        ),
    );
    let small: Vec<usize> = random[..1_000].to_vec();
    let (small_table, _) = filled(records, &small);
    let small_probe: Vec<usize> = (0..200).flat_map(|_| small.iter().copied()).collect();
    row(
        "get, 1,000-key table (in cache)",
        per_op(
            small_probe.len(),
            || (),
            |_| {
                for &i in &small_probe {
                    black_box(small_table.get(&records[i].key));
                }
            },
        ),
    );

    // Entry allocation: building and freeing one key-and-value allocation,
    // as an overwrite does, without the tree.
    row(
        "entry allocation and free alone",
        per_op(
            count,
            || (),
            |_| {
                for &i in &second {
                    let record = &records[i];
                    let value = record.value.as_deref().unwrap();
                    let mut data: Arc<[u8]> =
                        std::iter::repeat_n(0, record.key.len() + value.len()).collect();
                    let buffer = Arc::get_mut(&mut data).unwrap();
                    buffer[..record.key.len()].copy_from_slice(&record.key);
                    buffer[record.key.len()..].copy_from_slice(value);
                    black_box(data);
                }
            },
        ),
    );

    // Writes: no snapshot, a snapshot every 1,000 writes dropped at once,
    // and one kept until the next (which makes each write copy its path).
    let setup = || {
        let (table, seq) = filled(records, &random);
        (table, seq, Vec::new())
    };
    row(
        "overwrite, no snapshot",
        per_op(count, setup, |state| {
            overwrite(records, &second, state, 0, false)
        }),
    );
    row(
        "overwrite, snapshot every 1,000 dropped at once",
        per_op(count, setup, |state| {
            overwrite(records, &second, state, 1_000, true)
        }),
    );
    row(
        "overwrite, snapshot every 1,000 kept until the next",
        per_op(count, setup, |state| {
            overwrite(records, &second, state, 1_000, false)
        }),
    );
    row(
        "overwrite, snapshot every 100 kept until the next",
        per_op(count, setup, |state| {
            overwrite(records, &second, state, 100, false)
        }),
    );
    row(
        "overwrite, snapshot every 10,000 kept until the next",
        per_op(count, setup, |state| {
            overwrite(records, &second, state, 10_000, false)
        }),
    );
    // Dropping the history a kept snapshot pins: one snapshot over the whole
    // table, then a full overwrite pass, then the snapshot alone is dropped.
    let mut frees = Vec::new();
    for _ in 0..ROUNDS {
        let (mut copy, mut seq) = filled(records, &random);
        let snapshot = copy.snapshot();
        insert_all(records, &second, &mut copy, &mut seq);
        let start = Instant::now();
        drop(snapshot);
        frees.push(start.elapsed().as_nanos() as f64 / count as f64);
        black_box(&copy);
    }
    row("free a snapshot's history, per entry", (median(frees), 0.0));
    let mut ordered = Vec::new();
    for _ in 0..ROUNDS {
        let start = Instant::now();
        black_box(table.iter().count());
        ordered.push(start.elapsed().as_nanos() as f64 / count as f64);
    }
    row("ordered iteration, per record", (median(ordered), 0.0));
    let frozen = table.freeze();
    let start = Instant::now();
    black_box(frozen.flush(TARGET_RUN_LEN).count());
    row(
        "flush (encode and compress), per record",
        (start.elapsed().as_nanos() as f64 / count as f64, 0.0),
    );
    let start = Instant::now();
    drop(frozen);
    row(
        "free a whole table, per entry",
        (start.elapsed().as_nanos() as f64 / count as f64, 0.0),
    );
}

/// Synchronization: the same overwrites through the engine, which takes the
/// gate and the write lock and assigns the sequence number, with a budget
/// that never flushes.
fn engine_overhead(name: &str, records: &[Owned]) {
    let count = records.len();
    let random = shuffled(count, 91);
    let second = shuffled(count, 92);
    let options = EngineOptions {
        memtable_budget: MEMTABLE_BUDGET,
        ..EngineOptions::default()
    };
    let (ns, allocs) = per_op(
        count,
        || {
            let engine = Engine::new(options);
            for &i in &random {
                engine
                    .put(&records[i].key, records[i].value.as_deref().unwrap())
                    .unwrap();
            }
            engine
        },
        |engine| {
            for &i in &second {
                engine
                    .put(&records[i].key, records[i].value.as_deref().unwrap())
                    .unwrap();
            }
        },
    );
    println!("| {name} | engine.put overwrite, never flushing | {ns:.1} | {allocs:.2} |");
}

/// One sustained pass, split into the writer's time in puts, flush builds,
/// flush publication (which frees the flushed table), snapshots, and the
/// worker's CPU time compacting.
fn sustained_split(name: &str, records: &[Owned], held: usize) {
    const PUTS: usize = 300_000;
    let count = records.len();
    let order = shuffled(count, 900);
    let puts: Vec<usize> = {
        let mut rng = super::Rng(901);
        (0..PUTS)
            .map(|_| (rng.next() % count as u64) as usize)
            .collect()
    };
    let engine = snapshot_base(records, &order, snapshot_options(4 << 20, true));
    let worker = worker_task().expect("the worker thread is named");
    let worker_before = task_cpu_ns(&worker);
    let (mut put_ns, mut build_ns, mut publish_ns, mut snap_ns) = (0u128, 0u128, 0u128, 0u128);
    let (mut flushes, mut stalls) = (0, 0);
    let mut kept: Vec<Snapshot> = Vec::new();
    let mut rng = super::Rng(5);
    let start = Instant::now();
    let writer_before = task_cpu_ns("thread-self");
    for (at, &i) in puts.iter().enumerate() {
        let record = &records[i];
        loop {
            let begun = Instant::now();
            let result = engine.put(&record.key, record.value.as_deref().unwrap());
            put_ns += begun.elapsed().as_nanos();
            match result {
                Ok(_) => break,
                Err(UpdateError::FlushRefused(_)) => loop {
                    let Some(job) = engine.begin_flush() else {
                        break;
                    };
                    let begun = Instant::now();
                    let built = job.build();
                    build_ns += begun.elapsed().as_nanos();
                    let begun = Instant::now();
                    let published = built.publish();
                    publish_ns += begun.elapsed().as_nanos();
                    match published {
                        Ok(_) => flushes += 1,
                        Err(_) => {
                            stalls += 1;
                            engine.wait_for_compaction().unwrap();
                        }
                    }
                },
                Err(error) => panic!("{error}"),
            }
        }
        if held > 0 && at % 1_000 == 999 {
            let begun = Instant::now();
            kept.push(engine.snapshot().unwrap());
            if kept.len() > held {
                let at = (rng.next() % kept.len() as u64) as usize;
                drop(kept.swap_remove(at));
            }
            snap_ns += begun.elapsed().as_nanos();
        }
    }
    let wall = start.elapsed().as_secs_f64();
    let writer_cpu = (task_cpu_ns("thread-self") - writer_before) as f64 / 1e9;
    let worker_cpu = (task_cpu_ns(&worker) - worker_before) as f64 / 1e9;
    let s = |ns: u128| ns as f64 / 1e9;
    println!(
        "| {name} | {held} | {wall:.2} | {writer_cpu:.2} | {:.2} | {:.0} | {:.2} | {:.2} | {:.3} | {flushes} | {stalls} | {worker_cpu:.2} |",
        s(put_ns),
        put_ns as f64 / PUTS as f64,
        s(build_ns),
        s(publish_ns),
        s(snap_ns),
    );
    drop(kept);
}

pub(super) fn profile_bench() {
    let json = tree_json(16 << 20);
    let tiny = tree_small(8 << 20);
    println!("isolated memtable: median of {ROUNDS} rounds, allocations from one more");
    println!("| dataset | component | ns/op | allocs/op |");
    println!("|---|---|---|---|");
    memtable_rows("json", &json);
    engine_overhead("json", &json);
    memtable_rows("8-byte", &tiny);
    engine_overhead("8-byte", &tiny);
    println!();
    println!(
        "sustained: 300,000 random puts, 4 MiB memtables, background worker; snapshots every 1,000 puts with up to `held` kept, a random one dropped beyond that"
    );
    println!(
        "| dataset | held | wall s | writer CPU s | in put s | ns per put | flush build s | flush publish s | snapshots s | flushes | stalls | worker CPU s |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    for held in [0, 1, 8] {
        sustained_split("json", &json, held);
    }
    for held in [0, 1, 8] {
        sustained_split("8-byte", &tiny, held);
    }
}
