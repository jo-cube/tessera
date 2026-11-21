//! Multi-thread reader scaling, writer scaling and Zipfian skewed read/update
//! workloads with oracle correctness gates.
use crate::data::*;
use crate::store::Store;
use crate::zipf::ScrambledZipfian;
use crate::{MISS, Out, Span, expect, hist, rec};
use hdrhistogram::Histogram;
use std::{
    fs,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

const SCALE_WINDOW: Duration = Duration::from_millis(1500);
const WARMUP: Duration = Duration::from_millis(150);
const BG_WRITER_RATE: f64 = 20_000.0;

fn live_indices(data: &Data) -> Vec<u32> {
    (0..data.recs.len() as u32)
        .filter(|&i| data.state[i as usize] != DELETED)
        .collect()
}

fn thread_counts() -> Vec<(String, usize)> {
    let avail = thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let mut counts: Vec<(String, usize)> = [1, 2, 4, 8]
        .into_iter()
        .map(|t| (format!("{t}t"), t))
        .collect();
    if avail > 8 {
        counts.push(("maxt".into(), avail));
    }
    counts
}

fn valid_value_len(data: &Data, idx: usize, got: Option<usize>) -> bool {
    let Some(len) = got else { return false };
    len == data.value(idx).len() || (data.recs[idx].has_update() && len == data.update(idx).len())
}

fn valid_value_bytes(data: &Data, idx: usize, got: Option<&[u8]>) -> bool {
    let Some(bytes) = got else { return false };
    bytes == data.value(idx) || (data.recs[idx].has_update() && bytes == data.update(idx))
}

/// Reader scaling across 1, 2, 4, 8 and (if larger) host core count:
/// latest-state Reader and Snapshot reader, uniform and scrambled Zipfian
/// (theta = 0.99), with and without one background writer at a fixed rate.
pub fn reader_scale(store: &Store, data: &Data, rng: &mut Rng, out: &Out) {
    let live = live_indices(data);
    let n_live = live.len();
    let sample_len = 200_000usize;
    let uniform_q: Vec<(KeyBuf, u32)> = (0..sample_len)
        .map(|_| {
            let idx = live[rng.n(n_live)];
            (data.recs[idx as usize].key_buf(), idx)
        })
        .collect();
    let zgen = ScrambledZipfian::new(n_live, 0.99, 0x7273_6361_6c65);
    let zipf_q: Vec<(KeyBuf, u32)> = (0..sample_len)
        .map(|_| {
            let idx = live[zgen.sample(rng.unit())];
            (data.recs[idx as usize].key_buf(), idx)
        })
        .collect();

    let counts = thread_counts();
    // (1) No background writer: latest x {uniform, zipf}, snapshot x {uniform, zipf}.
    for &(dist, queries) in &[("uniform", &uniform_q), ("zipf", &zipf_q)] {
        for (label, threads) in &counts {
            run_reader_point(
                store,
                data,
                queries,
                *threads,
                false,
                false,
                &format!("rscale_latest_{dist}_{label}"),
                out,
            );
            if store.snapshot().is_some() {
                run_reader_point(
                    store,
                    data,
                    queries,
                    *threads,
                    true,
                    false,
                    &format!("rscale_snap_{dist}_{label}"),
                    out,
                );
            }
        }
    }

    // (2) With one background writer at a fixed offered rate: latest vs snapshot on uniform.
    for (label, threads) in &counts {
        run_reader_point(
            store,
            data,
            &uniform_q,
            *threads,
            false,
            true,
            &format!("rscale_latest_uniform_w_{label}"),
            out,
        );
        if store.snapshot().is_some() {
            run_reader_point(
                store,
                data,
                &uniform_q,
                *threads,
                true,
                true,
                &format!("rscale_snap_uniform_w_{label}"),
                out,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_reader_point(
    store: &Store,
    data: &Data,
    queries: &[(KeyBuf, u32)],
    threads: usize,
    use_snapshot: bool,
    with_writer: bool,
    case: &str,
    out: &Out,
) {
    let snap = if use_snapshot { store.snapshot() } else { None };
    let stop = AtomicBool::new(false);
    let ready = AtomicUsize::new(0);
    let go = AtomicBool::new(false);
    let m0 = store.maintenance();
    let (span, results, written) = thread::scope(|s| {
        let mut handles = Vec::with_capacity(threads);
        for tid in 0..threads {
            let (stop, ready, go, snap) = (&stop, &ready, &go, snap.as_ref());
            handles.push(s.spawn(move || {
                let mut offset = (tid * 31_337) % queries.len();
                let mut next_q = || {
                    let item = queries[offset];
                    offset += 1;
                    if offset == queries.len() {
                        offset = 0;
                    }
                    item
                };
                if let Some(snap) = snap {
                    let mut r = snap.reader();
                    let w_end = Instant::now() + WARMUP;
                    while Instant::now() < w_end {
                        let (k, _) = next_q();
                        r.get(k.as_slice(), |v| v.map(<[u8]>::len));
                    }
                    let (dec0, blk0, _) = r.read_counters();
                    ready.fetch_add(1, Ordering::Release);
                    while !go.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                    let (mut h, mut ops, mut bad) = (hist(), 0u64, 0usize);
                    while !stop.load(Ordering::Relaxed) {
                        let (k, idx) = next_q();
                        let t = Instant::now();
                        let got = r.get(k.as_slice(), |v| v.map(<[u8]>::len));
                        rec(&mut h, t);
                        let ok = if with_writer {
                            valid_value_len(data, idx as usize, got)
                        } else {
                            got == expect(data, idx)
                        };
                        bad += (!ok) as usize;
                        ops += 1;
                    }
                    let (dec1, blk1, _) = r.read_counters();
                    (h, ops, bad, dec1 - dec0, blk1 - blk0)
                } else {
                    let mut r = store.reader();
                    let w_end = Instant::now() + WARMUP;
                    while Instant::now() < w_end {
                        let (k, _) = next_q();
                        store.get(k.as_slice(), &mut r, |v| v.map(<[u8]>::len));
                    }
                    let (dec0, blk0, _) = r.read_counters();
                    ready.fetch_add(1, Ordering::Release);
                    while !go.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                    let (mut h, mut ops, mut bad) = (hist(), 0u64, 0usize);
                    while !stop.load(Ordering::Relaxed) {
                        let (k, idx) = next_q();
                        let t = Instant::now();
                        let got = store.get(k.as_slice(), &mut r, |v| v.map(<[u8]>::len));
                        rec(&mut h, t);
                        let ok = if with_writer {
                            valid_value_len(data, idx as usize, got)
                        } else {
                            got == expect(data, idx)
                        };
                        bad += (!ok) as usize;
                        ops += 1;
                    }
                    let (dec1, blk1, _) = r.read_counters();
                    (h, ops, bad, dec1 - dec0, blk1 - blk0)
                }
            }));
        }
        let writer_handle = with_writer.then(|| {
            let (stop, go) = (&stop, &go);
            s.spawn(move || {
                while !go.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                let mut b = store.batch();
                let interval = Duration::from_secs_f64(256.0 / BG_WRITER_RATE);
                let mut due = Instant::now();
                let mut next = 0usize;
                let mut written = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let now = Instant::now();
                    if due > now {
                        thread::sleep((due - now).min(Duration::from_millis(5)));
                        continue;
                    }
                    for _ in 0..256 {
                        let i = data.updates[next % data.updates.len()] as usize;
                        b.push(data.recs[i].key(), Some(data.update(i)));
                        next += 1;
                    }
                    store.write(&mut b);
                    written += 256;
                    due += interval;
                }
                written
            })
        });

        while ready.load(Ordering::Acquire) < threads {
            thread::sleep(Duration::from_millis(1));
        }
        let span = Span::new();
        go.store(true, Ordering::Release);
        thread::sleep(SCALE_WINDOW);
        stop.store(true, Ordering::Relaxed);
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let written = writer_handle.map_or(0, |h| h.join().unwrap());
        (span, results, written)
    });
    let total_ops: u64 = results.iter().map(|r| r.1).sum();
    let total_bad: usize = results.iter().map(|r| r.2).sum();
    let total_dec: u64 = results.iter().map(|r| r.3).sum();
    let total_blk: u64 = results.iter().map(|r| r.4).sum();
    let mut h = hist();
    results.iter().for_each(|r| h.add(&r.0).unwrap());

    out.emit(case, "threads", threads);
    span.finish(out, case, total_ops as usize, 0);
    out.lat(case, &h);
    out.emit(case, "errors", total_bad);
    if with_writer {
        out.emit(case, "offered_rec_s", format!("{BG_WRITER_RATE:.0}"));
        out.emit(case, "written_records", written);
    }
    if let (Some(b), Some(a)) = (m0, store.maintenance()) {
        out.emit(
            case,
            "decoded_bytes_per_get",
            format!("{:.1}", total_dec as f64 / total_ops.max(1) as f64),
        );
        out.emit(
            case,
            "blocks_per_get",
            format!("{:.4}", total_blk as f64 / total_ops.max(1) as f64),
        );
        out.emit(case, "gate_waits", a.gate_waits - b.gate_waits);
        out.emit(case, "write_locks", a.write_locks - b.write_locks);
    }
    assert_eq!(total_bad, 0, "reader_scale oracle mismatch in {case}");
}

/// Writer scaling across 1, 2, 4, 8 writer threads, batch sizes {1, 16, 256},
/// and disjoint vs overlapping key sets, each on a fresh store with oracle gate.
pub fn writer_scale(engine: &str, data: &Data, dir: &str, out: &Out) {
    let n = data.recs.len();
    let pool = 4_096usize.min(n);
    for key_mode in ["disjoint", "overlap"] {
        for batch_size in [1usize, 16, 256] {
            for threads in [1usize, 2, 4, 8] {
                let case = format!("wscale_{key_mode}_b{batch_size}_{threads}t");
                let sub = format!("{dir}/{case}");
                fs::create_dir_all(&sub).unwrap();
                let store = match engine {
                    "tessera" => Store::tessera(),
                    "r-small" => Store::rocks(&sub, 128 << 20),
                    _ => Store::rocks(&sub, data.raw as u64 * 5 / 4),
                };
                let stop = AtomicBool::new(false);
                let go = AtomicBool::new(false);
                let total_cap = n;
                let global_written = AtomicUsize::new(0);
                let span = Span::new();
                let (write_wall, per_thread) = thread::scope(|s| {
                    let mut handles = Vec::with_capacity(threads);
                    for tid in 0..threads {
                        let (stop, go, global_written, store) =
                            (&stop, &go, &global_written, &store);
                        handles.push(s.spawn(move || {
                            let mut b = store.batch();
                            let mut h = hist();
                            let mut rng = Rng(0x7773_6361_6c65
                                ^ ((tid as u64 + 1) << 32)
                                ^ (batch_size as u64));
                            let mut written = 0usize;
                            let mut step = 0usize;
                            while !go.load(Ordering::Acquire) {
                                thread::yield_now();
                            }
                            while !stop.load(Ordering::Relaxed) {
                                let prev = global_written.fetch_add(batch_size, Ordering::Relaxed);
                                if prev >= total_cap {
                                    break;
                                }
                                for _ in 0..batch_size {
                                    if key_mode == "disjoint" {
                                        let idx = (tid + step * threads) % n;
                                        b.push(data.recs[idx].key(), Some(data.value(idx)));
                                    } else {
                                        let idx = rng.n(pool);
                                        let val = if tid % 2 == 1 && data.recs[idx].has_update() {
                                            data.update(idx)
                                        } else {
                                            data.value(idx)
                                        };
                                        b.push(data.recs[idx].key(), Some(val));
                                    }
                                    step += 1;
                                }
                                let t = Instant::now();
                                store.write(&mut b);
                                rec(&mut h, t);
                                written += batch_size;
                            }
                            (h, written, step)
                        }));
                    }
                    let t0 = Instant::now();
                    go.store(true, Ordering::Release);
                    let end = t0 + SCALE_WINDOW;
                    while Instant::now() < end && global_written.load(Ordering::Relaxed) < total_cap
                    {
                        thread::sleep(Duration::from_millis(5));
                    }
                    stop.store(true, Ordering::Relaxed);
                    let res: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
                    (t0.elapsed().as_secs_f64(), res)
                });
                let drain_start = Instant::now();
                store.drain();
                let drain_s = drain_start.elapsed().as_secs_f64();

                let total_records: usize = per_thread.iter().map(|r| r.1).sum();
                let mut h = hist();
                per_thread.iter().for_each(|r| h.add(&r.0).unwrap());

                out.emit(&case, "threads", threads);
                out.emit(&case, "batch", batch_size);
                out.emit(&case, "written_records", total_records);
                out.emit(&case, "write_wall_s", format!("{write_wall:.3}"));
                out.emit(
                    &case,
                    "write_ops_s",
                    format!("{:.0}", total_records as f64 / write_wall),
                );
                out.emit(&case, "drain_s", format!("{drain_s:.3}"));
                span.finish(out, &case, total_records, 0);
                out.lat(&case, &h);
                if let Some(m) = store.maintenance() {
                    out.emit(&case, "write_locks", m.write_locks);
                    out.emit(
                        &case,
                        "level0_wait_s",
                        format!("{:.3}", m.level0_wait_ns as f64 / 1e9),
                    );
                    out.emit(
                        &case,
                        "flush_wait_s",
                        format!("{:.3}", m.flush_wait_ns as f64 / 1e9),
                    );
                    out.emit(
                        &case,
                        "writer_wait_s",
                        format!("{:.3}", m.writer_wait_ns as f64 / 1e9),
                    );
                }

                // Oracle correctness gate.
                let mut r = store.reader();
                let mut bad = 0usize;
                if key_mode == "disjoint" {
                    for (tid, &(_, _, steps)) in per_thread.iter().enumerate() {
                        let checked = steps.min(n / threads).min(500);
                        let stride = (steps.min(n / threads) / checked.max(1)).max(1);
                        for j in 0..checked {
                            let idx = (tid + (j * stride) * threads) % n;
                            bad += store
                                .get(data.recs[idx].key(), &mut r, |v| v != Some(data.value(idx)))
                                as usize;
                        }
                    }
                } else {
                    let check_n = pool.min(2_000);
                    for idx in 0..check_n {
                        bad += store.get(data.recs[idx].key(), &mut r, |v| {
                            v.is_some() && !valid_value_bytes(data, idx, v)
                        }) as usize;
                    }
                }
                out.emit(
                    &case,
                    "gate",
                    if bad == 0 {
                        "pass".to_string()
                    } else {
                        format!("FAIL({bad})")
                    },
                );
                assert_eq!(bad, 0, "writer_scale gate failed in {case}");
                drop(r);
                drop(store);
                let _ = fs::remove_dir_all(&sub);
            }
        }
    }
}

/// Zipfian point reads (1t and 4t, theta = 0.99 scrambled) and Zipfian
/// read+update mixes (95/5 and 50/50, 4 threads, 10s each).
pub fn zipf_workloads(store: &Store, data: &Data, rng: &mut Rng, out: &Out) {
    let live = live_indices(data);
    let n_live = live.len();
    let zgen = ScrambledZipfian::new(n_live, 0.99, 0x7a69_7066_1111);

    // (a) 200k Zipfian hits + 20k misses, 1t and 4t.
    let mut pairs = Vec::with_capacity(220_000);
    for _ in 0..200_000 {
        let idx = live[zgen.sample(rng.unit())];
        pairs.push((data.recs[idx as usize].key_buf(), idx));
    }
    for _ in 0..20_000 {
        pairs.push((data.miss(rng.n(data.recs.len())), MISS));
    }
    rng.shuffle(&mut pairs);
    let (keys, want): (Vec<KeyBuf>, Vec<u32>) = pairs.into_iter().unzip();

    for threads in [1usize, 4] {
        let case = format!("zipf_get_{threads}t");
        let span = Span::new();
        let parts: Vec<(Histogram<u64>, usize, u64, u64)> = thread::scope(|s| {
            let q = keys.len().div_ceil(threads);
            let handles: Vec<_> = keys
                .chunks(q)
                .zip(want.chunks(q))
                .map(|(ks, ws)| {
                    s.spawn(move || {
                        let mut r = store.reader();
                        let (mut h, mut bad) = (hist(), 0);
                        for (k, &i) in ks.iter().zip(ws) {
                            let t = Instant::now();
                            let got = store.get(k.as_slice(), &mut r, |v| v.map(<[u8]>::len));
                            rec(&mut h, t);
                            bad += (got != expect(data, i)) as usize;
                        }
                        let (dec, blk, _) = r.read_counters();
                        (h, bad, dec, blk)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        span.finish(out, &case, keys.len(), 0);
        let mut h = hist();
        parts.iter().for_each(|p| h.add(&p.0).unwrap());
        out.lat(&case, &h);
        let bad: usize = parts.iter().map(|p| p.1).sum();
        out.emit(&case, "errors", bad);
        if matches!(store, Store::T(..)) {
            let dec: u64 = parts.iter().map(|p| p.2).sum();
            let blk: u64 = parts.iter().map(|p| p.3).sum();
            out.emit(
                &case,
                "decoded_bytes_per_get",
                format!("{:.1}", dec as f64 / keys.len() as f64),
            );
            out.emit(
                &case,
                "blocks_per_get",
                format!("{:.4}", blk as f64 / keys.len() as f64),
            );
        }
        assert_eq!(bad, 0, "zipf get mismatch in {case}");
    }

    // (b) Zipfian read+update mixes: 95% read / 5% update and 50% read / 50% update.
    struct MixPart {
        rh: Histogram<u64>,
        wh: Histogram<u64>,
        reads: u64,
        writes: u64,
        bad: usize,
        dec: u64,
    }
    for &(case, write_pct) in &[("zipf_mix_95_5", 5usize), ("zipf_mix_50_50", 50)] {
        let stop = AtomicBool::new(false);
        let m0 = store.maintenance();
        let span = Span::new();
        let parts: Vec<MixPart> = thread::scope(|s| {
            let handles: Vec<_> = (0..4u64)
                .map(|tid| {
                    let (live, zgen, stop) = (&live, &zgen, &stop);
                    s.spawn(move || {
                        let mut r = store.reader();
                        let mut b = store.batch();
                        let mut rng = Rng(0x7a6d_6978_0000 ^ (tid << 16) ^ (write_pct as u64));
                        let (mut rh, mut wh) = (hist(), hist());
                        let (mut reads, mut writes, mut bad) = (0u64, 0u64, 0usize);
                        while !stop.load(Ordering::Relaxed) {
                            let idx = live[zgen.sample(rng.unit())] as usize;
                            if rng.n(100) < write_pct {
                                let val = if data.recs[idx].has_update() {
                                    data.update(idx)
                                } else {
                                    data.value(idx)
                                };
                                b.push(data.recs[idx].key(), Some(val));
                                let t = Instant::now();
                                store.write(&mut b);
                                rec(&mut wh, t);
                                writes += 1;
                            } else {
                                let t = Instant::now();
                                let got =
                                    store.get(data.recs[idx].key(), &mut r, |v| v.map(<[u8]>::len));
                                rec(&mut rh, t);
                                bad += (!valid_value_len(data, idx, got)) as usize;
                                reads += 1;
                            }
                        }
                        let (dec, _, _) = r.read_counters();
                        MixPart {
                            rh,
                            wh,
                            reads,
                            writes,
                            bad,
                            dec,
                        }
                    })
                })
                .collect();
            thread::sleep(Duration::from_secs(10));
            stop.store(true, Ordering::Relaxed);
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let total_reads: u64 = parts.iter().map(|p| p.reads).sum();
        let total_writes: u64 = parts.iter().map(|p| p.writes).sum();
        let total_bad: usize = parts.iter().map(|p| p.bad).sum();
        let total_dec: u64 = parts.iter().map(|p| p.dec).sum();
        let wall = span.t.elapsed().as_secs_f64();
        span.finish(out, case, (total_reads + total_writes) as usize, 0);
        out.emit(
            case,
            "read_ops_s",
            format!("{:.0}", total_reads as f64 / wall),
        );
        out.emit(
            case,
            "write_ops_s",
            format!("{:.0}", total_writes as f64 / wall),
        );
        out.emit(case, "errors", total_bad);
        let (mut rh, mut wh) = (hist(), hist());
        for p in &parts {
            rh.add(&p.rh).unwrap();
            wh.add(&p.wh).unwrap();
        }
        for (m, q) in [
            ("read_p50_us", 0.5),
            ("read_p99_us", 0.99),
            ("read_p999_us", 0.999),
        ] {
            out.emit(
                case,
                m,
                format!("{:.2}", rh.value_at_quantile(q) as f64 / 1e3),
            );
        }
        for (m, q) in [
            ("write_p50_us", 0.5),
            ("write_p99_us", 0.99),
            ("write_p999_us", 0.999),
        ] {
            out.emit(
                case,
                m,
                format!("{:.2}", wh.value_at_quantile(q) as f64 / 1e3),
            );
        }
        if let (Some(b), Some(a)) = (m0, store.maintenance()) {
            out.emit(
                case,
                "decoded_bytes_per_get",
                format!("{:.1}", total_dec as f64 / total_reads.max(1) as f64),
            );
            out.emit(case, "gate_waits", a.gate_waits - b.gate_waits);
            out.emit(case, "write_locks", a.write_locks - b.write_locks);
        }
        store.drain();
        let mut r = store.reader();
        let mut gate_bad = 0usize;
        for _ in 0..10_000 {
            let idx = live[zgen.sample(rng.unit())] as usize;
            gate_bad += store.get(data.recs[idx].key(), &mut r, |v| {
                !valid_value_bytes(data, idx, v)
            }) as usize;
        }
        out.emit(
            case,
            "gate",
            if gate_bad == 0 {
                "pass".to_string()
            } else {
                format!("FAIL({gate_bad})")
            },
        );
        assert_eq!(total_bad + gate_bad, 0, "zipf mix gate failed in {case}");
    }
}
