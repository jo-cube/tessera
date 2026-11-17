//! Linux counters-example instrumentation; never linked into the library.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tessera::engine::{Engine, EngineStats};

struct Counting;
static BACKEND: System = System;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static PHASE: AtomicU8 = AtomicU8::new(0);
static META: OnceLock<(String, String, Instant)> = OnceLock::new();
static OUTPUT: Mutex<()> = Mutex::new(());

fn add(bytes: usize) {
    let now = LIVE.fetch_add(bytes, Relaxed) + bytes;
    PEAK.fetch_max(now, Relaxed);
}

// SAFETY: Every operation forwards the original pointer/layout to System.
// Counters never allocate. A failed allocation/reallocation changes no count.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { BACKEND.alloc(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { BACKEND.alloc_zeroed(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Relaxed);
        unsafe { BACKEND.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new = unsafe { BACKEND.realloc(ptr, layout, size) };
        if !new.is_null() {
            if size >= layout.size() {
                add(size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - size, Relaxed);
            }
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

pub fn sample_stats(reason: &str, s: EngineStats) {
    let _lock = OUTPUT.lock().unwrap();
    let Some((name, variant, started)) = META.get() else {
        return;
    };
    // Requested Rust allocation sizes; native Zstd mallocs and allocator
    // bookkeeping are outside this counter. Read before allocating /proc text.
    let live = LIVE.load(Relaxed);
    let peak = PEAK.load(Relaxed);
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let proc_bytes = |field: &str| -> usize {
        status
            .lines()
            .find_map(|line| {
                line.strip_prefix(field)?
                    .split_whitespace()
                    .next()?
                    .parse::<usize>()
                    .ok()
            })
            .unwrap()
            * 1024
    };
    let phase = ["load", "update", "drain", "reads"][usize::from(PHASE.load(Relaxed))];
    println!(
        "memory,{name},{variant},{phase},{reason},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        started.elapsed().as_secs_f64(),
        proc_bytes("VmRSS:"),
        proc_bytes("VmHWM:"),
        proc_bytes("VmSwap:"),
        live,
        peak,
        s.used,
        s.memtable_bytes,
        s.build_output_bytes,
        s.build_workspace_bytes,
        s.pool_reserve_bytes,
        s.pool_in_use_bytes,
        s.pool_unaccounted_bytes,
        s.run_bytes,
        s.held_bytes,
        s.snapshot_bytes,
        s.active.arena_bytes,
        s.frozen_tables,
        s.retained_bytes
    );
}

pub fn phase_end(engine: &Engine, reason: &str, next: u8) {
    sample_stats(reason, engine.stats());
    PHASE.store(next, Relaxed);
}

pub struct Sampler {
    stop: mpsc::Sender<()>,
    handle: JoinHandle<()>,
}
impl Sampler {
    pub fn start(engine: Arc<Engine>, name: &str, variant: &str) -> Self {
        assert!(
            META.set((name.into(), variant.into(), Instant::now()))
                .is_ok(),
            "memory instrumentation requires one scenario per process"
        );
        println!(
            "memory,scenario,variant,phase,event,seconds,rss,hwm,swap,live,peak_live,used,memtables,build_output,build_workspace,pool_reserve,pool_in_use,pool_unaccounted,run_bytes,held_bytes,snapshots,active_arena,frozen_tables,retained"
        );
        sample_stats("start", engine.stats());
        let (stop, receiver) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) =
                receiver.recv_timeout(Duration::from_secs(1))
            {
                sample_stats("tick", engine.stats());
            }
        });
        Self { stop, handle }
    }
    pub fn stop(self) {
        self.stop.send(()).unwrap();
        self.handle.join().unwrap();
    }
}
