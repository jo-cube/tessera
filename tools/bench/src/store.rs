//! One interface over Tessera and the RocksDB adapter in native.cc.
use std::ffi::{CStr, CString, c_char, c_void};
use tessera::engine::{Engine, EngineOptions, EngineWorkspace, WriteBatch};

#[repr(C)]
struct Rec {
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    del: u8,
}
type Visit = unsafe extern "C" fn(*mut c_void, usize, *const u8, usize);
type ScanVisit = unsafe extern "C" fn(*mut c_void, *const u8, usize, *const u8, usize);
unsafe extern "C" {
    fn tb_version() -> *const c_char;
    fn tb_open(path: *const c_char, cache: u64) -> *mut c_void;
    fn tb_close(p: *mut c_void);
    fn tb_write(p: *mut c_void, r: *const Rec, n: usize);
    fn tb_ingest(p: *mut c_void, file: *const c_char, r: *const Rec, n: usize);
    fn tb_reader() -> *mut c_void;
    fn tb_reader_free(r: *mut c_void);
    fn tb_get(p: *mut c_void, r: *mut c_void, k: *const u8, kl: usize, v: *mut *const u8, vl: *mut usize) -> i32;
    fn tb_multi_get(p: *mut c_void, r: *mut c_void, keys: *const u8, kl: usize, n: usize, sorted: i32, ctx: *mut c_void, f: Visit);
    fn tb_scan(p: *mut c_void, pre: *const u8, n: usize, ctx: *mut c_void, f: ScanVisit);
    fn tb_drain(p: *mut c_void);
    fn tb_stats(p: *mut c_void, a: *mut u64);
    pub fn tb_rss() -> u64;
    pub fn tb_footprint() -> u64;
    pub fn tb_live_heap() -> u64;
}

pub fn rocksdb_version() -> String {
    unsafe { CStr::from_ptr(tb_version()) }.to_string_lossy().into_owned()
}

pub enum Store {
    T(Engine),
    R(*mut c_void),
}
// The RocksDB DB object is thread-safe; readers are per thread.
unsafe impl Send for Store {}
unsafe impl Sync for Store {}

impl Drop for Store {
    fn drop(&mut self) {
        if let Self::R(p) = self {
            unsafe { tb_close(*p) }
        }
    }
}

pub struct Reader {
    ws: EngineWorkspace,
    r: *mut c_void,
    keys: Vec<u8>,
}
impl Reader {
    pub fn new() -> Self {
        Self { ws: EngineWorkspace::new(), r: unsafe { tb_reader() }, keys: Vec::new() }
    }
    pub fn read_counters(&self) -> (u64, u64, u64) {
        (self.ws.decoded_bytes(), self.ws.blocks_loaded(), self.ws.filter_probes())
    }
}
impl Drop for Reader {
    fn drop(&mut self) {
        unsafe { tb_reader_free(self.r) }
    }
}

/// Reusable write buffers: a Tessera batch or RocksDB record pointers.
/// RocksDB records borrow the input; its keys and values must remain at stable
/// addresses until the synchronous write/ingest returns.
pub struct Batch {
    rocks: bool,
    t: WriteBatch,
    r: Vec<Rec>,
}
impl Batch {
    pub fn push(&mut self, key: &[u8], value: Option<&[u8]>) {
        if !self.rocks {
            match value {
                Some(v) => self.t.put(key, v),
                None => self.t.delete(key),
            }
            return;
        }
        let v = value.unwrap_or(&[]);
        self.r.push(Rec { key: key.as_ptr(), key_len: key.len(), value: v.as_ptr(), value_len: v.len(), del: value.is_none() as u8 });
    }
    fn clear(&mut self) {
        self.t.clear();
        self.r.clear();
    }
}

impl Store {
    pub fn tessera() -> Self {
        let mut options = EngineOptions::default();
        if let Ok(ratio) = std::env::var("TESSERA_BENCH_LEVEL_RATIO") {
            options.level_ratio = ratio.parse().expect("integer level ratio");
        }
        if let Ok(threads) = std::env::var("TESSERA_BENCH_BACKGROUND_THREADS") {
            options.background_threads = threads.parse().expect("integer builder count");
        }
        eprintln!("Tessera: level_ratio={}, background_threads={}", options.level_ratio, options.background_threads);
        Self::T(Engine::new(options).unwrap())
    }
    pub fn rocks(dir: &str, cache: u64) -> Self {
        let c = CString::new(dir).unwrap();
        Self::R(unsafe { tb_open(c.as_ptr(), cache) })
    }
    pub fn batch(&self) -> Batch {
        Batch { rocks: matches!(self, Self::R(_)), t: WriteBatch::new(), r: Vec::new() }
    }
    /// Writes and clears `b`.
    pub fn write(&self, b: &mut Batch) {
        match self {
            Self::T(e) => {
                e.write(&b.t).unwrap();
            }
            Self::R(p) => unsafe { tb_write(*p, b.r.as_ptr(), b.r.len()) },
        }
        b.clear();
    }
    /// Ingests sorted unique records, through `file` for RocksDB.
    pub fn ingest(&self, b: &mut Batch, file: &str) {
        match self {
            Self::T(e) => {
                e.ingest_sorted(&b.t).unwrap();
            }
            Self::R(p) => {
                let f = CString::new(file).unwrap();
                unsafe { tb_ingest(*p, f.as_ptr(), b.r.as_ptr(), b.r.len()) }
            }
        }
        b.clear();
    }
    pub fn get<R>(&self, k: &[u8], r: &mut Reader, f: impl FnOnce(Option<&[u8]>) -> R) -> R {
        match self {
            Self::T(e) => f(e.get(k, &mut r.ws).unwrap().as_ref().map(|v| v.value)),
            Self::R(p) => {
                let (mut v, mut n) = (std::ptr::null(), 0);
                let found = unsafe { tb_get(*p, r.r, k.as_ptr(), k.len(), &mut v, &mut n) };
                f((found == 1).then(|| unsafe { std::slice::from_raw_parts(v, n) }))
            }
        }
    }
    pub fn multi_get<F: FnMut(usize, Option<&[u8]>)>(&self, keys: &[[u8; 16]], sorted: bool, r: &mut Reader, mut f: F) {
        match self {
            Self::T(e) => e.multi_get(keys, &mut r.ws, f).unwrap(),
            Self::R(p) => {
                unsafe extern "C" fn cb<F: FnMut(usize, Option<&[u8]>)>(ctx: *mut c_void, i: usize, v: *const u8, n: usize) {
                    let f = unsafe { &mut *(ctx as *mut F) };
                    f(i, (!v.is_null()).then(|| unsafe { std::slice::from_raw_parts(v, n) }))
                }
                r.keys.clear();
                keys.iter().for_each(|k| r.keys.extend_from_slice(k));
                let ctx = &mut f as *mut F as *mut c_void;
                unsafe { tb_multi_get(*p, r.r, r.keys.as_ptr(), 16, keys.len(), sorted as i32, ctx, cb::<F>) }
            }
        }
    }
    pub fn scan<F: FnMut(&[u8], &[u8])>(&self, prefix: &[u8], r: &mut Reader, mut f: F) {
        match self {
            Self::T(e) => {
                let guard = e.read();
                let mut scan = guard.scan_prefix(prefix, &mut r.ws);
                while let Some(x) = scan.next_record().unwrap() {
                    f(x.key, x.value)
                }
            }
            Self::R(p) => {
                unsafe extern "C" fn cb<F: FnMut(&[u8], &[u8])>(ctx: *mut c_void, k: *const u8, kl: usize, v: *const u8, vl: usize) {
                    let f = unsafe { &mut *(ctx as *mut F) };
                    unsafe { f(std::slice::from_raw_parts(k, kl), std::slice::from_raw_parts(v, vl)) }
                }
                let ctx = &mut f as *mut F as *mut c_void;
                unsafe { tb_scan(*p, prefix.as_ptr(), prefix.len(), ctx, cb::<F>) }
            }
        }
    }
    /// Flushes and waits until compaction is idle.
    pub fn drain(&self) {
        match self {
            Self::T(e) => {
                e.flush().unwrap();
                e.wait_for_compaction().unwrap();
                let s = e.background_status().unwrap();
                assert!(s.failures == 0 && s.panic.is_none(), "compaction failed: {s:?}");
            }
            Self::R(p) => unsafe { tb_drain(*p) },
        }
    }
    pub fn emit_shape(&self, phase: &str, out: &super::Out) {
        if let Self::T(engine) = self {
            let shape = tessera::engine::diagnostics::streaming_shape(engine);
            out.emit(phase, "runs_per_level", shape.runs_per_level.iter().map(usize::to_string).collect::<Vec<_>>().join(";"));
            out.emit(phase, "max_overlap", shape.max_overlap);
            out.emit(phase, "level0_runs", shape.level0_runs);
            out.emit(phase, "bottom_runs", shape.bottom_runs);
            out.emit(phase, "bottom_mean_stored_bytes", shape.bottom_mean_stored_bytes);
            out.emit(phase, "bounded_views", shape.bounded_views);
            out.emit(phase, "stored_bytes", shape.stored_bytes);
            let m = tessera::engine::diagnostics::maintenance(engine);
            out.emit(phase, "writer_wait_total_s", format!("{:.3}", m.writer_wait_ns as f64 / 1e9));
            out.emit(phase, "level0_wait_total_s", format!("{:.3}", m.level0_wait_ns as f64 / 1e9));
            out.emit(phase, "level0_waits_total", m.level0_waits);
        }
    }
    /// Stored runs plus held filters/segment metadata, after a drain.
    pub fn accounted_run_bytes(&self) -> Option<u64> {
        match self {
            Self::T(e) => {
                let s = e.stats();
                Some((s.run_bytes + s.held_bytes) as u64)
            }
            Self::R(_) => None,
        }
    }
    pub fn pool_stats(&self) -> Option<(usize, usize, usize)> {
        if let Self::T(e) = self {
            let s = e.stats();
            Some((s.pool_reserve_bytes, s.pool_in_use_bytes, s.pool_reserve_limit))
        } else { None }
    }
    /// Flush bytes written, compaction bytes written, stored run/SST bytes.
    pub fn counters(&self) -> [u64; 3] {
        match self {
            Self::T(e) => {
                let s = e.stats();
                let b = e.background_status().unwrap();
                [s.flushed_bytes, b.output_bytes, s.run_bytes as u64]
            }
            Self::R(p) => {
                let mut a = [0; 5];
                unsafe { tb_stats(*p, a.as_mut_ptr()) };
                [a[0], a[1], a[2]]
            }
        }
    }
}
