//! One interface over Tessera and the RocksDB adapter in native.cc.
use std::ffi::{CStr, CString, c_char, c_void};
use tessera::diagnostics::{EngineDiagnostics, ReaderDiagnostics};
use tessera::{Engine, Keyspace, Options, WriteBatch};

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
    fn tb_get(
        p: *mut c_void,
        r: *mut c_void,
        k: *const u8,
        kl: usize,
        v: *mut *const u8,
        vl: *mut usize,
    ) -> i32;
    fn tb_multi_get(
        p: *mut c_void,
        r: *mut c_void,
        keys: *const u8,
        kl: usize,
        n: usize,
        sorted: i32,
        ctx: *mut c_void,
        f: Visit,
    );
    fn tb_scan(p: *mut c_void, pre: *const u8, n: usize, ctx: *mut c_void, f: ScanVisit);
    fn tb_drain(p: *mut c_void);
    fn tb_stats(p: *mut c_void, a: *mut u64);
    pub fn tb_rss() -> u64;
    pub fn tb_footprint() -> u64;
    pub fn tb_live_heap() -> u64;
}

pub fn rocksdb_version() -> String {
    unsafe { CStr::from_ptr(tb_version()) }
        .to_string_lossy()
        .into_owned()
}

pub enum Store {
    T(Engine, Keyspace),
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

/// A Tessera snapshot handle for lock-free reader scaling benchmarks.
pub struct Snapshot {
    snap: tessera::Snapshot,
    ks: Keyspace,
}

impl Snapshot {
    pub fn reader(&self) -> SnapReader<'_> {
        SnapReader {
            r: self.snap.reader(),
            ks: &self.ks,
        }
    }
}

pub struct SnapReader<'s> {
    r: tessera::Reader<'s>,
    ks: &'s Keyspace,
}

impl SnapReader<'_> {
    pub fn get<R>(&mut self, k: &[u8], f: impl FnOnce(Option<&[u8]>) -> R) -> R {
        f(self.r.get(self.ks, k).unwrap())
    }
    pub fn read_counters(&self) -> (u64, u64, u64) {
        let c = self.r.counters();
        (c.decoded_bytes, c.blocks_loaded, c.filter_probes)
    }
}

/// A per-thread reader: Tessera's pooled reader of the latest state, or a
/// RocksDB read context.
pub struct Reader<'s> {
    t: Option<tessera::Reader<'s>>,
    r: *mut c_void,
    keys: Vec<u8>,
}
impl Reader<'_> {
    pub fn read_counters(&self) -> (u64, u64, u64) {
        let c = self
            .t
            .as_ref()
            .map(ReaderDiagnostics::counters)
            .unwrap_or_default();
        (c.decoded_bytes, c.blocks_loaded, c.filter_probes)
    }
}
impl Drop for Reader<'_> {
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
    ks: Option<Keyspace>,
    r: Vec<Rec>,
}
impl Batch {
    pub fn push(&mut self, key: &[u8], value: Option<&[u8]>) {
        if !self.rocks {
            let ks = self.ks.as_ref().unwrap();
            match value {
                Some(v) => self.t.put(ks, key, v),
                None => self.t.delete(ks, key),
            }
            return;
        }
        let v = value.unwrap_or(&[]);
        self.r.push(Rec {
            key: key.as_ptr(),
            key_len: key.len(),
            value: v.as_ptr(),
            value_len: v.len(),
            del: value.is_none() as u8,
        });
    }
    fn clear(&mut self) {
        self.t.clear();
        self.r.clear();
    }
}

impl Store {
    pub fn tessera() -> Self {
        let mut options = Options::default();
        if let Ok(ratio) = std::env::var("TESSERA_BENCH_LEVEL_RATIO") {
            options.level_ratio = ratio.parse().expect("integer level ratio");
        }
        if let Ok(threads) = std::env::var("TESSERA_BENCH_BACKGROUND_THREADS") {
            options.background_threads = threads.parse().expect("integer builder count");
        }
        eprintln!(
            "Tessera: level_ratio={}, background_threads={}",
            options.level_ratio, options.background_threads
        );
        let engine = Engine::in_memory(options).unwrap();
        let ks = engine.keyspace("bench").unwrap();
        Self::T(engine, ks)
    }
    pub fn rocks(dir: &str, cache: u64) -> Self {
        let c = CString::new(dir).unwrap();
        Self::R(unsafe { tb_open(c.as_ptr(), cache) })
    }
    pub fn batch(&self) -> Batch {
        let ks = if let Self::T(_, ks) = self {
            Some(ks.clone())
        } else {
            None
        };
        Batch {
            rocks: matches!(self, Self::R(_)),
            t: WriteBatch::new(),
            ks,
            r: Vec::new(),
        }
    }
    pub fn reader(&self) -> Reader<'_> {
        let t = if let Self::T(e, _) = self {
            Some(e.reader())
        } else {
            None
        };
        Reader {
            t,
            r: unsafe { tb_reader() },
            keys: Vec::new(),
        }
    }
    pub fn snapshot(&self) -> Option<Snapshot> {
        if let Self::T(e, ks) = self {
            Some(Snapshot {
                snap: e.snapshot(),
                ks: ks.clone(),
            })
        } else {
            None
        }
    }
    pub fn maintenance(&self) -> Option<tessera::diagnostics::Maintenance> {
        if let Self::T(e, _) = self {
            Some(tessera::diagnostics::maintenance(e))
        } else {
            None
        }
    }
    /// Writes and clears `b`.
    pub fn write(&self, b: &mut Batch) {
        match self {
            Self::T(e, _) => {
                e.write(&b.t).unwrap();
            }
            Self::R(p) => unsafe { tb_write(*p, b.r.as_ptr(), b.r.len()) },
        }
        b.clear();
    }
    /// Ingests sorted unique records, through `file` for RocksDB. Tessera
    /// builds a large batch of ascending keys into runs directly.
    pub fn ingest(&self, b: &mut Batch, file: &str) {
        match self {
            Self::T(e, _) => {
                e.write(&b.t).unwrap();
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
            Self::T(_, ks) => f(r.t.as_mut().unwrap().get(ks, k).unwrap()),
            Self::R(p) => {
                let (mut v, mut n) = (std::ptr::null(), 0);
                let found = unsafe { tb_get(*p, r.r, k.as_ptr(), k.len(), &mut v, &mut n) };
                f((found == 1).then(|| unsafe { std::slice::from_raw_parts(v, n) }))
            }
        }
    }
    pub fn multi_get<K: AsRef<[u8]>, F: FnMut(usize, Option<&[u8]>)>(
        &self,
        keys: &[K],
        sorted: bool,
        r: &mut Reader,
        mut f: F,
    ) {
        match self {
            Self::T(_, ks) => {
                for (i, v) in
                    r.t.as_mut()
                        .unwrap()
                        .multi_get(ks, keys)
                        .unwrap()
                        .iter()
                        .enumerate()
                {
                    f(i, *v)
                }
            }
            Self::R(p) => {
                unsafe extern "C" fn cb<F: FnMut(usize, Option<&[u8]>)>(
                    ctx: *mut c_void,
                    i: usize,
                    v: *const u8,
                    n: usize,
                ) {
                    let f = unsafe { &mut *(ctx as *mut F) };
                    f(
                        i,
                        (!v.is_null()).then(|| unsafe { std::slice::from_raw_parts(v, n) }),
                    )
                }
                let kl = keys.first().map_or(0, |k| k.as_ref().len());
                r.keys.clear();
                keys.iter()
                    .for_each(|k| r.keys.extend_from_slice(k.as_ref()));
                let ctx = &mut f as *mut F as *mut c_void;
                unsafe {
                    tb_multi_get(
                        *p,
                        r.r,
                        r.keys.as_ptr(),
                        kl,
                        keys.len(),
                        sorted as i32,
                        ctx,
                        cb::<F>,
                    )
                }
            }
        }
    }
    pub fn scan<F: FnMut(&[u8], &[u8])>(&self, prefix: &[u8], r: &mut Reader, mut f: F) {
        match self {
            Self::T(_, ks) => {
                let mut scan = r.t.as_mut().unwrap().prefix(ks, prefix);
                while let Some((k, v)) = scan.next().unwrap() {
                    f(k, v)
                }
            }
            Self::R(p) => {
                unsafe extern "C" fn cb<F: FnMut(&[u8], &[u8])>(
                    ctx: *mut c_void,
                    k: *const u8,
                    kl: usize,
                    v: *const u8,
                    vl: usize,
                ) {
                    let f = unsafe { &mut *(ctx as *mut F) };
                    unsafe {
                        f(
                            std::slice::from_raw_parts(k, kl),
                            std::slice::from_raw_parts(v, vl),
                        )
                    }
                }
                let ctx = &mut f as *mut F as *mut c_void;
                unsafe { tb_scan(*p, prefix.as_ptr(), prefix.len(), ctx, cb::<F>) }
            }
        }
    }
    /// Flushes and waits until compaction is idle.
    pub fn drain(&self) {
        match self {
            Self::T(e, _) => {
                e.flush().unwrap();
                e.wait_for_compaction().unwrap();
                let s = e.background_status().unwrap();
                assert!(
                    s.failures == 0 && s.panic.is_none(),
                    "compaction failed: {s:?}"
                );
            }
            Self::R(p) => unsafe { tb_drain(*p) },
        }
    }
    pub fn emit_levels(&self, phase: &str, live_keys: impl FnOnce() -> usize, out: &super::Out) {
        if std::env::var_os("TESSERA_BENCH_LEVEL_DIAG").is_none() {
            return;
        }
        let live_keys = live_keys();
        if let Self::T(engine, _) = self {
            let levels = tessera::diagnostics::levels(engine);
            let bottom = levels.len().saturating_sub(1);
            let mut total_records = 0usize;
            let mut non_bottom_runs = 0usize;
            let mut non_bottom_records = 0usize;
            let mut non_bottom_stored = 0usize;
            for (idx, l) in levels.iter().enumerate() {
                let stored = l.frames + l.dictionaries + l.segment_table + l.index + l.footer;
                total_records += l.records;
                if idx < bottom {
                    non_bottom_runs += l.runs;
                    non_bottom_records += l.records;
                    non_bottom_stored += stored;
                }
                out.emit(phase, &format!("level_{idx}_runs"), l.runs);
                out.emit(phase, &format!("level_{idx}_records"), l.records);
                out.emit(phase, &format!("level_{idx}_raw_bytes"), l.raw);
                out.emit(phase, &format!("level_{idx}_frames_bytes"), l.frames);
                out.emit(phase, &format!("level_{idx}_dict_bytes"), l.dictionaries);
                out.emit(phase, &format!("level_{idx}_stored_bytes"), stored);
                out.emit(phase, &format!("level_{idx}_filter_bytes"), l.filters);
            }
            out.emit(phase, "bottom_level_index", bottom);
            out.emit(
                phase,
                "level0_flushes",
                tessera::diagnostics::level0_flushes(engine),
            );
            out.emit(phase, "non_bottom_runs", non_bottom_runs);
            out.emit(phase, "non_bottom_records", non_bottom_records);
            out.emit(phase, "non_bottom_stored_bytes", non_bottom_stored);
            out.emit(
                phase,
                "non_bottom_has_data",
                usize::from(non_bottom_runs > 0),
            );
            out.emit(phase, "distinct_live_keys", live_keys);
            out.emit(phase, "total_stored_records", total_records);
            out.emit(
                phase,
                "obsolete_records",
                total_records.saturating_sub(live_keys),
            );
            let stats = engine.engine_stats();
            let bg = engine.background_status().unwrap();
            out.emit(phase, "flushes_total", stats.flushes);
            out.emit(phase, "compactions_total", bg.compactions);
            let copy = tessera::diagnostics::copy_forward(engine);
            out.emit(phase, "copy_forward_blocks", copy.blocks);
            out.emit(phase, "copy_forward_raw", copy.raw);
        }
    }
    pub fn emit_shape(&self, phase: &str, out: &super::Out) {
        if let Self::T(engine, _) = self {
            let shape = tessera::diagnostics::streaming_shape(engine);
            out.emit(
                phase,
                "runs_per_level",
                shape
                    .runs_per_level
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(";"),
            );
            out.emit(phase, "max_overlap", shape.max_overlap);
            out.emit(phase, "level0_runs", shape.level0_runs);
            out.emit(phase, "bottom_runs", shape.bottom_runs);
            out.emit(
                phase,
                "bottom_mean_stored_bytes",
                shape.bottom_mean_stored_bytes,
            );
            out.emit(phase, "bounded_views", shape.bounded_views);
            out.emit(phase, "stored_bytes", shape.stored_bytes);
            let m = tessera::diagnostics::maintenance(engine);
            out.emit(
                phase,
                "writer_wait_total_s",
                format!("{:.3}", m.writer_wait_ns as f64 / 1e9),
            );
            out.emit(
                phase,
                "level0_wait_total_s",
                format!("{:.3}", m.level0_wait_ns as f64 / 1e9),
            );
            out.emit(phase, "level0_waits_total", m.level0_waits);
        }
    }
    /// Stored runs plus held filters/segment metadata, after a drain.
    pub fn accounted_run_bytes(&self) -> Option<u64> {
        match self {
            Self::T(e, _) => {
                let s = e.engine_stats();
                Some((s.run_bytes + s.held_bytes) as u64)
            }
            Self::R(_) => None,
        }
    }
    pub fn pool_stats(&self) -> Option<(usize, usize, usize)> {
        if let Self::T(e, _) = self {
            let s = e.engine_stats();
            Some((
                s.pool_reserve_bytes,
                s.pool_in_use_bytes,
                s.pool_reserve_limit,
            ))
        } else {
            None
        }
    }
    /// Flush bytes written, compaction bytes written, stored run/SST bytes.
    pub fn counters(&self) -> [u64; 3] {
        match self {
            Self::T(e, _) => {
                let s = e.engine_stats();
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
