//! The few RocksDB C API calls the comparison needs, declared by hand from
//! `rocksdb/c.h` of RocksDB 7.8.3 and linked against the system library.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::Path;
use std::ptr;

/// Every RocksDB handle is opaque here.
#[repr(C)]
pub struct Handle {
    _private: [u8; 0],
}

type H = *mut Handle;
type Err = *mut *mut c_char;

#[link(name = "rocksdb")]
unsafe extern "C" {
    fn rocksdb_options_create() -> H;
    fn rocksdb_options_destroy(o: H);
    fn rocksdb_options_set_create_if_missing(o: H, v: u8);
    fn rocksdb_options_set_compression(o: H, v: c_int);
    fn rocksdb_options_set_compression_options(
        o: H,
        window_bits: c_int,
        level: c_int,
        strategy: c_int,
        max_dict_bytes: c_int,
    );
    fn rocksdb_options_set_compression_options_zstd_max_train_bytes(o: H, v: c_int);
    fn rocksdb_options_set_bottommost_compression(o: H, v: c_int);
    fn rocksdb_options_set_bottommost_compression_options(
        o: H,
        window_bits: c_int,
        level: c_int,
        strategy: c_int,
        max_dict_bytes: c_int,
        enabled: u8,
    );
    fn rocksdb_options_set_bottommost_compression_options_zstd_max_train_bytes(
        o: H,
        v: c_int,
        enabled: u8,
    );
    fn rocksdb_options_set_write_buffer_size(o: H, v: usize);
    fn rocksdb_options_set_max_write_buffer_number(o: H, v: c_int);
    fn rocksdb_options_set_max_background_jobs(o: H, v: c_int);
    fn rocksdb_options_set_target_file_size_base(o: H, v: u64);
    fn rocksdb_options_set_allow_mmap_reads(o: H, v: u8);
    fn rocksdb_options_set_level_compaction_dynamic_level_bytes(o: H, v: u8);
    fn rocksdb_options_set_block_based_table_factory(o: H, t: H);
    fn rocksdb_block_based_options_create() -> H;
    fn rocksdb_block_based_options_destroy(t: H);
    fn rocksdb_block_based_options_set_block_size(t: H, v: usize);
    fn rocksdb_block_based_options_set_no_block_cache(t: H, v: u8);
    fn rocksdb_block_based_options_set_block_cache(t: H, cache: H);
    fn rocksdb_block_based_options_set_filter_policy(t: H, policy: H);
    fn rocksdb_filterpolicy_create_bloom_full(bits_per_key: f64) -> H;
    fn rocksdb_cache_create_lru(capacity: usize) -> H;
    fn rocksdb_cache_destroy(cache: H);
    fn rocksdb_cache_get_usage(cache: H) -> usize;
    fn rocksdb_open(o: H, name: *const c_char, err: Err) -> H;
    fn rocksdb_close(db: H);
    fn rocksdb_writeoptions_create() -> H;
    fn rocksdb_writeoptions_destroy(w: H);
    fn rocksdb_writeoptions_disable_WAL(w: H, v: c_int);
    fn rocksdb_put(db: H, w: H, k: *const c_char, kl: usize, v: *const c_char, vl: usize, err: Err);
    fn rocksdb_delete(db: H, w: H, k: *const c_char, kl: usize, err: Err);
    fn rocksdb_readoptions_create() -> H;
    fn rocksdb_readoptions_destroy(r: H);
    fn rocksdb_readoptions_set_iterate_upper_bound(r: H, k: *const c_char, kl: usize);
    fn rocksdb_get_pinned(db: H, r: H, k: *const c_char, kl: usize, err: Err) -> H;
    fn rocksdb_pinnableslice_value(p: H, len: *mut usize) -> *const c_char;
    fn rocksdb_pinnableslice_destroy(p: H);
    fn rocksdb_create_iterator(db: H, r: H) -> H;
    fn rocksdb_iter_destroy(i: H);
    fn rocksdb_iter_seek(i: H, k: *const c_char, kl: usize);
    fn rocksdb_iter_valid(i: H) -> u8;
    fn rocksdb_iter_next(i: H);
    fn rocksdb_iter_key(i: H, len: *mut usize) -> *const c_char;
    fn rocksdb_iter_value(i: H, len: *mut usize) -> *const c_char;
    fn rocksdb_iter_get_error(i: H, err: Err);
    fn rocksdb_flushoptions_create() -> H;
    fn rocksdb_flushoptions_destroy(f: H);
    fn rocksdb_flushoptions_set_wait(f: H, v: u8);
    fn rocksdb_flush(db: H, f: H, err: Err);
    fn rocksdb_compactoptions_create() -> H;
    fn rocksdb_compactoptions_destroy(c: H);
    fn rocksdb_compactoptions_set_bottommost_level_compaction(c: H, v: u8);
    fn rocksdb_compactoptions_set_exclusive_manual_compaction(c: H, v: u8);
    fn rocksdb_compact_range_opt(
        db: H,
        c: H,
        start: *const c_char,
        start_len: usize,
        limit: *const c_char,
        limit_len: usize,
    );
    fn rocksdb_property_value(db: H, name: *const c_char) -> *mut c_char;
    fn rocksdb_property_int(db: H, name: *const c_char, out: *mut u64) -> c_int;
    fn rocksdb_free(p: *mut c_void);
    fn rocksdb_set_perf_level(level: c_int);
    fn rocksdb_perfcontext_create() -> H;
    fn rocksdb_perfcontext_reset(pc: H);
    fn rocksdb_perfcontext_metric(pc: H, metric: c_int) -> u64;
    fn rocksdb_perfcontext_destroy(pc: H);
}

const ZSTD: c_int = 7;
const LZ4: c_int = 4;
/// `BottommostLevelCompaction::kForce`.
const FORCE: u8 = 2;
const PERF_DISABLE: c_int = 1;
const PERF_COUNT: c_int = 2;

/// Perf context metrics, by their position in the C API's enum.
pub mod metric {
    pub const BLOCK_READ_COUNT: i32 = 2;
    pub const INTERNAL_KEY_SKIPPED: i32 = 10;
    pub const INTERNAL_DELETE_SKIPPED: i32 = 11;
    pub const SEEK_CHILD_SEEK_COUNT: i32 = 24;
    pub const BLOOM_SST_HIT: i32 = 44;
    pub const BLOOM_SST_MISS: i32 = 45;
}

fn check(err: *mut c_char, what: &str) {
    if !err.is_null() {
        // SAFETY: RocksDB returned a NUL-terminated message it allocated.
        let message = unsafe { CStr::from_ptr(err) }
            .to_string_lossy()
            .into_owned();
        unsafe { rocksdb_free(err.cast()) };
        panic!("rocksdb {what}: {message}");
    }
}

/// The configurations compared.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub lz4: bool,
    /// Bytes per data block before compression: 16 KiB, as 5Y, or 4 KiB.
    pub block_size: usize,
    /// Zstandard dictionaries of 16 KiB trained per SST file from up to
    /// 1.6 MiB of samples, the sizes Tessera uses, at every level.
    pub dictionary: bool,
    /// An LRU cache of decompressed blocks of this many bytes, mmap reads
    /// and dynamic level sizes; `None` for the architecture-matched setup.
    pub tuned_cache: Option<usize>,
}

pub struct Db {
    db: H,
    options: H,
    write: H,
    read: H,
    scan: H,
    cache: H,
    perf: H,
}

impl Db {
    pub fn open(path: &Path, config: Config) -> Self {
        // SAFETY: each handle is created here, used only through this
        // struct, and destroyed once in `drop`.
        unsafe {
            let options = rocksdb_options_create();
            rocksdb_options_set_create_if_missing(options, 1);
            let codec = if config.lz4 { LZ4 } else { ZSTD };
            rocksdb_options_set_compression(options, codec);
            // Zstandard level 3, as Tessera, with or without a dictionary.
            let dict_bytes: c_int = if config.dictionary { 16 << 10 } else { 0 };
            let train_bytes: c_int = if config.dictionary {
                100 * (16 << 10)
            } else {
                0
            };
            rocksdb_options_set_compression_options(options, -14, 3, 0, dict_bytes);
            rocksdb_options_set_compression_options_zstd_max_train_bytes(options, train_bytes);
            if config.dictionary {
                rocksdb_options_set_bottommost_compression(options, codec);
                rocksdb_options_set_bottommost_compression_options(
                    options, -14, 3, 0, dict_bytes, 1,
                );
                rocksdb_options_set_bottommost_compression_options_zstd_max_train_bytes(
                    options,
                    train_bytes,
                    1,
                );
            }
            rocksdb_options_set_write_buffer_size(options, 64 << 20);
            rocksdb_options_set_max_write_buffer_number(options, 2);
            rocksdb_options_set_max_background_jobs(options, 2);
            rocksdb_options_set_target_file_size_base(options, 64 << 20);

            let table = rocksdb_block_based_options_create();
            rocksdb_block_based_options_set_block_size(table, config.block_size);
            // Ownership passes to the table options.
            rocksdb_block_based_options_set_filter_policy(
                table,
                rocksdb_filterpolicy_create_bloom_full(10.0),
            );
            let mut cache = ptr::null_mut();
            match config.tuned_cache {
                None => rocksdb_block_based_options_set_no_block_cache(table, 1),
                Some(bytes) => {
                    cache = rocksdb_cache_create_lru(bytes);
                    rocksdb_block_based_options_set_block_cache(table, cache);
                    rocksdb_options_set_allow_mmap_reads(options, 1);
                    rocksdb_options_set_level_compaction_dynamic_level_bytes(options, 1);
                }
            }
            rocksdb_options_set_block_based_table_factory(options, table);
            rocksdb_block_based_options_destroy(table);

            let name = CString::new(path.to_str().unwrap()).unwrap();
            let mut err = ptr::null_mut();
            let db = rocksdb_open(options, name.as_ptr(), &mut err);
            check(err, "open");

            let write = rocksdb_writeoptions_create();
            rocksdb_writeoptions_disable_WAL(write, 1);
            Self {
                db,
                options,
                write,
                read: rocksdb_readoptions_create(),
                scan: rocksdb_readoptions_create(),
                cache,
                perf: ptr::null_mut(),
            }
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let mut err = ptr::null_mut();
        // SAFETY: the slices outlive the call, which copies them.
        unsafe {
            rocksdb_put(
                self.db,
                self.write,
                key.as_ptr().cast(),
                key.len(),
                value.as_ptr().cast(),
                value.len(),
                &mut err,
            );
        }
        check(err, "put");
    }

    pub fn delete(&self, key: &[u8]) {
        let mut err = ptr::null_mut();
        // SAFETY: as for `put`.
        unsafe {
            rocksdb_delete(
                self.db,
                self.write,
                key.as_ptr().cast(),
                key.len(),
                &mut err,
            )
        };
        check(err, "delete");
    }

    /// Calls `found` with the value of `key`, if any, while it is pinned.
    pub fn get<T>(&self, key: &[u8], found: impl FnOnce(Option<&[u8]>) -> T) -> T {
        let mut err = ptr::null_mut();
        // SAFETY: the pinned slice is valid until it is destroyed below.
        unsafe {
            let pinned =
                rocksdb_get_pinned(self.db, self.read, key.as_ptr().cast(), key.len(), &mut err);
            check(err, "get");
            if pinned.is_null() {
                return found(None);
            }
            let mut len = 0;
            let value = rocksdb_pinnableslice_value(pinned, &mut len);
            let result = found(Some(std::slice::from_raw_parts(value.cast(), len)));
            rocksdb_pinnableslice_destroy(pinned);
            result
        }
    }

    /// Visits every key starting with `prefix`, iterating up to the
    /// prefix's successor as RocksDB's upper bound.
    pub fn scan(&self, prefix: &[u8], mut visit: impl FnMut(&[u8], &[u8])) {
        let bound = crate::successor(prefix);
        // SAFETY: the read options copy the bound; key and value slices are
        // valid until the iterator moves, and are not kept.
        unsafe {
            match &bound {
                Some(bound) => rocksdb_readoptions_set_iterate_upper_bound(
                    self.scan,
                    bound.as_ptr().cast(),
                    bound.len(),
                ),
                None => rocksdb_readoptions_set_iterate_upper_bound(self.scan, ptr::null(), 0),
            }
            let iter = rocksdb_create_iterator(self.db, self.scan);
            rocksdb_iter_seek(iter, prefix.as_ptr().cast(), prefix.len());
            while rocksdb_iter_valid(iter) != 0 {
                let (mut kl, mut vl) = (0, 0);
                let key = std::slice::from_raw_parts(rocksdb_iter_key(iter, &mut kl).cast(), kl);
                if !key.starts_with(prefix) {
                    break;
                }
                let value =
                    std::slice::from_raw_parts(rocksdb_iter_value(iter, &mut vl).cast(), vl);
                visit(key, value);
                rocksdb_iter_next(iter);
            }
            let mut err = ptr::null_mut();
            rocksdb_iter_get_error(iter, &mut err);
            rocksdb_iter_destroy(iter);
            check(err, "iterate");
        }
    }

    pub fn flush(&self) {
        let mut err = ptr::null_mut();
        // SAFETY: the options handle is destroyed after the call.
        unsafe {
            let options = rocksdb_flushoptions_create();
            rocksdb_flushoptions_set_wait(options, 1);
            rocksdb_flush(self.db, options, &mut err);
            rocksdb_flushoptions_destroy(options);
        }
        check(err, "flush");
    }

    /// Compacts the whole key range into the bottom level, rewriting it.
    pub fn compact_all(&self) {
        // SAFETY: null bounds mean the whole range.
        unsafe {
            let options = rocksdb_compactoptions_create();
            rocksdb_compactoptions_set_bottommost_level_compaction(options, FORCE);
            rocksdb_compactoptions_set_exclusive_manual_compaction(options, 1);
            rocksdb_compact_range_opt(self.db, options, ptr::null(), 0, ptr::null(), 0);
            rocksdb_compactoptions_destroy(options);
        }
    }

    pub fn int(&self, name: &str) -> u64 {
        let name = CString::new(name).unwrap();
        let mut out = 0;
        // SAFETY: `out` is written only on success.
        let status = unsafe { rocksdb_property_int(self.db, name.as_ptr(), &mut out) };
        assert_eq!(status, 0, "property {name:?}");
        out
    }

    pub fn string(&self, name: &str) -> String {
        let name = CString::new(name).unwrap();
        // SAFETY: the returned string is NUL-terminated and freed here.
        unsafe {
            let value = rocksdb_property_value(self.db, name.as_ptr());
            assert!(!value.is_null(), "property {name:?}");
            let text = CStr::from_ptr(value).to_string_lossy().into_owned();
            rocksdb_free(value.cast());
            text
        }
    }

    /// Whether no flush or compaction is running or pending.
    pub fn idle(&self) -> bool {
        [
            "rocksdb.num-running-compactions",
            "rocksdb.compaction-pending",
            "rocksdb.num-running-flushes",
            "rocksdb.mem-table-flush-pending",
        ]
        .iter()
        .all(|name| self.int(name) == 0)
    }

    pub fn cache_usage(&self) -> Option<usize> {
        // SAFETY: the cache lives as long as `self`.
        (!self.cache.is_null()).then(|| unsafe { rocksdb_cache_get_usage(self.cache) })
    }

    /// Turns this thread's perf counters on, reset, or off.
    pub fn counting(&mut self, on: bool) {
        // SAFETY: the perf context is this thread's and lives until drop.
        unsafe {
            if self.perf.is_null() {
                self.perf = rocksdb_perfcontext_create();
            }
            rocksdb_set_perf_level(if on { PERF_COUNT } else { PERF_DISABLE });
            rocksdb_perfcontext_reset(self.perf);
        }
    }

    pub fn metric(&self, metric: i32) -> u64 {
        if self.perf.is_null() {
            return 0;
        }
        // SAFETY: as for `counting`.
        unsafe { rocksdb_perfcontext_metric(self.perf, metric) }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // SAFETY: every handle was created in `open` or `counting`, and the
        // database closes before the options and cache it uses.
        unsafe {
            rocksdb_close(self.db);
            rocksdb_writeoptions_destroy(self.write);
            rocksdb_readoptions_destroy(self.read);
            rocksdb_readoptions_destroy(self.scan);
            rocksdb_options_destroy(self.options);
            if !self.cache.is_null() {
                rocksdb_cache_destroy(self.cache);
            }
            if !self.perf.is_null() {
                rocksdb_perfcontext_destroy(self.perf);
            }
        }
    }
}
