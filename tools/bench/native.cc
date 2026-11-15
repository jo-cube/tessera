// Benchmark-only C adapter over the RocksDB C++ API.
#include <rocksdb/advanced_cache.h>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/version.h>
#include <rocksdb/write_batch.h>
#include <mach/mach.h>
#include <malloc/malloc.h>
#include <libproc.h>
#include <sys/resource.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>
using namespace ROCKSDB_NAMESPACE;

static void check(const Status& s) {
  if (!s.ok()) { fprintf(stderr, "RocksDB error: %s\n", s.ToString().c_str()); abort(); }
}

// The DB lives on a RAM disk and the WAL is off; syncs are suppressed so no
// F_FULLFSYNC reaches APFS.
class NoSyncFile final : public FSWritableFileWrapper {
  std::unique_ptr<FSWritableFile> owner_;
 public:
  explicit NoSyncFile(std::unique_ptr<FSWritableFile> f) : FSWritableFileWrapper(f.get()), owner_(std::move(f)) {}
  IOStatus Sync(const IOOptions&, IODebugContext*) override { return IOStatus::OK(); }
  IOStatus Fsync(const IOOptions&, IODebugContext*) override { return IOStatus::OK(); }
  IOStatus RangeSync(uint64_t, uint64_t, const IOOptions&, IODebugContext*) override { return IOStatus::OK(); }
};
class NoSyncDir final : public FSDirectoryWrapper {
 public:
  explicit NoSyncDir(std::unique_ptr<FSDirectory> f) : FSDirectoryWrapper(std::move(f)) {}
  IOStatus Fsync(const IOOptions&, IODebugContext*) override { return IOStatus::OK(); }
  IOStatus FsyncWithDirOptions(const IOOptions&, IODebugContext*, const DirFsyncOptions&) override { return IOStatus::OK(); }
};
class NoSyncFS final : public FileSystemWrapper {
  static void wrap(const IOStatus& s, std::unique_ptr<FSWritableFile>* f) { if (s.ok()) f->reset(new NoSyncFile(std::move(*f))); }
 public:
  NoSyncFS() : FileSystemWrapper(FileSystem::Default()) {}
  const char* Name() const override { return "TesseraBenchNoSyncFS"; }
  IOStatus NewWritableFile(const std::string& n, const FileOptions& o, std::unique_ptr<FSWritableFile>* f, IODebugContext* d) override { auto s = target()->NewWritableFile(n, o, f, d); wrap(s, f); return s; }
  IOStatus ReopenWritableFile(const std::string& n, const FileOptions& o, std::unique_ptr<FSWritableFile>* f, IODebugContext* d) override { auto s = target()->ReopenWritableFile(n, o, f, d); wrap(s, f); return s; }
  IOStatus ReuseWritableFile(const std::string& n, const std::string& old, const FileOptions& o, std::unique_ptr<FSWritableFile>* f, IODebugContext* d) override { auto s = target()->ReuseWritableFile(n, old, o, f, d); wrap(s, f); return s; }
  IOStatus NewDirectory(const std::string& n, const IOOptions& o, std::unique_ptr<FSDirectory>* f, IODebugContext* d) override { auto s = target()->NewDirectory(n, o, f, d); if (s.ok()) f->reset(new NoSyncDir(std::move(*f))); return s; }
  IOStatus SyncFile(const std::string&, const FileOptions&, const IOOptions&, bool, IODebugContext*) override { return IOStatus::OK(); }
};

struct Store { std::unique_ptr<Env> env; std::shared_ptr<Cache> cache; Options opts; std::unique_ptr<DB> db; WriteOptions w; };
struct Reader { PinnableSlice value; std::vector<Slice> keys; std::vector<PinnableSlice> values; std::vector<Status> statuses; };
struct Rec { const char* key; size_t key_len; const char* value; size_t value_len; unsigned char del; };

extern "C" {
uint64_t tb_rss() {
  mach_task_basic_info_data_t m{};
  mach_msg_type_number_t n = MACH_TASK_BASIC_INFO_COUNT;
  task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&m, &n);
  return m.resident_size;
}
uint64_t tb_live_heap() {
  malloc_statistics_t stats{};
  // A null zone aggregates every allocator zone in the process.
  malloc_zone_statistics(nullptr, &stats);
  return stats.size_in_use;
}
uint64_t tb_footprint() {
  rusage_info_v2 info{};
  if (proc_pid_rusage(getpid(), RUSAGE_INFO_V2, reinterpret_cast<rusage_info_t*>(&info)) != 0) {
    perror("proc_pid_rusage"); abort();
  }
  return info.ri_phys_footprint;
}
const char* tb_version() { static std::string v = GetRocksVersionAsString(true); return v.c_str(); }

void* tb_open(const char* path, uint64_t cache_bytes) {
  auto* s = new Store;
  s->env = NewCompositeEnv(std::make_shared<NoSyncFS>());
  Options& o = s->opts;
  o.create_if_missing = true;
  o.env = s->env.get();
  o.statistics = CreateDBStatistics();
  o.max_background_jobs = 4;
  o.compaction_style = kCompactionStyleLevel;
  o.level_compaction_dynamic_level_bytes = true;
  o.write_buffer_size = 64ULL << 20;
  o.max_write_buffer_number = 2;
  o.compression = kLZ4Compression;
  o.bottommost_compression = kZSTD;
  o.bottommost_compression_opts.enabled = true;
  o.bottommost_compression_opts.max_dict_bytes = 16 << 10;
  o.bottommost_compression_opts.zstd_max_train_bytes = 100 * (16 << 10);
  o.prefix_extractor.reset(NewFixedPrefixTransform(8));  // the 8-byte group id
  BlockBasedTableOptions t;
  t.block_size = 16 << 10;
  t.filter_policy.reset(NewBloomFilterPolicy(10));
  t.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
  t.partition_filters = true;
  t.cache_index_and_filter_blocks = true;
  t.pin_top_level_index_and_filter = true;
  HyperClockCacheOptions c(cache_bytes, 0);  // 0: automatic entry sizing
  s->cache = c.MakeSharedCache();
  t.block_cache = s->cache;
  o.table_factory.reset(NewBlockBasedTableFactory(t));
  s->w.disableWAL = true;
  s->w.sync = false;
  check(DB::Open(o, path, &s->db));
  return s;
}
void tb_close(void* p) { auto* s = static_cast<Store*>(p); check(s->db->Close()); delete s; }

void tb_write(void* p, const Rec* r, size_t n) {
  auto* s = static_cast<Store*>(p);
  WriteBatch b;
  for (size_t i = 0; i < n; ++i) {
    Slice k(r[i].key, r[i].key_len);
    check(r[i].del ? b.Delete(k) : b.Put(k, Slice(r[i].value, r[i].value_len)));
  }
  check(s->db->Write(s->w, &b));
}

// Writes one sorted SST file at `file` and ingests it, moving the file.
void tb_ingest(void* p, const char* file, const Rec* r, size_t n) {
  auto* s = static_cast<Store*>(p);
  SstFileWriter w(EnvOptions(), s->opts);
  check(w.Open(file));
  for (size_t i = 0; i < n; ++i) check(w.Put(Slice(r[i].key, r[i].key_len), Slice(r[i].value, r[i].value_len)));
  check(w.Finish());
  IngestExternalFileOptions io;
  io.move_files = true;
  check(s->db->IngestExternalFile({file}, io));
}

void* tb_reader() { return new Reader; }
void tb_reader_free(void* r) { delete static_cast<Reader*>(r); }

int tb_get(void* p, void* rp, const char* k, size_t kl, const char** v, size_t* vl) {
  auto* s = static_cast<Store*>(p); auto* r = static_cast<Reader*>(rp);
  r->value.Reset();
  auto st = s->db->Get(ReadOptions(), s->db->DefaultColumnFamily(), Slice(k, kl), &r->value);
  if (st.IsNotFound()) return 0;
  check(st);
  *v = r->value.data(); *vl = r->value.size();
  return 1;
}

typedef void (*Visit)(void*, size_t, const char*, size_t);
// Keys are `n` keys of `kl` bytes each, packed.
void tb_multi_get(void* p, void* rp, const char* keys, size_t kl, size_t n, int sorted, void* ctx, Visit visit) {
  auto* s = static_cast<Store*>(p); auto* r = static_cast<Reader*>(rp);
  r->keys.clear();
  for (size_t i = 0; i < n; ++i) r->keys.emplace_back(keys + i * kl, kl);
  if (r->values.size() < n) { r->values.resize(n); r->statuses.resize(n); }
  for (size_t i = 0; i < n; ++i) r->values[i].Reset();
  s->db->MultiGet(ReadOptions(), s->db->DefaultColumnFamily(), n, r->keys.data(), r->values.data(), r->statuses.data(), sorted != 0);
  for (size_t i = 0; i < n; ++i) {
    if (r->statuses[i].IsNotFound()) { visit(ctx, i, nullptr, 0); continue; }
    check(r->statuses[i]);
    visit(ctx, i, r->values[i].data(), r->values[i].size());
  }
}

typedef void (*ScanVisit)(void*, const char*, size_t, const char*, size_t);
void tb_scan(void* p, const char* pre, size_t n, void* ctx, ScanVisit visit) {
  auto* s = static_cast<Store*>(p);
  std::string upper(pre, n);
  while (!upper.empty() && (unsigned char)upper.back() == 255) upper.pop_back();
  if (upper.empty()) abort();
  upper.back()++;
  Slice end(upper);
  ReadOptions o;
  o.iterate_upper_bound = &end;
  o.prefix_same_as_start = true;
  std::unique_ptr<Iterator> it(s->db->NewIterator(o));
  for (it->Seek(Slice(pre, n)); it->Valid(); it->Next())
    visit(ctx, it->key().data(), it->key().size(), it->value().data(), it->value().size());
  check(it->status());
}

// Flushes memtables and waits until no flush or compaction is pending.
void tb_drain(void* p) {
  auto* s = static_cast<Store*>(p);
  WaitForCompactOptions o;
  o.flush = true;
  check(s->db->WaitForCompact(o));
  uint64_t errors = 0;
  if (!s->db->GetIntProperty("rocksdb.background-errors", &errors) || errors) abort();
}

// flush bytes, compaction bytes, SST bytes, memtable bytes, cache usage
void tb_stats(void* p, uint64_t* a) {
  auto* s = static_cast<Store*>(p);
  a[0] = s->opts.statistics->getTickerCount(FLUSH_WRITE_BYTES);
  a[1] = s->opts.statistics->getTickerCount(COMPACT_WRITE_BYTES);
  s->db->GetIntProperty("rocksdb.total-sst-files-size", &a[2]);
  s->db->GetIntProperty("rocksdb.cur-size-all-mem-tables", &a[3]);
  a[4] = s->cache->GetUsage();
}
}
