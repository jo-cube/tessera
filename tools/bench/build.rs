// Links the cached RocksDB static library (see run.sh) and the C++ adapter.
fn main() {
    let root = std::env::var("ROCKSDB_DIR").expect("ROCKSDB_DIR is required; use run.sh");
    cc::Build::new()
        .cpp(true)
        .std("c++20")
        .include(format!("{root}/include"))
        .file("native.cc")
        .define("NDEBUG", None)
        .flag("-O2")
        .compile("bench_native");
    println!("cargo:rustc-link-search=native={root}");
    println!("cargo:rustc-link-lib=static=rocksdb");
    println!("cargo:rustc-link-search=native=/opt/homebrew/opt/lz4/lib");
    println!("cargo:rustc-link-search=native=/opt/homebrew/opt/zstd/lib");
    for lib in ["lz4", "zstd", "z", "bz2", "c++"] {
        println!("cargo:rustc-link-lib={lib}");
    }
    println!("cargo:rerun-if-changed=native.cc");
    println!("cargo:rerun-if-env-changed=ROCKSDB_DIR");
}
