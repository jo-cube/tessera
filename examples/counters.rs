//! Deterministic, counter-only compaction scenarios; no clocks or timings.
use tessera::engine::{Engine, EngineOptions, EngineWorkspace, MergeWorkspace};
const ENTITIES: usize = 512;
const MEMBERS: usize = 64;
const RECORDS: usize = ENTITIES * MEMBERS;
const FLUSH: usize = 4096;
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn shuffle<T>(&mut self, values: &mut [T]) {
        for i in (1..values.len()).rev() {
            values.swap(i, self.next() as usize % (i + 1));
        }
    }
}
fn key(i: usize) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&(i as u64 / MEMBERS as u64).to_be_bytes());
    key[8..].copy_from_slice(&(i as u64 % MEMBERS as u64).to_be_bytes());
    key
}
fn value(i: usize, version: u8, weak: bool) -> Vec<u8> {
    if weak {
        let mut rng =
            Rng((i as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15) ^ (u64::from(version) << 40));
        (0..600)
            .map(|_| b"abcdefghijklmnopqrstuvwxyz0123456789"[rng.next() as usize % 36])
            .collect()
    } else {
        let prefix = format!(
            "{{\"entity\":{},\"member\":{},\"version\":{},\"status\":\"active\",\"region\":\"north\",\"description\":\"",
            i / MEMBERS,
            i % MEMBERS,
            version
        );
        let mut out = prefix.into_bytes();
        while out.len() < 598 {
            out.extend_from_slice(
                b"stable member profile with shared field names and repeated content ",
            );
        }
        out.truncate(598);
        out.extend_from_slice(b"\"}");
        out
    }
}
fn flush(engine: &Engine) {
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
}
fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 }
}
fn scenario(name: &str, weak: bool, clustered: bool) {
    let engine = Engine::new(EngineOptions::default()).unwrap();
    let mut order: Vec<_> = (0..RECORDS).collect();
    let mut rng = Rng(0x123456789abcdef);
    if !clustered {
        rng.shuffle(&mut order);
    }
    for (at, &i) in order.iter().enumerate() {
        engine.put(&key(i), &value(i, 0, weak)).unwrap();
        if (at + 1) % FLUSH == 0 {
            flush(&engine);
        }
    }
    let mut updated = vec![false; RECORDS];
    let mut updates: Vec<_> = if clustered {
        (ENTITIES * 9 / 10 * MEMBERS..RECORDS).collect()
    } else {
        (0..RECORDS).collect()
    };
    if !clustered {
        rng.shuffle(&mut updates);
        updates.truncate(RECORDS / 2);
    }
    for (at, &i) in updates.iter().enumerate() {
        engine.put(&key(i), &value(i, 1, weak)).unwrap();
        updated[i] = true;
        if (at + 1) % FLUSH == 0 {
            flush(&engine);
        }
    }
    flush(&engine);
    let mut merge = MergeWorkspace::new();
    while engine.compact(&mut merge).unwrap().is_some() {}
    engine.wait_for_compaction().unwrap();
    // All reader paths are checked against the independent version oracle.
    let mut ws = EngineWorkspace::new();
    for (i, &changed) in updated.iter().enumerate() {
        assert_eq!(
            engine.get(&key(i), &mut ws).unwrap().unwrap().value,
            value(i, u8::from(changed), weak)
        );
    }
    for start in (0..RECORDS).step_by(128) {
        let keys: Vec<_> = (start..start + 128).rev().map(key).collect();
        engine
            .multi_get(&keys, &mut ws, |at, got| {
                let i = start + 127 - at;
                assert_eq!(got.unwrap(), value(i, u8::from(updated[i]), weak));
            })
            .unwrap();
    }
    let guard = engine.read();
    let mut scan = guard.scan(&mut ws);
    for (i, &changed) in updated.iter().enumerate() {
        let rec = scan.next_record().unwrap().unwrap();
        assert_eq!(rec.key, key(i));
        assert_eq!(rec.value, value(i, u8::from(changed), weak));
    }
    assert!(scan.next_record().unwrap().is_none());
    drop(scan);
    drop(guard);
    let s = engine.stats();
    let c = s.build;
    println!(
        "{name},{:.6},{:.6},{:.6},{:.6},{},{},{},{:.6},{},{},{},{},{},{}",
        ratio(c.raw_bytes_compressed, c.raw_bytes_ingested),
        ratio(c.bytes_compressed + c.bytes_reused, c.raw_bytes_ingested),
        ratio(c.blocks_reused, c.blocks_reused + c.blocks_compressed),
        ratio(c.bytes_reused, c.bytes_reused + c.bytes_compressed),
        s.run_bytes,
        s.chunk_bytes_held,
        s.block_bytes_live,
        ratio(s.block_bytes_live as u64, s.chunk_bytes_held as u64),
        c.forced_undersized_blocks,
        c.filter_decoded_bytes,
        c.raw_bytes_ingested,
        c.raw_bytes_compressed,
        c.blocks_compressed,
        c.blocks_reused
    );
    engine.close().unwrap();
}
fn main() {
    println!(
        "scenario,compressed_raw_per_ingested_raw,logical_frame_write_amplification,blocks_reused_fraction,bytes_reused_fraction,stored_bytes,chunk_bytes_held,block_bytes_live,live_fraction,forced_undersized_blocks,filter_decoded_bytes,ingested_raw_bytes,compressed_raw_bytes,new_blocks,reused_blocks"
    );
    scenario("B-random", true, false);
    scenario("clustered", false, true);
    scenario("A-random", false, false);
}
