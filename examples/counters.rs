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
fn scenario(name: &str, weak: bool, clustered: bool, records: usize, small: bool) {
    let value = |i, version, weak| {
        let mut out = value(i, version, weak);
        if small {
            out.truncate(48);
        }
        out
    };
    let engine = Engine::new(EngineOptions::default()).unwrap();
    let mut order: Vec<_> = (0..records).collect();
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
    let mut updated = vec![false; records];
    let mut updates: Vec<_> = if clustered {
        (records / MEMBERS * 9 / 10 * MEMBERS..records).collect()
    } else {
        (0..records).collect()
    };
    if !clustered {
        rng.shuffle(&mut updates);
        updates.truncate(records / 2);
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
    for start in (0..records).step_by(128) {
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
    // Fixed present keys and in-range gaps. Each get has its own block load;
    // workspace buffers are reused, but decoded blocks are never cached.
    let mut hits = EngineWorkspace::new();
    for at in 0..10_000 {
        let i = at * 7919 % records;
        assert_eq!(
            engine.get(&key(i), &mut hits).unwrap().unwrap().value,
            value(i, u8::from(updated[i]), weak)
        );
    }
    let mut misses = EngineWorkspace::new();
    for at in 0..1_000 {
        let i = at * 7919 % (records - 1);
        let mut missing = key(i).to_vec();
        missing.push(0);
        assert!(engine.get(&missing, &mut misses).unwrap().is_none());
    }
    let s = engine.stats();
    println!(
        "{name},{},{},{},{},{:.3},{:.3},{},{}",
        s.run_bytes,
        s.filter_bytes,
        s.index_bytes,
        s.block_bytes_live,
        ratio(hits.decoded_bytes(), 10_000),
        ratio(misses.decoded_bytes(), 1_000),
        hits.blocks_loaded(),
        misses.blocks_loaded()
    );
    engine.close().unwrap();
}
fn main() {
    println!(
        "scenario,stored_bytes,filter_bytes,index_bytes,frame_bytes,hit_decoded_bytes_per_get,miss_decoded_bytes_per_get,hit_blocks,miss_blocks"
    );
    scenario("B-random", true, false, RECORDS, false);
    scenario("clustered", false, true, RECORDS, false);
    scenario("A-random", false, false, RECORDS, false);
    scenario("A-large", false, false, 200_704, true);
}
