//! Deterministic grouped and weak-locality JSON datasets. Keys, update values
//! and the independent oracle are generated before in-memory measurements,
//! keeping timed operations separate from input construction.
pub const MIB: usize = 1 << 20;

pub fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

pub struct Rng(pub u64);
impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        mix(self.0)
    }
    pub fn n(&mut self, n: usize) -> usize {
        (self.next() % (n as u64)) as usize
    }
    pub fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 * (1.0 / ((1u64 << 53) as f64))
    }
    pub(super) fn text(&mut self, n: usize) -> String {
        (0..n)
            .map(|_| b"abcdefghijklmnopqrstuvwxyz0123456789"[self.n(36)] as char)
            .collect()
    }
    pub(super) fn hex(&mut self, n: usize) -> String {
        (0..n)
            .map(|_| b"0123456789abcdef"[self.n(16)] as char)
            .collect()
    }
    pub fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            v.swap(i, self.n(i + 1));
        }
    }
}

/// Fixed-capacity key buffer supporting 16-byte (A, B) and 20-byte (C) keys
/// without per-key heap allocations.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct KeyBuf {
    bytes: [u8; 20],
    len: u8,
}

impl KeyBuf {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

impl AsRef<[u8]> for KeyBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Clone, Copy)]
pub struct Rec {
    key: [u8; 20],
    key_len: u8,
    off: u64,
    len: u32,
    upd_off: u64,
    upd_len: u32,
}

impl Rec {
    pub fn key(&self) -> &[u8] {
        &self.key[..usize::from(self.key_len)]
    }
    pub fn key_buf(&self) -> KeyBuf {
        KeyBuf {
            bytes: self.key,
            len: self.key_len,
        }
    }
    pub fn has_update(&self) -> bool {
        self.upd_len > 0
    }
}

pub const ORIGINAL: u8 = 0;
pub const UPDATED: u8 = 1;
pub const DELETED: u8 = 2;

pub struct Data {
    arena: Vec<u8>,
    pub key_len: usize,
    /// Records in key order.
    pub recs: Vec<Rec>,
    /// Record index ranges sharing an 8-byte group prefix.
    pub groups: Vec<(usize, usize)>,
    /// Groups of 64..=144 records, used by the prefix-scan case.
    pub scan_groups: Vec<usize>,
    /// Records overwritten by the update case, in write order.
    pub updates: Vec<u32>,
    /// Records deleted after the updates, in write order.
    pub deletes: Vec<u32>,
    pub raw: usize,
    /// Oracle: ORIGINAL, UPDATED or DELETED per record.
    pub state: Vec<u8>,
}

impl Data {
    pub fn value(&self, i: usize) -> &[u8] {
        let r = &self.recs[i];
        &self.arena[r.off as usize..r.off as usize + r.len as usize]
    }
    pub fn update(&self, i: usize) -> &[u8] {
        let r = &self.recs[i];
        &self.arena[r.upd_off as usize..r.upd_off as usize + r.upd_len as usize]
    }
    /// The oracle's current value.
    pub fn live(&self, i: usize) -> Option<&[u8]> {
        match self.state[i] {
            ORIGINAL => Some(self.value(i)),
            UPDATED => Some(self.update(i)),
            _ => None,
        }
    }
    pub fn live_bytes(&self) -> usize {
        (0..self.recs.len())
            .filter_map(|i| self.live(i))
            .map(|v| self.key_len + v.len())
            .sum()
    }
    /// A key absent from the data that shares a group prefix with record `i`.
    pub fn miss(&self, i: usize) -> KeyBuf {
        let mut k = self.recs[i].key_buf();
        k.bytes[8..16].copy_from_slice(&(u64::MAX - i as u64).to_be_bytes());
        k
    }
}

pub fn generate(name: char) -> Data {
    // TESSERA_BENCH_SCALE=n divides the dataset sizes, for smoke runs only.
    let scale: usize = std::env::var("TESSERA_BENCH_SCALE").map_or(1, |s| s.parse().unwrap());
    let target = match name {
        'A' => 1024 * MIB,
        'B' | 'C' => 512 * MIB,
        _ => panic!("unknown dataset {name}"),
    } / scale;
    generate_target(name, target)
}

pub fn generate_target(name: char, target: usize) -> Data {
    let mut rng = Rng(0x4c53504b ^ name as u64);
    let mut urng = Rng(0x55504454 ^ name as u64);
    let key_len = if name == 'C' { 20 } else { 16 };
    let mut arena = Vec::with_capacity(target * 3 / 2 + 64 * MIB);
    let (mut recs, mut groups, mut scan_groups) = (Vec::new(), Vec::new(), Vec::new());
    let (mut updates, mut deletes) = (Vec::new(), Vec::new());
    let mut raw = 0;
    let mut g = 0u64;
    let push = |arena: &mut Vec<u8>, s: String| {
        let off = arena.len() as u64;
        arena.extend_from_slice(s.as_bytes());
        (off, s.len() as u32)
    };
    while raw < target {
        let n = match name {
            'A' => {
                let q = rng.n(45);
                20 + q * q
            }
            'C' => 64,
            _ => 100,
        };
        let start = recs.len();
        let (a, b, c) = if name == 'C' {
            (String::new(), String::new(), String::new())
        } else {
            (rng.text(360), rng.text(260), rng.text(60))
        };
        for member in 0..n {
            let mut key = [0u8; 20];
            key[..8].copy_from_slice(&g.to_be_bytes());
            if name == 'C' {
                key[8..16].copy_from_slice(&((member / 4) as u64).to_be_bytes());
                key[16..20].copy_from_slice(&((member % 4) as u32).to_be_bytes());
            } else {
                key[8..16].copy_from_slice(&(member as u64).to_be_bytes());
            }
            let i = recs.len() as u32;
            // Half the keys are updated and 2% deleted, each chosen uniformly.
            let h = mix(0x75706464 ^ i as u64);
            let upd = h & 1 == 0;
            let (value, update) = match name {
                'A' => {
                    let json = |ts: usize, q: usize, p: usize, st: &str, tok: String| {
                        format!(
                            r#"{{"group_id":{g},"description":"{a}","attributes":"{b}","category_text":"{c}","member_id":{member},"timestamp":{ts},"quantity":{q},"price":{p},"status":"{st}","token":"{tok}","preferences":{{"notifications":true,"locale":"en-US","timezone":"UTC","delivery":"standard","visibility":"members","version":1}},"source":"synthetic-group-members","kind":"membership-record"}}"#
                        )
                    };
                    let st = ["active", "pending", "paused"];
                    let v = json(
                        1700000000 + member,
                        member % 17,
                        1000 + member % 250,
                        st[member % 3],
                        rng.text(8),
                    );
                    let u = upd.then(|| {
                        json(
                            1700086400 + urng.n(86400),
                            urng.n(17),
                            1000 + urng.n(250),
                            st[urng.n(3)],
                            urng.text(8),
                        )
                    });
                    (v, u)
                }
                'C' => {
                    const STATUS: [&str; 3] = ["active", "idle", "warm"];
                    const REGION: [&str; 4] = ["us-e1", "us-w2", "eu-w1", "ap-s1"];
                    let region = REGION[(g as usize) % REGION.len()];
                    let v = format!(
                        r#"{{"v":0,"c":{},"s":{},"st":"{}","r":"{region}"}}"#,
                        rng.n(10_000),
                        rng.n(1_000_000),
                        STATUS[member % STATUS.len()],
                    );
                    let u = upd.then(|| {
                        format!(
                            r#"{{"v":1,"c":{},"s":{},"st":"{}","r":"{region}"}}"#,
                            urng.n(10_000),
                            urng.n(1_000_000),
                            STATUS[(member + 1) % STATUS.len()],
                        )
                    });
                    (v, u)
                }
                _ => {
                    let (id, nm, em, dom) = (rng.hex(32), rng.hex(24), rng.hex(16), rng.hex(12));
                    let (free, city, acct, notes) =
                        (rng.hex(270), rng.hex(20), rng.hex(32), rng.hex(100));
                    let ts = rng.next() % 2000000000;
                    let json = |free: &str, notes: &str, ts: u64, st: &str| {
                        format!(
                            r#"{{"id":"{id}","name":"{nm}","email":"{em}@{dom}.org","free_text":"{free}","city":"{city}","account":"{acct}","notes":"{notes}","timestamp":{ts},"state":"{st}"}}"#
                        )
                    };
                    let v = json(&free, &notes, ts, "active");
                    let u = upd
                        .then(|| json(&free, &urng.hex(100), urng.next() % 2000000000, "updated"));
                    (v, u)
                }
            };
            raw += key_len + value.len();
            let (off, len) = push(&mut arena, value);
            let (upd_off, upd_len) = match update {
                Some(u) => {
                    updates.push(i);
                    push(&mut arena, u)
                }
                None => (0, 0),
            };
            if (h >> 8).is_multiple_of(50) {
                deletes.push(i);
            }
            recs.push(Rec {
                key,
                key_len: key_len as u8,
                off,
                len,
                upd_off,
                upd_len,
            });
        }
        if (64..=144).contains(&n) {
            scan_groups.push(groups.len());
        }
        groups.push((start, recs.len()));
        g += 1;
    }
    let mut order = Rng(0x5348554631 ^ name as u64);
    order.shuffle(&mut updates);
    order.shuffle(&mut deletes);
    let state = vec![ORIGINAL; recs.len()];
    Data {
        arena,
        key_len,
        recs,
        groups,
        scan_groups,
        updates,
        deletes,
        raw,
        state,
    }
}
