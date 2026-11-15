//! Data generators shared by compression layout and dictionary research.

/// Deterministic xorshift generator, as in the benchmark.
pub(super) struct Rng(u64);

impl Rng {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    pub(super) fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

const WORDS: [&str; 8] = [
    "alpha", "bravo", "charlie", "delta", "north", "south", "amber", "green",
];

fn words(rng: &mut Rng, count: u64) -> String {
    (0..count)
        .map(|_| WORDS[rng.below(8) as usize])
        .collect::<Vec<_>>()
        .join(" ")
}

/// An order document of about 1 to 6 KiB.
pub(super) fn document(index: u64) -> Vec<u8> {
    let mut rng = Rng::new(3_000 + index);
    let items = 10 + rng.below(51);
    let lines: Vec<String> = (0..items)
        .map(|_| {
            let (sku, qty, note) = (rng.below(50_000), 1 + rng.below(9), rng.below(6));
            format!(
                r#"{{"sku":"SKU-{sku:06}","qty":{qty},"note":"{}"}}"#,
                words(&mut rng, note)
            )
        })
        .collect();
    format!(
        r#"{{"order":{index},"status":"paid","items":[{}]}}"#,
        lines.join(",")
    )
    .into_bytes()
}

pub(super) mod compare {
    use super::Rng;

    const GROUP: u64 = 16_384;
    const WORDS: [&str; 32] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra",
        "tango", "uniform", "victor", "whiskey", "xray", "yankee", "zulu", "red", "green", "blue",
        "amber", "north", "south",
    ];

    fn words(rng: &mut Rng, count: u64) -> String {
        (0..count)
            .map(|_| WORDS[rng.below(32) as usize])
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub(crate) fn json(index: u64, version: u64) -> Vec<u8> {
        let mut rng = Rng::new(2_000 + index + (version << 40));
        let (first, last) = (WORDS[rng.below(32) as usize], WORDS[rng.below(32) as usize]);
        let bio_words = rng.below(120);
        format!(
            r#"{{"id":{index},"name":"{first} {last}","email":"{first}.{last}{}@example.com","active":{},"score":{},"plan":"{}","tags":["{}","{}"],"created":"2026-{:02}-{:02}T{:02}:{:02}:00Z","bio":"{}"}}"#,
            rng.below(100),
            rng.below(2) == 0,
            rng.below(100_000),
            ["free", "pro", "team"][rng.below(3) as usize],
            WORDS[rng.below(32) as usize],
            WORDS[rng.below(32) as usize],
            1 + rng.below(12),
            1 + rng.below(28),
            rng.below(24),
            rng.below(60),
            words(&mut rng, bio_words),
        )
        .into_bytes()
    }

    pub(crate) fn sensor(index: u64, version: u64) -> Vec<u8> {
        let mut rng = Rng::new(1_000 + index + (version << 40));
        let mut value = Vec::with_capacity(32);
        value.extend_from_slice(&(1_700_000_000_000 + index * 250 + version).to_be_bytes());
        value.extend_from_slice(&((index % 1_000) as u32).to_be_bytes());
        value.extend_from_slice(&[rng.below(4) as u8, 0]);
        let extra = rng.below(19);
        value.extend((0..extra).map(|_| rng.below(256) as u8));
        value
    }

    pub(crate) fn key(groups: u64, index: u64) -> [u8; 8] {
        let (group, rest) = (index / GROUP, index % GROUP);
        let (sub, rest) = (rest / 512, rest % 512);
        let (leaf, place) = (rest / 32, rest % 32);
        let head = if group == groups - 1 {
            [0xFF, 0xFF]
        } else {
            [group as u8, if group % 2 == 1 { 0xFF } else { 0x00 }]
        };
        [
            head[0],
            head[1],
            (sub / 4) as u8,
            (sub % 4 * 0x40) as u8,
            0x00,
            leaf as u8,
            place as u8,
            0xFF,
        ]
    }

    /// Records in key order, with deleted records kept as tombstones.
    pub(crate) fn history(
        groups: u64,
        value: fn(u64, u64) -> Vec<u8>,
    ) -> impl Iterator<Item = ([u8; 8], u64, Option<Vec<u8>>)> {
        let records = groups * GROUP;
        let shuffled = move |j: u64, multiplier: u64| {
            (j.wrapping_mul(multiplier).wrapping_add(0x7F4A_7C15)) & (records - 1)
        };
        let mut seqs = vec![0u64; records as usize];
        for j in 0..records {
            seqs[shuffled(j, 0x9E37_79B1) as usize] = j + 1;
        }
        let mut next = records;
        for j in 0..records {
            let index = shuffled(j, 0x85EB_CA6B);
            if matches!(index % 16, 0 | 8) {
                next += 1;
                seqs[index as usize] = next;
            }
        }
        (0..records).map(move |index| {
            let version = u64::from(index % 16 == 0);
            let live = index % 16 != 8;
            (
                key(groups, index),
                seqs[index as usize],
                live.then(|| value(index, version)),
            )
        })
    }
}
