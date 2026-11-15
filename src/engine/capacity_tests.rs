//! Range-local replacement, sharing and retention correctness.

use super::*;

const RUN_LEN: usize = 64 << 10;

fn engine_options() -> EngineOptions {
    EngineOptions {
        memtable_budget: RUN_LEN,
        target_run_len: RUN_LEN,

        background_compaction: None,
        ..EngineOptions::default()
    }
}

/// The defaults with their byte limits scaled as the runs are.
pub(super) fn compaction_options() -> CompactionPolicy {
    CompactionPolicy {
        max_input_bytes: 4 * RUN_LEN,

        ..CompactionPolicy::default()
    }
}

fn key(index: u64) -> Vec<u8> {
    format!("user/{index:010}").into_bytes()
}

/// [`value`], shared with the compaction correctness tests.
pub(super) fn value_for(seed: u64) -> Vec<u8> {
    value(seed)
}

/// A 100-byte value from a small vocabulary, so that it compresses about
/// as text does.
fn value(seed: u64) -> Vec<u8> {
    const WORDS: [&[u8]; 8] = [
        b"alpha ",
        b"bravo ",
        b"charlie ",
        b"delta ",
        b"echo ",
        b"foxtrot ",
        b"golf ",
        b"hotel ",
    ];
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(100);
    while out.len() < 100 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(WORDS[(state % 8) as usize]);
    }
    out.truncate(100);
    out
}

pub(super) struct Rng(pub(super) u64);

impl Rng {
    pub(super) fn below(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) % bound
    }
}

/// Prototype (5AR): a range-local compaction over the production job. The
/// runs a small update overlaps are found by binary search over the sorted
/// disjoint runs, merged with it, and replaced; every other run stays the
/// same shared run, a snapshot keeps reading what it saw, and the replaced
/// runs are retained only while it lives.
#[test]
fn a_range_local_replacement_leaves_other_runs_shared() {
    let engine = Engine::new(engine_options()).unwrap();
    let mut workspace = EngineWorkspace::new();
    for index in 0..3_000 {
        engine.put(&key(index), &value(index)).unwrap();
    }
    engine.flush().unwrap();
    let disjoint = engine.core.read_state().runs.len();
    assert!(disjoint >= 5, "{disjoint} runs");
    let snapshot = engine.snapshot();
    for index in 1_500..1_520 {
        engine.put(&key(index), b"updated").unwrap();
    }
    engine.flush().unwrap();

    // Selection: the update's run, and the older runs its range meets.
    let (picks, before, update_bytes) = {
        let state = engine.core.read_state();
        let range = |run: &SharedRun| {
            let range = run.run.key_range().unwrap();
            (range.min.to_vec(), range.max.to_vec())
        };
        let newest = (0..state.runs.len())
            .max_by_key(|&at| state.runs[at].run.max_seq())
            .unwrap();
        let (low, high) = range(&state.runs[newest]);
        let mut older: Vec<usize> = (0..state.runs.len()).filter(|&at| at != newest).collect();
        older.sort_by_key(|&at| range(&state.runs[at]).0);
        let first = older.partition_point(|&at| range(&state.runs[at]).1 < low);
        let mut picks = vec![newest];
        picks.extend(
            older[first..]
                .iter()
                .take_while(|&&at| range(&state.runs[at]).0 <= high),
        );
        let update_bytes = state.runs[newest].run.as_bytes().len();
        (picks, state.runs.clone(), update_bytes)
    };
    assert!(
        picks.len() < before.len() - 1,
        "{} of {}",
        picks.len(),
        before.len()
    );

    let report = engine
        .compact_picked(
            compaction_options(),
            picks.clone(),
            &mut MergeWorkspace::new(),
        )
        .unwrap();
    let after = engine.core.read_state().runs.clone();
    for (at, run) in before.iter().enumerate() {
        let kept = after.iter().any(|other| Arc::ptr_eq(other, run));
        assert_eq!(kept, !picks.contains(&at), "run {at}");
    }
    assert_eq!(after.len(), before.len() - picks.len() + report.outputs);

    let read = |found: Option<LiveValue<'_>>| found.map(|live| live.value.to_vec());
    assert_eq!(
        read(engine.get(&key(1_510), &mut workspace).unwrap()),
        Some(b"updated".to_vec())
    );
    assert_eq!(
        read(snapshot.get(&key(1_510), &mut workspace).unwrap()),
        Some(value(1_510))
    );
    assert!(engine.stats().retained_run_bytes >= report.input_bytes);
    // The test's own references to the replaced runs count as a reader's.
    // Without them, the snapshot holds the older inputs; the update's run
    // was flushed after it, from a memtable it holds instead.
    drop((before, after));
    assert_eq!(
        engine.stats().retained_run_bytes,
        report.input_bytes - update_bytes
    );
    drop(snapshot);
    assert_eq!(engine.stats().retained_run_bytes, 0);
}
