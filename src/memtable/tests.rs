//! Memtable updates, budget accounting, freezing and flushing.
//!
//! Expected results come from plain vectors and hash maps maintained by the
//! tests, never from the structure under test.

use std::collections::HashMap;

use crate::block::{MAX_KEY_LEN, MAX_VALUE_LEN, Record, Value, Version};
use crate::memtable::{
    ENTRY_OVERHEAD, FlushCuts, FrozenMemtable, MAX_TARGET_RUN_LEN, MemoryUsage, Memtable,
    TARGET_RUN_LEN, TableError, flush_merged,
};
use crate::run::{
    Codec, LogicalWorkspace, MergeCursor, MergeWorkspace, RECORD_INPUT_OVERHEAD, ReadWorkspace,
    Run, RunBuilder, RunSet, RunView, TARGET_BLOCK_LEN,
};

type Entry = (Vec<u8>, u64, Option<Vec<u8>>);

const ROOMY: usize = 1 << 24;

fn table() -> Memtable {
    Memtable::new(ROOMY, None)
}

fn charge(key: &[u8], value: &[u8]) -> usize {
    key.len() + value.len() + ENTRY_OVERHEAD
}

fn owned(record: Record<'_>) -> Entry {
    let value = match record.value {
        Value::Bytes(bytes) => Some(bytes.to_vec()),
        Value::Tombstone => None,
    };
    (record.key.to_vec(), record.seq, value)
}

fn contents(table: &Memtable) -> Vec<Entry> {
    table.iter().map(owned).collect()
}

fn frozen_contents(table: &FrozenMemtable) -> Vec<Entry> {
    table.iter().map(owned).collect()
}

fn run_contents(run: &Run<Vec<u8>>) -> Vec<Entry> {
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    let mut out = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        out.push(owned(record));
    }
    out
}

fn put(key: &[u8], seq: u64, value: &[u8]) -> Entry {
    (key.to_vec(), seq, Some(value.to_vec()))
}

fn del(key: &[u8], seq: u64) -> Entry {
    (key.to_vec(), seq, None)
}

/// Everything a rejected update must leave untouched.
fn state(table: &Memtable) -> (Vec<Entry>, MemoryUsage, Option<u64>, usize) {
    (
        contents(table),
        table.memory(),
        table.last_seq(),
        table.len(),
    )
}

#[track_caller]
fn assert_rejected(table: &mut Memtable, expected: TableError, key: &[u8], seq: u64, value: &[u8]) {
    let before = state(table);
    assert_eq!(table.put(key, seq, value), Err(expected));
    assert_eq!(state(table), before);
}

// Updates.

#[test]
fn an_absent_key_reads_as_nothing() {
    let mut table = table();
    assert_eq!(table.get(b"a"), None);
    assert!(table.is_empty());
    table.put(b"a", 1, b"v").unwrap();
    assert_eq!(table.get(b"b"), None);
    assert_eq!(table.len(), 1);
    assert!(!table.is_empty());
}

#[test]
fn a_put_is_read_back_with_its_sequence_number() {
    let mut table = table();
    table.put(b"a", 7, b"value").unwrap();
    assert_eq!(
        table.get(b"a"),
        Some(Version {
            seq: 7,
            value: Value::Bytes(b"value")
        })
    );
}

#[test]
fn a_delete_is_read_as_a_tombstone_not_as_absence() {
    let mut table = table();
    table.put(b"a", 1, b"v").unwrap();
    table.delete(b"a", 2).unwrap();
    assert_eq!(
        table.get(b"a"),
        Some(Version {
            seq: 2,
            value: Value::Tombstone
        })
    );
    assert_eq!(table.len(), 1);
}

#[test]
fn deleting_an_absent_key_stores_a_tombstone() {
    let mut table = table();
    table.delete(b"a", 1).unwrap();
    assert_eq!(contents(&table), [del(b"a", 1)]);
}

#[test]
fn an_overwrite_keeps_only_the_latest_update() {
    let mut table = table();
    table.put(b"a", 1, b"old").unwrap();
    table.put(b"a", 2, b"new").unwrap();
    assert_eq!(contents(&table), [put(b"a", 2, b"new")]);
}

#[test]
fn a_key_can_be_reinserted_after_a_delete() {
    let mut table = table();
    table.put(b"a", 1, b"old").unwrap();
    table.delete(b"a", 2).unwrap();
    table.put(b"a", 3, b"back").unwrap();
    assert_eq!(contents(&table), [put(b"a", 3, b"back")]);
}

#[test]
fn iteration_is_bytewise_ascending_whatever_the_write_order() {
    let keys: [&[u8]; 7] = [b"", b"\x00", b"a", b"a\x00", b"ab", b"b", b"\xFF"];
    let mut table = table();
    for (seq, key) in keys.iter().rev().enumerate() {
        table.put(key, seq as u64, b"").unwrap();
    }
    let seen: Vec<Vec<u8>> = table.iter().map(|record| record.key.to_vec()).collect();
    assert_eq!(seen, keys.map(<[u8]>::to_vec));
}

#[test]
fn an_empty_key_and_binary_values_are_ordinary() {
    let binary = [0, 0xFF, b'\n', 0, 0x80];
    let mut table = table();
    table.put(b"", 1, &binary).unwrap();
    table.put(b"\x00\xFF", 2, &binary).unwrap();
    assert_eq!(table.get(b"").unwrap().value, Value::Bytes(&binary));
    assert_eq!(table.get(b"\x00\xFF").unwrap().value, Value::Bytes(&binary));
}

#[test]
fn an_empty_value_is_not_a_tombstone() {
    let mut table = table();
    table.put(b"a", 1, b"").unwrap();
    assert_eq!(table.get(b"a").unwrap().value, Value::Bytes(b""));
}

// Sequence numbers.

#[test]
fn the_first_update_may_use_sequence_number_zero() {
    let mut table = table();
    table.put(b"a", 0, b"v").unwrap();
    assert_eq!(table.last_seq(), Some(0));
    let expected = TableError::SequenceNotIncreasing { seq: 0, last: 0 };
    assert_rejected(&mut table, expected, b"b", 0, b"v");
}

#[test]
fn sequence_numbers_may_have_gaps() {
    let mut table = table();
    table.put(b"a", 1, b"v").unwrap();
    table.put(b"b", 1000, b"v").unwrap();
    assert_eq!(table.last_seq(), Some(1000));
}

#[test]
fn a_repeated_or_lower_sequence_number_is_rejected_on_any_key() {
    let mut table = table();
    table.put(b"a", 5, b"v").unwrap();
    for (key, seq) in [(&b"a"[..], 5), (b"a", 4), (b"b", 5), (b"b", 0)] {
        let expected = TableError::SequenceNotIncreasing { seq, last: 5 };
        assert_rejected(&mut table, expected, key, seq, b"new");
    }
}

#[test]
fn a_rejected_delete_keeps_the_previous_value() {
    let mut table = table();
    table.put(b"a", 5, b"v").unwrap();
    let before = state(&table);
    assert_eq!(
        table.delete(b"a", 5),
        Err(TableError::SequenceNotIncreasing { seq: 5, last: 5 })
    );
    assert_eq!(state(&table), before);
}

#[test]
fn the_starting_bound_is_exclusive() {
    let mut table = Memtable::new(ROOMY, Some(10));
    assert_eq!(table.last_seq(), Some(10));
    for seq in [9, 10] {
        let expected = TableError::SequenceNotIncreasing { seq, last: 10 };
        assert_rejected(&mut table, expected, b"a", seq, b"v");
    }
    table.put(b"a", 11, b"v").unwrap();
}

#[test]
fn the_largest_sequence_number_is_usable_and_ends_the_table() {
    let mut table = table();
    table.put(b"a", u64::MAX, b"v").unwrap();
    for seq in [u64::MAX, u64::MAX - 1, 0] {
        let expected = TableError::SequenceNotIncreasing {
            seq,
            last: u64::MAX,
        };
        assert_rejected(&mut table, expected, b"b", seq, b"v");
    }
    assert_eq!(table.last_seq(), Some(u64::MAX));
    assert_eq!(table.get(b"a").unwrap().seq, u64::MAX);
}

#[test]
fn a_table_after_the_largest_sequence_number_accepts_nothing() {
    let mut table = Memtable::new(ROOMY, Some(u64::MAX));
    let expected = TableError::SequenceNotIncreasing {
        seq: u64::MAX,
        last: u64::MAX,
    };
    assert_rejected(&mut table, expected, b"a", u64::MAX, b"v");
    assert!(table.is_empty());
}

#[test]
fn a_rejected_sequence_number_stays_unused_and_can_be_retried() {
    let key = b"k";
    let value = [1; 64];
    let mut table = Memtable::new(charge(key, &value) - 1, None);
    assert!(table.put(key, 9, &value).is_err());
    assert_eq!(table.last_seq(), None);
    table.put(key, 9, b"").unwrap();
    assert_eq!(table.last_seq(), Some(9));
}

// Limits.

#[test]
fn a_key_at_the_limit_is_accepted_and_one_byte_more_is_rejected() {
    let long = vec![b'k'; MAX_KEY_LEN + 1];
    let mut table = table();
    table.put(&long[..MAX_KEY_LEN], 1, b"v").unwrap();
    let expected = TableError::KeyTooLong {
        len: MAX_KEY_LEN + 1,
    };
    assert_rejected(&mut table, expected, &long, 2, b"v");
    let before = state(&table);
    assert_eq!(table.delete(&long, 2), Err(expected));
    assert_eq!(state(&table), before);
}

#[test]
fn an_oversized_value_is_rejected_before_it_is_copied() {
    // Never written or read, so its pages are not touched.
    let huge = vec![0; MAX_VALUE_LEN + 1];
    let mut table = table();
    let expected = TableError::ValueTooLong {
        len: MAX_VALUE_LEN + 1,
    };
    assert_rejected(&mut table, expected, b"a", 1, &huge);
}

#[test]
fn limits_are_reported_before_sequence_order_and_budget() {
    let long_key = vec![b'k'; MAX_KEY_LEN + 1];
    let huge = vec![0; MAX_VALUE_LEN + 1];
    let mut table = Memtable::new(0, Some(5));
    assert_eq!(
        table.put(&long_key, 1, &huge),
        Err(TableError::KeyTooLong {
            len: MAX_KEY_LEN + 1
        })
    );
    assert_eq!(
        table.put(b"a", 1, &huge),
        Err(TableError::ValueTooLong {
            len: MAX_VALUE_LEN + 1
        })
    );
    assert_eq!(
        table.put(b"a", 1, b"v"),
        Err(TableError::SequenceNotIncreasing { seq: 1, last: 5 })
    );
    assert_eq!(
        table.put(b"a", 6, b"v"),
        Err(TableError::RecordExceedsBudget {
            charge: charge(b"a", b"v"),
            budget: 0
        })
    );
}

// Budget.

#[test]
fn an_entry_is_charged_its_key_value_and_overhead() {
    let mut table = table();
    table.put(b"key", 1, b"value").unwrap();
    table.delete(b"gone", 2).unwrap();
    assert_eq!(
        table.memory(),
        MemoryUsage {
            payload: 3 + 5 + 4,
            arena_bytes: 3 + 5 + 4,
            charged: 3 + 5 + 4 + 2 * ENTRY_OVERHEAD,
            pinned: 0,
            budget: ROOMY
        }
    );
}

#[test]
fn a_record_that_exactly_fills_the_budget_is_admitted() {
    let mut table = Memtable::new(charge(b"k", b"value"), None);
    table.put(b"k", 1, b"value").unwrap();
    let memory = table.memory();
    assert_eq!(memory.charged, memory.budget);
}

#[test]
fn a_record_one_byte_over_the_budget_can_never_be_admitted() {
    let budget = charge(b"k", b"value") - 1;
    let mut table = Memtable::new(budget, None);
    let expected = TableError::RecordExceedsBudget {
        charge: budget + 1,
        budget,
    };
    assert_rejected(&mut table, expected, b"k", 1, b"value");
}

#[test]
fn a_new_entry_that_does_not_fit_reports_what_it_needs() {
    let first = charge(b"a", b"v");
    let second = charge(b"b", b"v");
    let mut table = Memtable::new(first + second - 1, None);
    table.put(b"a", 1, b"v").unwrap();
    let expected = TableError::BudgetExceeded {
        needed: second,
        available: second - 1,
    };
    assert_rejected(&mut table, expected, b"b", 2, b"v");
}

#[test]
fn a_new_entry_may_use_exactly_the_remaining_budget() {
    let first = charge(b"a", b"v");
    let mut table = Memtable::new(first + charge(b"b", b"v"), None);
    table.put(b"a", 1, b"v").unwrap();
    table.put(b"b", 2, b"v").unwrap();
    assert_eq!(table.memory().charged, table.memory().budget);
}

#[test]
fn replacements_charge_all_appended_bytes_including_overwritten_garbage() {
    let mut table = table();
    table.put(b"k", 1, b"abc").unwrap();
    let before = table.memory().charged;
    table.put(b"k", 2, b"abcdefg").unwrap();
    assert_eq!(table.memory().charged, before + 8);
    table.put(b"k", 3, b"a").unwrap();
    assert_eq!(table.memory().charged, before + 10);
    assert_eq!(table.memory().arena_bytes, 14);
    assert_eq!(table.memory().payload, 2);
}

#[test]
fn an_equal_sized_replacement_needs_arena_room_even_without_snapshots() {
    let mut table = Memtable::new(charge(b"k", b"abc"), None);
    table.put(b"k", 1, b"abc").unwrap();
    assert_rejected(
        &mut table,
        TableError::BudgetExceeded {
            needed: 4,
            available: 0,
        },
        b"k",
        2,
        b"xyz",
    );
    assert_eq!(table.get(b"k").unwrap().value, Value::Bytes(b"abc"));
}

#[test]
fn a_replacement_cannot_reuse_the_previous_values_arena_bytes() {
    let budget = charge(b"k", b"abc") + 1;
    let mut table = Memtable::new(budget, None);
    table.put(b"k", 1, b"abc").unwrap();
    assert_rejected(
        &mut table,
        TableError::BudgetExceeded {
            needed: 5,
            available: 1,
        },
        b"k",
        2,
        b"abcd",
    );
}

#[test]
fn a_replacement_that_grows_too_far_keeps_the_old_value() {
    let budget = charge(b"k", b"abc") + charge(b"other", b"");
    let mut table = Memtable::new(budget, None);
    table.put(b"k", 1, b"abc").unwrap();
    table.put(b"other", 2, b"").unwrap();
    let expected = TableError::BudgetExceeded {
        needed: 5,
        available: 0,
    };
    assert_rejected(&mut table, expected, b"k", 3, b"abcd");
    assert_eq!(table.get(b"k").unwrap().value, Value::Bytes(b"abc"));
}

#[test]
fn a_delete_keeps_the_old_payload_and_appends_its_key() {
    let mut table = table();
    table.put(b"k", 1, b"value").unwrap();
    table.delete(b"k", 2).unwrap();
    assert_eq!(table.memory().charged, charge(b"k", b"value") + 1);
    assert_eq!(table.memory().arena_bytes, 7);
    assert_eq!(table.memory().payload, 1);
}

#[test]
fn a_delete_needs_room_for_its_key_at_a_full_budget() {
    let mut table = Memtable::new(charge(b"k", b"v"), None);
    table.put(b"k", 1, b"v").unwrap();
    let before = state(&table);
    assert_eq!(
        table.delete(b"k", 2),
        Err(TableError::BudgetExceeded {
            needed: 1,
            available: 0
        })
    );
    assert_eq!(state(&table), before);
}

#[test]
fn another_tombstone_appends_its_key() {
    let mut table = table();
    table.delete(b"k", 1).unwrap();
    let before = table.memory();
    table.delete(b"k", 2).unwrap();
    assert_eq!(table.memory().charged, before.charged + 1);
    assert_eq!(table.memory().arena_bytes, before.arena_bytes + 1);
    assert_eq!(table.get(b"k").unwrap().seq, 2);
}

#[test]
fn a_delete_of_an_absent_key_is_subject_to_the_budget() {
    let first = charge(b"a", b"");
    let mut table = Memtable::new(first, None);
    table.put(b"a", 1, b"").unwrap();
    let before = state(&table);
    assert_eq!(
        table.delete(b"b", 2),
        Err(TableError::BudgetExceeded {
            needed: charge(b"b", b""),
            available: 0
        })
    );
    assert_eq!(state(&table), before);
}

#[test]
fn a_reinsertion_appends_its_key_and_value() {
    let mut table = table();
    table.put(b"k", 1, b"").unwrap();
    table.delete(b"k", 2).unwrap();
    let before = table.memory().charged;
    table.put(b"k", 3, b"abc").unwrap();
    assert_eq!(table.memory().charged, before + 4);
}

#[test]
fn a_zero_budget_admits_nothing() {
    let mut table = Memtable::new(0, None);
    let expected = TableError::RecordExceedsBudget {
        charge: charge(b"", b""),
        budget: 0,
    };
    assert_rejected(&mut table, expected, b"", 1, b"");
}

#[test]
fn an_extreme_budget_does_not_overflow_the_accounting() {
    let mut table = Memtable::new(usize::MAX, None);
    table.put(b"k", 1, b"abc").unwrap();
    table.put(b"k", 2, b"abcdef").unwrap();
    assert_eq!(table.memory().charged, charge(b"k", b"abc") + 7);
}

// Freezing.

fn sample() -> (Memtable, Vec<Entry>) {
    let mut table = table();
    table.put(b"b", 1, b"two").unwrap();
    table.delete(b"c", 2).unwrap();
    table.put(b"a", 3, b"one").unwrap();
    let expected = vec![put(b"a", 3, b"one"), put(b"b", 1, b"two"), del(b"c", 2)];
    (table, expected)
}

#[test]
fn freezing_preserves_contents_and_accounting() {
    let (table, expected) = sample();
    let memory = table.memory();
    let frozen = table.freeze();
    assert_eq!(frozen_contents(&frozen), expected);
    assert_eq!(frozen.memory(), memory);
    assert_eq!(frozen.last_seq(), Some(3));
    assert_eq!(frozen.len(), 3);
    assert!(!frozen.is_empty());
    assert_eq!(frozen.get(b"a").unwrap().value, Value::Bytes(b"one"));
    assert_eq!(frozen.get(b"c").unwrap().value, Value::Tombstone);
    assert_eq!(frozen.get(b"d"), None);
}

#[test]
fn a_frozen_table_traverses_the_same_way_every_time() {
    let (table, expected) = sample();
    let frozen = table.freeze();
    for _ in 0..3 {
        assert_eq!(frozen_contents(&frozen), expected);
    }
}

#[test]
fn a_frozen_table_can_be_shared_between_threads_by_type() {
    fn shareable<T: Send + Sync>() {}
    shareable::<FrozenMemtable>();
}

#[test]
fn a_successor_cannot_reuse_a_frozen_tables_sequence_numbers() {
    let (table, _) = sample();
    let frozen = table.freeze();
    let mut next = Memtable::new(ROOMY, frozen.last_seq());
    let expected = TableError::SequenceNotIncreasing { seq: 3, last: 3 };
    assert_rejected(&mut next, expected, b"z", 3, b"v");
    next.put(b"z", 4, b"v").unwrap();
}

#[test]
fn an_empty_table_passes_its_starting_bound_to_a_successor() {
    let frozen = Memtable::new(ROOMY, Some(40)).freeze();
    assert!(frozen.is_empty());
    assert_eq!(frozen.last_seq(), Some(40));
}

// Flushing.

#[test]
fn an_empty_table_flushes_to_no_runs() {
    let frozen = table().freeze();
    assert_eq!(frozen.flush(TARGET_RUN_LEN).count(), 0);
}

#[test]
fn a_flush_matches_a_run_built_directly_with_the_defaults() {
    let (table, expected) = sample();
    let frozen = table.freeze();
    let mut builder = RunBuilder::default();
    for (key, seq, value) in &expected {
        let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
        builder.add(key, *seq, value).unwrap();
    }
    let runs: Vec<_> = frozen.flush(TARGET_RUN_LEN).collect();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].as_bytes(), builder.finish().as_bytes());
}

#[test]
fn flushed_runs_pass_full_validation_and_equal_the_table() {
    let mut table = table();
    for i in 0..2000u32 {
        let key = format!("key{:05}", i * 7 % 2000);
        table
            .put(key.as_bytes(), u64::from(i), &[i as u8; 40])
            .unwrap();
    }
    let frozen = table.freeze();
    let runs: Vec<_> = frozen.flush(TARGET_RUN_LEN).collect();
    let mut flushed = Vec::new();
    for run in &runs {
        Run::new(run.as_bytes().to_vec()).unwrap();
        flushed.extend(run_contents(run));
    }
    assert_eq!(flushed, frozen_contents(&frozen));
}

#[test]
fn every_tombstone_is_flushed_with_its_sequence_number() {
    let mut table = table();
    table.delete(b"never-held", 4).unwrap();
    table.put(b"replaced", 5, b"v").unwrap();
    table.delete(b"replaced", 6).unwrap();
    let frozen = table.freeze();
    let runs: Vec<_> = frozen.flush(TARGET_RUN_LEN).collect();
    assert_eq!(
        run_contents(&runs[0]),
        [del(b"never-held", 4), del(b"replaced", 6)]
    );
}

fn numbered(count: u32, value_len: usize) -> FrozenMemtable {
    let mut table = table();
    for i in 0..count {
        let key = format!("key{i:05}");
        table
            .put(key.as_bytes(), u64::from(i), &vec![i as u8; value_len])
            .unwrap();
    }
    table.freeze()
}

#[test]
fn a_flush_splits_at_the_target_into_ordered_disjoint_runs() {
    // Each record counts 8 + 100 + 16 = 124 bytes of input, so ten records
    // reach the target and the 95 records make nine runs of ten and one of five.
    let frozen = numbered(95, 100);
    let runs: Vec<_> = frozen.flush(1240).collect();
    let sizes: Vec<usize> = runs.iter().map(Run::len).collect();
    assert_eq!(sizes, [10, 10, 10, 10, 10, 10, 10, 10, 10, 5]);
    let mut flushed = Vec::new();
    for pair in runs.windows(2) {
        let first = pair[0].key_range().unwrap();
        let second = pair[1].key_range().unwrap();
        assert!(first.max < second.min);
    }
    for run in &runs {
        flushed.extend(run_contents(run));
    }
    assert_eq!(flushed, frozen_contents(&frozen));
}

#[test]
fn a_run_ends_only_when_the_target_is_reached() {
    let frozen = numbered(20, 100);
    let sizes: Vec<usize> = frozen.flush(1241).map(|run| run.len()).collect();
    assert_eq!(sizes, [11, 9]);
}

#[test]
fn a_zero_target_makes_one_run_per_record() {
    let frozen = numbered(5, 8);
    assert_eq!(
        frozen.flush(0).map(|run| run.len()).collect::<Vec<_>>(),
        [1; 5]
    );
}

#[test]
fn an_oversized_target_is_reduced_and_still_makes_one_run() {
    let frozen = numbered(5, 8);
    assert_eq!(frozen.flush(usize::MAX).count(), 1);
    assert_eq!(frozen.flush(MAX_TARGET_RUN_LEN).count(), 1);
    let limit = format!("target: {MAX_TARGET_RUN_LEN}");
    assert!(format!("{:?}", frozen.flush(usize::MAX)).contains(&limit));
}

#[test]
fn a_finished_flush_stays_finished() {
    let frozen = numbered(3, 8);
    let mut flush = frozen.flush(TARGET_RUN_LEN);
    assert!(flush.next().is_some());
    assert!(flush.next().is_none());
    assert!(flush.next().is_none());
}

#[test]
fn an_abandoned_flush_leaves_the_table_intact_for_an_identical_retry() {
    let frozen = numbered(200, 64);
    let expected = frozen_contents(&frozen);
    let memory = frozen.memory();

    // A consumer that cannot use the second run gives up on the flush.
    let kept = {
        let mut failed = frozen.flush(2000);
        let kept = failed.next().unwrap();
        failed.next().unwrap();
        kept
    };
    assert_eq!(frozen_contents(&frozen), expected);
    assert_eq!(frozen.memory(), memory);

    let retried: Vec<_> = frozen.flush(2000).collect();
    assert_eq!(retried[0].as_bytes(), kept.as_bytes());
    let flushed: Vec<Entry> = retried.iter().flat_map(run_contents).collect();
    assert_eq!(flushed, expected);
}

#[test]
fn a_table_can_be_read_while_a_flush_is_in_progress() {
    let frozen = numbered(50, 32);
    let mut flush = frozen.flush(600);
    flush.next().unwrap();
    assert_eq!(frozen.get(b"key00049").unwrap().seq, 49);
    assert_eq!(frozen.iter().count(), 50);
    assert!(flush.next().is_some());
}

// Composition with existing runs.

fn older_run(records: &[Entry]) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::default();
    for (key, seq, value) in records {
        let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
        builder.add(key, *seq, value).unwrap();
    }
    builder.finish()
}

fn views(runs: &[Run<Vec<u8>>]) -> Vec<RunView<'_>> {
    runs.iter().map(Run::view).collect()
}

fn live(runs: &[Run<Vec<u8>>]) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
    let views = views(runs);
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&views).cursor(&mut workspace);
    let mut out = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        out.push((record.key.to_vec(), record.seq, record.value.to_vec()));
    }
    out
}

#[test]
fn flushed_updates_supersede_older_runs_in_logical_reads() {
    let older = older_run(&[
        put(b"changed", 1, b"old"),
        put(b"deleted", 2, b"old"),
        put(b"kept", 3, b"old"),
    ]);
    let mut table = Memtable::new(ROOMY, Some(3));
    table.put(b"changed", 4, b"new").unwrap();
    table.delete(b"deleted", 5).unwrap();
    table.put(b"fresh", 6, b"").unwrap();

    let frozen = table.freeze();
    let mut runs = vec![older];
    runs.extend(frozen.flush(TARGET_RUN_LEN));
    assert_eq!(
        live(&runs),
        [
            (b"changed".to_vec(), 4, b"new".to_vec()),
            (b"fresh".to_vec(), 6, b"".to_vec()),
            (b"kept".to_vec(), 3, b"old".to_vec()),
        ]
    );
    let views = views(&runs);
    let mut workspace = LogicalWorkspace::new();
    assert_eq!(
        RunSet::new(&views).get(b"deleted", &mut workspace),
        Ok(None)
    );
}

#[test]
fn a_flushed_run_merges_physically_in_internal_key_order() {
    let older = older_run(&[put(b"a", 1, b"old"), put(b"b", 2, b"old")]);
    let mut table = Memtable::new(ROOMY, Some(2));
    table.delete(b"a", 3).unwrap();
    table.put(b"c", 4, b"new").unwrap();
    let frozen = table.freeze();
    let mut runs: Vec<_> = frozen.flush(TARGET_RUN_LEN).collect();
    runs.push(older);

    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
    let mut merged = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        merged.push(owned(record));
    }
    assert_eq!(
        merged,
        [
            del(b"a", 3),
            put(b"a", 1, b"old"),
            put(b"b", 2, b"old"),
            put(b"c", 4, b"new"),
        ]
    );
}

#[test]
fn flushing_twice_yields_copies_that_logical_reads_count_once() {
    let frozen = numbered(30, 16);
    let mut runs: Vec<_> = frozen.flush(TARGET_RUN_LEN).collect();
    runs.extend(frozen.flush(300));
    assert_eq!(live(&runs).len(), 30);
}

// Reference model.

use crate::test_support::Rng;

type Model = HashMap<Vec<u8>, (u64, Option<Vec<u8>>)>;

fn key_pool() -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![0],
        vec![0xFF],
        vec![0xFF, 0xFF],
        b"a".to_vec(),
        b"a\x00".to_vec(),
    ];
    keys.extend((0..40u8).map(|i| vec![b'k', i, 0xFF - i]));
    keys
}

fn entry_charge(key: &[u8], value: Option<&[u8]>) -> usize {
    key.len() + value.map_or(0, <[u8]>::len) + ENTRY_OVERHEAD
}

fn total_charge(model: &Model) -> usize {
    model
        .iter()
        .map(|(key, (_, value))| entry_charge(key, value.as_deref()))
        .sum()
}

/// What the rules say an update should do, from the model alone.
fn expected_outcome(
    model: &Model,
    key: &[u8],
    seq: u64,
    value: Option<&[u8]>,
    last: Option<u64>,
    budget: usize,
    arena_bytes: usize,
) -> Result<(), &'static str> {
    if key.len() > MAX_KEY_LEN {
        return Err("key");
    }
    if value.is_some_and(|value| value.len() > MAX_VALUE_LEN) {
        return Err("value");
    }
    if last.is_some_and(|last| seq <= last) {
        return Err("sequence");
    }
    let entry = entry_charge(key, value);
    if entry > budget {
        return Err("record");
    }
    let appended = key.len() + value.map_or(0, <[u8]>::len);
    if arena_bytes
        + appended
        + (model.len() + usize::from(!model.contains_key(key))) * ENTRY_OVERHEAD
        > budget
    {
        return Err("budget");
    }
    Ok(())
}

fn kind(error: TableError) -> &'static str {
    match error {
        TableError::KeyTooLong { .. } => "key",
        TableError::ValueTooLong { .. } => "value",
        TableError::SequenceNotIncreasing { .. } => "sequence",
        TableError::RecordExceedsBudget { .. } => "record",
        TableError::BudgetExceeded { .. } => "budget",
    }
}

fn sorted_entries(model: &Model) -> Vec<Entry> {
    let mut entries: Vec<Entry> = model
        .iter()
        .map(|(key, (seq, value))| (key.clone(), *seq, value.clone()))
        .collect();
    entries.sort();
    entries
}

fn assert_table_matches(table: &Memtable, model: &Model, pool: &[Vec<u8>], arena_bytes: usize) {
    assert_eq!(contents(table), sorted_entries(model));
    for key in pool {
        let found = table.get(key).map(|version| {
            let value = match version.value {
                Value::Bytes(bytes) => Some(bytes.to_vec()),
                Value::Tombstone => None,
            };
            (version.seq, value)
        });
        assert_eq!(found.as_ref(), model.get(key));
    }
    let memory = table.memory();
    assert_eq!(memory.arena_bytes, arena_bytes);
    assert_eq!(memory.charged, arena_bytes + model.len() * ENTRY_OVERHEAD);
    assert_eq!(
        memory.payload,
        total_charge(model) - model.len() * ENTRY_OVERHEAD
    );
    assert!(memory.charged <= memory.budget);
}

fn assert_runs_match(runs: &[Run<Vec<u8>>], history: &Model, pool: &[Vec<u8>]) {
    let mut expected: Vec<(Vec<u8>, u64, Vec<u8>)> = history
        .iter()
        .filter_map(|(key, (seq, value))| Some((key.clone(), *seq, value.clone()?)))
        .collect();
    expected.sort();
    assert_eq!(live(runs), expected);

    let views = views(runs);
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    for key in pool {
        let found = set
            .get(key, &mut workspace)
            .unwrap()
            .map(|live| (live.seq, live.value.to_vec()));
        let wanted = history
            .get(key)
            .and_then(|(seq, value)| Some((*seq, value.clone()?)));
        assert_eq!(found, wanted);
    }
}

/// Several generations of updates, each frozen and flushed, checked after
/// every operation against maps maintained only by this test.
#[test]
fn randomized_updates_match_an_independent_model_across_flushes() {
    const SEEDS: u64 = 48;
    const GENERATIONS: usize = 4;
    const OPERATIONS: usize = 250;

    let pool = key_pool();
    let long_key = vec![b'x'; MAX_KEY_LEN + 1];
    let huge = vec![0u8; MAX_VALUE_LEN + 1];
    let mut rejected = HashMap::new();
    let mut accepted = 0;

    for seed in 0..SEEDS {
        let mut rng = Rng(seed);
        let mut history = Model::new();
        let mut runs: Vec<Run<Vec<u8>>> = Vec::new();
        let mut last = None;
        let mut next_seq = 0u64;

        for generation in 0..GENERATIONS {
            let budget = [600, 2_000, 8_000, 40_000][rng.below(4)];
            let mut table = Memtable::new(budget, last);
            let mut model = Model::new();
            let mut arena_bytes = 0;

            for _ in 0..OPERATIONS {
                let roll = rng.below(100);
                let key: &[u8] = if roll == 0 {
                    &long_key
                } else {
                    &pool[rng.below(pool.len())]
                };
                let value_bytes: Vec<u8>;
                let value: Option<&[u8]> = if rng.below(4) == 0 {
                    None
                } else if roll == 1 {
                    Some(&huge)
                } else {
                    let len = if rng.below(10) == 0 {
                        rng.below(1500)
                    } else {
                        rng.below(32)
                    };
                    value_bytes = (0..len).map(|_| rng.next() as u8).collect();
                    Some(&value_bytes)
                };
                let seq = match last {
                    Some(last) if roll < 12 => last.saturating_sub(rng.below(3) as u64),
                    _ => {
                        next_seq += 1 + rng.below(3) as u64;
                        next_seq
                    }
                };

                let want = expected_outcome(&model, key, seq, value, last, budget, arena_bytes);
                let got = match value {
                    Some(value) => table.put(key, seq, value),
                    None => table.delete(key, seq),
                };
                assert_eq!(got.map_err(kind), want, "seed {seed}");

                match want {
                    Ok(()) => {
                        accepted += 1;
                        arena_bytes += key.len() + value.map_or(0, <[u8]>::len);
                        let update = (seq, value.map(<[u8]>::to_vec));
                        model.insert(key.to_vec(), update.clone());
                        history.insert(key.to_vec(), update);
                        last = Some(seq);
                    }
                    Err(reason) => *rejected.entry(reason).or_insert(0) += 1,
                }
                assert_eq!(table.last_seq(), last);
                assert_table_matches(&table, &model, &pool, arena_bytes);
            }

            let frozen = table.freeze();
            assert_eq!(frozen_contents(&frozen), sorted_entries(&model));
            let target = [1, 256, 4096, TARGET_RUN_LEN][generation];
            let flushed: Vec<_> = frozen.flush(target).collect();
            let mut in_runs = Vec::new();
            for run in &flushed {
                Run::new(run.as_bytes().to_vec()).unwrap();
                in_runs.extend(run_contents(run));
            }
            assert_eq!(in_runs, sorted_entries(&model));
            runs.extend(flushed);
            assert_runs_match(&runs, &history, &pool);
        }
    }

    // The sequence must have exercised every outcome to mean anything.
    assert!(accepted > 10_000, "{accepted}");
    for reason in ["key", "value", "sequence", "record", "budget"] {
        assert!(rejected.get(reason).copied().unwrap_or(0) > 0, "{reason}");
    }
}

fn entries_from(table: &FrozenMemtable, start: &[u8]) -> Vec<Entry> {
    table.iter_from(start).map(owned).collect()
}

/// Snapshots of a table under random writes, across enough keys for the tree
/// to split several levels deep. Each snapshot must keep exactly the model it
/// was taken with, whatever is written, rejected or dropped after it, and the
/// pinned charge must stay within the budget and vanish with the snapshots.
#[test]
fn snapshots_keep_their_state_through_random_writes() {
    const SEEDS: u64 = 12;
    const OPERATIONS: usize = 6_000;

    let pool: Vec<Vec<u8>> = (0..3_000u32)
        .map(|i| {
            (i.wrapping_mul(2_654_435_761) % 100_000)
                .to_be_bytes()
                .to_vec()
        })
        .chain([Vec::new(), vec![0xFF; 5]])
        .collect();
    let (mut taken, mut refused) = (0, 0);
    for seed in 0..SEEDS {
        let mut rng = Rng(seed);
        let budget = [1 << 24, 400_000][seed as usize % 2];
        let mut table = Memtable::new(budget, None);
        let mut model = Model::new();
        let mut arena_bytes = 0;
        let mut held: Vec<(FrozenMemtable, Vec<Entry>)> = Vec::new();
        let mut seq = 0;

        for op in 0..OPERATIONS {
            match rng.below(100) {
                0..=1 if held.len() < 8 => {
                    held.push((table.snapshot(), sorted_entries(&model)));
                    taken += 1;
                }
                2..=3 if !held.is_empty() => {
                    let (snapshot, then) = held.swap_remove(rng.below(held.len()));
                    assert_eq!(frozen_contents(&snapshot), then, "seed {seed}");
                }
                _ => {
                    let key = &pool[rng.below(pool.len())];
                    let value: Option<Vec<u8>> = (rng.below(5) != 0)
                        .then(|| (0..rng.below(60)).map(|_| rng.next() as u8).collect());
                    seq += 1;
                    // Contents are compared with the model at checkpoints.
                    let before = (table.memory(), table.len(), table.last_seq());
                    let result = match &value {
                        Some(value) => table.put(key, seq, value),
                        None => table.delete(key, seq),
                    };
                    match result {
                        Ok(()) => {
                            arena_bytes += key.len() + value.as_ref().map_or(0, Vec::len);
                            model.insert(key.clone(), (seq, value));
                        }
                        Err(TableError::BudgetExceeded { .. }) => {
                            let after = (table.memory(), table.len(), table.last_seq());
                            assert_eq!(after, before, "seed {seed}");
                            refused += 1;
                        }
                        Err(error) => panic!("seed {seed}: {error}"),
                    }
                }
            }
            let memory = table.memory();
            assert!(memory.charged <= memory.budget, "seed {seed}");
            assert_eq!(memory.arena_bytes, arena_bytes);
            assert_eq!(
                memory.charged,
                arena_bytes + model.len() * ENTRY_OVERHEAD + memory.pinned
            );
            if held.is_empty() {
                assert_eq!(memory.pinned, 0, "seed {seed}");
            }
            if op % 250 == 0 {
                assert_eq!(contents(&table), sorted_entries(&model), "seed {seed}");
                for (snapshot, then) in &held {
                    assert_eq!(&frozen_contents(snapshot), then, "seed {seed}");
                    let start = &pool[rng.below(pool.len())];
                    let from: Vec<Entry> = then.iter().filter(|e| e.0 >= *start).cloned().collect();
                    assert_eq!(entries_from(snapshot, start), from, "seed {seed}");
                    let probe = &pool[rng.below(pool.len())];
                    let found = snapshot.get(probe).map(|version| version.seq);
                    let wanted = then.iter().find(|e| &e.0 == probe).map(|e| e.1);
                    assert_eq!(found, wanted, "seed {seed}");
                }
            }
        }
        let final_entries = sorted_entries(&model);
        for (snapshot, then) in &held {
            assert_eq!(&frozen_contents(snapshot), then, "seed {seed}");
        }
        drop(held);
        assert_eq!(table.memory().pinned, 0);
        assert_eq!(
            table.memory().charged,
            arena_bytes + model.len() * ENTRY_OVERHEAD
        );
        let frozen = table.freeze();
        assert_eq!(frozen_contents(&frozen), final_entries);
    }
    assert!(taken > 200, "{taken}");
    assert!(refused > 0, "{refused}");
}

/// A snapshot of an untouched table pins nothing, an overwrite pins its
/// leaf and old value once, and a write the budget refuses pins nothing.
#[test]
fn a_snapshot_pins_copied_nodes_and_the_partial_arena_chunk() {
    let mut table = Memtable::new(4_000, None);
    table.put(b"a", 1, &[1; 100]).unwrap();
    table.put(b"b", 2, &[1; 100]).unwrap();
    let charged = table.memory().charged;
    let snapshot = table.snapshot();
    assert_eq!(table.memory().pinned, 0);
    assert_eq!(snapshot.memory().charged, charged);

    table.put(b"a", 3, &[2; 100]).unwrap();
    let once = table.memory().pinned;
    assert!(once > 100);
    // The leaf and partial chunk are now private; later appends pin no more.
    table.put(b"b", 4, &[2; 100]).unwrap();
    let twice = table.memory().pinned;
    assert_eq!(twice, once);
    // Overwriting a value no snapshot shares pins nothing more.
    table.put(b"b", 5, &[3; 100]).unwrap();
    assert_eq!(table.memory().pinned, twice);

    let before = state(&table);
    let large = vec![0; 4_000 - table.memory().charged];
    assert!(matches!(
        table.put(b"c", 6, &large),
        Err(TableError::BudgetExceeded { .. })
    ));
    assert_eq!(state(&table), before);
    assert_eq!(
        frozen_contents(&snapshot),
        vec![put(b"a", 1, &[1; 100]), put(b"b", 2, &[1; 100])]
    );
    drop(snapshot);
    assert_eq!(table.memory().pinned, 0);
    assert_eq!(table.memory().charged, charged + 3 * 101);
}

// Flush splitting.

/// Flushes `tables` with `cuts` and returns each run's records, checking
/// that together they are the merged tables' newest records in order.
fn split_flush(tables: &[&FrozenMemtable], target: usize, cuts: &FlushCuts) -> Vec<Vec<Entry>> {
    let runs = flush_merged(
        tables,
        target,
        Codec::None,
        TARGET_BLOCK_LEN,
        cuts,
        &crate::chunk::ChunkPool::new(0),
    );
    let pieces: Vec<Vec<Entry>> = runs.iter().map(run_contents).collect();
    let mut newest: std::collections::BTreeMap<Vec<u8>, Entry> = Default::default();
    for table in tables {
        for entry in frozen_contents(table) {
            newest.insert(entry.0.clone(), entry);
        }
    }
    let flat: Vec<Entry> = pieces.iter().flatten().cloned().collect();
    assert_eq!(
        flat,
        newest.into_values().collect::<Vec<_>>(),
        "nothing lost or doubled"
    );
    assert!(pieces.iter().all(|piece| !piece.is_empty()), "no empty run");
    pieces
}

fn cuts_at(keys: &[&[u8]], min_piece: usize, max_pieces: usize) -> FlushCuts {
    FlushCuts {
        at: keys.iter().map(|key| key.to_vec()).collect(),
        min_piece,
        max_pieces,
    }
}

fn counts(pieces: &[Vec<Entry>]) -> Vec<usize> {
    pieces.iter().map(Vec::len).collect()
}

/// Pieces end before the first key at or above a cut, only while the piece
/// and the rest of the flush both hold the least piece size, and a run
/// length of input is cut into at most `max_pieces` runs.
#[test]
fn a_flush_is_cut_at_given_keys_into_bounded_pieces() {
    let frozen = numbered(100, 100);
    let record = 8 + 100 + RECORD_INPUT_OVERHEAD;
    let tens: Vec<String> = (1..10).map(|i| format!("key{:05}", i * 10)).collect();
    let tens: Vec<&[u8]> = tens.iter().map(|key| key.as_bytes()).collect();
    let big = 1 << 20;
    let pieces = |min: usize, max: usize, target: usize| {
        counts(&split_flush(&[&frozen], target, &cuts_at(&tens, min, max)))
    };
    assert_eq!(pieces(1, 100, big), [10; 10]);
    // At most eight pieces: the rest of the cuts are passed over.
    assert_eq!(pieces(1, 8, big), [10, 10, 10, 10, 10, 10, 10, 30]);
    // A piece of at least 15 records: every other cut.
    assert_eq!(pieces(15 * record, 8, big), [20, 20, 20, 20, 20]);
    // ... and 25 after it: no cut leaves fewer at the end.
    assert_eq!(pieces(25 * record, 8, big), [30, 30, 40]);
    // Each run length of input starts its own count of pieces.
    assert_eq!(pieces(1, 2, 50 * record), [10, 40, 10, 40]);
    // Cuts between keys that are not in the table, before and after them.
    let around: [&[u8]; 3] = [b"a", b"key00042x", b"zz"];
    let pieces = split_flush(&[&frozen], big, &cuts_at(&around, 1, 8));
    assert_eq!(counts(&pieces), [43, 57]);
}

/// Binary keys: the empty key, keys ending in 0xFF and a cut equal to a key
/// all split where byte order says, and a record longer than a run length
/// is a run of its own between cuts.
#[test]
fn flush_cuts_follow_byte_order_and_keep_large_records_whole() {
    let mut table = table();
    let keys: [&[u8]; 6] = [b"", b"a", b"a\xff", b"a\xff\xff", b"b", b"\xff"];
    for (seq, key) in keys.iter().enumerate() {
        table.put(key, seq as u64 + 1, b"v").unwrap();
    }
    let frozen = table.freeze();
    let cuts = cuts_at(&[b"a\xff", b"a\xff\x00", b"\xff"], 1, 8);
    let pieces = split_flush(&[&frozen], 1 << 20, &cuts);
    let firsts: Vec<&[u8]> = pieces.iter().map(|piece| piece[0].0.as_slice()).collect();
    assert_eq!(firsts, [&b""[..], b"a\xff", b"a\xff\xff", b"\xff"]);

    let mut large = self::table();
    large.put(b"a", 1, b"small").unwrap();
    large.put(b"m", 2, &[7; 3_000]).unwrap();
    large.put(b"z", 3, b"small").unwrap();
    let frozen = large.freeze();
    let pieces = split_flush(&[&frozen], 1_000, &cuts_at(&[b"b", b"n"], 1, 8));
    assert_eq!(counts(&pieces), [1, 1, 1]);
}

/// Tables merged in one flush, sharing keys and tombstones: the newest
/// update of each key is written once, wherever the cuts fall.
#[test]
fn split_flushes_of_several_tables_keep_each_newest_update_once() {
    let mut older = table();
    for i in 0..40u32 {
        older
            .put(format!("k{i:02}").as_bytes(), u64::from(i) + 1, b"old")
            .unwrap();
    }
    let older = older.freeze();
    let mut newer = Memtable::new(ROOMY, Some(40));
    for i in (0..40u32).step_by(3) {
        let key = format!("k{i:02}");
        if i % 2 == 0 {
            newer.delete(key.as_bytes(), u64::from(i) + 100).unwrap();
        } else {
            newer
                .put(key.as_bytes(), u64::from(i) + 100, b"new")
                .unwrap();
        }
    }
    let newer = newer.freeze();
    let cuts = cuts_at(&[b"k05", b"k12", b"k12", b"k30"], 1, 8);
    let pieces = split_flush(&[&older, &newer], 1 << 20, &cuts);
    let firsts: Vec<&[u8]> = pieces.iter().map(|piece| piece[0].0.as_slice()).collect();
    assert_eq!(firsts, [&b"k00"[..], b"k05", b"k12", b"k30"]);
    let k12 = pieces[2].iter().find(|entry| entry.0 == b"k12").unwrap();
    assert_eq!(k12, &del(b"k12", 112));
}

#[test]
fn arena_overwrites_deletes_and_large_records_survive_the_last_owning_snapshot() {
    use crate::chunk::{CHUNK_BYTES, ChunkPool};
    let pool = ChunkPool::new(3 * CHUNK_BYTES);
    let mut table = Memtable::in_pool(8 * CHUNK_BYTES, None, &pool);
    let large = vec![9; CHUNK_BYTES + 13];
    table.put(b"large", 1, &large).unwrap();
    table.put(b"small", 2, b"old").unwrap();
    let snapshot = table.snapshot();
    table.put(b"small", 3, b"new").unwrap();
    table.delete(b"large", 4).unwrap();
    let used = 5 + large.len() + 8 + 8 + 5;
    let memory = table.memory();
    assert_eq!(memory.arena_bytes, used);
    assert_eq!(memory.charged, used + 2 * ENTRY_OVERHEAD + memory.pinned);
    assert_eq!(memory.payload, 5 + 8);
    assert_eq!(pool.stats().1, 2 * CHUNK_BYTES);
    assert_eq!(table.get(b"large").unwrap().value, Value::Tombstone);
    let frozen = table.freeze();
    drop(frozen);
    assert_eq!(pool.stats().1, CHUNK_BYTES);
    assert_eq!(snapshot.get(b"large").unwrap().value, Value::Bytes(&large));
    assert_eq!(snapshot.get(b"small").unwrap().value, Value::Bytes(b"old"));
    drop(snapshot);
    assert_eq!(pool.stats().1, 0);
    assert_eq!(pool.stats().0, 2 * CHUNK_BYTES);
}
