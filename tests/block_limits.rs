//! Boundary tests that must touch maximum-sized values and blocks.
//!
//! Each test writes about 130 MiB and reserves more, so they are ignored by a
//! plain `cargo test`. Run them one at a time under explicit limits with
//! `scripts/validate.sh limits`.

use tessera::block::{
    Block, BlockBuilder, BuildError, MAX_BLOCK_LEN, MAX_KEY_LEN, MAX_VALUE_LEN, RESTART_INTERVAL,
    Value,
};
use tessera::run::{MAX_TARGET_BLOCK_LEN, Run, RunBuilder};

#[test]
#[ignore = "large allocations; run with scripts/validate.sh limits"]
fn largest_allowed_record_round_trips() {
    let key = vec![b'k'; MAX_KEY_LEN];
    let value = vec![7; MAX_VALUE_LEN];
    let mut builder = BlockBuilder::new();
    builder.add(&key, u64::MAX, Value::Bytes(&value)).unwrap();

    let block = Block::new(builder.finish()).unwrap();
    assert_eq!(block.cursor().next_record().unwrap().key, key.as_slice());
    assert_eq!(block.get(&key).unwrap().value, Value::Bytes(&value));
}

#[test]
#[ignore = "large allocations; run with scripts/validate.sh limits"]
fn full_builder_rejects_the_record_and_can_still_finish() {
    let value = vec![0; MAX_VALUE_LEN];
    let mut builder = BlockBuilder::new();
    builder.add(b"a", 1, Value::Bytes(&value)).unwrap();
    // A second full value needs slightly more than the remaining half.
    let before = builder.encoded_len();
    assert_eq!(
        builder.add(b"b", 1, Value::Bytes(&value)),
        Err(BuildError::BlockFull)
    );
    assert_eq!(builder.encoded_len(), before);

    builder.add(b"b", 1, Value::Bytes(b"small")).unwrap();
    let bytes = builder.finish();
    assert!(bytes.len() <= MAX_BLOCK_LEN);
    assert_eq!(Block::new(bytes).unwrap().len(), 2);
}

/// The final record lands exactly on the limit, both when it extends the
/// current restart interval and when it needs a new directory entry.
#[test]
#[ignore = "large allocations; run with scripts/validate.sh limits"]
fn block_may_fill_exactly_to_the_length_limit() {
    for (small_records, directory_growth) in [(0, 0), (RESTART_INTERVAL - 1, 4)] {
        let mut builder = BlockBuilder::new();
        let value = vec![0; MAX_VALUE_LEN];
        builder.add(b"a", 1, Value::Bytes(&value)).unwrap();
        for i in 0..small_records {
            let key = format!("a{i:02}");
            builder.add(key.as_bytes(), 1, Value::Tombstone).unwrap();
        }

        // Last record overhead: shared, suffix_len, 4-byte tag, seq, 1-byte key.
        let remaining = MAX_BLOCK_LEN - builder.encoded_len();
        let filler_len = remaining - (1 + 1 + 4 + 1 + 1) - directory_growth;
        let filler = vec![0; filler_len + 1];
        assert_eq!(
            builder.add(b"b", 1, Value::Bytes(&filler)),
            Err(BuildError::BlockFull)
        );
        builder
            .add(b"b", 1, Value::Bytes(&filler[..filler_len]))
            .unwrap();
        assert_eq!(builder.encoded_len(), MAX_BLOCK_LEN);

        let bytes = builder.finish();
        assert_eq!(bytes.len(), MAX_BLOCK_LEN);
        assert_eq!(Block::new(bytes).unwrap().len(), small_records + 2);
    }
}

/// Even at the largest target, a block is always cut before it can fill.
#[test]
#[ignore = "large allocations; run with scripts/validate.sh limits"]
fn run_never_reports_a_full_block() {
    let mut builder = RunBuilder::with_target_block_len(usize::MAX);
    let largest = vec![1; MAX_VALUE_LEN];
    builder.add(b"a", 2, Value::Bytes(&largest)).unwrap();
    builder.add(b"a", 1, Value::Bytes(&largest)).unwrap();

    // Just below the target, so these are not treated as oversized.
    let near_target = vec![2; MAX_TARGET_BLOCK_LEN - 2];
    for seq in (0..3).rev() {
        builder.add(b"b", seq, Value::Bytes(&near_target)).unwrap();
    }
    drop(largest);

    let run = builder.finish();
    assert_eq!(run.len(), 5);
    let validated = Run::new(run.as_bytes()).unwrap();
    assert_eq!(validated.get(b"a").unwrap().seq, 2);
    assert_eq!(validated.get(b"b").unwrap().seq, 2);
}
