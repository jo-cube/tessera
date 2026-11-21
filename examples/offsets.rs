//! A consumer of a partitioned log, in the style of a Kafka consumer. Each
//! poll's records and the partitions' next offsets go into one `WriteBatch`,
//! so a checkpoint holds offsets that match its data exactly, whatever is
//! written concurrently. The consumer checkpoints periodically, stops without
//! a checkpoint (losing its latest batches), reopens and resumes from the
//! offsets it recovers.

use tessera::{Engine, Keyspace, Options, WriteBatch};

const PARTITIONS: u32 = 4;
const LOG_LEN: u64 = 5_000;

/// The log being consumed: record `offset` of `partition`, generated.
fn log_record(partition: u32, offset: u64) -> (Vec<u8>, Vec<u8>) {
    let key = format!("user/{:04}", (offset * 7 + u64::from(partition)) % 997);
    let value = format!("p{partition}@{offset}");
    (key.into_bytes(), value.into_bytes())
}

/// The next offset to read in `partition`: 0 if nothing was consumed.
fn next_offset(offsets: &Keyspace, partition: u32) -> tessera::Result<u64> {
    Ok(match offsets.get(&partition.to_be_bytes())? {
        Some(bytes) => u64::from_be_bytes(bytes.try_into().expect("8-byte offset")),
        None => 0,
    })
}

/// Consumes up to `polls` polls of at most 100 records from each partition,
/// checkpointing every `every` polls. Returns the polls made.
fn consume(engine: &Engine, polls: usize, every: usize) -> tessera::Result<usize> {
    let state = engine.keyspace("state")?;
    let offsets = engine.keyspace("offsets")?;
    let mut next: Vec<u64> = (0..PARTITIONS)
        .map(|p| next_offset(&offsets, p))
        .collect::<tessera::Result<_>>()?;
    let mut batch = WriteBatch::new();
    for poll in 1..=polls {
        if next.iter().all(|&n| n == LOG_LEN) {
            return Ok(poll - 1);
        }
        batch.clear();
        for (partition, offset) in (0..PARTITIONS).zip(next.iter_mut()) {
            let end = (*offset + 100).min(LOG_LEN);
            for o in *offset..end {
                let (key, value) = log_record(partition, o);
                batch.put(&state, &key, &value);
            }
            *offset = end;
            batch.put(&offsets, &partition.to_be_bytes(), &end.to_be_bytes());
        }
        engine.write(&batch)?;
        if poll % every == 0 {
            engine.checkpoint()?;
        }
    }
    Ok(polls)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("tessera-offsets-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // 13 polls with a checkpoint every 5: polls 11 to 13 are not durable.
    let engine = Engine::open(&dir, Options::default())?;
    assert_eq!(consume(&engine, 13, 5)?, 13);
    engine.close()?;

    let engine = Engine::open(&dir, Options::default())?;
    // The recovered checkpoint holds the offsets; a missing keyspace here
    // would mean the directory is not this consumer's.
    let offsets = engine
        .existing_keyspace("offsets")?
        .expect("the checkpoint holds the offsets");
    for partition in 0..PARTITIONS {
        assert_eq!(next_offset(&offsets, partition)?, 1000);
    }
    // Resume: the polls after the checkpoint are read again from the log.
    let polls = consume(&engine, usize::MAX, 5)?;
    assert_eq!(polls, 40);
    engine.checkpoint()?;
    engine.close()?;

    // The state equals one pass over the whole log in poll order.
    let engine = Engine::open(&dir, Options::default())?;
    let state = engine
        .existing_keyspace("state")?
        .expect("the checkpoint holds the state");
    let mut expected = std::collections::BTreeMap::new();
    for start in (0..LOG_LEN).step_by(100) {
        for partition in 0..PARTITIONS {
            for offset in start..start + 100 {
                let (key, value) = log_record(partition, offset);
                expected.insert(key, value);
            }
        }
    }
    let snapshot = engine.snapshot();
    let mut reader = snapshot.reader();
    let mut scan = reader.prefix(&state, b"");
    let mut expected = expected.into_iter();
    while let Some((key, value)) = scan.next()? {
        let (k, v) = expected.next().expect("no extra keys");
        assert_eq!((key, value), (&k[..], &v[..]));
    }
    assert!(expected.next().is_none());
    drop(scan);
    drop(reader);
    drop(snapshot);
    engine.close()?;
    std::fs::remove_dir_all(&dir)?;
    println!("offsets: resumed exactly from the last checkpoint");
    Ok(())
}
