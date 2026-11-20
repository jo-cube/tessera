//! Writes, batches, reads, snapshots, range scans and checkpoint recovery.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use tessera::{Engine, Options, WriteBatch};

    // Use an application-owned directory for persistent checkpoints.
    let dir = std::env::temp_dir().join(format!("tessera-basic-{}", std::process::id()));
    let mut options = Options::default();
    options.memory_limit = Some(256 << 20);
    let engine = Engine::open(&dir, options)?;
    let users = engine.keyspace("users")?;
    users.put(b"user/1", b"ada")?;
    assert_eq!(users.get(b"user/1")?.as_deref(), Some(&b"ada"[..]));

    let mut batch = WriteBatch::new();
    batch.put(&users, b"user/2", b"grace");
    batch.put(&users, b"user/3", b"katherine");
    engine.write(&batch)?; // All operations become visible together.

    let mut reader = engine.reader(); // Reuses its buffers across reads.
    let values = reader.multi_get(&users, &[b"user/1", b"user/0"])?;
    assert_eq!(&*values, &[Some(&b"ada"[..]), None]);
    drop(values);
    drop(reader);

    let snapshot = engine.snapshot(); // Holds no lock after acquisition.
    users.delete(b"user/1")?;
    let mut reader = snapshot.reader();
    let mut scan = reader.prefix(&users, b"user/");
    for expected in [b"ada".as_slice(), b"grace", b"katherine"] {
        assert_eq!(scan.next()?.unwrap().1, expected);
    }
    assert_eq!(scan.next()?, None);
    drop(scan);
    let mut scan = reader.range(&users, &b"user/2"[..]..);
    assert_eq!(scan.next()?.unwrap().0, b"user/2");
    drop(scan);
    drop(reader);
    drop(snapshot);

    engine.checkpoint_with(b"schema=1")?;
    engine.close()?;

    let recovered = Engine::open(&dir, options)?;
    assert_eq!(recovered.metadata().as_deref(), Some(&b"schema=1"[..]));
    let users = recovered.keyspace("users")?;
    assert_eq!(users.get(b"user/1")?, None);
    assert_eq!(users.get(b"user/3")?.as_deref(), Some(&b"katherine"[..]));
    recovered.close()?;
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
