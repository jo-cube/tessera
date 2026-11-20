//! Atomic writes, reads, snapshots, sorted ingestion and checkpoint recovery.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use tessera::engine::{CheckpointOptions, Engine, EngineOptions, EngineWorkspace, WriteBatch};

    let mut options = EngineOptions::default();
    options.memory_limit = Some(256 << 20);
    let engine = Engine::new(options)?;
    engine.put(b"user/1", b"ada")?;
    let mut workspace = EngineWorkspace::new();
    assert_eq!(
        engine.get(b"user/1", &mut workspace)?.unwrap().value,
        b"ada"
    );

    let mut batch = WriteBatch::new();
    batch.put(b"user/2", b"grace");
    batch.put(b"user/3", b"katherine");
    engine.write(&batch)?; // All operations become visible together.
    engine.multi_get(&[b"user/1", b"user/0"], &mut workspace, |index, value| {
        assert_eq!(value, if index == 0 { Some(&b"ada"[..]) } else { None });
    })?;

    let snapshot = engine.snapshot(); // Holds no lock after acquisition.
    let mut scan = snapshot.scan_prefix(b"user/", &mut workspace);
    for expected in [b"ada".as_slice(), b"grace", b"katherine"] {
        assert_eq!(scan.next_record()?.unwrap().value, expected);
    }
    assert_eq!(scan.next_record()?, None);
    drop(scan);
    drop(snapshot);

    batch.clear(); // Retains capacity; ingestion requires sorted, unique keys.
    batch.put(b"user/4", b"dorothy");
    batch.put(b"user/5", b"mary");
    engine.ingest_sorted(&batch)?;

    // Use an application-owned directory for persistent checkpoints.
    let dir = std::env::temp_dir().join(format!("tessera-basic-{}", std::process::id()));
    engine.checkpoint_with_metadata(&dir, CheckpointOptions::default(), b"schema=1")?;
    engine.close()?;
    let (recovered, metadata) = Engine::recover_with_metadata(&dir, EngineOptions::default())?;
    assert_eq!(metadata, b"schema=1");
    assert_eq!(
        recovered.get(b"user/5", &mut workspace)?.unwrap().value,
        b"mary"
    );
    recovered.close()?;

    let recovered = Engine::recover(&dir, EngineOptions::default())?;
    assert_eq!(
        recovered.get(b"user/1", &mut workspace)?.unwrap().value,
        b"ada"
    );
    recovered.close()?;
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
