//! Read-heavy threads over one keyspace: each thread takes its own `Reader`
//! and keeps it, so its buffers and decompression contexts are reused
//! across point reads, multi-gets and range scans while a writer updates
//! the data.

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use tessera::{Engine, Options, WriteBatch};

const KEYS: u32 = 20_000;

fn key(i: u32) -> Vec<u8> {
    format!("item/{i:08}").into_bytes()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::in_memory(Options::default())?;
    let items = engine.keyspace("items")?;
    let mut batch = WriteBatch::new();
    for i in 0..KEYS {
        batch.put(
            &items,
            &key(i),
            format!("{{\"id\":{i},\"version\":0}}").as_bytes(),
        );
    }
    engine.write(&batch)?;

    let stop = AtomicBool::new(false);
    thread::scope(|scope| -> tessera::Result<()> {
        // A writer bumps versions while the readers run.
        let writer = scope.spawn(|| -> tessera::Result<()> {
            let mut version = 0;
            while !stop.load(Ordering::Relaxed) && version < 50 {
                version += 1;
                let mut batch = WriteBatch::new();
                for i in (0..KEYS).step_by(97) {
                    let value = format!("{{\"id\":{i},\"version\":{version}}}");
                    batch.put(&items, &key(i), value.as_bytes());
                }
                engine.write(&batch)?;
            }
            Ok(())
        });

        let readers: Vec<_> = (0..4u32)
            .map(|t| {
                let (engine, items) = (&engine, &items);
                scope.spawn(move || -> tessera::Result<(usize, usize)> {
                    let mut reader = engine.reader();
                    let (mut found, mut scanned) = (0, 0);
                    for round in 0..200u32 {
                        let base = (round * 7919 + t * 104_729) % KEYS;
                        found += reader.get(items, &key(base))?.is_some() as usize;

                        let keys: Vec<Vec<u8>> =
                            (0..64).map(|j| key((base + j * 13) % KEYS)).collect();
                        // `MultiGet` returns its vector to the reader when
                        // dropped, so it holds the borrow until then.
                        let values = reader.multi_get(items, &keys)?;
                        found += values.iter().flatten().count();
                        drop(values);

                        let (start, end) = (key(base), key(base + 50));
                        let mut scan = reader.range(items, start..end);
                        while let Some((k, v)) = scan.next()? {
                            assert!(k.starts_with(b"item/") && v.starts_with(b"{\"id\":"));
                            scanned += 1;
                        }
                    }
                    Ok((found, scanned))
                })
            })
            .collect();

        let mut total = (0, 0);
        for reader in readers {
            let (found, scanned) = reader.join().expect("reader panicked")?;
            total = (total.0 + found, total.1 + scanned);
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().expect("writer panicked")?;
        // Every key exists, so every lookup finds one; scans near the end of
        // the key space are shorter.
        assert_eq!(total.0, 4 * 200 * 65);
        assert!(total.1 > 4 * 200 * 40);
        println!(
            "hot_reads: {} values found, {} records scanned",
            total.0, total.1
        );
        Ok(())
    })?;
    engine.close()?;
    Ok(())
}
