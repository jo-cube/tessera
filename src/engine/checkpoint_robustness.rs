//! Mutate real checkpoint files, then exercise both validation and recovery.
use super::*;
use crate::block::Value;
use crate::engine::{CompactionPolicy, Compression, EngineWorkspace};
use crate::run::ReadWorkspace;
use crate::test_support::{Rng, Scratch, mutate};
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn contents(engine: &Engine) -> Model {
    let snapshot = engine.snapshot();
    let mut workspace = EngineWorkspace::new();
    let mut scan = snapshot.scan(&mut workspace);
    let mut records = Model::new();
    while let Some(record) = scan.next_record().unwrap() {
        assert!(records.len() < 128, "unbounded scan");
        assert!(
            records
                .insert(record.key.to_vec(), record.value.to_vec())
                .is_none()
        );
    }
    drop(scan);
    for key in records
        .keys()
        .cloned()
        .chain((0..36u32).map(|key| key.to_be_bytes().to_vec()))
    {
        let expected = records.get(&key).map(Vec::as_slice);
        assert_eq!(
            snapshot.get(&key, &mut workspace).unwrap().map(|v| v.value),
            expected
        );
    }
    records
}

fn run_fields(bytes: &[u8]) -> Vec<(usize, usize)> {
    let footer = bytes.len() - 48;
    let mut fields = vec![
        (footer, 8),
        (footer + 8, 8),
        (footer + 16, 4),
        (footer + 20, 4),
        (footer + 24, 8),
        (footer + 32, 4),
    ];
    let table = u64::from_le_bytes(bytes[footer..footer + 8].try_into().unwrap()) as usize;
    let count = u32::from_le_bytes(bytes[footer + 16..footer + 20].try_into().unwrap()) as usize;
    for index in 0..count {
        let at = table + index * 8;
        fields.extend([(at, 4), (at + 4, 4)]);
        let frame = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        fields.push((frame + 4, 4));
    }
    fields
}

/// Accepted bytes must give identical cursor and point-lookup results, with
/// ordered records, matching counts and sequence bounds. No declared count
/// controls a test allocation or an unbounded traversal.
fn validate_run(bytes: &[u8]) -> bool {
    let run = match Run::new(bytes) {
        Ok(run) => run,
        Err(error) => {
            let _: CorruptRun = error;
            return false;
        }
    };
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    let mut records = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        assert!(records.len() < 128, "unbounded run traversal");
        let value = match record.value {
            Value::Bytes(v) => Some(v.to_vec()),
            Value::Tombstone => None,
        };
        records.push((record.key.to_vec(), record.seq, value));
    }
    assert_eq!(records.len(), run.len());
    for pair in records.windows(2) {
        assert!(pair[0].0 < pair[1].0 || (pair[0].0 == pair[1].0 && pair[0].1 > pair[1].1));
    }
    for (i, (key, seq, value)) in records.iter().enumerate() {
        if i > 0 && records[i - 1].0 == *key {
            continue;
        }
        let found = run.get(key, &mut workspace).unwrap().unwrap();
        assert_eq!(found.seq, *seq);
        assert_eq!(
            found.value,
            value.as_deref().map_or(Value::Tombstone, Value::Bytes)
        );
    }
    true
}

fn reseal(bytes: &mut [u8]) {
    let body = bytes.len() - 4;
    let sum = crc(&[&bytes[..body]]);
    bytes[body..].copy_from_slice(&sum.to_le_bytes());
}

// Use the valid seed's boundaries so broken offsets cannot guide this helper.
fn reseal_run(bytes: &mut [u8], original: &[u8]) {
    let footer = original.len() - 48;
    let u32_at = |at| u32::from_le_bytes(original[at..at + 4].try_into().unwrap()) as usize;
    let u64_at = |at| u64::from_le_bytes(original[at..at + 8].try_into().unwrap()) as usize;
    let (table, frames_end, segments) = (u64_at(footer), u64_at(footer + 24), u32_at(footer + 32));
    let count = u32_at(footer + 16);
    for i in 0..count {
        let start = u32_at(table + i * 8);
        let end = if i + 1 == count {
            frames_end
        } else {
            u32_at(table + (i + 1) * 8)
        };
        let sum = crc(&[&bytes[start..start + 8], &bytes[start + 12..end]]);
        bytes[start + 8..start + 12].copy_from_slice(&sum.to_le_bytes());
    }
    for i in 0..segments {
        let at = table - segments * 16 + i * 16;
        let (start, len) = (u64_at(at), u32_at(at + 8));
        let sum = crc(&[&bytes[start..start + len]]);
        bytes[at + 12..at + 16].copy_from_slice(&sum.to_le_bytes());
    }
    let sum = crc(&[&bytes[table..footer]]);
    bytes[footer + 36..footer + 40].copy_from_slice(&sum.to_le_bytes());
}

fn mutations(iterations: usize) {
    let started = Instant::now();
    let mut rng = Rng(0x6861_7264_656e_696e);
    let (mut changed, mut accepted, mut accepted_runs, mut unrepaired_runs) = (0, 0, 0, 0);
    for compression in [Compression::Uncompressed, Compression::Zstd] {
        for filters in [true, false] {
            let scratch = Scratch::new("mutation");
            let options = EngineOptions {
                compression,
                memtable_budget: 4096,
                target_run_len: 2048,
                background_compaction: None,
                ..EngineOptions::default()
            };
            let engine = Engine::new(options).unwrap();
            for round in 0..3u8 {
                for key in 0..32u32 {
                    if round == 2 && key % 5 == 0 {
                        engine.delete(&key.to_be_bytes()).unwrap();
                    } else {
                        engine.put(&key.to_be_bytes(), &[round; 96]).unwrap();
                    }
                }
                engine.flush().unwrap();
            }
            if !filters {
                let policy = CompactionPolicy {
                    max_overlap: 1,
                    ..CompactionPolicy::default()
                };
                while engine
                    .core
                    .compact(policy, &mut crate::run::MergeWorkspace::new())
                    .unwrap()
                    .is_some()
                {}
            }
            let expected = contents(&engine);
            engine
                .checkpoint(&scratch.0, CheckpointOptions::default())
                .unwrap();
            engine.close().unwrap();
            let (_, manifest_path, manifest) = committed_manifest(&scratch.0).unwrap();
            assert!(!manifest.runs.is_empty());
            assert_eq!(manifest.runs.iter().any(|r| r.len > r.run_len + 4), filters);
            let current_path = scratch.0.join(CURRENT);
            let current = fs::read(&current_path).unwrap();
            let original_manifest = fs::read(&manifest_path).unwrap();
            let mut manifest_fields = vec![
                (8, 4),
                (12, 4),
                (16, 8),
                (24, 8),
                (32, 8),
                (40, 8),
                (48, 8),
                (56, 4),
                (60, 8),
                (68, 4),
            ];
            for index in 0..manifest.runs.len() {
                let at = MANIFEST_HEADER_LEN + index * MANIFEST_ENTRY_LEN;
                manifest_fields.extend([
                    (at + 16, 4),
                    (at + 20, 4),
                    (at + 24, 8),
                    (at + 32, 8),
                    (at + 40, 8),
                    (at + 48, 8),
                ]);
            }
            for iteration in 0..iterations {
                let entry = &manifest.runs[rng.below(manifest.runs.len())];
                let run_path = scratch.0.join(RUNS).join(run_name(entry.id));
                let run_file = fs::read(&run_path).unwrap();
                let run_len = entry.run_len as usize;
                // Raw validators are exercised even when outer file checksums
                // would prevent recovery from reaching the run decoder.
                let mut raw = mutate(
                    &run_file[..run_len],
                    &run_fields(&run_file[..run_len]),
                    iteration,
                    &mut rng,
                );
                if iteration % 2 == 0 && raw.len() == run_len {
                    reseal_run(&mut raw, &run_file[..run_len]);
                }
                if validate_run(&raw) && raw != run_file[..run_len] {
                    accepted_runs += 1;
                    if iteration % 2 != 0 || raw.len() != run_len {
                        unrepaired_runs += 1;
                    }
                }
                let (path, original, fields) = match iteration % 3 {
                    0 => (&current_path, &current, vec![(8, 8)]),
                    1 => (&manifest_path, &original_manifest, manifest_fields.clone()),
                    _ => (&run_path, &run_file, run_fields(&run_file[..run_len])),
                };
                let mut mutated = mutate(original, &fields, iteration, &mut rng);
                if mutated == *original {
                    continue;
                }
                changed += 1;
                // Repair half of same-sized mutations, reaching semantic
                // checks as well as the outer file integrity checks.
                let resealed = iteration % 2 == 0 && mutated.len() == original.len();
                if resealed && path != &run_path {
                    reseal(&mut mutated);
                }
                if resealed && path == &run_path {
                    reseal_run(&mut mutated[..run_len], &run_file[..run_len]);
                    let end = mutated.len() - 4;
                    let sum = crc(&[&mutated[run_len..end]]);
                    mutated[end..].copy_from_slice(&sum.to_le_bytes());
                    let mut updated = Manifest::decode(&original_manifest).unwrap();
                    updated
                        .runs
                        .iter_mut()
                        .find(|r| r.id == entry.id)
                        .unwrap()
                        .sum = crc(&[&mutated]);
                    fs::write(&manifest_path, updated.encode()).unwrap();
                }
                fs::write(path, &mutated).unwrap();
                let result =
                    catch_unwind(AssertUnwindSafe(|| Engine::recover(&scratch.0, options)));
                fs::write(path, original).unwrap();
                fs::write(&manifest_path, &original_manifest).unwrap();
                match result.expect("mutated recovery panicked") {
                    Ok(recovered) => {
                        let actual = contents(&recovered);
                        // CRC-preserving metadata changes may change the ceiling
                        // or ordering, but all read paths must still agree.
                        if !resealed {
                            assert_eq!(actual, expected);
                            panic!("changed checkpoint accepted without checksum repair");
                        }
                        recovered.close().unwrap();
                        accepted += 1;
                    }
                    Err(error) => {
                        assert!(matches!(error, OpenError::Corrupt { .. }), "{error:?}");
                    }
                }
            }
            let recovered = Engine::recover(&scratch.0, options).unwrap();
            assert_eq!(contents(&recovered), expected);
            recovered.close().unwrap();
        }
    }
    eprintln!(
        "mutations: {changed} changed files, {accepted} accepted after checksum repair; {accepted_runs} changed raw runs accepted ({unrepaired_runs} without repair); {:?}",
        started.elapsed()
    );
}

#[test]
fn bounded_checkpoint_mutations() {
    mutations(300);
}

#[test]
#[ignore = "long deterministic checkpoint mutation campaign"]
fn long_checkpoint_mutations() {
    mutations(10_000);
}
