//! Mutate real checkpoint files, then exercise both validation and recovery.
use super::*;
use crate::block::Value;
use crate::engine::{CompactionPolicy, Compression, Engine, ErrorKind};
use crate::run::ReadWorkspace;
use crate::test_support::{Rng, Scratch, campaign_seed, mutate};
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// Every record of the engine's keyspace, whatever its recovered name,
/// checked against point reads.
fn contents(engine: &Engine) -> Model {
    let Some(name) = engine.keyspaces().pop() else {
        return Model::new();
    };
    let ks = engine.keyspace(&name).unwrap();
    let snapshot = engine.snapshot();
    let mut reader = snapshot.reader();
    let mut scan = reader.range(&ks, ..);
    let mut records = Model::new();
    while let Some((key, value)) = scan.next().unwrap() {
        assert!(records.len() < 128, "unbounded scan");
        assert!(records.insert(key.to_vec(), value.to_vec()).is_none());
    }
    drop(scan);
    for key in records
        .keys()
        .cloned()
        .chain((0..36u32).map(|key| key.to_be_bytes().to_vec()))
    {
        let expected = records.get(&key).map(Vec::as_slice);
        assert_eq!(reader.get(&ks, &key).unwrap(), expected);
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

/// The mutation stream's fixed seed. In the long campaign it derives from
/// `TESSERA_TEST_SEED` when that is set.
const MUTATION_SEED: u64 = 0x6861_7264_656e_696e;

fn mutations(iterations: usize, seed: u64) {
    let started = Instant::now();
    let mut rng = Rng(seed);
    let (mut changed, mut accepted, mut accepted_runs, mut unrepaired_runs) = (0, 0, 0, 0);
    for compression in [
        Compression::Uncompressed,
        Compression::Zstd,
        Compression::ZstdFastDecode,
    ] {
        for filters in [true, false] {
            let scratch = Scratch::new("mutation");
            let options = Options {
                compression,
                // Each seeded round fits one table, so worker scheduling
                // cannot change how tables are grouped into checkpoint runs.
                memtable_size: 64 << 10,
                target_run_len: 2048,
                background_compaction: None,
                ..Options::default()
            };
            let engine = Engine::in_memory(options).unwrap();
            for round in 0..3u8 {
                for key in 0..32u32 {
                    if round == 2 && key % 5 == 0 {
                        engine.ks().delete(&key.to_be_bytes()).unwrap();
                    } else {
                        engine.ks().put(&key.to_be_bytes(), &[round; 96]).unwrap();
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
            engine.checkpoint_to(&scratch.0).unwrap();
            engine.close().unwrap();
            let (_, manifest_path, mut manifest) = committed_manifest(&scratch.0).unwrap();
            // Namespace randomness must not change mutated bytes or checksums.
            // Keep the engine's run bytes and all other manifest fields, only
            // renaming IDs to stable fixture-local values.
            for (index, entry) in manifest.runs.iter_mut().enumerate() {
                let id = index as u128 + 1;
                fs::rename(
                    scratch.0.join(RUNS).join(run_name(entry.id)),
                    scratch.0.join(RUNS).join(run_name(id)),
                )
                .unwrap();
                entry.id = id;
            }
            manifest.next_run_id = manifest.runs.len() as u64 + 1;
            fs::write(&manifest_path, manifest.encode()).unwrap();
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
                (60, 4),
                (64, 4),
                (68, 4),
                (72, 8),
                (80, 8),
                (MANIFEST_HEADER_LEN, 4),
                (MANIFEST_HEADER_LEN + 4, 4),
                (MANIFEST_HEADER_LEN + 8, 1),
            ];
            // One keyspace, "test": a 9-byte entry and its name.
            let entries = MANIFEST_HEADER_LEN + 9 + "test".len();
            for index in 0..manifest.runs.len() {
                let at = entries + index * MANIFEST_ENTRY_LEN;
                manifest_fields.extend([
                    (at + 16, 4),
                    (at + 20, 4),
                    (at + 24, 4),
                    (at + 28, 8),
                    (at + 36, 8),
                    (at + 44, 8),
                    (at + 52, 8),
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
                let result = catch_unwind(AssertUnwindSafe(|| Engine::open(&scratch.0, options)));
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
                        assert_eq!(error.kind(), ErrorKind::Corruption, "{error:?}");
                    }
                }
            }
            let recovered = Engine::open(&scratch.0, options).unwrap();
            assert_eq!(contents(&recovered), expected);
            recovered.close().unwrap();
        }
    }
    eprintln!(
        "mutations (seed {seed}): {changed} changed files, {accepted} accepted after checksum repair; {accepted_runs} changed raw runs accepted ({unrepaired_runs} without repair); {:?}",
        started.elapsed()
    );
}

#[test]
fn bounded_checkpoint_mutations() {
    mutations(300, MUTATION_SEED);
}

#[test]
#[ignore = "long deterministic checkpoint mutation campaign"]
fn long_checkpoint_mutations() {
    mutations(10_000, campaign_seed(MUTATION_SEED));
}

/// A checkpoint of keyspaces "a", "b" and "c", where "b" was dropped and
/// created again so that its ID is 3 and the next ID is 4.
fn keyspace_checkpoint(scratch: &Scratch) -> Manifest {
    let options = Options {
        background_compaction: None,
        ..Options::default()
    };
    let engine = Engine::in_memory(options).unwrap();
    for name in ["a", "b", "c"] {
        engine
            .keyspace(name)
            .unwrap()
            .put(b"k", name.as_bytes())
            .unwrap();
    }
    engine.drop_keyspace("b").unwrap();
    engine.keyspace("b").unwrap().put(b"k", b"b2").unwrap();
    engine.checkpoint_to(&scratch.0).unwrap();
    engine.close().unwrap();
    let (_, _, manifest) = committed_manifest(&scratch.0).unwrap();
    let ids: Vec<_> = manifest
        .keyspaces
        .iter()
        .map(|keyspace| (keyspace.id, keyspace.name.as_str()))
        .collect();
    assert_eq!(ids, [(0, "a"), (2, "c"), (3, "b")]);
    assert_eq!(manifest.next_keyspace, 4);
    manifest
}

/// Writes `manifest` over the committed one, as encoded, and opens the
/// directory, expecting corruption with a message containing `message`.
fn rejected(scratch: &Scratch, bytes: &[u8], message: &str) {
    let (_, path, _) = committed_manifest(&scratch.0).unwrap();
    let original = fs::read(&path).unwrap();
    fs::write(&path, bytes).unwrap();
    let error = Engine::open(&scratch.0, Options::default()).unwrap_err();
    fs::write(&path, original).unwrap();
    assert_eq!(error.kind(), ErrorKind::Corruption, "{error}");
    assert!(error.to_string().contains(message), "{error}");
}

#[test]
fn recovery_rejects_each_damaged_keyspace_table() {
    let scratch = Scratch::new("keyspace-table");
    let manifest = keyspace_checkpoint(&scratch);
    let damaged = |change: &dyn Fn(&mut Manifest)| {
        let mut copy = Manifest {
            keyspaces: manifest.keyspaces.clone(),
            runs: manifest.runs.clone(),
            metadata: manifest.metadata.clone(),
            ..manifest
        };
        change(&mut copy);
        copy.encode()
    };
    // A run whose keyspace is not listed.
    rejected(
        &scratch,
        &damaged(&|m| m.keyspaces.retain(|k| k.name != "c")),
        "a run belongs to keyspace ID 2, which is not listed",
    );
    // Two keyspaces with one name, and two with one ID.
    rejected(
        &scratch,
        &damaged(&|m| m.keyspaces[1].name = "a".into()),
        "two keyspaces share a name or an ID",
    );
    rejected(
        &scratch,
        &damaged(&|m| {
            m.keyspaces[1].id = 0;
            m.runs.retain(|run| run.keyspace == 0 || run.keyspace == 3);
        }),
        "two keyspaces share a name or an ID",
    );
    // An ID at or above the next ID.
    rejected(
        &scratch,
        &damaged(&|m| m.next_keyspace = 3),
        "keyspace ID 3 is not below the next keyspace ID 3",
    );
    // A malformed table: an invalid name, a zero depth, IDs out of order.
    rejected(
        &scratch,
        &damaged(&|m| m.keyspaces[0].name = "a b".into()),
        "malformed keyspace table",
    );
    rejected(
        &scratch,
        &damaged(&|m| m.keyspaces[0].depth = 0),
        "malformed keyspace table",
    );
    rejected(
        &scratch,
        &damaged(&|m| m.keyspaces.swap(0, 1)),
        "malformed keyspace table",
    );
    // A table length that disagrees with its entries, resealed.
    let mut bytes = manifest.encode();
    let table = u32::from_le_bytes(bytes[64..68].try_into().unwrap());
    bytes[64..68].copy_from_slice(&(table - 1).to_le_bytes());
    let mut shorter = bytes[..MANIFEST_HEADER_LEN + table as usize - 1].to_vec();
    shorter.extend_from_slice(&bytes[MANIFEST_HEADER_LEN + table as usize..]);
    reseal(&mut shorter);
    rejected(&scratch, &shorter, "malformed keyspace table");
    // More keyspaces than the limit.
    let mut bytes = manifest.encode();
    bytes[60..64].copy_from_slice(&(MAX_KEYSPACES as u32 + 1).to_le_bytes());
    reseal(&mut bytes);
    rejected(&scratch, &bytes, "malformed keyspace table");
    // Runs of one keyspace out of table order.
    rejected(
        &scratch,
        &damaged(&|m| m.runs.reverse()),
        "run IDs, levels, flush groups, level order or copies of one update disagree",
    );
    // The undamaged checkpoint still opens, with every keyspace.
    let engine = Engine::open(&scratch.0, Options::default()).unwrap();
    assert_eq!(engine.keyspaces(), ["a", "b", "c"]);
    assert_eq!(
        engine.keyspace("b").unwrap().get(b"k").unwrap().unwrap(),
        b"b2"
    );
    engine.close().unwrap();
}

#[test]
fn recovery_names_an_unsupported_manifest_version() {
    let scratch = Scratch::new("manifest-version");
    let manifest = keyspace_checkpoint(&scratch);
    for version in [0, 2, 5, u32::MAX] {
        let mut bytes = manifest.encode();
        bytes[8..12].copy_from_slice(&version.to_le_bytes());
        reseal(&mut bytes);
        rejected(
            &scratch,
            &bytes,
            &format!("unsupported manifest version {version}"),
        );
        // A manifest of another version is reported by its version, whatever
        // its length.
        rejected(
            &scratch,
            &bytes[..MANIFEST_HEADER_LEN / 2],
            &format!("unsupported manifest version {version}"),
        );
    }
    let mut bytes = manifest.encode();
    bytes[12..16].copy_from_slice(&2u32.to_le_bytes());
    reseal(&mut bytes);
    rejected(&scratch, &bytes, "unsupported manifest flags 0x2");
}
