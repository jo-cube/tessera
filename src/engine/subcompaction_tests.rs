//! Serial and parallel compaction must agree on records and sequence bounds.
//! Independent physical output and snapshot checks cover shared publication
//! and workspace allowances without depending on builder scheduling.

use super::*;

fn physical(runs: &[Run<Vec<u8>>]) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
    runs.iter()
        .flat_map(|run| {
            let mut workspace = crate::run::ReadWorkspace::new();
            let mut cursor = run.cursor(&mut workspace);
            let mut records = Vec::new();
            while let Some(record) = cursor.next_record().unwrap() {
                records.push((
                    record.key.to_vec(),
                    record.seq,
                    match record.value {
                        Value::Bytes(bytes) => Some(bytes.to_vec()),
                        Value::Tombstone => None,
                    },
                ));
            }
            records
        })
        .collect()
}

#[test]
fn parallel_jobs_match_serial_and_preserve_snapshots_and_settled_sequences() {
    for (snapshot_at, target_run_len) in [0, 1, 2]
        .into_iter()
        .flat_map(|at| [256 << 10, 4 << 20].map(|target| (at, target)))
    {
        let engine = Engine::new(EngineOptions {
            background_threads: 4,
            background_compaction: None,
            compression: Compression::Uncompressed,
            memtable_budget: 8 << 20,
            target_run_len,
            ..EngineOptions::default()
        })
        .unwrap();
        for at in 0..6000u64 {
            engine.put(&at.to_be_bytes(), &[1; 128]).unwrap();
        }
        engine.flush().unwrap();
        let early = (snapshot_at == 1).then(|| engine.snapshot());
        for at in 0..6000u64 {
            if at % 5 == 0 {
                engine.delete(&at.to_be_bytes()).unwrap();
            } else {
                engine.put(&at.to_be_bytes(), &[2; 128]).unwrap();
            }
        }
        engine.flush().unwrap();
        let options = CompactionPolicy {
            max_overlap: 1,
            ..CompactionPolicy::default()
        };
        let job = engine.core.begin_job(options).unwrap().unwrap();
        assert_eq!(job.zero_seq, target_run_len == 4 << 20 && snapshot_at != 1);
        let late = (snapshot_at == 2).then(|| engine.snapshot());
        let views: Vec<_> = job.inputs.iter().map(|run| run.run.view()).collect();
        let ranges: Vec<_> = job
            .others
            .iter()
            .map(|run| run.run.key_range().unwrap())
            .collect();
        let serial = crate::run::compact_with(
            &views,
            |key| ranges.iter().any(|r| r.min <= key && key <= r.max),
            CompactLimits {
                target_run_len: engine.core.target_run_len,

                max_workspace_bytes: options.max_workspace_bytes,
                codec: engine.core.codec,
                target_block_compressed: engine.core.target_block_compressed,
                zero_seq: job.zero_seq,
            },
            &job.cuts,
            &mut MergeWorkspace::new(),
        )
        .unwrap();
        drop(ranges);
        drop(views);
        let built = job.build(&mut MergeWorkspace::new()).unwrap();
        assert_eq!(physical(&serial.runs), physical(&built.output.runs));
        if target_run_len == 4 << 20 {
            assert_eq!(built.output.runs.len(), serial.runs.len());
            assert_eq!(built.output.workspace_bytes, serial.workspace_bytes);
        } else {
            assert!(built.output.workspace_bytes > serial.workspace_bytes);
        }
        built.publish().unwrap();
        let mut workspace = EngineWorkspace::new();
        for at in 0..6000u64 {
            let key = at.to_be_bytes();
            let expected = (at % 5 != 0).then_some([2; 128].as_slice());
            assert_eq!(
                engine.get(&key, &mut workspace).unwrap().map(|v| v.value),
                expected
            );
            if let Some(snapshot) = &early {
                assert_eq!(
                    snapshot.get(&key, &mut workspace).unwrap().unwrap().value,
                    &[1; 128]
                );
            }
            if let Some(snapshot) = &late {
                assert_eq!(
                    snapshot.get(&key, &mut workspace).unwrap().map(|v| v.value),
                    expected
                );
            }
        }
    }
}

#[test]
fn background_thread_option_accepts_one_and_rejects_zero() {
    let options = EngineOptions {
        background_threads: 1,
        ..EngineOptions::default()
    };
    let engine = Engine::new(options).unwrap();
    engine.put(b"key", b"value").unwrap();
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
    assert_eq!(engine.core.background_threads, 1);
    assert!(matches!(
        Engine::new(EngineOptions {
            background_threads: 0,
            ..options
        }),
        Err(OpenError::Config(ConfigError {
            setting: "EngineOptions::background_threads",
            ..
        }))
    ));
}

#[test]
fn non_default_ratios_preserve_updates_and_deletes() {
    for ratio in [2, 10, usize::MAX] {
        let engine = Engine::new(EngineOptions {
            level_ratio: ratio,
            background_threads: 1,
            target_run_len: 4096,
            memtable_budget: 16 << 10,
            ..EngineOptions::default()
        })
        .unwrap();
        for round in 0..4u8 {
            for at in 0..600u64 {
                if round == 3 && at % 3 == 0 {
                    engine.delete(&at.to_be_bytes()).unwrap();
                } else {
                    engine.put(&at.to_be_bytes(), &[round; 80]).unwrap();
                }
            }
            engine.flush().unwrap();
            engine.wait_for_compaction().unwrap();
        }
        let mut workspace = EngineWorkspace::new();
        for at in 0..600u64 {
            assert_eq!(
                engine
                    .get(&at.to_be_bytes(), &mut workspace)
                    .unwrap()
                    .map(|v| v.value),
                (at % 3 != 0).then_some([3; 80].as_slice())
            );
        }
    }
    for ratio in [0, 1] {
        assert!(matches!(
            Engine::new(EngineOptions {
                level_ratio: ratio,
                ..EngineOptions::default()
            }),
            Err(OpenError::Config(ConfigError {
                setting: "EngineOptions::level_ratio",
                ..
            }))
        ));
    }
}
