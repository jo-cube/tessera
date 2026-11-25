//! Scans and multi-gets whose bounds fall on piece and slice boundaries:
//! before a bottom job, after two of its pieces publish and it stops, and
//! once later jobs finish the range.
use super::*;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

type Bounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

fn within(key: &[u8], (lo, hi): &Bounds) -> bool {
    let above = match lo {
        Included(b) => key >= b.as_slice(),
        Excluded(b) => key > b.as_slice(),
        Unbounded => true,
    };
    let below = match hi {
        Included(b) => key <= b.as_slice(),
        Excluded(b) => key < b.as_slice(),
        Unbounded => true,
    };
    above && below
}

/// Keys on and beside the boundaries: the first bottom runs start at 0, 32,
/// 64 and 96, the stopped job leaves its source sliced from 64, and keys
/// that no run holds fall below, between and above them.
fn probes() -> Vec<Vec<u8>> {
    let mut probes: Vec<Vec<u8>> = [0u16, 1, 31, 32, 33, 63, 64, 65, 95, 96, 127, 128, 254, 255]
        .iter()
        .map(|k| k.to_be_bytes().to_vec())
        .collect();
    probes.extend([
        vec![],
        vec![0],
        vec![0, 31, 0],
        vec![0, 63, 255],
        vec![0, 64, 0],
        vec![0, 255, 0],
        vec![1],
    ]);
    probes.sort();
    probes
}

fn all_bounds(probes: &[Vec<u8>]) -> Vec<Bounds> {
    let mut bounds = vec![(Unbounded, Unbounded)];
    for p in probes {
        bounds.push((Included(p.clone()), Unbounded));
        bounds.push((Excluded(p.clone()), Unbounded));
        bounds.push((Unbounded, Included(p.clone())));
        bounds.push((Unbounded, Excluded(p.clone())));
        for q in probes.iter().filter(|q| *q >= p) {
            bounds.push((Included(p.clone()), Excluded(q.clone())));
            bounds.push((Excluded(p.clone()), Included(q.clone())));
        }
    }
    bounds
}

fn check_bounds(reader: &mut Reader<'_>, ks: &Keyspace, what: &str) {
    let keys: Vec<Vec<u8>> = (0..KEYS).map(|k| k.to_be_bytes().to_vec()).collect();
    let probes = probes();
    for bounds in all_bounds(&probes) {
        let expected: Vec<&Vec<u8>> = keys.iter().filter(|k| within(k, &bounds)).collect();
        let mut scan = reader.range(ks, bounds.clone());
        let mut seen = Vec::new();
        while let Some((key, value)) = scan.next().unwrap() {
            assert_eq!(value, &[1; 128][..], "{what} {bounds:?}");
            seen.push(key.to_vec());
        }
        drop(scan);
        assert_eq!(
            seen.iter().collect::<Vec<_>>(),
            expected,
            "{what} {bounds:?}"
        );
    }
    for prefix in &probes {
        let mut scan = reader.prefix(ks, prefix);
        let mut seen = Vec::new();
        while let Some((key, _)) = scan.next().unwrap() {
            seen.push(key.to_vec());
        }
        drop(scan);
        let expected: Vec<_> = keys.iter().filter(|k| k.starts_with(prefix)).collect();
        assert_eq!(
            seen.iter().collect::<Vec<_>>(),
            expected,
            "{what} {prefix:?}"
        );
    }
    // A sorted multi-get of keys and absent probes agrees with single gets.
    let singles: Vec<Option<Vec<u8>>> = probes
        .iter()
        .map(|p| reader.get(ks, p).unwrap().map(<[u8]>::to_vec))
        .collect();
    let many: Vec<Option<Vec<u8>>> = reader
        .multi_get(ks, &probes)
        .unwrap()
        .iter()
        .map(|v| v.map(<[u8]>::to_vec))
        .collect();
    assert_eq!(many, singles, "{what}");
    for (probe, value) in probes.iter().zip(&singles) {
        assert_eq!(value.is_some(), probe.len() == 2, "{what} {probe:?}");
    }
}

#[test]
fn bounds_on_piece_and_slice_boundaries_scan_exactly_their_keys() {
    let engine = fixture();
    let ks = engine.ks();
    let before = engine.snapshot();
    check_bounds(&mut before.reader(), &ks, "before");
    engine.core.fail_piece.store(3, Ordering::Relaxed);
    assert_eq!(
        engine.compact(&mut MergeWorkspace::new()),
        Err(CompactionError::Stale)
    );
    engine.core.fail_piece.store(0, Ordering::Relaxed);
    {
        let state = engine.core.read_state();
        assert!(
            state.only().levels.runs()[0].iter().any(|r| r
                .run
                .slice()
                .is_some_and(|(min, _)| min == 64u16.to_be_bytes())),
            "the stopped job leaves its source sliced at the third bottom run"
        );
    }
    let stopped = engine.snapshot();
    check_bounds(&mut engine.reader(), &ks, "latest, stopped");
    check_bounds(&mut stopped.reader(), &ks, "snapshot, stopped");
    check_bounds(&mut before.reader(), &ks, "before, after stop");
    while engine
        .compact(&mut MergeWorkspace::new())
        .unwrap()
        .is_some()
    {}
    check_bounds(&mut engine.reader(), &ks, "latest, finished");
    check_bounds(&mut stopped.reader(), &ks, "stopped, after finish");
}
