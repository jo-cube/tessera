use super::*;
use crate::block::BlockBuilder;
use crate::run::RunBuilder;
use crate::run::dictionary::SEGMENT_RAW_LEN;

fn build(codec: Codec) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::new(codec);
    for i in 0..9_000u32 {
        builder
            .add(
                &i.to_be_bytes(),
                u64::from(i) + 1,
                Value::Bytes(&vec![(i % 19) as u8; 1024]),
            )
            .unwrap();
    }
    builder.finish()
}
fn parts(run: &Run<Vec<u8>>) -> &Segmented {
    let RunData::Segmented(parts) = &run.data else {
        panic!("expected segmented storage");
    };
    parts
}
#[test]
fn multiple_allocations_preserve_serialization_and_all_read_paths() {
    for codec in [Codec::None, Codec::Zstd] {
        let run = build(codec);
        assert_eq!(parts(&run).segments.len(), 3);
        let bytes = run.as_bytes().into_owned();
        assert_eq!(bytes.len(), run.stored_len());
        let (recovered, max) = Run::recover(bytes.clone()).unwrap();
        assert_eq!(max, 9_000);
        assert!(parts(&recovered).segments.len() >= 3);
        assert_eq!(recovered.as_bytes(), bytes);
        let mut workspace = ReadWorkspace::new();
        for candidate in [&run, &recovered] {
            let mut cursor = candidate.cursor(&mut workspace);
            for i in 0..9_000u32 {
                let record = cursor.next_record().unwrap().unwrap();
                assert_eq!(record.key, i.to_be_bytes());
                assert_eq!(record.value, Value::Bytes(&vec![(i % 19) as u8; 1024]));
            }
            assert!(cursor.next_record().unwrap().is_none());
            for i in [0u32, 4_000, 4_100, 8_000, 8_999] {
                let found = candidate
                    .get(&i.to_be_bytes(), &mut workspace)
                    .unwrap()
                    .unwrap();
                assert_eq!(found.seq, u64::from(i) + 1);
                assert_eq!(found.value, Value::Bytes(&vec![(i % 19) as u8; 1024]));
            }
        }
    }
}
#[test]
fn a_segment_exactly_at_the_raw_boundary_ends_before_the_next_record() {
    let value = vec![7; SEGMENT_RAW_LEN - 16];
    let mut block = BlockBuilder::new();
    block.add(b"a", 1, Value::Bytes(&value)).unwrap();
    assert_eq!(block.encoded_len(), SEGMENT_RAW_LEN);
    for codec in [Codec::None, Codec::Zstd] {
        let mut builder = RunBuilder::new(codec);
        builder.add(b"a", 1, Value::Bytes(&value)).unwrap();
        builder.add(b"b", 2, Value::Bytes(b"next")).unwrap();
        let run = builder.finish();
        assert_eq!(parts(&run).segments.len(), 2);
        assert_eq!(run.view().frame(0).unwrap().decoded_len, SEGMENT_RAW_LEN);
        let mut workspace = ReadWorkspace::new();
        assert_eq!(
            run.get(b"a", &mut workspace).unwrap().unwrap().value,
            Value::Bytes(&value)
        );
        assert_eq!(
            run.get(b"b", &mut workspace).unwrap().unwrap().value,
            Value::Bytes(b"next")
        );
    }
}
#[test]
fn damage_in_one_segment_leaves_other_segments_readable() {
    let run = build(Codec::Zstd);
    let middle = &parts(&run).segments[1];
    let view = run.view();
    let block = view
        .table
        .partition_point(|entry| frame_offset(*entry) < middle.frame_start);
    let frame = view.frame(block).unwrap();
    let mut workspace = ReadWorkspace::new();
    let decoded = workspace.load(&frame).unwrap();
    let mut position = BlockPosition::new();
    let key = position
        .next_record(BlockView::open(decoded).unwrap())
        .unwrap()
        .unwrap()
        .key
        .to_vec();
    let damaged = run.damaged_at(middle.frame_start + frame::HEADER_LEN);
    assert_eq!(
        damaged.get(&key, &mut workspace).unwrap_err().kind,
        RunCorruption::ChecksumMismatch
    );
    for i in [0u32, 8_999] {
        assert!(
            damaged
                .get(&i.to_be_bytes(), &mut workspace)
                .unwrap()
                .is_some()
        );
    }
    let mut cursor = damaged.cursor(&mut workspace);
    assert!(cursor.seek(&key).is_err());
    cursor.seek(&8_999u32.to_be_bytes()).unwrap();
    assert_eq!(
        cursor.next_record().unwrap().unwrap().key,
        8_999u32.to_be_bytes()
    );
    assert_eq!(
        Run::new(damaged.as_bytes()).unwrap_err().kind,
        RunCorruption::ChecksumMismatch
    );
}
