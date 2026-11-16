//! Pins the encoding of every log operation. If this fails, the bytes of a
//! segment changed: that is a format change (docs/wal-format.md,
//! "Evolution"), not a refactor.

#![allow(clippy::too_many_lines, clippy::cast_possible_wrap)]

use skyfs_format::wal::v1::{
    BlobRange, CloneRange, DataRef, Fallocate, FileKind, InlineRange, Link, Make, MountBegin,
    MountEnd, Reclaim, Record, Remove, RemoveXattr, Rename, SegmentHeader, SetAttr, SetXattr,
    VolumeCreate, Write, data_ref, record::Op,
};
use skyfs_format::{Segment, SegmentBuilder};

fn every_op() -> Vec<Op> {
    vec![
        Op::VolumeCreate(VolumeCreate {
            volume_id: vec![1; 16],
            root_mode: 0o755,
            root_uid: 1,
            root_gid: 2,
            inline_data_max: 1 << 20,
        }),
        Op::MountBegin(MountBegin {
            epoch: 3,
            writer_id: b"host/1".to_vec(),
            previous_clean: true,
            lost_after_op_id: 4,
            base_snapshot_gen: 5,
        }),
        Op::Make(Make {
            parent: 1,
            name: b"f".to_vec(),
            ino: 2,
            kind: FileKind::Symlink as i32,
            mode: 0o644,
            uid: 6,
            gid: 7,
            rdev: 8,
            symlink_target: b"t".to_vec(),
        }),
        Op::Link(Link {
            ino: 2,
            new_parent: 1,
            new_name: b"g".to_vec(),
        }),
        Op::Remove(Remove {
            parent: 1,
            name: b"g".to_vec(),
            ino: 2,
            is_dir: true,
        }),
        Op::Rename(Rename {
            old_parent: 1,
            old_name: b"a".to_vec(),
            new_parent: 9,
            new_name: b"b".to_vec(),
            mode: 2,
            ino: 10,
            replaced_ino: 11,
        }),
        Op::Write(Write {
            ino: 2,
            offset: 12,
            len: 5,
            data: Some(DataRef {
                location: Some(data_ref::Location::Inline(InlineRange {
                    offset: 0,
                    len: 5,
                })),
            }),
            crc32c: 13,
            cleared_setid: true,
        }),
        Op::Write(Write {
            ino: 2,
            offset: 14,
            len: 15,
            data: Some(DataRef {
                location: Some(data_ref::Location::Blob(BlobRange {
                    key: "blobs/0000000000000003/0000000000000000".into(),
                    offset: 16,
                    len: 15,
                })),
            }),
            crc32c: 17,
            cleared_setid: false,
        }),
        Op::Fallocate(Fallocate {
            ino: 2,
            offset: 18,
            len: 19,
            mode: 6,
        }),
        Op::CloneRange(CloneRange {
            src_ino: 2,
            src_offset: 20,
            dst_ino: 21,
            dst_offset: 22,
            len: 23,
        }),
        Op::SetAttr(SetAttr {
            ino: 2,
            mode: Some(0o4755),
            uid: Some(24),
            gid: Some(25),
            size: Some(26),
            atime_ns: Some(-27),
            mtime_ns: Some(28),
            ctime_ns: Some(29),
        }),
        Op::SetXattr(SetXattr {
            ino: 2,
            name: b"user.k".to_vec(),
            value: b"v".to_vec(),
            mode: 1,
        }),
        Op::RemoveXattr(RemoveXattr {
            ino: 2,
            name: b"user.k".to_vec(),
        }),
        Op::Reclaim(Reclaim { ino: 2 }),
        Op::MountEnd(MountEnd {}),
    ]
}

#[test]
fn golden_segment_bytes() {
    let mut b = SegmentBuilder::new(SegmentHeader {
        volume_id: vec![1; 16],
        seq: 30,
        epoch: 3,
        prev_digest: vec![0xaa; 32],
        first_op_id: 100,
        record_count: 0,
        snapshot_gen_hint: 31,
    });
    b.push_inline(b"hello");
    for (i, op) in every_op().into_iter().enumerate() {
        b.push_record(&Record {
            op_id: 100 + i as u64,
            time_ns: 1_700_000_000_000_000_000 + i as i64,
            op: Some(op),
        });
    }
    let (bytes, _) = b.finish();
    let (_, seg) = Segment::decode_metadata(&bytes).unwrap();
    assert_eq!(seg.records.len(), 15, "every op round-trips");
    assert_eq!(
        blake3::hash(&bytes).to_hex().as_str(),
        GOLDEN,
        "segment encoding changed: this is a format change"
    );
}

const GOLDEN: &str = "85328d95a466ac2411305702ecc626314b58d89c654a057af3d1958b6530b8c6";
