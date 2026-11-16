//! One scenario per row of design §9.1: each operation mountpoint-s3 cannot
//! do is logged, replayed from encoded segments on a fresh store, and
//! checked against the expected tree and bytes.

#![allow(
    clippy::many_single_char_names,
    clippy::unused_self,
    clippy::cast_possible_truncation
)]

mod common;

use common::Harness;
use skyfs_format::wal::v1::{FallocateMode, RenameMode, XattrMode};
use skyfs_meta::plan::AttrChanges;
use skyfs_meta::{Errno, Kind, ROOT_INO};

#[test]
fn partial_write_at_random_offset() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    h.write(f, 0, &[b'a'; 100]).unwrap();
    h.write(f, 40, b"XYZ").unwrap();
    h.write(f, 200, b"tail").unwrap(); // leaves a hole [100, 200)
    h.check();
    let replayed = h.replay();
    let bytes = h.read(&replayed, f);
    assert_eq!(bytes.len(), 204);
    assert_eq!(&bytes[38..45], b"aaXYZaa");
    assert!(bytes[100..200].iter().all(|&b| b == 0));
}

#[test]
fn in_place_edit_splits_extents_and_keeps_chunk_crcs() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "db").unwrap();
    h.write(f, 0, &[1; 4096]).unwrap();
    h.seal();
    h.write(f, 1000, &[2; 8]).unwrap(); // the 8-byte patch of §9.1
    h.check();
    // Three extents: head of the first chunk, the patch, tail of the first.
    let ext = h.replay().extents(f, 0, 4096).unwrap();
    assert_eq!(ext.len(), 3);
    assert_eq!((ext[2].skip, ext[2].chunk_len), (1008, 4096));
}

#[test]
fn append_grows_file() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "train.log").unwrap();
    for i in 0..10u8 {
        let size = h.live.getattr(f).unwrap().unwrap().size;
        h.write(f, size, &[i; 512]).unwrap();
    }
    h.check();
    assert_eq!(h.replay().getattr(f).unwrap().unwrap().size, 5120);
}

#[test]
fn overwrite_with_o_trunc() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    h.write(f, 0, b"old content that is long").unwrap();
    h.truncate(f, 0).unwrap();
    h.write(f, 0, b"new").unwrap();
    h.check();
    assert_eq!(h.read(&h.replay(), f), b"new");
}

#[test]
fn truncate_and_sparse_files() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    h.write(f, 0, &[7; 3000]).unwrap(); // a blob write
    h.truncate(f, 1500).unwrap();
    h.truncate(f, 1 << 20).unwrap(); // grows with a hole
    h.fallocate(f, 100, 200, FallocateMode::PunchHole).unwrap();
    h.check();
    let bytes = h.read(&h.replay(), f);
    assert_eq!(bytes.len(), 1 << 20);
    assert!(bytes[100..300].iter().all(|&b| b == 0));
    assert!(bytes[300..1500].iter().all(|&b| b == 7));
}

#[test]
fn rename_file_and_directory_with_contents() {
    let mut h = Harness::new();
    let tmp = h.mkdir(ROOT_INO, "run-tmp").unwrap();
    for i in 0..50 {
        let f = h.create(tmp, &format!("part-{i}")).unwrap();
        h.write(f, 0, format!("{i}").as_bytes()).unwrap();
    }
    let other = h.mkdir(ROOT_INO, "out").unwrap();
    h.rename(
        ROOT_INO,
        "run-tmp",
        other,
        "run-17",
        RenameMode::Unspecified,
    )
    .unwrap();
    h.check();
    let r = h.replay();
    let tree = h.tree(&r);
    assert!(tree.contains_key("/out/run-17/part-49"));
    assert!(!tree.contains_key("/run-tmp"));
    assert_eq!(r.parent(tmp).unwrap(), Some(other));
    assert_eq!(r.getattr(other).unwrap().unwrap().nlink, 3);
    assert_eq!(r.getattr(ROOT_INO).unwrap().unwrap().nlink, 3);
}

#[test]
fn replace_by_rename_frees_the_old_file() {
    let mut h = Harness::new();
    let old = h.create(ROOT_INO, "notes.txt").unwrap();
    h.write(old, 0, b"v1").unwrap();
    let swp = h.create(ROOT_INO, ".notes.txt.swp").unwrap();
    h.write(swp, 0, b"v2").unwrap();
    h.rename(
        ROOT_INO,
        ".notes.txt.swp",
        ROOT_INO,
        "notes.txt",
        RenameMode::Unspecified,
    )
    .unwrap();
    h.reclaim(old);
    h.check();
    let r = h.replay();
    assert_eq!(r.lookup(ROOT_INO, b"notes.txt").unwrap(), Some(swp));
    assert!(r.getattr(old).unwrap().is_none());
    assert_eq!(h.read(&r, swp), b"v2");
}

#[test]
fn rename_noreplace_and_exchange() {
    let mut h = Harness::new();
    let a = h.mkdir(ROOT_INO, "a").unwrap();
    let b = h.create(ROOT_INO, "b").unwrap();
    assert_eq!(
        h.rename(ROOT_INO, "a", ROOT_INO, "b", RenameMode::Noreplace),
        Err(Errno::EEXIST)
    );
    let sub = h.mkdir(ROOT_INO, "sub").unwrap();
    h.rename(ROOT_INO, "a", sub, "b", RenameMode::Noreplace)
        .unwrap();
    h.create(sub, "x").unwrap();
    // Swap a directory in `sub` with a file in the root.
    h.rename(sub, "b", ROOT_INO, "b", RenameMode::Exchange)
        .unwrap();
    h.check();
    let r = h.replay();
    assert_eq!(r.lookup(ROOT_INO, b"b").unwrap(), Some(a));
    assert_eq!(r.lookup(sub, b"b").unwrap(), Some(b));
    assert_eq!(r.parent(a).unwrap(), Some(ROOT_INO));
    assert_eq!(r.getattr(sub).unwrap().unwrap().nlink, 2);
}

#[test]
fn rename_errors() {
    let mut h = Harness::new();
    let d = h.mkdir(ROOT_INO, "d").unwrap();
    let e = h.mkdir(d, "e").unwrap();
    h.create(e, "f").unwrap();
    h.create(ROOT_INO, "g").unwrap();
    let rn = |h: &mut Harness, a, an, b, bn| h.rename(a, an, b, bn, RenameMode::Unspecified);
    assert_eq!(rn(&mut h, ROOT_INO, "d", e, "x"), Err(Errno::EINVAL));
    assert_eq!(rn(&mut h, ROOT_INO, "g", d, "e"), Err(Errno::EISDIR));
    assert_eq!(rn(&mut h, d, "e", ROOT_INO, "g"), Err(Errno::ENOTDIR));
    h.mkdir(ROOT_INO, "full").unwrap();
    assert_eq!(rn(&mut h, ROOT_INO, "full", d, "e"), Err(Errno::ENOTEMPTY));
    assert_eq!(rn(&mut h, ROOT_INO, "nope", d, "x"), Err(Errno::ENOENT));
    assert_eq!(
        h.rename(ROOT_INO, "g", ROOT_INO, "nope", RenameMode::Exchange),
        Err(Errno::ENOENT)
    );
    h.check();
}

#[test]
fn rmdir_and_unlink() {
    let mut h = Harness::new();
    let d = h.mkdir(ROOT_INO, "d").unwrap();
    h.create(d, "f").unwrap();
    assert_eq!(h.remove(ROOT_INO, "d", true), Err(Errno::ENOTEMPTY));
    assert_eq!(h.remove(ROOT_INO, "d", false), Err(Errno::EISDIR));
    assert_eq!(h.remove(d, "f", true), Err(Errno::ENOTDIR));
    h.unlink_closed(d, "f", false).unwrap();
    h.unlink_closed(ROOT_INO, "d", true).unwrap();
    h.check();
    assert!(h.tree(&h.replay()).is_empty());
    assert_eq!(h.replay().getattr(ROOT_INO).unwrap().unwrap().nlink, 2);
}

#[test]
fn hard_links_and_symlinks() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    h.write(f, 0, b"shared").unwrap();
    let d = h.mkdir(ROOT_INO, "d").unwrap();
    h.link(f, d, "g").unwrap();
    assert_eq!(h.link(d, ROOT_INO, "dl"), Err(Errno::EPERM));
    let s = h.make(ROOT_INO, "s", Kind::Symlink, "d/g").unwrap();
    h.unlink_closed(ROOT_INO, "f", false).unwrap(); // one link remains: not reclaimed
    h.check();
    let r = h.replay();
    assert_eq!(r.getattr(f).unwrap().unwrap().nlink, 1);
    assert_eq!(h.read(&r, f), b"shared");
    assert_eq!(r.readlink(s).unwrap().unwrap(), b"d/g");
    assert_eq!(r.getattr(s).unwrap().unwrap().size, 3);
    assert_eq!(r.getattr(s).unwrap().unwrap().mode, 0o777);
}

#[test]
fn chmod_chown_utimens() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    h.set_attr(
        f,
        &AttrChanges {
            mode: Some(0o4755),
            ..Default::default()
        },
    )
    .unwrap();
    // chown clears the setuid bit.
    h.set_attr(
        f,
        &AttrChanges {
            uid: Some(7),
            gid: Some(8),
            atime_ns: Some(5),
            mtime_ns: Some(6),
            ..Default::default()
        },
    )
    .unwrap();
    h.check();
    let a = h.replay().getattr(f).unwrap().unwrap();
    assert_eq!(
        (a.mode, a.uid, a.gid, a.atime_ns, a.mtime_ns),
        (0o755, 7, 8, 5, 6)
    );
}

#[test]
fn xattrs() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    h.set_xattr(f, "user.a", b"1", XattrMode::Create).unwrap();
    assert_eq!(
        h.set_xattr(f, "user.a", b"2", XattrMode::Create),
        Err(Errno::EEXIST)
    );
    assert_eq!(
        h.set_xattr(f, "user.b", b"2", XattrMode::Replace),
        Err(Errno::ENODATA)
    );
    h.set_xattr(f, "user.a", b"3", XattrMode::Replace).unwrap();
    h.set_xattr(f, "system.posix_acl_access", b"acl", XattrMode::Unspecified)
        .unwrap();
    h.set_xattr(f, "user.c", b"", XattrMode::Unspecified)
        .unwrap();
    h.remove_xattr(f, "user.c").unwrap();
    assert_eq!(h.remove_xattr(f, "user.c"), Err(Errno::ENODATA));
    h.check();
    let r = h.replay();
    assert_eq!(r.getxattr(f, b"user.a").unwrap().unwrap(), b"3");
    assert_eq!(r.listxattr(f).unwrap().len(), 2);
}

#[test]
fn unlink_while_open_then_reclaim_or_remount() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    h.write(f, 0, b"still readable").unwrap();
    h.remove(ROOT_INO, "f", false).unwrap();
    h.check();
    // Still alive, data intact, while "open".
    assert_eq!(h.read(&h.replay(), f), b"still readable");
    assert_eq!(h.replay().orphans().unwrap(), vec![f]);
    // A crash and remount frees it through MountBegin.
    h.remount();
    h.check();
    assert!(h.replay().getattr(f).unwrap().is_none());
}

#[test]
fn special_files() {
    let mut h = Harness::new();
    for (name, kind) in [
        ("fifo", Kind::Fifo),
        ("sock", Kind::Socket),
        ("chr", Kind::CharDevice),
    ] {
        h.make(ROOT_INO, name, kind, "").unwrap();
    }
    h.check();
    assert_eq!(h.tree(&h.replay()).len(), 3);
}

#[test]
fn fallocate_collapse_insert_and_clone() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    let data: Vec<u8> = (0..=255).cycle().take(4000).collect();
    h.write(f, 0, &data[..900]).unwrap();
    h.write(f, 900, &data[900..]).unwrap();
    h.fallocate(f, 500, 1000, FallocateMode::CollapseRange)
        .unwrap();
    h.fallocate(f, 100, 64, FallocateMode::InsertRange).unwrap();
    h.fallocate(f, 2000, 5000, FallocateMode::Unspecified)
        .unwrap();
    h.fallocate(f, 0, 10, FallocateMode::ZeroRangeKeepSize)
        .unwrap();
    assert_eq!(
        h.fallocate(f, 6000, 2000, FallocateMode::CollapseRange),
        Err(Errno::EINVAL)
    );
    let g = h.create(ROOT_INO, "g").unwrap();
    h.clone_range(f, 50, g, 10, 3000).unwrap();
    h.clone_range(g, 0, f, 5000, 100).unwrap();
    h.check();
}

/// The worked example of design §9.1.
#[test]
fn worked_example() {
    let mut h = Harness::new();
    let notes = h.create(ROOT_INO, "notes.txt").unwrap();
    h.write(notes, 0, b"old").unwrap();
    let log = h.create(ROOT_INO, "train.log").unwrap();
    h.write(log, 0, &[0; 1000]).unwrap();
    let db = h.create(ROOT_INO, "db").unwrap();
    h.write(db, 0, &[1; 8192]).unwrap();
    let run = h.mkdir(ROOT_INO, "run-tmp").unwrap();
    for i in 0..100 {
        h.create(run, &format!("{i}")).unwrap();
    }
    let ckpt = h.create(ROOT_INO, "old.ckpt").unwrap();
    h.seal();

    let swp = h.create(ROOT_INO, ".notes.txt.swp").unwrap();
    h.write(swp, 0, &[b'n'; 2048]).unwrap();
    h.rename(
        ROOT_INO,
        ".notes.txt.swp",
        ROOT_INO,
        "notes.txt",
        RenameMode::Unspecified,
    )
    .unwrap();
    h.reclaim(notes);
    h.write(log, 1000, &[2; 512]).unwrap();
    h.write(db, 4096, &[3; 8]).unwrap();
    h.rename(
        ROOT_INO,
        "run-tmp",
        ROOT_INO,
        "run-17",
        RenameMode::Unspecified,
    )
    .unwrap();
    h.unlink_closed(ROOT_INO, "old.ckpt", false).unwrap();
    h.check();

    let r = h.replay();
    assert_eq!(r.lookup(ROOT_INO, b"notes.txt").unwrap(), Some(swp));
    assert!(r.getattr(notes).unwrap().is_none());
    assert!(r.getattr(ckpt).unwrap().is_none());
    assert_eq!(r.readdir(run, 0, 1000).unwrap().len(), 100);
    assert_eq!(r.getattr(log).unwrap().unwrap().size, 1512);
}

#[test]
fn readdir_cookies_are_stable_across_changes() {
    let mut h = Harness::new();
    for n in ["a", "b", "c", "d"] {
        h.create(ROOT_INO, n).unwrap();
    }
    let first = h.live.readdir(ROOT_INO, 0, 2).unwrap();
    let resume = first.last().unwrap().cookie;
    // Concurrent changes: remove an entry already returned, add a new one.
    h.unlink_closed(ROOT_INO, "a", false).unwrap();
    h.create(ROOT_INO, "e").unwrap();
    let rest: Vec<_> = h
        .live
        .readdir(ROOT_INO, resume, 10)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(rest, vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
    h.check();
}

#[test]
fn setgid_directories_pass_on_their_group() {
    let mut h = Harness::new();
    let d = h.mkdir(ROOT_INO, "shared").unwrap();
    h.set_attr(
        d,
        &AttrChanges {
            mode: Some(0o2775),
            gid: Some(500),
            ..Default::default()
        },
    )
    .unwrap();
    let f = h.create(d, "f").unwrap();
    let sub = h.mkdir(d, "sub").unwrap();
    h.check();
    let r = h.replay();
    assert_eq!(r.getattr(f).unwrap().unwrap().gid, 500);
    let s = r.getattr(sub).unwrap().unwrap();
    assert_eq!((s.gid, s.mode & 0o2000), (500, 0o2000));
}

#[test]
fn snapshot_plus_tail_equals_full_replay() {
    let mut h = Harness::new();
    let d = h.mkdir(ROOT_INO, "d").unwrap();
    let f = h.create(d, "f").unwrap();
    h.write(f, 0, b"before snapshot").unwrap();
    h.seal();
    let cut = h.segments.len();
    h.write(f, 7, b"AFTER").unwrap();
    h.rename(d, "f", ROOT_INO, "g", RenameMode::Unspecified)
        .unwrap();
    h.seal();

    // Replica through `cut`, exported with VACUUM INTO, reopened, then the tail.
    let dir = tempfile::tempdir().unwrap();
    let mut replica = skyfs_meta::MetaStore::in_memory().unwrap();
    common::replay_into(&mut replica, &h.segments[..cut], 0);
    let snap = dir.path().join("snap.db");
    replica.export_snapshot(&snap).unwrap();
    let mut mounted = skyfs_meta::MetaStore::open(&snap).unwrap();
    common::replay_into(&mut mounted, &h.segments, cut);

    assert_eq!(mounted.dump().unwrap(), h.live.dump().unwrap());
    assert_eq!(h.read(&mounted, f), b"before AFTERhot");
}

#[test]
fn corrupt_identity_refuses_replay() {
    use skyfs_format::wal::v1::{Record, Remove, record::Op};
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "f").unwrap();
    h.seal();
    let mut r = h.replay();
    let next = r.position().unwrap().next_op_id;
    let bad = Record {
        op_id: next,
        time_ns: 0,
        op: Some(Op::Remove(Remove {
            parent: ROOT_INO,
            name: b"f".to_vec(),
            ino: f + 1,
            is_dir: false,
        })),
    };
    assert!(matches!(
        r.apply(&bad, 99),
        Err(skyfs_meta::ApplyError::Corrupt { .. })
    ));
    // A skipped op id is corruption too.
    let gap = Record {
        op_id: next + 1,
        ..bad
    };
    assert!(r.apply(&gap, 99).is_err());
}
