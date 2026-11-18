//! Turning a snapshot into another volume's genesis snapshot (design §13):
//! pruning paths, externalizing extents, resetting identity, and reading a
//! schema-1 file.

#![allow(clippy::cast_possible_truncation, clippy::unused_self)]

mod common;

use common::{Harness, INLINE_MAX};
use skyfs_format::wal::v1::XattrMode;
use skyfs_meta::{BaseVolume, Location, MetaError, MetaStore, ROOT_INO, SCHEMA_VERSION};

/// Bytes big enough to go to a blob, as a snapshot's data always is after
/// restore (inline data is repacked into packs).
fn blob_bytes(seed: u8) -> Vec<u8> {
    (0..INLINE_MAX * 2)
        .map(|i| (i as u8).wrapping_mul(7).wrapping_add(seed))
        .collect()
}

fn base(id: u8) -> BaseVolume {
    BaseVolume {
        volume: [id; 16],
        root: format!("published/volumes/{}", format!("{id:02x}").repeat(16)),
    }
}

/// keep/a (also hard-linked as out/a2), out/b (xattr), out/sub/c.
fn sample() -> (Harness, MetaStore) {
    let mut h = Harness::new();
    let keep = h.mkdir(ROOT_INO, "keep").unwrap();
    let out = h.mkdir(ROOT_INO, "out").unwrap();
    let a = h.create(keep, "a").unwrap();
    h.write(a, 0, &blob_bytes(1)).unwrap();
    h.link(a, out, "a2").unwrap();
    let b = h.create(out, "b").unwrap();
    h.write(b, 0, &blob_bytes(2)).unwrap();
    h.set_xattr(b, "user.k", b"v", XattrMode::Unspecified)
        .unwrap();
    let sub = h.mkdir(out, "sub").unwrap();
    let c = h.create(sub, "c").unwrap();
    h.write(c, 0, &blob_bytes(3)).unwrap();
    h.close_all();
    h.seal();
    let store = h.replay();
    (h, store)
}

#[test]
fn retain_paths_drops_subtrees_and_keeps_surviving_hard_links() {
    let (h, mut store) = sample();
    let a = h.resolve("keep/a").unwrap();
    assert_eq!(store.getattr(a).unwrap().unwrap().nlink, 2);
    let mut seen = Vec::new();
    let pruned = store
        .retain_paths(&mut |p, _| {
            seen.push(String::from_utf8_lossy(p).into_owned());
            p != b"out"
        })
        .unwrap();
    // A rejected directory is not walked into.
    assert!(seen.contains(&"keep/a".to_owned()));
    assert!(!seen.iter().any(|p| p.starts_with("out/")));
    let tree: Vec<String> = h.tree(&store).into_keys().collect();
    assert_eq!(tree, ["/keep", "/keep/a"]);
    // Entries: out, out/a2, out/b, out/sub, out/sub/c. Inodes: out, b, sub, c.
    assert_eq!((pruned.entries, pruned.inodes), (5, 4));
    assert_eq!(store.getattr(a).unwrap().unwrap().nlink, 1);
    assert_eq!(store.getattr(ROOT_INO).unwrap().unwrap().nlink, 3);
    assert_eq!(h.read(&store, a), blob_bytes(1));
    assert!(
        store
            .getattr(h.resolve("out/b").unwrap())
            .unwrap()
            .is_none()
    );
    assert_eq!(store.fsck().unwrap(), Vec::<String>::new());
}

#[test]
fn keeping_everything_changes_nothing() {
    let (h, mut store) = sample();
    let before = store.dump().unwrap();
    let pruned = store.retain_paths(&mut |_, _| true).unwrap();
    assert_eq!((pruned.entries, pruned.inodes), (0, 0));
    assert_eq!(store.dump().unwrap(), before);
    drop(h);
}

#[test]
fn externalize_then_genesis_makes_a_clone_snapshot() {
    let (h, mut store) = sample();
    let a = h.resolve("keep/a").unwrap();
    let own_before = store.referenced_objects().unwrap();
    assert!(!own_before.is_empty());

    let moved = store.externalize(&base(7)).unwrap();
    assert!(moved >= 3);
    for e in store.extents(a, 0, u64::MAX).unwrap() {
        assert!(
            matches!(&e.loc, Location::External { volume, key } if *volume == [7; 16] && own_before.contains(key)),
            "{e:?}"
        );
    }
    // No own objects left: this volume's GC has nothing of the base's to take.
    assert!(store.referenced_objects().unwrap().is_empty());
    // References name the owning volume, never a chain: a second base
    // adopts nothing that already belongs to the first.
    assert_eq!(store.externalize(&base(8)).unwrap(), 0);
    assert_eq!(store.bases().unwrap(), [base(7), base(8)]);

    let digest = [0xd1; 32];
    store.reset_genesis(&[0x42; 16], &digest).unwrap();
    assert_eq!(store.volume().unwrap().unwrap().volume_id, [0x42; 16]);
    let pos = store.position().unwrap();
    assert_eq!(pos.applied_seq, Some(0));
    assert_eq!(pos.applied_digest, digest);
    assert_eq!((pos.applied_epoch, pos.epoch, pos.clean), (0, 0, true));
    // Bases no extent uses are dropped at genesis.
    assert_eq!(store.bases().unwrap(), [base(7)]);
    assert_eq!(h.read(&store, a), blob_bytes(1));
    assert_eq!(store.fsck().unwrap(), Vec::<String>::new());
}

#[test]
fn only_snapshots_seed_a_volume() {
    let mut h = Harness::new();
    let f = h.create(ROOT_INO, "small").unwrap();
    h.write(f, 0, b"inline in a segment").unwrap();
    h.close_all();
    h.seal();
    let mut store = h.replay();
    assert!(matches!(
        store.retain_paths(&mut |_, _| true),
        Err(MetaError::Invalid(_))
    ));
    assert!(matches!(
        store.externalize(&base(1)),
        Err(MetaError::Invalid(_))
    ));
}

#[test]
fn fsck_reports_an_external_extent_without_its_base() {
    let (_h, mut store) = sample();
    store.externalize(&base(7)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("s.db");
    store.export_snapshot(&file).unwrap();
    let conn = rusqlite::Connection::open(&file).unwrap();
    conn.execute("DELETE FROM bases", []).unwrap();
    drop(conn);
    let reopened = MetaStore::open(&file).unwrap();
    let problems = reopened.fsck().unwrap();
    assert!(
        problems.iter().any(|p| p.contains("base volume")),
        "{problems:?}"
    );
}

#[test]
fn schema_1_files_are_migrated_on_open() {
    let (h, store) = sample();
    let a = h.resolve("keep/a").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("v1.db");
    store.export_snapshot(&file).unwrap();
    // Turn it into exactly what a version-1 build wrote.
    let conn = rusqlite::Connection::open(&file).unwrap();
    conn.execute_batch(
        "ALTER TABLE extents DROP COLUMN vol;
         DROP TABLE bases;
         UPDATE meta SET v = 1 WHERE k = 'schema_version';",
    )
    .unwrap();
    drop(conn);

    let migrated = MetaStore::open(&file).unwrap();
    assert_eq!(h.read(&migrated, a), blob_bytes(1));
    assert!(migrated.bases().unwrap().is_empty());
    assert_eq!(migrated.fsck().unwrap(), Vec::<String>::new());
    drop(migrated);
    let conn = rusqlite::Connection::open(&file).unwrap();
    let v: u32 = conn
        .query_row("SELECT v FROM meta WHERE k = 'schema_version'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(v, SCHEMA_VERSION);
}
