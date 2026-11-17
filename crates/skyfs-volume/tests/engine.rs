//! End-to-end engine tests against an in-memory bucket. Separate local
//! directories stand in for separate machines.

#![allow(clippy::many_single_char_names, clippy::cast_possible_truncation)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use skyfs_format::wal::v1::RenameMode;
use skyfs_meta::plan::{Caller, MakeRequest};
use skyfs_meta::{Errno, Ino, Kind, ROOT_INO};
use skyfs_store::{Key, KeyClass, ObjectClient, VolumeRoot};
use skyfs_volume::{CreateOptions, Replication, Volume, VolumeConfig, VolumeError, create_volume};

const VID: [u8; 16] = [0x5a; 16];
const ROOT: Caller = Caller { uid: 0, gid: 0 };

struct Bucket {
    store: Arc<dyn ObjectStore>,
}

impl Bucket {
    fn new() -> Self {
        let b = Self {
            store: Arc::new(InMemory::new()),
        };
        let scratch = tempfile::tempdir().unwrap();
        let opts = CreateOptions {
            inline_data_max: 64 * 1024,
            ..CreateOptions::default()
        };
        create_volume(&b.client(), VID, &opts, scratch.path()).unwrap();
        b
    }

    fn client(&self) -> ObjectClient {
        ObjectClient::new(self.store.clone(), VolumeRoot::new("tenant", &VID))
    }

    fn list(&self, class: KeyClass) -> Vec<Key> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut keys: Vec<Key> = rt
            .block_on(self.client().list(class))
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        keys.sort_by_key(ToString::to_string);
        keys
    }

    fn exists(&self, key: &Key) -> bool {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(self.client().exists(key)).unwrap()
    }

    fn delete(&self, key: &Key) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(self.client().delete(key)).unwrap();
    }

    fn mount(&self, dir: &Path, f: impl FnOnce(&mut VolumeConfig)) -> Result<Volume, VolumeError> {
        let mut cfg = VolumeConfig::new(dir.to_path_buf());
        cfg.max_lag = Duration::from_millis(20);
        cfg.restore_after = Duration::from_hours(1);
        cfg.retention = Duration::ZERO;
        cfg.restore_on_exit_timeout = Duration::from_secs(20);
        f(&mut cfg);
        Volume::mount(self.client(), VID, cfg)
    }
}

fn mkdir(v: &Volume, parent: Ino, name: &str) -> Ino {
    let req = MakeRequest {
        parent,
        name: name.as_bytes(),
        kind: Kind::Directory,
        mode: 0o755,
        umask: 0,
        rdev: 0,
        target: b"",
    };
    v.finish(v.make(ROOT, &req).unwrap()).unwrap().ino
}

fn write_file(v: &Volume, parent: Ino, name: &str, bytes: &[u8], chunk: usize) -> Ino {
    let (attr, fh) = v
        .finish(
            v.create(ROOT, parent, name.as_bytes(), 0o644, 0o022)
                .unwrap(),
        )
        .unwrap();
    for (i, c) in bytes.chunks(chunk).enumerate() {
        assert_eq!(
            v.write(fh, (i * chunk) as u64, c).unwrap() as usize,
            c.len()
        );
    }
    v.finish(v.flush(fh).unwrap()).unwrap();
    v.release(fh).unwrap();
    attr.ino
}

fn read_all(v: &Volume, ino: Ino) -> Vec<u8> {
    let size = v.getattr(ino).unwrap().size;
    let mut out = Vec::new();
    while (out.len() as u64) < size {
        let chunk = v.read(ino, out.len() as u64, 1 << 20).unwrap();
        assert!(!chunk.is_empty());
        out.extend(chunk);
    }
    out
}

fn resolve(v: &Volume, path: &str) -> Result<Ino, Errno> {
    let mut ino = ROOT_INO;
    for part in path.split('/').filter(|p| !p.is_empty()) {
        ino = v.lookup(ino, part.as_bytes())?.ino;
    }
    Ok(ino)
}

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[test]
fn clean_unmount_then_mount_elsewhere() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let v = b.mount(m1.path(), |_| {}).unwrap();
    assert_eq!(v.report().epoch, 1);
    let d = mkdir(&v, ROOT_INO, "out");
    let f = write_file(&v, d, "a.txt", b"hello skyfs", 4);
    v.finish(
        v.rename(d, b"a.txt", ROOT_INO, b"b.txt", RenameMode::Unspecified)
            .unwrap(),
    )
    .unwrap();
    v.finish(
        v.setxattr(
            f,
            b"user.k",
            b"v",
            skyfs_format::wal::v1::XattrMode::Unspecified,
        )
        .unwrap(),
    )
    .unwrap();
    let report = v.unmount().unwrap();
    assert!(report.fully_restored, "restore at exit reaches the head");

    // Another machine: empty local dir, everything from the bucket.
    let m2 = tempfile::tempdir().unwrap();
    let v = b.mount(m2.path(), |_| {}).unwrap();
    assert!(v.report().previous_clean);
    assert_eq!(
        v.report().tail_segments,
        0,
        "the snapshot covers the whole log"
    );
    assert_eq!(v.report().epoch, 2);
    let f2 = resolve(&v, "b.txt").unwrap();
    assert_eq!(f2, f, "inode numbers are stable across mounts");
    assert_eq!(read_all(&v, f2), b"hello skyfs");
    assert_eq!(v.getxattr(f2, b"user.k").unwrap(), b"v");
    assert!(resolve(&v, "out/a.txt").is_err());
    // Inline data now lives in packs, not segments.
    assert!(!b.list(KeyClass::Packs).is_empty());
    v.unmount().unwrap();
}

#[test]
fn large_writes_go_to_blobs_and_read_back_remotely() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let v = b.mount(m1.path(), |c| c.blob_max = 1 << 20).unwrap();
    let data = pattern(3 << 20, 7);
    let f = write_file(&v, ROOT_INO, "model.bin", &data, 128 * 1024);
    // In-place patch inside the blob-backed region.
    let fh = v.open(f).unwrap();
    v.write(fh, 2_000_000, b"PATCH").unwrap();
    v.release(fh).unwrap();
    v.finish(v.fsync().unwrap()).unwrap();
    let blobs = b.list(KeyClass::Blobs);
    assert!(
        blobs.len() >= 2,
        "a 3 MiB sequential write with 1 MiB blobs: {blobs:?}"
    );
    drop(v); // crash after fsync: everything acknowledged is in the bucket

    let m2 = tempfile::tempdir().unwrap();
    let v = b.mount(m2.path(), |_| {}).unwrap();
    let mut expect = data;
    expect[2_000_000..2_000_005].copy_from_slice(b"PATCH");
    assert_eq!(read_all(&v, f), expect);
    v.unmount().unwrap();
}

#[test]
fn crash_on_same_disk_recovers_the_local_wal() {
    let b = Bucket::new();
    let m = tempfile::tempdir().unwrap();
    // Nothing is uploaded on its own for an hour.
    let v = b
        .mount(m.path(), |c| c.max_lag = Duration::from_hours(1))
        .unwrap();
    let f = write_file(&v, ROOT_INO, "notes", b"unreplicated", 5);
    mkdir(&v, ROOT_INO, "d");
    drop(v); // crash: the local WAL holds the ops, the bucket does not

    let v = b.mount(m.path(), |_| {}).unwrap();
    let r = v.report().clone();
    assert!(r.recovered_segments >= 1, "{r:?}");
    assert_eq!(r.lost_segments, 0);
    assert!(!r.previous_clean);
    assert_eq!(read_all(&v, f), b"unreplicated");
    assert!(resolve(&v, "d").is_ok());
    v.unmount().unwrap();
}

#[test]
fn machine_loss_in_async_mode_loses_only_the_unreplicated_tail() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let v = b
        .mount(m1.path(), |c| c.max_lag = Duration::from_hours(1))
        .unwrap();
    let kept = write_file(&v, ROOT_INO, "kept", b"durable", 100);
    v.finish(v.fsync().unwrap()).unwrap();
    write_file(&v, ROOT_INO, "lost", b"in flight", 100);
    drop(v);

    let m2 = tempfile::tempdir().unwrap();
    let v = b.mount(m2.path(), |_| {}).unwrap();
    assert!(!v.report().previous_clean);
    assert_eq!(read_all(&v, kept), b"durable");
    assert_eq!(resolve(&v, "lost"), Err(Errno::ENOENT));
    v.unmount().unwrap();
}

#[test]
fn sync_mode_acknowledges_after_commit() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let v = b
        .mount(m1.path(), |c| {
            c.replication = Replication::Sync;
            c.max_lag = Duration::from_hours(1);
        })
        .unwrap();
    let req = MakeRequest {
        parent: ROOT_INO,
        name: b"d",
        kind: Kind::Directory,
        mode: 0o755,
        umask: 0,
        rdev: 0,
        target: b"",
    };
    let done = v.make(ROOT, &req).unwrap();
    let seq = done.wait.expect("sync mode waits for namespace ops");
    v.finish(done).unwrap();
    assert!(b.exists(&Key::Segment(seq)));
    assert!(v.commit_state().committed >= Some(seq));
    drop(v);

    let m2 = tempfile::tempdir().unwrap();
    let v = b.mount(m2.path(), |_| {}).unwrap();
    assert!(resolve(&v, "d").is_ok());
    v.unmount().unwrap();
}

#[test]
fn a_new_mount_fences_the_old_writer() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let old = b.mount(m1.path(), |c| c.restorer = false).unwrap();
    let m2 = tempfile::tempdir().unwrap();
    let new = b.mount(m2.path(), |c| c.restorer = false).unwrap();
    assert_eq!(new.report().epoch, 2);

    // The old writer's next commit collides with the new MountBegin's seq.
    mkdir(&old, ROOT_INO, "stale");
    let r = old.finish(old.fsync().unwrap());
    assert_eq!(r, Err(Errno::EIO));
    assert!(old.commit_state().fenced);
    let req = MakeRequest {
        parent: ROOT_INO,
        name: b"again",
        kind: Kind::Directory,
        mode: 0o755,
        umask: 0,
        rdev: 0,
        target: b"",
    };
    assert_eq!(old.make(ROOT, &req).unwrap_err(), Errno::EIO);
    drop(old);

    mkdir(&new, ROOT_INO, "fresh");
    new.unmount().unwrap();
    let m3 = tempfile::tempdir().unwrap();
    let v = b.mount(m3.path(), |_| {}).unwrap();
    assert!(resolve(&v, "fresh").is_ok());
    assert_eq!(
        resolve(&v, "stale"),
        Err(Errno::ENOENT),
        "the new history wins"
    );
    v.unmount().unwrap();
}

#[test]
fn restore_passes_move_the_pointer_and_truncate_the_log() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let v = b.mount(m1.path(), |_| {}).unwrap();
    let f = write_file(&v, ROOT_INO, "f", &pattern(200_000, 1), 50_000);
    // Overwrite most of it so the first pack becomes sparse later.
    let fh = v.open(f).unwrap();
    v.write(fh, 0, &pattern(150_000, 2)).unwrap();
    v.release(fh).unwrap();
    v.unmount().unwrap();
    assert!(b.exists(&Key::SnapshotManifest(1)));

    // The next mount bases on the new snapshot; its restore pass may now
    // drop the log the old base needed.
    let m2 = tempfile::tempdir().unwrap();
    let v = b.mount(m2.path(), |_| {}).unwrap();
    write_file(&v, ROOT_INO, "g", b"more", 4);
    v.unmount().unwrap();
    assert!(
        !b.exists(&Key::Segment(0)),
        "segments below every base are truncated"
    );

    let m3 = tempfile::tempdir().unwrap();
    let v = b.mount(m3.path(), |_| {}).unwrap();
    let mut expect = pattern(200_000, 1);
    expect[..150_000].copy_from_slice(&pattern(150_000, 2));
    assert_eq!(read_all(&v, f), expect);
    v.unmount().unwrap();
}

#[test]
fn a_broken_hash_chain_refuses_the_mount() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let v = b.mount(m1.path(), |c| c.restorer = false).unwrap();
    for i in 0..3 {
        mkdir(&v, ROOT_INO, &format!("d{i}"));
        v.finish(v.fsync().unwrap()).unwrap();
    }
    v.unmount().unwrap();
    let segs = b.list(KeyClass::Segments);
    assert!(segs.len() >= 4);
    b.delete(&segs[2]);

    let m2 = tempfile::tempdir().unwrap();
    // The head probe stops at the gap; the mount must not silently lose the
    // later segments, and it must not skip one either.
    match b.mount(m2.path(), |_| {}) {
        Err(VolumeError::Corrupt(_) | VolumeError::Fenced(_)) => {}
        Ok(v) => {
            // A gap at the end of the probe is indistinguishable from a
            // shorter log only if nothing follows; here something does.
            let st = v.report().clone();
            panic!("mounted across a gap: {st:?}");
        }
        Err(e) => panic!("unexpected error {e}"),
    }
}

#[test]
fn unlink_while_open_keeps_data_until_close() {
    let b = Bucket::new();
    let m = tempfile::tempdir().unwrap();
    let v = b.mount(m.path(), |_| {}).unwrap();
    let f = write_file(&v, ROOT_INO, "tmp", b"scratch", 7);
    let fh = v.open(f).unwrap();
    v.finish(v.remove(ROOT_INO, b"tmp", false).unwrap())
        .unwrap();
    assert_eq!(read_all(&v, f), b"scratch");
    assert_eq!(v.getattr(f).unwrap().nlink, 0);
    v.release(fh).unwrap();
    assert_eq!(v.getattr(f), Err(Errno::ENOENT));
    v.unmount().unwrap();
}

#[test]
fn names_longer_than_name_max_are_too_long() {
    let b = Bucket::new();
    let m = tempfile::tempdir().unwrap();
    let v = b.mount(m.path(), |_| {}).unwrap();
    let max = "x".repeat(255);
    write_file(&v, ROOT_INO, &max, b"ok", 2);
    assert!(v.lookup(ROOT_INO, max.as_bytes()).is_ok());
    let long = "x".repeat(256);
    assert_eq!(
        v.lookup(ROOT_INO, long.as_bytes()),
        Err(Errno::ENAMETOOLONG)
    );
    assert_eq!(
        v.create(ROOT, ROOT_INO, long.as_bytes(), 0o644, 0)
            .unwrap_err(),
        Errno::ENAMETOOLONG
    );
    v.unmount().unwrap();
}

#[test]
fn relatime_updates_atime_once_per_change_and_replays() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let v = b.mount(m1.path(), |_| {}).unwrap();
    let f = write_file(&v, ROOT_INO, "f", b"data", 4);
    let before = v.getattr(f).unwrap();
    assert!(before.atime_ns <= before.mtime_ns);

    // The first read after a change moves atime, and only atime.
    read_all(&v, f);
    let read1 = v.getattr(f).unwrap();
    assert!(read1.atime_ns > before.mtime_ns);
    assert_eq!(
        (read1.mtime_ns, read1.ctime_ns),
        (before.mtime_ns, before.ctime_ns)
    );
    // Later reads leave it until the file changes again.
    read_all(&v, f);
    assert_eq!(v.getattr(f).unwrap().atime_ns, read1.atime_ns);

    // readdir from the start and readlink are accesses too.
    let dir = v.getattr(ROOT_INO).unwrap();
    v.readdir(ROOT_INO, 2, 16).unwrap();
    assert!(v.getattr(ROOT_INO).unwrap().atime_ns > dir.atime_ns);
    v.unmount().unwrap();

    // Replay elsewhere gives the same atime, from the log.
    let m2 = tempfile::tempdir().unwrap();
    let v = b.mount(m2.path(), |_| {}).unwrap();
    assert_eq!(v.getattr(f).unwrap(), read1);
    v.unmount().unwrap();
}

#[test]
fn noatime_never_logs_an_access() {
    let b = Bucket::new();
    let m = tempfile::tempdir().unwrap();
    let v = b
        .mount(m.path(), |c| c.atime = skyfs_volume::AtimeMode::NoAtime)
        .unwrap();
    let f = write_file(&v, ROOT_INO, "f", b"data", 4);
    let before = v.getattr(f).unwrap();
    read_all(&v, f);
    v.readdir(ROOT_INO, 2, 16).unwrap();
    assert_eq!(v.getattr(f).unwrap(), before);
    v.unmount().unwrap();
}

#[test]
fn one_process_per_local_directory() {
    let b = Bucket::new();
    let m = tempfile::tempdir().unwrap();
    let v = b.mount(m.path(), |_| {}).unwrap();
    let second = b.mount(m.path(), |c| c.local_lock_wait = Duration::from_millis(300));
    assert!(matches!(second, Err(VolumeError::Busy(_))), "{second:?}");
    v.unmount().unwrap();
    // Released at unmount.
    b.mount(m.path(), |_| {}).unwrap().unmount().unwrap();
}

#[test]
fn an_open_for_reading_is_an_access() {
    let b = Bucket::new();
    let m = tempfile::tempdir().unwrap();
    let v = b.mount(m.path(), |_| {}).unwrap();
    let f = write_file(&v, ROOT_INO, "f", b"data", 4);
    let before = v.getattr(f).unwrap();
    // Reads may come from the kernel's page cache; the open is seen.
    assert!(v.access(f).unwrap(), "atime moved");
    assert!(v.getattr(f).unwrap().atime_ns > before.atime_ns);
    assert!(!v.access(f).unwrap(), "relatime: once per change");
    // Directories and symlinks are accessed by readdir and readlink.
    assert!(!v.access(ROOT_INO).unwrap());
    v.unmount().unwrap();

    let m2 = tempfile::tempdir().unwrap();
    let v = b
        .mount(m2.path(), |c| c.atime = skyfs_volume::AtimeMode::Strict)
        .unwrap();
    assert!(v.access(f).unwrap());
    std::thread::sleep(Duration::from_millis(2));
    assert!(v.access(f).unwrap(), "strictatime: every access");
    v.unmount().unwrap();
}

#[test]
fn a_long_backlog_is_restored_in_published_steps() {
    let b = Bucket::new();
    // A mount that never restores leaves a long tail: one segment per fsync.
    let m1 = tempfile::tempdir().unwrap();
    let v = b.mount(m1.path(), |c| c.restorer = false).unwrap();
    for i in 0..40 {
        write_file(&v, ROOT_INO, &format!("f{i}"), b"x", 1);
        v.finish(v.fsync().unwrap()).unwrap();
    }
    v.unmount().unwrap();
    assert!(!b.exists(&Key::SnapshotManifest(1)));

    // The next mount folds it at most 8 segments per pass, publishing each.
    let m2 = tempfile::tempdir().unwrap();
    let v = b.mount(m2.path(), |c| c.restore_batch = 8).unwrap();
    let report = v.unmount().unwrap();
    assert!(report.fully_restored);
    assert!(
        b.exists(&Key::SnapshotManifest(5)),
        "at least five steps for 40+ segments"
    );
    let m3 = tempfile::tempdir().unwrap();
    let v = b.mount(m3.path(), |_| {}).unwrap();
    assert!(v.report().tail_segments <= 2, "{:?}", v.report());
    assert!(v.lookup(ROOT_INO, b"f39").is_ok());
    v.unmount().unwrap();
}

#[test]
fn fallocate_beyond_the_reported_capacity_is_enospc() {
    use skyfs_format::wal::v1::FallocateMode;
    let b = Bucket::new();
    let m = tempfile::tempdir().unwrap();
    let v = b.mount(m.path(), |_| {}).unwrap();
    let f = write_file(&v, ROOT_INO, "f", b"x", 1);
    let free = v.statfs().unwrap().free();
    assert_eq!(
        v.fallocate(f, 0, free + 1, FallocateMode::Unspecified)
            .unwrap_err(),
        Errno::ENOSPC
    );
    // Punching a hole reserves nothing.
    let _ = v
        .fallocate(f, 0, free + 1, FallocateMode::PunchHole)
        .unwrap();
    let _ = v
        .fallocate(f, 0, 1 << 30, FallocateMode::Unspecified)
        .unwrap();
    v.unmount().unwrap();
}

#[test]
fn a_read_only_mount_serves_and_writes_nothing() {
    let b = Bucket::new();
    let m1 = tempfile::tempdir().unwrap();
    let v = b.mount(m1.path(), |_| {}).unwrap();
    let f = write_file(&v, ROOT_INO, "f", b"data", 4);
    v.unmount().unwrap();
    let segments = b.list(KeyClass::Segments);
    let snapshots = b.list(KeyClass::Snapshots);

    let m2 = tempfile::tempdir().unwrap();
    let v = b.mount(m2.path(), |c| c.read_only = true).unwrap();
    assert_eq!(read_all(&v, f), b"data");
    let before = v.getattr(f).unwrap();
    assert!(!v.access(f).unwrap(), "no atime on a read-only mount");
    assert_eq!(v.getattr(f).unwrap(), before);
    assert_eq!(
        v.create(ROOT, ROOT_INO, b"new", 0o644, 0).unwrap_err(),
        Errno::EROFS
    );
    v.unmount().unwrap();
    assert_eq!(b.list(KeyClass::Segments), segments, "no segment written");
    assert_eq!(b.list(KeyClass::Snapshots), snapshots);
}

#[test]
fn dot_finds_any_inode_for_file_handles() {
    let b = Bucket::new();
    let m = tempfile::tempdir().unwrap();
    let v = b.mount(m.path(), |_| {}).unwrap();
    let f = write_file(&v, ROOT_INO, "f", b"x", 1);
    assert_eq!(v.lookup(f, b".").unwrap().ino, f);
    assert_eq!(v.lookup(f, b"x"), Err(Errno::ENOTDIR));
    let _ = v.remove(ROOT_INO, b"f", false).unwrap();
    assert_eq!(v.lookup(f, b"."), Err(Errno::ENOENT), "stale once unlinked");
    v.unmount().unwrap();
}
