//! Published volumes and clones (design §13), end to end against an
//! in-memory bucket: publish copies the source's own objects and seals the
//! copy; a clone copies metadata only, reads through to its bases, writes
//! copy-on-write, and never collects or changes a base.

#![allow(clippy::cast_possible_truncation, clippy::many_single_char_names)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};
use skyfs_format::wal::v1::XattrMode;
use skyfs_meta::plan::{Caller, MakeRequest};
use skyfs_meta::{Errno, Ino, Kind, ROOT_INO};
use skyfs_store::faults::{Fault, FaultStore, Faults};
use skyfs_store::{Key, ObjectClient, VolumeRoot};
use skyfs_volume::{
    CreateOptions, Volume, VolumeConfig, VolumeError, clone_volume, create_volume, publish_volume,
};

const ROOT: Caller = Caller { uid: 0, gid: 0 };
const SRC: [u8; 16] = [0x51; 16];
const PUB: [u8; 16] = [0x70; 16];
const CLONE: [u8; 16] = [0xc1; 16];
const P2: [u8; 16] = [0x72; 16];
const C2: [u8; 16] = [0xc2; 16];

struct Bucket {
    store: Arc<dyn ObjectStore>,
}

impl Bucket {
    fn new() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
        }
    }

    fn client(&self, prefix: &str, id: [u8; 16]) -> ObjectClient {
        ObjectClient::new(self.store.clone(), VolumeRoot::new(prefix, &id))
    }

    fn create(&self, prefix: &str, id: [u8; 16]) -> ObjectClient {
        let c = self.client(prefix, id);
        let scratch = tempfile::tempdir().unwrap();
        let opts = CreateOptions {
            inline_data_max: 64 * 1024,
            ..CreateOptions::default()
        };
        create_volume(&c, id, &opts, scratch.path()).unwrap();
        c
    }

    /// Every object under `root`, as `key -> bytes`, to prove what changed.
    fn objects(&self, c: &ObjectClient) -> Vec<(String, Vec<u8>)> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let prefix = object_store::path::Path::from(c.root().to_string());
            let metas: Vec<_> = self.store.list(Some(&prefix)).try_collect().await.unwrap();
            let mut out = Vec::new();
            for m in metas {
                let bytes = self
                    .store
                    .get(&m.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                out.push((m.location.to_string(), bytes.to_vec()));
            }
            out.sort();
            out
        })
    }

    fn data_keys(&self, c: &ObjectClient) -> BTreeSet<String> {
        let root = c.root().to_string();
        self.objects(c)
            .into_iter()
            .map(|(k, _)| k.strip_prefix(&format!("{root}/")).unwrap().to_owned())
            .filter(|k| k.starts_with("blobs/") || k.starts_with("packs/"))
            .collect()
    }
}

fn mount(
    c: &ObjectClient,
    id: [u8; 16],
    dir: &Path,
    read_only: bool,
) -> Result<Volume, VolumeError> {
    let mut cfg = VolumeConfig::new(dir.to_path_buf());
    cfg.max_lag = Duration::from_millis(20);
    cfg.restore_after = Duration::from_hours(1);
    cfg.retention = Duration::ZERO;
    cfg.restore_on_exit_timeout = Duration::from_secs(20);
    cfg.read_only = read_only;
    Volume::mount(c.clone(), id, cfg)
}

fn exists(c: &ObjectClient, key: &Key) -> bool {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(c.exists(key))
        .unwrap()
}

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
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
        .finish(v.create(ROOT, parent, name.as_bytes(), 0o644, 0).unwrap())
        .unwrap();
    for (i, c) in bytes.chunks(chunk).enumerate() {
        v.write(fh, (i * chunk) as u64, c).unwrap();
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

fn read_path(v: &Volume, path: &str) -> Vec<u8> {
    read_all(v, resolve(v, path).unwrap())
}

const WORKFLOW: &[u8] = b"{\"nodes\": [1, 2, 3]}";

fn model() -> Vec<u8> {
    pattern(300_000, 9)
}

/// A workspace: a small workflow (repacked into a pack), a 300 KB model in
/// blobs with a hard link, a symlink, an xattr, and outputs to leave out.
fn source(b: &Bucket) -> ObjectClient {
    let src = b.create("tenant", SRC);
    let dir = tempfile::tempdir().unwrap();
    let v = mount(&src, SRC, dir.path(), false).unwrap();
    let wf = mkdir(&v, ROOT_INO, "workflows");
    let w = write_file(&v, wf, "w.json", WORKFLOW, 64);
    v.finish(
        v.setxattr(w, b"user.author", b"alice", XattrMode::Unspecified)
            .unwrap(),
    )
    .unwrap();
    let models = mkdir(&v, ROOT_INO, "models");
    let m = write_file(&v, models, "m.bin", &model(), 100_000);
    v.finish(v.link(m, models, b"m-link.bin").unwrap()).unwrap();
    let link = MakeRequest {
        parent: ROOT_INO,
        name: b"latest",
        kind: Kind::Symlink,
        mode: 0o777,
        umask: 0,
        rdev: 0,
        target: b"models/m.bin",
    };
    v.finish(v.make(ROOT, &link).unwrap()).unwrap();
    let out = mkdir(&v, ROOT_INO, "output");
    write_file(&v, out, "o.png", &pattern(200_000, 4), 100_000);
    let report = v.unmount().unwrap();
    assert!(report.fully_restored, "a clean unmount restores everything");
    src
}

fn publish(
    b: &Bucket,
    src: &ObjectClient,
    src_id: [u8; 16],
    dst_id: [u8; 16],
) -> (ObjectClient, skyfs_volume::Published) {
    let dst = b.client("published", dst_id);
    let scratch = tempfile::tempdir().unwrap();
    let mut keep = |p: &[u8], _| p != b"output";
    let p = publish_volume(src, src_id, &dst, dst_id, &mut keep, scratch.path()).unwrap();
    (dst, p)
}

fn clone(
    b: &Bucket,
    base: &ObjectClient,
    base_id: [u8; 16],
    dst_id: [u8; 16],
) -> (ObjectClient, skyfs_volume::Cloned) {
    let dst = b.client("tenant2", dst_id);
    let scratch = tempfile::tempdir().unwrap();
    let c = clone_volume(base, base_id, &dst, dst_id, scratch.path()).unwrap();
    (dst, c)
}

#[test]
fn publish_seals_a_pruned_copy_of_the_source() {
    let b = Bucket::new();
    let src = source(&b);
    let (published, report) = publish(&b, &src, SRC, PUB);
    assert!(!report.existing);
    assert!(report.copied_objects > 0);
    assert!(report.pruned.entries >= 2, "output and output/o.png");
    assert!(report.bases.is_empty(), "never references its source");
    assert!(exists(&published, &Key::Sealed));
    // Exactly the objects the kept files use were copied (the outputs' were not).
    let src_keys = b.data_keys(&src);
    let pub_keys = b.data_keys(&published);
    assert_eq!(pub_keys.len() as u64, report.copied_objects);
    assert!(pub_keys.is_subset(&src_keys) && pub_keys.len() < src_keys.len());

    // Sealed: read-only mounts only.
    let d = tempfile::tempdir().unwrap();
    assert!(matches!(
        mount(&published, PUB, d.path(), false),
        Err(VolumeError::Sealed(_))
    ));
    let v = mount(&published, PUB, d.path(), true).unwrap();
    assert_eq!(read_path(&v, "workflows/w.json"), WORKFLOW);
    assert_eq!(read_path(&v, "models/m.bin"), model());
    assert_eq!(resolve(&v, "output"), Err(Errno::ENOENT));
    v.unmount().unwrap();

    // A retry of the same publish returns the published volume as it is.
    let (_, again) = publish(&b, &src, SRC, PUB);
    assert!(again.existing);
    assert_eq!(again.inodes, report.inodes);

    // The source is untouched and keeps working.
    let d = tempfile::tempdir().unwrap();
    let v = mount(&src, SRC, d.path(), false).unwrap();
    assert_eq!(read_path(&v, "output/o.png"), pattern(200_000, 4));
    v.unmount().unwrap();

    // Once it has moved on, the published volume is not its state any
    // more: publishing it again to the same id is refused, never merged.
    let scratch = tempfile::tempdir().unwrap();
    let mut keep = |_: &[u8], _| true;
    assert!(matches!(
        publish_volume(&src, SRC, &published, PUB, &mut keep, scratch.path()),
        Err(VolumeError::Exists(_))
    ));
}

#[test]
fn a_clone_copies_metadata_only_and_writes_copy_on_write() {
    let b = Bucket::new();
    let src = source(&b);
    let (published, _) = publish(&b, &src, SRC, PUB);
    let published_before = b.objects(&published);

    let (cl, report) = clone(&b, &published, PUB, CLONE);
    assert_eq!(report.bases, [PUB]);
    assert!(report.data_bytes > 300_000);
    assert!(b.data_keys(&cl).is_empty(), "no data object was copied");

    let d = tempfile::tempdir().unwrap();
    let v = mount(&cl, CLONE, d.path(), false).unwrap();
    assert_eq!(v.report().epoch, 1);
    assert_eq!(read_path(&v, "workflows/w.json"), WORKFLOW);
    assert_eq!(read_path(&v, "models/m-link.bin"), model());
    let m = resolve(&v, "models/m.bin").unwrap();
    assert_eq!(v.getattr(m).unwrap().nlink, 2);
    assert_eq!(
        v.readlink(resolve(&v, "latest").unwrap()).unwrap(),
        b"models/m.bin"
    );
    let w = resolve(&v, "workflows/w.json").unwrap();
    assert_eq!(v.getxattr(w, b"user.author").unwrap(), b"alice");
    assert_eq!(resolve(&v, "output"), Err(Errno::ENOENT));

    // Overwrite the middle of the inherited model, append, add, delete.
    let fh = v.open(m).unwrap();
    v.write(fh, 100_000, &pattern(1000, 77)).unwrap();
    v.write(fh, 300_000, b"appended").unwrap();
    v.finish(v.flush(fh).unwrap()).unwrap();
    v.release(fh).unwrap();
    write_file(&v, ROOT_INO, "mine.txt", b"clone's own", 64);
    v.finish(
        v.remove(resolve(&v, "workflows").unwrap(), b"w.json", false)
            .unwrap(),
    )
    .unwrap();
    assert!(v.unmount().unwrap().fully_restored);

    let mut expect = model();
    expect[100_000..101_000].copy_from_slice(&pattern(1000, 77));
    expect.extend_from_slice(b"appended");
    let d = tempfile::tempdir().unwrap();
    let v = mount(&cl, CLONE, d.path(), false).unwrap();
    assert_eq!(read_path(&v, "models/m.bin"), expect);
    assert_eq!(read_path(&v, "models/m-link.bin"), expect);
    assert_eq!(read_path(&v, "mine.txt"), b"clone's own");
    assert_eq!(resolve(&v, "workflows/w.json"), Err(Errno::ENOENT));
    v.unmount().unwrap();

    assert_eq!(
        b.objects(&published),
        published_before,
        "the base never changes"
    );
    let d = tempfile::tempdir().unwrap();
    let v = mount(&published, PUB, d.path(), true).unwrap();
    assert_eq!(read_path(&v, "workflows/w.json"), WORKFLOW);
    assert_eq!(read_path(&v, "models/m.bin"), model());
    v.unmount().unwrap();
}

#[test]
fn a_clone_never_collects_its_base() {
    let b = Bucket::new();
    let src = source(&b);
    let (published, _) = publish(&b, &src, SRC, PUB);
    let published_before = b.objects(&published);
    let (cl, _) = clone(&b, &published, PUB, CLONE);

    // Delete everything inherited, then churn through several mounts (each
    // restore pass collects); zero retention makes GC as eager as it gets.
    for round in 0..3_u8 {
        let d = tempfile::tempdir().unwrap();
        let v = mount(&cl, CLONE, d.path(), false).unwrap();
        for (dir, name) in [
            ("workflows", "w.json"),
            ("models", "m.bin"),
            ("models", "m-link.bin"),
            ("", "latest"),
        ] {
            let parent = resolve(&v, dir).unwrap();
            if v.lookup(parent, name.as_bytes()).is_ok() {
                v.finish(v.remove(parent, name.as_bytes(), false).unwrap())
                    .unwrap();
            }
        }
        let name = format!("churn-{round}");
        write_file(&v, ROOT_INO, &name, &pattern(100_000, round), 100_000);
        if round > 0 {
            let old = format!("churn-{}", round - 1);
            v.finish(v.remove(ROOT_INO, old.as_bytes(), false).unwrap())
                .unwrap();
        }
        assert!(v.unmount().unwrap().fully_restored);
    }
    // GC deletes only under the clone's own root, so the base is safe by
    // construction; and keys equal to a base's (both volumes write blobs
    // from epoch 1) must not confuse which of the clone's own objects live.
    assert_eq!(b.objects(&published), published_before);
    let d = tempfile::tempdir().unwrap();
    let v = mount(&cl, CLONE, d.path(), true).unwrap();
    assert_eq!(read_path(&v, "churn-2"), pattern(100_000, 2));
    assert_eq!(resolve(&v, "churn-0"), Err(Errno::ENOENT));
    v.unmount().unwrap();
}

#[test]
fn clones_of_published_clones_reference_the_owning_volumes() {
    let b = Bucket::new();
    let src = source(&b);
    let (p1, _) = publish(&b, &src, SRC, PUB);
    let (c1, _) = clone(&b, &p1, PUB, CLONE);
    let d = tempfile::tempdir().unwrap();
    let v = mount(&c1, CLONE, d.path(), false).unwrap();
    // Its first blob has the same relative key as the source's first blob
    // (both written in epoch 1): keys only mean something with their volume.
    write_file(&v, ROOT_INO, "lora.bin", &pattern(150_000, 5), 150_000);
    assert!(v.unmount().unwrap().fully_restored);

    let (p2, published) = publish(&b, &c1, CLONE, P2);
    assert_eq!(
        published.bases,
        [PUB],
        "inherited data stays a reference to p1"
    );
    assert_eq!(published.copied_objects, b.data_keys(&c1).len() as u64);

    let (c2, cloned) = clone(&b, &p2, P2, C2);
    let expect: BTreeSet<[u8; 16]> = [PUB, P2].into();
    assert_eq!(cloned.bases.into_iter().collect::<BTreeSet<_>>(), expect);
    let d = tempfile::tempdir().unwrap();
    let v = mount(&c2, C2, d.path(), false).unwrap();
    assert_eq!(read_path(&v, "models/m.bin"), model());
    assert_eq!(read_path(&v, "lora.bin"), pattern(150_000, 5));
    assert_eq!(read_path(&v, "workflows/w.json"), WORKFLOW);
    v.unmount().unwrap();
}

#[test]
fn publish_needs_a_quiescent_source_and_clone_a_sealed_base() {
    let b = Bucket::new();
    let src = source(&b);
    let scratch = tempfile::tempdir().unwrap();

    // Mounted: its log runs past its latest snapshot.
    let d = tempfile::tempdir().unwrap();
    let v = mount(&src, SRC, d.path(), false).unwrap();
    let dst = b.client("published", PUB);
    let mut keep = |_: &[u8], _| true;
    assert!(matches!(
        publish_volume(&src, SRC, &dst, PUB, &mut keep, scratch.path()),
        Err(VolumeError::NotQuiescent(..))
    ));
    drop(v); // a crash: still not quiescent
    assert!(matches!(
        publish_volume(&src, SRC, &dst, PUB, &mut keep, scratch.path()),
        Err(VolumeError::NotQuiescent(..))
    ));
    assert!(
        !exists(&dst, &Key::SnapshotManifest(0)),
        "nothing was committed"
    );

    // Not sealed: a writable volume is no base.
    let target = b.client("tenant2", CLONE);
    assert!(matches!(
        clone_volume(&src, SRC, &target, CLONE, scratch.path()),
        Err(VolumeError::NotSealed(_))
    ));
}

#[test]
fn a_clone_refuses_to_mount_over_an_unsealed_base_and_never_overwrites() {
    let b = Bucket::new();
    let src = source(&b);
    let (published, _) = publish(&b, &src, SRC, PUB);
    let (cl, _) = clone(&b, &published, PUB, CLONE);

    // The same clone again is recognised; any other base is refused.
    let scratch = tempfile::tempdir().unwrap();
    let again = clone_volume(&published, PUB, &cl, CLONE, scratch.path()).unwrap();
    assert!(again.existing);
    let (other, _) = publish(&b, &src, SRC, P2);
    assert!(matches!(
        clone_volume(&other, P2, &cl, CLONE, scratch.path()),
        Err(VolumeError::Exists(_))
    ));

    // Losing the seal means the base could change under the clone: fail closed.
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(published.delete(&Key::Sealed))
        .unwrap();
    let d = tempfile::tempdir().unwrap();
    assert!(matches!(
        mount(&cl, CLONE, d.path(), false),
        Err(VolumeError::NotSealed(_))
    ));
    let (problems, _) = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(skyfs_volume::mount::fsck(&cl, CLONE, scratch.path()))
        .unwrap();
    assert!(
        problems.iter().any(|p| p.contains("not sealed")),
        "{problems:?}"
    );
}

// ------------------------------------------------------------ fault sweeps

fn fsck_clean(c: &ObjectClient, id: [u8; 16]) {
    let scratch = tempfile::tempdir().unwrap();
    let (problems, _) = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(skyfs_volume::mount::fsck(c, id, scratch.path()))
        .unwrap();
    assert_eq!(problems, Vec::<String>::new());
}

/// The published volume holds exactly the kept tree.
fn check_published(p: &ObjectClient) {
    assert!(exists(p, &Key::Sealed));
    let d = tempfile::tempdir().unwrap();
    let v = mount(p, PUB, d.path(), true).unwrap();
    assert_eq!(read_path(&v, "workflows/w.json"), WORKFLOW);
    assert_eq!(read_path(&v, "models/m-link.bin"), model());
    assert_eq!(resolve(&v, "output"), Err(Errno::ENOENT));
    v.unmount().unwrap();
    fsck_clean(p, PUB);
}

/// The clone mounts writable and reads through to its base.
fn check_clone(c: &ObjectClient) {
    let d = tempfile::tempdir().unwrap();
    let v = mount(c, CLONE, d.path(), false).unwrap();
    assert_eq!(read_path(&v, "models/m.bin"), model());
    write_file(&v, ROOT_INO, "after", b"x", 1);
    assert!(v.unmount().unwrap().fully_restored);
    fsck_clean(c, CLONE);
}

/// A view of `b` that injects `fault` at mutation `k`.
fn faulty(b: &Bucket, k: u64, fault: Fault) -> (Arc<Faults>, Arc<dyn ObjectStore>) {
    let faults = Faults::at(k, fault);
    let store: Arc<dyn ObjectStore> = Arc::new(FaultStore::new(b.store.clone(), faults.clone()));
    (faults, store)
}

/// Every fault at every mutation of a publish: whatever it leaves visible
/// is a complete, sealed volume, and a retry converges.
#[test]
fn publish_survives_a_fault_at_every_mutation() {
    let mut cases = 0;
    for fault in Fault::ALL {
        for k in 0.. {
            let b = Bucket::new();
            let src = source(&b);
            let (faults, store) = faulty(&b, k, fault);
            let fsrc = ObjectClient::new(store, VolumeRoot::new("tenant", &SRC));
            let fdst = fsrc.sibling(VolumeRoot::new("published", &PUB));
            let scratch = tempfile::tempdir().unwrap();
            let mut keep = |p: &[u8], _| p != b"output";
            let first = publish_volume(&fsrc, SRC, &fdst, PUB, &mut keep, scratch.path());
            if !faults.fired() {
                first.unwrap();
                break;
            }
            cases += 1;
            let dst = b.client("published", PUB);
            if exists(&dst, &Key::SnapshotManifest(0)) {
                check_published(&dst); // visible means complete and sealed
            }
            let (dst, _) = publish(&b, &src, SRC, PUB);
            check_published(&dst);
        }
    }
    assert!(cases >= 4 * 5, "too few mutations enumerated: {cases}");
}

/// Every fault at every mutation of a clone: whatever it leaves visible is
/// a complete clone, a retry converges, and the base never changes.
#[test]
fn clone_survives_a_fault_at_every_mutation() {
    let mut cases = 0;
    for fault in Fault::ALL {
        for k in 0.. {
            let b = Bucket::new();
            let src = source(&b);
            let (published, _) = publish(&b, &src, SRC, PUB);
            let published_before = b.objects(&published);
            let (faults, store) = faulty(&b, k, fault);
            let fbase = ObjectClient::new(store, VolumeRoot::new("published", &PUB));
            let fdst = fbase.sibling(VolumeRoot::new("tenant2", &CLONE));
            let scratch = tempfile::tempdir().unwrap();
            let first = clone_volume(&fbase, PUB, &fdst, CLONE, scratch.path());
            if !faults.fired() {
                first.unwrap();
                break;
            }
            cases += 1;
            let dst = b.client("tenant2", CLONE);
            if exists(&dst, &Key::SnapshotManifest(0)) {
                check_clone(&dst);
            } else {
                let (dst, again) = clone(&b, &published, PUB, CLONE);
                assert!(!again.existing);
                check_clone(&dst);
            }
            // A retry of a committed clone recognises it.
            let (_, again) = clone(&b, &published, PUB, CLONE);
            assert!(again.existing);
            assert_eq!(b.objects(&published), published_before);
        }
    }
    assert!(cases >= 4 * 2, "too few mutations enumerated: {cases}");
}
