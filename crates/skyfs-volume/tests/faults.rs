//! Fault injection at every crash point (design §11).
//!
//! A fixed workload runs against a bucket that injects one fault at the
//! k-th mutation (PUT, DELETE), for every k the workload reaches and every
//! kind of fault: a transient failure, a lost response, and a crash just
//! before or just after the mutation lands. Mutations come from every
//! engine path: blob uploads, segment commits, restore passes (packs,
//! snapshot metadata, manifests, the hint), GC deletes and unmount.
//!
//! The oracle, for every case:
//! - transient faults and lost responses change nothing: the run ends with
//!   a clean unmount, fully restored, holding exactly the final state;
//! - after a crash, a mount on another machine succeeds and holds the state
//!   after some prefix of the workload that includes every op acknowledged
//!   by `fsync`; a mount on the same disk recovers every op;
//! - every volume passes fsck.
//!
//! A second enumeration crashes the recovery mount itself at each of its
//! mutations, and a third tears the local WAL at every byte.

#![allow(clippy::cast_possible_truncation)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use skyfs_format::wal::v1::RenameMode;
use skyfs_meta::plan::{AttrChanges, Caller, MakeRequest};
use skyfs_meta::{Ino, Kind, ROOT_INO};
use skyfs_store::faults::{Fault, FaultStore, Faults};
use skyfs_store::{ObjectClient, VolumeRoot};
use skyfs_volume::{AtimeMode, CreateOptions, Volume, VolumeConfig, VolumeError, create_volume};

const VID: [u8; 16] = [0x7c; 16];
const ROOT: Caller = Caller { uid: 0, gid: 0 };

// ------------------------------------------------------------------ model

#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    Dir,
    File(Vec<u8>),
    Link(Vec<u8>),
}

/// Path → node; the root is implicit.
type Tree = BTreeMap<String, Node>;

#[derive(Debug, Clone)]
enum Step {
    Mkdir(&'static str),
    /// Create and write in `chunk`-sized sequential writes.
    Write(&'static str, Vec<u8>, usize),
    /// Overwrite bytes of an existing file in place.
    Patch(&'static str, u64, &'static [u8]),
    Truncate(&'static str, u64),
    Symlink(&'static str, &'static str),
    Rename(&'static str, &'static str),
    Unlink(&'static str),
    /// A durability point: everything before it is acknowledged.
    Fsync,
}

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u8).wrapping_mul(29).wrapping_add(seed))
        .collect()
}

/// Inline writes, blobs (runs over `inline_data_max`), in-place patches,
/// truncation, symlinks, file and directory renames, unlink; durability
/// points throughout so the log has many segments and restore passes run.
fn workload() -> Vec<Step> {
    use Step::{Fsync, Mkdir, Patch, Rename, Symlink, Truncate, Unlink, Write};
    vec![
        Mkdir("d"),
        Write("d/small", pattern(3000, 1), 1000),
        Fsync,
        Write("big", pattern(150_000, 2), 32 * 1024),
        Fsync,
        Symlink("d/link", "../big"),
        Rename("d/small", "d/renamed"),
        Fsync,
        Patch("d/renamed", 100, b"PATCHED"),
        Truncate("big", 70_000),
        Fsync,
        Write("tmp", pattern(5000, 3), 5000),
        Unlink("tmp"),
        Fsync,
        Mkdir("e"),
        Write("e/x", pattern(100, 4), 100),
        Rename("e", "d/e"),
        Fsync,
        Write("g1", pattern(2000, 5), 2000),
        Fsync,
        Write("g2", pattern(40_000, 6), 20_000),
        Fsync,
        Patch("big", 0, b"HEAD"),
        Fsync,
    ]
}

impl Step {
    /// Applies the step to `t`, pushing the tree after each of its log
    /// operations: durability is a prefix of operations, and a segment may
    /// be sealed in the middle of a step.
    fn apply(&self, t: &mut Tree, states: &mut Vec<Tree>) {
        match self {
            Step::Mkdir(p) => {
                t.insert((*p).into(), Node::Dir);
            }
            Step::Write(p, data, chunk) => {
                t.insert((*p).into(), Node::File(Vec::new()));
                states.push(t.clone());
                for c in data.chunks(*chunk) {
                    let Some(Node::File(d)) = t.get_mut(*p) else {
                        unreachable!()
                    };
                    d.extend_from_slice(c);
                    states.push(t.clone());
                }
                return;
            }
            Step::Patch(p, off, bytes) => {
                let Some(Node::File(d)) = t.get_mut(*p) else {
                    panic!("patch of a missing file")
                };
                let off = *off as usize;
                if d.len() < off + bytes.len() {
                    d.resize(off + bytes.len(), 0);
                }
                d[off..off + bytes.len()].copy_from_slice(bytes);
            }
            Step::Truncate(p, len) => {
                let Some(Node::File(d)) = t.get_mut(*p) else {
                    panic!("truncate of a missing file")
                };
                d.resize(*len as usize, 0);
            }
            Step::Symlink(p, target) => {
                t.insert((*p).into(), Node::Link(target.as_bytes().to_vec()));
            }
            Step::Rename(from, to) => {
                let moved: Vec<(String, Node)> = t
                    .iter()
                    .filter(|(k, _)| *k == from || k.starts_with(&format!("{from}/")))
                    .map(|(k, v)| (format!("{to}{}", &k[from.len()..]), v.clone()))
                    .collect();
                t.retain(|k, _| !(k == from || k.starts_with(&format!("{from}/"))));
                t.extend(moved);
            }
            Step::Unlink(p) => {
                t.remove(*p);
            }
            Step::Fsync => return,
        }
        states.push(t.clone());
    }

    /// Runs the step. Local steps never touch the bucket, so they complete
    /// even after the bucket has gone away; only `Fsync` waits for it.
    /// Returns false if the step could not complete because the process
    /// is dead.
    fn run(&self, v: &Volume, faults: &Faults) -> bool {
        match self {
            Step::Mkdir(p) => {
                let (parent, name) = parent_of(v, p);
                let _ = v
                    .make(ROOT, &make(parent, name, Kind::Directory, b""))
                    .unwrap();
            }
            Step::Write(p, data, chunk) => {
                let (parent, name) = parent_of(v, p);
                let (_, fh) = v
                    .create(ROOT, parent, name.as_bytes(), 0o644, 0o022)
                    .unwrap()
                    .value;
                for (i, c) in data.chunks(*chunk).enumerate() {
                    v.write(fh, (i * chunk) as u64, c).unwrap();
                }
                v.release(fh).unwrap();
            }
            Step::Patch(p, off, bytes) => {
                let fh = v.open(resolve(v, p)).unwrap();
                v.write(fh, *off, bytes).unwrap();
                v.release(fh).unwrap();
            }
            Step::Truncate(p, len) => {
                let ch = AttrChanges {
                    size: Some(*len),
                    ..AttrChanges::default()
                };
                let _ = v.setattr(resolve(v, p), &ch).unwrap();
            }
            Step::Symlink(p, target) => {
                let (parent, name) = parent_of(v, p);
                let _ = v
                    .make(ROOT, &make(parent, name, Kind::Symlink, target.as_bytes()))
                    .unwrap();
            }
            Step::Rename(from, to) => {
                let (fp, fname) = parent_of(v, from);
                let (tp, tname) = parent_of(v, to);
                let _ = v
                    .rename(
                        fp,
                        fname.as_bytes(),
                        tp,
                        tname.as_bytes(),
                        RenameMode::Unspecified,
                    )
                    .unwrap();
            }
            Step::Unlink(p) => {
                let (parent, name) = parent_of(v, p);
                let _ = v.remove(parent, name.as_bytes(), false).unwrap();
            }
            Step::Fsync => {
                let Some(seq) = v.fsync().unwrap().wait else {
                    return true;
                };
                return wait_or_die(v, seq, faults);
            }
        }
        true
    }
}

fn make<'a>(parent: Ino, name: &'a str, kind: Kind, target: &'a [u8]) -> MakeRequest<'a> {
    MakeRequest {
        parent,
        name: name.as_bytes(),
        kind,
        mode: 0o755,
        umask: 0,
        rdev: 0,
        target,
    }
}

fn resolve(v: &Volume, path: &str) -> Ino {
    path.split('/').fold(ROOT_INO, |ino, part| {
        v.lookup(ino, part.as_bytes()).unwrap().ino
    })
}

fn parent_of<'a>(v: &Volume, path: &'a str) -> (Ino, &'a str) {
    match path.rsplit_once('/') {
        Some((dir, name)) => (resolve(v, dir), name),
        None => (ROOT_INO, path),
    }
}

/// Waits for a commit, or gives up when the simulated process dies.
fn wait_or_die(v: &Volume, seq: u64, faults: &Faults) -> bool {
    let commit = v.waiter(seq);
    v.runtime().block_on(async {
        tokio::select! {
            r = commit => r.is_ok(),
            () = async {
                while !faults.dead() {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            } => false,
        }
    })
}

/// The whole tree, read through the volume.
fn read_tree(v: &Volume) -> Tree {
    fn walk(v: &Volume, dir: Ino, prefix: &str, out: &mut Tree) {
        let (entries, _) = v.readdir(dir, 2, 10_000).unwrap();
        for e in entries {
            let path = format!("{prefix}{}", String::from_utf8(e.name).unwrap());
            match e.kind {
                Kind::Directory => {
                    out.insert(path.clone(), Node::Dir);
                    walk(v, e.ino, &format!("{path}/"), out);
                }
                Kind::Symlink => {
                    out.insert(path, Node::Link(v.readlink(e.ino).unwrap()));
                }
                _ => {
                    let size = v.getattr(e.ino).unwrap().size;
                    let data = v.read(e.ino, 0, size as u32).unwrap();
                    out.insert(path, Node::File(data));
                }
            }
        }
    }
    let mut t = Tree::new();
    walk(v, ROOT_INO, "", &mut t);
    t
}

// ---------------------------------------------------------------- harness

fn client(store: Arc<dyn ObjectStore>) -> ObjectClient {
    ObjectClient::new(store, VolumeRoot::new("t", &VID))
}

fn config(dir: &Path) -> VolumeConfig {
    let mut cfg = VolumeConfig::new(dir.to_path_buf());
    cfg.max_lag = Duration::from_millis(5);
    cfg.group_commit = Duration::from_millis(2);
    // A restore pass every few segments, with GC of everything it can.
    cfg.restore_segments = 3;
    cfg.restore_after = Duration::from_secs(3600);
    cfg.retention = Duration::ZERO;
    cfg.pack_size = 32 * 1024;
    cfg.restore_on_exit_timeout = Duration::from_secs(10);
    cfg.local_lock_wait = Duration::from_secs(10);
    cfg.mount_timeout = Duration::from_secs(1);
    cfg.atime = AtimeMode::NoAtime;
    cfg
}

/// The states the workload passes through.
struct Expect {
    /// Every tree after each log operation, from the empty volume.
    states: Vec<Tree>,
    /// `step_end[i]`: index into `states` after the first i local steps.
    step_end: Vec<usize>,
}

fn expected() -> Expect {
    let mut t = Tree::new();
    let mut states = vec![t.clone()];
    let mut step_end = vec![0];
    for s in workload() {
        if !matches!(s, Step::Fsync) {
            s.apply(&mut t, &mut states);
            step_end.push(states.len() - 1);
        }
    }
    Expect { states, step_end }
}

/// What a faulted run did before it ended.
struct Outcome {
    /// Local steps completed (index into `Expect::step_end`).
    done: usize,
    /// Local steps acknowledged durable by an `fsync`.
    durable: usize,
    /// The unmount result, if the run got that far.
    unmount: Option<Result<skyfs_volume::UnmountReport, VolumeError>>,
}

/// Mounts with `faults`, runs the workload until it ends or the process
/// dies, then unmounts (abandoning the unmount if the process dies).
fn faulted_run(mem: &Arc<InMemory>, dir: &Path, faults: &Arc<Faults>) -> Outcome {
    run_with(mem, dir, faults, false)
}

/// `partitioned`: when the bucket dies, the process lives on and keeps
/// logging locally (durability points cannot complete), and dies at the
/// end of the workload instead of unmounting.
fn run_with(mem: &Arc<InMemory>, dir: &Path, faults: &Arc<Faults>, partitioned: bool) -> Outcome {
    let store: Arc<dyn ObjectStore> = Arc::new(FaultStore::new(mem.clone(), faults.clone()));
    let mut out = Outcome {
        done: 0,
        durable: 0,
        unmount: None,
    };
    let v = match Volume::mount(client(store), VID, config(dir)) {
        Ok(v) => v,
        // A crash before MountBegin committed: nothing of this mount counts.
        Err(_) if faults.dead() => return out,
        Err(e) => panic!("mount failed without a crash: {e}"),
    };
    for step in workload() {
        let local = !matches!(step, Step::Fsync);
        if faults.dead() {
            if !partitioned {
                return out;
            }
            if !local {
                continue;
            }
        }
        if !step.run(&v, faults) {
            if partitioned {
                continue;
            }
            return out;
        }
        if local {
            out.done += 1;
        } else {
            out.durable = out.done;
        }
    }
    if partitioned && faults.dead() {
        return out;
    }
    // The unmount waits for the bucket; a crash abandons it.
    let abort = v.abort_handle();
    let finished = Arc::new(AtomicBool::new(false));
    let dog = {
        let (faults, finished) = (faults.clone(), finished.clone());
        std::thread::spawn(move || {
            while !finished.load(Ordering::SeqCst) {
                if faults.dead() {
                    abort.abort();
                    return;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    };
    out.unmount = Some(v.unmount());
    finished.store(true, Ordering::SeqCst);
    dog.join().unwrap();
    out
}

/// Mounts without faults and returns the tree; checks fsck and that a
/// clean unmount works.
fn recover(store: &Arc<dyn ObjectStore>, dir: &Path) -> (Tree, skyfs_volume::MountReport) {
    let v = Volume::mount(client(store.clone()), VID, config(dir))
        .unwrap_or_else(|e| panic!("recovery mount refused: {e}"));
    let report = v.report().clone();
    let tree = read_tree(&v);
    let u = v.unmount().unwrap();
    assert!(u.fully_restored, "restore did not finish at unmount");
    fsck(store);
    (tree, report)
}

fn fsck(store: &Arc<dyn ObjectStore>) {
    let scratch = tempfile::tempdir().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (problems, _) = rt
        .block_on(skyfs_volume::mount::fsck(
            &client(store.clone()),
            VID,
            scratch.path(),
        ))
        .unwrap();
    assert!(problems.is_empty(), "fsck: {problems:?}");
}

fn describe(t: &Tree) -> String {
    t.iter()
        .map(|(k, v)| match v {
            Node::Dir => format!("{k}/"),
            Node::File(d) => format!("{k}[{}]", d.len()),
            Node::Link(l) => format!("{k}->{}", String::from_utf8_lossy(l)),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `t` is the state after some number of steps in `range`.
/// (States repeat: a file created then unlinked restores an earlier tree.)
fn is_state_in(e: &Expect, t: &Tree, range: std::ops::RangeInclusive<usize>) -> bool {
    range.into_iter().any(|i| e.states[i] == *t)
}

/// One case: `fault` at mutation `k`. Returns whether it fired.
fn case(k: u64, fault: Fault, e: &Expect) -> bool {
    let mem = Arc::new(InMemory::new());
    {
        let opts = CreateOptions {
            inline_data_max: 16 * 1024,
            ..CreateOptions::default()
        };
        let scratch = tempfile::tempdir().unwrap();
        create_volume(&client(mem.clone()), VID, &opts, scratch.path()).unwrap();
    }
    let disk = tempfile::tempdir().unwrap();
    let faults = Faults::at(k, fault);
    let out = faulted_run(&mem, disk.path(), &faults);
    let ctx = || {
        format!(
            "{fault:?} at mutation {k}; log:\n{}",
            faults.log().join("\n")
        )
    };
    if !faults.fired() {
        return false;
    }

    if !fault.is_crash() {
        // Nothing may be lost or stuck: a full run, a clean unmount, fully
        // restored, and the final state from the bucket alone.
        assert_eq!(out.done, e.step_end.len() - 1, "{}", ctx());
        let u = out.unmount.expect("ran to the end");
        let u = u.unwrap_or_else(|err| panic!("unmount failed: {err}; {}", ctx()));
        assert!(u.fully_restored, "restorer gave up: {}", ctx());
        let elsewhere = tempfile::tempdir().unwrap();
        let (tree, _) = recover(&(mem.clone() as Arc<dyn ObjectStore>), elsewhere.path());
        assert!(tree == e.states[e.states.len() - 1], "{}", ctx());
        return true;
    }

    // Another machine: the bucket alone. A prefix including every fsync.
    let fork: Arc<dyn ObjectStore> = Arc::new(mem.fork());
    let elsewhere = tempfile::tempdir().unwrap();
    let (tree, _) = recover(&fork, elsewhere.path());
    assert!(
        is_state_in(e, &tree, e.step_end[out.durable]..=e.step_end[out.done]),
        "not the state after {}..={} ops (matches {:?}; {}): {}",
        e.step_end[out.durable],
        e.step_end[out.done],
        (0..e.states.len())
            .filter(|&i| e.states[i] == tree)
            .collect::<Vec<_>>(),
        describe(&tree),
        ctx()
    );

    // The same disk: every completed step comes back.
    let (tree, _) = recover(&(mem.clone() as Arc<dyn ObjectStore>), disk.path());
    assert!(
        tree == e.states[e.step_end[out.done]],
        "same disk: {}",
        ctx()
    );
    // ... and is in the bucket now.
    let third = tempfile::tempdir().unwrap();
    let (tree, _) = recover(&(mem.clone() as Arc<dyn ObjectStore>), third.path());
    assert!(
        tree == e.states[e.step_end[out.done]],
        "after recovery: {}",
        ctx()
    );
    true
}

/// Runs `fault` at every mutation index until it stops firing.
fn every_mutation(fault: Fault) {
    let e = expected();
    let mut k = 0;
    let mut misses = 0;
    // Timing varies the count a little between runs; stop after several
    // indices in a row that the run never reached.
    while misses < 3 {
        if case(k, fault, &e) {
            misses = 0;
        } else {
            misses += 1;
        }
        k += 1;
    }
    assert!(k > 25, "the workload made only {k} mutations");
}

#[test]
fn a_transient_failure_at_every_mutation() {
    every_mutation(Fault::Fail);
}

#[test]
fn a_lost_response_at_every_mutation() {
    every_mutation(Fault::LostAck);
}

#[test]
fn a_crash_before_every_mutation() {
    every_mutation(Fault::CrashBefore);
}

#[test]
fn a_crash_after_every_mutation() {
    every_mutation(Fault::CrashAfter);
}

/// Without faults, the workload makes enough mutations to be worth
/// enumerating and ends fully restored. `FAULT_CASE=<Fault>:<k>` runs one
/// case instead, for debugging.
#[test]
fn baseline() {
    if let Ok(c) = std::env::var("FAULT_CASE") {
        let (f, k) = c.split_once(':').unwrap();
        let fault = *Fault::ALL.iter().find(|x| format!("{x:?}") == f).unwrap();
        assert!(case(k.parse().unwrap(), fault, &expected()), "did not fire");
        return;
    }
    let started = std::time::Instant::now();
    let mem = Arc::new(InMemory::new());
    let scratch = tempfile::tempdir().unwrap();
    let opts = CreateOptions {
        inline_data_max: 16 * 1024,
        ..CreateOptions::default()
    };
    create_volume(&client(mem.clone()), VID, &opts, scratch.path()).unwrap();
    let disk = tempfile::tempdir().unwrap();
    let faults = Faults::none();
    let out = faulted_run(&mem, disk.path(), &faults);
    let u = out.unmount.unwrap().unwrap();
    eprintln!(
        "{} mutations in {:?}\n{}",
        faults.mutations(),
        started.elapsed(),
        faults.log().join("\n")
    );
    assert!(u.fully_restored);
}

// ------------------------------------------------------- recovery crashes

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

/// A bucket and a disk where the bucket went away early and the process
/// kept logging locally, then died: a long local WAL with blobs.
fn stranded(e: &Expect) -> (Arc<InMemory>, tempfile::TempDir, usize) {
    let mem = Arc::new(InMemory::new());
    let opts = CreateOptions {
        inline_data_max: 16 * 1024,
        ..CreateOptions::default()
    };
    let scratch = tempfile::tempdir().unwrap();
    create_volume(&client(mem.clone()), VID, &opts, scratch.path()).unwrap();
    let disk = tempfile::tempdir().unwrap();
    let out = run_with(&mem, disk.path(), &Faults::at(2, Fault::CrashBefore), true);
    assert_eq!(out.done, e.step_end.len() - 1);
    // The dead mount's tasks wind down in the background; they are gone
    // when its lock on the directory is.
    let lock = std::fs::File::open(disk.path().join("lock")).unwrap();
    while lock.try_lock().is_err() {
        std::thread::sleep(Duration::from_millis(5));
    }
    drop(lock);
    (mem, disk, out.done)
}

/// The recovery mount itself crashes at each of its mutations (uploading
/// recovered blobs and segments, `MountBegin`, a restore pass); a second
/// recovery must still bring back every operation.
#[test]
fn a_crash_at_every_point_of_recovery() {
    let e = expected();
    let (mem, disk, done) = stranded(&e);
    let want = &e.states[e.step_end[done]];
    for fault in [Fault::CrashBefore, Fault::CrashAfter] {
        let mut misses = 0;
        let mut j = 0;
        while misses < 3 {
            let bucket = Arc::new(mem.fork());
            let copy = tempfile::tempdir().unwrap();
            copy_dir(disk.path(), copy.path());
            let faults = Faults::at(j, fault);
            let store: Arc<dyn ObjectStore> =
                Arc::new(FaultStore::new(bucket.clone(), faults.clone()));
            match Volume::mount(client(store), VID, config(copy.path())) {
                Ok(v) => {
                    // Let the restorer reach the fault, then die.
                    let deadline = std::time::Instant::now() + Duration::from_secs(2);
                    while !faults.dead() && std::time::Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    drop(v);
                }
                Err(err) => assert!(faults.dead(), "recovery failed without a crash: {err}"),
            }
            if faults.fired() {
                misses = 0;
                let (tree, _) = recover(&(bucket.clone() as Arc<dyn ObjectStore>), copy.path());
                assert!(
                    tree == *want,
                    "{fault:?} at recovery mutation {j}: {}\nlog:\n{}",
                    describe(&tree),
                    faults.log().join("\n")
                );
            } else {
                misses += 1;
            }
            j += 1;
        }
        assert!(j > 5, "recovery made only {j} mutations");
    }
}

// --------------------------------------------------------- torn local WAL

fn truncate(path: &Path, len: u64) {
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(len)
        .unwrap();
}

/// Lengths to tear a file of `len` bytes at: every byte near the end,
/// sampled elsewhere.
fn tear_points(len: u64) -> Vec<u64> {
    let mut v: Vec<u64> = (0..=len)
        .step_by(usize::try_from(len / 40 + 1).unwrap())
        .collect();
    v.extend(len.saturating_sub(64)..len);
    v.sort_unstable();
    v.dedup();
    v
}

/// Power loss tears the newest local files at any byte: the segment's
/// record frames, its inline data, or a large-write blob. Recovery on the
/// same disk must mount, serve a prefix of the operations, and never
/// serve bytes that fail their checksum.
#[test]
fn a_torn_local_wal_recovers_a_prefix() {
    let e = expected();
    let (mem, disk, done) = stranded(&e);
    let wal = disk.path().join("wal");
    let mut files: Vec<_> = std::fs::read_dir(&wal)
        .unwrap()
        .map(|d| d.unwrap().path())
        .collect();
    files.sort();
    let newest = |ext: &str| {
        files
            .iter()
            .rfind(|p| p.extension().is_some_and(|x| x == ext))
            .cloned()
    };
    let blob = files
        .iter()
        .rfind(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("blob-")
        })
        .cloned();
    let targets: Vec<_> = [newest("rec"), newest("dat"), blob]
        .into_iter()
        .flatten()
        .collect();
    assert!(
        targets.len() == 3,
        "expected a segment and a blob: {files:?}"
    );
    for target in targets {
        let len = std::fs::metadata(&target).unwrap().len();
        for at in tear_points(len) {
            let bucket = Arc::new(mem.fork());
            let copy = tempfile::tempdir().unwrap();
            copy_dir(disk.path(), copy.path());
            truncate(
                &copy.path().join("wal").join(target.file_name().unwrap()),
                at,
            );
            let (tree, _) = recover(&(bucket as Arc<dyn ObjectStore>), copy.path());
            assert!(
                is_state_in(&e, &tree, 0..=e.step_end[done]),
                "{} torn at {at}/{len}: {}",
                target.display(),
                describe(&tree)
            );
        }
    }
}
