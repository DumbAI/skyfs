//! The `NFSv3` adapter: serves a volume on localhost to the kernel's own NFS
//! client (design §10.2). It is how skyfs mounts on macOS, where FUSE needs
//! a kernel extension or a third-party transport; it works on Linux too.
//!
//! The daemon is the NFS server: one user process, bound to `127.0.0.1`,
//! mounted with the system's `mount_nfs` (macOS, no root) or `mount -t nfs`
//! (Linux, root). Nothing else is installed.
//!
//! Like the FUSE adapter it is a thin translation over [`Volume`]. The only
//! state it keeps is an open write handle per inode, because NFS has no
//! open/close: a handle is opened on the first write and released at the
//! client's COMMIT (sent at `fsync` and at `close`), when the file is
//! removed, or after it has been idle for [`HANDLE_IDLE`].
//!
//! Durability: writes are answered `UNSTABLE`, and the client's COMMIT is
//! [`Volume::fsync`], so durability follows `--fsync` exactly. The write
//! verifier changes with every daemon start, so a client resends any write
//! it had not seen committed when a daemon dies.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nessie_nfsserve::UnixCred;
use nessie_nfsserve::nfs::{
    fattr3, fileid3, filename3, fsinfo3, ftype3, nfs_fh3, nfspath3, nfsstat3, nfsstring, nfstime3,
    post_op_attr, sattr3, set_atime, set_gid3, set_mode3, set_mtime, set_size3, set_uid3,
    specdata3,
};
use nessie_nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nessie_nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use parking_lot::Mutex;
use skyfs_format::wal::v1::RenameMode;
use skyfs_meta::plan::{AttrChanges, Caller, MakeRequest};
use skyfs_meta::{Attr, Errno, Ino, Kind, ROOT_INO};
use skyfs_store::ObjectClient;
use skyfs_volume::{Done, Volume, VolumeConfig};

use crate::cli::{AtimeArg, MountArgs};
use crate::signal;

/// A write handle unused this long is released (its blob run closes and
/// uploads). Clients normally COMMIT at close long before this.
const HANDLE_IDLE: Duration = Duration::from_secs(5);

/// Largest READ and WRITE the client may send, and its preferred size.
const IO_SIZE: u32 = 1 << 20;

/// `fsinfo` properties: hard links, symlinks, POSIX semantics everywhere,
/// settable times (RFC 1813 §3.3.19).
const FSF3_LINK: u32 = 0x0001;
const FSF3_SYMLINK: u32 = 0x0002;
const FSF3_HOMOGENEOUS: u32 = 0x0008;
const FSF3_CANSETTIME: u32 = 0x0010;

/// Seconds a dead daemon's mount stays before macOS removes it.
const DEAD_TIMEOUT_SECS: u32 = 60;

// ------------------------------------------------------------ conversions

/// The NFS status for a skyfs error.
fn status(e: Errno) -> nfsstat3 {
    use nfsstat3::{
        NFS3ERR_ACCES, NFS3ERR_DQUOT, NFS3ERR_EXIST, NFS3ERR_FBIG, NFS3ERR_INVAL, NFS3ERR_IO,
        NFS3ERR_ISDIR, NFS3ERR_MLINK, NFS3ERR_NAMETOOLONG, NFS3ERR_NOENT, NFS3ERR_NOSPC,
        NFS3ERR_NOTDIR, NFS3ERR_NOTEMPTY, NFS3ERR_NOTSUPP, NFS3ERR_PERM, NFS3ERR_ROFS,
        NFS3ERR_STALE, NFS3ERR_XDEV,
    };
    match e.0 {
        libc::EPERM => NFS3ERR_PERM,
        libc::ENOENT | libc::ENODATA => NFS3ERR_NOENT,
        libc::EACCES => NFS3ERR_ACCES,
        libc::EEXIST => NFS3ERR_EXIST,
        libc::EXDEV => NFS3ERR_XDEV,
        libc::ENOTDIR => NFS3ERR_NOTDIR,
        libc::EISDIR => NFS3ERR_ISDIR,
        libc::EINVAL | libc::ERANGE => NFS3ERR_INVAL,
        libc::EFBIG => NFS3ERR_FBIG,
        libc::ENOSPC => NFS3ERR_NOSPC,
        libc::EROFS => NFS3ERR_ROFS,
        libc::EMLINK => NFS3ERR_MLINK,
        libc::ENAMETOOLONG => NFS3ERR_NAMETOOLONG,
        libc::ENOTEMPTY => NFS3ERR_NOTEMPTY,
        libc::EDQUOT => NFS3ERR_DQUOT,
        libc::ESTALE | libc::EBADF => NFS3ERR_STALE,
        libc::EOPNOTSUPP | libc::ENOSYS => NFS3ERR_NOTSUPP,
        _ => NFS3ERR_IO,
    }
}

/// The status for an operation on an object the client names by handle: a
/// missing inode is a stale handle, not a missing name.
fn stale(e: Errno) -> nfsstat3 {
    if e == Errno::ENOENT {
        nfsstat3::NFS3ERR_STALE
    } else {
        status(e)
    }
}

fn nfs_time(ns: i64) -> nfstime3 {
    let secs = ns.div_euclid(1_000_000_000);
    nfstime3 {
        seconds: u32::try_from(secs.max(0)).unwrap_or(u32::MAX),
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        nseconds: if secs < 0 {
            0
        } else {
            ns.rem_euclid(1_000_000_000) as u32
        },
    }
}

fn ns_of(t: nfstime3) -> i64 {
    i64::from(t.seconds) * 1_000_000_000 + i64::from(t.nseconds)
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

fn ftype(k: Kind) -> ftype3 {
    match k {
        Kind::Regular => ftype3::NF3REG,
        Kind::Directory => ftype3::NF3DIR,
        Kind::Symlink => ftype3::NF3LNK,
        Kind::Fifo => ftype3::NF3FIFO,
        Kind::Socket => ftype3::NF3SOCK,
        Kind::CharDevice => ftype3::NF3CHR,
        Kind::BlockDevice => ftype3::NF3BLK,
    }
}

/// Major and minor of a Linux `dev_t` (the encoding the log keeps).
#[allow(clippy::cast_possible_truncation)]
fn specdata(rdev: u64) -> specdata3 {
    specdata3 {
        specdata1: (((rdev >> 8) & 0xfff) | ((rdev >> 32) & !0xfff)) as u32,
        specdata2: ((rdev & 0xff) | ((rdev >> 12) & !0xff)) as u32,
    }
}

fn fattr(a: &Attr, fsid: u64) -> fattr3 {
    fattr3 {
        ftype: ftype(a.kind),
        mode: a.mode & 0o7777,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        size: a.size,
        used: a.size.div_ceil(4096) * 4096,
        rdev: specdata(a.rdev),
        fsid,
        fileid: a.ino,
        atime: nfs_time(a.atime_ns),
        mtime: nfs_time(a.mtime_ns),
        ctime: nfs_time(a.ctime_ns),
    }
}

/// The attribute changes a `sattr3` asks for.
fn changes(s: &sattr3) -> AttrChanges {
    let time = |client: Option<nfstime3>, server: bool| match (client, server) {
        (Some(t), _) => Some(ns_of(t)),
        (None, true) => Some(now_ns()),
        (None, false) => None,
    };
    AttrChanges {
        mode: match s.mode {
            set_mode3::mode(m) => Some(m),
            set_mode3::Void => None,
        },
        uid: match s.uid {
            set_uid3::uid(u) => Some(u),
            set_uid3::Void => None,
        },
        gid: match s.gid {
            set_gid3::gid(g) => Some(g),
            set_gid3::Void => None,
        },
        size: match s.size {
            set_size3::size(n) => Some(n),
            set_size3::Void => None,
        },
        atime_ns: match s.atime {
            set_atime::SET_TO_CLIENT_TIME(t) => time(Some(t), false),
            set_atime::SET_TO_SERVER_TIME => time(None, true),
            set_atime::DONT_CHANGE => None,
        },
        mtime_ns: match s.mtime {
            set_mtime::SET_TO_CLIENT_TIME(t) => time(Some(t), false),
            set_mtime::SET_TO_SERVER_TIME => time(None, true),
            set_mtime::DONT_CHANGE => None,
        },
    }
}

fn is_noop(c: &AttrChanges) -> bool {
    c.mode.is_none()
        && c.uid.is_none()
        && c.gid.is_none()
        && c.size.is_none()
        && c.atime_ns.is_none()
        && c.mtime_ns.is_none()
}

/// Runs a call into the engine. Volume calls are synchronous (SQLite, the
/// local WAL, sometimes a fetch on the engine's runtime), so they step off
/// this runtime's worker for their duration.
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    tokio::task::block_in_place(f)
}

// ------------------------------------------------------------ the server

/// Open write handles, one per inode. NFS has no open or close, so a
/// handle opens at a file's first write and is released at COMMIT, at
/// remove, or once idle. Methods are synchronous: async callers wrap them
/// in [`blocking`].
pub struct Writers {
    vol: Arc<Volume>,
    open: Mutex<HashMap<Ino, (u64, Instant)>>,
}

impl Writers {
    fn new(vol: Arc<Volume>) -> Self {
        Self {
            vol,
            open: Mutex::default(),
        }
    }

    /// Writes through the inode's handle, opening one if needed. The map
    /// stays locked for the write so a sweep cannot release the handle
    /// under it; the volume serializes writes anyway.
    fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let mut open = self.open.lock();
        let fh = match open.get(&ino) {
            Some(&(fh, _)) => fh,
            None => self.vol.open(ino)?,
        };
        open.insert(ino, (fh, Instant::now()));
        self.vol.write(fh, offset, data)
    }

    /// Adopts the handle a create opened.
    fn adopt(&self, ino: Ino, fh: u64) {
        if let Some((old, _)) = self.open.lock().insert(ino, (fh, Instant::now())) {
            let _ = self.vol.release(old);
        }
    }

    /// Releases the handle on `ino`, if one is open.
    fn release(&self, ino: Ino) {
        let taken = self.open.lock().remove(&ino);
        if let Some((fh, _)) = taken
            && let Err(e) = self.vol.release(fh)
        {
            tracing::warn!(ino, errno = e.0, "releasing a write handle");
        }
    }

    /// Releases handles unused for at least `idle`.
    fn sweep(&self, idle: Duration) {
        let now = Instant::now();
        let idle: Vec<u64> = {
            let mut open = self.open.lock();
            let old: Vec<Ino> = open
                .iter()
                .filter(|(_, (_, used))| now.duration_since(*used) >= idle)
                .map(|(ino, _)| *ino)
                .collect();
            old.iter()
                .filter_map(|ino| open.remove(ino))
                .map(|(fh, _)| fh)
                .collect()
        };
        for fh in idle {
            let _ = self.vol.release(fh);
        }
    }

    /// Releases every handle (shutdown).
    fn release_all(&self) {
        self.sweep(Duration::ZERO);
    }
}

/// The NFS filesystem over one mounted volume.
pub struct SkyNfs {
    vol: Arc<Volume>,
    writers: Arc<Writers>,
    /// Owner of objects the client creates without credentials (symlinks):
    /// the daemon's user, who is the user at the keyboard on a laptop.
    owner: Caller,
    /// The first 8 bytes of the volume id: the `fsid`, and the prefix of
    /// every file handle.
    fsid: [u8; 8],
    opts: NfsOptions,
}

/// How the NFS server behaves.
#[derive(Debug, Clone, Copy, Default)]
pub struct NfsOptions {
    /// Serve read-only.
    pub read_only: bool,
}

/// Whether `name` is an AppleDouble sibling (`._name`).
///
/// NFSv3 has no extended attributes, so the macOS client keeps a file's
/// xattrs in a `._name` sibling it manages itself, and macOS gives nearly
/// every file one (`com.apple.provenance`). The client finds a sibling by
/// name, never by listing, so listings leave them out: tools that walk a
/// tree (uv's cache, `git`, `ls`) would otherwise trip over them (uv tries
/// to hard-link them: EPERM). Refusing them is no option: macOS turns any
/// failure to create one into EPERM on the xattr write, which `cp` and
/// `copyfile` treat as fatal.
fn is_apple_double(name: &[u8]) -> bool {
    name.starts_with(b"._")
}

impl SkyNfs {
    /// Serves `vol` (`volume_id` scopes its file handles), writing through
    /// `writers`.
    #[must_use]
    pub fn new(writers: Arc<Writers>, volume_id: &[u8; 16], opts: NfsOptions) -> Self {
        let mut fsid = [0u8; 8];
        fsid.copy_from_slice(&volume_id[..8]);
        let owner = Caller {
            uid: rustix::process::geteuid().as_raw(),
            gid: rustix::process::getegid().as_raw(),
        };
        Self {
            vol: writers.vol.clone(),
            writers,
            owner,
            fsid,
            opts,
        }
    }

    fn fsid(&self) -> u64 {
        u64::from_le_bytes(self.fsid)
    }

    fn attr(&self, a: &Attr) -> fattr3 {
        fattr(a, self.fsid())
    }

    /// Waits for whatever `done` requires (sync replication, fsync).
    async fn finish<T>(&self, done: Done<T>) -> Result<T, nfsstat3> {
        if let Some(seq) = done.wait {
            self.vol.waiter(seq).await.map_err(status)?;
        }
        Ok(done.value)
    }

    fn getattr_now(&self, ino: Ino) -> Result<fattr3, nfsstat3> {
        blocking(|| self.vol.getattr(ino))
            .map(|a| self.attr(&a))
            .map_err(stale)
    }

    /// Removes `dir`'s entries if every one is an AppleDouble sibling;
    /// whether it did (the directory is then empty).
    fn clear_siblings(&self, dir: Ino) -> Result<bool, nfsstat3> {
        let mut names = Vec::new();
        let mut after = 2;
        loop {
            let (batch, _) = self.vol.readdir(dir, after, 1024).map_err(status)?;
            let last = batch.len() < 1024;
            for e in batch {
                if !is_apple_double(&e.name) || e.kind == Kind::Directory {
                    return Ok(false);
                }
                after = e.cookie;
                names.push(e.name);
            }
            if last {
                break;
            }
        }
        for n in &names {
            let done = self.vol.remove(dir, n, false).map_err(status)?;
            self.vol.finish(done).map_err(status)?;
        }
        Ok(true)
    }

    fn name(n: &filename3) -> &[u8] {
        &n.0
    }

    async fn make(
        &self,
        caller: Caller,
        m: &MakeRequest<'_>,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let done = blocking(|| self.vol.make(caller, m)).map_err(status)?;
        let a = self.finish(done).await?;
        Ok((a.ino, self.attr(&a)))
    }

    async fn create_file(
        &self,
        caller: Caller,
        dir: fileid3,
        name: &filename3,
        attr: Option<&sattr3>,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let mode = match attr.map(|a| &a.mode) {
            Some(set_mode3::mode(m)) => *m,
            _ => 0o644,
        };
        // The client applied its umask to `mode` already.
        let done =
            blocking(|| self.vol.create(caller, dir, Self::name(name), mode, 0)).map_err(status)?;
        let (a, fh) = self.finish(done).await?;
        blocking(|| self.writers.adopt(a.ino, fh));
        // Anything else the client set at create (size, owner, times).
        if let Some(s) = attr {
            let mut ch = changes(s);
            ch.mode = None;
            if !is_noop(&ch) {
                let done = blocking(|| self.vol.setattr(a.ino, &ch)).map_err(status)?;
                let a = self.finish(done).await?;
                return Ok((a.ino, self.attr(&a)));
            }
        }
        Ok((a.ino, self.attr(&a)))
    }
}

fn cred(c: &UnixCred) -> Caller {
    Caller {
        uid: c.uid,
        gid: c.gid,
    }
}

#[async_trait]
impl NFSFileSystem for SkyNfs {
    fn capabilities(&self) -> VFSCapabilities {
        if self.opts.read_only {
            VFSCapabilities::ReadOnly
        } else {
            VFSCapabilities::ReadWrite
        }
    }

    fn root_dir(&self) -> fileid3 {
        ROOT_INO
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        blocking(|| self.vol.lookup(dirid, Self::name(filename)))
            .map(|a| a.ino)
            .map_err(status)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        self.getattr_now(id)
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        let ch = changes(&setattr);
        if is_noop(&ch) {
            return self.getattr_now(id);
        }
        let done = blocking(|| self.vol.setattr(id, &ch)).map_err(stale)?;
        let a = self.finish(done).await?;
        Ok(self.attr(&a))
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        blocking(|| {
            let a = self.vol.getattr(id)?;
            if a.kind != Kind::Regular {
                return Err(Errno::EISDIR);
            }
            // NFS has no open: the first read of a file is its access.
            if offset == 0 {
                let _ = self.vol.access(id);
            }
            let data = self.vol.read(id, offset, count.min(IO_SIZE))?;
            let eof = offset + data.len() as u64 >= a.size;
            Ok((data, eof))
        })
        .map_err(stale)
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        self.write_stable(id, offset, data, false)
            .await
            .map(|(a, _)| a)
    }

    async fn write_stable(
        &self,
        id: fileid3,
        offset: u64,
        data: &[u8],
        stable: bool,
    ) -> Result<(fattr3, bool), nfsstat3> {
        blocking(|| self.writers.write(id, offset, data)).map_err(stale)?;
        if stable {
            let done = blocking(|| self.vol.fsync()).map_err(status)?;
            self.finish(done).await?;
        }
        Ok((self.getattr_now(id)?, stable))
    }

    async fn commit(&self, id: fileid3, _offset: u64, _count: u32) -> Result<(), nfsstat3> {
        // COMMIT comes at fsync and at close: the file's run of writes is
        // over, so its handle goes (its blob closes and uploads).
        blocking(|| self.writers.release(id));
        let done = blocking(|| self.vol.fsync()).map_err(status)?;
        self.finish(done).await
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.create_file(self.owner, dirid, filename, Some(&attr))
            .await
    }

    async fn create_with_cred(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
        cred: &UnixCred,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.create_file(self::cred(cred), dirid, filename, Some(&attr))
            .await
    }

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        self.create_file(self.owner, dirid, filename, None)
            .await
            .map(|(id, _)| id)
    }

    async fn create_exclusive_with_cred(
        &self,
        dirid: fileid3,
        filename: &filename3,
        cred: &UnixCred,
    ) -> Result<fileid3, nfsstat3> {
        self.create_file(self::cred(cred), dirid, filename, None)
            .await
            .map(|(id, _)| id)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let m = MakeRequest {
            parent: dirid,
            name: Self::name(dirname),
            kind: Kind::Directory,
            mode: 0o755,
            umask: 0,
            rdev: 0,
            target: b"",
        };
        self.make(self.owner, &m).await
    }

    async fn mkdir_with_cred(
        &self,
        dirid: fileid3,
        dirname: &filename3,
        cred: &UnixCred,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let m = MakeRequest {
            parent: dirid,
            name: Self::name(dirname),
            kind: Kind::Directory,
            mode: 0o755,
            umask: 0,
            rdev: 0,
            target: b"",
        };
        self.make(self::cred(cred), &m).await
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        let name = Self::name(filename);
        let a = blocking(|| self.vol.lookup(dirid, name)).map_err(status)?;
        // Our handle would keep an unlinked inode alive until it idles out.
        blocking(|| self.writers.release(a.ino));
        let is_dir = a.kind == Kind::Directory;
        let done = match blocking(|| self.vol.remove(dirid, name, is_dir)) {
            // A directory holding only hidden AppleDouble siblings looks
            // empty to the client: clear them, then remove it.
            Err(e)
                if is_dir && e == Errno::ENOTEMPTY && blocking(|| self.clear_siblings(a.ino))? =>
            {
                blocking(|| self.vol.remove(dirid, name, true))
            }
            r => r,
        }
        .map_err(status)?;
        self.finish(done).await
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let to = Self::name(to_filename);
        // A replaced target loses a name; release our handle on it.
        if let Ok(victim) = blocking(|| self.vol.lookup(to_dirid, to)) {
            blocking(|| self.writers.release(victim.ino));
        }
        let done = blocking(|| {
            self.vol.rename(
                from_dirid,
                Self::name(from_filename),
                to_dirid,
                to,
                RenameMode::Unspecified,
            )
        })
        .map_err(status)?;
        self.finish(done).await
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        // skyfs cookies are stable and start at 3; 0 starts the listing.
        // AppleDouble siblings are left out ([`is_apple_double`]), so read
        // until there is one visible entry past the page, or no more.
        let limit = max_entries.max(1);
        blocking(|| {
            let mut visible = Vec::with_capacity(limit + 1);
            let mut after = start_after.max(2);
            loop {
                let (batch, _parent) = self.vol.readdir(dirid, after, limit + 1)?;
                let exhausted = batch.len() <= limit;
                for e in batch {
                    after = e.cookie;
                    if !is_apple_double(&e.name) {
                        visible.push(e);
                    }
                }
                if exhausted || visible.len() > limit {
                    break;
                }
            }
            let end = visible.len() <= limit;
            let mut out = Vec::with_capacity(visible.len().min(limit));
            for e in visible.into_iter().take(limit) {
                let a = self.vol.getattr(e.ino)?;
                out.push(DirEntry {
                    fileid: e.ino,
                    cookie: e.cookie,
                    name: nfsstring(e.name),
                    attr: self.attr(&a),
                });
            }
            tracing::debug!(
                dirid,
                start_after,
                max_entries,
                returned = out.len(),
                end,
                "readdir"
            );
            Ok(ReadDirResult { entries: out, end })
        })
        .map_err(stale)
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let m = MakeRequest {
            parent: dirid,
            name: Self::name(linkname),
            kind: Kind::Symlink,
            mode: 0o777,
            umask: 0,
            rdev: 0,
            target: &symlink.0,
        };
        self.make(self.owner, &m).await
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        blocking(|| self.vol.readlink(id))
            .map(nfsstring)
            .map_err(stale)
    }

    async fn link(
        &self,
        id: fileid3,
        dirid: fileid3,
        linkname: &filename3,
    ) -> Result<fattr3, nfsstat3> {
        let done = blocking(|| self.vol.link(id, dirid, Self::name(linkname))).map_err(status)?;
        let a = self.finish(done).await?;
        Ok(self.attr(&a))
    }

    async fn fsinfo(&self, root_fileid: fileid3) -> Result<fsinfo3, nfsstat3> {
        Ok(fsinfo3 {
            obj_attributes: match self.getattr_now(root_fileid) {
                Ok(a) => post_op_attr::attributes(a),
                Err(_) => post_op_attr::Void,
            },
            rtmax: IO_SIZE,
            rtpref: IO_SIZE,
            rtmult: 4096,
            wtmax: IO_SIZE,
            wtpref: IO_SIZE,
            wtmult: 4096,
            dtpref: 64 * 1024,
            maxfilesize: i64::MAX as u64,
            time_delta: nfstime3 {
                seconds: 0,
                nseconds: 1,
            },
            properties: FSF3_LINK | FSF3_SYMLINK | FSF3_HOMOGENEOUS | FSF3_CANSETTIME,
        })
    }

    /// Volume id prefix + inode: valid across daemon restarts (inode numbers
    /// are part of the volume), and never for another volume.
    fn id_to_fh(&self, id: fileid3) -> nfs_fh3 {
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&self.fsid);
        data.extend_from_slice(&id.to_le_bytes());
        nfs_fh3 { data }
    }

    fn fh_to_id(&self, fh: &nfs_fh3) -> Result<fileid3, nfsstat3> {
        let (vol, ino) = fh
            .data
            .split_at_checked(8)
            .ok_or(nfsstat3::NFS3ERR_BADHANDLE)?;
        let ino: [u8; 8] = ino.try_into().map_err(|_| nfsstat3::NFS3ERR_BADHANDLE)?;
        if vol != self.fsid {
            return Err(nfsstat3::NFS3ERR_STALE);
        }
        Ok(u64::from_le_bytes(ino))
    }
}

// ------------------------------------------------------------ mounting

/// Whether `dir` is a mount point now (a different device than its parent).
fn is_mounted(dir: &Path) -> bool {
    let parent = dir.join("..");
    match (std::fs::metadata(dir), std::fs::metadata(parent)) {
        (Ok(d), Ok(p)) => d.dev() != p.dev(),
        _ => false,
    }
}

/// The system command that mounts the server on `port` at `dir`.
fn mount_command(port: u16, dir: &Path, args: &MountArgs) -> Command {
    let mut opts: Vec<String> = Vec::new();
    let mut cmd;
    if cfg!(target_os = "macos") {
        // Hard (a cold read may wait on the bucket), interruptible, and
        // removed if the daemon dies; locks stay local (single writer).
        opts.extend(
            [
                "vers=3",
                "tcp",
                "intr",
                "locallocks",
                "noresvport",
                &format!("port={port}"),
                &format!("mountport={port}"),
                &format!("rsize={IO_SIZE}"),
                &format!("wsize={IO_SIZE}"),
                &format!("deadtimeout={DEAD_TIMEOUT_SECS}"),
            ]
            .map(str::to_owned),
        );
        cmd = Command::new("mount_nfs");
    } else {
        opts.extend(
            [
                "vers=3",
                "proto=tcp",
                "mountproto=tcp",
                "nolock",
                "hard",
                &format!("port={port}"),
                &format!("mountport={port}"),
                &format!("rsize={IO_SIZE}"),
                &format!("wsize={IO_SIZE}"),
            ]
            .map(str::to_owned),
        );
        cmd = Command::new("mount");
        cmd.args(["-t", "nfs"]);
    }
    opts.push("nodev".into());
    opts.push("nosuid".into());
    if args.read_only {
        opts.push(
            if cfg!(target_os = "macos") {
                "rdonly"
            } else {
                "ro"
            }
            .into(),
        );
    }
    if args.atime == AtimeArg::Noatime {
        opts.push("noatime".into());
    }
    cmd.arg("-o").arg(opts.join(","));
    cmd.arg("127.0.0.1:/").arg(dir);
    cmd
}

fn run(mut cmd: Command) -> Result<(), String> {
    let out = cmd.output().map_err(|e| format!("{cmd:?}: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{cmd:?}: {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

fn umount(dir: &Path, force: bool) -> Result<(), String> {
    let mut cmd = Command::new("umount");
    if force {
        cmd.arg("-f");
    }
    cmd.arg(dir);
    run(cmd)
}

/// Mounts and serves until the mount goes away or SIGINT/SIGTERM, then
/// unmounts cleanly (drain, `MountEnd`, restore within the budget).
pub fn serve(
    client: ObjectClient,
    volume_id: [u8; 16],
    cfg: VolumeConfig,
    args: &MountArgs,
) -> Result<(), String> {
    let read_only = cfg.read_only;
    let vol = Arc::new(Volume::mount(client, volume_id, cfg).map_err(|e| e.to_string())?);
    signal::log_mounted(&vol);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.max(2))
        .thread_name("skyfs-nfs")
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let writers = Arc::new(Writers::new(vol.clone()));
    let fs = SkyNfs::new(writers.clone(), &volume_id, NfsOptions { read_only });
    let dir = args.mountpoint.clone();
    let served = rt.block_on(async {
        let listener = NFSTcpListener::bind("127.0.0.1:0", fs)
            .await
            .map_err(|e| format!("binding the NFS server: {e}"))?;
        let port = listener.get_listen_port();
        let server = tokio::spawn(async move { listener.handle_forever().await });
        let sweeper = {
            let writers = writers.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(HANDLE_IDLE / 2).await;
                    let w = writers.clone();
                    let _ = tokio::task::spawn_blocking(move || w.sweep(HANDLE_IDLE)).await;
                }
            })
        };
        let mounted = {
            let cmd = mount_command(port, &dir, args);
            tokio::task::spawn_blocking(move || run(cmd))
                .await
                .map_err(|e| e.to_string())
                .and_then(|r| r)
        };
        if let Err(e) = mounted {
            server.abort();
            sweeper.abort();
            return Err(format!("mount: {e}"));
        }
        tracing::info!(mountpoint = %dir.display(), port, "serving over NFS");

        // Until a signal, or until the mount disappears (`umount` by hand).
        let mut gone = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                () = signal::signalled() => break,
                _ = gone.tick() => {
                    let d = dir.clone();
                    if !tokio::task::spawn_blocking(move || is_mounted(&d)).await.unwrap_or(true) {
                        tracing::info!("unmounted");
                        server.abort();
                        sweeper.abort();
                        return Ok(());
                    }
                }
            }
        }
        // Signalled: unmount, which flushes and COMMITs the client's dirty
        // pages. A busy mount is retried; another signal forces it.
        tracing::info!("signalled; unmounting");
        let mut force = false;
        loop {
            let d = dir.clone();
            match tokio::task::spawn_blocking(move || umount(&d, force)).await {
                Ok(Ok(())) => break,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "unmount failed; retrying (signal again to force)");
                }
                Err(e) => tracing::warn!(error = %e, "unmount task failed"),
            }
            tokio::select! {
                () = signal::signalled() => force = true,
                () = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
        server.abort();
        sweeper.abort();
        Ok(())
    });
    // The server (and its filesystem) goes with the runtime; then the
    // handles, so nothing but `vol` holds the volume.
    rt.shutdown_timeout(Duration::from_secs(5));
    writers.release_all();
    drop(writers);
    // A failed NFS mount still ends the volume cleanly.
    let unmounted = signal::unmount_volume(vol);
    served.and(unmounted)
}

#[cfg(test)]
#[allow(clippy::many_single_char_names)] // inodes named like the paths they are
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use skyfs_store::{ObjectClient, VolumeRoot};
    use skyfs_volume::{CreateOptions, FsyncMode, Volume, VolumeConfig, create_volume};

    use super::*;

    const VID: [u8; 16] = [0x3c; 16];

    fn name(s: &str) -> filename3 {
        nfsstring(s.as_bytes().to_vec())
    }

    fn mode(m: u32) -> sattr3 {
        sattr3 {
            mode: set_mode3::mode(m),
            ..sattr3::default()
        }
    }

    struct Fixture {
        store: Arc<InMemory>,
        /// Local directories of the mounts made, kept until the end.
        dirs: Vec<tempfile::TempDir>,
    }

    impl Fixture {
        fn new() -> Self {
            let store = Arc::new(InMemory::new());
            let scratch = tempfile::tempdir().unwrap();
            let f = Self {
                store,
                dirs: vec![],
            };
            let client = f.client();
            blocking(|| create_volume(&client, VID, &CreateOptions::default(), scratch.path()))
                .unwrap();
            f
        }

        fn client(&self) -> ObjectClient {
            ObjectClient::new(self.store.clone(), VolumeRoot::new("t", &VID))
        }

        /// Mounts on a fresh local directory (another machine).
        fn mount(&mut self, read_only: bool) -> (SkyNfs, Arc<Writers>) {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = VolumeConfig::new(dir.path().to_path_buf());
            cfg.max_lag = Duration::from_millis(20);
            cfg.fsync = FsyncMode::Local;
            cfg.read_only = read_only;
            cfg.restore_on_exit_timeout = Duration::from_secs(10);
            self.dirs.push(dir);
            let client = self.client();
            let vol = blocking(|| Volume::mount(client, VID, cfg)).unwrap();
            let writers = Arc::new(Writers::new(Arc::new(vol)));
            let opts = NfsOptions { read_only };
            (SkyNfs::new(writers.clone(), &VID, opts), writers)
        }
    }

    /// Releases the handles and unmounts cleanly.
    fn unmount(fs: SkyNfs, writers: Arc<Writers>) {
        writers.release_all();
        drop(fs);
        let vol = Arc::into_inner(writers).unwrap().vol;
        blocking(|| Arc::into_inner(vol).unwrap().unmount()).unwrap();
    }

    async fn read_all(fs: &SkyNfs, id: fileid3) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let (chunk, eof) = fs.read(id, out.len() as u64, 7).await.unwrap();
            out.extend_from_slice(&chunk);
            if eof {
                return out;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unstable_writes_commit_and_survive_a_remount() {
        let mut f = Fixture::new();
        let (fs, w) = f.mount(false);
        let (id, a) = fs.create(ROOT_INO, &name("f"), mode(0o640)).await.unwrap();
        assert_eq!(a.mode, 0o640);
        assert!(matches!(a.ftype, ftype3::NF3REG));
        let (_, stable) = fs.write_stable(id, 0, b"hello ", false).await.unwrap();
        assert!(!stable, "an unstable write is answered UNSTABLE");
        let (a, stable) = fs.write_stable(id, 6, b"world", true).await.unwrap();
        assert!(stable, "a FILE_SYNC write is synced before the answer");
        assert_eq!(a.size, 11);
        fs.commit(id, 0, 0).await.unwrap();
        assert_eq!(read_all(&fs, id).await, b"hello world");
        let fh = fs.id_to_fh(id);
        unmount(fs, w);

        // Another machine: the same file handle names the same file.
        let (fs, w) = f.mount(false);
        let id2 = fs.fh_to_id(&fh).unwrap();
        assert_eq!(id2, id);
        assert_eq!(read_all(&fs, id2).await, b"hello world");
        unmount(fs, w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handles_are_released_at_commit_remove_and_idle() {
        let mut f = Fixture::new();
        let (fs, w) = f.mount(false);
        let (a, _) = fs.create(ROOT_INO, &name("a"), mode(0o644)).await.unwrap();
        assert_eq!(w.open.lock().len(), 1, "create opens a handle");
        fs.commit(a, 0, 0).await.unwrap();
        assert!(w.open.lock().is_empty(), "COMMIT (close) releases it");

        fs.write(a, 0, b"x").await.unwrap();
        assert_eq!(w.open.lock().len(), 1, "a write reopens one");
        w.sweep(Duration::ZERO);
        assert!(w.open.lock().is_empty(), "an idle handle is swept");

        // Removing a file we hold open frees it at once: no orphan.
        fs.write(a, 0, b"y").await.unwrap();
        fs.remove(ROOT_INO, &name("a")).await.unwrap();
        assert!(w.open.lock().is_empty());
        assert!(matches!(fs.getattr(a).await, Err(nfsstat3::NFS3ERR_STALE)));
        unmount(fs, w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn readdir_pages_by_cookie() {
        let mut f = Fixture::new();
        let (fs, w) = f.mount(false);
        let mut want: Vec<String> = (0..10).map(|i| format!("f{i}")).collect();
        for n in &want {
            fs.create(ROOT_INO, &name(n), mode(0o644)).await.unwrap();
        }
        let mut got = Vec::new();
        let mut cookie = 0;
        loop {
            let page = fs.readdir(ROOT_INO, cookie, 3).await.unwrap();
            assert!(page.entries.len() <= 3);
            for e in &page.entries {
                got.push(String::from_utf8(e.name.0.clone()).unwrap());
                assert!(e.cookie > cookie, "cookies grow");
                cookie = e.cookie;
            }
            if page.end {
                break;
            }
        }
        got.sort();
        want.sort();
        assert_eq!(got, want, "every entry exactly once");
        unmount(fs, w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn namespace_operations() {
        let mut f = Fixture::new();
        let (fs, w) = f.mount(false);
        let (d, da) = fs.mkdir(ROOT_INO, &name("d")).await.unwrap();
        assert!(matches!(da.ftype, ftype3::NF3DIR));
        let (x, _) = fs.create(d, &name("x"), mode(0o644)).await.unwrap();
        fs.write(x, 0, b"data").await.unwrap();

        // Hard link and symlink.
        let la = fs.link(x, ROOT_INO, &name("hard")).await.unwrap();
        assert_eq!(la.nlink, 2);
        let (s, _) = fs
            .symlink(
                ROOT_INO,
                &name("soft"),
                &nfsstring(b"d/x".to_vec()),
                &sattr3::default(),
            )
            .await
            .unwrap();
        assert_eq!(fs.readlink(s).await.unwrap().0, b"d/x");

        // Rename over an existing file replaces it.
        let (y, _) = fs.create(ROOT_INO, &name("y"), mode(0o644)).await.unwrap();
        fs.rename(d, &name("x"), ROOT_INO, &name("y"))
            .await
            .unwrap();
        assert_eq!(fs.lookup(ROOT_INO, &name("y")).await.unwrap(), x);
        assert!(matches!(fs.getattr(y).await, Err(nfsstat3::NFS3ERR_STALE)));

        // chmod, truncate, utimes.
        let a = fs
            .setattr(
                x,
                sattr3 {
                    mode: set_mode3::mode(0o600),
                    size: set_size3::size(2),
                    mtime: set_mtime::SET_TO_CLIENT_TIME(nfstime3 {
                        seconds: 1_000_000,
                        nseconds: 5,
                    }),
                    ..sattr3::default()
                },
            )
            .await
            .unwrap();
        assert_eq!((a.mode, a.size, a.mtime.seconds), (0o600, 2, 1_000_000));
        assert_eq!(read_all(&fs, x).await, b"da");

        // Errors map to NFS statuses.
        assert!(matches!(
            fs.lookup(ROOT_INO, &name("missing")).await,
            Err(nfsstat3::NFS3ERR_NOENT)
        ));
        assert!(matches!(
            fs.create(ROOT_INO, &name("y"), mode(0o644)).await,
            Err(nfsstat3::NFS3ERR_EXIST)
        ));
        fs.create(d, &name("z"), mode(0o644)).await.unwrap();
        assert!(matches!(
            fs.remove(ROOT_INO, &name("d")).await,
            Err(nfsstat3::NFS3ERR_NOTEMPTY)
        ));
        fs.remove(d, &name("z")).await.unwrap();
        fs.remove(ROOT_INO, &name("d")).await.unwrap();
        unmount(fs, w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn file_handles_are_scoped_to_the_volume() {
        let mut f = Fixture::new();
        let (fs, w) = f.mount(false);
        let fh = fs.id_to_fh(42);
        assert_eq!(fs.fh_to_id(&fh).unwrap(), 42);
        let mut other = fh.clone();
        other.data[0] ^= 1;
        assert!(matches!(fs.fh_to_id(&other), Err(nfsstat3::NFS3ERR_STALE)));
        let short = nfs_fh3 {
            data: fh.data[..12].to_vec(),
        };
        assert!(matches!(
            fs.fh_to_id(&short),
            Err(nfsstat3::NFS3ERR_BADHANDLE)
        ));
        unmount(fs, w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn apple_double_siblings_are_stored_but_not_listed() {
        let mut f = Fixture::new();
        let (fs, w) = f.mount(false);
        // Pages of visible entries with runs of hidden siblings between them.
        for i in 0..20 {
            fs.create(ROOT_INO, &name(&format!("f{i}")), mode(0o644))
                .await
                .unwrap();
            for j in 0..3 {
                fs.create(ROOT_INO, &name(&format!("._f{i}-{j}")), mode(0o644))
                    .await
                    .unwrap();
            }
        }
        let mut got = Vec::new();
        let mut cookie = 0;
        loop {
            let page = fs.readdir(ROOT_INO, cookie, 4).await.unwrap();
            assert!(page.entries.len() <= 4);
            assert!(
                !page.entries.is_empty() || page.end,
                "an empty page must be the last"
            );
            for e in &page.entries {
                got.push(String::from_utf8(e.name.0.clone()).unwrap());
                cookie = e.cookie;
            }
            if page.end {
                break;
            }
        }
        got.sort();
        let mut want: Vec<String> = (0..20).map(|i| format!("f{i}")).collect();
        want.sort();
        assert_eq!(got, want, "every visible entry once, no sibling");
        // The client still reaches a sibling by name.
        assert!(fs.lookup(ROOT_INO, &name("._f3-1")).await.is_ok());
        unmount(fs, w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rmdir_clears_leftover_siblings() {
        let mut f = Fixture::new();
        let (fs, w) = f.mount(false);
        let (d, _) = fs.mkdir(ROOT_INO, &name("d")).await.unwrap();
        fs.create(d, &name("._gone"), mode(0o644)).await.unwrap();
        // Looks empty to the client, so rmdir must succeed.
        fs.remove(ROOT_INO, &name("d")).await.unwrap();
        // A real entry still keeps a directory.
        let (d, _) = fs.mkdir(ROOT_INO, &name("e")).await.unwrap();
        fs.create(d, &name("._x"), mode(0o644)).await.unwrap();
        fs.create(d, &name("real"), mode(0o644)).await.unwrap();
        assert!(matches!(
            fs.remove(ROOT_INO, &name("e")).await,
            Err(nfsstat3::NFS3ERR_NOTEMPTY)
        ));
        assert!(fs.lookup(d, &name("._x")).await.is_ok(), "nothing removed");
        unmount(fs, w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_only_mount_says_so() {
        let mut f = Fixture::new();
        let (fs, w) = f.mount(true);
        assert!(matches!(fs.capabilities(), VFSCapabilities::ReadOnly));
        assert!(matches!(
            fs.create(ROOT_INO, &name("f"), mode(0o644)).await,
            Err(nfsstat3::NFS3ERR_ROFS)
        ));
        unmount(fs, w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reads_across_split_extents_end_at_eof() {
        // An 8 KiB file patched in place: three extents after a remount.
        let mut f = Fixture::new();
        let (fs, w) = f.mount(false);
        let (id, _) = fs.create(ROOT_INO, &name("db"), mode(0o644)).await.unwrap();
        fs.write(id, 0, &[b'x'; 8192]).await.unwrap();
        fs.commit(id, 0, 0).await.unwrap();
        fs.write(id, 4096, &[b'Y'; 8]).await.unwrap();
        fs.commit(id, 0, 0).await.unwrap();
        unmount(fs, w);
        let (fs, w) = f.mount(false);
        for (offset, count, len, eof) in [
            (0, 8192, 8192, true),
            (0, 4096, 4096, false),
            (4096, 4096, 4096, true),
            (4096, IO_SIZE, 4096, true),
            (8192, 4096, 0, true),
        ] {
            let (data, at_end) = fs.read(id, offset, count).await.unwrap();
            assert_eq!((data.len(), at_end), (len, eof), "read({offset}, {count})");
        }
        let (data, _) = fs.read(id, 4090, 16).await.unwrap();
        assert_eq!(&data, b"xxxxxxYYYYYYYYxx");
        unmount(fs, w);
    }

    #[test]
    fn conversions() {
        assert!(matches!(
            status(Errno::ENOTEMPTY),
            nfsstat3::NFS3ERR_NOTEMPTY
        ));
        assert!(matches!(status(Errno::EIO), nfsstat3::NFS3ERR_IO));
        assert!(matches!(stale(Errno::ENOENT), nfsstat3::NFS3ERR_STALE));
        let time = nfs_time(1_500_000_000_123);
        assert_eq!((time.seconds, time.nseconds), (1500, 123));
        assert_eq!(nfs_time(-5).seconds, 0, "pre-1970 clamps to the epoch");
        assert_eq!(ns_of(time), 1_500_000_000_123);
        let dev = specdata((8 << 8) | 1);
        assert_eq!((dev.specdata1, dev.specdata2), (8, 1));
    }
}
