//! Validation: turns a POSIX request into a log operation, or an errno.
//!
//! "Validate, then log, then apply." Each function here reads the current
//! state, checks what the operation requires (existence, types, emptiness,
//! name rules, ...) and returns the operation with every effect resolved:
//! inode numbers allocated, gid inherited from setgid directories, umask
//! applied, offsets fixed. A request that fails returns an errno and is
//! never logged; an operation returned here always applies.
//!
//! Permission checks are not here: the kernel performs them
//! (`default_permissions`) before the request reaches skyfs.

use skyfs_format::wal::v1::{
    BlobRange, CloneRange, DataRef, Fallocate, FallocateMode, FileKind, InlineRange, Link, Make,
    Reclaim, Remove, RemoveXattr, Rename, RenameMode, SetAttr, SetXattr, Write, XattrMode,
    data_ref, record::Op,
};

use crate::errno::Errno;
use crate::model::{Attr, Ino, Kind, ROOT_INO};
use crate::store::{MetaError, MetaStore};
use crate::{MODE_MASK, NAME_MAX};

type Result<T> = std::result::Result<T, Errno>;

/// Largest file offset or size.
pub const MAX_FILE_SIZE: u64 = i64::MAX as u64;

/// Largest xattr value.
pub const XATTR_SIZE_MAX: usize = 64 * 1024;

/// Internal store failures surface as EIO; they are never logged.
#[allow(clippy::needless_pass_by_value)]
fn io(_: MetaError) -> Errno {
    Errno::EIO
}

/// Checks a directory entry name.
pub fn check_name(name: &[u8]) -> Result<()> {
    if name.len() > NAME_MAX {
        return Err(Errno::ENAMETOOLONG);
    }
    if name.is_empty() || name == b"." || name == b".." || name.iter().any(|&b| b == b'/' || b == 0)
    {
        return Err(Errno::EINVAL);
    }
    Ok(())
}

fn attr(store: &MetaStore, ino: Ino) -> Result<Attr> {
    store.getattr(ino).map_err(io)?.ok_or(Errno::ENOENT)
}

/// A live directory: exists, is a directory and has not been removed.
fn dir(store: &MetaStore, ino: Ino) -> Result<Attr> {
    let a = attr(store, ino)?;
    if a.kind != Kind::Directory {
        return Err(Errno::ENOTDIR);
    }
    if a.nlink == 0 {
        return Err(Errno::ENOENT);
    }
    Ok(a)
}

fn regular(store: &MetaStore, ino: Ino) -> Result<Attr> {
    let a = attr(store, ino)?;
    match a.kind {
        Kind::Regular => Ok(a),
        Kind::Directory => Err(Errno::EISDIR),
        _ => Err(Errno::EINVAL),
    }
}

/// Who is asking. uid and gid come from the FUSE request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caller {
    /// Effective uid.
    pub uid: u32,
    /// Effective gid.
    pub gid: u32,
}

/// A request to create an inode.
#[derive(Debug, Clone)]
pub struct MakeRequest<'a> {
    /// Directory to create in.
    pub parent: Ino,
    /// New entry name.
    pub name: &'a [u8],
    /// Inode type.
    pub kind: Kind,
    /// Requested permission bits (`S_IFMT` ignored).
    pub mode: u32,
    /// Caller's umask, applied here.
    pub umask: u32,
    /// Device number, for device nodes.
    pub rdev: u64,
    /// Target, for symlinks.
    pub target: &'a [u8],
}

/// `creat`, `mkdir`, `mknod`, `symlink`. Allocates the inode number.
pub fn make(store: &MetaStore, caller: Caller, req: &MakeRequest<'_>) -> Result<Op> {
    check_name(req.name)?;
    let parent = dir(store, req.parent)?;
    if store.lookup(req.parent, req.name).map_err(io)?.is_some() {
        return Err(Errno::EEXIST);
    }
    if parent.nlink == u32::MAX && req.kind == Kind::Directory {
        return Err(Errno::EMLINK);
    }
    if req.kind == Kind::Symlink && (req.target.is_empty() || req.target.len() > 4095) {
        return Err(if req.target.is_empty() {
            Errno::ENOENT
        } else {
            Errno::ENAMETOOLONG
        });
    }
    let mut mode = if req.kind == Kind::Symlink {
        0o777
    } else {
        req.mode & MODE_MASK & !req.umask
    };
    let mut gid = caller.gid;
    if parent.mode & crate::mode::S_ISGID != 0 {
        // setgid directory: new entries take its group, and new
        // directories inherit the bit.
        gid = parent.gid;
        if req.kind == Kind::Directory {
            mode |= crate::mode::S_ISGID;
        }
    }
    Ok(Op::Make(Make {
        parent: req.parent,
        name: req.name.to_vec(),
        ino: store.next_ino().map_err(io)?,
        kind: req.kind.to_wire() as i32,
        mode,
        uid: caller.uid,
        gid,
        rdev: if matches!(req.kind, Kind::CharDevice | Kind::BlockDevice) {
            req.rdev
        } else {
            0
        },
        symlink_target: if req.kind == Kind::Symlink {
            req.target.to_vec()
        } else {
            Vec::new()
        },
    }))
}

/// `link(2)`.
pub fn link(store: &MetaStore, ino: Ino, new_parent: Ino, new_name: &[u8]) -> Result<Op> {
    check_name(new_name)?;
    let a = attr(store, ino)?;
    if a.kind == Kind::Directory {
        return Err(Errno::EPERM);
    }
    if a.nlink == 0 {
        return Err(Errno::ENOENT);
    }
    if a.nlink == u32::MAX {
        return Err(Errno::EMLINK);
    }
    dir(store, new_parent)?;
    if store.lookup(new_parent, new_name).map_err(io)?.is_some() {
        return Err(Errno::EEXIST);
    }
    Ok(Op::Link(Link {
        ino,
        new_parent,
        new_name: new_name.to_vec(),
    }))
}

/// `unlink(2)` (`is_dir` false) or `rmdir(2)` (true).
pub fn remove(store: &MetaStore, parent: Ino, name: &[u8], is_dir: bool) -> Result<Op> {
    if is_dir && (name == b"." || name == b"..") {
        return Err(if name == b"." {
            Errno::EINVAL
        } else {
            Errno::ENOTEMPTY
        });
    }
    check_name(name)?;
    dir(store, parent)?;
    let ino = store
        .lookup(parent, name)
        .map_err(io)?
        .ok_or(Errno::ENOENT)?;
    let a = attr(store, ino)?;
    match (a.kind == Kind::Directory, is_dir) {
        (true, false) => return Err(Errno::EISDIR),
        (false, true) => return Err(Errno::ENOTDIR),
        (true, true) if store.has_entries(ino).map_err(io)? => return Err(Errno::ENOTEMPTY),
        _ => {}
    }
    Ok(Op::Remove(Remove {
        parent,
        name: name.to_vec(),
        ino,
        is_dir,
    }))
}

/// Whether directory `dir` is `ancestor` or below it.
fn is_within(store: &MetaStore, mut dir: Ino, ancestor: Ino) -> Result<bool> {
    loop {
        if dir == ancestor {
            return Ok(true);
        }
        if dir == ROOT_INO {
            return Ok(false);
        }
        dir = store.parent(dir).map_err(io)?.ok_or(Errno::EIO)?;
    }
}

/// `rename(2)` / `renameat2(2)`. Returns `None` when POSIX says the call
/// succeeds without doing anything (both names are links to one inode).
pub fn rename(
    store: &MetaStore,
    old_parent: Ino,
    old_name: &[u8],
    new_parent: Ino,
    new_name: &[u8],
    mode: RenameMode,
) -> Result<Option<Op>> {
    check_name(old_name)?;
    check_name(new_name)?;
    dir(store, old_parent)?;
    dir(store, new_parent)?;
    let ino = store
        .lookup(old_parent, old_name)
        .map_err(io)?
        .ok_or(Errno::ENOENT)?;
    let src = attr(store, ino)?;
    let dst = store.lookup(new_parent, new_name).map_err(io)?;

    if src.kind == Kind::Directory && is_within(store, new_parent, ino)? {
        return Err(Errno::EINVAL);
    }
    let replaced_ino = match (mode, dst) {
        (RenameMode::Exchange, None) => return Err(Errno::ENOENT),
        (RenameMode::Noreplace, Some(_)) => return Err(Errno::EEXIST),
        (_, None) => 0,
        (_, Some(d)) if d == ino => return Ok(None),
        (RenameMode::Exchange, Some(d)) => {
            let other = attr(store, d)?;
            if other.kind == Kind::Directory && is_within(store, old_parent, d)? {
                return Err(Errno::EINVAL);
            }
            d
        }
        (_, Some(d)) => {
            let victim = attr(store, d)?;
            match (src.kind == Kind::Directory, victim.kind == Kind::Directory) {
                (true, false) => return Err(Errno::ENOTDIR),
                (false, true) => return Err(Errno::EISDIR),
                (true, true) if store.has_entries(d).map_err(io)? => return Err(Errno::ENOTEMPTY),
                _ => d,
            }
        }
    };
    Ok(Some(Op::Rename(Rename {
        old_parent,
        old_name: old_name.to_vec(),
        new_parent,
        new_name: new_name.to_vec(),
        mode: mode as i32,
        ino,
        replaced_ino,
    })))
}

/// Where the bytes of a write were placed by the writer.
#[derive(Debug, Clone)]
pub enum WriteData {
    /// In the open segment's data region.
    Inline {
        /// Offset in the data region.
        offset: u64,
    },
    /// In a blob, uploaded before the segment commits.
    Blob {
        /// Volume-relative key of the blob.
        key: String,
        /// Offset of the bytes in the blob.
        offset: u64,
    },
}

/// Validates a write of `len` bytes at `offset` to `ino`. The caller
/// places the bytes and then calls [`write_op`].
pub fn check_write(store: &MetaStore, ino: Ino, offset: u64, len: u64) -> Result<Attr> {
    let a = regular(store, ino)?;
    match offset.checked_add(len) {
        Some(end) if end <= MAX_FILE_SIZE => Ok(a),
        _ => Err(Errno::EFBIG),
    }
}

/// The `Write` record for bytes already placed.
#[must_use]
pub fn write_op(ino: Ino, offset: u64, bytes: &[u8], data: WriteData) -> Op {
    let len = bytes.len() as u64;
    let location = match data {
        WriteData::Inline { offset } => data_ref::Location::Inline(InlineRange { offset, len }),
        WriteData::Blob { key, offset } => data_ref::Location::Blob(BlobRange { key, offset, len }),
    };
    Op::Write(Write {
        ino,
        offset,
        len,
        data: Some(DataRef {
            location: Some(location),
        }),
        crc32c: crc32c::crc32c(bytes),
        cleared_setid: false,
    })
}

/// Requested attribute changes (`setattr`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttrChanges {
    /// New permission bits.
    pub mode: Option<u32>,
    /// New owner.
    pub uid: Option<u32>,
    /// New group.
    pub gid: Option<u32>,
    /// New size (truncate).
    pub size: Option<u64>,
    /// New atime, already resolved from `UTIME_NOW`.
    pub atime_ns: Option<i64>,
    /// New mtime, already resolved from `UTIME_NOW`.
    pub mtime_ns: Option<i64>,
}

/// `chmod`, `chown`, `truncate`, `utimensat`. `now` is the writer clock,
/// used for the mtime a size change implies.
pub fn set_attr(store: &MetaStore, ino: Ino, ch: &AttrChanges, now: i64) -> Result<Op> {
    let a = attr(store, ino)?;
    let mut mode = ch.mode.map(|m| m & MODE_MASK);
    if let Some(size) = ch.size {
        match a.kind {
            Kind::Regular => {}
            Kind::Directory => return Err(Errno::EISDIR),
            _ => return Err(Errno::EINVAL),
        }
        if size > MAX_FILE_SIZE {
            return Err(Errno::EFBIG);
        }
    }
    // chown by anyone clears set-id bits of non-directories, as Linux does
    // (the kernel leaves this to the filesystem for FUSE).
    let owner_changes = ch.uid.is_some_and(|u| u != a.uid) || ch.gid.is_some_and(|g| g != a.gid);
    if owner_changes && a.kind != Kind::Directory && mode.is_none() {
        let setid = crate::mode::S_ISUID | crate::mode::S_ISGID;
        if a.mode & setid != 0 {
            let mut m = a.mode & !crate::mode::S_ISUID;
            if m & crate::mode::S_IXGRP != 0 {
                m &= !crate::mode::S_ISGID;
            }
            mode = Some(m);
        }
    }
    let mtime_ns = ch.mtime_ns.or(ch.size.map(|_| now));
    Ok(Op::SetAttr(SetAttr {
        ino,
        mode,
        uid: ch.uid,
        gid: ch.gid,
        size: ch.size,
        atime_ns: ch.atime_ns,
        mtime_ns,
        ctime_ns: None,
    }))
}

/// `fallocate(2)`.
pub fn fallocate(
    store: &MetaStore,
    ino: Ino,
    offset: u64,
    len: u64,
    mode: FallocateMode,
) -> Result<Op> {
    let a = regular(store, ino)?;
    if len == 0 {
        return Err(Errno::EINVAL);
    }
    let end = offset
        .checked_add(len)
        .filter(|&e| e <= MAX_FILE_SIZE)
        .ok_or(Errno::EFBIG)?;
    match mode {
        FallocateMode::CollapseRange if end >= a.size => return Err(Errno::EINVAL),
        FallocateMode::InsertRange if offset >= a.size => return Err(Errno::EINVAL),
        FallocateMode::InsertRange if a.size.checked_add(len).is_none_or(|s| s > MAX_FILE_SIZE) => {
            return Err(Errno::EFBIG);
        }
        _ => {}
    }
    Ok(Op::Fallocate(Fallocate {
        ino,
        offset,
        len,
        mode: mode as i32,
    }))
}

/// `copy_file_range(2)`, `FICLONE`, `FICLONERANGE`. `len` is clamped to
/// the source's end; `None` when nothing is left to copy.
pub fn clone_range(
    store: &MetaStore,
    src: Ino,
    src_offset: u64,
    dst: Ino,
    dst_offset: u64,
    len: u64,
) -> Result<Option<(Op, u64)>> {
    let s = regular(store, src)?;
    regular(store, dst)?;
    let len = len.min(s.size.saturating_sub(src_offset));
    if len == 0 {
        return Ok(None);
    }
    if dst_offset
        .checked_add(len)
        .is_none_or(|e| e > MAX_FILE_SIZE)
    {
        return Err(Errno::EFBIG);
    }
    if src == dst && src_offset < dst_offset + len && dst_offset < src_offset + len {
        return Err(Errno::EINVAL);
    }
    // len 0 with zero offsets means "whole file" on the wire; a real
    // range starting at 0 is spelled out.
    Ok(Some((
        Op::CloneRange(CloneRange {
            src_ino: src,
            src_offset,
            dst_ino: dst,
            dst_offset,
            len,
        }),
        len,
    )))
}

/// `setxattr(2)`.
pub fn set_xattr(
    store: &MetaStore,
    ino: Ino,
    name: &[u8],
    value: &[u8],
    mode: XattrMode,
) -> Result<Op> {
    attr(store, ino)?;
    if name.is_empty() || name.len() > 255 {
        return Err(if name.is_empty() {
            Errno::EINVAL
        } else {
            Errno::ERANGE
        });
    }
    if value.len() > XATTR_SIZE_MAX {
        return Err(Errno::ERANGE);
    }
    let exists = store.getxattr(ino, name).map_err(io)?.is_some();
    match mode {
        XattrMode::Create if exists => return Err(Errno::EEXIST),
        XattrMode::Replace if !exists => return Err(Errno::ENODATA),
        _ => {}
    }
    Ok(Op::SetXattr(SetXattr {
        ino,
        name: name.to_vec(),
        value: value.to_vec(),
        mode: mode as i32,
    }))
}

/// `removexattr(2)`.
pub fn remove_xattr(store: &MetaStore, ino: Ino, name: &[u8]) -> Result<Op> {
    attr(store, ino)?;
    if store.getxattr(ino, name).map_err(io)?.is_none() {
        return Err(Errno::ENODATA);
    }
    Ok(Op::RemoveXattr(RemoveXattr {
        ino,
        name: name.to_vec(),
    }))
}

/// Frees an inode at nlink 0 whose last handle closed. `None` if it is
/// still linked (or already gone).
pub fn reclaim(store: &MetaStore, ino: Ino) -> Result<Option<Op>> {
    Ok(match store.getattr(ino).map_err(io)? {
        Some(a) if a.nlink == 0 && ino != ROOT_INO => Some(Op::Reclaim(Reclaim { ino })),
        _ => None,
    })
}

/// The kind of a `mknod` mode, or `EINVAL`.
pub fn kind_of_mode(mode: u32) -> Result<Kind> {
    let kind = Kind::from_mode(mode).ok_or(Errno::EINVAL)?;
    debug_assert_ne!(kind.to_wire(), FileKind::Unspecified);
    Ok(kind)
}
