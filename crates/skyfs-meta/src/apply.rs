//! `apply`: the one function that changes metadata, shared by the live
//! writer, mount replay and the restore replica.
//!
//! Every record is the effect of an operation the writer already validated,
//! so nothing here may fail on a valid log. Each rule below still checks the
//! identities a record carries (`Remove.ino`, `Rename.replaced_ino`, the op
//! id sequence, ...) and reports any mismatch as corruption: replay never
//! guesses.

use rusqlite::{Connection, params};
use skyfs_format::wal::v1::{
    CloneRange, DataRef, Fallocate, FallocateMode, Link, Make, MountBegin, Reclaim, Record, Remove,
    RemoveXattr, Rename, RenameMode, SetAttr, SetXattr, VolumeCreate, Write, XattrMode, data_ref,
    record::Op,
};

use crate::extents;
use crate::model::{Attr, Extent, Ino, Kind, Location, ROOT_INO};
use crate::schema::key;
use crate::store::{ApplyError, get_attr, has_entries, lookup};
use crate::{MODE_MASK, NAME_MAX};

struct Ctx<'a> {
    conn: &'a Connection,
    op_id: u64,
    time: i64,
    seq: u64,
}

type Result<T> = std::result::Result<T, ApplyError>;

impl Ctx<'_> {
    fn corrupt(&self, reason: impl Into<String>) -> ApplyError {
        ApplyError::Corrupt {
            op_id: self.op_id,
            reason: reason.into(),
        }
    }

    fn sql<T>(&self, r: rusqlite::Result<T>) -> Result<T> {
        r.map_err(|source| ApplyError::Sql {
            op_id: self.op_id,
            source,
        })
    }

    fn meta_u64(&self, k: &str) -> Result<Option<u64>> {
        use rusqlite::OptionalExtension;
        self.sql(
            self.conn
                .prepare_cached("SELECT v FROM meta WHERE k = ?1")
                .and_then(|mut s| s.query_row([k], |r| r.get(0)).optional()),
        )
    }

    fn set_meta<T: rusqlite::ToSql>(&self, k: &str, v: T) -> Result<()> {
        self.exec(
            "INSERT OR REPLACE INTO meta (k, v) VALUES (?1, ?2)",
            params![k, v],
        )
    }

    fn exec(&self, sql: &str, p: impl rusqlite::Params) -> Result<()> {
        self.sql(self.conn.prepare_cached(sql).and_then(|mut s| s.execute(p)))
            .map(|_| ())
    }

    fn attr(&self, ino: Ino) -> Result<Attr> {
        self.sql(get_attr(self.conn, ino))?
            .ok_or_else(|| self.corrupt(format!("inode {ino} does not exist")))
    }

    fn dir(&self, ino: Ino) -> Result<Attr> {
        let a = self.attr(ino)?;
        if a.kind != Kind::Directory {
            return Err(self.corrupt(format!("inode {ino} is not a directory")));
        }
        if a.nlink == 0 {
            return Err(self.corrupt(format!("directory {ino} was removed")));
        }
        Ok(a)
    }

    fn regular(&self, ino: Ino) -> Result<Attr> {
        let a = self.attr(ino)?;
        if a.kind != Kind::Regular {
            return Err(self.corrupt(format!("inode {ino} is not a regular file")));
        }
        Ok(a)
    }

    fn lookup(&self, parent: Ino, name: &[u8]) -> Result<Option<Ino>> {
        self.sql(lookup(self.conn, parent, name))
    }

    fn check_name(&self, name: &[u8]) -> Result<()> {
        if name.is_empty()
            || name.len() > NAME_MAX
            || name == b"."
            || name == b".."
            || name.iter().any(|&b| b == b'/' || b == 0)
        {
            return Err(self.corrupt(format!("invalid name {name:?}")));
        }
        Ok(())
    }

    fn add_entry(&self, parent: Ino, name: &[u8], ino: Ino) -> Result<()> {
        let cookie: u64 = self.sql(
            self.conn
                .prepare_cached("SELECT next_cookie FROM inodes WHERE ino = ?1")
                .and_then(|mut s| s.query_row([parent], |r| r.get(0))),
        )?;
        self.exec(
            "INSERT INTO dirents (parent, name, ino, cookie) VALUES (?1, ?2, ?3, ?4)",
            params![parent, name, ino, cookie],
        )?;
        self.exec(
            "UPDATE inodes SET next_cookie = next_cookie + 1 WHERE ino = ?1",
            [parent],
        )
    }

    fn remove_entry(&self, parent: Ino, name: &[u8]) -> Result<()> {
        self.exec(
            "DELETE FROM dirents WHERE parent = ?1 AND name = ?2",
            params![parent, name],
        )
    }

    fn set_entry_ino(&self, parent: Ino, name: &[u8], ino: Ino) -> Result<()> {
        self.exec(
            "UPDATE dirents SET ino = ?3 WHERE parent = ?1 AND name = ?2",
            params![parent, name, ino],
        )
    }

    fn add_nlink(&self, ino: Ino, delta: i64) -> Result<()> {
        let a = self.attr(ino)?;
        let n = i64::from(a.nlink) + delta;
        let n =
            u32::try_from(n).map_err(|_| self.corrupt(format!("nlink of {ino} out of range")))?;
        self.exec(
            "UPDATE inodes SET nlink = ?2 WHERE ino = ?1",
            params![ino, n],
        )
    }

    /// A directory's contents changed: mtime and ctime become the op time.
    fn touch_dir(&self, ino: Ino) -> Result<()> {
        self.exec(
            "UPDATE inodes SET mtime = ?2, ctime = ?2 WHERE ino = ?1",
            params![ino, self.time],
        )
    }

    fn set_ctime(&self, ino: Ino) -> Result<()> {
        self.exec(
            "UPDATE inodes SET ctime = ?2 WHERE ino = ?1",
            params![ino, self.time],
        )
    }

    fn set_parent(&self, dir: Ino, parent: Ino) -> Result<()> {
        self.exec(
            "UPDATE inodes SET parent = ?2 WHERE ino = ?1",
            params![dir, parent],
        )
    }

    fn set_size(&self, ino: Ino, size: u64) -> Result<()> {
        self.exec(
            "UPDATE inodes SET size = ?2 WHERE ino = ?1",
            params![ino, size],
        )
    }

    /// Drops one link of `a`, whose entry in `parent` is already gone.
    /// Directories go to nlink 0 and give back their `..` link.
    fn unlink_inode(&self, a: &Attr, parent: Ino) -> Result<()> {
        if a.kind == Kind::Directory {
            if self.sql(has_entries(self.conn, a.ino))? {
                return Err(self.corrupt(format!("directory {} is not empty", a.ino)));
            }
            self.exec("UPDATE inodes SET nlink = 0 WHERE ino = ?1", [a.ino])?;
            self.add_nlink(parent, -1)?;
        } else {
            self.add_nlink(a.ino, -1)?;
        }
        self.set_ctime(a.ino)
    }

    fn free_inode(&self, ino: Ino) -> Result<()> {
        self.sql(extents::drop_all(self.conn, ino))?;
        self.exec("DELETE FROM xattrs WHERE ino = ?1", [ino])?;
        self.exec("DELETE FROM inodes WHERE ino = ?1", [ino])
    }

    fn end(&self, a: u64, b: u64) -> Result<u64> {
        a.checked_add(b)
            .filter(|&e| i64::try_from(e).is_ok())
            .ok_or_else(|| self.corrupt("range overflows"))
    }
}

/// Applies `record` from segment `seq` inside the caller's transaction.
pub(crate) fn apply(conn: &Connection, record: &Record, seq: u64) -> Result<()> {
    let cx = Ctx {
        conn,
        op_id: record.op_id,
        time: record.time_ns,
        seq,
    };
    let expected = cx.meta_u64(key::NEXT_OP_ID)?.unwrap_or(0);
    if record.op_id != expected {
        return Err(cx.corrupt(format!("expected op id {expected}")));
    }
    let op = record
        .op
        .as_ref()
        .ok_or_else(|| cx.corrupt("unknown operation"))?;
    let created = cx.meta_u64(key::NEXT_INO)?.is_some()
        && cx.sql(
            conn.prepare_cached("SELECT EXISTS (SELECT 1 FROM meta WHERE k = ?1)")
                .and_then(|mut s| s.query_row([key::VOLUME_ID], |r| r.get::<_, bool>(0))),
        )?;
    match (op, created) {
        (Op::VolumeCreate(v), false) => volume_create(&cx, v)?,
        (Op::VolumeCreate(_), true) => return Err(cx.corrupt("volume created twice")),
        (_, false) => return Err(cx.corrupt("operation before VolumeCreate")),
        (Op::MountBegin(m), true) => mount_begin(&cx, m)?,
        (Op::MountEnd(_), true) => cx.set_meta(key::CLEAN, true)?,
        (Op::Make(m), true) => make(&cx, m)?,
        (Op::Link(l), true) => link(&cx, l)?,
        (Op::Remove(r), true) => remove(&cx, r)?,
        (Op::Rename(r), true) => rename(&cx, r)?,
        (Op::Write(w), true) => write(&cx, w)?,
        (Op::Fallocate(f), true) => fallocate(&cx, f)?,
        (Op::CloneRange(c), true) => clone_range(&cx, c)?,
        (Op::SetAttr(s), true) => set_attr(&cx, s)?,
        (Op::SetXattr(x), true) => set_xattr(&cx, x)?,
        (Op::RemoveXattr(x), true) => remove_xattr(&cx, x)?,
        (Op::Reclaim(r), true) => reclaim(&cx, *r)?,
    }
    cx.set_meta(key::NEXT_OP_ID, expected + 1)
}

fn volume_create(cx: &Ctx<'_>, v: &VolumeCreate) -> Result<()> {
    if cx.op_id != 0 {
        return Err(cx.corrupt("VolumeCreate must be op 0"));
    }
    if v.volume_id.len() != 16 {
        return Err(cx.corrupt("volume id must be 16 bytes"));
    }
    cx.exec(
        "INSERT INTO inodes (ino, kind, mode, uid, gid, nlink, size, atime, mtime, ctime, rdev,
                             target, parent, next_cookie)
         VALUES (?1, ?2, ?3, ?4, ?5, 2, 0, ?6, ?6, ?6, 0, NULL, ?1, 3)",
        params![
            ROOT_INO,
            Kind::Directory.to_wire() as i32,
            v.root_mode & MODE_MASK,
            v.root_uid,
            v.root_gid,
            cx.time
        ],
    )?;
    cx.set_meta(key::VOLUME_ID, v.volume_id.as_slice())?;
    cx.set_meta(key::INLINE_DATA_MAX, v.inline_data_max)?;
    cx.set_meta(key::NEXT_INO, ROOT_INO + 1)
}

fn mount_begin(cx: &Ctx<'_>, m: &MountBegin) -> Result<()> {
    let epoch = cx.meta_u64(key::EPOCH)?.unwrap_or(0);
    if m.epoch <= epoch {
        return Err(cx.corrupt(format!("epoch {} does not follow {epoch}", m.epoch)));
    }
    // Inodes unlinked but open when the previous mount ended were never
    // reclaimed; nothing can have them open now (ext4's orphan list).
    let orphans: Vec<Ino> = cx.sql(
        cx.conn
            .prepare_cached("SELECT ino FROM inodes WHERE nlink = 0")
            .and_then(|mut s| s.query_map([], |r| r.get(0))?.collect()),
    )?;
    for ino in orphans {
        cx.free_inode(ino)?;
    }
    cx.set_meta(key::CLEAN, false)?;
    cx.set_meta(key::EPOCH, m.epoch)
}

fn make(cx: &Ctx<'_>, m: &Make) -> Result<()> {
    cx.check_name(&m.name)?;
    cx.dir(m.parent)?;
    if cx.lookup(m.parent, &m.name)?.is_some() {
        return Err(cx.corrupt("Make target exists"));
    }
    let next_ino = cx.meta_u64(key::NEXT_INO)?.unwrap_or(2);
    if m.ino < next_ino {
        return Err(cx.corrupt(format!("inode {} reused (next is {next_ino})", m.ino)));
    }
    let kind = Kind::from_wire(m.kind).ok_or_else(|| cx.corrupt("bad kind"))?;
    let is_dir = kind == Kind::Directory;
    let target = (kind == Kind::Symlink).then_some(m.symlink_target.as_slice());
    let size = target.map_or(0, |t| t.len() as u64);
    let rdev = if matches!(kind, Kind::CharDevice | Kind::BlockDevice) {
        m.rdev
    } else {
        0
    };
    cx.exec(
        "INSERT INTO inodes (ino, kind, mode, uid, gid, nlink, size, atime, mtime, ctime, rdev,
                             target, parent, next_cookie)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?8, ?9, ?10, ?11, 3)",
        params![
            m.ino,
            kind.to_wire() as i32,
            m.mode & MODE_MASK,
            m.uid,
            m.gid,
            if is_dir { 2 } else { 1 },
            size,
            cx.time,
            rdev,
            target,
            if is_dir { m.parent } else { 0 },
        ],
    )?;
    cx.set_meta(key::NEXT_INO, m.ino + 1)?;
    cx.add_entry(m.parent, &m.name, m.ino)?;
    if is_dir {
        cx.add_nlink(m.parent, 1)?;
    }
    cx.touch_dir(m.parent)
}

fn link(cx: &Ctx<'_>, l: &Link) -> Result<()> {
    cx.check_name(&l.new_name)?;
    cx.dir(l.new_parent)?;
    let a = cx.attr(l.ino)?;
    if a.kind == Kind::Directory || a.nlink == 0 {
        return Err(cx.corrupt("Link of a directory or an unlinked inode"));
    }
    if cx.lookup(l.new_parent, &l.new_name)?.is_some() {
        return Err(cx.corrupt("Link target exists"));
    }
    cx.add_entry(l.new_parent, &l.new_name, l.ino)?;
    cx.add_nlink(l.ino, 1)?;
    cx.set_ctime(l.ino)?;
    cx.touch_dir(l.new_parent)
}

fn remove(cx: &Ctx<'_>, r: &Remove) -> Result<()> {
    cx.dir(r.parent)?;
    if cx.lookup(r.parent, &r.name)? != Some(r.ino) {
        return Err(cx.corrupt(format!("Remove expected inode {}", r.ino)));
    }
    let a = cx.attr(r.ino)?;
    if (a.kind == Kind::Directory) != r.is_dir {
        return Err(cx.corrupt("Remove.is_dir does not match the inode"));
    }
    cx.remove_entry(r.parent, &r.name)?;
    cx.unlink_inode(&a, r.parent)?;
    cx.touch_dir(r.parent)
}

/// Whether `dir` is `ancestor` or lies below it.
fn is_within(cx: &Ctx<'_>, mut dir: Ino, ancestor: Ino) -> Result<bool> {
    for _ in 0..=u16::MAX {
        if dir == ancestor {
            return Ok(true);
        }
        if dir == ROOT_INO {
            return Ok(false);
        }
        dir = cx.sql(
            cx.conn
                .prepare_cached("SELECT parent FROM inodes WHERE ino = ?1")
                .and_then(|mut s| s.query_row([dir], |r| r.get(0))),
        )?;
    }
    Err(cx.corrupt("directory parent chain too deep or cyclic"))
}

fn rename(cx: &Ctx<'_>, r: &Rename) -> Result<()> {
    cx.check_name(&r.new_name)?;
    cx.dir(r.old_parent)?;
    cx.dir(r.new_parent)?;
    if cx.lookup(r.old_parent, &r.old_name)? != Some(r.ino) {
        return Err(cx.corrupt(format!("Rename expected source inode {}", r.ino)));
    }
    if r.old_parent == r.new_parent && r.old_name == r.new_name {
        return Err(cx.corrupt("Rename onto itself"));
    }
    let src = cx.attr(r.ino)?;
    let dst = cx.lookup(r.new_parent, &r.new_name)?;
    let moves_dir = |a: &Attr| a.kind == Kind::Directory && r.old_parent != r.new_parent;
    if src.kind == Kind::Directory && is_within(cx, r.new_parent, r.ino)? {
        return Err(cx.corrupt("Rename of a directory into itself"));
    }

    let mode = RenameMode::try_from(r.mode).map_err(|_| cx.corrupt("bad rename mode"))?;
    if mode == RenameMode::Exchange {
        if r.replaced_ino == 0 || dst != Some(r.replaced_ino) {
            return Err(cx.corrupt("Exchange target mismatch"));
        }
        let other = cx.attr(r.replaced_ino)?;
        if other.kind == Kind::Directory && is_within(cx, r.old_parent, other.ino)? {
            return Err(cx.corrupt("Exchange of a directory into itself"));
        }
        cx.set_entry_ino(r.old_parent, &r.old_name, other.ino)?;
        cx.set_entry_ino(r.new_parent, &r.new_name, src.ino)?;
        if moves_dir(&src) {
            cx.set_parent(src.ino, r.new_parent)?;
            cx.add_nlink(r.old_parent, -1)?;
            cx.add_nlink(r.new_parent, 1)?;
        }
        if moves_dir(&other) {
            cx.set_parent(other.ino, r.old_parent)?;
            cx.add_nlink(r.new_parent, -1)?;
            cx.add_nlink(r.old_parent, 1)?;
        }
        cx.set_ctime(other.ino)?;
    } else {
        match (dst, r.replaced_ino) {
            (None, 0) => {}
            (Some(d), expected) if d == expected && mode == RenameMode::Unspecified => {
                if d == src.ino {
                    return Err(cx.corrupt("Rename between links of one inode"));
                }
                let victim = cx.attr(d)?;
                if (victim.kind == Kind::Directory) != (src.kind == Kind::Directory) {
                    return Err(cx.corrupt("Rename replaces across file types"));
                }
                cx.remove_entry(r.new_parent, &r.new_name)?;
                cx.unlink_inode(&victim, r.new_parent)?;
            }
            _ => return Err(cx.corrupt("Rename target mismatch")),
        }
        if r.old_parent == r.new_parent {
            // Renamed in place: the entry keeps its readdir cookie, so a
            // listing in progress neither revisits it nor runs forever
            // while a program renames what it lists.
            cx.exec(
                "UPDATE dirents SET name = ?3 WHERE parent = ?1 AND name = ?2",
                params![r.old_parent, r.old_name, r.new_name],
            )?;
        } else {
            cx.remove_entry(r.old_parent, &r.old_name)?;
            cx.add_entry(r.new_parent, &r.new_name, src.ino)?;
        }
        if moves_dir(&src) {
            cx.set_parent(src.ino, r.new_parent)?;
            cx.add_nlink(r.old_parent, -1)?;
            cx.add_nlink(r.new_parent, 1)?;
        }
    }
    cx.set_ctime(src.ino)?;
    cx.touch_dir(r.old_parent)?;
    if r.new_parent != r.old_parent {
        cx.touch_dir(r.new_parent)?;
    }
    Ok(())
}

fn data_location(cx: &Ctx<'_>, data: Option<&DataRef>, len: u64) -> Result<(Location, u64)> {
    let (loc, off, chunk_len) = match data.and_then(|d| d.location.as_ref()) {
        Some(data_ref::Location::Inline(i)) => (Location::Segment { seq: cx.seq }, i.offset, i.len),
        Some(data_ref::Location::Blob(b)) => {
            (Location::Object { key: b.key.clone() }, b.offset, b.len)
        }
        None => return Err(cx.corrupt("Write without data")),
    };
    if chunk_len != len {
        return Err(cx.corrupt("Write length differs from its data"));
    }
    Ok((loc, off))
}

fn write(cx: &Ctx<'_>, w: &Write) -> Result<()> {
    let a = cx.regular(w.ino)?;
    if w.len == 0 {
        return Err(cx.corrupt("empty Write"));
    }
    let end = cx.end(w.offset, w.len)?;
    let (loc, chunk_off) = data_location(cx, w.data.as_ref(), w.len)?;
    cx.sql(extents::punch(cx.conn, w.ino, w.offset, end))?;
    cx.sql(extents::insert(
        cx.conn,
        w.ino,
        &Extent {
            off: w.offset,
            len: w.len,
            loc,
            chunk_off,
            chunk_len: w.len,
            crc: w.crc32c,
            skip: 0,
        },
    ))?;
    let mut mode = a.mode;
    if w.cleared_setid {
        mode &= !crate::mode::S_ISUID;
        if mode & crate::mode::S_IXGRP != 0 {
            mode &= !crate::mode::S_ISGID;
        }
    }
    cx.exec(
        "UPDATE inodes SET size = ?2, mtime = ?3, ctime = ?3, mode = ?4 WHERE ino = ?1",
        params![w.ino, a.size.max(end), cx.time, mode],
    )
}

fn fallocate(cx: &Ctx<'_>, f: &Fallocate) -> Result<()> {
    let a = cx.regular(f.ino)?;
    let end = cx.end(f.offset, f.len)?;
    let mode = FallocateMode::try_from(f.mode).map_err(|_| cx.corrupt("bad fallocate mode"))?;
    let new_size = match mode {
        FallocateMode::Unspecified => a.size.max(end),
        FallocateMode::KeepSize => a.size,
        FallocateMode::PunchHole | FallocateMode::ZeroRangeKeepSize => {
            cx.sql(extents::punch(cx.conn, f.ino, f.offset, end.min(a.size)))?;
            a.size
        }
        FallocateMode::ZeroRange => {
            cx.sql(extents::punch(cx.conn, f.ino, f.offset, end))?;
            a.size.max(end)
        }
        FallocateMode::CollapseRange => {
            if end >= a.size {
                return Err(cx.corrupt("collapse reaches end of file"));
            }
            cx.sql(extents::punch(cx.conn, f.ino, f.offset, end))?;
            cx.sql(extents::shift(cx.conn, f.ino, end, -i128::from(f.len)))?;
            a.size - f.len
        }
        FallocateMode::InsertRange => {
            if f.offset >= a.size {
                return Err(cx.corrupt("insert at or past end of file"));
            }
            cx.sql(extents::split_at(cx.conn, f.ino, f.offset))?;
            cx.sql(extents::shift(cx.conn, f.ino, f.offset, i128::from(f.len)))?;
            cx.end(a.size, f.len)?
        }
    };
    if mode == FallocateMode::KeepSize || (mode == FallocateMode::Unspecified && new_size == a.size)
    {
        return Ok(());
    }
    cx.exec(
        "UPDATE inodes SET size = ?2, mtime = ?3, ctime = ?3 WHERE ino = ?1",
        params![f.ino, new_size, cx.time],
    )
}

fn clone_range(cx: &Ctx<'_>, c: &CloneRange) -> Result<()> {
    let src = cx.regular(c.src_ino)?;
    let dst = cx.regular(c.dst_ino)?;
    let whole = c.len == 0 && c.src_offset == 0 && c.dst_offset == 0;
    let new_size = if whole {
        let pieces = cx.sql(extents::all(cx.conn, c.src_ino))?;
        cx.sql(extents::drop_all(cx.conn, c.dst_ino))?;
        for e in pieces {
            cx.sql(extents::insert(cx.conn, c.dst_ino, &e))?;
        }
        src.size
    } else {
        let src_end = cx.end(c.src_offset, c.len)?;
        let dst_end = cx.end(c.dst_offset, c.len)?;
        if src_end > src.size {
            return Err(cx.corrupt("clone source past end of file"));
        }
        if c.src_ino == c.dst_ino && c.src_offset < dst_end && c.dst_offset < src_end {
            return Err(cx.corrupt("clone ranges overlap"));
        }
        let pieces: Vec<Extent> = cx
            .sql(extents::overlapping(
                cx.conn,
                c.src_ino,
                c.src_offset,
                src_end,
            ))?
            .into_iter()
            .filter_map(|e| e.clip(c.src_offset, src_end))
            .collect();
        cx.sql(extents::punch(cx.conn, c.dst_ino, c.dst_offset, dst_end))?;
        for mut e in pieces {
            e.off = e.off - c.src_offset + c.dst_offset;
            cx.sql(extents::insert(cx.conn, c.dst_ino, &e))?;
        }
        dst.size.max(dst_end)
    };
    cx.exec(
        "UPDATE inodes SET size = ?2, mtime = ?3, ctime = ?3 WHERE ino = ?1",
        params![c.dst_ino, new_size, cx.time],
    )
}

fn set_attr(cx: &Ctx<'_>, s: &SetAttr) -> Result<()> {
    let a = cx.attr(s.ino)?;
    if let Some(size) = s.size {
        if a.kind != Kind::Regular {
            return Err(cx.corrupt("size change of a non-regular file"));
        }
        if i64::try_from(size).is_err() {
            return Err(cx.corrupt("size out of range"));
        }
        if size < a.size {
            cx.sql(extents::punch(cx.conn, s.ino, size, a.size))?;
        }
        cx.set_size(s.ino, size)?;
    }
    cx.exec(
        "UPDATE inodes SET mode = ?2, uid = ?3, gid = ?4, atime = ?5, mtime = ?6, ctime = ?7
         WHERE ino = ?1",
        params![
            s.ino,
            s.mode.map_or(a.mode, |m| m & MODE_MASK),
            s.uid.unwrap_or(a.uid),
            s.gid.unwrap_or(a.gid),
            s.atime_ns.unwrap_or(a.atime_ns),
            s.mtime_ns.unwrap_or(a.mtime_ns),
            s.ctime_ns.unwrap_or(cx.time),
        ],
    )
}

fn set_xattr(cx: &Ctx<'_>, x: &SetXattr) -> Result<()> {
    cx.attr(x.ino)?;
    let exists: bool = cx.sql(
        cx.conn
            .prepare_cached("SELECT EXISTS (SELECT 1 FROM xattrs WHERE ino = ?1 AND name = ?2)")
            .and_then(|mut s| s.query_row(params![x.ino, x.name], |r| r.get(0))),
    )?;
    match XattrMode::try_from(x.mode).map_err(|_| cx.corrupt("bad xattr mode"))? {
        XattrMode::Create if exists => return Err(cx.corrupt("XATTR_CREATE of an existing xattr")),
        XattrMode::Replace if !exists => return Err(cx.corrupt("XATTR_REPLACE of a missing xattr")),
        _ => {}
    }
    if x.name.is_empty() {
        return Err(cx.corrupt("empty xattr name"));
    }
    cx.exec(
        "INSERT OR REPLACE INTO xattrs (ino, name, value) VALUES (?1, ?2, ?3)",
        params![x.ino, x.name, x.value],
    )?;
    cx.set_ctime(x.ino)
}

fn remove_xattr(cx: &Ctx<'_>, x: &RemoveXattr) -> Result<()> {
    cx.attr(x.ino)?;
    let n = cx.sql(
        cx.conn
            .prepare_cached("DELETE FROM xattrs WHERE ino = ?1 AND name = ?2")
            .and_then(|mut s| s.execute(params![x.ino, x.name])),
    )?;
    if n != 1 {
        return Err(cx.corrupt("RemoveXattr of a missing xattr"));
    }
    cx.set_ctime(x.ino)
}

fn reclaim(cx: &Ctx<'_>, r: Reclaim) -> Result<()> {
    let a = cx.attr(r.ino)?;
    if a.nlink != 0 || r.ino == ROOT_INO {
        return Err(cx.corrupt(format!("Reclaim of linked inode {}", r.ino)));
    }
    cx.free_inode(r.ino)
}
