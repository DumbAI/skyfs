//! The SQLite metadata store: live store, restore replica and snapshot file.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::Path;

use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, params};
use skyfs_format::wal::v1::Record;

use crate::model::{Attr, DirEntry, Extent, Ino, Kind, Location, Position, StoreStats, VolumeInfo};
use crate::schema::{self, SCHEMA_VERSION, key};

/// A store-level failure that is not a property of the log: I/O, SQLite.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    /// SQLite failed.
    #[error("sqlite: {0}")]
    Sql(#[from] rusqlite::Error),
    /// The file is not a skyfs metadata store this build can read.
    #[error("unsupported metadata schema version {0}")]
    SchemaVersion(u32),
}

/// Applying a record failed. For a record from the log this means the log
/// or the store is corrupt, and the mount must be refused.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// The record contradicts the state (identity mismatch, missing inode,
    /// broken op id sequence, ...).
    #[error("corrupt log at op {op_id}: {reason}")]
    Corrupt {
        /// The record's op id.
        op_id: u64,
        /// What did not hold.
        reason: String,
    },
    /// SQLite failed while applying.
    #[error("sqlite while applying op {op_id}: {source}")]
    Sql {
        /// The record's op id.
        op_id: u64,
        /// The SQLite error.
        source: rusqlite::Error,
    },
}

/// Moves one extent onto new bytes, used by the restorer to repack inline
/// data into packs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relocation {
    /// The inode.
    pub ino: Ino,
    /// The extent's file offset (its key).
    pub off: u64,
    /// The extent's length, checked against the stored one.
    pub len: u64,
    /// New location; the new chunk is exactly the extent's bytes.
    pub key: String,
    /// Chunk start in the new object.
    pub chunk_off: u64,
    /// CRC32C of the extent's bytes.
    pub crc: u32,
}

/// The metadata store. One connection; the single writer owns it.
#[derive(Debug)]
pub struct MetaStore {
    pub(crate) conn: Connection,
}

const ATTR_COLS: &str = "ino, kind, mode, uid, gid, nlink, size, atime, mtime, ctime, rdev";
const EXTENT_COLS: &str = "off, len, seg, obj, chunk_off, chunk_len, crc, skip";

impl MetaStore {
    /// Creates an empty store at `path`, replacing nothing: the file must
    /// not exist.
    pub fn create(path: &Path) -> Result<Self, MetaError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        Self::init(conn)
    }

    /// An empty in-memory store, for tests and tools.
    pub fn in_memory() -> Result<Self, MetaError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self, MetaError> {
        conn.execute_batch(schema::CREATE)?;
        let store = Self { conn };
        store.configure()?;
        store.set_meta(key::SCHEMA_VERSION, SCHEMA_VERSION)?;
        store.set_meta(key::NEXT_OP_ID, 0)?;
        store.set_meta(key::NEXT_INO, 2)?;
        store.set_meta(key::EPOCH, 0)?;
        Ok(store)
    }

    /// Opens an existing store, e.g. a downloaded snapshot copied into place.
    pub fn open(path: &Path) -> Result<Self, MetaError> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        let store = Self { conn };
        let version: u32 = store.meta(key::SCHEMA_VERSION)?.unwrap_or(0);
        if version != SCHEMA_VERSION {
            return Err(MetaError::SchemaVersion(version));
        }
        store.configure()?;
        Ok(store)
    }

    fn configure(&self) -> Result<(), MetaError> {
        // Derived state: after an OS crash the file is rebuilt from the
        // snapshot and the log, so local durability is not needed (§5.1).
        self.conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = OFF;
             PRAGMA foreign_keys = OFF;
             PRAGMA cache_size = -65536;",
        )?;
        Ok(())
    }

    // ----------------------------------------------------------- meta table

    pub(crate) fn meta<T: rusqlite::types::FromSql>(&self, k: &str) -> rusqlite::Result<Option<T>> {
        self.conn
            .prepare_cached("SELECT v FROM meta WHERE k = ?1")?
            .query_row([k], |r| r.get(0))
            .optional()
    }

    pub(crate) fn set_meta<T: rusqlite::ToSql>(&self, k: &str, v: T) -> rusqlite::Result<()> {
        self.conn
            .prepare_cached("INSERT OR REPLACE INTO meta (k, v) VALUES (?1, ?2)")?
            .execute(params![k, v])?;
        Ok(())
    }

    /// The volume identity, once `VolumeCreate` has been applied.
    pub fn volume(&self) -> Result<Option<VolumeInfo>, MetaError> {
        let Some(volume_id) = self.meta::<Vec<u8>>(key::VOLUME_ID)? else {
            return Ok(None);
        };
        Ok(Some(VolumeInfo {
            volume_id,
            inline_data_max: self.meta(key::INLINE_DATA_MAX)?.unwrap_or(0),
        }))
    }

    /// How far into the log this store is.
    pub fn position(&self) -> Result<Position, MetaError> {
        Ok(Position {
            next_op_id: self.meta(key::NEXT_OP_ID)?.unwrap_or(0),
            applied_seq: self.meta(key::APPLIED_SEQ)?,
            applied_digest: self.meta(key::APPLIED_DIGEST)?.unwrap_or_default(),
            applied_epoch: self.meta(key::APPLIED_EPOCH)?.unwrap_or(0),
            epoch: self.meta(key::EPOCH)?.unwrap_or(0),
            clean: self.meta(key::CLEAN)?.unwrap_or(false),
        })
    }

    /// The inode number the writer allocates next.
    pub fn next_ino(&self) -> Result<Ino, MetaError> {
        Ok(self.meta(key::NEXT_INO)?.unwrap_or(2))
    }

    // ---------------------------------------------------------------- apply

    /// Applies one logged record found in segment `seq`, atomically. Inline
    /// data references resolve to that segment's data region.
    pub fn apply(&mut self, record: &Record, seq: u64) -> Result<(), ApplyError> {
        let op_id = record.op_id;
        let sql = |source| ApplyError::Sql { op_id, source };
        let tx = self.conn.transaction().map_err(sql)?;
        crate::apply::apply(&tx, record, seq)?;
        tx.commit().map_err(sql)
    }

    /// The writer's apply: applies `record` and, before committing the
    /// SQLite transaction, runs `log` (the append to the local WAL). If
    /// `log` fails the transaction rolls back, so the live state never runs
    /// ahead of the log, and the log never holds a record that did not
    /// apply.
    pub fn apply_logged<E>(
        &mut self,
        record: &Record,
        seq: u64,
        log: impl FnOnce() -> Result<(), E>,
    ) -> Result<Result<(), E>, ApplyError> {
        let op_id = record.op_id;
        let sql = |source| ApplyError::Sql { op_id, source };
        let tx = self.conn.transaction().map_err(sql)?;
        crate::apply::apply(&tx, record, seq)?;
        if let Err(e) = log() {
            return Ok(Err(e));
        }
        tx.commit().map_err(sql)?;
        Ok(Ok(()))
    }

    /// Records that segment `seq` has been applied completely. Called after
    /// its last record; the restorer uses it to write the restore pointer.
    pub fn finish_segment(&mut self, seq: u64, digest: &[u8], epoch: u64) -> Result<(), MetaError> {
        let tx = self.conn.transaction()?;
        for (k, v) in [(key::APPLIED_SEQ, seq), (key::APPLIED_EPOCH, epoch)] {
            tx.prepare_cached("INSERT OR REPLACE INTO meta (k, v) VALUES (?1, ?2)")?
                .execute(params![k, v])?;
        }
        tx.prepare_cached("INSERT OR REPLACE INTO meta (k, v) VALUES (?1, ?2)")?
            .execute(params![key::APPLIED_DIGEST, digest])?;
        tx.commit()?;
        Ok(())
    }

    // -------------------------------------------------------------- queries

    /// Attributes of `ino`.
    pub fn getattr(&self, ino: Ino) -> Result<Option<Attr>, MetaError> {
        Ok(get_attr(&self.conn, ino)?)
    }

    /// Resolves `name` in directory `parent`.
    pub fn lookup(&self, parent: Ino, name: &[u8]) -> Result<Option<Ino>, MetaError> {
        Ok(lookup(&self.conn, parent, name)?)
    }

    /// Up to `limit` entries of `dir` with cookie greater than `after`, in
    /// cookie order. `.` and `..` are not stored; see [`MetaStore::parent`].
    pub fn readdir(&self, dir: Ino, after: u64, limit: usize) -> Result<Vec<DirEntry>, MetaError> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT d.name, d.ino, i.kind, d.cookie FROM dirents d JOIN inodes i ON i.ino = d.ino
             WHERE d.parent = ?1 AND d.cookie > ?2 ORDER BY d.cookie LIMIT ?3",
        )?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = stmt.query_map(params![dir, after, limit], |r| {
            Ok(DirEntry {
                name: r.get(0)?,
                ino: r.get(1)?,
                kind: kind_col(r, 2)?,
                cookie: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The directory containing directory `dir` (the root's is itself).
    pub fn parent(&self, dir: Ino) -> Result<Option<Ino>, MetaError> {
        Ok(self
            .conn
            .prepare_cached("SELECT parent FROM inodes WHERE ino = ?1")?
            .query_row([dir], |r| r.get(0))
            .optional()?)
    }

    /// The target of symlink `ino`.
    pub fn readlink(&self, ino: Ino) -> Result<Option<Vec<u8>>, MetaError> {
        Ok(self
            .conn
            .prepare_cached("SELECT target FROM inodes WHERE ino = ?1")?
            .query_row([ino], |r| r.get::<_, Option<Vec<u8>>>(0))
            .optional()?
            .flatten())
    }

    /// Extents of `ino` overlapping `[start, end)`, in file order, unclipped.
    pub fn extents(&self, ino: Ino, start: u64, end: u64) -> Result<Vec<Extent>, MetaError> {
        Ok(crate::extents::overlapping(&self.conn, ino, start, end)?)
    }

    /// The value of xattr `name` of `ino`.
    pub fn getxattr(&self, ino: Ino, name: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        Ok(self
            .conn
            .prepare_cached("SELECT value FROM xattrs WHERE ino = ?1 AND name = ?2")?
            .query_row(params![ino, name], |r| r.get(0))
            .optional()?)
    }

    /// Names of the xattrs of `ino`, sorted.
    pub fn listxattr(&self, ino: Ino) -> Result<Vec<Vec<u8>>, MetaError> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT name FROM xattrs WHERE ino = ?1 ORDER BY name")?;
        let rows = stmt.query_map([ino], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Whether directory `dir` has any entry.
    pub fn has_entries(&self, dir: Ino) -> Result<bool, MetaError> {
        Ok(has_entries(&self.conn, dir)?)
    }

    /// Inodes at nlink 0: unlinked but possibly still open.
    pub fn orphans(&self) -> Result<Vec<Ino>, MetaError> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT ino FROM inodes WHERE nlink = 0 ORDER BY ino")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Live inode count and referenced bytes.
    pub fn stats(&self) -> Result<StoreStats, MetaError> {
        let inodes = self
            .conn
            .query_row("SELECT COUNT(*) FROM inodes", [], |r| r.get(0))?;
        let data_bytes =
            self.conn
                .query_row("SELECT COALESCE(SUM(len), 0) FROM extents", [], |r| {
                    r.get(0)
                })?;
        Ok(StoreStats { inodes, data_bytes })
    }

    // ------------------------------------------------------ restore replica

    /// Every extent whose bytes are still inline in a segment, as
    /// `(ino, extent)`, ordered by location so reads are sequential.
    pub fn inline_extents(&self) -> Result<Vec<(Ino, Extent)>, MetaError> {
        self.extents_where("seg IS NOT NULL ORDER BY seg, chunk_off, skip")
    }

    /// Every extent stored in object `key`.
    pub fn extents_in_object(&self, key: &str) -> Result<Vec<(Ino, Extent)>, MetaError> {
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT ino, {EXTENT_COLS} FROM extents WHERE obj = ?1 ORDER BY chunk_off, skip"
        ))?;
        let rows = stmt.query_map([key], |r| Ok((r.get(0)?, extent_row(r, 1)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    fn extents_where(&self, clause: &str) -> Result<Vec<(Ino, Extent)>, MetaError> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT ino, {EXTENT_COLS} FROM extents WHERE {clause}"
        ))?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, extent_row(r, 1)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Points extents at repacked bytes and registers the pack, in one
    /// transaction. Each moved extent becomes its own chunk.
    pub fn relocate(
        &mut self,
        pack: &str,
        pack_len: u64,
        moves: &[Relocation],
    ) -> Result<(), MetaError> {
        let tx = self.conn.transaction()?;
        {
            let mut upd = tx.prepare_cached(
                "UPDATE extents SET seg = NULL, obj = ?3, chunk_off = ?4, chunk_len = len,
                 crc = ?5, skip = 0 WHERE ino = ?1 AND off = ?2 AND len = ?6",
            )?;
            for m in moves {
                let n = upd.execute(params![m.ino, m.off, m.key, m.chunk_off, m.crc, m.len])?;
                if n != 1 {
                    return Err(MetaError::Sql(rusqlite::Error::QueryReturnedNoRows));
                }
            }
            tx.prepare_cached("INSERT OR REPLACE INTO packs (key, size) VALUES (?1, ?2)")?
                .execute(params![pack, pack_len])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Packs with their size and the bytes extents still use, for finding
    /// sparse packs. Packs nothing references any more are dropped from the
    /// table (and become garbage for GC).
    pub fn pack_usage(&mut self) -> Result<Vec<(String, u64, u64)>, MetaError> {
        let tx = self.conn.transaction()?;
        let usage: Vec<(String, u64, u64)> = {
            let mut stmt = tx.prepare(
                "SELECT p.key, p.size, COALESCE((SELECT SUM(len) FROM extents e WHERE e.obj = p.key), 0)
                 FROM packs p ORDER BY p.key",
            )?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            rows.collect::<Result<_, _>>()?
        };
        tx.execute(
            "DELETE FROM packs WHERE NOT EXISTS (SELECT 1 FROM extents e WHERE e.obj = packs.key)",
            [],
        )?;
        tx.commit()?;
        Ok(usage.into_iter().filter(|u| u.2 > 0).collect())
    }

    /// Every object key extents reference, for GC.
    pub fn referenced_objects(&self) -> Result<BTreeSet<String>, MetaError> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT obj FROM extents WHERE obj IS NOT NULL")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Records which objects stopped or resumed being referenced: `dropped`
    /// become garbage as of snapshot `generation` (at `time_ns`), and
    /// `revived` (referenced again, e.g. a pack with the same content) are
    /// no longer garbage.
    pub fn track_garbage(
        &mut self,
        dropped: &[String],
        revived: &[String],
        generation: u64,
        time_ns: i64,
    ) -> Result<(), MetaError> {
        let tx = self.conn.transaction()?;
        for k in dropped {
            tx.prepare_cached(
                "INSERT OR IGNORE INTO garbage (key, gen, time_ns) VALUES (?1, ?2, ?3)",
            )?
            .execute(params![k, generation, time_ns])?;
        }
        for k in revived {
            tx.prepare_cached("DELETE FROM garbage WHERE key = ?1")?
                .execute([k])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Garbage that became unreferenced before `cutoff_ns` and in a
    /// generation no later than `max_gen`: every snapshot that could
    /// reference it is older than the retention window and is not a
    /// running mount's base.
    pub fn collectable(&self, cutoff_ns: i64, max_gen: u64) -> Result<Vec<String>, MetaError> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT key FROM garbage WHERE time_ns < ?1 AND gen <= ?2 ORDER BY key",
        )?;
        let rows = stmt.query_map(params![cutoff_ns, max_gen], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Forgets garbage entries after their objects were deleted.
    pub fn forget_garbage(&mut self, keys: &[String]) -> Result<(), MetaError> {
        let tx = self.conn.transaction()?;
        for k in keys {
            tx.prepare_cached("DELETE FROM garbage WHERE key = ?1")?
                .execute([k])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Writes a compact copy of the store to `dest` (`VACUUM INTO`).
    pub fn export_snapshot(&self, dest: &Path) -> Result<(), MetaError> {
        let dest = dest
            .to_str()
            .ok_or_else(|| MetaError::Sql(rusqlite::Error::InvalidPath(dest.to_path_buf())))?;
        self.conn.execute("VACUUM INTO ?1", [dest])?;
        Ok(())
    }

    // ------------------------------------------------------------ inspection

    /// A canonical text rendering of the filesystem state: every inode,
    /// entry, extent and xattr, sorted. Two stores with equal dumps hold the
    /// same filesystem. Bookkeeping (packs, position) is left out.
    pub fn dump(&self) -> Result<String, MetaError> {
        let mut out = String::new();
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {ATTR_COLS}, target, parent, next_cookie FROM inodes ORDER BY ino"
        ))?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let a = attr_row(r)?;
            let target: Option<Vec<u8>> = r.get(11)?;
            let parent: u64 = r.get(12)?;
            let cookie: u64 = r.get(13)?;
            writeln!(
                out,
                "inode {a:?} target={target:?} parent={parent} next_cookie={cookie}"
            )
            .expect("String grows");
        }
        let mut stmt = self
            .conn
            .prepare("SELECT parent, name, ino, cookie FROM dirents ORDER BY parent, name")?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let (p, n, i, c): (u64, Vec<u8>, u64, u64) =
                (r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?);
            writeln!(
                out,
                "dirent {p}/{} -> {i} @{c}",
                String::from_utf8_lossy(&n)
            )
            .expect("String grows");
        }
        for (ino, e) in self.extents_where("1 ORDER BY ino, off")? {
            writeln!(out, "extent {ino} {e:?}").expect("String grows");
        }
        let mut stmt = self
            .conn
            .prepare("SELECT ino, name, value FROM xattrs ORDER BY ino, name")?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let (i, n, v): (u64, Vec<u8>, Vec<u8>) = (r.get(0)?, r.get(1)?, r.get(2)?);
            writeln!(out, "xattr {i} {n:?}={v:?}").expect("String grows");
        }
        Ok(out)
    }

    /// Checks structural invariants and returns every violation found:
    /// link counts, directory parents, entries pointing at live inodes,
    /// non-overlapping extents within file sizes.
    pub fn fsck(&self) -> Result<Vec<String>, MetaError> {
        let mut problems = Vec::new();
        let mut check = |sql: &str, what: &str| -> Result<(), MetaError> {
            let mut stmt = self.conn.prepare(sql)?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let ino: i64 = r.get(0)?;
                problems.push(format!("{what}: ino {ino}"));
            }
            Ok(())
        };
        let dir = Kind::Directory.to_wire() as i32;
        check(
            "SELECT d.ino FROM dirents d LEFT JOIN inodes i ON i.ino = d.ino WHERE i.ino IS NULL",
            "entry points at a missing inode",
        )?;
        check(
            &format!(
                "SELECT i.ino FROM inodes i WHERE i.kind != {dir} AND i.nlink !=
                 (SELECT COUNT(*) FROM dirents d WHERE d.ino = i.ino)"
            ),
            "file nlink differs from its entry count",
        )?;
        check(
            &format!(
                "SELECT i.ino FROM inodes i WHERE i.kind = {dir} AND i.nlink != 0 AND i.nlink !=
                 2 + (SELECT COUNT(*) FROM dirents d JOIN inodes c ON c.ino = d.ino
                      WHERE d.parent = i.ino AND c.kind = {dir})"
            ),
            "directory nlink differs from 2 + subdirectories",
        )?;
        check(
            &format!(
                "SELECT i.ino FROM inodes i WHERE i.kind = {dir} AND i.ino != 1 AND i.nlink != 0 AND
                 (SELECT COUNT(*) FROM dirents d WHERE d.ino = i.ino AND d.parent = i.parent) != 1"
            ),
            "directory parent is not its one entry",
        )?;
        check(
            &format!(
                "SELECT d.parent FROM dirents d JOIN inodes p ON p.ino = d.parent WHERE p.kind != {dir}"
            ),
            "entry inside a non-directory",
        )?;
        check(
            "SELECT e.ino FROM extents e JOIN inodes i ON i.ino = e.ino WHERE e.off + e.len > i.size",
            "extent past end of file",
        )?;
        check(
            "SELECT e.ino FROM extents e LEFT JOIN inodes i ON i.ino = e.ino WHERE i.ino IS NULL",
            "extent of a missing inode",
        )?;
        check(
            "SELECT a.ino FROM extents a JOIN extents b ON a.ino = b.ino AND b.off > a.off
             AND b.off < a.off + a.len",
            "overlapping extents",
        )?;
        check(
            "SELECT ino FROM extents WHERE len = 0 OR skip + len > chunk_len
             OR (seg IS NULL) = (obj IS NULL)",
            "malformed extent",
        )?;
        Ok(problems)
    }
}

// ------------------------------------------------------- shared row helpers

pub(crate) fn kind_col(r: &Row<'_>, i: usize) -> rusqlite::Result<Kind> {
    let v: i32 = r.get(i)?;
    Kind::from_wire(v).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            i,
            rusqlite::types::Type::Integer,
            "bad kind".into(),
        )
    })
}

pub(crate) fn attr_row(r: &Row<'_>) -> rusqlite::Result<Attr> {
    Ok(Attr {
        ino: r.get(0)?,
        kind: kind_col(r, 1)?,
        mode: r.get(2)?,
        uid: r.get(3)?,
        gid: r.get(4)?,
        nlink: r.get(5)?,
        size: r.get(6)?,
        atime_ns: r.get(7)?,
        mtime_ns: r.get(8)?,
        ctime_ns: r.get(9)?,
        rdev: r.get(10)?,
    })
}

pub(crate) fn extent_row(r: &Row<'_>, base: usize) -> rusqlite::Result<Extent> {
    let seg: Option<u64> = r.get(base + 2)?;
    let obj: Option<String> = r.get(base + 3)?;
    let loc = match (seg, obj) {
        (Some(seq), None) => Location::Segment { seq },
        (None, Some(key)) => Location::Object { key },
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                base + 2,
                rusqlite::types::Type::Null,
                "extent must have exactly one of seg, obj".into(),
            ));
        }
    };
    Ok(Extent {
        off: r.get(base)?,
        len: r.get(base + 1)?,
        loc,
        chunk_off: r.get(base + 4)?,
        chunk_len: r.get(base + 5)?,
        crc: r.get(base + 6)?,
        skip: r.get(base + 7)?,
    })
}

pub(crate) fn get_attr(conn: &Connection, ino: Ino) -> rusqlite::Result<Option<Attr>> {
    conn.prepare_cached(&format!("SELECT {ATTR_COLS} FROM inodes WHERE ino = ?1"))?
        .query_row([ino], attr_row)
        .optional()
}

pub(crate) fn lookup(conn: &Connection, parent: Ino, name: &[u8]) -> rusqlite::Result<Option<Ino>> {
    conn.prepare_cached("SELECT ino FROM dirents WHERE parent = ?1 AND name = ?2")?
        .query_row(params![parent, name], |r| r.get(0))
        .optional()
}

pub(crate) fn has_entries(conn: &Connection, dir: Ino) -> rusqlite::Result<bool> {
    conn.prepare_cached("SELECT EXISTS (SELECT 1 FROM dirents WHERE parent = ?1)")?
        .query_row([dir], |r| r.get(0))
}

pub(crate) const EXTENT_SELECT: &str = EXTENT_COLS;
