//! Extent-list operations used by `apply`. Extents of one inode never
//! overlap; holes are absent extents.

use rusqlite::{Connection, params};

use crate::model::{Extent, Ino, Location};
use crate::store::{EXTENT_SELECT, extent_row};

/// Extents of `ino` overlapping `[start, end)`, in file order.
pub(crate) fn overlapping(
    conn: &Connection,
    ino: Ino,
    start: u64,
    end: u64,
) -> rusqlite::Result<Vec<Extent>> {
    // Offsets fit in i64 (SQLite's integer); "to the end" is spelled u64::MAX.
    let end = end.min(crate::plan::MAX_FILE_SIZE);
    if start >= end {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    // The extent starting before `start` may reach into the range.
    let before = conn
        .prepare_cached(&format!(
            "SELECT {EXTENT_SELECT} FROM extents WHERE ino = ?1 AND off < ?2 ORDER BY off DESC LIMIT 1"
        ))?
        .query_map(params![ino, start], |r| extent_row(r, 0))?
        .next()
        .transpose()?;
    if let Some(e) = before.filter(|e| e.end() > start) {
        out.push(e);
    }
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT {EXTENT_SELECT} FROM extents WHERE ino = ?1 AND off >= ?2 AND off < ?3 ORDER BY off"
    ))?;
    for e in stmt.query_map(params![ino, start, end], |r| extent_row(r, 0))? {
        out.push(e?);
    }
    Ok(out)
}

/// Every extent of `ino`, in file order.
pub(crate) fn all(conn: &Connection, ino: Ino) -> rusqlite::Result<Vec<Extent>> {
    overlapping(conn, ino, 0, u64::MAX)
}

pub(crate) fn insert(conn: &Connection, ino: Ino, e: &Extent) -> rusqlite::Result<()> {
    let (seg, obj, vol) = match &e.loc {
        Location::Segment { seq } => (Some(*seq), None, None),
        Location::Object { key } => (None, Some(key.as_str()), None),
        Location::External { volume, key } => (None, Some(key.as_str()), Some(volume.as_slice())),
    };
    conn.prepare_cached(
        "INSERT INTO extents (ino, off, len, seg, obj, chunk_off, chunk_len, crc, skip, vol)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?
    .execute(params![
        ino,
        e.off,
        e.len,
        seg,
        obj,
        e.chunk_off,
        e.chunk_len,
        e.crc,
        e.skip,
        vol
    ])?;
    Ok(())
}

fn delete(conn: &Connection, ino: Ino, off: u64) -> rusqlite::Result<()> {
    conn.prepare_cached("DELETE FROM extents WHERE ino = ?1 AND off = ?2")?
        .execute(params![ino, off])?;
    Ok(())
}

/// Makes `[start, end)` of `ino` a hole, trimming extents that straddle it.
pub(crate) fn punch(conn: &Connection, ino: Ino, start: u64, end: u64) -> rusqlite::Result<()> {
    for e in overlapping(conn, ino, start, end)? {
        delete(conn, ino, e.off)?;
        if let Some(left) = e.clip(0, start) {
            insert(conn, ino, &left)?;
        }
        if let Some(right) = e.clip(end, u64::MAX) {
            insert(conn, ino, &right)?;
        }
    }
    Ok(())
}

/// Moves every extent at or after `from` by `delta` bytes (collapse and
/// insert range). The caller has already split any extent at `from`.
pub(crate) fn shift(conn: &Connection, ino: Ino, from: u64, delta: i128) -> rusqlite::Result<()> {
    let moved = overlapping(conn, ino, from, u64::MAX)?;
    for e in &moved {
        delete(conn, ino, e.off)?;
    }
    for mut e in moved {
        let off = i128::from(e.off) + delta;
        e.off = u64::try_from(off).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, 0))?;
        insert(conn, ino, &e)?;
    }
    Ok(())
}

/// Splits the extent containing `at` (if any) into two at `at`.
pub(crate) fn split_at(conn: &Connection, ino: Ino, at: u64) -> rusqlite::Result<()> {
    for e in overlapping(conn, ino, at, at.saturating_add(1))? {
        if e.off < at {
            delete(conn, ino, e.off)?;
            for piece in [e.clip(0, at), e.clip(at, u64::MAX)].into_iter().flatten() {
                insert(conn, ino, &piece)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn drop_all(conn: &Connection, ino: Ino) -> rusqlite::Result<()> {
    conn.prepare_cached("DELETE FROM extents WHERE ino = ?1")?
        .execute([ino])?;
    Ok(())
}
