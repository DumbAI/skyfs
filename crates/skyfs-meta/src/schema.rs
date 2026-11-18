//! SQLite schema of the metadata store, which is also the snapshot format.
//!
//! Bump [`SCHEMA_VERSION`] on any change; a snapshot's manifest records the
//! version it was written with and a mount refuses one it does not know.

/// Version of the tables below, stored in `meta` and in snapshot manifests.
///
/// - 1: the original tables.
/// - 2: `extents.vol` and `bases` (extents in sealed base volumes, design
///   §13). Version 1 files are migrated on open ([`MIGRATE_1_TO_2`]).
pub const SCHEMA_VERSION: u32 = 2;

/// Versions this build reads (and migrates to [`SCHEMA_VERSION`] on open).
pub const READABLE: [u32; 2] = [1, SCHEMA_VERSION];

/// Version 1 to 2: both additions are empty, so every v1 extent keeps its
/// meaning (an object of this volume).
pub const MIGRATE_1_TO_2: &str = r"
ALTER TABLE extents ADD COLUMN vol BLOB;
CREATE TABLE bases (
    vol  BLOB PRIMARY KEY,
    root TEXT NOT NULL
) WITHOUT ROWID;
";

pub const CREATE: &str = r"
CREATE TABLE meta (
    k TEXT PRIMARY KEY,
    v BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE inodes (
    ino         INTEGER PRIMARY KEY,
    kind        INTEGER NOT NULL,
    mode        INTEGER NOT NULL,
    uid         INTEGER NOT NULL,
    gid         INTEGER NOT NULL,
    nlink       INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    atime       INTEGER NOT NULL,
    mtime       INTEGER NOT NULL,
    ctime       INTEGER NOT NULL,
    rdev        INTEGER NOT NULL,
    target      BLOB,
    -- directories: the directory holding this one (root: itself)
    parent      INTEGER NOT NULL,
    -- directories: cookie for the next entry
    next_cookie INTEGER NOT NULL
);

CREATE TABLE dirents (
    parent INTEGER NOT NULL,
    name   BLOB NOT NULL,
    ino    INTEGER NOT NULL,
    cookie INTEGER NOT NULL,
    PRIMARY KEY (parent, name)
) WITHOUT ROWID;
CREATE UNIQUE INDEX dirents_by_cookie ON dirents (parent, cookie);
CREATE INDEX dirents_by_ino ON dirents (ino);

-- Exactly one of seg (inline bytes of a log segment) and obj (an object key)
-- is set. vol, with obj, names the sealed base volume that owns the object
-- (a row of `bases`); NULL means this volume.
CREATE TABLE extents (
    ino       INTEGER NOT NULL,
    off       INTEGER NOT NULL,
    len       INTEGER NOT NULL,
    seg       INTEGER,
    obj       TEXT,
    chunk_off INTEGER NOT NULL,
    chunk_len INTEGER NOT NULL,
    crc       INTEGER NOT NULL,
    skip      INTEGER NOT NULL,
    vol       BLOB,
    PRIMARY KEY (ino, off)
) WITHOUT ROWID;
CREATE INDEX extents_by_seg ON extents (seg) WHERE seg IS NOT NULL;
CREATE INDEX extents_by_obj ON extents (obj) WHERE obj IS NOT NULL;

CREATE TABLE xattrs (
    ino   INTEGER NOT NULL,
    name  BLOB NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (ino, name)
) WITHOUT ROWID;

-- Sealed volumes whose objects extents reference read-only (design §13),
-- with their root in the bucket. Never collected by this volume's GC.
CREATE TABLE bases (
    vol  BLOB PRIMARY KEY,
    root TEXT NOT NULL
) WITHOUT ROWID;

-- Packs written by restore passes and their sizes, to find sparse ones.
CREATE TABLE packs (
    key  TEXT PRIMARY KEY,
    size INTEGER NOT NULL
) WITHOUT ROWID;

-- Objects (packs, blobs) no extent references any more, with the snapshot
-- generation and time at which that became true. GC deletes one once no
-- retained snapshot and no running mount can reference it, without LIST.
CREATE TABLE garbage (
    key     TEXT PRIMARY KEY,
    gen     INTEGER NOT NULL,
    time_ns INTEGER NOT NULL
) WITHOUT ROWID;
";

/// Keys of the `meta` table.
pub mod key {
    pub const SCHEMA_VERSION: &str = "schema_version";
    pub const VOLUME_ID: &str = "volume_id";
    pub const INLINE_DATA_MAX: &str = "inline_data_max";
    pub const NEXT_OP_ID: &str = "next_op_id";
    pub const NEXT_INO: &str = "next_ino";
    pub const EPOCH: &str = "epoch";
    pub const APPLIED_SEQ: &str = "applied_seq";
    pub const APPLIED_DIGEST: &str = "applied_digest";
    pub const APPLIED_EPOCH: &str = "applied_epoch";
    pub const CLEAN: &str = "clean";
}
