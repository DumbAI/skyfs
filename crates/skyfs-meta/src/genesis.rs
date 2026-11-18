//! Turning a snapshot into the first snapshot of another volume (design
//! §13): what `publish` and `clone` do to a copy of a snapshot's metadata
//! before it is uploaded as generation 0 of the new volume.
//!
//! None of this goes through `apply`: the result is a snapshot, not a log
//! record. A new volume made this way has no log at all. Its restore pointer
//! is a *genesis* position (seq 0, a digest naming where it came from) that
//! the first `MountBegin` chains from, exactly as it chains from the
//! snapshot of a volume made by `create`.

use std::collections::HashSet;

use rusqlite::params;

use crate::model::{Ino, Kind, ROOT_INO};
use crate::schema::key;
use crate::store::{MetaError, MetaStore, kind_col};

/// What [`MetaStore::retain_paths`] removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pruned {
    /// Directory entries removed.
    pub entries: u64,
    /// Inodes no kept entry reaches any more, removed with their extents
    /// and xattrs. Includes inodes at nlink 0 (unlinked while open).
    pub inodes: u64,
}

/// A sealed volume whose objects a store's extents reference.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BaseVolume {
    /// Its 16-byte id.
    pub volume: [u8; 16],
    /// Its root in the bucket (`<prefix>/volumes/<id hex>`).
    pub root: String,
}

impl MetaStore {
    /// Keeps only the entries `keep` accepts, walking from the root. `keep`
    /// gets each entry's path relative to the root (`a/b/c`, no leading
    /// slash) and its kind; rejecting a directory drops everything below
    /// it. Inodes no kept entry reaches are removed, and link counts are
    /// recomputed, so a hard-linked file survives if any of its names does.
    pub fn retain_paths(
        &mut self,
        keep: &mut dyn FnMut(&[u8], Kind) -> bool,
    ) -> Result<Pruned, MetaError> {
        self.require_snapshot()?;
        // 1. Walk the kept tree.
        let mut kept_entries: Vec<(Ino, Vec<u8>)> = Vec::new();
        let mut reached: HashSet<Ino> = HashSet::from([ROOT_INO]);
        let mut dirs: Vec<(Ino, Vec<u8>)> = vec![(ROOT_INO, Vec::new())];
        {
            let mut stmt = self.conn.prepare(
                "SELECT d.name, d.ino, i.kind FROM dirents d JOIN inodes i ON i.ino = d.ino
                 WHERE d.parent = ?1",
            )?;
            while let Some((dir, path)) = dirs.pop() {
                let children: Vec<(Vec<u8>, Ino, Kind)> = stmt
                    .query_map([dir], |r| Ok((r.get(0)?, r.get(1)?, kind_col(r, 2)?)))?
                    .collect::<Result<_, _>>()?;
                for (name, ino, kind) in children {
                    let mut full = path.clone();
                    if !full.is_empty() {
                        full.push(b'/');
                    }
                    full.extend_from_slice(&name);
                    if !keep(&full, kind) {
                        continue;
                    }
                    kept_entries.push((dir, name));
                    // A directory has exactly one entry, so it is walked once.
                    if reached.insert(ino) && kind == Kind::Directory {
                        dirs.push((ino, full));
                    }
                }
            }
        }

        // 2. Drop everything else, then recompute link counts.
        let tx = self.conn.transaction()?;
        tx.execute_batch(
            "CREATE TEMP TABLE keep_d (parent INTEGER, name BLOB, PRIMARY KEY (parent, name))
                 WITHOUT ROWID;
             CREATE TEMP TABLE keep_i (ino INTEGER PRIMARY KEY);",
        )?;
        {
            let mut d = tx.prepare("INSERT INTO keep_d (parent, name) VALUES (?1, ?2)")?;
            for (parent, name) in &kept_entries {
                d.execute(params![parent, name])?;
            }
            let mut i = tx.prepare("INSERT INTO keep_i (ino) VALUES (?1)")?;
            for ino in &reached {
                i.execute([ino])?;
            }
        }
        let entries = tx.execute(
            "DELETE FROM dirents WHERE NOT EXISTS
             (SELECT 1 FROM keep_d k WHERE k.parent = dirents.parent AND k.name = dirents.name)",
            [],
        )?;
        let inodes = tx.execute(
            "DELETE FROM inodes WHERE ino NOT IN (SELECT ino FROM keep_i)",
            [],
        )?;
        tx.execute(
            "DELETE FROM extents WHERE ino NOT IN (SELECT ino FROM keep_i)",
            [],
        )?;
        tx.execute(
            "DELETE FROM xattrs WHERE ino NOT IN (SELECT ino FROM keep_i)",
            [],
        )?;
        let dir = Kind::Directory.to_wire() as i32;
        tx.execute(
            &format!(
                "UPDATE inodes SET nlink = CASE WHEN kind = {dir} THEN
                     2 + (SELECT COUNT(*) FROM dirents d JOIN inodes c ON c.ino = d.ino
                          WHERE d.parent = inodes.ino AND c.kind = {dir})
                 ELSE (SELECT COUNT(*) FROM dirents d WHERE d.ino = inodes.ino) END"
            ),
            [],
        )?;
        tx.execute_batch("DROP TABLE keep_d; DROP TABLE keep_i;")?;
        tx.commit()?;
        Ok(Pruned {
            entries: entries as u64,
            inodes: inodes as u64,
        })
    }

    /// Makes every extent that points at this volume's own objects point at
    /// the same keys in sealed volume `base` instead, and records `base`.
    /// Extents already in other bases are unchanged, so references always
    /// name the volume that owns the bytes and never form a chain.
    pub fn externalize(&mut self, base: &BaseVolume) -> Result<u64, MetaError> {
        self.require_snapshot()?;
        let tx = self.conn.transaction()?;
        let moved = tx.execute(
            "UPDATE extents SET vol = ?1 WHERE vol IS NULL AND obj IS NOT NULL",
            [base.volume.as_slice()],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO bases (vol, root) VALUES (?1, ?2)",
            params![base.volume.as_slice(), base.root],
        )?;
        tx.commit()?;
        Ok(moved as u64)
    }

    /// Makes this store the genesis snapshot of volume `volume_id`: the
    /// identity becomes the new volume's, the restore pointer becomes seq 0
    /// with `digest` (the first `MountBegin` chains from it) in epoch 0 of a
    /// cleanly ended history, and the old volume's bookkeeping (packs,
    /// garbage, bases no extent uses) is dropped. Inode numbers and op ids
    /// continue from the source.
    pub fn reset_genesis(
        &mut self,
        volume_id: &[u8; 16],
        digest: &[u8; 32],
    ) -> Result<(), MetaError> {
        self.require_snapshot()?;
        let tx = self.conn.transaction()?;
        let set = "INSERT OR REPLACE INTO meta (k, v) VALUES (?1, ?2)";
        tx.execute(set, params![key::VOLUME_ID, volume_id.as_slice()])?;
        tx.execute(set, params![key::APPLIED_SEQ, 0])?;
        tx.execute(set, params![key::APPLIED_DIGEST, digest.as_slice()])?;
        tx.execute(set, params![key::APPLIED_EPOCH, 0])?;
        tx.execute(set, params![key::EPOCH, 0])?;
        tx.execute(set, params![key::CLEAN, true])?;
        tx.execute_batch(
            "DELETE FROM packs;
             DELETE FROM garbage;
             DELETE FROM bases WHERE NOT EXISTS (SELECT 1 FROM extents e WHERE e.vol = bases.vol);",
        )?;
        tx.commit()?;
        Ok(())
    }

    /// The base volumes this store lists, sorted by id.
    pub fn bases(&self) -> Result<Vec<BaseVolume>, MetaError> {
        let mut stmt = self
            .conn
            .prepare("SELECT vol, root FROM bases ORDER BY vol")?;
        let rows = stmt.query_map([], |r| {
            let vol: Vec<u8> = r.get(0)?;
            Ok((vol, r.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (vol, root) = row?;
            let volume: [u8; 16] = vol
                .try_into()
                .map_err(|_| MetaError::Invalid("a base volume id is not 16 bytes".into()))?;
            out.push(BaseVolume { volume, root });
        }
        Ok(out)
    }

    /// Snapshots never point into log segments; a store that does is a
    /// live store or a replica mid-pass, and cannot seed a new volume.
    fn require_snapshot(&self) -> Result<(), MetaError> {
        let inline: bool = self.conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM extents WHERE seg IS NOT NULL)",
            [],
            |r| r.get(0),
        )?;
        if inline {
            return Err(MetaError::Invalid(
                "extents point into log segments: not a snapshot".into(),
            ));
        }
        Ok(())
    }
}
