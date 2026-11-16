//! Snapshots and the restore pointer (design §7), and the `volume.json` hint.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use skyfs_format::snapshot::v1::SnapshotManifest;
use skyfs_meta::{MetaStore, SCHEMA_VERSION};
use skyfs_store::{Created, Key, ObjectClient, find_end};

use crate::error::VolumeError;

/// zstd level for snapshot metadata.
const ZSTD_LEVEL: i32 = 3;

/// The mutable hint: where to start looking. Never trusted for correctness;
/// every reader verifies by probing forward from it.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hint {
    /// A snapshot generation known to exist.
    pub snapshot_gen: u64,
    /// A segment seq known to exist.
    pub head_seq: u64,
}

/// Reads the hint; any failure yields the zero hint.
pub async fn read_hint(client: &ObjectClient) -> Hint {
    match client.get(&Key::Hint).await {
        Ok(Some(b)) => serde_json::from_slice(&b).unwrap_or_default(),
        _ => Hint::default(),
    }
}

/// Writes the hint, best effort.
pub async fn write_hint(client: &ObjectClient, hint: Hint) {
    let body = serde_json::to_vec(&hint).expect("hint serializes");
    if let Err(e) = client.put_hint(Bytes::from(body)).await {
        tracing::warn!(error = %e, "could not update volume.json hint");
    }
}

/// Reads and checks the manifest of snapshot `generation`.
pub async fn read_manifest(
    client: &ObjectClient,
    generation: u64,
    volume_id: &[u8],
) -> Result<SnapshotManifest, VolumeError> {
    let bytes = client
        .get(&Key::SnapshotManifest(generation))
        .await?
        .ok_or_else(|| VolumeError::Corrupt(format!("snapshot manifest {generation} vanished")))?;
    let m = SnapshotManifest::decode(bytes)?;
    if m.r#gen != generation || m.volume_id != volume_id {
        return Err(VolumeError::Corrupt(format!(
            "manifest {generation} names gen {} of another volume or slot",
            m.r#gen
        )));
    }
    if m.meta_schema_version != SCHEMA_VERSION {
        return Err(VolumeError::Corrupt(format!(
            "snapshot {generation} has metadata schema {}, this build reads {SCHEMA_VERSION}",
            m.meta_schema_version
        )));
    }
    Ok(m)
}

/// The latest snapshot: probes manifests forward from the hint. Manifests
/// are never deleted, so existence is a dense prefix.
pub async fn latest(
    client: &ObjectClient,
    hint: Hint,
    volume_id: &[u8],
) -> Result<Option<SnapshotManifest>, VolumeError> {
    let start = if client
        .exists(&Key::SnapshotManifest(hint.snapshot_gen))
        .await?
    {
        hint.snapshot_gen
    } else {
        0
    };
    let end = find_end(start, |g| async move {
        client.exists(&Key::SnapshotManifest(g)).await
    })
    .await?;
    if end == 0 {
        return Ok(None);
    }
    Ok(Some(read_manifest(client, end - 1, volume_id).await?))
}

/// Downloads a snapshot's metadata, verifies it, and writes the SQLite file
/// to `dest`.
pub async fn download_meta(
    client: &ObjectClient,
    m: &SnapshotManifest,
    dest: &Path,
) -> Result<(), VolumeError> {
    let key: Key = m
        .meta_key
        .parse()
        .map_err(|_| VolumeError::Corrupt(format!("bad meta key {:?}", m.meta_key)))?;
    let bytes = client.get(&key).await?.ok_or_else(|| {
        VolumeError::Corrupt(format!("snapshot metadata {} is missing", m.meta_key))
    })?;
    if bytes.len() as u64 != m.meta_len
        || blake3::hash(&bytes).as_bytes().as_slice() != m.meta_blake3
    {
        return Err(VolumeError::Corrupt(format!(
            "snapshot metadata {} fails its hash",
            m.meta_key
        )));
    }
    let raw = zstd::decode_all(bytes.as_ref())?;
    let tmp = dest.with_extension("download");
    std::fs::write(&tmp, raw)?;
    std::fs::rename(tmp, dest)?;
    Ok(())
}

/// A snapshot exported from a replica, ready to upload.
#[derive(Debug)]
pub struct Exported {
    compressed: Vec<u8>,
    pos: skyfs_meta::Position,
    stats: skyfs_meta::StoreStats,
}

/// Serializes `replica` (exactly at the end of a segment) for a snapshot:
/// `VACUUM INTO`, then zstd. Synchronous: the store stays on its thread.
pub fn export(replica: &MetaStore, scratch: &Path, tag: u64) -> Result<Exported, VolumeError> {
    let pos = replica.position()?;
    if pos.applied_seq.is_none() {
        return Err(VolumeError::Corrupt("snapshot of an empty replica".into()));
    }
    let stats = replica.stats()?;
    let file = scratch.join(format!("snapshot-{tag}.db"));
    let _ = std::fs::remove_file(&file);
    replica.export_snapshot(&file)?;
    let raw = std::fs::read(&file)?;
    let _ = std::fs::remove_file(&file);
    let compressed = zstd::encode_all(raw.as_slice(), ZSTD_LEVEL)?;
    Ok(Exported {
        compressed,
        pos,
        stats,
    })
}

/// Identity of a snapshot being published.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotId<'a> {
    /// Generation to write.
    pub generation: u64,
    /// The snapshot it was built from.
    pub parent_gen: u64,
    /// Volume id.
    pub volume_id: &'a [u8],
    /// Who wrote it.
    pub restorer_id: &'a str,
}

/// Uploads snapshot metadata, then its manifest. The manifest PUT is the
/// commit point that moves the restore pointer; `Ok(None)` means another
/// manifest already holds this generation (this writer is stale).
pub async fn publish(
    client: &ObjectClient,
    exported: Exported,
    id: SnapshotId<'_>,
) -> Result<Option<SnapshotManifest>, VolumeError> {
    let Exported {
        compressed,
        pos,
        stats,
    } = exported;
    let restored_seq = pos.applied_seq.unwrap_or(0);
    let hash = *blake3::hash(&compressed).as_bytes();
    let meta_key = Key::SnapshotMeta {
        generation: id.generation,
        hash,
    };
    // Content-addressed: an existing object is these exact bytes.
    let meta_len = compressed.len() as u64;
    client.put_new(&meta_key, Bytes::from(compressed)).await?;

    let manifest = SnapshotManifest {
        volume_id: id.volume_id.to_vec(),
        r#gen: id.generation,
        parent_gen: id.parent_gen,
        restored_seq,
        restored_op_id: pos.next_op_id.saturating_sub(1),
        restored_digest: pos.applied_digest,
        restored_epoch: pos.applied_epoch,
        meta_key: meta_key.to_string(),
        meta_len,
        meta_blake3: hash.to_vec(),
        meta_schema_version: SCHEMA_VERSION,
        inode_count: stats.inodes,
        data_bytes: stats.data_bytes,
        created_ns: now_ns(),
        restorer_id: id.restorer_id.as_bytes().to_vec(),
    };
    match client
        .put_new(
            &Key::SnapshotManifest(id.generation),
            Bytes::from(manifest.encode_to_vec()),
        )
        .await?
    {
        Created::New => Ok(Some(manifest)),
        Created::AlreadyExists => Ok(None),
    }
}

/// Wall clock in ns since the epoch.
#[must_use]
pub fn now_ns() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}
