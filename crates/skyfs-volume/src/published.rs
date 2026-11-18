//! Published volumes and clones (design §13).
//!
//! - [`publish`] seals a copy of a quiescent volume as a new, read-only
//!   volume: the source's own objects that the kept files reference are
//!   copied server-side, references it already has into sealed bases are
//!   kept, and the new volume's only snapshot is written. The source keeps
//!   working; nothing it writes later is reachable from the copy.
//! - [`clone_volume`] creates a writable volume whose first snapshot is a
//!   sealed volume's metadata, every extent pointing into the sealed volume
//!   (or the bases it already used). No data is copied. Writes go to the
//!   clone's own objects; the base is never written and never collected by
//!   the clone.
//!
//! Both produce a *genesis* snapshot: generation 0 with no log, whose
//! restore pointer (seq 0, a digest naming the source) is what the first
//! `MountBegin` chains from. Mount, replay, the restorer and fencing then
//! work exactly as for a volume made by `create`.
//!
//! Commit order, so a crash at any step leaves no volume or a complete one,
//! and a retry converges:
//!
//! ```text
//! publish: copy objects → metadata (content-addressed) → `sealed` → manifest 0
//! clone:                  metadata (content-addressed) →            manifest 0
//! ```
//!
//! The seal is written before the manifest, so no reader ever sees the
//! published volume unsealed (and no read-write mount can claim it).

use std::path::Path;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use skyfs_format::snapshot::v1::SnapshotManifest;
use skyfs_meta::{BaseVolume, Kind, Pruned};
use skyfs_store::{Key, ObjectClient, hex};

use crate::error::VolumeError;
use crate::mount::replay;
use crate::restorer::tool_id;
use crate::snapshot::{self, Exported, Hint, SnapshotId};

/// Server-side copies in flight while publishing.
const COPIES: usize = 16;

/// What [`publish`] produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Published {
    /// Inodes in the published volume.
    pub inodes: u64,
    /// Logical bytes its files hold.
    pub data_bytes: u64,
    /// Objects copied from the source (zero if it was already published).
    pub copied_objects: u64,
    /// What the filter removed.
    pub pruned: Pruned,
    /// Sealed volumes it references (it never references the source).
    pub bases: Vec<[u8; 16]>,
    /// The destination was already published by an earlier attempt and is
    /// returned as it is.
    pub existing: bool,
}

/// What [`clone_volume`] produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Cloned {
    /// Inodes in the clone.
    pub inodes: u64,
    /// Logical bytes its files hold, all still in base volumes.
    pub data_bytes: u64,
    /// Sealed volumes it references.
    pub bases: Vec<[u8; 16]>,
    /// The clone was made by an earlier attempt (its response lost) and is
    /// returned as it is.
    pub existing: bool,
}

/// The digest a genesis snapshot's restore pointer carries: it names the
/// new volume, the volume and snapshot it came from, and how. A retry
/// recomputes it to recognise its own earlier, already committed attempt.
fn genesis_digest(how: &str, new: &[u8; 16], from: &[u8; 16], from_meta: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"skyfs/genesis/v1\0");
    h.update(how.as_bytes());
    h.update(b"\0");
    h.update(new);
    h.update(from);
    h.update(from_meta);
    *h.finalize().as_bytes()
}

fn base_ids(m: &SnapshotManifest) -> Vec<[u8; 16]> {
    m.bases
        .iter()
        .filter_map(|b| <[u8; 16]>::try_from(b.volume_id.as_slice()).ok())
        .collect()
}

/// A path filter for [`publish`]: `true` keeps the entry. `Send`, so a
/// publish can run on a multi-threaded runtime.
pub type Keep<'a> = dyn FnMut(&[u8], Kind) -> bool + Send + 'a;

/// Uploads an exported genesis snapshot as generation 0 of `dst`.
/// `Ok(None)`: another manifest already holds generation 0. (The store is
/// exported before this is called: a `MetaStore` must not be borrowed
/// across an await, or the future is not `Send`.)
async fn commit_genesis(
    dst: &ObjectClient,
    dst_id: &[u8; 16],
    exported: Exported,
    before_manifest: impl AsyncFnOnce() -> Result<(), VolumeError>,
) -> Result<Option<SnapshotManifest>, VolumeError> {
    let restorer_id = tool_id(dst_id);
    let m = snapshot::upload_meta(
        dst,
        exported,
        SnapshotId {
            generation: 0,
            parent_gen: 0,
            volume_id: dst_id,
            restorer_id: &restorer_id,
        },
    )
    .await?;
    before_manifest().await?;
    if !snapshot::commit_manifest(dst, &m).await? {
        return Ok(None);
    }
    snapshot::write_hint(dst, Hint::default()).await;
    Ok(Some(m))
}

/// Publishes the latest state of volume `src` as the new sealed volume
/// `dst`, keeping only the paths `keep` accepts (see
/// [`MetaStore::retain_paths`]). `dst` must be in the same bucket as `src`.
///
/// The source must be quiescent: not mounted, with its last mount ended
/// and fully restored into its latest snapshot (a clean unmount does
/// this), so that snapshot is its whole state and an app's files are
/// consistent. Choose a fresh `dst_id` per publish. Retrying after any
/// failure converges: a `dst` this exact publish already committed is
/// returned as it is; any other existing `dst` is refused.
pub async fn publish(
    src: &ObjectClient,
    src_id: [u8; 16],
    dst: &ObjectClient,
    dst_id: [u8; 16],
    keep: &mut Keep<'_>,
    scratch: &Path,
) -> Result<Published, VolumeError> {
    let source = snapshot::latest(src, snapshot::read_hint(src).await, &src_id)
        .await?
        .ok_or_else(|| VolumeError::NotFound(src.root().to_string()))?;
    let digest = genesis_digest("publish", &dst_id, &src_id, &source.meta_blake3);
    if let Some(m) = existing_genesis(dst, &dst_id, &digest).await? {
        // The seal is written before the manifest.
        if !dst.exists(&Key::Sealed).await? {
            return Err(VolumeError::Corrupt(format!(
                "{} has a published manifest but no seal",
                dst.root()
            )));
        }
        return Ok(Published {
            inodes: m.inode_count,
            data_bytes: m.data_bytes,
            bases: base_ids(&m),
            existing: true,
            ..Published::default()
        });
    }

    let work = scratch.join(format!("publish-{}", hex(&dst_id)));
    let r = replay(src, src_id, &work.join("meta"), false).await?;
    let quiet = |why: &str| VolumeError::NotQuiescent(src.root().to_string(), why.to_owned());
    if r.manifest.r#gen != source.r#gen {
        return Err(quiet("a restore pass moved its pointer while publishing"));
    }
    if !r.tail.is_empty() {
        return Err(quiet(
            "its log has segments its latest snapshot does not include (mounted, or not fully restored at unmount)",
        ));
    }
    let mut store = r.live;
    if !store.position()?.clean {
        return Err(quiet("its last mount did not end cleanly"));
    }

    let pruned = store.retain_paths(keep)?;
    let own: Vec<String> = store.referenced_objects()?.into_iter().collect();
    let copied_objects = own.len() as u64;
    futures::stream::iter(own)
        .map(|key| async move {
            let k: Key = key
                .parse()
                .map_err(|_| VolumeError::Corrupt(format!("bad object key {key:?}")))?;
            dst.copy_from(src, &k).await?;
            Ok::<_, VolumeError>(())
        })
        .buffer_unordered(COPIES)
        .try_collect::<()>()
        .await?;

    store.reset_genesis(&dst_id, &digest)?;
    let exported = snapshot::export(&store, &work, 0)?;
    drop(store);
    let seal = Bytes::from(
        serde_json::json!({
            "published_from": hex(&src_id),
            "snapshot_gen": r.manifest.r#gen,
        })
        .to_string(),
    );
    let committed = commit_genesis(dst, &dst_id, exported, async || {
        // Any existing seal is final: the volume is sealed either way.
        dst.put_new(&Key::Sealed, seal).await?;
        Ok(())
    })
    .await?;
    let _ = std::fs::remove_dir_all(&work);
    let m = committed.ok_or_else(|| VolumeError::Exists(dst.root().to_string()))?;
    Ok(Published {
        inodes: m.inode_count,
        data_bytes: m.data_bytes,
        copied_objects,
        pruned,
        bases: base_ids(&m),
        existing: false,
    })
}

/// Creates volume `dst` as a clone of sealed volume `base`: metadata only,
/// every extent pointing into `base` (or the sealed volumes `base` already
/// references). `dst` must be in the same bucket as `base` and must not
/// exist.
pub async fn clone_volume(
    base: &ObjectClient,
    base_id: [u8; 16],
    dst: &ObjectClient,
    dst_id: [u8; 16],
    scratch: &Path,
) -> Result<Cloned, VolumeError> {
    if !base.exists(&Key::Sealed).await? {
        return Err(VolumeError::NotSealed(base.root().to_string()));
    }
    let sealed = snapshot::latest(base, snapshot::read_hint(base).await, &base_id)
        .await?
        .ok_or_else(|| VolumeError::NotFound(base.root().to_string()))?;
    let digest = genesis_digest("clone", &dst_id, &base_id, &sealed.meta_blake3);
    if let Some(m) = existing_genesis(dst, &dst_id, &digest).await? {
        return Ok(Cloned {
            inodes: m.inode_count,
            data_bytes: m.data_bytes,
            bases: base_ids(&m),
            existing: true,
        });
    }
    if dst.exists(&Key::Segment(0)).await? {
        return Err(VolumeError::Exists(dst.root().to_string()));
    }
    let work = scratch.join(format!("clone-{}", hex(&dst_id)));
    let r = replay(base, base_id, &work.join("meta"), false).await?;
    if !r.tail.is_empty() {
        return Err(VolumeError::Corrupt(format!(
            "sealed volume {} has a log after its snapshot",
            base.root()
        )));
    }
    let mut store = r.live;
    store.externalize(&BaseVolume {
        volume: base_id,
        root: base.root().to_string(),
    })?;
    store.reset_genesis(&dst_id, &digest)?;
    let exported = snapshot::export(&store, &work, 0)?;
    drop(store);
    let committed = commit_genesis(dst, &dst_id, exported, async || Ok(())).await?;
    let _ = std::fs::remove_dir_all(&work);
    let m = committed.ok_or_else(|| VolumeError::Exists(dst.root().to_string()))?;
    Ok(Cloned {
        inodes: m.inode_count,
        data_bytes: m.data_bytes,
        bases: base_ids(&m),
        existing: false,
    })
}

/// Generation 0's manifest of `dst` if an earlier attempt of this same
/// operation committed it (its restore pointer carries `digest`); `None`
/// if `dst` has no generation 0; `Exists` if something else made it.
async fn existing_genesis(
    dst: &ObjectClient,
    dst_id: &[u8; 16],
    digest: &[u8; 32],
) -> Result<Option<SnapshotManifest>, VolumeError> {
    if !dst.exists(&Key::SnapshotManifest(0)).await? {
        return Ok(None);
    }
    let m = snapshot::read_manifest(dst, 0, dst_id).await?;
    if m.restored_seq != 0 || m.restored_digest != digest {
        return Err(VolumeError::Exists(dst.root().to_string()));
    }
    Ok(Some(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>(_: &T) {}

    /// Callers run these inside multi-threaded runtimes (Temporal
    /// activities, servers): the futures must stay `Send`.
    #[test]
    fn the_futures_are_send() {
        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let c = ObjectClient::new(store, skyfs_store::VolumeRoot::new("", &[1; 16]));
        let mut keep = |_: &[u8], _: Kind| true;
        let scratch = Path::new("/nonexistent");
        assert_send(&publish(&c, [1; 16], &c, [2; 16], &mut keep, scratch));
        assert_send(&clone_volume(&c, [1; 16], &c, [2; 16], scratch));
    }
}
