//! The object store client: the only code that talks to the bucket.
//!
//! It exposes exactly the operations the invariants allow: create-only PUTs
//! (`If-None-Match: *`) for every immutable object, one overwriting PUT for
//! the `volume.json` hint, range GETs, HEAD, DELETE, and LIST for GC only.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    WriteMultipart,
};
use tokio::io::AsyncReadExt;

use crate::key::{Key, KeyClass, VolumeRoot};

/// Objects up to this size go up in one PUT; larger ones use multipart.
pub const SINGLE_PUT_MAX: u64 = 8 << 20;

/// Multipart part size. R2 requires all parts but the last to be equal;
/// S3's minimum is 5 MiB.
pub const PART_SIZE: usize = 8 << 20;

/// Parts of one upload in flight at once. With `blob_uploads` uploads, at
/// most `blob_uploads × PARTS_IN_FLIGHT × PART_SIZE` bytes are on the wire,
/// and each request must finish within the request timeout: together they
/// set the slowest uplink that still makes progress (design §6.3). More
/// parallelism on a slow link makes every part time out and retry forever.
pub const PARTS_IN_FLIGHT: usize = 2;

/// Why an object operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The object does not exist.
    #[error("object {0} not found")]
    NotFound(String),
    /// The backend failed: network, credentials, throttling. Retryable.
    #[error("object store: {0}")]
    Backend(#[from] object_store::Error),
    /// Reading a local file to upload failed.
    #[error("local I/O: {0}")]
    Io(#[from] std::io::Error),
    /// The backend returned a key outside this layout.
    #[error("unexpected key {0:?} in a listing")]
    ForeignKey(String),
}

/// Outcome of a create-only PUT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Created {
    /// This call created the object.
    New,
    /// An object already existed at the key; nothing was written.
    AlreadyExists,
}

/// A volume's view of an object store.
#[derive(Debug, Clone)]
pub struct ObjectClient {
    store: Arc<dyn ObjectStore>,
    root: VolumeRoot,
}

impl ObjectClient {
    /// A client for the volume under `root` in `store`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, root: VolumeRoot) -> Self {
        Self { store, root }
    }

    /// The volume root.
    #[must_use]
    pub fn root(&self) -> &VolumeRoot {
        &self.root
    }

    fn path(&self, key: &Key) -> ObjPath {
        ObjPath::from(self.root.join(key))
    }

    /// Creates `key` with `bytes` unless it exists (`If-None-Match: *`).
    pub async fn put_new(&self, key: &Key, bytes: Bytes) -> Result<Created, StoreError> {
        debug_assert!(*key != Key::Hint, "the hint is the one mutable object");
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&self.path(key), PutPayload::from(bytes), opts)
            .await
        {
            Ok(_) => Ok(Created::New),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(Created::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    /// Overwrites the `volume.json` hint, the only mutable object.
    pub async fn put_hint(&self, bytes: Bytes) -> Result<(), StoreError> {
        self.store
            .put(&self.path(&Key::Hint), PutPayload::from(bytes))
            .await?;
        Ok(())
    }

    /// Uploads a local file to a key that is unique by construction (a
    /// blob). Large files go up with multipart in equal-sized parts.
    pub async fn upload_file(&self, key: &Key, file: &Path) -> Result<(), StoreError> {
        let len = tokio::fs::metadata(file).await?.len();
        if len <= SINGLE_PUT_MAX {
            let bytes = tokio::fs::read(file).await?;
            self.store
                .put(&self.path(key), PutPayload::from(bytes))
                .await?;
            return Ok(());
        }
        let upload = self.store.put_multipart(&self.path(key)).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, PART_SIZE);
        match Self::send_parts(&mut writer, file).await {
            Ok(()) => {
                writer.finish().await?;
                Ok(())
            }
            Err(e) => {
                // A retry starts a new upload; free this one's stored parts
                // now rather than leave them to a lifecycle rule.
                if let Err(abort) = writer.abort().await {
                    tracing::debug!(error = %abort, "aborting a failed multipart upload");
                }
                Err(e)
            }
        }
    }

    async fn send_parts(writer: &mut WriteMultipart, file: &Path) -> Result<(), StoreError> {
        let mut f = tokio::fs::File::open(file).await?;
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
            writer.wait_for_capacity(PARTS_IN_FLIGHT).await?;
            writer.write(&buf[..n]);
        }
    }

    /// The whole object, or `None` if it does not exist.
    pub async fn get(&self, key: &Key) -> Result<Option<Bytes>, StoreError> {
        match self.store.get(&self.path(key)).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Bytes `range` of an object.
    pub async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, StoreError> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        match self.store.get_range(&self.path(key), range).await {
            Ok(b) => Ok(b),
            Err(object_store::Error::NotFound { .. }) => Err(StoreError::NotFound(key.to_string())),
            Err(e) => Err(e.into()),
        }
    }

    /// Up to the first `len` bytes of an object (fewer if it is shorter).
    pub async fn get_prefix(&self, key: &Key, len: u64) -> Result<Option<Bytes>, StoreError> {
        let opts = GetOptions {
            range: Some(GetRange::Bounded(0..len)),
            ..GetOptions::default()
        };
        match self.store.get_opts(&self.path(key), opts).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Whether the object exists (HEAD).
    pub async fn exists(&self, key: &Key) -> Result<bool, StoreError> {
        match self.store.head(&self.path(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes an object; deleting a missing one succeeds.
    pub async fn delete(&self, key: &Key) -> Result<(), StoreError> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Every object of `class` with its size. For GC only: no correctness
    /// path may depend on a listing.
    pub async fn list(&self, class: KeyClass) -> Result<Vec<(Key, u64)>, StoreError> {
        let prefix = ObjPath::from(self.root.class_prefix(class));
        let metas: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
        metas
            .into_iter()
            .map(|m| {
                let abs = m.location.to_string();
                let key = self
                    .root
                    .relative(&abs)
                    .and_then(|r| r.parse::<Key>().ok())
                    .ok_or_else(|| StoreError::ForeignKey(abs.clone()))?;
                Ok((key, m.size))
            })
            .collect()
    }
}

/// Finds the first `n` in `[from, ∞)` for which `exists(n)` is false, given
/// that existence is a prefix property (dense sequences). Gallops, then
/// binary-searches: O(log distance) HEADs, never a LIST.
pub async fn find_end<F, Fut>(from: u64, mut exists: F) -> Result<u64, StoreError>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<bool, StoreError>>,
{
    if !exists(from).await? {
        return Ok(from);
    }
    // Invariant: lo exists, hi does not.
    let (mut lo, mut step) = (from, 1u64);
    let mut hi = loop {
        let probe = lo.saturating_add(step);
        if !exists(probe).await? {
            break probe;
        }
        lo = probe;
        step = step.saturating_mul(2);
    };
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if exists(mid).await? {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok(hi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn client() -> ObjectClient {
        ObjectClient::new(Arc::new(InMemory::new()), VolumeRoot::new("t", &[1; 16]))
    }

    #[tokio::test]
    async fn create_only_put_never_overwrites() {
        let c = client();
        let k = Key::Segment(0);
        assert_eq!(
            c.put_new(&k, Bytes::from_static(b"a")).await.unwrap(),
            Created::New
        );
        assert_eq!(
            c.put_new(&k, Bytes::from_static(b"b")).await.unwrap(),
            Created::AlreadyExists
        );
        assert_eq!(c.get(&k).await.unwrap().unwrap(), Bytes::from_static(b"a"));
        assert_eq!(
            c.get_range(&k, 0..1).await.unwrap(),
            Bytes::from_static(b"a")
        );
        assert_eq!(c.get_prefix(&k, 100).await.unwrap().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_parses_keys() {
        let c = client();
        for seq in 0..3 {
            c.put_new(&Key::Segment(seq), Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
        c.put_new(&Key::Pack([2; 32]), Bytes::from_static(b"p"))
            .await
            .unwrap();
        let mut segs = c.list(KeyClass::Segments).await.unwrap();
        segs.sort_by_key(|(k, _)| k.to_string());
        assert_eq!(
            segs,
            vec![
                (Key::Segment(0), 1),
                (Key::Segment(1), 1),
                (Key::Segment(2), 1)
            ]
        );
    }

    #[tokio::test]
    async fn find_end_gallops() {
        for end in [0u64, 1, 2, 3, 17, 1000] {
            let mut probes = 0;
            let found = find_end(0, |n| {
                probes += 1;
                async move { Ok(n < end) }
            })
            .await
            .unwrap();
            assert_eq!(found, end);
            assert!(probes <= 2 * 64);
        }
        assert_eq!(find_end(5, |n| async move { Ok(n < 9) }).await.unwrap(), 9);
    }
}
