//! Fault injection for the bucket (tests only, feature `testing`).
//!
//! [`FaultStore`] wraps an object store and numbers every mutation (PUT,
//! completed multipart upload, DELETE) in the order it reaches the bucket.
//! A [`Faults`] plan picks one mutation and what happens to it:
//!
//! - [`Fault::Fail`]: refused before it lands, a transient error.
//! - [`Fault::LostAck`]: lands, but the caller sees an error (the response
//!   was lost), so it retries something that already happened.
//! - [`Fault::CrashBefore`] / [`Fault::CrashAfter`]: the process dies just
//!   before or just after the mutation lands. Every later operation, reads
//!   included, fails: nothing of the dead process reaches the bucket again.
//!
//! Enumerating every mutation index with every fault covers every crash
//! point of commit, recovery, restore, GC and shutdown that the run hits.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use parking_lot::Mutex;

/// What happens to the chosen mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Refused before it lands; the caller may retry.
    Fail,
    /// Lands, but the caller sees an error.
    LostAck,
    /// The process dies before it lands.
    CrashBefore,
    /// The process dies after it lands, before the caller hears.
    CrashAfter,
}

impl Fault {
    /// Every fault.
    pub const ALL: [Self; 4] = [
        Self::Fail,
        Self::LostAck,
        Self::CrashBefore,
        Self::CrashAfter,
    ];

    /// Whether the process dies.
    #[must_use]
    pub fn is_crash(self) -> bool {
        matches!(self, Self::CrashBefore | Self::CrashAfter)
    }
}

/// The plan and the record of what happened.
#[derive(Debug, Default)]
pub struct Faults {
    plan: Mutex<Option<(u64, Fault)>>,
    mutations: AtomicU64,
    dead: AtomicBool,
    fired: AtomicBool,
    log: Mutex<Vec<String>>,
}

impl Faults {
    /// No fault: counts and logs mutations only.
    #[must_use]
    pub fn none() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `fault` at mutation number `at` (0-based).
    #[must_use]
    pub fn at(at: u64, fault: Fault) -> Arc<Self> {
        let f = Self::default();
        *f.plan.lock() = Some((at, fault));
        Arc::new(f)
    }

    /// Mutations seen so far.
    #[must_use]
    pub fn mutations(&self) -> u64 {
        self.mutations.load(Ordering::SeqCst)
    }

    /// Whether the planned fault happened.
    #[must_use]
    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// Whether the simulated process is dead.
    #[must_use]
    pub fn dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    /// The mutations in order, for diagnosing a failing case.
    #[must_use]
    pub fn log(&self) -> Vec<String> {
        self.log.lock().clone()
    }

    fn check_alive(&self) -> object_store::Result<()> {
        if self.dead() {
            Err(injected("the process is dead"))
        } else {
            Ok(())
        }
    }

    /// Gate before a mutation: the fault to apply after it lands, if any.
    fn before(&self, what: &str) -> object_store::Result<Option<Fault>> {
        self.check_alive()?;
        let n = self.mutations.fetch_add(1, Ordering::SeqCst);
        let planned = (*self.plan.lock()).and_then(|(at, f)| (at == n).then_some(f));
        self.log.lock().push(format!(
            "{n}: {what}{}",
            planned.map_or(String::new(), |f| format!("  <- {f:?}"))
        ));
        match planned {
            None => Ok(None),
            Some(f) => {
                self.fired.store(true, Ordering::SeqCst);
                match f {
                    Fault::Fail => Err(injected("transient failure")),
                    Fault::CrashBefore => {
                        self.dead.store(true, Ordering::SeqCst);
                        Err(injected("crashed before the request"))
                    }
                    Fault::LostAck | Fault::CrashAfter => Ok(Some(f)),
                }
            }
        }
    }

    /// Gate after a mutation landed.
    fn after<T>(
        &self,
        landed: object_store::Result<T>,
        f: Option<Fault>,
    ) -> object_store::Result<T> {
        match f {
            None => landed,
            Some(Fault::CrashAfter) => {
                self.dead.store(true, Ordering::SeqCst);
                Err(injected("crashed before the response"))
            }
            Some(_) => landed.and(Err(injected("response lost"))),
        }
    }
}

fn injected(what: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "faults",
        source: what.into(),
    }
}

/// An object store that injects [`Faults`].
#[derive(Debug, Clone)]
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    faults: Arc<Faults>,
}

impl FaultStore {
    /// Wraps `inner`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, faults: Arc<Faults>) -> Self {
        Self { inner, faults }
    }
}

impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let f = self.faults.before(&format!("PUT {location}"))?;
        let landed = self.inner.put_opts(location, payload, opts).await;
        self.faults.after(landed, f)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.faults.check_alive()?;
        Ok(Box::new(FaultUpload {
            inner: self.inner.put_multipart_opts(location, opts).await?,
            faults: self.faults.clone(),
            location: location.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.faults.check_alive()?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.faults.check_alive()?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let faults = self.faults.clone();
        locations
            .then(move |p| {
                let inner = inner.clone();
                let faults = faults.clone();
                async move {
                    let p = p?;
                    let f = faults.before(&format!("DELETE {p}"))?;
                    let landed = inner.delete(&p).await.map(|()| p);
                    faults.after(landed, f)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.faults.dead() {
            return futures::stream::once(async { Err(injected("the process is dead")) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.faults.check_alive()?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let f = self.faults.before(&format!("COPY {from} {to}"))?;
        let landed = self.inner.copy_opts(from, to, options).await;
        self.faults.after(landed, f)
    }
}

/// A multipart upload whose completion is the mutation.
#[derive(Debug)]
struct FaultUpload {
    inner: Box<dyn MultipartUpload>,
    faults: Arc<Faults>,
    location: Path,
}

#[async_trait]
impl MultipartUpload for FaultUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.faults.dead() {
            return Box::pin(async { Err(injected("the process is dead")) });
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let f = self
            .faults
            .before(&format!("COMPLETE MULTIPART {}", self.location))?;
        let landed = self.inner.complete().await;
        self.faults.after(landed, f)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    async fn put(s: &FaultStore, k: &str) -> object_store::Result<PutResult> {
        s.put(&Path::from(k), PutPayload::from_static(b"x")).await
    }

    #[tokio::test]
    async fn faults_fire_once_at_their_index() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = FaultStore::new(mem.clone(), Faults::at(1, Fault::LostAck));
        put(&s, "a").await.unwrap();
        assert!(put(&s, "b").await.is_err());
        assert!(mem.head(&Path::from("b")).await.is_ok(), "landed");
        put(&s, "c").await.unwrap();

        let s = FaultStore::new(mem.clone(), Faults::at(0, Fault::CrashBefore));
        assert!(put(&s, "d").await.is_err());
        assert!(mem.head(&Path::from("d")).await.is_err(), "never landed");
        assert!(s.head(&Path::from("a")).await.is_err(), "dead: reads fail");

        let faults = Faults::at(0, Fault::CrashAfter);
        let s = FaultStore::new(mem.clone(), faults.clone());
        assert!(s.delete(&Path::from("a")).await.is_err());
        assert!(
            mem.head(&Path::from("a")).await.is_err(),
            "the delete landed"
        );
        assert!(faults.dead() && faults.fired());
    }
}
