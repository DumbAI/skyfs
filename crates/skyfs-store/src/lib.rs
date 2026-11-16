//! skyfs storage: the object layout, the object store client, credentials
//! and the local cache.
//!
//! - [`key`]: the only place that spells an object key.
//! - [`client`]: the only code that talks to the bucket, with exactly the
//!   operations the invariants allow (create-only PUTs, one mutable hint).
//! - [`credentials`]: short-lived, volume-scoped credentials, refreshed
//!   ahead of expiry (design §10.1).
//! - [`backend`]: opens S3, R2, a local directory or memory from a URL.
//! - [`cache`]: the local LRU of immutable bytes.

pub mod backend;
pub mod cache;
pub mod client;
pub mod credentials;
pub mod key;

pub use backend::{Backend, BackendError, StoreConfig};
pub use cache::Cache;
pub use client::{Created, ObjectClient, StoreError, find_end};
pub use credentials::CredentialSource;
pub use key::{Key, KeyClass, ParseKeyError, VolumeRoot, hex, unhex32};
