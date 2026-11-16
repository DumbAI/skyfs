//! Opening an object store from a URL and a credential source.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;

use crate::credentials::{
    BrokerSource, CredentialError, CredentialSource, FileSource, ProcessSource, Refreshing,
    refuse_static_env,
};

/// Where a volume's bucket is and how to reach it.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// `s3://bucket/prefix`, `file:///abs/dir/prefix` or `memory://`.
    pub url: String,
    /// S3-compatible endpoint, e.g. `https://<account>.r2.cloudflarestorage.com`.
    pub endpoint: Option<String>,
    /// Region; R2 uses `auto`.
    pub region: Option<String>,
    /// Allow plain HTTP endpoints (local test servers).
    pub allow_http: bool,
    /// Where credentials come from.
    pub credentials: CredentialSource,
}

/// Failure to open a backend.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// The URL is not one skyfs understands.
    #[error("unsupported store URL {0:?} (expected s3://, file:// or memory://)")]
    Url(String),
    /// The backend refused its configuration.
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
    /// Credentials are unusable.
    #[error(transparent)]
    Credentials(#[from] CredentialError),
}

/// An opened backend and the tenant prefix under which volumes live.
#[derive(Debug, Clone)]
pub struct Backend {
    /// The object store.
    pub store: Arc<dyn ObjectStore>,
    /// Prefix inside the bucket, without slashes at either end.
    pub prefix: String,
}

/// Opens the backend `config` names. `volume_id_hex` and `writer_id` scope
/// broker credentials to one volume and identify the mount.
pub fn open(
    config: &StoreConfig,
    volume_id_hex: &str,
    writer_id: &str,
    read_only: bool,
) -> Result<Backend, BackendError> {
    let url = config.url.as_str();
    if url == "memory://" {
        return Ok(Backend {
            store: Arc::new(InMemory::new()),
            prefix: String::new(),
        });
    }
    if let Some(dir) = url.strip_prefix("file://") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).map_err(|e| object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(e),
        })?;
        return Ok(Backend {
            store: Arc::new(LocalFileSystem::new_with_prefix(dir)?),
            prefix: String::new(),
        });
    }
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| BackendError::Url(url.to_owned()))?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err(BackendError::Url(url.to_owned()));
    }

    let mut b = match &config.credentials {
        CredentialSource::Ambient { allow_static } => {
            if !allow_static {
                refuse_static_env()?;
            }
            AmazonS3Builder::from_env()
        }
        CredentialSource::File(path) => AmazonS3Builder::from_env()
            .with_credentials(Arc::new(Refreshing::new(FileSource::new(path.clone())))),
        CredentialSource::Process(argv) => AmazonS3Builder::from_env()
            .with_credentials(Arc::new(Refreshing::new(ProcessSource::new(argv.clone())))),
        CredentialSource::Broker { url, token_file } => AmazonS3Builder::from_env()
            .with_credentials(Arc::new(Refreshing::new(BrokerSource::new(
                url,
                volume_id_hex.to_owned(),
                token_file.clone(),
                writer_id.to_owned(),
                read_only,
            )))),
    };
    b = b.with_bucket_name(bucket);
    if let Some(endpoint) = &config.endpoint {
        b = b.with_endpoint(endpoint);
    }
    if let Some(region) = &config.region {
        b = b.with_region(region);
    }
    if config.allow_http {
        b = b.with_allow_http(true);
    }
    Ok(Backend {
        store: Arc::new(b.build()?),
        prefix: prefix.trim_matches('/').to_owned(),
    })
}
