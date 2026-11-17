//! Opening an object store from a URL and a credential source.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{ClientOptions, ObjectStore};

use crate::credentials::{
    Access, CredentialError, CredentialSource, FileSource, Refreshing, WebIdentitySource,
    refuse_static_env, session_policy,
};
use crate::key::VolumeRoot;

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
    /// Longest a single request may take, body included. With the upload
    /// parallelism this sets the slowest link that makes progress.
    pub request_timeout: Duration,
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

/// Opens the backend `config` names. OIDC credentials are narrowed to
/// `volume_id` (read-only if `read_only`) and their session is named after
/// `writer_id`.
pub fn open(
    config: &StoreConfig,
    volume_id: &[u8; 16],
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

    let prefix = prefix.trim_matches('/');

    let builder = AmazonS3Builder::from_env();
    let mut b = match &config.credentials {
        CredentialSource::Ambient => {
            refuse_static_env()?;
            builder
        }
        CredentialSource::Static { file: None } => {
            if std::env::var_os("AWS_ACCESS_KEY_ID").is_none() {
                return Err(CredentialError::Config(
                    "--credentials static needs --credentials-file or AWS_ACCESS_KEY_ID".into(),
                )
                .into());
            }
            builder
        }
        CredentialSource::Static { file: Some(path) } => {
            builder.with_credentials(Arc::new(Refreshing::new(FileSource::new(path.clone()))))
        }
        CredentialSource::WebIdentity(w) => {
            let sts = match (&w.sts_endpoint, &config.region) {
                (Some(e), _) => e.clone(),
                (None, Some(r)) if r != "auto" => format!("https://sts.{r}.amazonaws.com"),
                (None, _) => "https://sts.amazonaws.com".to_owned(),
            };
            let access = if read_only {
                Access::ReadOnly
            } else {
                Access::ReadWrite
            };
            let root = VolumeRoot::new(prefix, volume_id).to_string();
            let policy = w
                .session_policy
                .then(|| session_policy(bucket, &root, access));
            builder.with_credentials(Arc::new(Refreshing::new(WebIdentitySource::new(
                &sts,
                w.role_arn.clone(),
                w.token_file.clone(),
                writer_id,
                policy,
            ))))
        }
    };
    b = b
        .with_bucket_name(bucket)
        .with_client_options(ClientOptions::new().with_timeout(config.request_timeout));
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
        prefix: prefix.to_owned(),
    })
}
