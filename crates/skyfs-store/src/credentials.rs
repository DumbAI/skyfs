//! Short-lived, scoped credentials for blob storage (design §10.1).
//!
//! A skyfs daemon never needs a long-lived key. It holds one credential at
//! a time, scoped to one volume's prefix and valid for minutes to an hour,
//! and replaces it before it expires. Every source below yields the same
//! thing, an S3-style key triple with an expiry, in the AWS
//! `credential_process` JSON format, so any tool that speaks that format
//! (Vault, a cloud SDK, a script) can feed skyfs:
//!
//! ```json
//! {"Version": 1, "AccessKeyId": "…", "SecretAccessKey": "…",
//!  "SessionToken": "…", "Expiration": "2026-11-16T12:00:00Z"}
//! ```
//!
//! [`Refreshing`] caches the credential and fetches a new one when the
//! current one is within a margin of expiry. If a refresh fails it keeps
//! serving the old credential until it actually expires; after that,
//! requests fail and the callers' retry loops take over (writes continue
//! into the local WAL, exactly as when the bucket is unreachable).

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use object_store::CredentialProvider;
use object_store::aws::AwsCredential;
use serde::Deserialize;

/// Why a credential could not be obtained.
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    /// Reading a credential or token file failed.
    #[error("reading {path}: {source}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The error.
        source: std::io::Error,
    },
    /// The credential document is malformed.
    #[error("malformed credential: {0}")]
    Malformed(String),
    /// The credential command failed.
    #[error("credential command failed: {0}")]
    Command(String),
    /// The broker refused or could not be reached.
    #[error("credential broker: {0}")]
    Broker(String),
    /// Static keys were found where short-lived ones are required.
    #[error(
        "long-lived static keys found in the environment (AWS_ACCESS_KEY_ID without \
         AWS_SESSION_TOKEN); use a short-lived credential source, or pass \
         --allow-static-credentials for development"
    )]
    StaticRefused,
}

/// A credential and when it stops working.
#[derive(Clone)]
pub struct ShortLived {
    /// The key triple.
    pub credential: Arc<AwsCredential>,
    /// Expiry; `None` means the source did not say (treated as static).
    pub expires_at: Option<SystemTime>,
}

impl fmt::Debug for ShortLived {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShortLived")
            .field("key_id", &self.credential.key_id)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// The AWS `credential_process` output format.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ProcessCredential {
    #[serde(default)]
    version: Option<u32>,
    access_key_id: String,
    secret_access_key: String,
    #[serde(default)]
    session_token: Option<String>,
    #[serde(default)]
    expiration: Option<String>,
    /// skyfs broker extension: a single-use token for the next exchange.
    #[serde(default)]
    refresh_token: Option<String>,
}

/// Parses a `credential_process` JSON document.
pub fn parse_credential(json: &[u8]) -> Result<ShortLived, CredentialError> {
    parse_with_refresh(json).map(|(c, _)| c)
}

fn parse_with_refresh(json: &[u8]) -> Result<(ShortLived, Option<String>), CredentialError> {
    let c: ProcessCredential =
        serde_json::from_slice(json).map_err(|e| CredentialError::Malformed(e.to_string()))?;
    if c.version.is_some_and(|v| v != 1) {
        return Err(CredentialError::Malformed(format!(
            "unsupported Version {:?}",
            c.version
        )));
    }
    let expires_at = c
        .expiration
        .map(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .map(SystemTime::from)
                .map_err(|e| CredentialError::Malformed(format!("Expiration {s:?}: {e}")))
        })
        .transpose()?;
    Ok((
        ShortLived {
            credential: Arc::new(AwsCredential {
                key_id: c.access_key_id,
                secret_key: c.secret_access_key,
                token: c.session_token,
            }),
            expires_at,
        },
        c.refresh_token,
    ))
}

/// Something that can produce a fresh credential.
#[async_trait]
pub trait Fetch: fmt::Debug + Send + Sync + 'static {
    /// Fetches a new credential.
    async fn fetch(&self) -> Result<ShortLived, CredentialError>;
}

/// Reads a `credential_process` JSON file that an agent keeps fresh: a
/// sidecar, Vault Agent, or a Kubernetes projected secret. Mount it on a
/// tmpfs, readable only by the daemon.
#[derive(Debug, Clone)]
pub struct FileSource {
    path: PathBuf,
}

impl FileSource {
    /// A source reading `path` on every refresh.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl Fetch for FileSource {
    async fn fetch(&self) -> Result<ShortLived, CredentialError> {
        let bytes = tokio::fs::read(&self.path)
            .await
            .map_err(|source| CredentialError::Read {
                path: self.path.clone(),
                source,
            })?;
        parse_credential(&bytes)
    }
}

/// Runs a command that prints a `credential_process` JSON document, like
/// the AWS CLI's `credential_process` setting.
#[derive(Debug, Clone)]
pub struct ProcessSource {
    argv: Vec<String>,
}

impl ProcessSource {
    /// A source running `argv[0]` with the remaining arguments.
    #[must_use]
    pub fn new(argv: Vec<String>) -> Self {
        Self { argv }
    }
}

#[async_trait]
impl Fetch for ProcessSource {
    async fn fetch(&self) -> Result<ShortLived, CredentialError> {
        let (cmd, args) = self
            .argv
            .split_first()
            .ok_or_else(|| CredentialError::Command("empty command".into()))?;
        let out = tokio::process::Command::new(cmd)
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .map_err(|e| CredentialError::Command(format!("{cmd}: {e}")))?;
        if !out.status.success() {
            return Err(CredentialError::Command(format!(
                "{cmd} exited with {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        parse_credential(&out.stdout)
    }
}

/// Exchanges a workload identity token for a volume-scoped credential at
/// the skyfs credential broker (design §10.1).
///
/// `POST {url}/v1/volumes/{volume_id}/credentials` with
/// `Authorization: Bearer <token>` and body `{"writer_id", "access"}`;
/// the response is a `credential_process` document.
///
/// The bearer token is, in order of preference:
/// - the `RefreshToken` the broker returned last time: single-use,
///   rotated on every exchange, held only in memory. It lets a container
///   whose only proof of identity was a one-time mount token keep
///   refreshing without any long-lived secret;
/// - the token file, read on every exchange (Kubernetes rotates projected
///   service-account tokens in place; an orchestrator may rewrite a mount
///   token). Used first, and again whenever a refresh token is refused.
pub struct BrokerSource {
    url: String,
    volume_id: String,
    token_file: PathBuf,
    writer_id: String,
    read_only: bool,
    http: reqwest::Client,
    refresh: parking_lot::Mutex<Option<String>>,
}

impl fmt::Debug for BrokerSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerSource")
            .field("url", &self.url)
            .field("volume_id", &self.volume_id)
            .field("token_file", &self.token_file)
            .finish_non_exhaustive()
    }
}

impl BrokerSource {
    /// A broker client for one volume (`volume_id` in hex).
    #[must_use]
    pub fn new(
        url: &str,
        volume_id: String,
        token_file: PathBuf,
        writer_id: String,
        read_only: bool,
    ) -> Self {
        Self {
            url: url.trim_end_matches('/').to_owned(),
            volume_id,
            token_file,
            writer_id,
            read_only,
            http: reqwest::Client::new(),
            refresh: parking_lot::Mutex::new(None),
        }
    }

    async fn exchange(
        &self,
        token: &str,
    ) -> Result<(ShortLived, Option<String>), (bool, CredentialError)> {
        let body = serde_json::json!({
            "writer_id": self.writer_id,
            "access": if self.read_only { "read-only" } else { "read-write" },
        });
        let resp = self
            .http
            .post(format!(
                "{}/v1/volumes/{}/credentials",
                self.url, self.volume_id
            ))
            .bearer_auth(token.trim())
            .json(&body)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| (false, CredentialError::Broker(e.to_string())))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| (false, CredentialError::Broker(e.to_string())))?;
        if !status.is_success() {
            let unauthorized = status == reqwest::StatusCode::UNAUTHORIZED;
            return Err((
                unauthorized,
                CredentialError::Broker(format!(
                    "{status}: {}",
                    String::from_utf8_lossy(&bytes).trim()
                )),
            ));
        }
        parse_with_refresh(&bytes).map_err(|e| (false, e))
    }

    async fn token_from_file(&self) -> Result<String, CredentialError> {
        tokio::fs::read_to_string(&self.token_file)
            .await
            .map_err(|source| CredentialError::Read {
                path: self.token_file.clone(),
                source,
            })
    }
}

#[async_trait]
impl Fetch for BrokerSource {
    async fn fetch(&self) -> Result<ShortLived, CredentialError> {
        let held = self.refresh.lock().take();
        let result = match held {
            Some(t) => match self.exchange(&t).await {
                // Refused (used, expired, revoked): fall back to the file.
                Err((true, _)) => self.exchange(&self.token_from_file().await?).await,
                other => other,
            },
            None => self.exchange(&self.token_from_file().await?).await,
        };
        let (cred, next) = result.map_err(|(_, e)| e)?;
        *self.refresh.lock() = next;
        Ok(cred)
    }
}

/// Caches a [`Fetch`] source's credential and refreshes it ahead of expiry.
#[derive(Debug)]
pub struct Refreshing<F> {
    source: F,
    current: tokio::sync::Mutex<Option<(ShortLived, SystemTime)>>,
}

/// Refresh when less than this much validity remains (or a quarter of the
/// credential's lifetime, whichever is less).
pub const REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// Re-read sources that give no expiry this often, so a rotated file or
/// broker credential is picked up.
pub const UNDATED_REFRESH: Duration = Duration::from_secs(300);

impl<F: Fetch> Refreshing<F> {
    /// Wraps `source`.
    pub fn new(source: F) -> Self {
        Self {
            source,
            current: tokio::sync::Mutex::new(None),
        }
    }

    fn fresh(cached: &ShortLived, fetched_at: SystemTime, now: SystemTime) -> bool {
        match cached.expires_at {
            Some(exp) => {
                let life = exp.duration_since(fetched_at).unwrap_or_default();
                let margin = REFRESH_MARGIN.min(life / 4);
                now + margin < exp
            }
            None => now.duration_since(fetched_at).unwrap_or_default() < UNDATED_REFRESH,
        }
    }

    /// The current credential, refreshed if needed.
    pub async fn get(&self) -> Result<Arc<AwsCredential>, CredentialError> {
        let now = SystemTime::now();
        let mut guard = self.current.lock().await;
        if let Some((cached, at)) = guard.as_ref() {
            if Self::fresh(cached, *at, now) {
                return Ok(cached.credential.clone());
            }
        }
        match self.source.fetch().await {
            Ok(fresh) => {
                if fresh.expires_at.is_some_and(|e| e <= now) {
                    return Err(CredentialError::Malformed(
                        "credential already expired".into(),
                    ));
                }
                let cred = fresh.credential.clone();
                *guard = Some((fresh, now));
                Ok(cred)
            }
            Err(e) => match guard.as_ref() {
                // Keep using a credential that still works; retry next time.
                Some((cached, _)) if cached.expires_at.is_none_or(|exp| now < exp) => {
                    tracing::warn!(error = %e, "credential refresh failed; using the current one");
                    Ok(cached.credential.clone())
                }
                _ => Err(e),
            },
        }
    }
}

#[async_trait]
impl<F: Fetch> CredentialProvider for Refreshing<F> {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        self.get().await.map_err(|e| object_store::Error::Generic {
            store: "skyfs-credentials",
            source: Box::new(e),
        })
    }
}

/// Where a mount gets blob storage credentials.
#[derive(Debug, Clone)]
pub enum CredentialSource {
    /// The platform's workload identity, through the standard AWS chain:
    /// EKS IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE`), EKS Pod Identity and ECS
    /// (`AWS_CONTAINER_CREDENTIALS_*`), EC2 instance roles. Static keys in
    /// the environment are refused unless `allow_static`.
    Ambient {
        /// Accept `AWS_ACCESS_KEY_ID` without a session token (development).
        allow_static: bool,
    },
    /// A `credential_process` JSON file kept fresh by an agent.
    File(PathBuf),
    /// A command printing a `credential_process` JSON document.
    Process(Vec<String>),
    /// The skyfs credential broker, authenticated with a workload token.
    Broker {
        /// Broker base URL.
        url: String,
        /// File holding the bearer token (re-read on every exchange).
        token_file: PathBuf,
    },
}

/// Fails if the environment carries long-lived static keys.
pub fn refuse_static_env() -> Result<(), CredentialError> {
    let has = |k: &str| std::env::var_os(k).is_some_and(|v| !v.is_empty());
    if has("AWS_ACCESS_KEY_ID") && !has("AWS_SESSION_TOKEN") {
        return Err(CredentialError::StaticRefused);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug)]
    struct Counting {
        calls: AtomicU32,
        ttl: Option<Duration>,
        fail_after: u32,
    }

    #[async_trait]
    impl Fetch for Counting {
        async fn fetch(&self) -> Result<ShortLived, CredentialError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n >= self.fail_after {
                return Err(CredentialError::Broker("down".into()));
            }
            Ok(ShortLived {
                credential: Arc::new(AwsCredential {
                    key_id: format!("k{n}"),
                    secret_key: "s".into(),
                    token: Some("t".into()),
                }),
                expires_at: self.ttl.map(|t| SystemTime::now() + t),
            })
        }
    }

    #[test]
    fn parses_credential_process_json() {
        let c = parse_credential(
            br#"{"Version":1,"AccessKeyId":"AK","SecretAccessKey":"SK","SessionToken":"T",
                 "Expiration":"2030-01-02T03:04:05Z"}"#,
        )
        .unwrap();
        assert_eq!(c.credential.key_id, "AK");
        assert_eq!(c.credential.token.as_deref(), Some("T"));
        assert!(c.expires_at.is_some());
        assert!(
            parse_credential(br#"{"Version":2,"AccessKeyId":"a","SecretAccessKey":"b"}"#).is_err()
        );
        assert!(parse_credential(b"not json").is_err());
    }

    #[tokio::test]
    async fn caches_until_near_expiry() {
        let r = Refreshing::new(Counting {
            calls: AtomicU32::new(0),
            ttl: Some(Duration::from_secs(3600)),
            fail_after: u32::MAX,
        });
        assert_eq!(r.get().await.unwrap().key_id, "k0");
        assert_eq!(r.get().await.unwrap().key_id, "k0");
        assert_eq!(r.source.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refreshes_when_inside_the_margin() {
        // A 2-second credential is always within a quarter of its life.
        let r = Refreshing::new(Counting {
            calls: AtomicU32::new(0),
            ttl: Some(Duration::from_secs(2)),
            fail_after: u32::MAX,
        });
        r.get().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1600)).await;
        assert_eq!(r.get().await.unwrap().key_id, "k1");
    }

    #[tokio::test]
    async fn keeps_a_valid_credential_when_refresh_fails() {
        let r = Refreshing::new(Counting {
            calls: AtomicU32::new(0),
            ttl: Some(Duration::from_secs(2)),
            fail_after: 1,
        });
        r.get().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1600)).await;
        assert_eq!(r.get().await.unwrap().key_id, "k0");
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(r.get().await.is_err(), "expired and no refresh");
    }

    /// A fake broker: answers each request with the next canned response,
    /// recording the bearer token it was given.
    async fn fake_broker(
        responses: Vec<(u16, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut seen = Vec::new();
            for (status, body) in responses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 8192];
                let mut req = Vec::new();
                loop {
                    let n = sock.read(&mut buf).await.unwrap();
                    req.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&req);
                    if let Some(h) = text.find("\r\n\r\n") {
                        let len: usize = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse().unwrap())
                            })
                            .unwrap_or(0);
                        if req.len() >= h + 4 + len {
                            break;
                        }
                    }
                }
                let text = String::from_utf8_lossy(&req).into_owned();
                let auth = text
                    .lines()
                    .find_map(|l| {
                        l.strip_prefix("authorization: Bearer ")
                            .or_else(|| l.strip_prefix("Authorization: Bearer "))
                    })
                    .unwrap_or("")
                    .to_owned();
                seen.push(auth);
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(reply.as_bytes()).await.unwrap();
            }
            seen
        });
        (url, task)
    }

    #[tokio::test]
    async fn broker_rotates_refresh_tokens_and_falls_back_to_the_file() {
        let cred = |id: &str, refresh: &str| {
            format!(
                r#"{{"AccessKeyId":"{id}","SecretAccessKey":"s","SessionToken":"t","RefreshToken":"{refresh}"}}"#
            )
        };
        let (url, task) = fake_broker(vec![
            (200, cred("a", "r1")),
            (200, cred("b", "r2")),
            (401, "refresh token revoked".into()),
            (200, cred("c", "r3")),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "mount-token\n").unwrap();
        let b = BrokerSource::new(&url, "00".repeat(16), token, "w".into(), false);
        assert_eq!(b.fetch().await.unwrap().credential.key_id, "a");
        assert_eq!(b.fetch().await.unwrap().credential.key_id, "b");
        // r2 is refused: the file token is used instead.
        assert_eq!(b.fetch().await.unwrap().credential.key_id, "c");
        assert_eq!(
            task.await.unwrap(),
            vec!["mount-token", "r1", "r2", "mount-token"]
        );
    }

    #[tokio::test]
    async fn file_source_rereads_rotations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cred.json");
        let doc = |id: &str| {
            format!(r#"{{"AccessKeyId":"{id}","SecretAccessKey":"s","SessionToken":"t"}}"#)
        };
        std::fs::write(&path, doc("one")).unwrap();
        let src = FileSource::new(path.clone());
        assert_eq!(src.fetch().await.unwrap().credential.key_id, "one");
        std::fs::write(&path, doc("two")).unwrap();
        assert_eq!(src.fetch().await.unwrap().credential.key_id, "two");
    }
}
