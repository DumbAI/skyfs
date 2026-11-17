//! Credentials for blob storage (design §10.1).
//!
//! skyfs is a standalone daemon or sidecar and talks to no service but the
//! bucket (and, for OIDC, the bucket's STS). A deployment gives it one of:
//!
//! - **OIDC workload identity** ([`WebIdentitySource`]): a token the
//!   platform issues and rotates (a Kubernetes projected service-account
//!   token, a SPIFFE JWT-SVID, a CI job token) exchanged at STS with
//!   `AssumeRoleWithWebIdentity` for a credential valid for at most an
//!   hour. No secret is stored anywhere. AWS S3 (EKS natively, or any
//!   cluster whose issuer is public), MinIO, Ceph RGW and Backblaze B2
//!   accept it.
//! - **The platform's ambient identity**: EKS Pod Identity, ECS task roles,
//!   EC2 instance roles, through the standard AWS chain.
//! - **Static keys** ([`FileSource`] or the environment): for stores with
//!   no identity federation (Cloudflare R2), scoped to one bucket by the
//!   store's own token policy. A file is re-read, so a rotated Secret is
//!   picked up without a restart.
//!
//! Every source yields an S3-style key triple with an optional expiry.
//! Files use the AWS `credential_process` JSON format:
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
use std::fmt::Write as _;
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
    /// STS refused the token or could not be reached.
    #[error("STS AssumeRoleWithWebIdentity: {0}")]
    Sts(String),
    /// A credential source is missing a setting.
    #[error("credentials: {0}")]
    Config(String),
    /// Static keys were found where the platform identity was asked for.
    #[error(
        "static keys found in the environment (AWS_ACCESS_KEY_ID without \
         AWS_SESSION_TOKEN) with --credentials ambient; pass --credentials static \
         to use them, or remove them to use the platform identity"
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
}

fn parse_expiration(s: &str) -> Result<SystemTime, CredentialError> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(SystemTime::from)
        .map_err(|e| CredentialError::Malformed(format!("Expiration {s:?}: {e}")))
}

/// Parses a `credential_process` JSON document.
pub fn parse_credential(json: &[u8]) -> Result<ShortLived, CredentialError> {
    let c: ProcessCredential =
        serde_json::from_slice(json).map_err(|e| CredentialError::Malformed(e.to_string()))?;
    if c.version.is_some_and(|v| v != 1) {
        return Err(CredentialError::Malformed(format!(
            "unsupported Version {:?}",
            c.version
        )));
    }
    Ok(ShortLived {
        credential: Arc::new(AwsCredential {
            key_id: c.access_key_id,
            secret_key: c.secret_access_key,
            token: c.session_token,
        }),
        expires_at: c.expiration.as_deref().map(parse_expiration).transpose()?,
    })
}

/// Something that can produce a fresh credential.
#[async_trait]
pub trait Fetch: fmt::Debug + Send + Sync + 'static {
    /// Fetches a new credential.
    async fn fetch(&self) -> Result<ShortLived, CredentialError>;
}

/// Reads a `credential_process` JSON file: static keys from a mounted
/// Kubernetes Secret, or a short-lived credential an agent (Vault Agent, a
/// sidecar) keeps fresh. Re-read on every refresh, so rotation needs no
/// restart. Mount it on a tmpfs, readable only by the daemon.
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

/// What a session policy allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// `GetObject` only.
    ReadOnly,
    /// Read, create and delete objects.
    ReadWrite,
}

/// An IAM session policy narrowing a role to one volume's objects.
///
/// It bounds what this process can do with the credential (least
/// privilege for the daemon, and read-only for a read-only mount); it is
/// not a boundary against a compromised workload, which holds the identity
/// token and could ask STS without it. Isolation between tenants comes from
/// the role's own policy: one role per service account, allowed only its
/// prefix. `s3:ListBucket` on the prefix makes a HEAD of a missing key
/// answer 404 rather than 403; skyfs never lists on a correctness path.
#[must_use]
pub fn session_policy(bucket: &str, root: &str, access: Access) -> serde_json::Value {
    let objects = match access {
        Access::ReadOnly => serde_json::json!(["s3:GetObject"]),
        Access::ReadWrite => serde_json::json!([
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:AbortMultipartUpload"
        ]),
    };
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": objects,
                "Resource": format!("arn:aws:s3:::{bucket}/{root}/*"),
            },
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": format!("arn:aws:s3:::{bucket}"),
                "Condition": {"StringLike": {"s3:prefix": format!("{root}/*")}},
            }
        ]
    })
}

/// OIDC workload identity: exchanges a JWT for a short-lived credential
/// with STS `AssumeRoleWithWebIdentity`.
///
/// The request is unsigned; the token is the proof. The token file is read
/// on every exchange, because the platform rotates it in place (kubelet
/// refreshes projected service-account tokens at 80% of their lifetime).
/// The session is named after the mount's writer id, so the store's audit
/// log (CloudTrail) shows which mount made each request.
pub struct WebIdentitySource {
    sts_endpoint: String,
    role_arn: String,
    token_file: PathBuf,
    session_name: String,
    policy: Option<String>,
    duration: Duration,
    http: reqwest::Client,
}

impl fmt::Debug for WebIdentitySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebIdentitySource")
            .field("sts_endpoint", &self.sts_endpoint)
            .field("role_arn", &self.role_arn)
            .field("token_file", &self.token_file)
            .field("session_name", &self.session_name)
            .finish_non_exhaustive()
    }
}

/// Lifetime asked of STS: an hour, the longest a role chain allows and the
/// longest the design lets one credential live.
pub const WEB_IDENTITY_DURATION: Duration = Duration::from_secs(3600);

impl WebIdentitySource {
    /// A source assuming `role_arn` at `sts_endpoint` with the token in
    /// `token_file`, narrowed by `policy` (an IAM policy document) if given.
    #[must_use]
    pub fn new(
        sts_endpoint: &str,
        role_arn: String,
        token_file: PathBuf,
        session_name: &str,
        policy: Option<serde_json::Value>,
    ) -> Self {
        Self {
            sts_endpoint: sts_endpoint.trim_end_matches('/').to_owned(),
            role_arn,
            token_file,
            session_name: session_name_from(session_name),
            policy: policy.map(|p| p.to_string()),
            duration: WEB_IDENTITY_DURATION,
            http: reqwest::Client::new(),
        }
    }

    fn body(&self, token: &str) -> String {
        let mut fields = vec![
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", "2011-06-15"),
            ("RoleArn", self.role_arn.as_str()),
            ("RoleSessionName", self.session_name.as_str()),
            ("WebIdentityToken", token),
        ];
        let secs = self.duration.as_secs().to_string();
        fields.push(("DurationSeconds", &secs));
        if let Some(p) = &self.policy {
            fields.push(("Policy", p));
        }
        fields
            .iter()
            .map(|(k, v)| format!("{k}={}", form_encode(v)))
            .collect::<Vec<_>>()
            .join("&")
    }
}

/// A valid `RoleSessionName` (`[\w+=,.@-]{2,64}`) from a writer id.
fn session_name_from(writer_id: &str) -> String {
    let mut s: String = writer_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "_+=,.@-".contains(c) {
                c
            } else {
                '-'
            }
        })
        .take(64)
        .collect();
    while s.len() < 2 {
        s.push('-');
    }
    s
}

/// `application/x-www-form-urlencoded` encoding of one value.
fn form_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// The text of `<tag>…</tag>` in an STS response, unescaped.
fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let start = xml.find(&format!("<{tag}>"))? + tag.len() + 2;
    let end = start + xml[start..].find(&format!("</{tag}>"))?;
    Some(
        xml[start..end]
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&"),
    )
}

/// Parses an STS `AssumeRole*` response.
pub fn parse_sts_response(xml: &str) -> Result<ShortLived, CredentialError> {
    let get = |t: &str| {
        xml_text(xml, t)
            .ok_or_else(|| CredentialError::Malformed(format!("STS response lacks {t}")))
    };
    Ok(ShortLived {
        credential: Arc::new(AwsCredential {
            key_id: get("AccessKeyId")?,
            secret_key: get("SecretAccessKey")?,
            token: Some(get("SessionToken")?),
        }),
        expires_at: Some(parse_expiration(&get("Expiration")?)?),
    })
}

#[async_trait]
impl Fetch for WebIdentitySource {
    async fn fetch(&self) -> Result<ShortLived, CredentialError> {
        let token = tokio::fs::read_to_string(&self.token_file)
            .await
            .map_err(|source| CredentialError::Read {
                path: self.token_file.clone(),
                source,
            })?;
        let resp = self
            .http
            .post(&self.sts_endpoint)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(self.body(token.trim()))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| CredentialError::Sts(e.to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| CredentialError::Sts(e.to_string()))?;
        if !status.is_success() {
            // The error document names the code and message, never the token.
            let code = xml_text(&text, "Code").unwrap_or_default();
            let message = xml_text(&text, "Message").unwrap_or_default();
            return Err(CredentialError::Sts(format!("{status} {code}: {message}")));
        }
        parse_sts_response(&text)
    }
}

#[async_trait]
impl<T: Fetch> Fetch for Arc<T> {
    async fn fetch(&self) -> Result<ShortLived, CredentialError> {
        (**self).fetch().await
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

/// Re-read sources that give no expiry this often, so a rotated file is
/// picked up.
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
        if let Some((cached, at)) = guard.as_ref()
            && Self::fresh(cached, *at, now)
        {
            return Ok(cached.credential.clone());
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
    /// The platform's identity through the standard AWS chain: EKS Pod
    /// Identity and ECS (`AWS_CONTAINER_CREDENTIALS_*`), EC2 instance roles,
    /// IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE`). Static keys in the environment
    /// are refused here, so they are never used by accident.
    Ambient,
    /// Static keys: from a `credential_process` JSON file (re-read, so a
    /// rotated Secret is picked up), or from `AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY` and optionally `AWS_SESSION_TOKEN`.
    Static {
        /// The file; `None` reads the environment.
        file: Option<PathBuf>,
    },
    /// OIDC workload identity: `AssumeRoleWithWebIdentity`.
    WebIdentity(WebIdentityConfig),
}

/// Settings for [`CredentialSource::WebIdentity`].
#[derive(Debug, Clone)]
pub struct WebIdentityConfig {
    /// The role to assume (`AWS_ROLE_ARN`).
    pub role_arn: String,
    /// The OIDC token, re-read on every exchange
    /// (`AWS_WEB_IDENTITY_TOKEN_FILE`).
    pub token_file: PathBuf,
    /// STS endpoint (`AWS_ENDPOINT_URL_STS`); default
    /// `https://sts.<region>.amazonaws.com`.
    pub sts_endpoint: Option<String>,
    /// Narrow the credential to the volume's prefix with a session policy
    /// (AWS; turn off for an STS that rejects `Policy`).
    pub session_policy: bool,
}

/// Fails if the environment carries static keys.
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
                return Err(CredentialError::Sts("down".into()));
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

    /// A fake STS: answers each request with the next canned response and
    /// returns the request bodies it was given.
    async fn fake_sts(
        responses: Vec<(u16, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for (status, body) in responses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 16384];
                let mut req = Vec::new();
                let body_at = loop {
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
                            break h + 4;
                        }
                    }
                };
                bodies.push(String::from_utf8_lossy(&req[body_at..]).into_owned());
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: text/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(reply.as_bytes()).await.unwrap();
            }
            bodies
        });
        (url, task)
    }

    fn sts_ok(key: &str) -> String {
        format!(
            "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
             <AccessKeyId>{key}</AccessKeyId><SecretAccessKey>s&amp;s</SecretAccessKey>\
             <SessionToken>t</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration>\
             </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
        )
    }

    fn form(body: &str) -> std::collections::HashMap<String, String> {
        body.split('&')
            .map(|kv| {
                let (k, v) = kv.split_once('=').unwrap();
                let mut out = Vec::new();
                let b = v.as_bytes();
                let mut i = 0;
                while i < b.len() {
                    if b[i] == b'%' {
                        out.push(u8::from_str_radix(&v[i + 1..i + 3], 16).unwrap());
                        i += 3;
                    } else {
                        out.push(b[i]);
                        i += 1;
                    }
                }
                (k.to_owned(), String::from_utf8(out).unwrap())
            })
            .collect()
    }

    #[tokio::test]
    async fn web_identity_exchanges_the_current_token_with_a_scoped_policy() {
        let (url, task) = fake_sts(vec![
            (200, sts_ok("k1")),
            (
                403,
                "<ErrorResponse><Error><Code>InvalidIdentityToken</Code>\
                 <Message>expired</Message></Error></ErrorResponse>"
                    .into(),
            ),
            (200, sts_ok("k2")),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "jwt-1\n").unwrap();
        let policy = session_policy("b", "t/volumes/00", Access::ReadOnly);
        let src = WebIdentitySource::new(
            &url,
            "arn:aws:iam::1:role/r".into(),
            token.clone(),
            "host/pod 1",
            Some(policy.clone()),
        );
        let c = src.fetch().await.unwrap();
        assert_eq!(c.credential.key_id, "k1");
        assert_eq!(c.credential.secret_key, "s&s");
        assert!(c.expires_at.is_some());
        let err = src.fetch().await.unwrap_err().to_string();
        assert!(err.contains("InvalidIdentityToken: expired"), "{err}");
        assert!(
            !err.contains("jwt"),
            "an error must not echo the token: {err}"
        );
        // The platform rotated the token: the next exchange uses the new one.
        std::fs::write(&token, "jwt-2").unwrap();
        assert_eq!(src.fetch().await.unwrap().credential.key_id, "k2");

        let bodies = task.await.unwrap();
        let first = form(&bodies[0]);
        assert_eq!(first["Action"], "AssumeRoleWithWebIdentity");
        assert_eq!(first["RoleArn"], "arn:aws:iam::1:role/r");
        assert_eq!(first["RoleSessionName"], "host-pod-1");
        assert_eq!(first["WebIdentityToken"], "jwt-1");
        assert_eq!(first["DurationSeconds"], "3600");
        let sent: serde_json::Value = serde_json::from_str(&first["Policy"]).unwrap();
        assert_eq!(sent, policy);
        assert_eq!(form(&bodies[2])["WebIdentityToken"], "jwt-2");
    }

    #[test]
    fn session_policy_scopes_to_the_volume() {
        let ro = session_policy("bkt", "t/volumes/ab", Access::ReadOnly);
        assert_eq!(
            ro["Statement"][0]["Action"],
            serde_json::json!(["s3:GetObject"])
        );
        assert_eq!(
            ro["Statement"][0]["Resource"],
            "arn:aws:s3:::bkt/t/volumes/ab/*"
        );
        assert_eq!(
            ro["Statement"][1]["Condition"]["StringLike"]["s3:prefix"],
            "t/volumes/ab/*"
        );
        let rw = session_policy("bkt", "t/volumes/ab", Access::ReadWrite);
        assert!(
            rw["Statement"][0]["Action"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("s3:PutObject"))
        );
    }

    #[test]
    fn session_names_are_valid() {
        assert_eq!(session_name_from("a"), "a-");
        assert_eq!(
            session_name_from("ip-10-0-0-1/skyfs:42"),
            "ip-10-0-0-1-skyfs-42"
        );
        assert_eq!(session_name_from(&"x".repeat(100)).len(), 64);
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
