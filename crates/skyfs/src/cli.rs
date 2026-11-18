//! Command-line interface.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use skyfs_store::{CredentialSource, StoreConfig, WebIdentityConfig};

/// skyfs: a POSIX filesystem whose source of truth is a log in blob storage.
#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create a new volume in the bucket.
    Create {
        #[command(flatten)]
        store: StoreArgs,
        /// Volume id (32 hex digits); random if omitted.
        #[arg(long)]
        volume: Option<String>,
        /// Largest write stored inline in a log segment.
        #[arg(long, default_value_t = 1 << 20)]
        inline_data_max: u32,
        /// Root directory owner uid. Default: root on Linux, where the
        /// daemon runs as root for containers; on macOS, the user running
        /// `create`, who also runs the mount.
        #[arg(long)]
        root_uid: Option<u32>,
        /// Root directory group gid (default as for `--root-uid`).
        #[arg(long)]
        root_gid: Option<u32>,
    },
    /// Mount a volume with FUSE (Linux) and serve until unmounted or signalled.
    Mount(MountArgs),
    /// Replay a volume from the bucket and check every invariant (offline).
    Fsck {
        #[command(flatten)]
        store: StoreArgs,
        /// Volume id.
        #[arg(long, env = "SKYFS_VOLUME")]
        volume: String,
    },
    /// Publish a quiescent volume as a new sealed, read-only volume
    /// (design §13): its own objects are copied server-side, then sealed.
    /// Prints the new volume's id.
    Publish {
        #[command(flatten)]
        store: StoreArgs,
        /// The volume to publish (cleanly unmounted).
        #[arg(long, env = "SKYFS_VOLUME")]
        volume: String,
        /// Id of the published volume (32 hex digits); random if omitted.
        #[arg(long)]
        to: Option<String>,
        /// Tenant prefix of the published volume within the bucket (default:
        /// the store URL's prefix).
        #[arg(long)]
        to_prefix: Option<String>,
        /// Leave out this path (relative to the volume root) and everything
        /// below it. Repeatable.
        #[arg(long)]
        exclude: Vec<String>,
    },
    /// Create a writable volume as a clone of a sealed volume: metadata
    /// only, the data stays in the sealed volume (design §13). Prints the
    /// new volume's id.
    Clone {
        #[command(flatten)]
        store: StoreArgs,
        /// The sealed volume to clone.
        #[arg(long)]
        base: String,
        /// Tenant prefix of the sealed volume within the bucket (default:
        /// the store URL's prefix).
        #[arg(long)]
        base_prefix: Option<String>,
        /// Id of the new volume (32 hex digits); random if omitted.
        #[arg(long, env = "SKYFS_VOLUME")]
        volume: Option<String>,
    },
    /// Show the restore pointer and log head of a volume.
    Info {
        #[command(flatten)]
        store: StoreArgs,
        /// Volume id.
        #[arg(long, env = "SKYFS_VOLUME")]
        volume: String,
    },
}

// Command-line flags are naturally booleans.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Args)]
pub struct MountArgs {
    #[command(flatten)]
    pub store: StoreArgs,
    /// Volume id.
    #[arg(long, env = "SKYFS_VOLUME")]
    pub volume: String,
    /// Local directory for metadata, the local WAL and the cache.
    #[arg(long, env = "SKYFS_CACHE_DIR")]
    pub cache_dir: PathBuf,
    /// Where to mount.
    pub mountpoint: PathBuf,
    /// How the kernel reaches the volume: FUSE (Linux, the production path)
    /// or `NFSv3` served on localhost (macOS, where it is the default; also
    /// works on Linux).
    #[arg(long, value_enum, env = "SKYFS_TRANSPORT", default_value_t = Transport::default_for_os())]
    pub transport: Transport,
    /// When namespace operations are acknowledged.
    #[arg(long, value_enum, default_value_t = ReplicationArg::Async)]
    pub replication: ReplicationArg,
    /// Where fsync waits in async mode. Default: `remote` with FUSE;
    /// `local` with NFS, whose client also commits at every close.
    #[arg(long, value_enum)]
    pub fsync: Option<FsyncArg>,
    /// Async mode: upload a segment at most this long after its first op.
    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    pub max_lag: Duration,
    /// Keep restoring at unmount for at most this long.
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    pub restore_on_exit_timeout: Duration,
    /// Keep snapshots this long for point-in-time restore.
    #[arg(long, default_value = "7d", value_parser = parse_duration)]
    pub retention: Duration,
    /// Local cache size limit, in GiB.
    #[arg(long, default_value_t = 20)]
    pub cache_gib: u64,
    /// Allow other users to access the mount (needs `user_allow_other`
    /// unless root).
    #[arg(long)]
    pub allow_other: bool,
    /// FUSE worker threads (NFS: server threads).
    #[arg(long, default_value_t = 8)]
    pub threads: usize,
    /// Source shown in /proc/mounts (`mount.fuse.skyfs` passes the volume id).
    #[arg(long, default_value = "skyfs")]
    pub fsname: String,
    /// Filesystem subtype: the mount's type is `fuse.<subtype>`, or plain
    /// `fuse` when empty.
    #[arg(long, default_value = "skyfs")]
    pub subtype: String,
    /// How reads update access times.
    #[arg(long, value_enum, default_value_t = AtimeArg::Relatime)]
    pub atime: AtimeArg,
    /// Serve the volume as of mount time, read-only: no lease, no writes,
    /// read-only credentials.
    #[arg(long)]
    pub read_only: bool,
    /// Honour device nodes on the volume (`dev`; default `nodev`).
    #[arg(long)]
    pub dev: bool,
    /// Honour set-uid/set-gid bits on the volume (`suid`; default `nosuid`).
    #[arg(long)]
    pub suid: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Transport {
    /// FUSE through `/dev/fuse` (Linux).
    Fuse,
    /// `NFSv3` served by the daemon on 127.0.0.1, mounted with the system's
    /// NFS client (no kernel extension or install on macOS).
    Nfs,
}

impl Transport {
    /// FUSE on Linux, NFS elsewhere.
    #[must_use]
    pub fn default_for_os() -> Self {
        if cfg!(target_os = "linux") {
            Self::Fuse
        } else {
            Self::Nfs
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AtimeArg {
    /// Update atime when it is older than mtime or ctime, or a day old.
    Relatime,
    /// Never update atime on access.
    Noatime,
    /// Update atime on every open for reading, read and listing.
    Strictatime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReplicationArg {
    Sync,
    Async,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FsyncArg {
    Remote,
    Local,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CredentialKind {
    /// The platform identity: EKS Pod Identity, ECS, instance roles, IRSA.
    Ambient,
    /// Static keys from `--credentials-file` or `AWS_ACCESS_KEY_ID` (R2,
    /// stores without identity federation).
    Static,
    /// OIDC workload identity: exchange a token (Kubernetes projected
    /// service-account token, SPIFFE JWT) at STS, `AssumeRoleWithWebIdentity`.
    Oidc,
}

/// Where the bucket is and how to authenticate (design §10.1).
#[derive(Debug, Clone, Args)]
pub struct StoreArgs {
    /// `s3://bucket/prefix`, `file:///dir` or `memory://`.
    #[arg(long, env = "SKYFS_STORE")]
    pub store: String,
    /// S3-compatible endpoint, e.g. `https://<account>.r2.cloudflarestorage.com`.
    #[arg(long, env = "SKYFS_ENDPOINT")]
    pub endpoint: Option<String>,
    /// Region (R2: `auto`).
    #[arg(long, env = "SKYFS_REGION")]
    pub region: Option<String>,
    /// Allow plain-HTTP endpoints (local test servers only).
    #[arg(long)]
    pub allow_http: bool,
    /// Longest one bucket request may take. The default suits uplinks down
    /// to about 2 Mbit/s; raise it for slower links.
    #[arg(long, env = "SKYFS_REQUEST_TIMEOUT", default_value = "5m", value_parser = parse_duration)]
    pub request_timeout: Duration,
    /// Credential source.
    #[arg(long, value_enum, env = "SKYFS_CREDENTIALS", default_value_t = CredentialKind::Ambient)]
    pub credentials: CredentialKind,
    /// `static`: AWS credential-process JSON file, re-read so a rotated
    /// Secret is picked up (default: the `AWS_*` environment variables).
    #[arg(long, env = "SKYFS_CREDENTIALS_FILE")]
    pub credentials_file: Option<PathBuf>,
    /// `oidc`: the role to assume.
    #[arg(long, env = "AWS_ROLE_ARN")]
    pub role_arn: Option<String>,
    /// `oidc`: file holding the OIDC token, re-read on every exchange.
    #[arg(long, env = "AWS_WEB_IDENTITY_TOKEN_FILE")]
    pub web_identity_token_file: Option<PathBuf>,
    /// `oidc`: STS endpoint (default `https://sts.<region>.amazonaws.com`;
    /// MinIO, Ceph RGW and Backblaze B2 have their own).
    #[arg(long, env = "AWS_ENDPOINT_URL_STS")]
    pub sts_endpoint: Option<String>,
    /// `oidc`: do not narrow the credential to the volume with a session
    /// policy (for an STS that rejects `Policy`).
    #[arg(long)]
    pub no_session_policy: bool,
}

impl StoreArgs {
    /// Publish and clone read one volume and write another; a session
    /// policy narrows a credential to one volume, so they need the role's
    /// own scope.
    pub fn check_two_volumes(&self) -> Result<(), String> {
        if matches!(self.credentials, CredentialKind::Oidc) && !self.no_session_policy {
            return Err(
                "publish and clone use two volumes: with --credentials oidc, pass \
                        --no-session-policy and scope the role to both"
                    .into(),
            );
        }
        Ok(())
    }

    pub fn config(&self) -> Result<StoreConfig, String> {
        let credentials = match self.credentials {
            CredentialKind::Ambient => CredentialSource::Ambient,
            CredentialKind::Static => CredentialSource::Static {
                file: self.credentials_file.clone(),
            },
            CredentialKind::Oidc => CredentialSource::WebIdentity(WebIdentityConfig {
                role_arn: self
                    .role_arn
                    .clone()
                    .ok_or("--credentials oidc needs --role-arn (AWS_ROLE_ARN)")?,
                token_file: self.web_identity_token_file.clone().ok_or(
                    "--credentials oidc needs --web-identity-token-file \
                     (AWS_WEB_IDENTITY_TOKEN_FILE)",
                )?,
                sts_endpoint: self.sts_endpoint.clone(),
                session_policy: !self.no_session_policy,
            }),
        };
        Ok(StoreConfig {
            url: self.store.clone(),
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            allow_http: self.allow_http,
            credentials,
            request_timeout: self.request_timeout,
        })
    }
}

/// The owner of a new volume's root directory when `--root-uid` /
/// `--root-gid` are not given: root on Linux, the current user on macOS.
#[must_use]
pub fn default_root_owner() -> (u32, u32) {
    if cfg!(target_os = "macos") {
        (
            rustix::process::geteuid().as_raw(),
            rustix::process::getegid().as_raw(),
        )
    } else {
        (0, 0)
    }
}

/// Parses `250ms`, `30s`, `5m`, `2h`, `7d`.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().map_err(|_| format!("bad duration {s:?}"))?;
    Ok(match unit {
        "ms" => Duration::from_millis(n),
        "s" | "" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        "d" => Duration::from_secs(n * 86_400),
        _ => return Err(format!("bad duration unit in {s:?}")),
    })
}

/// Parses a 32-hex-digit volume id.
pub fn parse_volume_id(s: &str) -> Result<[u8; 16], String> {
    let s = s.trim();
    if s.len() != 32 {
        return Err(format!("volume id must be 32 hex digits, got {s:?}"));
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16)
            .map_err(|_| format!("bad volume id {s:?}"))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_hours(168));
        assert!(parse_duration("1y").is_err());
    }

    #[test]
    fn volume_ids() {
        let id = parse_volume_id("000102030405060708090a0b0c0d0e0f").unwrap();
        assert_eq!(id[15], 15);
        assert!(parse_volume_id("xyz").is_err());
    }

    #[test]
    fn root_owner_defaults_to_root_on_linux_and_the_user_on_macos() {
        let (uid, gid) = default_root_owner();
        if cfg!(target_os = "macos") {
            assert_eq!(uid, rustix::process::geteuid().as_raw());
            assert_eq!(gid, rustix::process::getegid().as_raw());
        } else {
            assert_eq!((uid, gid), (0, 0));
        }
    }

    #[test]
    fn cli_parses() {
        Cli::command_for_test();
    }

    impl Cli {
        fn command_for_test() {
            use clap::CommandFactory;
            Self::command().debug_assert();
        }
    }
}
