# skyfs

A POSIX filesystem whose durable copy lives in blob storage (Cloudflare R2,
S3), mounted with FUSE by one writer at a time, on any cloud, and on a Mac
over the system's own NFS client.

- **Mounts in seconds, whatever the volume size.** Only metadata is
  downloaded; every file is listed and `stat`-able at once.
- **Reads on demand.** File contents are fetched by range on first read,
  verified and cached on local disk.
- **Every change is a log record.** Operations are appended to an immutable
  write-ahead log of segments in blob storage, replicated synchronously or
  asynchronously (`--replication sync|async`).
- **Everything mountpoint-s3 refuses works.** Random and partial writes,
  in-place edits, appends, truncate, file and directory renames, hard links,
  symlinks, chmod/chown, xattrs. Each is a small log record, never a
  rewrite of existing objects. Replaying the log from the bucket alone
  restores the exact state, at the latest op or any retained earlier one.
- **Blob storage holds a snapshot plus a short log.** The mounted container
  folds the log into a new snapshot in the background and on shutdown; the
  snapshot's manifest is the restore pointer. Mount replays only minutes of
  log.
- **Crash-consistent.** After any crash the volume is the result of a prefix
  of the operations, never a mixture.

- **Standalone.** A daemon or sidecar that needs only the bucket: no
  control plane. It authenticates with OIDC workload identity (a
  Kubernetes service-account token exchanged at STS for an hour-long
  credential narrowed to its volume, no stored secret) or, for stores
  without federation such as R2, static keys from a mounted secret.

Status: v1 implemented and tested end to end through FUSE on Linux (the
production path) and through the NFS transport on macOS and Linux;
passes the full pjdfstest suite (8,798 tests) and the generic xfstests
`quick` group (650 tests; 12 exclusions, each justified in
`scripts/xfstests.exclude`); fault
injection at every bucket mutation, every point of recovery and every
torn byte of the local WAL. Open items are in
[design §12](docs/design.md#12-build-order-and-what-settles-the-open-questions).

## Use

```sh
# Create a volume (prints its id).
skyfs create --store s3://bucket/tenant --region us-east-2

# Mount it (Linux, /dev/fuse). SIGTERM unmounts cleanly.
skyfs mount --store s3://bucket/tenant --volume <id> --cache-dir /var/cache/skyfs \
  --replication async --max-lag 1s /mnt/data

skyfs info --store ... --volume <id>   # restore pointer and log head
skyfs fsck --store ... --volume <id>   # replay from the bucket and check invariants

# Or through mount(8) / fstab, with scripts/mount.fuse.skyfs as /sbin/mount.fuse.skyfs:
mount -t fuse.skyfs <id> /mnt/data -o store=s3://bucket/tenant,credentials=oidc,...
```

### macOS

```sh
skyfs mount --store s3://bucket/tenant --volume <id> --cache-dir ~/.cache/skyfs ~/skyfs
```

On macOS `skyfs mount` serves the volume over NFSv3 on `127.0.0.1` and
mounts it with the system's `mount_nfs`: as your user, with no kernel
extension, macFUSE or anything else to install (`--transport nfs`, the
default there). `umount ~/skyfs` or Ctrl-C unmounts. On macOS
`skyfs create` makes the volume's root directory yours (on Linux, root's;
`--root-uid`/`--root-gid` override). It is a single-user
transport: no device nodes or FIFOs, xattrs live in hidden `._` files,
and `--fsync` defaults to
`local` because the NFS client commits at every close
([design §10.2](docs/design.md#102-macos-the-nfs-transport); how-to:
[docs/macos.md](docs/macos.md)).

Access times follow `--atime relatime` (default), `noatime` or
`strictatime`. `--read-only` (`-o ro`) serves the volume as of mount time
and writes nothing. Device nodes and set-id bits are ignored unless the mount
passes `--dev` / `--suid`.

`--store` also takes `file:///dir` (a local bucket, for testing). Every flag
has an environment form (`SKYFS_STORE`, `SKYFS_VOLUME`, `SKYFS_CREDENTIALS`,
...).

### Credentials

`--credentials` ([design §10.1](docs/design.md#101-authentication-and-credentials)):

| Source | Use it for |
|---|---|
| `oidc` | Kubernetes anywhere, with S3, MinIO, Ceph RGW or Backblaze B2. Exchanges the pod's projected service-account token at STS (`AssumeRoleWithWebIdentity`) for a credential valid for an hour and narrowed to the volume. No stored secret. `--role-arn`, `--web-identity-token-file`, `--sts-endpoint` default to `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ENDPOINT_URL_STS` (set by EKS IRSA) |
| `ambient` (default) | EKS Pod Identity, ECS task roles, EC2 instance roles. Refuses static keys in the environment |
| `static` | Stores without identity federation (Cloudflare R2). `--credentials-file` (AWS `credential_process` JSON, re-read so a rotated Secret is picked up) or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. Scope the key to one bucket |

OIDC on Kubernetes (any cluster whose service-account issuer is registered
as an IAM OIDC provider; on EKS it already is):

```yaml
# The role's trust policy allows sts:AssumeRoleWithWebIdentity for
# sub = system:serviceaccount:<ns>:skyfs-<volume>, aud = sts.amazonaws.com.
containers:
  - name: skyfs
    args: [mount, --store, s3://bucket/tenant, --volume, <id>, --credentials, oidc,
           --cache-dir, /var/cache/skyfs, /mnt/data]
    env:
      - {name: AWS_ROLE_ARN, value: arn:aws:iam::<account>:role/skyfs-<volume>}
      - {name: AWS_WEB_IDENTITY_TOKEN_FILE, value: /var/run/secrets/skyfs/token}
      - {name: SKYFS_REGION, value: us-east-2}
    volumeMounts: [{name: token, mountPath: /var/run/secrets/skyfs, readOnly: true}]
volumes:
  - name: token
    projected:
      sources:
        - serviceAccountToken: {audience: sts.amazonaws.com, expirationSeconds: 3600, path: token}
```

Static keys from a Secret (R2):

```sh
kubectl create secret generic skyfs-r2 --from-literal=credentials.json='{"Version":1,"AccessKeyId":"...","SecretAccessKey":"..."}'
skyfs mount --store s3://bucket/tenant --endpoint https://<account>.r2.cloudflarestorage.com --region auto \
  --credentials static --credentials-file /etc/skyfs/credentials.json ...
```

One mount per volume is the deployment's job (a StatefulSet of one, or
`Recreate`); if two mount anyway, the newer fences the older through the
log, and the volume never forks.

In a container: `--device /dev/fuse --cap-add SYS_ADMIN`, or run skyfs on
the host or in a privileged sidecar and bind-propagate the mount so the app
container holds no credentials at all.

## Documents

| | |
|---|---|
| [docs/design.md](docs/design.md) | Requirements, review of the previous design, alternatives, architecture, credentials, guarantees, build order |
| [docs/wal-format.md](docs/wal-format.md) | The log format, normative |
| [docs/macos.md](docs/macos.md) | Mounting on macOS: setup, S3 credentials, limits, troubleshooting |
| [docs/testing.md](docs/testing.md) | Test layers, failure injection, kernel and real-bucket suites, coverage and gaps |
| [proto/skyfs/wal/v1/wal.proto](proto/skyfs/wal/v1/wal.proto) | The log schema, source of truth |
| [proto/skyfs/snapshot/v1/snapshot.proto](proto/skyfs/snapshot/v1/snapshot.proto) | The snapshot manifest (restore pointer) |
| [docs/workspace-storage.md](docs/workspace-storage.md) | Background: object store comparison and the read-only predecessor design |

## Layout

| Crate | Role |
|---|---|
| `crates/skyfs-format` | Log records and snapshot manifest (generated from `proto/`), segment encoding. No I/O. |
| `crates/skyfs-meta` | The POSIX state machine: validation (`plan`), `apply`, the SQLite store that is also the snapshot format, fsck |
| `crates/skyfs-store` | Object keys (the only place that spells one), the object client, credentials, the local chunk cache |
| `crates/skyfs-volume` | The engine: local WAL, replicator, blobs, read path, mount and recovery, restorer and GC. Platform-independent |
| `crates/skyfs` | CLI, the FUSE adapter (Linux) and the NFS adapter (macOS) |
| `vendor/nessie-nfsserve` | Vendored NFSv3 server library, with a fix (`PATCHES.md`) |

Dependencies point one way:
`skyfs → skyfs-volume → skyfs-meta, skyfs-store → skyfs-format`.

## Develop

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```

Everything builds and tests on macOS and Linux except the FUSE adapter
(Linux only); the engine's end-to-end tests run against in-memory buckets.
On a Mac, `cargo build -p skyfs && PATH="$PWD/target/debug:$PATH"
scripts/nfs-smoke.sh` mounts through the kernel with no root. To check the
Linux build and run the kernel smoke test from macOS:

```sh
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined \
  -v "$PWD":/src -w /src -e CARGO_TARGET_DIR=/tmp/t rust:1.96 bash -c \
  'apt-get update -qq && apt-get install -y -qq fuse3 attr python3 >/dev/null &&
   rustup component add clippy && cargo clippy --workspace --all-targets -- -D warnings &&
   cargo build -p skyfs && PATH=/tmp/t/debug:$PATH scripts/fuse-smoke.sh'
```

Conformance through the kernel (Linux, root): `scripts/fuse-smoke.sh`,
`scripts/pjdfstest.sh` and `scripts/xfstests.sh`, as in CI;
`scripts/nfs-smoke.sh` for the NFS transport (macOS as your user, Linux as
root); against a real
bucket, `scripts/s3-e2e.sh`. What each suite proves:
[docs/testing.md](docs/testing.md).

No `protoc` is needed: `build.rs` compiles the proto with `protox`.
