# Mounting skyfs on macOS

On a Mac, skyfs mounts with the NFS client macOS already has. The `skyfs`
daemon serves the volume over NFSv3 on `127.0.0.1`, and `mount_nfs` mounts
it. Everything runs as your user: no root, kernel extension, macFUSE or
other install. Linux with FUSE is the production path; macOS is for
working with a volume on a laptop. Design: [design §10.2](design.md#102-macos-the-nfs-transport).

```
app ──► macOS NFS client ──NFSv3 over 127.0.0.1──► skyfs daemon ──► S3 / R2
```

## Build

Rust 1.96 (pinned in `rust-toolchain.toml`):

```sh
cargo build --release -p skyfs
cp target/release/skyfs ~/bin/      # or anywhere on your PATH
```

## Try it with a local bucket

No cloud account needed: `file://` is a bucket in a local directory.

```sh
skyfs create --store file://$HOME/skyfs-bucket          # prints the volume id
skyfs mount  --store file://$HOME/skyfs-bucket --volume <id> \
             --cache-dir ~/.cache/skyfs ~/skyfs           # Ctrl-C unmounts
```

`skyfs mount` stays in the foreground until you unmount: Ctrl-C or
`kill -TERM <pid>` from it, or `umount ~/skyfs` from another terminal.
Either way the daemon drains, uploads and snapshots what is pending before
it exits. On macOS, `--transport nfs` is the default and needs no flag.

## Mount a volume in S3

```sh
export SKYFS_STORE=s3://<bucket>/<prefix>
export SKYFS_REGION=us-east-2
skyfs create                                   # once; prints the volume id
skyfs mount --volume <id> --cache-dir ~/.cache/skyfs/<id> ~/skyfs
```

Credentials come from `--credentials` ([README](../README.md#credentials)).
On a laptop with an AWS SSO profile, export its short-lived session
credentials to a file only you can read, and refresh it while the mount
runs. skyfs re-reads the file, so rotated credentials are picked up
without remounting:

```sh
export SKYFS_CREDENTIALS=static SKYFS_CREDENTIALS_FILE=~/.skyfs/creds.json
refresh() {
  (umask 077; aws configure export-credentials --profile <profile> --format process \
    > ~/.skyfs/creds.json.tmp && mv ~/.skyfs/creds.json.tmp ~/.skyfs/creds.json)
}
mkdir -p ~/.skyfs && chmod 700 ~/.skyfs && refresh
skyfs mount --volume <id> --cache-dir ~/.cache/skyfs/<id> ~/skyfs &
while kill -0 $! 2>/dev/null; do sleep 600; refresh; done &   # every 10 minutes
```

When the SSO login expires, `refresh` fails; run
`aws sso login --profile <profile>` and the next refresh picks it up.
Until the last credentials expire, nothing is noticed. After that, writes
keep going to local disk and upload once credentials work again.
Cloudflare R2 has no SSO: put an R2 API token, scoped to the bucket, in
the same file format, add `--endpoint https://<account>.r2.cloudflarestorage.com
--region auto`, and skip the refresh loop.

## Using it

It is an ordinary folder: Finder, the terminal, editors, `git`.

- **Durability.** On macOS's NFS client every `close`, as well as every
  `fsync`, is a durability point. With `--fsync local` (the default for this
  transport) both mean "on this Mac's disk". Every change also reaches the
  bucket within `--max-lag` (1 s). A clean unmount uploads everything.
  `--fsync remote` makes each one wait for the bucket instead, so every
  close becomes a network round trip.
- **Another machine.** A mount with an empty `--cache-dir` (another Mac, or
  after `rm -rf` of the cache) restores the volume from the bucket: listings
  at once, file contents on first read.
- **One writer.** Mount a volume in one place at a time. A second mount
  fences the first, whose writes then fail with `EIO`; the volume never
  forks.
- **Ownership.** `skyfs create` makes the root directory yours on macOS
  (`--root-uid`/`--root-gid` to choose).

## Limits

| What | Why |
|---|---|
| `._name` files next to some files | NFSv3 has no extended attributes; macOS keeps them (quarantine, tags, provenance) in these AppleDouble files. Harmless. |
| No device nodes or FIFOs (`mkfifo`) | The NFS server makes none. |
| Permissions are not enforced between users | A single-user transport: files are created as the calling user, and any local process can reach the port. |
| A volume mounts in one place at a time | skyfs is single-writer. |

## Troubleshooting

| Symptom | Fix |
|---|---|
| The folder hangs after the daemon died | macOS removes the mount after 60 s (`deadtimeout`); or `umount -f ~/skyfs` at once. Then mount again on the same `--cache-dir`: whatever had not reached the bucket is recovered from the local log. |
| `mount: ... failed` at start | The mount point must exist, be yours and be empty; another daemon may hold the same `--cache-dir` (it waits up to 2 minutes, then fails). |
| `skyfs mount` exits with a credentials error | Check the file (`--credentials-file`) or the `AWS_*` variables; for SSO, `aws sso login`. |
| Writes fail with `EIO` | Another mount of the same volume took over (fencing); unmount this one. |
| Slow uploads on a slow link | Large files upload in 8 MiB parts; below about 2 Mbit/s raise `--request-timeout`. |

Logs go to stderr; `RUST_LOG=skyfs=debug` shows more.

## Check it

`scripts/nfs-smoke.sh` runs the end-to-end test as your user, through the
kernel, against a local bucket. It covers every operation, a remount on an
empty cache, and recovery after `kill -9`:

```sh
cargo build -p skyfs && PATH="$PWD/target/debug:$PATH" scripts/nfs-smoke.sh
```
