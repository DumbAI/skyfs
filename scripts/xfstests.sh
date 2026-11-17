#!/usr/bin/env bash
# Runs the generic xfstests (fstests) against skyfs through the kernel.
#
#   cargo build --release -p skyfs && sudo PATH="$PWD/target/release:$PATH" scripts/xfstests.sh [check args]
#
# Default: `-g quick`, minus scripts/xfstests.exclude (each entry says why).
# Needs root, fuse3, and the xfstests build and run dependencies (see the
# xfstests job in .github/workflows/ci.yml). XFSTESTS_DIR reuses a built
# tree; XFSTESTS_RESULTS keeps the results directory.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
WORK=$(mktemp -d)
cleanup() {
  for m in /mnt/skyfs-test /mnt/skyfs-scratch; do
    mountpoint -q "$m" && umount "$m" 2>/dev/null || true
  done
  pkill -f 'skyfs mount' 2>/dev/null || true
}
trap cleanup EXIT

XFS="${XFSTESTS_DIR:-$WORK/xfstests}"
if [ ! -x "$XFS/check" ] || [ ! -e "$XFS/ltp/fsstress" ]; then
  [ -d "$XFS/.git" ] || git clone -q --depth 1 https://github.com/kdave/xfstests.git "$XFS"
  make -C "$XFS" -j"$(nproc)" >"$WORK/build.log" 2>&1 || { tail -40 "$WORK/build.log"; exit 1; }
fi

# The helper mount(8) runs for `-t fuse.skyfs`.
install -m 0755 "$HERE/mount.fuse.skyfs" /sbin/mount.fuse.skyfs
export SKYFS_BIN=$(command -v skyfs)
for u in fsgqa fsgqa2 123456-fsgqa; do id "$u" >/dev/null 2>&1 || useradd -m "$u"; done
getent group fsgqa >/dev/null || groupadd fsgqa

STORE="file://$WORK/bucket"
mkdir -p "$WORK/bucket" /mnt/skyfs-test /mnt/skyfs-scratch
TEST_VOL=$(skyfs create --store "$STORE")
SCRATCH_VOL=$(skyfs create --store "$STORE")
# Plain `fuse` type (no subtype): xfstests compares the mounted type with
# FSTYP. Tests remount constantly, each mount living seconds: give the
# shutdown restore enough time to keep the log tail short (an unmount with
# little to restore returns at once). `dev` for mknod tests; not `suid`:
# some kernels then fail the capability xattr lookup on exec for FUSE.
# Disk: the bucket, both caches and the results share one disk. With the
# default 7-day retention nothing superseded is ever deleted, so every byte
# the whole group writes stays (twice: log and repacked data). Test volumes
# need no point-in-time restore: `retention=0s` lets GC delete what each
# restore pass supersedes, and a small cache bounds the local side.
OPTS="store=$STORE,subtype=,allow_other,dev,max_lag=200ms,restore_on_exit_timeout=20s,retention=0s,cache_gib=1"
cat >"$XFS/local.config" <<CFG
export FSTYP=fuse
export FUSE_SUBTYP=.skyfs
export TEST_DEV=$TEST_VOL
export TEST_DIR=/mnt/skyfs-test
export SCRATCH_DEV=$SCRATCH_VOL
export SCRATCH_MNT=/mnt/skyfs-scratch
export TEST_FS_MOUNT_OPTS="-o $OPTS,cache_dir=$WORK/cache-test"
export MOUNT_OPTIONS="-o $OPTS,cache_dir=$WORK/cache-scratch"
export RESULT_BASE=${XFSTESTS_RESULTS:-$WORK/results}
CFG

# Disk watchdog. A full disk takes the CI runner down with it (no log, no
# results); stop the run instead, saying where the space went.
MIN_FREE_MB=${XFSTESTS_MIN_FREE_MB:-2048}
usage() {
  df -h "$WORK" | tail -1
  du -sh "$WORK"/bucket "$WORK"/cache-* "${XFSTESTS_RESULTS:-$WORK/results}" 2>/dev/null || true
}
watchdog() { # pid
  local low=""
  while kill -0 "$1" 2>/dev/null; do
    local free; free=$(df -Pm "$WORK" | awk 'NR==2 {print $4}')
    [ -z "$low" ] || [ "$free" -lt "$low" ] && low=$free
    if [ "$free" -lt "$MIN_FREE_MB" ]; then
      echo "xfstests.sh: only ${free} MiB free on $(df -P "$WORK" | awk 'NR==2 {print $6}'); stopping the run" >&2
      usage >&2
      pkill -TERM -P "$1" 2>/dev/null; kill -TERM "$1" 2>/dev/null
      return
    fi
    sleep 10
  done
  echo "xfstests.sh: lowest free space during the run: ${low} MiB" >&2
}

status=0
[ $# -gt 0 ] || set -- -g quick
echo "disk before: $(df -h "$WORK" | tail -1)"
(cd "$XFS" && exec ./check -E "$HERE/xfstests.exclude" "$@") &
CHECK=$!
watchdog "$CHECK" &
wait "$CHECK" || status=$?
wait
echo "disk after:"; usage
cleanup
for v in "$TEST_VOL" "$SCRATCH_VOL"; do
  skyfs fsck --store "$STORE" --volume "$v" || status=$?
done
exit $status
