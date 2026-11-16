#!/usr/bin/env bash
# Runs pjdfstest (the POSIX conformance bar, design §9) against a skyfs mount.
#
#   cargo build -p skyfs && sudo PATH="$PWD/target/debug:$PATH" scripts/pjdfstest.sh
#
# Needs: fuse3, git, autoconf, automake, gcc, make, perl (prove), root.
set -euo pipefail

WORK=$(mktemp -d)
MNT="$WORK/mnt"
PID=
cleanup() {
  [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true
  mountpoint -q "$MNT" && (fusermount3 -u "$MNT" 2>/dev/null || umount -l "$MNT")
  rm -rf "$WORK"
}
trap cleanup EXIT

PJD="${PJDFSTEST_DIR:-$WORK/pjdfstest}"
if [ ! -x "$PJD/pjdfstest" ]; then
  git clone -q https://github.com/pjd/pjdfstest.git "$PJD"
  (cd "$PJD" && autoreconf -ifs >/dev/null && ./configure -q >/dev/null && make -s pjdfstest >/dev/null)
fi

STORE="file://$WORK/bucket"
VOL=$(skyfs create --store "$STORE")
mkdir -p "$MNT"
skyfs mount --store "$STORE" --volume "$VOL" --cache-dir "$WORK/cache" --allow-other "$MNT" 2>"$WORK/mount.log" &
PID=$!
for _ in $(seq 100); do mountpoint -q "$MNT" && break; sleep 0.1; done
mountpoint -q "$MNT" || { cat "$WORK/mount.log"; exit 1; }

status=0
(cd "$MNT" && prove -r "$PJD/tests") || status=$?
kill -TERM "$PID"; wait "$PID" || status=$?
PID=
skyfs fsck --store "$STORE" --volume "$VOL" || status=$?
exit $status
