#!/usr/bin/env bash
# End-to-end smoke test of the NFS transport through the kernel's NFS client.
# macOS: runs as your user, nothing to install. Linux: root and nfs-common.
#
#   cargo build -p skyfs && PATH="$PWD/target/debug:$PATH" scripts/nfs-smoke.sh
#
# Same story as fuse-smoke.sh: exercise a mount, unmount, remount from an
# empty cache dir (another machine) and check every byte, then SIGKILL the
# daemon and check same-disk recovery. NFSv3 has no xattrs and skyfs's NFS
# server makes no device nodes or FIFOs, so those are not exercised.
set -euo pipefail

WORK=$(mktemp -d)
STORE="${STORE:-file://$WORK/bucket}"
A="$WORK/mnt-a"; B="$WORK/mnt-b"
mkdir -p "$A" "$B"
PID=
mounted() { python3 -c "import os,sys; d=sys.argv[1]; sys.exit(0 if os.stat(d).st_dev != os.stat(os.path.join(d, '..')).st_dev else 1)" "$1"; }
cleanup() {
  [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true
  for m in "$A" "$B"; do mounted "$m" 2>/dev/null && umount -f "$m" 2>/dev/null; done
  rm -rf "$WORK"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; for f in "$WORK"/*.log; do echo "== $f"; tail -20 "$f"; done; exit 1; }

mount_at() { # cache-dir mountpoint log
  skyfs mount --transport nfs --store "$STORE" --volume "$VOL" --cache-dir "$1" --max-lag 200ms "$2" 2>"$3" &
  PID=$!
  for _ in $(seq 100); do mounted "$2" && return 0; kill -0 "$PID" 2>/dev/null || break; sleep 0.1; done
  fail "mount at $2"
}
unmount() { kill -TERM "$PID"; wait "$PID" || fail "unmount exited $?"; PID=; }

VOL=$(skyfs create --store "$STORE")
echo "volume $VOL"

# --- machine 1
mount_at "$WORK/m1" "$A" "$WORK/m1.log"
cd "$A"
echo hello > hello.txt
mkdir -p dir/sub && echo nested > dir/sub/n.txt
dd if=/dev/urandom of=big.bin bs=1048576 count=24 2>/dev/null
printf 'PATCH' | dd of=big.bin bs=1 seek=1000000 conv=notrunc 2>/dev/null
python3 -c "import hashlib; print(hashlib.sha256(open('big.bin','rb').read()).hexdigest())" > "$WORK/big.sum"
echo line1 >> log.txt; echo line2 >> log.txt
ln hello.txt hard.txt; ln -s hello.txt soft.txt
mv dir renamed
chmod 600 log.txt
python3 - <<'PY'
import os
os.truncate("hello.txt", 3)
# Open, unlink, keep using: the client renames it aside until close.
fd = os.open("unlinked", os.O_CREAT | os.O_RDWR)
os.write(fd, b"ghost"); os.unlink("unlinked")
os.lseek(fd, 0, 0); assert os.read(fd, 5) == b"ghost"; os.close(fd)
os.rename("renamed/sub/n.txt", "n2.txt")
with open("db.sqlite", "wb") as f: f.write(b"x" * 8192)
with open("db.sqlite", "r+b") as f:
    f.seek(4096); f.write(b"Y" * 8); f.flush(); os.fsync(f.fileno())
for i in range(200):
    with open(f"renamed/many{i}", "w") as f: f.write(str(i))
PY
cd /
unmount
[ -z "$(ls -A "$A")" ] || fail "mountpoint not empty after unmount"

skyfs fsck --store "$STORE" --volume "$VOL" || fail fsck

# --- machine 2: empty cache, everything from the bucket
mount_at "$WORK/m2" "$B" "$WORK/m2.log"
cd "$B"
python3 - "$WORK/big.sum" <<'PY' || fail "remote state"
import hashlib, os, stat, sys
assert open("hello.txt").read() == "hel", "truncate"
assert open("hard.txt").read() == "hel", "hard link"
assert os.stat("hello.txt").st_nlink == 2, "nlink"
assert os.readlink("soft.txt") == "hello.txt", "symlink"
assert open("log.txt").read() == "line1\nline2\n", "append"
assert stat.S_IMODE(os.stat("log.txt").st_mode) == 0o600, "chmod"
assert open("n2.txt").read() == "nested\n", "rename file"
assert os.path.isdir("renamed/sub"), "rename dir"
assert not any(n.startswith((".nfs", "unlinked")) for n in os.listdir(".")), os.listdir(".")
d = open("db.sqlite", "rb").read(); assert d[4096:4104] == b"Y" * 8 and len(d) == 8192, "in-place edit"
assert len([n for n in os.listdir("renamed") if n.startswith("many")]) == 200, "readdir"
want = open(sys.argv[1]).read().strip()
assert hashlib.sha256(open("big.bin", "rb").read()).hexdigest() == want, "big file"
PY

# --- crash: SIGKILL, then recover the local WAL on the same disk
echo after-fsync > synced.txt
python3 -c "import os; fd=os.open('synced.txt', os.O_RDONLY); os.fsync(fd)"
echo unsynced > unsynced.txt
python3 -c "import os; fd=os.open('unsynced.txt', os.O_RDONLY); os.fsync(fd)"  # the close COMMITs anyway
cd /
kill -9 "$PID"; wait "$PID" 2>/dev/null || true; PID=
umount -f "$B" 2>/dev/null || true
mount_at "$WORK/m2" "$B" "$WORK/m3.log"
grep -q "recovered=1" "$WORK/m3.log" || fail "no same-disk recovery"
[ "$(cat "$B/synced.txt")" = after-fsync ] || fail "fsynced file"
[ "$(cat "$B/unsynced.txt")" = unsynced ] || fail "recovered file"
unmount
skyfs fsck --store "$STORE" --volume "$VOL" || fail "fsck after recovery"
echo "nfs smoke test passed"
