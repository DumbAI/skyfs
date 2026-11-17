#!/usr/bin/env bash
# End-to-end smoke test through the kernel (Linux, /dev/fuse, root).
#
#   cargo build -p skyfs && sudo PATH="$PWD/target/debug:$PATH" scripts/fuse-smoke.sh
#
# Creates a volume in a local-directory bucket, exercises it through a FUSE
# mount, unmounts, remounts from an empty cache dir (another machine),
# checks every byte, then crashes a mount with SIGKILL and checks same-disk
# recovery. Needs: fuse3, attr, python3, util-linux.
set -euo pipefail

WORK=$(mktemp -d)
STORE="${STORE:-file://$WORK/bucket}"  # or s3://bucket/prefix (SKYFS_REGION etc. from the environment)
A="$WORK/mnt-a"; B="$WORK/mnt-b"
mkdir -p "$A" "$B"
PID=
cleanup() {
  [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true
  for m in "$A" "$B"; do mountpoint -q "$m" && (fusermount3 -u "$m" 2>/dev/null || umount -l "$m"); done
  rm -rf "$WORK"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; for f in "$WORK"/*.log; do echo "== $f"; tail -20 "$f"; done; exit 1; }

mount_at() { # cache-dir mountpoint log
  skyfs mount --store "$STORE" --volume "$VOL" --cache-dir "$1" --max-lag 200ms "$2" 2>"$3" &
  PID=$!
  for _ in $(seq 100); do mountpoint -q "$2" && return 0; sleep 0.1; done
  fail "mount at $2"
}
unmount() { kill -TERM "$PID"; wait "$PID" || fail "unmount exited $?"; PID=; }

VOL=$(skyfs create --store "$STORE")
echo "volume $VOL"

# --- machine 1: every operation mountpoint-s3 refuses
mount_at "$WORK/m1" "$A" "$WORK/m1.log"
cd "$A"
echo hello > hello.txt
mkdir -p dir/sub && echo nested > dir/sub/n.txt
dd if=/dev/urandom of=big.bin bs=1M count=24 status=none
printf 'PATCH' | dd of=big.bin bs=1 seek=1000000 conv=notrunc status=none
sha256sum big.bin > "$WORK/big.sum"
echo line1 >> log.txt; echo line2 >> log.txt
ln hello.txt hard.txt; ln -s hello.txt soft.txt
mv dir renamed
setfattr -n user.color -v blue hello.txt
chmod 600 log.txt
truncate -s 3 hello.txt
mkfifo fifo
python3 - <<'PY'
import os
fd = os.open("unlinked", os.O_CREAT | os.O_RDWR)
os.write(fd, b"ghost"); os.unlink("unlinked")
os.lseek(fd, 0, 0); assert os.read(fd, 5) == b"ghost"; os.close(fd)
os.rename("renamed/sub/n.txt", "n2.txt")
with open("db.sqlite", "wb") as f: f.write(b"x" * 8192)
with open("db.sqlite", "r+b") as f:
    f.seek(4096); f.write(b"Y" * 8); f.flush(); os.fsync(f.fileno())
PY
cd /
unmount

skyfs fsck --store "$STORE" --volume "$VOL" || fail fsck

# --- machine 2: empty cache, everything from the bucket
mount_at "$WORK/m2" "$B" "$WORK/m2.log"
cd "$B"
[ "$(cat hello.txt)" = hel ] || fail "truncate"
[ "$(cat hard.txt)" = hel ] || fail "hard link"
[ "$(stat -c %h hello.txt)" = 2 ] || fail "nlink"
[ "$(readlink soft.txt)" = hello.txt ] || fail "symlink"
[ "$(cat log.txt)" = "$(printf 'line1\nline2')" ] || fail "append"
[ "$(stat -c %a log.txt)" = 600 ] || fail "chmod"
[ "$(cat n2.txt)" = nested ] || fail "rename file"
[ -d renamed/sub ] || fail "rename dir"
[ -p fifo ] || fail "fifo"
[ ! -e unlinked ] || fail "unlinked file survived"
[ "$(getfattr --only-values -n user.color hello.txt)" = blue ] || fail "xattr"
python3 -c "d=open('db.sqlite','rb').read(); assert d[4096:4104]==b'Y'*8 and len(d)==8192" || fail "in-place edit"
sha256sum -c --quiet "$WORK/big.sum" || fail "big file"

# --- crash: SIGKILL, then recover the local WAL on the same disk
echo after-fsync > synced.txt
python3 -c "import os; fd=os.open('synced.txt', os.O_RDONLY); os.fsync(fd)"
echo unsynced > unsynced.txt
cd /
kill -9 "$PID"; wait "$PID" 2>/dev/null || true; PID=
fusermount3 -u "$B" 2>/dev/null || umount -l "$B"
mount_at "$WORK/m2" "$B" "$WORK/m3.log"
grep -q "recovered=1" "$WORK/m3.log" || fail "no same-disk recovery"
[ "$(cat "$B/synced.txt")" = after-fsync ] || fail "fsynced file"
[ "$(cat "$B/unsynced.txt")" = unsynced ] || fail "recovered file"
unmount
skyfs fsck --store "$STORE" --volume "$VOL" || fail "fsck after recovery"
echo "fuse smoke test passed"
