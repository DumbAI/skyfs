#!/usr/bin/env bash
# End-to-end scenarios against a real bucket (S3 or S3-compatible), through
# the kernel. Complements fuse-smoke.sh, which checks POSIX behaviour on a
# local bucket; this checks what only a real bucket shows: latency, upload
# throughput, cold reads, fencing over the network, recovery from the bucket
# alone. See docs/testing.md.
#
#   cargo build --release -p skyfs
#   sudo env PATH="$PWD/target/release:$PATH" STORE=s3://bucket/prefix SKYFS_REGION=us-east-2 \
#     SKYFS_CREDENTIALS=static SKYFS_CREDENTIALS_FILE=/run/skyfs/creds.json scripts/s3-e2e.sh
#
# Credentials come from the usual skyfs settings (SKYFS_CREDENTIALS, ...).
# BIG_MIB sizes the large file (default 64; keep it small on slow uplinks).
# Leaves the volume in the bucket under STORE; delete the prefix afterwards.
set -euo pipefail

: "${STORE:?STORE=s3://bucket/prefix is required}"
BIG_MIB=${BIG_MIB:-64}
W=$(mktemp -d); A=$W/a; B=$W/b; R=$W/r; mkdir -p "$A" "$B" "$R"
PIDS=()
cleanup() {
  for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null || true; done
  for m in "$A" "$B" "$R"; do if mountpoint -q "$m"; then umount -l "$m" || true; fi; done
}
trap cleanup EXIT
fail() { echo "FAIL: $*"; for f in "$W"/*.log; do echo "== $f"; tail -15 "$f"; done; exit 1; }
now() { date +%s.%N; }
el() { python3 -c "print(f'{$(now)-$1:.2f}s')"; }
mount_at() { # cache mountpoint log [flags...]
  local c=$1 m=$2 l=$3 t; shift 3; t=$(now)
  skyfs mount --store "$STORE" --volume "$VOL" --cache-dir "$c" "$@" "$m" 2>"$l" & LAST=$!; PIDS+=("$LAST")
  for _ in $(seq 600); do
    mountpoint -q "$m" && { echo "  mounted $m in $(el "$t")"; return 0; }
    kill -0 "$LAST" 2>/dev/null || return 1; sleep 0.05
  done
  return 1
}
unmount() { local t; t=$(now); kill -TERM "$1"; wait "$1" || fail "unmount exited $?"; echo "  unmounted in $(el "$t")"; }
sum() { sha256sum "$1" | cut -d' ' -f1; }

VOL=$(skyfs create --store "$STORE"); echo "volume $VOL"

echo "== 1. writer (async, max-lag 1s): throughput and many small files"
mount_at "$W/c1" "$A" "$W/m1.log" || fail "mount"; P1=$LAST
t=$(now); dd if=/dev/urandom of="$A/big" bs=1M count="$BIG_MIB" status=none; sync "$A/big"
echo "  $BIG_MIB MiB write+fsync: $(el "$t")"
sum "$A/big" > "$W/big.sum"
t=$(now); mkdir "$A/many"; for i in $(seq 2000); do echo "$i" > "$A/many/f$i"; done; echo "  2000 small files: $(el "$t")"
t=$(now); ls -l "$A/many" >/dev/null; echo "  ls -l 2000: $(el "$t")"

echo "== 2. read-only mount beside the writer"
sleep 2  # past max-lag, so the segments are in the bucket
mount_at "$W/c3" "$R" "$W/m3.log" --read-only || fail "read-only mount"; P3=$LAST
[ "$(cat "$R/many/f1999")" = 1999 ] || fail "read-only content"
if touch "$R/x" 2>/dev/null; then fail "read-only mount accepted a write"; fi
echo "  write refused: EROFS"
t=$(now); [ "$(sum "$R/big")" = "$(cat "$W/big.sum")" ] || fail "read-only big file"
echo "  cold read $BIG_MIB MiB from the bucket: $(el "$t")"
unmount "$P3"

echo "== 3. a second writer fences the first (in band, no lease service)"
mount_at "$W/c2" "$B" "$W/m2.log" || fail "second writer"; P2=$LAST
echo from-b > "$B/b.txt"
echo stale > "$A/stale.txt" 2>/dev/null || true
sleep 3  # A's next segment hits the seq B claimed: 412
echo more > "$A/more.txt" 2>/dev/null && sleep 2 || true
grep -q fenced "$W/m1.log" || fail "old writer not fenced"
echo "  old writer: $(grep -o 'fenced[^"]\{0,40\}' "$W/m1.log" | head -1)"
if echo x > "$A/after" 2>/dev/null; then fail "fenced writer still accepts writes"; fi
echo "  fenced writer refuses writes (EIO)"
sleep 2; [ ! -e "$B/stale.txt" ] || fail "the fenced writer's write landed"
echo "  the fenced writer's last write is absent"
[ "$(sum "$B/big")" = "$(cat "$W/big.sum")" ] || fail "big file via the new writer"
kill -9 "$P1"; wait "$P1" 2>/dev/null || true; umount -l "$A"

echo "== 4. crash with the local disk lost: recover from the bucket alone"
echo durable > "$B/durable"; python3 -c "import os; os.fsync(os.open('$B/durable', os.O_RDONLY))"
for i in $(seq 50); do echo "$i" > "$B/tail$i"; done  # async: maybe not uploaded yet
kill -9 "$P2"; wait "$P2" 2>/dev/null || true
umount -l "$B" 2>/dev/null || true; rm -rf "$W/c2"   # the machine is gone
mount_at "$W/c4" "$A" "$W/m4.log" || fail "recovery mount"; P4=$LAST
[ "$(cat "$A/durable")" = durable ] || fail "fsynced file lost"
n=$(find "$A" -maxdepth 1 -name 'tail*' | wc -l)
for i in $(seq "$n"); do [ "$(cat "$A/tail$i")" = "$i" ] || fail "tail$i: not a prefix"; done
echo "  fsynced file present; $n/50 unsynced files survived, a prefix"
[ "$(sum "$A/big")" = "$(cat "$W/big.sum")" ] || fail "big file after the crash"
[ "$(find "$A/many" -type f | wc -l)" = 2000 ] || fail "small files after the crash"
unmount "$P4"

echo "== 5. offline fsck and info from the bucket"
skyfs fsck --store "$STORE" --volume "$VOL" || fail fsck
skyfs info --store "$STORE" --volume "$VOL"

echo "== 6. warm remount (cache kept)"
mount_at "$W/c4" "$A" "$W/m5.log" || fail "remount"; unmount "$LAST"
echo "s3 e2e passed"
