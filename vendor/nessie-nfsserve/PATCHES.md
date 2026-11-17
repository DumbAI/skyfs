# Vendored nessie-nfsserve

Source: crates.io `nessie-nfsserve` 0.4.4
(<https://github.com/Gilamonster-Foundation/nessie-store>), itself a fork of
Hugging Face's `nfsserve`. License: BSD-3-Clause (`LICENSE-UPSTREAM-BSD3`)
AND (MIT OR Apache-2.0), unchanged.

skyfs uses it as the NFSv3 wire layer of the macOS transport
(`crates/skyfs/src/nfs.rs`), wired in by `[patch.crates-io]` in the root
`Cargo.toml`. Drop the vendored copy once upstream carries the fix below.

## Patches

1. **READDIR ignores the cookie** (`src/nfs_handlers.rs`, `src/vfs.rs`).
   `nfsproc3_readdir` called `readdir_simple(dirid, count)`, which restarts
   at the first entry on every call. A directory longer than one READDIR
   page was listed forever by a client that pages with READDIR (Linux does,
   after its first READDIRPLUS). It now calls `readdir(dirid, cookie, n)`.
   Found by `scripts/nfs-smoke.sh` on Linux (a 201-entry directory).
   Reported upstream:
   <https://github.com/Gilamonster-Foundation/nessie-store/issues/171>,
   fix proposed in
   <https://github.com/Gilamonster-Foundation/nessie-store/pull/172>.

2. **Duplicate-request cache scanned on every request**
   (`src/transaction_tracker.rs`). `is_retransmission` swept the whole map
   of recent transactions (retained 60 s) on every RPC, under the lock that
   serializes all RPCs. A busy client keeps about 180,000 entries, so each
   RPC scanned them all, and the server slowed as a workload went on
   (90 µs per RPC at the start of `uv pip install`, 360 µs later). Now swept
   at most once per second. Found profiling `uv pip install` over the macOS
   transport. Hugging Face's `nfsserve` has the same code.
