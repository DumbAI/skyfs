//! The replicator (design §6.4): seals segments on deadline, size or a
//! durability request, and commits them to the bucket strictly in seq
//! order, one PUT in flight at a time. While segment n is being PUT, new
//! records go into n+1; callers waiting on n+1 are answered together when
//! it lands (group commit).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use skyfs_store::{Created, Key};

use crate::data::segment_data_id;
use crate::restorer::{CommittedSegment, RestoreMsg};
use crate::wal::Sealed;
use crate::writer::{Backoff, Shared};

/// How a segment's commit ended.
enum Outcome {
    Committed,
    Fenced,
    Aborted,
}

/// Runs until the volume is fenced or `stop` resolves.
pub(crate) async fn run(shared: Arc<Shared>, mut stop: tokio::sync::watch::Receiver<bool>) {
    loop {
        let job = {
            let mut st = shared.state.lock();
            if st.sealed.is_empty() {
                let force = shared.sync_requested.swap(false, Ordering::SeqCst);
                if st.open_due(shared.cfg.max_lag, force) {
                    let _ = shared.seal(&mut st);
                }
            }
            st.sealed.front().cloned()
        };
        if let Some(seg) = job {
            match commit(&shared, &seg).await {
                Outcome::Committed => continue,
                Outcome::Fenced => {
                    tracing::error!(
                        seq = seg.seq(),
                        "another writer committed this seq; this mount is fenced"
                    );
                    shared.commit.send_modify(|c| c.fenced = true);
                    return;
                }
                Outcome::Aborted => return,
            }
        }
        if *stop.borrow() {
            return;
        }

        // Idle: keep the local WAL durable at the group-commit cadence, and
        // sleep until the replication deadline, a kick, or the next tick.
        let handles = shared.state.lock().open_sync_handles();
        if let Some((dat, rec)) = handles {
            let _ = tokio::task::spawn_blocking(move || {
                let _ = dat.sync_data();
                let _ = rec.sync_data();
            })
            .await;
        }
        let deadline = shared.state.lock().open_deadline(shared.cfg.max_lag);
        let tick = Instant::now() + shared.cfg.group_commit;
        let wake = deadline.map_or(tick, |d| d.min(tick).max(Instant::now()));
        tokio::select! {
            () = shared.kick.notified() => {}
            () = tokio::time::sleep_until(wake.into()) => {}
            _ = stop.changed() => {}
        }
    }
}

async fn commit(shared: &Arc<Shared>, seg: &Arc<Sealed>) -> Outcome {
    // Data before reference: every blob this segment points at is uploaded.
    for dep in &seg.blob_deps {
        let rx = shared.uploads.lock().get(dep).cloned();
        if let Some(mut rx) = rx
            && rx.wait_for(|done| *done).await.is_err()
        {
            return Outcome::Aborted;
        }
    }
    let seg2 = seg.clone();
    let bytes = match tokio::task::spawn_blocking(move || seg2.object_bytes()).await {
        Ok(Ok(b)) => Bytes::from(b),
        Ok(Err(e)) => {
            tracing::error!(error = %e, seq = seg.seq(), "cannot read a sealed segment's local files");
            return Outcome::Aborted;
        }
        Err(_) => return Outcome::Aborted,
    };
    let key = Key::Segment(seg.seq());
    let mut backoff = Backoff::new();
    loop {
        match shared.client.put_new(&key, bytes.clone()).await {
            Ok(Created::New) => break,
            // Our own earlier attempt may have landed with its response
            // lost; anything else at this seq means another writer won.
            Ok(Created::AlreadyExists) => match shared.client.get(&key).await {
                Ok(Some(existing)) if existing == bytes => break,
                Ok(_) => return Outcome::Fenced,
                Err(e) => {
                    tracing::warn!(error = %e, "checking an existing segment failed; retrying");
                    backoff.wait().await;
                }
            },
            Err(e) => {
                if shared.abort.load(Ordering::Relaxed) {
                    return Outcome::Aborted;
                }
                tracing::warn!(error = %e, seq = seg.seq(), "segment upload failed; retrying");
                backoff.wait().await;
            }
        }
    }

    // Committed. Its local data becomes ordinary cache.
    shared.data.set_data_offset(seg.seq(), seg.data_offset());
    seg.retire();
    if let Err(e) = shared
        .cache
        .adopt(&segment_data_id(seg.seq()), &seg.data_path())
    {
        tracing::warn!(error = %e, "could not move committed segment data into the cache");
    }
    {
        let mut st = shared.state.lock();
        let front = st.sealed.pop_front();
        debug_assert!(front.is_some_and(|f| f.seq() == seg.seq()));
    }
    {
        let mut uploads = shared.uploads.lock();
        for dep in &seg.blob_deps {
            uploads.remove(dep);
        }
    }
    shared.commit.send_modify(|c| c.committed = Some(seg.seq()));
    let _ = shared
        .restore_tx
        .send(RestoreMsg::Segment(CommittedSegment::from_sealed(seg)));
    Outcome::Committed
}
