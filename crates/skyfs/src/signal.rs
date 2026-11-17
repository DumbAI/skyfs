//! Signals and the clean unmount every transport ends with.

use std::sync::Arc;

use skyfs_volume::Volume;

/// Resolves at the next SIGINT or SIGTERM.
pub async fn signalled() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    let terminated = async {
        match term.as_mut() {
            Some(t) => {
                t.recv().await;
            }
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminated => {}
    }
}

/// Unmounts the volume cleanly once the transport has let go of it (drain,
/// `MountEnd`, restore within the budget). The unmount waits for the
/// bucket; a further signal abandons it, keeping the local WAL for the next
/// mount on this disk.
pub fn unmount_volume(vol: Arc<Volume>) -> Result<(), String> {
    let vol = Arc::into_inner(vol).ok_or("volume still referenced after unmount")?;
    let abort = vol.abort_handle();
    std::thread::spawn(move || {
        if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            rt.block_on(signalled());
            tracing::warn!(
                "signalled again: abandoning the unmount; the local WAL is kept for the next mount on this disk"
            );
            abort.abort();
        }
    });
    let report = vol.unmount().map_err(|e| e.to_string())?;
    tracing::info!(
        head = report.head_seq,
        fully_restored = report.fully_restored,
        "clean unmount"
    );
    Ok(())
}

/// Logs what a mount found.
pub fn log_mounted(vol: &Volume) {
    let r = vol.report();
    tracing::info!(
        epoch = r.epoch,
        snapshot = r.snapshot_gen,
        tail = r.tail_segments,
        recovered = r.recovered_segments,
        lost = r.lost_segments,
        previous_clean = r.previous_clean,
        "volume mounted"
    );
}
