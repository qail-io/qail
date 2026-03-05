use super::TransactionSessionManager;
use std::sync::Arc;

/// Spawn the background reaper task that cleans up expired transaction sessions.
pub fn spawn_reaper(manager: Arc<TransactionSessionManager>) {
    let interval_secs = std::cmp::max(manager.timeout_secs / 2, 5);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            manager.reap_expired().await;
        }
    });
}
