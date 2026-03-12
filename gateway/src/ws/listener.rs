use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::GatewayState;

use super::{
    ListenControl, ListenerNotification, WS_LISTENER_CMD_TIMEOUT_MS, WS_LISTENER_RETRY_MS,
    WS_LISTENER_UNAVAILABLE_NOTICE_MS, WS_MAX_SUBSCRIPTIONS_PER_CONNECTION,
    WS_NOTIFY_DROP_NOTICE_MS, WsServerMessage,
};

async fn release_listener_conn(conn: &mut Option<qail_pg::PooledConnection>) {
    if let Some(mut c) = conn.take() {
        match c.unlisten_all().await {
            Ok(()) => {
                c.release().await;
            }
            Err(e) => {
                // Fail closed: LISTEN state is session-level. If cleanup fails,
                // dropping the pooled wrapper destroys the backend connection
                // instead of returning a potentially subscribed socket to pool.
                tracing::warn!("WS listener cleanup UNLISTEN failed, dropping conn: {}", e);
            }
        }
    }
}

async fn ensure_listener_conn(
    state: &Arc<GatewayState>,
    conn: &mut Option<qail_pg::PooledConnection>,
    channels: &std::collections::HashSet<String>,
) -> Result<(), String> {
    if conn.is_some() || channels.is_empty() {
        return Ok(());
    }

    let mut c = state
        .acquire_system_guarded("ws_listener", None)
        .await
        .map_err(|_| "Database connection unavailable".to_string())?;

    for channel in channels {
        if let Err(e) = c.listen(channel).await {
            if let Err(cleanup_err) = c.unlisten_all().await {
                tracing::debug!(
                    "WS listener partial LISTEN cleanup failed before release: {}",
                    cleanup_err
                );
                return Err(format!("LISTEN failed: {}", e));
            }
            c.release().await;
            return Err(format!("LISTEN failed: {}", e));
        }
    }
    *conn = Some(c);
    Ok(())
}

pub(super) async fn listener_rpc(
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    make_cmd: impl FnOnce(oneshot::Sender<Result<(), String>>) -> ListenControl,
) -> Result<(), String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    listener_tx
        .send(make_cmd(reply_tx))
        .map_err(|_| "Notification worker unavailable".to_string())?;

    match tokio::time::timeout(
        std::time::Duration::from_millis(WS_LISTENER_CMD_TIMEOUT_MS),
        reply_rx,
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("Notification worker unavailable".to_string()),
        Err(_) => Err("Notification worker timeout".to_string()),
    }
}

pub(super) async fn run_listener_session(
    state: Arc<GatewayState>,
    tx: mpsc::Sender<WsServerMessage>,
    notify_tx: mpsc::Sender<ListenerNotification>,
    mut control_rx: mpsc::UnboundedReceiver<ListenControl>,
) {
    let mut conn: Option<qail_pg::PooledConnection> = None;
    let mut channels: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut last_unavailable_notice: Option<std::time::Instant> = None;
    let mut last_notify_drop_notice: Option<std::time::Instant> = None;

    loop {
        if conn.is_none()
            && !channels.is_empty()
            && let Err(e) = ensure_listener_conn(&state, &mut conn, &channels).await
        {
            tracing::warn!("WS listener reconnect failed: {}", e);
            let should_notify = last_unavailable_notice.is_none_or(|t| {
                t.elapsed() >= std::time::Duration::from_millis(WS_LISTENER_UNAVAILABLE_NOTICE_MS)
            });
            if should_notify {
                let _ = tx.try_send(WsServerMessage::Error {
                    message: "Notification channel unavailable".to_string(),
                });
                last_unavailable_notice = Some(std::time::Instant::now());
            }
            tokio::select! {
                cmd = control_rx.recv() => {
                    match cmd {
                        Some(ListenControl::Listen { channel, reply }) => {
                            let _ = channel; // keep command shape explicit for future diagnostics.
                            // Connection is currently unavailable; don't claim success.
                            let _ = reply.send(Err("Notification channel unavailable".to_string()));
                        }
                        Some(ListenControl::Unlisten { channel, reply }) => {
                            channels.remove(&channel);
                            let _ = reply.send(Ok(()));
                        }
                        Some(ListenControl::Shutdown) | None => break,
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(WS_LISTENER_RETRY_MS)) => {}
            }
            continue;
        }
        if conn.is_some() {
            last_unavailable_notice = None;
        }

        if let Some(c) = conn.as_mut() {
            tokio::select! {
                cmd = control_rx.recv() => {
                    match cmd {
                        Some(ListenControl::Listen { channel, reply }) => {
                            if channels.contains(&channel) {
                                let _ = reply.send(Ok(()));
                                continue;
                            }
                            if channels.len() >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION {
                                let _ = reply.send(Err(format!(
                                    "Too many subscriptions (max {})",
                                    WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
                                )));
                                continue;
                            }
                            channels.insert(channel.clone());
                            if let Err(e) = c.listen(&channel).await {
                                channels.remove(&channel);
                                tracing::warn!("WS listener LISTEN failed: {}", e);
                                let _ = reply.send(Err("Subscribe failed".to_string()));
                                release_listener_conn(&mut conn).await;
                            } else if reply.send(Ok(())).is_err() {
                                // Caller timed out/cancelled; roll back LISTEN to avoid
                                // ghost subscriptions not tracked by connection state.
                                channels.remove(&channel);
                                if let Err(e) = c.unlisten(&channel).await {
                                    tracing::warn!(
                                        "WS listener rollback UNLISTEN failed after dropped reply: {}",
                                        e
                                    );
                                    let _ = c.unlisten_all().await;
                                }
                            }
                        }
                        Some(ListenControl::Unlisten { channel, reply }) => {
                            channels.remove(&channel);
                            if let Err(e) = c.unlisten(&channel).await {
                                tracing::warn!("WS listener UNLISTEN failed: {}", e);
                                let _ = reply.send(Err("Unsubscribe failed".to_string()));
                                release_listener_conn(&mut conn).await;
                            } else {
                                let _ = reply.send(Ok(()));
                            }
                            if channels.is_empty() {
                                release_listener_conn(&mut conn).await;
                            }
                        }
                        Some(ListenControl::Shutdown) | None => break,
                    }
                }
                result = c.recv_notification() => {
                    match result {
                        Ok(notification) => {
                            let channel = notification.channel;
                            if channels.contains(&channel) {
                                match notify_tx.try_send(ListenerNotification {
                                    channel: channel.clone(),
                                    payload: notification.payload,
                                }) {
                                    Ok(()) => {}
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        let should_log = last_notify_drop_notice.is_none_or(|t| {
                                            t.elapsed() >= std::time::Duration::from_millis(WS_NOTIFY_DROP_NOTICE_MS)
                                        });
                                        if should_log {
                                            tracing::warn!(
                                                channel = %channel,
                                                "Dropping WS notification: internal queue is full"
                                            );
                                            last_notify_drop_notice = Some(std::time::Instant::now());
                                        }
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("WS listener recv_notification failed: {}", e);
                            release_listener_conn(&mut conn).await;
                            if channels.is_empty() {
                                tokio::time::sleep(std::time::Duration::from_millis(WS_LISTENER_RETRY_MS)).await;
                            }
                        }
                    }
                }
            }
        } else {
            match control_rx.recv().await {
                Some(ListenControl::Listen { channel, reply }) => {
                    if channels.contains(&channel) {
                        let _ = reply.send(Ok(()));
                        continue;
                    }
                    if channels.len() >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION {
                        let _ = reply.send(Err(format!(
                            "Too many subscriptions (max {})",
                            WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
                        )));
                        continue;
                    }
                    channels.insert(channel.clone());
                    match ensure_listener_conn(&state, &mut conn, &channels).await {
                        Ok(()) => {
                            if reply.send(Ok(())).is_err() {
                                // Caller timed out/cancelled; roll back state and backend LISTEN.
                                channels.remove(&channel);
                                if let Some(c) = conn.as_mut()
                                    && let Err(e) = c.unlisten(&channel).await
                                {
                                    tracing::warn!(
                                        "WS listener rollback UNLISTEN failed after dropped reply: {}",
                                        e
                                    );
                                    let _ = c.unlisten_all().await;
                                }
                            }
                        }
                        Err(_) => {
                            channels.remove(&channel);
                            let _ = reply.send(Err("Subscribe failed".to_string()));
                        }
                    }
                }
                Some(ListenControl::Unlisten { channel, reply }) => {
                    channels.remove(&channel);
                    let _ = reply.send(Ok(()));
                }
                Some(ListenControl::Shutdown) | None => break,
            }
        }
    }

    release_listener_conn(&mut conn).await;
}
