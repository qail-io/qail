use std::sync::Arc;

use tokio::sync::mpsc;

use crate::GatewayState;
use crate::auth::AuthContext;

use super::super::super::{ListenControl, WsConnectionState, WsServerMessage};
use super::bootstrap::subscribe_and_spawn_live_query;
use super::validate::prepare_and_send_initial_snapshot;

pub(super) async fn handle_live_query(
    qail: String,
    table: String,
    interval_ms: u64,
    state: &Arc<GatewayState>,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &AuthContext,
    conn_state: &mut WsConnectionState,
) {
    tracing::info!(
        "User {} starting live query on table: {}",
        auth.user_id,
        table
    );

    let Some(cmd) = prepare_and_send_initial_snapshot(&qail, &table, state, tx, auth).await else {
        return;
    };

    subscribe_and_spawn_live_query(
        &table,
        interval_ms,
        cmd,
        state,
        tx,
        listener_tx,
        auth,
        conn_state,
    )
    .await;
}
