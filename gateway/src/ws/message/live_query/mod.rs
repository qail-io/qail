mod bootstrap;
mod poller;
mod start;
mod stop;
mod validate;

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::GatewayState;
use crate::auth::AuthContext;

use super::super::{ListenControl, WsConnectionState, WsServerMessage};

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
    start::handle_live_query(
        qail,
        table,
        interval_ms,
        state,
        tx,
        listener_tx,
        auth,
        conn_state,
    )
    .await;
}

pub(super) async fn handle_stop_live_query(
    table: String,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &AuthContext,
    conn_state: &mut WsConnectionState,
) {
    stop::handle_stop_live_query(table, tx, listener_tx, auth, conn_state).await;
}
