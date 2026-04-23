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

pub(super) struct LiveQueryRequest {
    pub qail: String,
    pub table: String,
    pub interval_ms: u64,
}

pub(super) struct LiveQueryRuntime<'a> {
    pub state: &'a Arc<GatewayState>,
    pub tx: &'a mpsc::Sender<WsServerMessage>,
    pub listener_tx: &'a mpsc::UnboundedSender<ListenControl>,
    pub auth: &'a AuthContext,
    pub conn_state: &'a mut WsConnectionState,
}

pub(super) async fn handle_live_query(
    request: LiveQueryRequest,
    runtime: &mut LiveQueryRuntime<'_>,
) {
    start::handle_live_query(request, runtime).await;
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
