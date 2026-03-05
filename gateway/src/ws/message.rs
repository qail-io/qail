use std::sync::Arc;

use tokio::sync::mpsc;

use crate::GatewayState;
use crate::auth::ensure_tenant_rate_limit;

use super::{ListenControl, WsClientMessage, WsConnectionState, WsServerMessage};

mod live_query;
mod query;
mod subscription;

pub(super) async fn handle_client_message(
    msg: WsClientMessage,
    state: &Arc<GatewayState>,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &crate::auth::AuthContext,
    conn_state: &mut WsConnectionState,
) {
    if let Err(e) = ensure_tenant_rate_limit(state.as_ref(), auth).await {
        let _ = tx
            .send(WsServerMessage::Error {
                message: e.message.clone(),
            })
            .await;
        return;
    }

    match msg {
        WsClientMessage::Subscribe { channel } => {
            subscription::handle_subscribe(channel, tx, listener_tx, auth, conn_state).await;
        }
        WsClientMessage::Unsubscribe { channel } => {
            subscription::handle_unsubscribe(channel, tx, listener_tx, auth, conn_state).await;
        }
        WsClientMessage::Query { qail } => {
            query::handle_query(qail, state, tx, auth).await;
        }
        WsClientMessage::Ping => {
            let _ = tx.send(WsServerMessage::Pong).await;
        }
        WsClientMessage::LiveQuery {
            qail,
            table,
            interval_ms,
        } => {
            live_query::handle_live_query(
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
        WsClientMessage::StopLiveQuery { table } => {
            live_query::handle_stop_live_query(table, tx, listener_tx, auth, conn_state).await;
        }
    }
}
