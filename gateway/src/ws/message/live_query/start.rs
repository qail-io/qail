use super::bootstrap::subscribe_and_spawn_live_query;
use super::validate::prepare_and_send_initial_snapshot;
use super::{LiveQueryRequest, LiveQueryRuntime};

pub(super) async fn handle_live_query(
    request: LiveQueryRequest,
    runtime: &mut LiveQueryRuntime<'_>,
) {
    tracing::info!(
        "User {} starting live query on table: {}",
        runtime.auth.user_id,
        request.table
    );

    let Some(cmd) = prepare_and_send_initial_snapshot(
        &request.qail,
        &request.table,
        runtime.state,
        runtime.tx,
        runtime.auth,
    )
    .await
    else {
        return;
    };

    subscribe_and_spawn_live_query(&request.table, request.interval_ms, cmd, runtime).await;
}
