use async_trait::async_trait;
use chrono::Utc;
use qail_workflow::{
    ChannelKind, ChargeRequest, ChargeResponse, PaymentKind, WorkflowContext, WorkflowCursor,
    WorkflowCursorFrame, WorkflowError, WorkflowExecutor, WorkflowLease, WorkflowOperation,
    WorkflowOperationKind, WorkflowOperationStatus, WorkflowPendingWait, WorkflowSideEffect,
    WorkflowSideEffectKind, WorkflowSideEffectStatus, WorkflowStep,
};
use qail_workflow_postgres::{PgWorkflowExecutor, PgWorkflowStore, PgWorkflowTables};

struct NoopExecutor;

fn assert_error_contains<T: std::fmt::Debug>(result: Result<T, WorkflowError>, expected: &str) {
    let err = result.expect_err("operation should fail");
    assert!(
        err.to_string().contains(expected),
        "expected error to contain {expected:?}, got {err}"
    );
}

#[async_trait]
impl WorkflowExecutor for NoopExecutor {
    async fn execute_query(&self, _cmd_json: &str) -> Result<serde_json::Value, WorkflowError> {
        Err(WorkflowError::Other(
            "live storage test does not execute app queries".to_string(),
        ))
    }

    async fn send_notification(
        &self,
        _channel: &ChannelKind,
        _recipient: &str,
        _template: &str,
        _params: &serde_json::Value,
    ) -> Result<(), WorkflowError> {
        Ok(())
    }

    async fn save_state(&self, _ctx: &WorkflowContext) -> Result<(), WorkflowError> {
        unreachable!("PgWorkflowExecutor must override storage hooks")
    }

    async fn load_state(
        &self,
        _workflow_id: &str,
    ) -> Result<Option<WorkflowContext>, WorkflowError> {
        unreachable!("PgWorkflowExecutor must override storage hooks")
    }

    async fn create_charge(
        &self,
        _provider: &PaymentKind,
        _request: ChargeRequest,
    ) -> Result<ChargeResponse, WorkflowError> {
        Err(WorkflowError::Other(
            "live storage test does not execute charges".to_string(),
        ))
    }
}

#[tokio::test]
async fn live_postgres_storage_runtime_guarantees() {
    let Ok(database_url) = std::env::var("QAIL_WORKFLOW_PG_DATABASE_URL") else {
        eprintln!("skipping live test; QAIL_WORKFLOW_PG_DATABASE_URL is not set");
        return;
    };

    let suffix = Utc::now()
        .timestamp_nanos_opt()
        .expect("timestamp should fit");
    let tables = PgWorkflowTables {
        states: format!("qail_workflow_states_{suffix}"),
        leases: format!("qail_workflow_leases_{suffix}"),
        operations: format!("qail_workflow_operations_{suffix}"),
        side_effects: format!("qail_workflow_side_effects_{suffix}"),
    };
    let store = PgWorkflowStore::connect_url(&database_url)
        .await
        .unwrap()
        .with_tables(tables);
    store.install_schema().await.unwrap();
    let executor = PgWorkflowExecutor::new(NoopExecutor, store);

    let mut ctx = WorkflowContext::new("wf-live", "awaiting_vendor");
    ctx.definition_name = Some("booking_recovery".to_string());
    ctx.set("booking_id", serde_json::json!("booking-1"));
    WorkflowExecutor::save_state(&executor, &ctx).await.unwrap();

    let loaded = WorkflowExecutor::load_state(&executor, "wf-live")
        .await
        .unwrap()
        .expect("saved state should load");
    assert_eq!(loaded.workflow_id, "wf-live");
    assert_eq!(loaded.get_str("booking_id"), Some("booking-1"));

    let lease = WorkflowLease {
        workflow_id: "wf-live".to_string(),
        owner: "worker-a".to_string(),
        ttl: std::time::Duration::from_secs(30),
    };
    assert!(
        WorkflowExecutor::acquire_workflow_lease(&executor, &lease)
            .await
            .unwrap()
    );

    let competing_lease = WorkflowLease {
        workflow_id: "wf-live".to_string(),
        owner: "worker-b".to_string(),
        ttl: std::time::Duration::from_secs(30),
    };
    assert!(
        !WorkflowExecutor::acquire_workflow_lease(&executor, &competing_lease)
            .await
            .unwrap(),
        "unexpired lease must block another owner"
    );
    WorkflowExecutor::release_workflow_lease(&executor, &lease)
        .await
        .unwrap();

    let operation = WorkflowOperation {
        workflow_name: "booking_recovery".to_string(),
        workflow_id: "wf-live".to_string(),
        idempotency_key: "event-1".to_string(),
        kind: WorkflowOperationKind::Resume {
            event: "vendor.accepted".to_string(),
        },
    };
    assert_eq!(
        WorkflowExecutor::begin_workflow_operation(&executor, &operation)
            .await
            .unwrap(),
        WorkflowOperationStatus::Started
    );
    assert_eq!(
        WorkflowExecutor::begin_workflow_operation(&executor, &operation)
            .await
            .unwrap(),
        WorkflowOperationStatus::InProgress
    );
    WorkflowExecutor::complete_workflow_operation(&executor, &operation, "confirmed")
        .await
        .unwrap();
    assert_eq!(
        WorkflowExecutor::begin_workflow_operation(&executor, &operation)
            .await
            .unwrap(),
        WorkflowOperationStatus::Completed {
            state: "confirmed".to_string()
        }
    );
    assert_error_contains(
        WorkflowExecutor::fail_workflow_operation(&executor, &operation, "late failure").await,
        "was not found in started state",
    );
    assert_eq!(
        WorkflowExecutor::begin_workflow_operation(&executor, &operation)
            .await
            .unwrap(),
        WorkflowOperationStatus::Completed {
            state: "confirmed".to_string()
        },
        "late failure must not overwrite a completed operation"
    );

    let mismatched_operation = WorkflowOperation {
        kind: WorkflowOperationKind::Timeout,
        ..operation.clone()
    };
    assert_error_contains(
        WorkflowExecutor::begin_workflow_operation(&executor, &mismatched_operation).await,
        "previously used for kind",
    );

    let missing_operation = WorkflowOperation {
        idempotency_key: "missing-event".to_string(),
        ..operation.clone()
    };
    assert_error_contains(
        WorkflowExecutor::complete_workflow_operation(&executor, &missing_operation, "confirmed")
            .await,
        "was not found in started state",
    );

    let side_effect = WorkflowSideEffect {
        workflow_id: "wf-live".to_string(),
        state: "awaiting_vendor".to_string(),
        step_path: "steps[1]".to_string(),
        kind: WorkflowSideEffectKind::Notify,
        operation_id: "notify-live-1".to_string(),
    };
    assert_eq!(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &side_effect)
            .await
            .unwrap(),
        WorkflowSideEffectStatus::Execute
    );
    WorkflowExecutor::complete_workflow_side_effect(
        &executor,
        &side_effect,
        Some(&serde_json::json!({"provider_id": "msg-1"})),
    )
    .await
    .unwrap();
    assert_eq!(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &side_effect)
            .await
            .unwrap(),
        WorkflowSideEffectStatus::AlreadyCompleted {
            result: Some(serde_json::json!({"provider_id": "msg-1"}))
        }
    );
    assert_error_contains(
        WorkflowExecutor::complete_workflow_side_effect(&executor, &side_effect, None).await,
        "was not found in started state",
    );
    assert_eq!(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &side_effect)
            .await
            .unwrap(),
        WorkflowSideEffectStatus::AlreadyCompleted {
            result: Some(serde_json::json!({"provider_id": "msg-1"}))
        },
        "late side-effect completion must not overwrite the stored result"
    );

    let missing_side_effect = WorkflowSideEffect {
        operation_id: "missing-notify-live".to_string(),
        ..side_effect.clone()
    };
    assert_error_contains(
        WorkflowExecutor::complete_workflow_side_effect(&executor, &missing_side_effect, None)
            .await,
        "was not found in started state",
    );

    let mut timed_out = WorkflowContext::new("wf-timeout-live", "awaiting_vendor");
    timed_out.definition_name = Some("booking_recovery".to_string());
    timed_out.set_cursor(WorkflowCursor {
        state: "awaiting_vendor".to_string(),
        frames: vec![WorkflowCursorFrame::Steps { index: 1 }],
        wait: Some(WorkflowPendingWait {
            event: "vendor.accepted".to_string(),
            deadline_at: Utc::now() - chrono::Duration::minutes(1),
            on_timeout: vec![WorkflowStep::transition("expired")],
        }),
    });
    WorkflowExecutor::save_state(&executor, &timed_out)
        .await
        .unwrap();

    let due =
        WorkflowExecutor::load_due_workflow_timeouts(&executor, "booking_recovery", Utc::now(), 10)
            .await
            .unwrap();
    assert_eq!(due, vec!["wf-timeout-live".to_string()]);

    let claimed_again =
        WorkflowExecutor::load_due_workflow_timeouts(&executor, "booking_recovery", Utc::now(), 10)
            .await
            .unwrap();
    assert!(
        claimed_again.is_empty(),
        "claimed timeout row must not be rediscovered until claim TTL expires"
    );

    let zero_ttl_executor = PgWorkflowExecutor::new(
        NoopExecutor,
        executor
            .store()
            .clone()
            .with_timeout_claim_ttl(std::time::Duration::ZERO),
    );
    assert_error_contains(
        WorkflowExecutor::load_due_workflow_timeouts(
            &zero_ttl_executor,
            "booking_recovery",
            Utc::now(),
            10,
        )
        .await,
        "claim TTL",
    );
}
