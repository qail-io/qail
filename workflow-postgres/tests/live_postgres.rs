use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use qail_core::ast::Qail;
use qail_pg::PgDriver;
use qail_workflow::{
    ChannelKind, ChargeRequest, ChargeResponse, PaymentKind, WorkflowContext, WorkflowCursor,
    WorkflowCursorFrame, WorkflowDefinition, WorkflowError, WorkflowExecutor, WorkflowLease,
    WorkflowOperation, WorkflowOperationKind, WorkflowOperationStatus, WorkflowPendingWait,
    WorkflowRunOptions, WorkflowSideEffect, WorkflowSideEffectKind, WorkflowSideEffectStatus,
    WorkflowStep, timeout_due_workflows,
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

fn live_timestamp(ts: chrono::DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

async fn set_operation_updated_at(
    database_url: &str,
    tables: &PgWorkflowTables,
    workflow_id: &str,
    idempotency_key: &str,
    updated_at: &str,
) {
    let mut driver = PgDriver::connect_url(database_url).await.unwrap();
    let cmd = Qail::set(&tables.operations)
        .set_value("updated_at", updated_at.to_string())
        .eq("workflow_id", workflow_id)
        .eq("idempotency_key", idempotency_key);
    assert_eq!(driver.execute(&cmd).await.unwrap(), 1);
}

async fn set_side_effect_updated_at(
    database_url: &str,
    tables: &PgWorkflowTables,
    operation_id: &str,
    updated_at: &str,
) {
    let mut driver = PgDriver::connect_url(database_url).await.unwrap();
    let cmd = Qail::set(&tables.side_effects)
        .set_value("updated_at", updated_at.to_string())
        .eq("operation_id", operation_id);
    assert_eq!(driver.execute(&cmd).await.unwrap(), 1);
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
        .with_tables(tables.clone());
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
    let stale_started_at = live_timestamp(Utc::now() - chrono::Duration::hours(2));
    set_operation_updated_at(
        &database_url,
        &tables,
        "wf-live",
        "event-1",
        &stale_started_at,
    )
    .await;
    assert_eq!(
        WorkflowExecutor::begin_workflow_operation(&executor, &operation)
            .await
            .unwrap(),
        WorkflowOperationStatus::Started,
        "stale started operations must be retryable after a worker crash"
    );
    assert_eq!(
        WorkflowExecutor::begin_workflow_operation(&executor, &operation)
            .await
            .unwrap(),
        WorkflowOperationStatus::InProgress,
        "retry refresh must restore the in-progress guard"
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
    let mismatched_workflow_operation = WorkflowOperation {
        workflow_name: "other_workflow".to_string(),
        ..operation.clone()
    };
    assert_error_contains(
        WorkflowExecutor::begin_workflow_operation(&executor, &mismatched_workflow_operation).await,
        "previously used for workflow",
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

    let mismatched_side_effect = WorkflowSideEffect {
        kind: WorkflowSideEffectKind::Query,
        ..side_effect.clone()
    };
    assert_error_contains(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &mismatched_side_effect).await,
        "previously used for",
    );

    let retry_side_effect = WorkflowSideEffect {
        workflow_id: "wf-live".to_string(),
        state: "awaiting_vendor".to_string(),
        step_path: "steps[2]".to_string(),
        kind: WorkflowSideEffectKind::Notify,
        operation_id: "notify-retry-live".to_string(),
    };
    assert_eq!(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &retry_side_effect)
            .await
            .unwrap(),
        WorkflowSideEffectStatus::Execute
    );
    let wrong_retry_identity = WorkflowSideEffect {
        workflow_id: "wf-other".to_string(),
        ..retry_side_effect.clone()
    };
    assert_error_contains(
        WorkflowExecutor::fail_workflow_side_effect(
            &executor,
            &wrong_retry_identity,
            "wrong identity",
        )
        .await,
        "was not found in started state",
    );
    WorkflowExecutor::fail_workflow_side_effect(&executor, &retry_side_effect, "transient failure")
        .await
        .unwrap();
    assert_eq!(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &retry_side_effect)
            .await
            .unwrap(),
        WorkflowSideEffectStatus::Execute,
        "failed side effect should be retryable"
    );
    WorkflowExecutor::complete_workflow_side_effect(&executor, &retry_side_effect, None)
        .await
        .unwrap();
    assert_eq!(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &retry_side_effect)
            .await
            .unwrap(),
        WorkflowSideEffectStatus::AlreadyCompleted { result: None }
    );
    assert_error_contains(
        WorkflowExecutor::fail_workflow_side_effect(&executor, &retry_side_effect, "late failure")
            .await,
        "was not found in started state",
    );

    let stale_side_effect = WorkflowSideEffect {
        workflow_id: "wf-live".to_string(),
        state: "awaiting_vendor".to_string(),
        step_path: "steps[3]".to_string(),
        kind: WorkflowSideEffectKind::Notify,
        operation_id: "notify-stale-live".to_string(),
    };
    assert_eq!(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &stale_side_effect)
            .await
            .unwrap(),
        WorkflowSideEffectStatus::Execute
    );
    assert_error_contains(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &stale_side_effect).await,
        "already in progress",
    );
    set_side_effect_updated_at(
        &database_url,
        &tables,
        "notify-stale-live",
        &stale_started_at,
    )
    .await;
    assert_eq!(
        WorkflowExecutor::begin_workflow_side_effect(&executor, &stale_side_effect)
            .await
            .unwrap(),
        WorkflowSideEffectStatus::Execute,
        "stale started side effects must be retryable after a worker crash"
    );
    WorkflowExecutor::complete_workflow_side_effect(&executor, &stale_side_effect, None)
        .await
        .unwrap();

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

    let mut future_due = WorkflowContext::new("wf-timeout-future-live", "awaiting_vendor");
    future_due.definition_name = Some("future_booking_recovery".to_string());
    future_due.set_cursor(WorkflowCursor {
        state: "awaiting_vendor".to_string(),
        frames: vec![WorkflowCursorFrame::Steps { index: 1 }],
        wait: Some(WorkflowPendingWait {
            event: "vendor.accepted".to_string(),
            deadline_at: Utc::now() + chrono::Duration::minutes(10),
            on_timeout: vec![WorkflowStep::transition("expired")],
        }),
    });
    WorkflowExecutor::save_state(&executor, &future_due)
        .await
        .unwrap();

    let scheduler_now = Utc::now() + chrono::Duration::minutes(20);
    let future_due_ids = WorkflowExecutor::load_due_workflow_timeouts(
        &executor,
        "future_booking_recovery",
        scheduler_now,
        10,
    )
    .await
    .unwrap();
    assert_eq!(future_due_ids, vec!["wf-timeout-future-live".to_string()]);
    let future_claimed_again = WorkflowExecutor::load_due_workflow_timeouts(
        &executor,
        "future_booking_recovery",
        scheduler_now,
        10,
    )
    .await
    .unwrap();
    assert!(
        future_claimed_again.is_empty(),
        "claim TTL must be relative to the scheduler timestamp used for due selection"
    );

    let failing_query = qail_core::wire::encode_cmd_text(&Qail::get("bookings").limit(1));
    let timeout_checkpoint_workflow = WorkflowDefinition::new("timeout_checkpoint_live")
        .initial_state("active")
        .transition(
            "active",
            "done",
            vec![WorkflowStep::wait_or(
                "vendor.accepted",
                std::time::Duration::from_secs(0),
                vec![
                    WorkflowStep::notify(ChannelKind::Email, "vendor_timeout", "customer.email"),
                    WorkflowStep::Query {
                        cmd_json: failing_query,
                        store_as: None,
                    },
                    WorkflowStep::transition("expired"),
                ],
            )],
        );
    let mut timeout_checkpoint = WorkflowContext::new("wf-timeout-checkpoint-live", "active");
    timeout_checkpoint.set(
        "customer",
        serde_json::json!({"email": "guest@example.com"}),
    );
    qail_workflow::run_workflow(
        &executor,
        &timeout_checkpoint_workflow,
        &mut timeout_checkpoint,
    )
    .await
    .unwrap();
    let outcomes = timeout_due_workflows(
        &executor,
        &timeout_checkpoint_workflow,
        Utc::now() + chrono::Duration::seconds(1),
        10,
        WorkflowRunOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].workflow_id, "wf-timeout-checkpoint-live");
    assert!(
        outcomes[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("live storage test does not execute app queries")),
        "timeout fallback query failure must be recorded, got {outcomes:?}"
    );
    let due_after_checkpoint = WorkflowExecutor::load_due_workflow_timeouts(
        &executor,
        "timeout_checkpoint_live",
        Utc::now() + chrono::Duration::seconds(2),
        10,
    )
    .await
    .unwrap();
    assert_eq!(
        due_after_checkpoint,
        vec!["wf-timeout-checkpoint-live".to_string()],
        "timeout fallback checkpoints must remain visible to the due-timeout scheduler"
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
