use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use qail_core::ast::{Expr, Value as QailValue};
use qail_pg::{PgDriver, PgError, PgRow};
use qail_workflow::{WorkflowError, WorkflowOperationKind};

pub(crate) const STATUS_STARTED: &str = "started";
pub(crate) const STATUS_COMPLETED: &str = "completed";
pub(crate) const STATUS_FAILED: &str = "failed";

const DUPLICATE_SQLSTATE: &str = "23505";

pub(crate) fn excluded(column: &str) -> Expr {
    Expr::Named(format!("EXCLUDED.{column}"))
}

pub(crate) fn option_string(value: Option<String>) -> QailValue {
    value.map(QailValue::String).unwrap_or(QailValue::Null)
}

pub(crate) fn timestamp(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

pub(crate) fn deadline_from_duration(ttl: Duration) -> Result<DateTime<Utc>, WorkflowError> {
    let ttl = chrono::Duration::from_std(ttl)
        .map_err(|_| WorkflowError::Other("Workflow Postgres duration is too large".to_string()))?;
    Utc::now()
        .checked_add_signed(ttl)
        .ok_or_else(|| WorkflowError::Other("Workflow Postgres deadline overflowed".to_string()))
}

pub(crate) fn operation_kind_text(kind: &WorkflowOperationKind) -> String {
    match kind {
        WorkflowOperationKind::Run => {
            serde_json::json!(["qail-workflow-operation", 1, "run"]).to_string()
        }
        WorkflowOperationKind::Resume { event } => {
            serde_json::json!(["qail-workflow-operation", 1, "resume", event]).to_string()
        }
        WorkflowOperationKind::Timeout => {
            serde_json::json!(["qail-workflow-operation", 1, "timeout"]).to_string()
        }
    }
}

pub(crate) fn required_string(
    row: &PgRow,
    idx: usize,
    name: &str,
) -> Result<String, WorkflowError> {
    row.get_string(idx)
        .ok_or_else(|| WorkflowError::Other(format!("Workflow Postgres row missing '{name}'")))
}

pub(crate) fn optional_json(
    row: &PgRow,
    idx: usize,
) -> Result<Option<serde_json::Value>, WorkflowError> {
    row.get_string(idx)
        .map(|value| serde_json::from_str(&value).map_err(json_error))
        .transpose()
}

pub(crate) fn json_error(err: serde_json::Error) -> WorkflowError {
    WorkflowError::Other(format!("Workflow Postgres JSON error: {err}"))
}

pub(crate) fn pg_error(err: PgError) -> WorkflowError {
    WorkflowError::Other(format!("Workflow Postgres error: {err}"))
}

pub(crate) fn is_duplicate(err: &PgError) -> bool {
    err.sqlstate() == Some(DUPLICATE_SQLSTATE)
}

pub(crate) async fn finish_tx<T>(
    driver: &mut PgDriver,
    result: Result<T, WorkflowError>,
) -> Result<T, WorkflowError> {
    match result {
        Ok(value) => {
            driver.commit().await.map_err(pg_error)?;
            Ok(value)
        }
        Err(err) => {
            let _ = driver.rollback().await;
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qail_workflow::{WorkflowOperationStatus, WorkflowSideEffectStatus};

    #[test]
    fn operation_kind_text_includes_resume_event() {
        let kind = WorkflowOperationKind::Resume {
            event: "payment.success".to_string(),
        };

        assert_eq!(
            operation_kind_text(&kind),
            r#"["qail-workflow-operation",1,"resume","payment.success"]"#
        );
    }

    #[test]
    fn default_timestamps_sort_lexically() {
        let earlier = timestamp(
            DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .into(),
        );
        let later = timestamp(
            DateTime::parse_from_rfc3339("2026-01-01T00:00:01Z")
                .unwrap()
                .into(),
        );

        assert!(earlier < later);
    }

    #[test]
    fn completed_side_effect_without_result_round_trips_as_none() {
        let row = PgRow {
            columns: vec![Some(STATUS_COMPLETED.as_bytes().to_vec()), None],
            column_info: None,
        };

        assert_eq!(optional_json(&row, 1).unwrap(), None);
    }

    #[test]
    fn completed_side_effect_result_decodes_json() {
        let row = PgRow {
            columns: vec![
                Some(STATUS_COMPLETED.as_bytes().to_vec()),
                Some(br#"{"charge_id":"ch_1"}"#.to_vec()),
            ],
            column_info: None,
        };

        assert_eq!(
            optional_json(&row, 1).unwrap(),
            Some(serde_json::json!({"charge_id": "ch_1"}))
        );
    }

    #[test]
    fn generic_finish_tx_type_is_visible_to_lints() {
        fn assert_send<T: Send>() {}
        assert_send::<WorkflowOperationStatus>();
        assert_send::<WorkflowSideEffectStatus>();
    }
}
