//! Extended Query protocol phase tracker.
//!
//! Validates backend message ordering for Parse/Bind/Execute/Sync flows.
//! This hardens against adversarial or malformed backend sequences that
//! could otherwise be accepted by broad response loops.

use super::{PgError, PgResult};
use crate::protocol::{BackendMessage, TransactionStatus};

/// Configuration for extended-protocol response ordering.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExtendedFlowConfig {
    /// Whether a ParseComplete is expected before BindComplete on success.
    pub expect_parse_complete: bool,
    /// Whether ParameterDescription is allowed in this flow.
    pub allow_parameter_description: bool,
    /// Whether RowDescription may appear before BindComplete.
    pub allow_row_description_before_bind: bool,
    /// Whether NoData may appear before BindComplete.
    pub allow_no_data_before_bind: bool,
    /// Whether NoData may appear after BindComplete.
    pub allow_no_data_after_bind: bool,
    /// Whether successful completion must include BindComplete.
    pub require_bind_complete_on_success: bool,
    /// Whether successful completion must include a terminal completion message.
    pub require_completion_on_success: bool,
}

impl ExtendedFlowConfig {
    /// Parse + Bind + Execute + Sync (no Describe roundtrip).
    pub(crate) fn parse_bind_execute(expect_parse_complete: bool) -> Self {
        Self {
            expect_parse_complete,
            allow_parameter_description: false,
            allow_row_description_before_bind: false,
            allow_no_data_before_bind: false,
            allow_no_data_after_bind: false,
            require_bind_complete_on_success: true,
            require_completion_on_success: true,
        }
    }

    /// Parse + Bind + Describe(Portal) + Execute + Sync.
    pub(crate) fn parse_bind_describe_portal_execute() -> Self {
        Self {
            expect_parse_complete: true,
            allow_parameter_description: false,
            allow_row_description_before_bind: false,
            allow_no_data_before_bind: false,
            allow_no_data_after_bind: true,
            require_bind_complete_on_success: true,
            require_completion_on_success: true,
        }
    }

    /// Parse + Describe(Statement) + Bind + Execute + Sync.
    pub(crate) fn parse_describe_statement_bind_execute(expect_parse_complete: bool) -> Self {
        Self {
            expect_parse_complete,
            allow_parameter_description: true,
            allow_row_description_before_bind: true,
            allow_no_data_before_bind: true,
            allow_no_data_after_bind: false,
            require_bind_complete_on_success: true,
            require_completion_on_success: true,
        }
    }
}

/// Runtime tracker for one extended-protocol response flow.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExtendedFlowTracker {
    cfg: ExtendedFlowConfig,
    saw_parse_complete: bool,
    saw_bind_complete: bool,
    saw_completion: bool,
    saw_error_response: bool,
}

impl ExtendedFlowTracker {
    pub(crate) fn new(cfg: ExtendedFlowConfig) -> Self {
        Self {
            cfg,
            saw_parse_complete: false,
            saw_bind_complete: false,
            saw_completion: false,
            saw_error_response: false,
        }
    }

    pub(crate) fn saw_parse_complete(&self) -> bool {
        self.saw_parse_complete
    }

    /// Validate that `msg` is legal for the current flow phase.
    ///
    /// `error_pending` should be `true` when the caller has already seen an
    /// ErrorResponse in the current flow and is draining to ReadyForQuery.
    pub(crate) fn validate(
        &mut self,
        msg: &BackendMessage,
        context: &'static str,
        error_pending: bool,
    ) -> PgResult<()> {
        use BackendMessage::*;

        match msg {
            ErrorResponse(_) => {
                self.saw_error_response = true;
                return Ok(());
            }
            ParseComplete => {
                if !self.cfg.expect_parse_complete {
                    return Err(violation(
                        context,
                        "unexpected ParseComplete (Parse was not sent)",
                    ));
                }
                if self.saw_parse_complete {
                    return Err(violation(context, "duplicate ParseComplete"));
                }
                if self.saw_bind_complete {
                    return Err(violation(context, "ParseComplete after BindComplete"));
                }
                if self.saw_completion {
                    return Err(violation(context, "ParseComplete after completion"));
                }
                self.saw_parse_complete = true;
                return Ok(());
            }
            ParameterDescription(_) => {
                if !self.cfg.allow_parameter_description {
                    return Err(violation(context, "unexpected ParameterDescription"));
                }
                if self.cfg.expect_parse_complete && !self.saw_parse_complete {
                    return Err(violation(
                        context,
                        "ParameterDescription before ParseComplete",
                    ));
                }
                if self.saw_bind_complete {
                    return Err(violation(
                        context,
                        "ParameterDescription after BindComplete",
                    ));
                }
                if self.saw_completion {
                    return Err(violation(context, "ParameterDescription after completion"));
                }
                return Ok(());
            }
            BindComplete => {
                if self.saw_bind_complete {
                    return Err(violation(context, "duplicate BindComplete"));
                }
                if self.cfg.expect_parse_complete
                    && !self.saw_parse_complete
                    && !error_pending
                    && !self.saw_error_response
                {
                    return Err(violation(context, "BindComplete before ParseComplete"));
                }
                if self.saw_completion {
                    return Err(violation(context, "BindComplete after completion"));
                }
                self.saw_bind_complete = true;
                return Ok(());
            }
            RowDescription(_) => {
                if self.saw_completion {
                    return Err(violation(context, "RowDescription after completion"));
                }
                if !self.saw_bind_complete {
                    if !self.cfg.allow_row_description_before_bind {
                        return Err(violation(context, "RowDescription before BindComplete"));
                    }
                    if self.cfg.expect_parse_complete && !self.saw_parse_complete {
                        return Err(violation(context, "RowDescription before ParseComplete"));
                    }
                }
                return Ok(());
            }
            NoData => {
                if self.saw_completion {
                    return Err(violation(context, "NoData after completion"));
                }
                if self.saw_bind_complete {
                    if !self.cfg.allow_no_data_after_bind {
                        return Err(violation(context, "unexpected NoData after BindComplete"));
                    }
                } else {
                    if !self.cfg.allow_no_data_before_bind {
                        return Err(violation(context, "unexpected NoData before BindComplete"));
                    }
                    if self.cfg.expect_parse_complete && !self.saw_parse_complete {
                        return Err(violation(context, "NoData before ParseComplete"));
                    }
                }
                return Ok(());
            }
            DataRow(_) => {
                if !self.saw_bind_complete {
                    return Err(violation(context, "DataRow before BindComplete"));
                }
                if self.saw_completion {
                    return Err(violation(context, "DataRow after completion"));
                }
                return Ok(());
            }
            CommandComplete(_) | PortalSuspended | EmptyQueryResponse => {
                if !self.saw_bind_complete && !error_pending && !self.saw_error_response {
                    return Err(violation(context, "completion before BindComplete"));
                }
                if self.saw_completion {
                    return Err(violation(context, "duplicate completion message"));
                }
                self.saw_completion = true;
                return Ok(());
            }
            ReadyForQuery(_) => {
                if error_pending || self.saw_error_response {
                    return Ok(());
                }
                if self.cfg.expect_parse_complete && !self.saw_parse_complete {
                    return Err(violation(context, "ReadyForQuery before ParseComplete"));
                }
                if self.cfg.require_bind_complete_on_success && !self.saw_bind_complete {
                    return Err(violation(context, "ReadyForQuery before BindComplete"));
                }
                if self.cfg.require_completion_on_success && !self.saw_completion {
                    return Err(violation(
                        context,
                        "ReadyForQuery before completion message",
                    ));
                }
                return Ok(());
            }
            _ => {}
        }

        Ok(())
    }

    /// Validate one backend message by wire-type byte.
    ///
    /// Useful for fast receive paths that inspect message type without
    /// constructing full `BackendMessage` values.
    pub(crate) fn validate_msg_type(
        &mut self,
        msg_type: u8,
        context: &'static str,
        error_pending: bool,
    ) -> PgResult<()> {
        if matches!(msg_type, b'N' | b'S') {
            return Ok(());
        }

        let msg = match msg_type {
            b'1' => BackendMessage::ParseComplete,
            b'2' => BackendMessage::BindComplete,
            b't' => BackendMessage::ParameterDescription(Vec::new()),
            b'T' => BackendMessage::RowDescription(Vec::new()),
            b'n' => BackendMessage::NoData,
            b'D' => BackendMessage::DataRow(Vec::new()),
            b'C' => BackendMessage::CommandComplete(String::new()),
            b's' => BackendMessage::PortalSuspended,
            b'I' => BackendMessage::EmptyQueryResponse,
            b'Z' => BackendMessage::ReadyForQuery(TransactionStatus::Idle),
            _ => {
                let printable = if msg_type.is_ascii_graphic() {
                    msg_type as char
                } else {
                    '?'
                };
                return Err(PgError::Protocol(format!(
                    "{}: unexpected backend message type byte={} char={}",
                    context, msg_type, printable
                )));
            }
        };
        self.validate(&msg, context, error_pending)
    }
}

fn violation(context: &'static str, detail: &str) -> PgError {
    PgError::Protocol(format!("{}: {}", context, detail))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{BackendMessage, TransactionStatus};

    fn parse_complete() -> BackendMessage {
        BackendMessage::ParseComplete
    }
    fn bind_complete() -> BackendMessage {
        BackendMessage::BindComplete
    }
    fn row_desc() -> BackendMessage {
        BackendMessage::RowDescription(Vec::new())
    }
    fn no_data() -> BackendMessage {
        BackendMessage::NoData
    }
    fn data_row() -> BackendMessage {
        BackendMessage::DataRow(Vec::new())
    }
    fn command_complete() -> BackendMessage {
        BackendMessage::CommandComplete("SELECT 1".to_string())
    }
    fn ready() -> BackendMessage {
        BackendMessage::ReadyForQuery(TransactionStatus::Idle)
    }
    fn parameter_description() -> BackendMessage {
        BackendMessage::ParameterDescription(Vec::new())
    }

    #[test]
    fn parse_bind_execute_happy_path() {
        let mut tracker = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));
        tracker.validate(&parse_complete(), "ctx", false).unwrap();
        tracker.validate(&bind_complete(), "ctx", false).unwrap();
        tracker.validate(&row_desc(), "ctx", false).unwrap();
        tracker.validate(&data_row(), "ctx", false).unwrap();
        tracker.validate(&command_complete(), "ctx", false).unwrap();
        tracker.validate(&ready(), "ctx", false).unwrap();
    }

    #[test]
    fn parse_bind_execute_rejects_bind_before_parse() {
        let mut tracker = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));
        let err = tracker
            .validate(&bind_complete(), "ctx", false)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("BindComplete before ParseComplete")
        );
    }

    #[test]
    fn parse_bind_execute_rejects_data_before_bind() {
        let mut tracker = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));
        tracker.validate(&parse_complete(), "ctx", false).unwrap();
        let err = tracker.validate(&data_row(), "ctx", false).unwrap_err();
        assert!(err.to_string().contains("DataRow before BindComplete"));
    }

    #[test]
    fn parse_bind_execute_rejects_ready_before_completion() {
        let mut tracker = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));
        tracker.validate(&parse_complete(), "ctx", false).unwrap();
        tracker.validate(&bind_complete(), "ctx", false).unwrap();
        let err = tracker.validate(&ready(), "ctx", false).unwrap_err();
        assert!(err.to_string().contains("ReadyForQuery before completion"));
    }

    #[test]
    fn parse_describe_statement_allows_pre_bind_describe_messages() {
        let mut tracker = ExtendedFlowTracker::new(
            ExtendedFlowConfig::parse_describe_statement_bind_execute(true),
        );
        tracker.validate(&parse_complete(), "ctx", false).unwrap();
        tracker
            .validate(&parameter_description(), "ctx", false)
            .unwrap();
        tracker.validate(&row_desc(), "ctx", false).unwrap();
        tracker.validate(&bind_complete(), "ctx", false).unwrap();
        tracker.validate(&command_complete(), "ctx", false).unwrap();
        tracker.validate(&ready(), "ctx", false).unwrap();
    }

    #[test]
    fn parse_bind_describe_portal_allows_no_data_after_bind() {
        let mut tracker =
            ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_describe_portal_execute());
        tracker.validate(&parse_complete(), "ctx", false).unwrap();
        tracker.validate(&bind_complete(), "ctx", false).unwrap();
        tracker.validate(&no_data(), "ctx", false).unwrap();
        tracker.validate(&command_complete(), "ctx", false).unwrap();
        tracker.validate(&ready(), "ctx", false).unwrap();
    }
}
