//! Replication helpers.
//!
//! Current scope:
//! - `IDENTIFY_SYSTEM`
//! - `CREATE_REPLICATION_SLOT ... LOGICAL ...`
//! - `DROP_REPLICATION_SLOT`
//! - `START_REPLICATION SLOT ... LOGICAL ...`
//! - CopyBoth stream message decode (`XLogData`, keepalive)
//! - Standby status updates (`'r'`)

use super::{
    PgConnection, PgDriver, PgError, PgResult, PgRow, is_ignorable_session_message,
    unexpected_backend_message,
};
use crate::protocol::{BackendMessage, PgEncoder};

/// Output of `IDENTIFY_SYSTEM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifySystem {
    /// Cluster system identifier.
    pub system_id: String,
    /// Current timeline ID.
    pub timeline: u32,
    /// Current WAL/LSN position as text (e.g. `0/16B6C50`).
    pub xlog_pos: String,
    /// Database name for logical replication sessions (if provided by server).
    pub dbname: Option<String>,
}

/// Output of `CREATE_REPLICATION_SLOT ... LOGICAL ...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSlotInfo {
    /// Created slot name.
    pub slot_name: String,
    /// Consistent point at which the slot became valid.
    pub consistent_point: String,
    /// Exported snapshot name (if any).
    pub snapshot_name: Option<String>,
    /// Output plugin used by this logical slot.
    pub output_plugin: String,
}

/// Metadata returned when a logical replication stream starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationStreamStart {
    /// Copy format (0=text, 1=binary).
    pub format: u8,
    /// Per-column format codes.
    pub column_formats: Vec<u8>,
}

/// WAL payload message from `CopyData('w' ...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationXLogData {
    /// WAL start position of this payload.
    pub wal_start: u64,
    /// Current WAL end on server.
    pub wal_end: u64,
    /// Server clock in microseconds since PostgreSQL epoch (2000-01-01).
    pub server_time_micros: i64,
    /// Output-plugin payload bytes.
    pub data: Vec<u8>,
}

/// Keepalive message from `CopyData('k' ...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationKeepalive {
    /// Current WAL end on server.
    pub wal_end: u64,
    /// Server clock in microseconds since PostgreSQL epoch (2000-01-01).
    pub server_time_micros: i64,
    /// Whether server requests an immediate status reply.
    pub reply_requested: bool,
}

/// Logical replication stream message decoded from `CopyData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationStreamMessage {
    /// XLog data frame (`'w'`).
    XLogData(ReplicationXLogData),
    /// Primary keepalive frame (`'k'`).
    Keepalive(ReplicationKeepalive),
    /// Unknown CopyData sub-message tag.
    Raw { tag: u8, payload: Vec<u8> },
}

/// Plugin options used in `START_REPLICATION ... LOGICAL ... (k 'v', ...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationOption {
    /// Option key (strict identifier).
    pub key: String,
    /// Option value (quoted SQL string in command text).
    pub value: String,
}

const MAX_REPLICATION_OPTIONS: usize = 64;
const MAX_REPLICATION_OPTION_VALUE_BYTES: usize = 16 * 1024;
const MAX_REPLICATION_XLOGDATA_BYTES: usize = 16 * 1024 * 1024;

fn validate_ident(kind: &str, ident: &str) -> PgResult<()> {
    if ident.is_empty() {
        return Err(PgError::Query(format!("{} must not be empty", kind)));
    }
    if ident.len() > 63 {
        return Err(PgError::Query(format!(
            "{} '{}' exceeds PostgreSQL identifier limit (63 bytes)",
            kind, ident
        )));
    }
    let mut chars = ident.chars();
    match chars.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => {
            return Err(PgError::Query(format!(
                "{} '{}' must start with [A-Za-z_]",
                kind, ident
            )));
        }
    }
    if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return Err(PgError::Query(format!(
            "{} '{}' contains unsupported characters (allowed: [A-Za-z0-9_])",
            kind, ident
        )));
    }
    Ok(())
}

fn sql_single_quote_literal(value: &str) -> PgResult<String> {
    if value.contains('\0') {
        return Err(PgError::Query(
            "replication option value contains NUL byte".to_string(),
        ));
    }
    Ok(value.replace('\'', "''"))
}

fn parse_lsn_text(lsn: &str) -> PgResult<u64> {
    let mut parts = lsn.split('/');
    let high = parts
        .next()
        .ok_or_else(|| PgError::Query(format!("Invalid LSN '{}'", lsn)))?;
    let low = parts
        .next()
        .ok_or_else(|| PgError::Query(format!("Invalid LSN '{}'", lsn)))?;
    if parts.next().is_some() {
        return Err(PgError::Query(format!("Invalid LSN '{}'", lsn)));
    }
    let high = u32::from_str_radix(high, 16)
        .map_err(|_| PgError::Query(format!("Invalid LSN '{}'", lsn)))?;
    let low = u32::from_str_radix(low, 16)
        .map_err(|_| PgError::Query(format!("Invalid LSN '{}'", lsn)))?;
    Ok(((high as u64) << 32) | (low as u64))
}

#[cfg(test)]
fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", (lsn >> 32) as u32, lsn as u32)
}

fn required_text(row: &PgRow, idx: usize, field: &str) -> PgResult<String> {
    row.get_string(idx).ok_or_else(|| {
        PgError::Protocol(format!("Missing or invalid '{}' in replication row", field))
    })
}

fn parse_identify_system_row(row: &PgRow) -> PgResult<IdentifySystem> {
    let system_id = required_text(row, 0, "systemid")?;
    let timeline = required_text(row, 1, "timeline")?
        .parse::<u32>()
        .map_err(|e| PgError::Protocol(format!("Invalid timeline value: {}", e)))?;
    let xlog_pos = required_text(row, 2, "xlogpos")?;
    let dbname = row
        .get_string(3)
        .and_then(|v| if v.is_empty() { None } else { Some(v) });

    Ok(IdentifySystem {
        system_id,
        timeline,
        xlog_pos,
        dbname,
    })
}

fn parse_create_slot_row(row: &PgRow) -> PgResult<ReplicationSlotInfo> {
    let slot_name = required_text(row, 0, "slot_name")?;
    let consistent_point = required_text(row, 1, "consistent_point")?;
    let snapshot_name = row
        .get_string(2)
        .and_then(|v| if v.is_empty() { None } else { Some(v) });
    let output_plugin = required_text(row, 3, "output_plugin")?;

    Ok(ReplicationSlotInfo {
        slot_name,
        consistent_point,
        snapshot_name,
        output_plugin,
    })
}

fn build_create_logical_replication_slot_sql(
    slot_name: &str,
    output_plugin: &str,
    temporary: bool,
    two_phase: bool,
) -> PgResult<String> {
    validate_ident("slot_name", slot_name)?;
    validate_ident("output_plugin", output_plugin)?;

    let mut sql = String::from("CREATE_REPLICATION_SLOT ");
    sql.push_str(slot_name);
    if temporary {
        sql.push_str(" TEMPORARY");
    }
    sql.push_str(" LOGICAL ");
    sql.push_str(output_plugin);
    if two_phase {
        sql.push_str(" TWO_PHASE");
    }
    Ok(sql)
}

fn build_drop_replication_slot_sql(slot_name: &str, wait: bool) -> PgResult<String> {
    validate_ident("slot_name", slot_name)?;
    let mut sql = String::from("DROP_REPLICATION_SLOT ");
    sql.push_str(slot_name);
    if wait {
        sql.push_str(" WAIT");
    }
    Ok(sql)
}

fn build_start_logical_replication_sql(
    slot_name: &str,
    start_lsn: &str,
    options: &[ReplicationOption],
) -> PgResult<String> {
    validate_ident("slot_name", slot_name)?;
    let _ = parse_lsn_text(start_lsn)?;
    if options.len() > MAX_REPLICATION_OPTIONS {
        return Err(PgError::Query(format!(
            "too many replication options: {} (max {})",
            options.len(),
            MAX_REPLICATION_OPTIONS
        )));
    }

    let mut sql = format!("START_REPLICATION SLOT {} LOGICAL {}", slot_name, start_lsn);
    if !options.is_empty() {
        sql.push_str(" (");
        for (idx, opt) in options.iter().enumerate() {
            validate_ident("replication option key", &opt.key)?;
            if opt.value.len() > MAX_REPLICATION_OPTION_VALUE_BYTES {
                return Err(PgError::Query(format!(
                    "replication option '{}' value too large: {} bytes (max {})",
                    opt.key,
                    opt.value.len(),
                    MAX_REPLICATION_OPTION_VALUE_BYTES
                )));
            }
            if idx > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&opt.key);
            sql.push_str(" '");
            sql.push_str(&sql_single_quote_literal(&opt.value)?);
            sql.push('\'');
        }
        sql.push(')');
    }
    Ok(sql)
}

fn parse_copy_data_message(payload: &[u8]) -> PgResult<ReplicationStreamMessage> {
    if payload.is_empty() {
        return Err(PgError::Protocol(
            "Replication CopyData payload is empty".to_string(),
        ));
    }
    match payload[0] {
        b'w' => {
            if payload.len() < 25 {
                return Err(PgError::Protocol(format!(
                    "XLogData payload too short: {} bytes",
                    payload.len()
                )));
            }
            let wal_start = u64::from_be_bytes(
                payload[1..9]
                    .try_into()
                    .map_err(|_| PgError::Protocol("Invalid wal_start bytes".to_string()))?,
            );
            let wal_end = u64::from_be_bytes(
                payload[9..17]
                    .try_into()
                    .map_err(|_| PgError::Protocol("Invalid wal_end bytes".to_string()))?,
            );
            let server_time_micros = i64::from_be_bytes(
                payload[17..25]
                    .try_into()
                    .map_err(|_| PgError::Protocol("Invalid server time bytes".to_string()))?,
            );
            if wal_end < wal_start {
                return Err(PgError::Protocol(format!(
                    "XLogData wal_end {} is behind wal_start {}",
                    wal_end, wal_start
                )));
            }
            let data_len = payload.len() - 25;
            if data_len > MAX_REPLICATION_XLOGDATA_BYTES {
                return Err(PgError::Protocol(format!(
                    "XLogData payload too large: {} bytes (max {})",
                    data_len, MAX_REPLICATION_XLOGDATA_BYTES
                )));
            }
            Ok(ReplicationStreamMessage::XLogData(ReplicationXLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data: payload[25..].to_vec(),
            }))
        }
        b'k' => {
            if payload.len() != 18 {
                return Err(PgError::Protocol(format!(
                    "Keepalive payload must be 18 bytes, got {}",
                    payload.len()
                )));
            }
            let wal_end =
                u64::from_be_bytes(payload[1..9].try_into().map_err(|_| {
                    PgError::Protocol("Invalid keepalive wal_end bytes".to_string())
                })?);
            let server_time_micros = i64::from_be_bytes(
                payload[9..17]
                    .try_into()
                    .map_err(|_| PgError::Protocol("Invalid keepalive time bytes".to_string()))?,
            );
            let reply_requested = match payload[17] {
                0 => false,
                1 => true,
                other => {
                    return Err(PgError::Protocol(format!(
                        "Keepalive reply_requested must be 0 or 1, got {}",
                        other
                    )));
                }
            };
            Ok(ReplicationStreamMessage::Keepalive(ReplicationKeepalive {
                wal_end,
                server_time_micros,
                reply_requested,
            }))
        }
        tag => Err(PgError::Protocol(format!(
            "Unsupported replication CopyData tag '{}'",
            if tag.is_ascii_graphic() {
                tag as char
            } else {
                '?'
            }
        ))),
    }
}

fn postgres_epoch_micros_now() -> i64 {
    const PG_UNIX_EPOCH_DIFF_SECS: i64 = 946_684_800;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let unix_micros = (now.as_secs() as i64) * 1_000_000 + i64::from(now.subsec_micros());
    unix_micros - PG_UNIX_EPOCH_DIFF_SECS * 1_000_000
}

fn build_standby_status_update_payload(
    write_lsn: u64,
    flush_lsn: u64,
    apply_lsn: u64,
    reply_requested: bool,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 8 + 8 + 8 + 8 + 1);
    payload.push(b'r');
    payload.extend_from_slice(&write_lsn.to_be_bytes());
    payload.extend_from_slice(&flush_lsn.to_be_bytes());
    payload.extend_from_slice(&apply_lsn.to_be_bytes());
    payload.extend_from_slice(&postgres_epoch_micros_now().to_be_bytes());
    payload.push(if reply_requested { 1 } else { 0 });
    payload
}

impl PgConnection {
    #[inline]
    fn ensure_replication_mode(&self, operation: &str) -> PgResult<()> {
        if self.replication_mode_enabled {
            return Ok(());
        }
        Err(PgError::Protocol(format!(
            "{} requires connection startup param replication=database",
            operation
        )))
    }

    #[inline]
    fn ensure_replication_control_idle(&self, operation: &str) -> PgResult<()> {
        if !self.replication_stream_active {
            return Ok(());
        }
        Err(PgError::Protocol(format!(
            "{} cannot run while replication stream is active",
            operation
        )))
    }

    #[inline]
    fn advance_replication_wal_end(&mut self, source: &str, wal_end: u64) -> PgResult<()> {
        if let Some(prev_wal_end) = self.last_replication_wal_end
            && wal_end < prev_wal_end
        {
            self.replication_stream_active = false;
            self.last_replication_wal_end = None;
            return Err(PgError::Protocol(format!(
                "Replication {} wal_end regressed: previous {}, current {}",
                source, prev_wal_end, wal_end
            )));
        }
        self.last_replication_wal_end = Some(wal_end);
        Ok(())
    }

    /// Run `IDENTIFY_SYSTEM` on a replication connection.
    pub async fn identify_system(&mut self) -> PgResult<IdentifySystem> {
        self.ensure_replication_mode("IDENTIFY_SYSTEM")?;
        self.ensure_replication_control_idle("IDENTIFY_SYSTEM")?;
        let rows = self.simple_query("IDENTIFY_SYSTEM").await?;
        let row = rows
            .first()
            .ok_or_else(|| PgError::Protocol("IDENTIFY_SYSTEM returned no rows".to_string()))?;
        parse_identify_system_row(row)
    }

    /// Create a logical replication slot.
    ///
    /// `slot_name` and `output_plugin` are strict SQL identifiers.
    pub async fn create_logical_replication_slot(
        &mut self,
        slot_name: &str,
        output_plugin: &str,
        temporary: bool,
        two_phase: bool,
    ) -> PgResult<ReplicationSlotInfo> {
        self.ensure_replication_mode("CREATE_REPLICATION_SLOT")?;
        self.ensure_replication_control_idle("CREATE_REPLICATION_SLOT")?;
        let sql = build_create_logical_replication_slot_sql(
            slot_name,
            output_plugin,
            temporary,
            two_phase,
        )?;
        let rows = self.simple_query(&sql).await?;
        let row = rows.first().ok_or_else(|| {
            PgError::Protocol("CREATE_REPLICATION_SLOT returned no rows".to_string())
        })?;
        parse_create_slot_row(row)
    }

    /// Drop a replication slot.
    ///
    /// `wait=true` uses `DROP_REPLICATION_SLOT <slot> WAIT`.
    pub async fn drop_replication_slot(&mut self, slot_name: &str, wait: bool) -> PgResult<()> {
        self.ensure_replication_mode("DROP_REPLICATION_SLOT")?;
        self.ensure_replication_control_idle("DROP_REPLICATION_SLOT")?;
        let sql = build_drop_replication_slot_sql(slot_name, wait)?;
        self.execute_simple(&sql).await
    }

    /// Start logical replication in CopyBoth mode.
    ///
    /// Requires a connection started with `replication=database`.
    pub async fn start_logical_replication(
        &mut self,
        slot_name: &str,
        start_lsn: &str,
        options: &[ReplicationOption],
    ) -> PgResult<ReplicationStreamStart> {
        self.ensure_replication_mode("START_REPLICATION")?;
        if self.replication_stream_active {
            return Err(PgError::Protocol(
                "logical replication stream already active".to_string(),
            ));
        }
        let sql = build_start_logical_replication_sql(slot_name, start_lsn, options)?;
        let bytes = PgEncoder::try_encode_query_string(&sql)?;
        self.write_all_with_timeout(&bytes, "stream write").await?;

        let mut startup_error: Option<PgError> = None;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyBothResponse {
                    format,
                    column_formats,
                } => {
                    if let Some(err) = startup_error {
                        return Err(err);
                    }
                    if format != 0 {
                        return Err(PgError::Protocol(format!(
                            "START_REPLICATION returned unsupported CopyBothResponse format {} (expected 0/text)",
                            format
                        )));
                    }
                    if !column_formats.is_empty() {
                        return Err(PgError::Protocol(format!(
                            "START_REPLICATION returned unexpected CopyBothResponse column formats (expected none, got {})",
                            column_formats.len()
                        )));
                    }
                    self.replication_stream_active = true;
                    self.last_replication_wal_end = None;
                    return Ok(ReplicationStreamStart {
                        format,
                        column_formats,
                    });
                }
                BackendMessage::ReadyForQuery(_) => {
                    return Err(startup_error.unwrap_or_else(|| {
                        PgError::Protocol(
                            "START_REPLICATION ended before CopyBothResponse".to_string(),
                        )
                    }));
                }
                BackendMessage::ErrorResponse(err) => {
                    if startup_error.is_none() {
                        startup_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => return Err(unexpected_backend_message("start replication", &other)),
            }
        }
    }

    /// Receive the next logical replication stream message.
    ///
    /// Uses a no-timeout read path so idle periods do not fail the stream.
    pub async fn recv_replication_message(&mut self) -> PgResult<ReplicationStreamMessage> {
        self.ensure_replication_mode("recv_replication_message")?;
        if !self.replication_stream_active {
            return Err(PgError::Protocol(
                "replication stream is not active; call START_REPLICATION first".to_string(),
            ));
        }
        loop {
            let msg = self.recv_without_timeout().await?;
            match msg {
                BackendMessage::CopyData(payload) => match parse_copy_data_message(&payload) {
                    Ok(ReplicationStreamMessage::XLogData(x)) => {
                        self.advance_replication_wal_end("XLogData", x.wal_end)?;
                        return Ok(ReplicationStreamMessage::XLogData(x));
                    }
                    Ok(ReplicationStreamMessage::Keepalive(k)) => {
                        self.advance_replication_wal_end("keepalive", k.wal_end)?;
                        return Ok(ReplicationStreamMessage::Keepalive(k));
                    }
                    Ok(parsed) => return Ok(parsed),
                    Err(err) => {
                        self.replication_stream_active = false;
                        self.last_replication_wal_end = None;
                        return Err(err);
                    }
                },
                BackendMessage::ErrorResponse(err) => {
                    self.replication_stream_active = false;
                    self.last_replication_wal_end = None;
                    return Err(PgError::QueryServer(err.into()));
                }
                BackendMessage::CopyDone => {
                    self.replication_stream_active = false;
                    self.last_replication_wal_end = None;
                    return Err(PgError::Protocol(
                        "Replication stream ended with CopyDone".to_string(),
                    ));
                }
                BackendMessage::ReadyForQuery(_) => {
                    self.replication_stream_active = false;
                    self.last_replication_wal_end = None;
                    return Err(PgError::Protocol(
                        "Replication stream ended with ReadyForQuery".to_string(),
                    ));
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    self.replication_stream_active = false;
                    self.last_replication_wal_end = None;
                    return Err(unexpected_backend_message("replication stream", &other));
                }
            }
        }
    }

    /// Send a standby status update (`CopyData('r' ...)`) to the server.
    pub async fn send_standby_status_update(
        &mut self,
        write_lsn: u64,
        flush_lsn: u64,
        apply_lsn: u64,
        reply_requested: bool,
    ) -> PgResult<()> {
        self.ensure_replication_mode("send_standby_status_update")?;
        if !self.replication_stream_active {
            return Err(PgError::Protocol(
                "replication stream is not active; call START_REPLICATION first".to_string(),
            ));
        }
        if flush_lsn > write_lsn {
            return Err(PgError::Protocol(format!(
                "Invalid standby status update: flush_lsn {} exceeds write_lsn {}",
                flush_lsn, write_lsn
            )));
        }
        if apply_lsn > flush_lsn {
            return Err(PgError::Protocol(format!(
                "Invalid standby status update: apply_lsn {} exceeds flush_lsn {}",
                apply_lsn, flush_lsn
            )));
        }
        if let Some(last_wal_end) = self.last_replication_wal_end
            && write_lsn > last_wal_end
        {
            return Err(PgError::Protocol(format!(
                "Invalid standby status update: write_lsn {} exceeds last seen server wal_end {}",
                write_lsn, last_wal_end
            )));
        }
        let payload =
            build_standby_status_update_payload(write_lsn, flush_lsn, apply_lsn, reply_requested);
        self.send_copy_data(&payload).await
    }
}

impl PgDriver {
    /// Driver wrapper for [`PgConnection::identify_system`].
    pub async fn identify_system(&mut self) -> PgResult<IdentifySystem> {
        self.connection.identify_system().await
    }

    /// Driver wrapper for [`PgConnection::create_logical_replication_slot`].
    pub async fn create_logical_replication_slot(
        &mut self,
        slot_name: &str,
        output_plugin: &str,
        temporary: bool,
        two_phase: bool,
    ) -> PgResult<ReplicationSlotInfo> {
        self.connection
            .create_logical_replication_slot(slot_name, output_plugin, temporary, two_phase)
            .await
    }

    /// Driver wrapper for [`PgConnection::drop_replication_slot`].
    pub async fn drop_replication_slot(&mut self, slot_name: &str, wait: bool) -> PgResult<()> {
        self.connection.drop_replication_slot(slot_name, wait).await
    }

    /// Driver wrapper for [`PgConnection::start_logical_replication`].
    pub async fn start_logical_replication(
        &mut self,
        slot_name: &str,
        start_lsn: &str,
        options: &[ReplicationOption],
    ) -> PgResult<ReplicationStreamStart> {
        self.connection
            .start_logical_replication(slot_name, start_lsn, options)
            .await
    }

    /// Driver wrapper for [`PgConnection::recv_replication_message`].
    pub async fn recv_replication_message(&mut self) -> PgResult<ReplicationStreamMessage> {
        self.connection.recv_replication_message().await
    }

    /// Driver wrapper for [`PgConnection::send_standby_status_update`].
    pub async fn send_standby_status_update(
        &mut self,
        write_lsn: u64,
        flush_lsn: u64,
        apply_lsn: u64,
        reply_requested: bool,
    ) -> PgResult<()> {
        self.connection
            .send_standby_status_update(write_lsn, flush_lsn, apply_lsn, reply_requested)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_row(values: &[Option<&str>]) -> PgRow {
        PgRow {
            columns: values
                .iter()
                .map(|v| v.map(|s| s.as_bytes().to_vec()))
                .collect(),
            column_info: None,
        }
    }

    #[test]
    fn validate_ident_rejects_bad_names() {
        assert!(validate_ident("slot_name", "").is_err());
        assert!(validate_ident("slot_name", "9slot").is_err());
        assert!(validate_ident("slot_name", "bad-name").is_err());
        assert!(validate_ident("slot_name", "has space").is_err());
    }

    #[test]
    fn validate_ident_accepts_safe_names() {
        assert!(validate_ident("slot_name", "slot_a1").is_ok());
        assert!(validate_ident("output_plugin", "pgoutput").is_ok());
    }

    #[test]
    fn parse_and_format_lsn_roundtrip() {
        let lsn = parse_lsn_text("16/B6C50").unwrap();
        assert_eq!(format_lsn(lsn), "16/000B6C50");
    }

    #[test]
    fn build_create_logical_replication_slot_sql_variants() {
        let sql =
            build_create_logical_replication_slot_sql("slot_main", "pgoutput", true, true).unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT slot_main TEMPORARY LOGICAL pgoutput TWO_PHASE"
        );
    }

    #[test]
    fn build_drop_replication_slot_sql_variants() {
        let sql = build_drop_replication_slot_sql("slot_main", true).unwrap();
        assert_eq!(sql, "DROP_REPLICATION_SLOT slot_main WAIT");
    }

    #[test]
    fn build_start_logical_replication_sql_with_options() {
        let sql = build_start_logical_replication_sql(
            "slot_main",
            "0/16B6C50",
            &[
                ReplicationOption {
                    key: "proto_version".to_string(),
                    value: "1".to_string(),
                },
                ReplicationOption {
                    key: "publication_names".to_string(),
                    value: "pub1,pub2".to_string(),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            sql,
            "START_REPLICATION SLOT slot_main LOGICAL 0/16B6C50 (proto_version '1', publication_names 'pub1,pub2')"
        );
    }

    #[test]
    fn build_start_logical_replication_sql_rejects_too_many_options() {
        let options: Vec<ReplicationOption> = (0..=MAX_REPLICATION_OPTIONS)
            .map(|i| ReplicationOption {
                key: format!("opt{}", i),
                value: "x".to_string(),
            })
            .collect();

        let err =
            build_start_logical_replication_sql("slot_main", "0/16B6C50", &options).unwrap_err();
        assert!(err.to_string().contains("too many replication options"));
    }

    #[test]
    fn build_start_logical_replication_sql_rejects_null_value() {
        let options = vec![ReplicationOption {
            key: "proto_version".to_string(),
            value: "1\0oops".to_string(),
        }];
        let err =
            build_start_logical_replication_sql("slot_main", "0/16B6C50", &options).unwrap_err();
        assert!(err.to_string().contains("contains NUL byte"));
    }

    #[test]
    fn build_start_logical_replication_sql_rejects_oversized_value() {
        let options = vec![ReplicationOption {
            key: "publication_names".to_string(),
            value: "x".repeat(MAX_REPLICATION_OPTION_VALUE_BYTES + 1),
        }];
        let err =
            build_start_logical_replication_sql("slot_main", "0/16B6C50", &options).unwrap_err();
        assert!(err.to_string().contains("value too large"));
    }

    #[test]
    fn parse_identify_system_row_happy_path() {
        let row = text_row(&[
            Some("7416469842679442267"),
            Some("1"),
            Some("0/16B6C50"),
            Some("app"),
        ]);
        let parsed = parse_identify_system_row(&row).unwrap();
        assert_eq!(parsed.system_id, "7416469842679442267");
        assert_eq!(parsed.timeline, 1);
        assert_eq!(parsed.xlog_pos, "0/16B6C50");
        assert_eq!(parsed.dbname.as_deref(), Some("app"));
    }

    #[test]
    fn parse_create_slot_row_happy_path() {
        let row = text_row(&[
            Some("slot_main"),
            Some("0/16B6C88"),
            Some("00000003-00000041-1"),
            Some("pgoutput"),
        ]);
        let parsed = parse_create_slot_row(&row).unwrap();
        assert_eq!(parsed.slot_name, "slot_main");
        assert_eq!(parsed.consistent_point, "0/16B6C88");
        assert_eq!(parsed.snapshot_name.as_deref(), Some("00000003-00000041-1"));
        assert_eq!(parsed.output_plugin, "pgoutput");
    }

    #[test]
    fn parse_copy_data_xlog_data() {
        let mut payload = Vec::new();
        payload.push(b'w');
        payload.extend_from_slice(&0x10_u64.to_be_bytes());
        payload.extend_from_slice(&0x20_u64.to_be_bytes());
        payload.extend_from_slice(&123_i64.to_be_bytes());
        payload.extend_from_slice(b"hello");

        match parse_copy_data_message(&payload).unwrap() {
            ReplicationStreamMessage::XLogData(x) => {
                assert_eq!(x.wal_start, 0x10);
                assert_eq!(x.wal_end, 0x20);
                assert_eq!(x.server_time_micros, 123);
                assert_eq!(x.data, b"hello");
            }
            _ => panic!("expected xlog data"),
        }
    }

    #[test]
    fn parse_copy_data_xlog_data_rejects_wal_end_before_start() {
        let mut payload = Vec::new();
        payload.push(b'w');
        payload.extend_from_slice(&0x20_u64.to_be_bytes());
        payload.extend_from_slice(&0x10_u64.to_be_bytes());
        payload.extend_from_slice(&123_i64.to_be_bytes());
        let err = parse_copy_data_message(&payload).unwrap_err();
        assert!(err.to_string().contains("wal_end"));
    }

    #[test]
    fn parse_copy_data_xlog_data_rejects_oversized_data() {
        let mut payload = Vec::with_capacity(25 + MAX_REPLICATION_XLOGDATA_BYTES + 1);
        payload.push(b'w');
        payload.extend_from_slice(&0x10_u64.to_be_bytes());
        payload.extend_from_slice(&0x20_u64.to_be_bytes());
        payload.extend_from_slice(&123_i64.to_be_bytes());
        payload.extend(std::iter::repeat_n(0u8, MAX_REPLICATION_XLOGDATA_BYTES + 1));
        let err = parse_copy_data_message(&payload).unwrap_err();
        assert!(err.to_string().contains("payload too large"));
    }

    #[test]
    fn parse_copy_data_keepalive() {
        let mut payload = Vec::new();
        payload.push(b'k');
        payload.extend_from_slice(&0xAB_u64.to_be_bytes());
        payload.extend_from_slice(&456_i64.to_be_bytes());
        payload.push(1);

        match parse_copy_data_message(&payload).unwrap() {
            ReplicationStreamMessage::Keepalive(k) => {
                assert_eq!(k.wal_end, 0xAB);
                assert_eq!(k.server_time_micros, 456);
                assert!(k.reply_requested);
            }
            _ => panic!("expected keepalive"),
        }
    }

    #[test]
    fn parse_copy_data_keepalive_rejects_invalid_reply_requested() {
        let mut payload = Vec::new();
        payload.push(b'k');
        payload.extend_from_slice(&0xAB_u64.to_be_bytes());
        payload.extend_from_slice(&456_i64.to_be_bytes());
        payload.push(2);
        let err = parse_copy_data_message(&payload).unwrap_err();
        assert!(err.to_string().contains("reply_requested must be 0 or 1"));
    }

    #[test]
    fn parse_copy_data_unknown_tag_rejected() {
        let payload = vec![b'x', 1, 2, 3];
        let err = parse_copy_data_message(&payload).unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported replication CopyData tag")
        );
    }

    #[test]
    fn build_standby_status_update_payload_layout() {
        let payload = build_standby_status_update_payload(1, 2, 3, true);
        assert_eq!(payload.len(), 34);
        assert_eq!(payload[0], b'r');
        assert_eq!(u64::from_be_bytes(payload[1..9].try_into().unwrap()), 1);
        assert_eq!(u64::from_be_bytes(payload[9..17].try_into().unwrap()), 2);
        assert_eq!(u64::from_be_bytes(payload[17..25].try_into().unwrap()), 3);
        assert_eq!(payload[33], 1);
    }
}
