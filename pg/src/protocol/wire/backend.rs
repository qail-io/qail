//! BackendMessage decoder — server-to-client wire format.

use super::types::*;

/// Maximum backend frame length accepted by the decoder.
///
/// Mirrors driver-side guards to keep standalone `BackendMessage::decode`
/// usage fail-closed against oversized frames.
pub(crate) const MAX_BACKEND_FRAME_LEN: usize = 64 * 1024 * 1024;

impl BackendMessage {
    /// Decode a message from wire bytes.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), String> {
        if buf.len() < 5 {
            return Err("Buffer too short".to_string());
        }

        let msg_type = buf[0];
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

        // PG protocol: length includes itself (4 bytes), so minimum valid length is 4.
        // Anything less is a malformed message.
        if len < 4 {
            return Err(format!("Invalid message length: {} (minimum is 4)", len));
        }
        if len > MAX_BACKEND_FRAME_LEN {
            return Err(format!(
                "Message too large: {} bytes (max {})",
                len, MAX_BACKEND_FRAME_LEN
            ));
        }

        let frame_len = len
            .checked_add(1)
            .ok_or_else(|| "Message length overflow".to_string())?;

        if buf.len() < frame_len {
            return Err("Incomplete message".to_string());
        }

        let payload = &buf[5..frame_len];

        let message = match msg_type {
            b'R' => Self::decode_auth(payload)?,
            b'S' => Self::decode_parameter_status(payload)?,
            b'K' => Self::decode_backend_key(payload)?,
            b'v' => Self::decode_negotiate_protocol_version(payload)?,
            b'Z' => Self::decode_ready_for_query(payload)?,
            b'T' => Self::decode_row_description(payload)?,
            b'D' => Self::decode_data_row(payload)?,
            b'C' => Self::decode_command_complete(payload)?,
            b'E' => Self::decode_error_response(payload)?,
            b'1' => {
                if !payload.is_empty() {
                    return Err("ParseComplete must have empty payload".to_string());
                }
                BackendMessage::ParseComplete
            }
            b'2' => {
                if !payload.is_empty() {
                    return Err("BindComplete must have empty payload".to_string());
                }
                BackendMessage::BindComplete
            }
            b'3' => {
                if !payload.is_empty() {
                    return Err("CloseComplete must have empty payload".to_string());
                }
                BackendMessage::CloseComplete
            }
            b'n' => {
                if !payload.is_empty() {
                    return Err("NoData must have empty payload".to_string());
                }
                BackendMessage::NoData
            }
            b's' => {
                if !payload.is_empty() {
                    return Err("PortalSuspended must have empty payload".to_string());
                }
                BackendMessage::PortalSuspended
            }
            b't' => Self::decode_parameter_description(payload)?,
            b'G' => Self::decode_copy_in_response(payload)?,
            b'H' => Self::decode_copy_out_response(payload)?,
            b'W' => Self::decode_copy_both_response(payload)?,
            b'd' => BackendMessage::CopyData(payload.to_vec()),
            b'c' => {
                if !payload.is_empty() {
                    return Err("CopyDone must have empty payload".to_string());
                }
                BackendMessage::CopyDone
            }
            b'A' => Self::decode_notification_response(payload)?,
            b'I' => {
                if !payload.is_empty() {
                    return Err("EmptyQueryResponse must have empty payload".to_string());
                }
                BackendMessage::EmptyQueryResponse
            }
            b'N' => BackendMessage::NoticeResponse(Self::parse_error_fields(payload)?),
            _ => return Err(format!("Unknown message type: {}", msg_type as char)),
        };

        Ok((message, frame_len))
    }

    fn decode_auth(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 4 {
            return Err("Auth payload too short".to_string());
        }
        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        match auth_type {
            0 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationOk invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationOk)
            }
            2 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationKerberosV5 invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationKerberosV5)
            }
            3 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationCleartextPassword invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationCleartextPassword)
            }
            5 => {
                if payload.len() != 8 {
                    return Err("MD5 auth payload too short (need salt)".to_string());
                }
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&payload[4..8]);
                Ok(BackendMessage::AuthenticationMD5Password(salt))
            }
            6 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationSCMCredential invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationSCMCredential)
            }
            7 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationGSS invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationGSS)
            }
            8 => Ok(BackendMessage::AuthenticationGSSContinue(
                payload[4..].to_vec(),
            )),
            9 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationSSPI invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationSSPI)
            }
            10 => {
                // SASL - parse mechanism list
                let mut mechanisms = Vec::new();
                let mut pos = 4;
                while pos < payload.len() {
                    if payload[pos] == 0 {
                        break; // list terminator
                    }
                    let end = payload[pos..]
                        .iter()
                        .position(|&b| b == 0)
                        .map(|p| pos + p)
                        .ok_or("SASL mechanism list missing null terminator")?;
                    mechanisms.push(String::from_utf8_lossy(&payload[pos..end]).to_string());
                    pos = end + 1;
                }
                if pos >= payload.len() {
                    return Err("SASL mechanism list missing final terminator".to_string());
                }
                if pos + 1 != payload.len() {
                    return Err("SASL mechanism list has trailing bytes".to_string());
                }
                if mechanisms.is_empty() {
                    return Err("SASL mechanism list is empty".to_string());
                }
                Ok(BackendMessage::AuthenticationSASL(mechanisms))
            }
            11 => {
                // SASL Continue - server challenge
                Ok(BackendMessage::AuthenticationSASLContinue(
                    payload[4..].to_vec(),
                ))
            }
            12 => {
                // SASL Final - server signature
                Ok(BackendMessage::AuthenticationSASLFinal(
                    payload[4..].to_vec(),
                ))
            }
            _ => Err(format!("Unknown auth type: {}", auth_type)),
        }
    }

    fn decode_parameter_status(payload: &[u8]) -> Result<Self, String> {
        let name_end = payload
            .iter()
            .position(|&b| b == 0)
            .ok_or("ParameterStatus missing name terminator")?;
        let value_start = name_end + 1;
        if value_start > payload.len() {
            return Err("ParameterStatus missing value".to_string());
        }
        let value_end_rel = payload[value_start..]
            .iter()
            .position(|&b| b == 0)
            .ok_or("ParameterStatus missing value terminator")?;
        let value_end = value_start + value_end_rel;
        if value_end + 1 != payload.len() {
            return Err("ParameterStatus has trailing bytes".to_string());
        }
        Ok(BackendMessage::ParameterStatus {
            name: String::from_utf8_lossy(&payload[..name_end]).to_string(),
            value: String::from_utf8_lossy(&payload[value_start..value_end]).to_string(),
        })
    }

    fn decode_backend_key(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 8 {
            return Err("BackendKeyData payload too short".to_string());
        }
        let key_len = payload.len() - 4;
        if !(4..=256).contains(&key_len) {
            return Err(format!(
                "BackendKeyData invalid secret key length: {} (expected 4..=256)",
                key_len
            ));
        }
        Ok(BackendMessage::BackendKeyData {
            process_id: i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]),
            secret_key: payload[4..].to_vec(),
        })
    }

    fn decode_negotiate_protocol_version(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 8 {
            return Err("NegotiateProtocolVersion payload too short".to_string());
        }

        let newest_minor_supported =
            i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        if newest_minor_supported < 0 {
            return Err("NegotiateProtocolVersion newest_minor_supported is negative".to_string());
        }

        let unrecognized_count =
            i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
        if unrecognized_count < 0 {
            return Err(
                "NegotiateProtocolVersion unrecognized option count is negative".to_string(),
            );
        }
        let unrecognized_count = unrecognized_count as usize;

        let mut options = Vec::with_capacity(unrecognized_count);
        let mut pos = 8usize;
        for _ in 0..unrecognized_count {
            if pos >= payload.len() {
                return Err("NegotiateProtocolVersion missing option string terminator".to_string());
            }
            let rel_end = payload[pos..]
                .iter()
                .position(|&b| b == 0)
                .ok_or("NegotiateProtocolVersion option missing null terminator")?;
            let end = pos + rel_end;
            options.push(String::from_utf8_lossy(&payload[pos..end]).to_string());
            pos = end + 1;
        }

        if pos != payload.len() {
            return Err("NegotiateProtocolVersion has trailing bytes".to_string());
        }

        Ok(BackendMessage::NegotiateProtocolVersion {
            newest_minor_supported,
            unrecognized_protocol_options: options,
        })
    }

    fn decode_ready_for_query(payload: &[u8]) -> Result<Self, String> {
        if payload.len() != 1 {
            return Err("ReadyForQuery payload empty".to_string());
        }
        let status = match payload[0] {
            b'I' => TransactionStatus::Idle,
            b'T' => TransactionStatus::InBlock,
            b'E' => TransactionStatus::Failed,
            _ => return Err("Unknown transaction status".to_string()),
        };
        Ok(BackendMessage::ReadyForQuery(status))
    }

    fn decode_row_description(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 2 {
            return Err("RowDescription payload too short".to_string());
        }

        let raw_count = i16::from_be_bytes([payload[0], payload[1]]);
        if raw_count < 0 {
            return Err(format!("RowDescription invalid field count: {}", raw_count));
        }
        let field_count = raw_count as usize;
        let mut fields = Vec::with_capacity(field_count);
        let mut pos = 2;

        for _ in 0..field_count {
            // Field name (null-terminated string)
            let name_end = payload[pos..]
                .iter()
                .position(|&b| b == 0)
                .ok_or("Missing null terminator in field name")?;
            let name = String::from_utf8_lossy(&payload[pos..pos + name_end]).to_string();
            pos += name_end + 1; // Skip null terminator

            // Ensure we have enough bytes for the fixed fields
            if pos + 18 > payload.len() {
                return Err("RowDescription field truncated".to_string());
            }

            let table_oid = u32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;

            let column_attr = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            pos += 2;

            let type_oid = u32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;

            let type_size = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            pos += 2;

            let type_modifier = i32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;

            let format = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            if !(0..=1).contains(&format) {
                return Err(format!("RowDescription invalid format code: {}", format));
            }
            pos += 2;

            fields.push(FieldDescription {
                name,
                table_oid,
                column_attr,
                type_oid,
                type_size,
                type_modifier,
                format,
            });
        }

        if pos != payload.len() {
            return Err("RowDescription has trailing bytes".to_string());
        }

        Ok(BackendMessage::RowDescription(fields))
    }

    fn decode_data_row(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 2 {
            return Err("DataRow payload too short".to_string());
        }

        let raw_count = i16::from_be_bytes([payload[0], payload[1]]);
        if raw_count < 0 {
            return Err(format!("DataRow invalid column count: {}", raw_count));
        }
        let column_count = raw_count as usize;
        // Sanity check: each column needs at least 4 bytes (length field)
        if column_count > (payload.len() - 2) / 4 + 1 {
            return Err(format!(
                "DataRow claims {} columns but payload is only {} bytes",
                column_count,
                payload.len()
            ));
        }
        let mut columns = Vec::with_capacity(column_count);
        let mut pos = 2;

        for _ in 0..column_count {
            if pos + 4 > payload.len() {
                return Err("DataRow truncated".to_string());
            }

            let len = i32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;

            if len == -1 {
                // NULL value
                columns.push(None);
            } else {
                if len < -1 {
                    return Err(format!("DataRow invalid column length: {}", len));
                }
                let len = len as usize;
                if len > payload.len().saturating_sub(pos) {
                    return Err("DataRow column data truncated".to_string());
                }
                let data = payload[pos..pos + len].to_vec();
                pos += len;
                columns.push(Some(data));
            }
        }

        if pos != payload.len() {
            return Err("DataRow has trailing bytes".to_string());
        }

        Ok(BackendMessage::DataRow(columns))
    }

    fn decode_command_complete(payload: &[u8]) -> Result<Self, String> {
        if payload.last().copied() != Some(0) {
            return Err("CommandComplete missing null terminator".to_string());
        }
        let tag_bytes = &payload[..payload.len() - 1];
        if tag_bytes.contains(&0) {
            return Err("CommandComplete contains interior null byte".to_string());
        }
        let tag = String::from_utf8_lossy(tag_bytes).to_string();
        Ok(BackendMessage::CommandComplete(tag))
    }

    fn decode_error_response(payload: &[u8]) -> Result<Self, String> {
        Ok(BackendMessage::ErrorResponse(Self::parse_error_fields(
            payload,
        )?))
    }

    fn parse_error_fields(payload: &[u8]) -> Result<ErrorFields, String> {
        if payload.last().copied() != Some(0) {
            return Err("ErrorResponse missing final terminator".to_string());
        }
        let mut fields = ErrorFields::default();
        let mut i = 0;
        while i < payload.len() && payload[i] != 0 {
            let field_type = payload[i];
            i += 1;
            let end = payload[i..]
                .iter()
                .position(|&b| b == 0)
                .map(|p| p + i)
                .ok_or("ErrorResponse field missing null terminator")?;
            let value = String::from_utf8_lossy(&payload[i..end]).to_string();
            i = end + 1;

            match field_type {
                b'S' => fields.severity = value,
                b'C' => fields.code = value,
                b'M' => fields.message = value,
                b'D' => fields.detail = Some(value),
                b'H' => fields.hint = Some(value),
                _ => {}
            }
        }
        if i + 1 != payload.len() {
            return Err("ErrorResponse has trailing bytes after terminator".to_string());
        }
        Ok(fields)
    }

    fn decode_parameter_description(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 2 {
            return Err("ParameterDescription payload too short".to_string());
        }
        let raw_count = i16::from_be_bytes([payload[0], payload[1]]);
        if raw_count < 0 {
            return Err(format!("ParameterDescription invalid count: {}", raw_count));
        }
        let count = raw_count as usize;
        let expected_len = 2 + count * 4;
        if payload.len() < expected_len {
            return Err(format!(
                "ParameterDescription truncated: expected {} bytes, got {}",
                expected_len,
                payload.len()
            ));
        }
        let mut oids = Vec::with_capacity(count);
        let mut pos = 2;
        for _ in 0..count {
            oids.push(u32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]));
            pos += 4;
        }
        if pos != payload.len() {
            return Err("ParameterDescription has trailing bytes".to_string());
        }
        Ok(BackendMessage::ParameterDescription(oids))
    }

    fn decode_copy_in_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 3 {
            return Err("CopyInResponse payload too short".to_string());
        }
        let format = payload[0];
        if format > 1 {
            return Err(format!(
                "CopyInResponse invalid overall format code: {}",
                format
            ));
        }
        let num_columns = if payload.len() >= 3 {
            let raw = i16::from_be_bytes([payload[1], payload[2]]);
            if raw < 0 {
                return Err(format!(
                    "CopyInResponse invalid negative column count: {}",
                    raw
                ));
            }
            raw as usize
        } else {
            0
        };
        let mut column_formats = Vec::with_capacity(num_columns);
        let mut pos = 3usize;
        for _ in 0..num_columns {
            if pos + 2 > payload.len() {
                return Err("CopyInResponse truncated column format list".to_string());
            }
            let raw = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            if !(0..=1).contains(&raw) {
                return Err(format!("CopyInResponse invalid format code: {}", raw));
            }
            column_formats.push(raw as u8);
            pos += 2;
        }
        if pos != payload.len() {
            return Err("CopyInResponse has trailing bytes".to_string());
        }
        Ok(BackendMessage::CopyInResponse {
            format,
            column_formats,
        })
    }

    fn decode_copy_out_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 3 {
            return Err("CopyOutResponse payload too short".to_string());
        }
        let format = payload[0];
        if format > 1 {
            return Err(format!(
                "CopyOutResponse invalid overall format code: {}",
                format
            ));
        }
        let num_columns = if payload.len() >= 3 {
            let raw = i16::from_be_bytes([payload[1], payload[2]]);
            if raw < 0 {
                return Err(format!(
                    "CopyOutResponse invalid negative column count: {}",
                    raw
                ));
            }
            raw as usize
        } else {
            0
        };
        let mut column_formats = Vec::with_capacity(num_columns);
        let mut pos = 3usize;
        for _ in 0..num_columns {
            if pos + 2 > payload.len() {
                return Err("CopyOutResponse truncated column format list".to_string());
            }
            let raw = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            if !(0..=1).contains(&raw) {
                return Err(format!("CopyOutResponse invalid format code: {}", raw));
            }
            column_formats.push(raw as u8);
            pos += 2;
        }
        if pos != payload.len() {
            return Err("CopyOutResponse has trailing bytes".to_string());
        }
        Ok(BackendMessage::CopyOutResponse {
            format,
            column_formats,
        })
    }

    fn decode_copy_both_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 3 {
            return Err("CopyBothResponse payload too short".to_string());
        }
        let format = payload[0];
        if format > 1 {
            return Err(format!(
                "CopyBothResponse invalid overall format code: {}",
                format
            ));
        }
        let num_columns = if payload.len() >= 3 {
            let raw = i16::from_be_bytes([payload[1], payload[2]]);
            if raw < 0 {
                return Err(format!(
                    "CopyBothResponse invalid negative column count: {}",
                    raw
                ));
            }
            raw as usize
        } else {
            0
        };
        let mut column_formats = Vec::with_capacity(num_columns);
        let mut pos = 3usize;
        for _ in 0..num_columns {
            if pos + 2 > payload.len() {
                return Err("CopyBothResponse truncated column format list".to_string());
            }
            let raw = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            if !(0..=1).contains(&raw) {
                return Err(format!("CopyBothResponse invalid format code: {}", raw));
            }
            column_formats.push(raw as u8);
            pos += 2;
        }
        if pos != payload.len() {
            return Err("CopyBothResponse has trailing bytes".to_string());
        }
        Ok(BackendMessage::CopyBothResponse {
            format,
            column_formats,
        })
    }

    fn decode_notification_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 6 {
            // Minimum: 4 (process_id) + 1 (channel NUL) + 1 (payload NUL)
            return Err("NotificationResponse too short".to_string());
        }
        let process_id = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

        // Channel name (null-terminated)
        let mut i = 4;
        let remaining = payload.get(i..).unwrap_or(&[]);
        let channel_end = remaining
            .iter()
            .position(|&b| b == 0)
            .ok_or("NotificationResponse: missing channel null terminator")?;
        let channel = String::from_utf8_lossy(&remaining[..channel_end]).to_string();
        i += channel_end + 1;

        // Payload (null-terminated)
        let remaining = payload.get(i..).unwrap_or(&[]);
        let payload_end = remaining
            .iter()
            .position(|&b| b == 0)
            .ok_or("NotificationResponse: missing payload null terminator")?;
        let notification_payload = String::from_utf8_lossy(&remaining[..payload_end]).to_string();
        if i + payload_end + 1 != payload.len() {
            return Err("NotificationResponse has trailing bytes".to_string());
        }

        Ok(BackendMessage::NotificationResponse {
            process_id,
            channel,
            payload: notification_payload,
        })
    }
}
