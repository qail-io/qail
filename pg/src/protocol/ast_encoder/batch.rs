//! Batch and wire protocol encoding.
//!
//! Extended Query protocol construction and batch operations.

use bytes::BytesMut;
use qail_core::ast::{Action, Qail};

use super::dml::{
    encode_count, encode_delete, encode_export, encode_insert, encode_select, encode_update,
};

use crate::protocol::EncodeError;

#[inline(always)]
fn validate_sql_bytes(sql: &[u8]) -> Result<(), EncodeError> {
    if sql.contains(&0) {
        return Err(EncodeError::NullByte);
    }
    Ok(())
}

#[inline(always)]
fn checked_i16_count(count: usize) -> Result<i16, EncodeError> {
    i16::try_from(count).map_err(|_| EncodeError::TooManyParameters(count))
}

#[inline(always)]
fn checked_i32_len(len: usize) -> Result<i32, EncodeError> {
    i32::try_from(len).map_err(|_| EncodeError::MessageTooLarge(len))
}

#[inline(always)]
fn checked_wire_len(content_len: usize) -> Result<i32, EncodeError> {
    let total = content_len
        .checked_add(4)
        .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
    checked_i32_len(total)
}

#[inline(always)]
fn params_wire_size(params: &[Option<Vec<u8>>]) -> Result<usize, EncodeError> {
    params.iter().try_fold(0usize, |acc, p| {
        let data_len = p.as_ref().map_or(0, |v| v.len());
        if data_len > i32::MAX as usize {
            return Err(EncodeError::MessageTooLarge(data_len));
        }
        let field_size = 4usize
            .checked_add(data_len)
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
        acc.checked_add(field_size)
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))
    })
}

#[inline(always)]
fn result_format_wire_len(result_format: i16) -> usize {
    if result_format == 0 { 2 } else { 4 }
}

#[inline(always)]
fn write_result_formats(buf: &mut BytesMut, result_format: i16) {
    if result_format == 0 {
        buf.extend_from_slice(&0i16.to_be_bytes());
    } else {
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf.extend_from_slice(&result_format.to_be_bytes());
    }
}

/// Build Extended Query protocol: Parse + Bind + Describe + Execute + Sync.
/// Includes Describe to get RowDescription (column metadata).
pub fn build_extended_query(
    sql: &[u8],
    params: &[Option<Vec<u8>>],
) -> Result<BytesMut, EncodeError> {
    build_extended_query_with_result_format(sql, params, 0)
}

/// Build Extended Query protocol with explicit result-column format.
/// `result_format`: 0 = text, 1 = binary.
pub fn build_extended_query_with_result_format(
    sql: &[u8],
    params: &[Option<Vec<u8>>],
    result_format: i16,
) -> Result<BytesMut, EncodeError> {
    if params.len() > i16::MAX as usize {
        return Err(EncodeError::TooManyParameters(params.len()));
    }
    validate_sql_bytes(sql)?;

    let params_size = params_wire_size(params)?;
    let result_formats_size = result_format_wire_len(result_format);
    // Extra 6 bytes for Describe message ('D' + len + 'P' + null)
    let total_size = 9usize
        .checked_add(sql.len())
        .and_then(|v| v.checked_add(11))
        .and_then(|v| v.checked_add(params_size))
        .and_then(|v| v.checked_add(result_formats_size))
        .and_then(|v| v.checked_add(6))
        .and_then(|v| v.checked_add(10))
        .and_then(|v| v.checked_add(5))
        .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;

    let mut buf = BytesMut::with_capacity(total_size);
    build_extended_query_into_with_result_format(&mut buf, sql, params, result_format)?;
    Ok(buf)
}

/// Build Extended Query protocol into a CALLER-PROVIDED buffer (ZERO-ALLOC).
/// Clears the buffer first but keeps capacity for reuse.
/// Includes Describe to get RowDescription (column metadata).
///
/// # Arguments
///
/// * `buf` — Caller-owned buffer to write protocol messages into.
/// * `sql` — SQL query bytes.
/// * `params` — Bind parameter values; `None` entries encode as SQL NULL.
pub fn build_extended_query_into(
    buf: &mut BytesMut,
    sql: &[u8],
    params: &[Option<Vec<u8>>],
) -> Result<(), EncodeError> {
    build_extended_query_into_with_result_format(buf, sql, params, 0)
}

/// Build Extended Query protocol into caller buffer with explicit result format.
/// `result_format`: 0 = text, 1 = binary.
pub fn build_extended_query_into_with_result_format(
    buf: &mut BytesMut,
    sql: &[u8],
    params: &[Option<Vec<u8>>],
    result_format: i16,
) -> Result<(), EncodeError> {
    if params.len() > i16::MAX as usize {
        return Err(EncodeError::TooManyParameters(params.len()));
    }
    validate_sql_bytes(sql)?;

    let params_size = params_wire_size(params)?;
    let result_formats_size = result_format_wire_len(result_format);
    // Extra 6 bytes for Describe message ('D' + len + 'P' + null)
    let total_size = 9usize
        .checked_add(sql.len())
        .and_then(|v| v.checked_add(11))
        .and_then(|v| v.checked_add(params_size))
        .and_then(|v| v.checked_add(result_formats_size))
        .and_then(|v| v.checked_add(6))
        .and_then(|v| v.checked_add(10))
        .and_then(|v| v.checked_add(5))
        .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
    let param_count = checked_i16_count(params.len())?;

    buf.clear();
    buf.reserve(total_size);

    // ===== PARSE =====
    buf.extend_from_slice(b"P");
    let parse_content_len = 1usize
        .checked_add(sql.len())
        .and_then(|v| v.checked_add(1))
        .and_then(|v| v.checked_add(2))
        .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
    let parse_len = checked_wire_len(parse_content_len)?;
    buf.extend_from_slice(&parse_len.to_be_bytes());
    buf.extend_from_slice(&[0]); // Unnamed statement
    buf.extend_from_slice(sql);
    buf.extend_from_slice(&[0]); // Null terminator
    buf.extend_from_slice(&0i16.to_be_bytes()); // No param types

    // ===== BIND =====
    buf.extend_from_slice(b"B");
    let bind_content_len = 1usize
        .checked_add(1)
        .and_then(|v| v.checked_add(2))
        .and_then(|v| v.checked_add(2))
        .and_then(|v| v.checked_add(params_size))
        .and_then(|v| v.checked_add(result_formats_size))
        .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
    let bind_len = checked_wire_len(bind_content_len)?;
    buf.extend_from_slice(&bind_len.to_be_bytes());
    buf.extend_from_slice(&[0]); // Unnamed portal
    buf.extend_from_slice(&[0]); // Unnamed statement
    buf.extend_from_slice(&0i16.to_be_bytes()); // Format codes
    buf.extend_from_slice(&param_count.to_be_bytes());
    for param in params {
        match param {
            None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(data) => {
                let data_len = checked_i32_len(data.len())?;
                buf.extend_from_slice(&data_len.to_be_bytes());
                buf.extend_from_slice(data);
            }
        }
    }
    write_result_formats(buf, result_format);

    // ===== DESCRIBE (Portal) =====
    // Send Describe to get RowDescription with column names
    buf.extend_from_slice(b"D");
    buf.extend_from_slice(&6i32.to_be_bytes()); // Length: 4 + 1 + 1
    buf.extend_from_slice(b"P"); // Describe Portal (not Statement)
    buf.extend_from_slice(&[0]); // Unnamed portal

    // ===== EXECUTE =====
    buf.extend_from_slice(b"E");
    buf.extend_from_slice(&9i32.to_be_bytes());
    buf.extend_from_slice(&[0]); // Unnamed portal
    buf.extend_from_slice(&0i32.to_be_bytes()); // Unlimited rows

    // ===== SYNC =====
    buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);

    Ok(())
}

/// Encode multiple Qails as a pipeline batch.
///
/// DML actions (Get/Add/Set/Del/Cnt/Export) use dedicated encoders.
/// DDL actions (Make/Drop/Index/Alter/etc.) fall through to `encode_cmd_sql_to`,
/// which already supports 24 DDL actions.
pub fn encode_batch(cmds: &[Qail]) -> Result<BytesMut, EncodeError> {
    encode_batch_with_result_format(cmds, 0)
}

/// Encode multiple Qails as a pipeline batch with explicit result-column format.
/// `result_format`: 0 = text, 1 = binary.
pub fn encode_batch_with_result_format(
    cmds: &[Qail],
    result_format: i16,
) -> Result<BytesMut, EncodeError> {
    let mut total_buf = BytesMut::with_capacity(cmds.len() * 256);
    let result_formats_size = result_format_wire_len(result_format);

    for cmd in cmds {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        match cmd.action {
            Action::Get => encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => encode_delete(cmd, &mut sql_buf, &mut params),
            Action::Cnt => encode_count(cmd, &mut sql_buf, &mut params),
            Action::Export => encode_export(cmd, &mut sql_buf, &mut params),
            // DDL/utility actions — delegate to encode_cmd_sql_to which
            // handles 24 DDL actions (Make, Drop, Index, Alter*, etc.)
            _ => {
                super::AstEncoder::encode_cmd_sql_to(cmd, &mut sql_buf, &mut params)?;
                Ok(())
            }
        }?;

        let sql_bytes = sql_buf.freeze();
        validate_sql_bytes(&sql_bytes)?;
        let params_size = params_wire_size(&params)?;
        let param_count = checked_i16_count(params.len())?;

        // PARSE
        total_buf.extend_from_slice(b"P");
        let parse_content_len = 1usize
            .checked_add(sql_bytes.len())
            .and_then(|v| v.checked_add(1))
            .and_then(|v| v.checked_add(2))
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
        let parse_len = checked_wire_len(parse_content_len)?;
        total_buf.extend_from_slice(&parse_len.to_be_bytes());
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&sql_bytes);
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&0i16.to_be_bytes());

        // BIND
        total_buf.extend_from_slice(b"B");
        let bind_content_len = 1usize
            .checked_add(1)
            .and_then(|v| v.checked_add(2))
            .and_then(|v| v.checked_add(2))
            .and_then(|v| v.checked_add(params_size))
            .and_then(|v| v.checked_add(result_formats_size))
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
        let bind_len = checked_wire_len(bind_content_len)?;
        total_buf.extend_from_slice(&bind_len.to_be_bytes());
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&0i16.to_be_bytes());
        total_buf.extend_from_slice(&param_count.to_be_bytes());
        for param in &params {
            match param {
                None => total_buf.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(data) => {
                    let data_len = checked_i32_len(data.len())?;
                    total_buf.extend_from_slice(&data_len.to_be_bytes());
                    total_buf.extend_from_slice(data);
                }
            }
        }
        write_result_formats(&mut total_buf, result_format);

        // EXECUTE
        total_buf.extend_from_slice(b"E");
        total_buf.extend_from_slice(&9i32.to_be_bytes());
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&0i32.to_be_bytes());
    }

    // Single SYNC at the end
    total_buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);

    Ok(total_buf)
}

/// Encode multiple Qails using Simple Query Protocol.
///
/// DDL/utility actions fall through to `encode_cmd_sql_to`.
pub fn encode_batch_simple(cmds: &[Qail]) -> Result<BytesMut, EncodeError> {
    let estimated_sql_size = cmds.len() * 48;
    let mut total_buf = BytesMut::with_capacity(5 + estimated_sql_size + 1);

    total_buf.extend_from_slice(&[b'Q', 0, 0, 0, 0]);

    let mut params: Vec<Option<Vec<u8>>> = Vec::new();

    for cmd in cmds {
        params.clear();

        match cmd.action {
            Action::Get => encode_select(cmd, &mut total_buf, &mut params),
            Action::Add => encode_insert(cmd, &mut total_buf, &mut params),
            Action::Set => encode_update(cmd, &mut total_buf, &mut params),
            Action::Del => encode_delete(cmd, &mut total_buf, &mut params),
            Action::Cnt => encode_count(cmd, &mut total_buf, &mut params),
            Action::Export => encode_export(cmd, &mut total_buf, &mut params),
            // DDL/utility — delegate to encode_cmd_sql_to
            _ => {
                super::AstEncoder::encode_cmd_sql_to(cmd, &mut total_buf, &mut params)?;
                Ok(())
            }
        }?;
        total_buf.extend_from_slice(b";");
    }

    if total_buf[5..].contains(&0) {
        return Err(EncodeError::NullByte);
    }

    total_buf.extend_from_slice(&[0]);

    let msg_len = i32::try_from(total_buf.len() - 1)
        .map_err(|_| EncodeError::MessageTooLarge(total_buf.len() - 1))?;
    total_buf[1..5].copy_from_slice(&msg_len.to_be_bytes());

    Ok(total_buf)
}
