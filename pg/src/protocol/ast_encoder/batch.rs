//! Batch and wire protocol encoding.
//!
//! Extended Query protocol construction and batch operations.

use bytes::BytesMut;
use qail_core::ast::{Action, AggregateFunc, Expr, Qail};

use super::dml::{encode_delete, encode_export, encode_insert, encode_select, encode_update};

use crate::protocol::EncodeError;

/// Build Extended Query protocol: Parse + Bind + Describe + Execute + Sync.
/// Includes Describe to get RowDescription (column metadata).
pub fn build_extended_query(sql: &[u8], params: &[Option<Vec<u8>>]) -> Result<BytesMut, EncodeError> {
    if params.len() > i16::MAX as usize {
        return Err(EncodeError::TooManyParameters(params.len()));
    }

    let params_size: usize = params
        .iter()
        .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
        .sum();
    // Extra 6 bytes for Describe message ('D' + len + 'P' + null)
    let total_size = 9 + sql.len() + 13 + params_size + 6 + 10 + 5;

    let mut buf = BytesMut::with_capacity(total_size);
    build_extended_query_into(&mut buf, sql, params)?;
    Ok(buf)
}

/// Build Extended Query protocol into a CALLER-PROVIDED buffer (ZERO-ALLOC).
/// Clears the buffer first but keeps capacity for reuse.
/// Includes Describe to get RowDescription (column metadata).
pub fn build_extended_query_into(buf: &mut BytesMut, sql: &[u8], params: &[Option<Vec<u8>>]) -> Result<(), EncodeError> {
    if params.len() > i16::MAX as usize {
        return Err(EncodeError::TooManyParameters(params.len()));
    }

    let params_size: usize = params
        .iter()
        .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
        .sum();
    // Extra 6 bytes for Describe message ('D' + len + 'P' + null)
    let total_size = 9 + sql.len() + 13 + params_size + 6 + 10 + 5;

    buf.clear();
    buf.reserve(total_size);

    // ===== PARSE =====
    buf.extend_from_slice(b"P");
    let parse_len = (1 + sql.len() + 1 + 2 + 4) as i32;
    buf.extend_from_slice(&parse_len.to_be_bytes());
    buf.extend_from_slice(&[0]); // Unnamed statement
    buf.extend_from_slice(sql);
    buf.extend_from_slice(&[0]); // Null terminator
    buf.extend_from_slice(&0i16.to_be_bytes()); // No param types

    // ===== BIND =====
    buf.extend_from_slice(b"B");
    let bind_len = (1 + 1 + 2 + 2 + params_size + 2 + 4) as i32;
    buf.extend_from_slice(&bind_len.to_be_bytes());
    buf.extend_from_slice(&[0]); // Unnamed portal
    buf.extend_from_slice(&[0]); // Unnamed statement
    buf.extend_from_slice(&0i16.to_be_bytes()); // Format codes
    buf.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        match param {
            None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(data) => {
                buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                buf.extend_from_slice(data);
            }
        }
    }
    buf.extend_from_slice(&0i16.to_be_bytes()); // Result format

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
    let mut total_buf = BytesMut::with_capacity(cmds.len() * 256);

    for cmd in cmds {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        match cmd.action {
            Action::Get => encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => encode_delete(cmd, &mut sql_buf, &mut params),
            Action::Cnt => {
                let mut count_cmd = cmd.clone();
                count_cmd.action = Action::Get;
                count_cmd.columns = vec![Expr::Aggregate {
                    col: "*".to_string(),
                    func: AggregateFunc::Count,
                    distinct: false,
                    filter: None,
                    alias: None,
                }];
                encode_select(&count_cmd, &mut sql_buf, &mut params)
            }
            Action::Export => encode_export(cmd, &mut sql_buf, &mut params),
            // DDL/utility actions — delegate to encode_cmd_sql_to which
            // handles 24 DDL actions (Make, Drop, Index, Alter*, etc.)
            _ => {
                super::AstEncoder::encode_cmd_sql_to(cmd, &mut sql_buf, &mut params)?;
                Ok(())
            }
        }.ok();

        let sql_bytes = sql_buf.freeze();
        let params_size: usize = params
            .iter()
            .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
            .sum();

        // PARSE
        total_buf.extend_from_slice(b"P");
        let parse_len = (1 + sql_bytes.len() + 1 + 2 + 4) as i32;
        total_buf.extend_from_slice(&parse_len.to_be_bytes());
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&sql_bytes);
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&0i16.to_be_bytes());

        // BIND
        total_buf.extend_from_slice(b"B");
        let bind_len = (1 + 1 + 2 + 2 + params_size + 2 + 4) as i32;
        total_buf.extend_from_slice(&bind_len.to_be_bytes());
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&[0]);
        total_buf.extend_from_slice(&0i16.to_be_bytes());
        total_buf.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for param in &params {
            match param {
                None => total_buf.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(data) => {
                    total_buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    total_buf.extend_from_slice(data);
                }
            }
        }
        total_buf.extend_from_slice(&0i16.to_be_bytes());

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
            Action::Cnt => {
                let mut count_cmd = cmd.clone();
                count_cmd.action = Action::Get;
                count_cmd.columns = vec![Expr::Aggregate {
                    col: "*".to_string(),
                    func: AggregateFunc::Count,
                    distinct: false,
                    filter: None,
                    alias: None,
                }];
                encode_select(&count_cmd, &mut total_buf, &mut params)
            }
            Action::Export => encode_export(cmd, &mut total_buf, &mut params),
            // DDL/utility — delegate to encode_cmd_sql_to
            _ => {
                super::AstEncoder::encode_cmd_sql_to(cmd, &mut total_buf, &mut params)?;
                Ok(())
            }
        }.ok();
        total_buf.extend_from_slice(b";");
    }

    total_buf.extend_from_slice(&[0]);

    let msg_len = (total_buf.len() - 1) as i32;
    total_buf[1..5].copy_from_slice(&msg_len.to_be_bytes());

    Ok(total_buf)
}
