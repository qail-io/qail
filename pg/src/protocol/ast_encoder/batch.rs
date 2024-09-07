//! Batch and wire protocol encoding.
//!
//! Extended Query protocol construction and batch operations.

use bytes::BytesMut;
use qail_core::ast::{Action, Qail};

use super::dml::{encode_delete, encode_insert, encode_select, encode_update};

/// Build Extended Query protocol: Parse + Bind + Execute + Sync.
pub fn build_extended_query(sql: &[u8], params: &[Option<Vec<u8>>]) -> BytesMut {
    let params_size: usize = params
        .iter()
        .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
        .sum();
    let total_size = 9 + sql.len() + 13 + params_size + 10 + 5;

    let mut buf = BytesMut::with_capacity(total_size);

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

    // ===== EXECUTE =====
    buf.extend_from_slice(b"E");
    buf.extend_from_slice(&9i32.to_be_bytes());
    buf.extend_from_slice(&[0]); // Unnamed portal
    buf.extend_from_slice(&0i32.to_be_bytes()); // Unlimited rows

    // ===== SYNC =====
    buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);

    buf
}

/// Encode multiple Qails as a pipeline batch.
pub fn encode_batch(cmds: &[Qail]) -> BytesMut {
    let mut total_buf = BytesMut::with_capacity(cmds.len() * 256);

    for cmd in cmds {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        match cmd.action {
            Action::Get => encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => encode_delete(cmd, &mut sql_buf, &mut params),
            _ => panic!("Unsupported action {:?} in AST-native batch encoder.", cmd.action),
        }

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

    total_buf
}

/// Encode multiple Qails using Simple Query Protocol.
pub fn encode_batch_simple(cmds: &[Qail]) -> BytesMut {
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
            _ => panic!("Unsupported action {:?}", cmd.action),
        }
        total_buf.extend_from_slice(b";");
    }

    total_buf.extend_from_slice(&[0]);

    let msg_len = (total_buf.len() - 1) as i32;
    total_buf[1..5].copy_from_slice(&msg_len.to_be_bytes());

    total_buf
}
