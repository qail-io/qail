//! QAIL wire codec (parser-friendly, serde-free for AST payloads).
//!
//! This module provides compact encodings that round-trip through the
//! canonical QAIL text formatter + parser, so consumers can transport AST
//! commands without requiring serde derives on AST types.

use crate::ast::Qail;

const CMD_TEXT_MAGIC: &str = "QAIL-CMD/1";
const CMDS_TEXT_MAGIC: &str = "QAIL-CMDS/1";
const CMD_BIN_MAGIC: [u8; 4] = *b"QWB1";

/// Encode one command into versioned text wire format.
pub fn encode_cmd_text(cmd: &Qail) -> String {
    let payload = cmd.to_string();
    let mut out = String::with_capacity(CMD_TEXT_MAGIC.len() + payload.len() + 32);
    out.push_str(CMD_TEXT_MAGIC);
    out.push('\n');
    out.push_str(&payload.len().to_string());
    out.push('\n');
    out.push_str(&payload);
    out
}

/// Decode one command from text wire format.
///
/// Also accepts raw QAIL query text as fallback for convenience.
pub fn decode_cmd_text(input: &str) -> Result<Qail, String> {
    let bytes = input.as_bytes();
    let mut idx = 0usize;

    let Ok(magic) = read_line(bytes, &mut idx) else {
        return crate::parse(input).map_err(|e| e.to_string());
    };

    if magic != CMD_TEXT_MAGIC {
        return crate::parse(input).map_err(|e| e.to_string());
    }

    let len_line = read_line(bytes, &mut idx)?;
    let payload_len = parse_usize("payload length", len_line)?;
    let payload = read_exact_utf8(bytes, &mut idx, payload_len)?;
    if idx != bytes.len() {
        return Err("trailing bytes after command payload".to_string());
    }

    crate::parse(payload).map_err(|e| e.to_string())
}

/// Encode multiple commands into versioned text wire format.
pub fn encode_cmds_text(cmds: &[Qail]) -> String {
    let mut out = String::new();
    out.push_str(CMDS_TEXT_MAGIC);
    out.push('\n');
    out.push_str(&cmds.len().to_string());
    out.push('\n');

    for cmd in cmds {
        let payload = cmd.to_string();
        out.push_str(&payload.len().to_string());
        out.push('\n');
        out.push_str(&payload);
    }

    out
}

/// Decode multiple commands from text wire format.
pub fn decode_cmds_text(input: &str) -> Result<Vec<Qail>, String> {
    let bytes = input.as_bytes();
    let mut idx = 0usize;

    let magic = read_line(bytes, &mut idx)?;
    if magic != CMDS_TEXT_MAGIC {
        return Err(format!(
            "invalid wire magic: expected {CMDS_TEXT_MAGIC}, got {magic}"
        ));
    }

    let count_line = read_line(bytes, &mut idx)?;
    let count = parse_usize("command count", count_line)?;
    let mut out = Vec::with_capacity(count);

    for _ in 0..count {
        let len_line = read_line(bytes, &mut idx)?;
        let payload_len = parse_usize("payload length", len_line)?;
        let payload = read_exact_utf8(bytes, &mut idx, payload_len)?;
        let cmd = crate::parse(payload).map_err(|e| e.to_string())?;
        out.push(cmd);
    }

    if idx != bytes.len() {
        return Err("trailing bytes after batch payload".to_string());
    }

    Ok(out)
}

/// Encode one command into compact binary wire format.
pub fn encode_cmd_binary(cmd: &Qail) -> Vec<u8> {
    let payload = cmd.to_string();
    let payload_bytes = payload.as_bytes();

    let mut out = Vec::with_capacity(8 + payload_bytes.len());
    out.extend_from_slice(&CMD_BIN_MAGIC);
    out.extend_from_slice(&(payload_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(payload_bytes);
    out
}

/// Decode one command from compact binary wire format.
///
/// Also accepts raw UTF-8 QAIL query text as fallback.
pub fn decode_cmd_binary(input: &[u8]) -> Result<Qail, String> {
    if input.len() < 8 {
        let text = std::str::from_utf8(input).map_err(|_| "invalid wire header".to_string())?;
        return crate::parse(text).map_err(|e| e.to_string());
    }

    if input[0..4] != CMD_BIN_MAGIC {
        let text = std::str::from_utf8(input).map_err(|_| "invalid wire header".to_string())?;
        return crate::parse(text).map_err(|e| e.to_string());
    }

    let len = u32::from_be_bytes([input[4], input[5], input[6], input[7]]) as usize;
    if input.len() != 8 + len {
        return Err(format!(
            "invalid payload length: header={len}, actual={}",
            input.len().saturating_sub(8)
        ));
    }

    let payload =
        std::str::from_utf8(&input[8..]).map_err(|_| "payload is not valid UTF-8".to_string())?;
    crate::parse(payload).map_err(|e| e.to_string())
}

fn read_line<'a>(bytes: &'a [u8], idx: &mut usize) -> Result<&'a str, String> {
    if *idx >= bytes.len() {
        return Err("unexpected EOF".to_string());
    }

    let start = *idx;
    while *idx < bytes.len() && bytes[*idx] != b'\n' {
        *idx += 1;
    }

    if *idx >= bytes.len() {
        return Err("unterminated header line".to_string());
    }

    let line =
        std::str::from_utf8(&bytes[start..*idx]).map_err(|_| "header is not UTF-8".to_string())?;
    *idx += 1; // consume '\n'
    Ok(line)
}

fn parse_usize(field: &str, line: &str) -> Result<usize, String> {
    line.parse::<usize>()
        .map_err(|_| format!("invalid {field}: {line}"))
}

fn read_exact_utf8<'a>(bytes: &'a [u8], idx: &mut usize, len: usize) -> Result<&'a str, String> {
    if *idx + len > bytes.len() {
        return Err("payload truncated".to_string());
    }
    let start = *idx;
    *idx += len;
    std::str::from_utf8(&bytes[start..start + len]).map_err(|_| "payload is not UTF-8".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmd_text_roundtrip() {
        let cmd = crate::ast::Qail::get("users")
            .columns(["id", "email"])
            .where_eq("active", true)
            .limit(10);

        let encoded = encode_cmd_text(&cmd);
        let decoded = decode_cmd_text(&encoded).unwrap();
        assert_eq!(decoded.to_string(), cmd.to_string());
    }

    #[test]
    fn cmd_binary_roundtrip() {
        let cmd = crate::ast::Qail::set("users")
            .set_value("active", true)
            .where_eq("id", 7);

        let encoded = encode_cmd_binary(&cmd);
        let decoded = decode_cmd_binary(&encoded).unwrap();
        assert_eq!(decoded.to_string(), cmd.to_string());
    }

    #[test]
    fn cmds_text_roundtrip() {
        let cmds = vec![
            crate::ast::Qail::get("users").columns(["id", "email"]),
            crate::ast::Qail::get("users").limit(1),
            crate::ast::Qail::del("users").where_eq("id", 99),
        ];

        let encoded = encode_cmds_text(&cmds);
        let decoded = decode_cmds_text(&encoded).unwrap();
        assert_eq!(decoded.len(), cmds.len());
        for (lhs, rhs) in decoded.iter().zip(cmds.iter()) {
            assert_eq!(lhs.to_string(), rhs.to_string());
        }
    }

    #[test]
    fn decode_cmd_text_falls_back_to_raw_qail() {
        let decoded = decode_cmd_text("get users limit 1").unwrap();
        assert_eq!(decoded.action, crate::ast::Action::Get);
        assert_eq!(decoded.table, "users");
        assert!(
            decoded
                .cages
                .iter()
                .any(|c| matches!(c.kind, crate::ast::CageKind::Limit(1)))
        );
    }
}
