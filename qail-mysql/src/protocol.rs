//! MySQL wire protocol constants and packet parsing.
//!
//! Implements only the packets needed for read-only queries:
//! - Initial Handshake (server greeting)
//! - Handshake Response (client auth)
//! - COM_QUERY
//! - Result Set (columns + rows)

use bytes::{Buf, BufMut, BytesMut};

/// MySQL packet header: 3-byte length + 1-byte sequence
pub const HEADER_SIZE: usize = 4;

/// Read a length-encoded integer from buffer.
pub fn read_len_enc_int(buf: &mut &[u8]) -> u64 {
    if buf.is_empty() {
        return 0;
    }
    let first = buf[0];
    *buf = &buf[1..];
    match first {
        0xfb => 0, // NULL (special case)
        0xfc => {
            let val = u16::from_le_bytes([buf[0], buf[1]]) as u64;
            *buf = &buf[2..];
            val
        }
        0xfd => {
            let val = u32::from_le_bytes([buf[0], buf[1], buf[2], 0]) as u64;
            *buf = &buf[3..];
            val
        }
        0xfe => {
            let val = u64::from_le_bytes([
                buf[0], buf[1], buf[2], buf[3],
                buf[4], buf[5], buf[6], buf[7],
            ]);
            *buf = &buf[8..];
            val
        }
        n => n as u64,
    }
}

/// Read a length-encoded string from buffer.
pub fn read_len_enc_string(buf: &mut &[u8]) -> Vec<u8> {
    let len = read_len_enc_int(buf) as usize;
    if len == 0 || buf.len() < len {
        return Vec::new();
    }
    let result = buf[..len].to_vec();
    *buf = &buf[len..];
    result
}

/// Read null-terminated string.
pub fn read_null_string(buf: &mut &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    while !buf.is_empty() && buf[0] != 0 {
        result.push(buf[0]);
        *buf = &buf[1..];
    }
    if !buf.is_empty() {
        *buf = &buf[1..]; // skip null byte
    }
    result
}

/// Write a length-encoded integer to buffer.
pub fn write_len_enc_int(buf: &mut BytesMut, val: u64) {
    if val < 251 {
        buf.put_u8(val as u8);
    } else if val < 65536 {
        buf.put_u8(0xfc);
        buf.put_u16_le(val as u16);
    } else if val < 16777216 {
        buf.put_u8(0xfd);
        buf.put_u8((val & 0xff) as u8);
        buf.put_u8(((val >> 8) & 0xff) as u8);
        buf.put_u8(((val >> 16) & 0xff) as u8);
    } else {
        buf.put_u8(0xfe);
        buf.put_u64_le(val);
    }
}

/// Initial handshake packet from server.
#[derive(Debug)]
pub struct InitialHandshake {
    pub protocol_version: u8,
    pub server_version: String,
    pub connection_id: u32,
    pub auth_plugin_data: Vec<u8>, // scramble (20 bytes)
    pub capability_flags: u32,
    pub character_set: u8,
    pub status_flags: u16,
    pub auth_plugin_name: String,
}

impl InitialHandshake {
    pub fn parse(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        
        // Protocol version
        let protocol_version = buf[0];
        buf = &buf[1..];
        
        // Server version (null-terminated)
        let server_version = String::from_utf8_lossy(&read_null_string(&mut buf)).to_string();
        
        // Connection ID
        if buf.len() < 4 { return None; }
        let connection_id = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        buf = &buf[4..];
        
        // Auth plugin data part 1 (8 bytes)
        let mut auth_data = buf[..8].to_vec();
        buf = &buf[8..];
        
        // Skip filler
        buf = &buf[1..];
        
        // Capability flags (lower 2 bytes)
        let cap_lower = u16::from_le_bytes([buf[0], buf[1]]) as u32;
        buf = &buf[2..];
        
        // Character set
        let character_set = buf[0];
        buf = &buf[1..];
        
        // Status flags
        let status_flags = u16::from_le_bytes([buf[0], buf[1]]);
        buf = &buf[2..];
        
        // Capability flags (upper 2 bytes)
        let cap_upper = u16::from_le_bytes([buf[0], buf[1]]) as u32;
        let capability_flags = cap_lower | (cap_upper << 16);
        buf = &buf[2..];
        
        // Auth plugin data length
        let auth_data_len = buf[0] as usize;
        buf = &buf[1..];
        
        // Skip 10 bytes reserved
        buf = &buf[10..];
        
        // Auth plugin data part 2
        if auth_data_len > 8 {
            let part2_len = auth_data_len - 8;
            auth_data.extend_from_slice(&buf[..part2_len.min(buf.len())]);
            buf = &buf[part2_len.min(buf.len())..];
        }
        
        // Remove trailing null from scramble
        while auth_data.last() == Some(&0) {
            auth_data.pop();
        }
        
        // Auth plugin name
        let auth_plugin_name = String::from_utf8_lossy(&read_null_string(&mut buf)).to_string();
        
        Some(Self {
            protocol_version,
            server_version,
            connection_id,
            auth_plugin_data: auth_data,
            capability_flags,
            character_set,
            status_flags,
            auth_plugin_name,
        })
    }
}

/// Encode handshake response (client authentication).
pub fn encode_handshake_response(
    user: &str,
    auth_response: &[u8],
    database: &str,
    character_set: u8,
    auth_plugin: &str,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(128);
    
    // Capability flags (CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH)
    let caps: u32 = 0x00000200 | 0x00008000 | 0x00000008 | 0x00080000;
    buf.put_u32_le(caps);
    
    // Max packet size
    buf.put_u32_le(16777215);
    
    // Character set
    buf.put_u8(character_set);
    
    // Reserved (23 bytes)
    buf.put_slice(&[0u8; 23]);
    
    // Username (null-terminated)
    buf.put_slice(user.as_bytes());
    buf.put_u8(0);
    
    // Auth response (length-encoded)
    buf.put_u8(auth_response.len() as u8);
    buf.put_slice(auth_response);
    
    // Database (null-terminated)
    buf.put_slice(database.as_bytes());
    buf.put_u8(0);
    
    // Auth plugin name (null-terminated)
    buf.put_slice(auth_plugin.as_bytes());
    buf.put_u8(0);
    
    buf
}

/// Encode SSL request packet (for TLS upgrade).
pub fn encode_ssl_request(character_set: u8) -> BytesMut {
    let mut buf = BytesMut::with_capacity(32);
    
    // Capability flags (CLIENT_SSL | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH)
    let caps: u32 = 0x00000800 | 0x00000200 | 0x00008000 | 0x00000008 | 0x00080000;
    buf.put_u32_le(caps);
    
    // Max packet size
    buf.put_u32_le(16777215);
    
    // Character set
    buf.put_u8(character_set);
    
    // Reserved (23 bytes)
    buf.put_slice(&[0u8; 23]);
    
    buf
}

/// Encode COM_QUERY command.
pub fn encode_query(sql: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(1 + sql.len());
    buf.put_u8(0x03); // COM_QUERY
    buf.put_slice(sql.as_bytes());
    buf
}

/// Column definition from result set.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: u8,
}

impl ColumnDef {
    pub fn parse(data: &[u8]) -> Option<Self> {
        let mut buf = &data[..];
        
        // Skip catalog, schema, table, org_table
        let _catalog = read_len_enc_string(&mut buf);
        let _schema = read_len_enc_string(&mut buf);
        let _table = read_len_enc_string(&mut buf);
        let _org_table = read_len_enc_string(&mut buf);
        
        // Column name
        let name = String::from_utf8_lossy(&read_len_enc_string(&mut buf)).to_string();
        
        // Skip org_name
        let _org_name = read_len_enc_string(&mut buf);
        
        // Fixed length fields
        if buf.len() < 12 { return None; }
        let _fixed_len = buf[0]; // 0x0c
        buf = &buf[1..];
        
        let _charset = u16::from_le_bytes([buf[0], buf[1]]);
        buf = &buf[2..];
        
        let _column_length = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        buf = &buf[4..];
        
        let column_type = buf[0];
        
        Some(Self { name, column_type })
    }
}
