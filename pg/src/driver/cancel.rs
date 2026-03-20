//! Query cancellation methods for PostgreSQL connection.

use super::{CANCEL_REQUEST_CODE, PgConnection, PgResult};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

fn encode_cancel_request(process_id: i32, secret_key: &[u8]) -> PgResult<Vec<u8>> {
    if !(4..=256).contains(&secret_key.len()) {
        return Err(crate::driver::PgError::Protocol(format!(
            "Invalid cancel key length: {} (expected 4..=256)",
            secret_key.len()
        )));
    }

    let total_len = 12usize.checked_add(secret_key.len()).ok_or_else(|| {
        crate::driver::PgError::Protocol("CancelRequest length overflow".to_string())
    })?;
    let total_len = i32::try_from(total_len).map_err(|_| {
        crate::driver::PgError::Protocol("CancelRequest length exceeds i32".to_string())
    })?;

    let mut buf = Vec::with_capacity(total_len as usize);
    buf.extend_from_slice(&total_len.to_be_bytes());
    buf.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    buf.extend_from_slice(&process_id.to_be_bytes());
    buf.extend_from_slice(secret_key);
    Ok(buf)
}

/// A token that can be used to cancel a running query.
/// This token is safe to send across threads and does not borrow the connection.
#[derive(Debug, Clone)]
pub struct CancelToken {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) process_id: i32,
    /// Full cancel secret key bytes (`4..=256`).
    pub(crate) secret_key_bytes: Vec<u8>,
}

impl CancelToken {
    /// Attempt to cancel the ongoing query.
    /// This opens a new TCP connection and sends a CancelRequest message.
    pub async fn cancel_query(&self) -> PgResult<()> {
        PgConnection::cancel_query_bytes(
            &self.host,
            self.port,
            self.process_id,
            &self.secret_key_bytes,
        )
        .await
    }

    /// Get the full cancel key bytes (`process_id`, `secret_key_bytes`).
    pub fn get_cancel_key_bytes(&self) -> (i32, &[u8]) {
        (self.process_id, &self.secret_key_bytes)
    }
}

impl PgConnection {
    /// Get the full cancel key bytes for this connection.
    pub fn get_cancel_key_bytes(&self) -> (i32, &[u8]) {
        (self.process_id, &self.cancel_key_bytes)
    }

    /// Legacy cancel key accessor (`process_id`, `secret_key_i32`).
    ///
    /// Compatibility-only: valid for protocol 3.0 4-byte cancel keys.
    /// For protocol 3.2 extended keys, this returns `(process_id, 0)`.
    pub fn get_cancel_key(&self) -> (i32, i32) {
        if self.cancel_key_bytes.len() == 4 {
            (
                self.process_id,
                i32::from_be_bytes([
                    self.cancel_key_bytes[0],
                    self.cancel_key_bytes[1],
                    self.cancel_key_bytes[2],
                    self.cancel_key_bytes[3],
                ]),
            )
        } else {
            (self.process_id, 0)
        }
    }

    /// Cancel a running query using bytes-native cancel key.
    pub async fn cancel_query_bytes(
        host: &str,
        port: u16,
        process_id: i32,
        secret_key: &[u8],
    ) -> PgResult<()> {
        // Open new connection just for cancel
        let addr = format!("{}:{}", host, port);
        let mut stream = TcpStream::connect(&addr).await?;

        // Send CancelRequest message:
        // Length + CancelRequest code + process_id + secret_key bytes
        let buf = encode_cancel_request(process_id, secret_key)?;

        stream.write_all(&buf).await?;

        // Server will close connection after receiving cancel request
        Ok(())
    }

    /// Legacy i32 cancel API wrapper (protocol 3.0-style 4-byte key).
    pub async fn cancel_query(
        host: &str,
        port: u16,
        process_id: i32,
        secret_key: i32,
    ) -> PgResult<()> {
        Self::cancel_query_bytes(host, port, process_id, &secret_key.to_be_bytes()).await
    }
}

#[cfg(test)]
mod tests {
    use super::{CANCEL_REQUEST_CODE, encode_cancel_request};

    #[test]
    fn encode_cancel_request_with_4_byte_key() {
        let buf = encode_cancel_request(42, &99i32.to_be_bytes()).expect("encode");
        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[0..4], &16i32.to_be_bytes());
        assert_eq!(&buf[4..8], &CANCEL_REQUEST_CODE.to_be_bytes());
        assert_eq!(&buf[8..12], &42i32.to_be_bytes());
        assert_eq!(&buf[12..16], &99i32.to_be_bytes());
    }

    #[test]
    fn encode_cancel_request_with_extended_key() {
        let key = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let buf = encode_cancel_request(7, &key).expect("encode");
        assert_eq!(&buf[0..4], &20i32.to_be_bytes());
        assert_eq!(&buf[4..8], &CANCEL_REQUEST_CODE.to_be_bytes());
        assert_eq!(&buf[8..12], &7i32.to_be_bytes());
        assert_eq!(&buf[12..], &key);
    }

    #[test]
    fn encode_cancel_request_rejects_invalid_key_lengths() {
        let short = encode_cancel_request(1, &[1, 2, 3]).expect_err("short");
        assert!(short.to_string().contains("Invalid cancel key length"));

        let long_key = vec![0u8; 257];
        let long = encode_cancel_request(1, &long_key).expect_err("long");
        assert!(long.to_string().contains("Invalid cancel key length"));
    }
}
