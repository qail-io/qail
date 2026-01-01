//! Query cancellation methods for PostgreSQL connection.

use super::{CANCEL_REQUEST_CODE, PgConnection, PgResult};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// A token that can be used to cancel a running query.
/// This token is safe to send across threads and does not borrow the connection.
#[derive(Debug, Clone)]
pub struct CancelToken {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) process_id: i32,
    pub(crate) secret_key: i32,
}

impl CancelToken {
    /// Attempt to cancel the ongoing query.
    /// This opens a new TCP connection and sends a CancelRequest message.
    pub async fn cancel_query(&self) -> PgResult<()> {
        PgConnection::cancel_query(
            &self.host,
            self.port,
            self.process_id,
            self.secret_key
        ).await
    }
}

impl PgConnection {
    /// Get the cancel key for this connection.
    pub fn get_cancel_key(&self) -> (i32, i32) {
        (self.process_id, self.secret_key)
    }

    /// Cancel a running query on a PostgreSQL backend.
    /// This opens a new TCP connection and sends a CancelRequest message.
    /// The original connection continues running but the query is interrupted.
    pub async fn cancel_query(
        host: &str,
        port: u16,
        process_id: i32,
        secret_key: i32,
    ) -> PgResult<()> {
        // Open new connection just for cancel
        let addr = format!("{}:{}", host, port);
        let mut stream = TcpStream::connect(&addr).await?;

        // Send CancelRequest message:
        // Length (16) + CancelRequest code (80877102) + process_id + secret_key
        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&16i32.to_be_bytes()); // Length
        buf[4..8].copy_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        buf[8..12].copy_from_slice(&process_id.to_be_bytes());
        buf[12..16].copy_from_slice(&secret_key.to_be_bytes());

        stream.write_all(&buf).await?;

        // Server will close connection after receiving cancel request
        Ok(())
    }
}
