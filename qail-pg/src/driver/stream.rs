//! Stream abstraction for TCP and TLS connections.
//!
//! This module provides a unified interface for both plain TCP
//! and TLS-encrypted connections.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

/// A PostgreSQL connection stream (TCP or TLS).
pub enum PgStream {
    /// Plain TCP connection (unencrypted)
    Tcp(TcpStream),
    /// TLS-encrypted connection
    Tls(TlsStream<TcpStream>),
}

impl AsyncRead for PgStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PgStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            PgStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PgStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            PgStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            PgStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PgStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            PgStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PgStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            PgStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
