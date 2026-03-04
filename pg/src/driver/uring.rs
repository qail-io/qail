//! Linux io_uring TCP stream wrapper.
//!
//! Phase 1 implementation:
//! - Plain TCP transport only (no TLS/mTLS/GSSENC wrapping here)
//! - Uses `io-uring` syscalls in a blocking worker (`spawn_blocking`)
//! - Integrates through `PgConnection` I/O helpers

#![cfg(all(target_os = "linux", feature = "io_uring"))]

use io_uring::{IoUring, opcode, types};
use std::io;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::fd::AsRawFd;

/// io_uring-backed plain TCP stream.
///
/// Uses a standard socket handle and issues io_uring ops inside
/// `spawn_blocking` closures. This keeps the async caller non-blocking while
/// allowing incremental io_uring rollout on Linux.
#[derive(Debug)]
pub struct UringTcpStream {
    stream: std::net::TcpStream,
}

impl UringTcpStream {
    /// Convert a Tokio TCP stream into an io_uring-backed stream.
    pub fn from_tokio(stream: tokio::net::TcpStream) -> io::Result<Self> {
        let std_stream = stream.into_std()?;
        // We execute I/O in blocking worker threads; keep socket blocking to
        // avoid busy EAGAIN loops.
        std_stream.set_nonblocking(false)?;
        Ok(Self { stream: std_stream })
    }

    /// Read up to `max_bytes` and append to `dst`.
    pub async fn read_into(
        &mut self,
        dst: &mut bytes::BytesMut,
        max_bytes: usize,
    ) -> io::Result<usize> {
        let stream = self.stream.try_clone()?;
        let bytes = tokio::task::spawn_blocking(move || uring_read_chunk(stream, max_bytes))
            .await
            .map_err(join_err_to_io)??;
        let n = bytes.len();
        dst.extend_from_slice(&bytes);
        Ok(n)
    }

    /// Write all bytes.
    pub async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let stream = self.stream.try_clone()?;
        let payload = bytes.to_vec();
        tokio::task::spawn_blocking(move || uring_write_all(stream, &payload))
            .await
            .map_err(join_err_to_io)??;
        Ok(())
    }

    /// Flush pending writes.
    ///
    /// TCP flush is effectively handled by write completion; keep a best-effort
    /// OS flush for interface parity.
    pub async fn flush(&mut self) -> io::Result<()> {
        let mut stream = self.stream.try_clone()?;
        tokio::task::spawn_blocking(move || stream.flush())
            .await
            .map_err(join_err_to_io)??;
        Ok(())
    }

    /// Best-effort hard abort for in-flight operations.
    ///
    /// Used by timeout paths to ensure blocking worker I/O on cloned file
    /// descriptors wakes promptly and the connection fails closed.
    pub fn abort_inflight(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Both)
    }
}

fn join_err_to_io(err: tokio::task::JoinError) -> io::Error {
    io::Error::other(format!("io_uring worker task failed: {}", err))
}

fn submit_single_entry(ring: &mut IoUring) -> io::Result<i32> {
    ring.submit_and_wait(1)?;
    let mut cq = ring.completion();
    let cqe = cq
        .next()
        .ok_or_else(|| io::Error::other("io_uring completion queue empty"))?;
    Ok(cqe.result())
}

fn uring_read_chunk(mut stream: std::net::TcpStream, max_bytes: usize) -> io::Result<Vec<u8>> {
    let fd = types::Fd(stream.as_raw_fd());
    let cap = max_bytes.max(1);
    let mut buf = vec![0u8; cap];
    let mut ring = match IoUring::new(32) {
        Ok(ring) => ring,
        Err(_) => {
            let n = stream.read(&mut buf)?;
            buf.truncate(n);
            return Ok(buf);
        }
    };

    let entry = opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as _)
        .build()
        .user_data(1);
    // SAFETY: entry references `buf`, which is kept alive until completion.
    unsafe {
        ring.submission()
            .push(&entry)
            .map_err(|_| io::Error::other("io_uring submission queue full"))?;
    }

    let res = submit_single_entry(&mut ring)?;
    if res >= 0 {
        let n = res as usize;
        buf.truncate(n);
        return Ok(buf);
    }

    let io_err = io::Error::from_raw_os_error(-res);
    if matches!(
        io_err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) {
        let n = stream.read(&mut buf)?;
        buf.truncate(n);
        return Ok(buf);
    }
    Err(io_err)
}

fn uring_write_all(mut stream: std::net::TcpStream, bytes: &[u8]) -> io::Result<()> {
    let fd = types::Fd(stream.as_raw_fd());
    let mut ring = match IoUring::new(32) {
        Ok(ring) => ring,
        Err(_) => {
            stream.write_all(bytes)?;
            return Ok(());
        }
    };
    let mut offset = 0usize;

    while offset < bytes.len() {
        let chunk = &bytes[offset..];
        let entry = opcode::Write::new(fd, chunk.as_ptr(), chunk.len() as _)
            .build()
            .user_data(2);
        // SAFETY: entry references `chunk` slice, alive until completion.
        unsafe {
            ring.submission()
                .push(&entry)
                .map_err(|_| io::Error::other("io_uring submission queue full"))?;
        }

        let res = submit_single_entry(&mut ring)?;
        if res > 0 {
            offset = offset.saturating_add(res as usize);
            continue;
        }
        if res == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "io_uring write returned 0 bytes",
            ));
        }

        let io_err = io::Error::from_raw_os_error(-res);
        if matches!(
            io_err.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
        ) {
            let n = stream.write(chunk)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "fallback write returned 0 bytes",
                ));
            }
            offset = offset.saturating_add(n);
            continue;
        }
        return Err(io_err);
    }

    Ok(())
}
