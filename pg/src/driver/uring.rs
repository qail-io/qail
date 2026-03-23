//! Linux io_uring TCP stream wrapper.
//!
//! Native plain-TCP implementation for Linux `io_uring` mode:
//! - Plain TCP transport only (no TLS/mTLS/GSSENC wrapping here)
//! - Dedicated per-connection worker thread with a persistent `io_uring` ring
//! - Integrates through `PgConnection` I/O helpers

#![cfg(all(target_os = "linux", feature = "io_uring"))]

use io_uring::{IoUring, opcode, types};
use std::io;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::fd::AsRawFd;
use std::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

/// io_uring-backed plain TCP stream.
///
/// Uses a dedicated worker thread that owns ring I/O, while the async side
/// communicates through channels.
#[derive(Debug)]
pub struct UringTcpStream {
    /// Control handle for force-abort on timeout/drop paths.
    control_stream: std::net::TcpStream,
    /// Command channel to per-connection io_uring worker.
    worker_tx: Sender<WorkerCommand>,
}

enum WorkerCommand {
    Read {
        max_bytes: usize,
        resp: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write {
        bytes: Vec<u8>,
        resp: oneshot::Sender<io::Result<()>>,
    },
    Flush {
        resp: oneshot::Sender<io::Result<()>>,
    },
    Shutdown,
}

impl UringTcpStream {
    /// Convert a standard TCP stream into an io_uring-backed stream.
    pub(crate) fn from_std(std_stream: std::net::TcpStream) -> io::Result<Self> {
        // Keep the fd in blocking mode: worker thread blocks on ring wait.
        std_stream.set_nonblocking(false)?;

        let worker_stream = std_stream.try_clone()?;
        let (worker_tx, worker_rx) = std::sync::mpsc::channel::<WorkerCommand>();

        std::thread::Builder::new()
            .name("qail-io-uring-worker".to_string())
            .spawn(move || worker_loop(worker_stream, worker_rx))
            .map_err(|e| io::Error::other(format!("failed to spawn io_uring worker: {}", e)))?;

        Ok(Self {
            control_stream: std_stream,
            worker_tx,
        })
    }

    /// Read up to `max_bytes` and append to `dst`.
    pub async fn read_into(
        &mut self,
        dst: &mut bytes::BytesMut,
        max_bytes: usize,
    ) -> io::Result<usize> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.worker_tx
            .send(WorkerCommand::Read {
                max_bytes,
                resp: resp_tx,
            })
            .map_err(worker_channel_err)?;

        let bytes = resp_rx.await.map_err(worker_oneshot_err)??;
        let n = bytes.len();
        dst.extend_from_slice(&bytes);
        Ok(n)
    }

    /// Write all bytes.
    pub async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.worker_tx
            .send(WorkerCommand::Write {
                bytes: bytes.to_vec(),
                resp: resp_tx,
            })
            .map_err(worker_channel_err)?;

        resp_rx.await.map_err(worker_oneshot_err)?
    }

    /// Flush pending writes.
    ///
    /// TCP flush is effectively handled by write completion; keep a best-effort
    /// OS flush for interface parity.
    pub async fn flush(&mut self) -> io::Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.worker_tx
            .send(WorkerCommand::Flush { resp: resp_tx })
            .map_err(worker_channel_err)?;

        resp_rx.await.map_err(worker_oneshot_err)?
    }

    /// Best-effort hard abort for in-flight operations.
    ///
    /// Used by timeout paths to ensure blocking worker I/O wakes promptly and
    /// the connection fails closed.
    pub fn abort_inflight(&self) -> io::Result<()> {
        self.control_stream.shutdown(Shutdown::Both)
    }
}

impl Drop for UringTcpStream {
    fn drop(&mut self) {
        let _ = self.abort_inflight();
        let _ = self.worker_tx.send(WorkerCommand::Shutdown);
    }
}

fn worker_channel_err(err: std::sync::mpsc::SendError<WorkerCommand>) -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        format!("io_uring worker channel send failed: {}", err),
    )
}

fn worker_oneshot_err(err: tokio::sync::oneshot::error::RecvError) -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        format!("io_uring worker response missing: {}", err),
    )
}

fn worker_loop(mut stream: std::net::TcpStream, rx: Receiver<WorkerCommand>) {
    let mut ring = match IoUring::new(128) {
        Ok(ring) => Some(ring),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "qail-pg io_uring: ring init failed; falling back to blocking TCP on this connection"
            );
            None
        }
    };

    while let Ok(cmd) = rx.recv() {
        match cmd {
            WorkerCommand::Read { max_bytes, resp } => {
                let _ = resp.send(uring_read_chunk(&mut ring, &mut stream, max_bytes));
            }
            WorkerCommand::Write { bytes, resp } => {
                let _ = resp.send(uring_write_all(&mut ring, &mut stream, &bytes));
            }
            WorkerCommand::Flush { resp } => {
                let _ = resp.send(stream.flush());
            }
            WorkerCommand::Shutdown => break,
        }
    }
}

fn submit_single_entry(ring: &mut IoUring) -> io::Result<i32> {
    ring.submit_and_wait(1)?;
    let mut cq = ring.completion();
    let cqe = cq
        .next()
        .ok_or_else(|| io::Error::other("io_uring completion queue empty"))?;
    Ok(cqe.result())
}

fn uring_read_chunk(
    ring: &mut Option<IoUring>,
    stream: &mut std::net::TcpStream,
    max_bytes: usize,
) -> io::Result<Vec<u8>> {
    let cap = max_bytes.max(1);
    let mut buf = vec![0u8; cap];
    let mut disable_ring = false;

    if let Some(ring_ref) = ring.as_mut() {
        let fd = types::Fd(stream.as_raw_fd());
        let entry = opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as _)
            .build()
            .user_data(1);

        // SAFETY: entry references `buf`, which is kept alive until completion.
        let submit = unsafe { ring_ref.submission().push(&entry) };
        match submit {
            Ok(()) => match submit_single_entry(ring_ref) {
                Ok(result) if result >= 0 => {
                    let n = result as usize;
                    buf.truncate(n);
                    return Ok(buf);
                }
                Ok(result) => {
                    let io_err = io::Error::from_raw_os_error(-result);
                    if !matches!(
                        io_err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) {
                        return Err(io_err);
                    }
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "qail-pg io_uring: read submit/wait failed; failing this connection closed"
                    );
                    return Err(io::Error::other(format!(
                        "io_uring read submit/wait failed: {}",
                        e
                    )));
                }
            },
            Err(_) => {
                disable_ring = true;
                tracing::warn!(
                    "qail-pg io_uring: submission queue full on read; downgrading this connection to blocking TCP"
                );
            }
        }
    }

    if disable_ring {
        *ring = None;
    }

    let n = stream.read(&mut buf)?;
    buf.truncate(n);
    Ok(buf)
}

fn uring_write_all(
    ring: &mut Option<IoUring>,
    stream: &mut std::net::TcpStream,
    bytes: &[u8],
) -> io::Result<()> {
    let mut offset = 0usize;
    let mut disable_ring = false;

    while offset < bytes.len() {
        let chunk = &bytes[offset..];

        if let Some(ring_ref) = ring.as_mut() {
            let fd = types::Fd(stream.as_raw_fd());
            let entry = opcode::Write::new(fd, chunk.as_ptr(), chunk.len() as _)
                .build()
                .user_data(2);
            // SAFETY: entry references `chunk`, which is kept alive until completion.
            let submit = unsafe { ring_ref.submission().push(&entry) };

            match submit {
                Ok(()) => match submit_single_entry(ring_ref) {
                    Ok(result) if result > 0 => {
                        offset = offset.saturating_add(result as usize);
                        continue;
                    }
                    Ok(0) => {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "io_uring write returned 0 bytes",
                        ));
                    }
                    Ok(result) => {
                        let io_err = io::Error::from_raw_os_error(-result);
                        if !matches!(
                            io_err.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) {
                            return Err(io_err);
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "qail-pg io_uring: write submit/wait failed; failing this connection closed"
                        );
                        return Err(io::Error::other(format!(
                            "io_uring write submit/wait failed: {}",
                            e
                        )));
                    }
                },
                Err(_) => {
                    disable_ring = true;
                    tracing::warn!(
                        "qail-pg io_uring: submission queue full on write; downgrading this connection to blocking TCP"
                    );
                }
            }
        }

        if disable_ring {
            *ring = None;
        }

        let n = stream.write(chunk)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "fallback write returned 0 bytes",
            ));
        }
        offset = offset.saturating_add(n);
    }

    Ok(())
}
