//! I/O Backend Auto-detection
//!
//! Reports active I/O backend and optional kernel capabilities.
//!
//! Current rollout:
//! - Tokio remains the universal default.
//! - On Linux with `io_uring` feature, plain TCP transport can use io_uring
//!   (TLS/mTLS/GSSENC still use Tokio stream path).

use std::sync::OnceLock;

/// The detected I/O backend
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    /// Tokio-based async I/O (cross-platform default).
    Tokio,
    /// Linux io_uring capability (kernel 5.1+, requires `io_uring` feature).
    /// Capability does not yet imply active transport path.
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring,
}

impl std::fmt::Display for IoBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoBackend::Tokio => write!(f, "tokio"),
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            IoBackend::IoUring => write!(f, "io_uring"),
        }
    }
}

static DETECTED_BACKEND: OnceLock<IoBackend> = OnceLock::new();

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn probe_uring_support() -> Result<(), std::io::Error> {
    io_uring::IoUring::new(32).map(|_| ())
}

/// Detect the preferred I/O backend used for plain TCP transport.
pub fn detect() -> IoBackend {
    *DETECTED_BACKEND.get_or_init(|| {
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            if should_use_uring_plain_transport() {
                tracing::info!("qail-pg: using io_uring backend for plain TCP transport");
                return IoBackend::IoUring;
            }
            match probe_uring_support() {
                Ok(_) => {
                    tracing::info!(
                        "qail-pg: io_uring kernel support detected; using tokio backend by policy"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        "qail-pg: io_uring kernel support unavailable; using tokio backend"
                    );
                }
            }
        }
        IoBackend::Tokio
    })
}

/// Check if io_uring is available on this system
#[inline]
pub fn is_uring_available() -> bool {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        probe_uring_support().is_ok()
    }
    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        false
    }
}

/// Returns `true` when plain TCP transport should use io_uring.
///
/// Policy:
/// - `QAIL_PG_IO_BACKEND=tokio` → force tokio
/// - `QAIL_PG_IO_BACKEND=io_uring` → force io_uring (requires availability)
/// - unset/other → auto-enable io_uring when available
#[inline]
pub fn should_use_uring_plain_transport() -> bool {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        let backend = std::env::var("QAIL_PG_IO_BACKEND")
            .unwrap_or_default()
            .to_ascii_lowercase();
        if backend == "tokio" {
            return false;
        }
        if backend == "io_uring" {
            return probe_uring_support().is_ok();
        }
        probe_uring_support().is_ok()
    }
    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        false
    }
}

/// Get the name of the current backend
#[inline]
pub fn backend_name() -> &'static str {
    match detect() {
        IoBackend::Tokio => "tokio",
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        IoBackend::IoUring => "io_uring",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_backend() {
        let backend = detect();
        // Should always succeed (either io_uring or tokio)
        println!("Detected backend: {backend}");
    }

    #[test]
    fn test_backend_name() {
        let name = backend_name();
        assert!(!name.is_empty());
    }
}
