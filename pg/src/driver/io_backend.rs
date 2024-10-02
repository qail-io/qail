//! I/O Backend Auto-detection
//!
//! Automatically selects the best I/O backend:
//! - Linux with io_uring feature: io_uring (fastest)
//! - Otherwise: tokio (universal fallback)

use std::sync::OnceLock;

/// The detected I/O backend
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    Tokio,
    /// Linux io_uring (kernel 5.1+, requires `io_uring` feature)
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

/// Detect the best available I/O backend for this system
pub fn detect() -> IoBackend {
    *DETECTED_BACKEND.get_or_init(|| {
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            // Try to create an io_uring instance to check kernel support
            match io_uring::IoUring::new(32) {
                Ok(_) => {
                    eprintln!("[qail-pg] io_uring available, using high-performance backend");
                    IoBackend::IoUring
                }
                Err(e) => {
                    eprintln!("[qail-pg] io_uring not available ({e}), falling back to tokio");
                    IoBackend::Tokio
                }
            }
        }

        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            IoBackend::Tokio
        }
    })
}

/// Check if io_uring is available on this system
#[inline]
pub fn is_uring_available() -> bool {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        matches!(detect(), IoBackend::IoUring)
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
