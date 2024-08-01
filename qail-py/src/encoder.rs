//! Pure sync encoder exposing AstEncoder to Python.
//!
//! No async, no Tokio - just AST → wire bytes conversion.
//! Python handles all I/O with native asyncio.
//!
//! Returns PyBytes directly for zero-copy to Python.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use qail_pg::protocol::AstEncoder;
use crate::cmd::PyQailCmd;

/// Encode a single QailCmd to PostgreSQL wire protocol bytes.
/// 
/// Returns bytes ready to send directly to PostgreSQL TCP socket.
#[pyfunction]
pub fn encode_cmd<'py>(py: Python<'py>, cmd: &PyQailCmd) -> Bound<'py, PyBytes> {
    let (wire_bytes, _) = AstEncoder::encode_cmd(&cmd.inner);
    PyBytes::new(py, &wire_bytes)
}

/// Encode multiple QailCmds to wire bytes for pipeline execution.
/// 
/// All commands in one buffer for single network round-trip.
#[pyfunction]
pub fn encode_batch<'py>(py: Python<'py>, cmds: Vec<PyQailCmd>) -> Bound<'py, PyBytes> {
    let inner: Vec<_> = cmds.into_iter().map(|c| c.inner).collect();
    let wire_bytes = AstEncoder::encode_batch(&inner);
    PyBytes::new(py, &wire_bytes)
}

/// Register encoder functions with the module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_cmd, m)?)?;
    m.add_function(wrap_pyfunction!(encode_batch, m)?)?;
    Ok(())
}
