//! qail-py: AST-Native Python Bindings for QAIL
//!
//! This crate provides Python bindings for QAIL's AST-native PostgreSQL driver.
//! 
//! **No SQL strings anywhere.** Python builds the AST → Rust encodes AST
//! directly to PostgreSQL wire protocol bytes.
//!
//! # Architecture (Blocking API + GIL Release)
//!
//! ```text
//! Python Application
//!        ↓ (GIL released)
//! Rust Tokio Runtime → PostgreSQL
//!        ↓
//! Results returned to Python
//! ```
//!
//! All I/O is done in Rust with GIL released for maximum throughput.

use pyo3::prelude::*;

mod types;
mod cmd;
mod row;
mod encoder;
// Keep driver.rs for backward compat but prefer Python driver
mod driver;

pub use types::PyOperator;
pub use cmd::PyQailCmd;
pub use row::PyRow;
pub use driver::PyPgDriver;

/// Python module for QAIL.
#[pymodule]
fn qail(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyOperator>()?;
    m.add_class::<PyQailCmd>()?;
    m.add_class::<PyRow>()?;
    m.add_class::<PyPgDriver>()?;
    
    // Register sync encoder functions
    encoder::register(m)?;
    
    Ok(())
}
