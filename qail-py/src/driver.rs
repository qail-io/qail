//! High-Performance PgDriver with Blocking API + GIL Release.
//!
//! Rust owns the TCP socket (Tokio). Python just calls methods.
//! GIL is released during I/O for maximum throughput.
//!
//! Uses PyO3 0.27's `Python::detach` API for GIL release.

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use qail_pg::{PgDriver, PgError};
use crate::cmd::PyQailCmd;
use crate::row::PyRow;
use std::sync::{Arc, Mutex};
use once_cell::sync::Lazy;

// Global Tokio runtime - shared across all connections
static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

/// Python-exposed PostgreSQL driver.
/// 
/// Uses blocking API with GIL release for maximum performance.
/// All I/O is done in Rust Tokio runtime.
#[pyclass(name = "PgDriver")]
pub struct PyPgDriver {
    inner: Arc<Mutex<Option<PgDriver>>>,
}

#[pymethods]
impl PyPgDriver {
    /// Connect to PostgreSQL with password authentication.
    /// 
    /// BLOCKING with GIL release - Python can do other work while connecting.
    #[staticmethod]
    fn connect(
        py: Python<'_>,
        host: String,
        port: u16,
        user: String,
        database: String,
        password: String,
    ) -> PyResult<Self> {
        // Release GIL while Rust does I/O (PyO3 0.27: detach replaces allow_threads)
        let result = py.detach(|| {
            RUNTIME.block_on(async {
                PgDriver::connect_with_password(&host, port, &user, &database, &password).await
            })
        });
        
        let driver = result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))?;
        
        Ok(PyPgDriver { 
            inner: Arc::new(Mutex::new(Some(driver))) 
        })
    }
    
    /// Connect without password (trust mode).
    #[staticmethod]
    fn connect_trust(
        py: Python<'_>,
        host: String,
        port: u16,
        user: String,
        database: String,
    ) -> PyResult<Self> {
        let result = py.detach(|| {
            RUNTIME.block_on(async {
                PgDriver::connect(&host, port, &user, &database).await
            })
        });
        
        let driver = result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))?;
        
        Ok(PyPgDriver { 
            inner: Arc::new(Mutex::new(Some(driver))) 
        })
    }
    
    /// Fetch all rows from a query.
    /// 
    /// BLOCKING with GIL release. AST → wire protocol → Postgres.
    fn fetch_all(&self, py: Python<'_>, cmd: &PyQailCmd) -> PyResult<Vec<PyRow>> {
        let cmd_clone = cmd.inner.clone();
        let driver_arc = Arc::clone(&self.inner);
        
        // Release GIL while Rust does I/O
        let result = py.detach(|| {
            let mut guard = driver_arc.lock().unwrap();
            let driver = guard.as_mut().ok_or_else(|| {
                PgError::Protocol("Connection closed".to_string())
            })?;
            
            RUNTIME.block_on(async {
                driver.fetch_all(&cmd_clone).await
            })
        });
        
        let rows = result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))?;
        Ok(rows.into_iter().map(PyRow::new).collect())
    }
    
    /// Execute a batch of commands in a single round-trip.
    /// 
    /// HIGH PERFORMANCE: All commands pipelined, GIL released.
    /// This is the fastest path for bulk operations.
    fn pipeline_batch(&self, py: Python<'_>, cmds: Vec<PyQailCmd>) -> PyResult<usize> {
        let inner_cmds: Vec<_> = cmds.into_iter().map(|c| c.inner).collect();
        let driver_arc = Arc::clone(&self.inner);
        
        // Release GIL while Rust does I/O
        let result = py.detach(|| {
            let mut guard = driver_arc.lock().unwrap();
            let driver = guard.as_mut().ok_or_else(|| {
                PgError::Protocol("Connection closed".to_string())
            })?;
            
            RUNTIME.block_on(async {
                driver.pipeline_batch(&inner_cmds).await
            })
        });
        
        result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
    }
    
    /// Execute a command without returning rows.
    fn execute(&self, py: Python<'_>, cmd: &PyQailCmd) -> PyResult<u64> {
        let cmd_clone = cmd.inner.clone();
        let driver_arc = Arc::clone(&self.inner);
        
        let result = py.detach(|| {
            let mut guard = driver_arc.lock().unwrap();
            let driver = guard.as_mut().ok_or_else(|| {
                PgError::Protocol("Connection closed".to_string())
            })?;
            
            RUNTIME.block_on(async {
                driver.execute(&cmd_clone).await
            })
        });
        
        result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
    }
    
    /// Begin a transaction.
    fn begin(&self, py: Python<'_>) -> PyResult<()> {
        let driver_arc = Arc::clone(&self.inner);
        
        let result = py.detach(|| {
            let mut guard = driver_arc.lock().unwrap();
            let driver = guard.as_mut().ok_or_else(|| {
                PgError::Protocol("Connection closed".to_string())
            })?;
            
            RUNTIME.block_on(async {
                driver.begin().await
            })
        });
        
        result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
    }
    
    /// Commit the current transaction.
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let driver_arc = Arc::clone(&self.inner);
        
        let result = py.detach(|| {
            let mut guard = driver_arc.lock().unwrap();
            let driver = guard.as_mut().ok_or_else(|| {
                PgError::Protocol("Connection closed".to_string())
            })?;
            
            RUNTIME.block_on(async {
                driver.commit().await
            })
        });
        
        result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
    }
    
    /// Rollback the current transaction.
    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let driver_arc = Arc::clone(&self.inner);
        
        let result = py.detach(|| {
            let mut guard = driver_arc.lock().unwrap();
            let driver = guard.as_mut().ok_or_else(|| {
                PgError::Protocol("Connection closed".to_string())
            })?;
            
            RUNTIME.block_on(async {
                driver.rollback().await
            })
        });
        
        result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
    }
    
    /// Bulk insert using PostgreSQL COPY protocol.
    /// 
    /// HIGH PERFORMANCE: Uses COPY FROM STDIN for maximum throughput.
    /// Requires QailCmd::Add with columns specified.
    fn copy_bulk(&self, py: Python<'_>, cmd: &PyQailCmd, rows: Vec<Vec<Bound<'_, PyAny>>>) -> PyResult<u64> {
        use qail_core::ast::Value;
        
        let cmd_clone = cmd.inner.clone();
        let driver_arc = Arc::clone(&self.inner);
        
        // Convert Python rows to Rust Values
        let rust_rows: Vec<Vec<Value>> = rows.iter()
            .map(|row| {
                row.iter()
                    .map(|item| {
                        // Convert Python object to qail Value
                        if item.is_none() {
                            Value::Null
                        } else if let Ok(b) = item.extract::<bool>() {
                            Value::Bool(b)
                        } else if let Ok(i) = item.extract::<i64>() {
                            Value::Int(i)
                        } else if let Ok(f) = item.extract::<f64>() {
                            Value::Float(f)
                        } else if let Ok(s) = item.extract::<String>() {
                            Value::String(s)
                        } else {
                            Value::Null
                        }
                    })
                    .collect()
            })
            .collect();
        
        let result = py.detach(|| {
            let mut guard = driver_arc.lock().unwrap();
            let driver = guard.as_mut().ok_or_else(|| {
                PgError::Protocol("Connection closed".to_string())
            })?;
            
            RUNTIME.block_on(async {
                driver.copy_bulk(&cmd_clone, &rust_rows).await
            })
        });
        
        result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
    }
    
    /// **Fastest** bulk insert using pre-encoded COPY data.
    /// 
    /// Accepts raw bytes in COPY text format (tab-separated, newline-terminated).
    /// Use when caller has already encoded rows to avoid PyO3 extraction overhead.
    /// 
    /// Example:
    ///     data = b"1\thello\t3.14\n2\tworld\t2.71\n"
    ///     driver.copy_bulk_bytes(cmd, data)
    fn copy_bulk_bytes(&self, py: Python<'_>, cmd: &PyQailCmd, data: &[u8]) -> PyResult<u64> {
        let cmd_clone = cmd.inner.clone();
        let driver_arc = Arc::clone(&self.inner);
        let data_vec = data.to_vec(); // Copy bytes (fast - no type extraction)
        
        let result = py.detach(|| {
            let mut guard = driver_arc.lock().unwrap();
            let driver = guard.as_mut().ok_or_else(|| {
                PgError::Protocol("Connection closed".to_string())
            })?;
            
            RUNTIME.block_on(async {
                driver.copy_bulk_bytes(&cmd_clone, &data_vec).await
            })
        });
        
        result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
    }
    
    /// Close the connection.
    fn close(&self) -> PyResult<()> {
        let mut guard = self.inner.lock().map_err(|_| {
            PyRuntimeError::new_err("Failed to acquire driver lock")
        })?;
        *guard = None;
        Ok(())
    }
}
