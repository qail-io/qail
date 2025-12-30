//! QAIL Daemon - Unix Socket IPC for zero-CGO database access
//!
//! This daemon handles all PostgreSQL communication, allowing Go/Python/etc
//! to communicate via Unix socket without CGO overhead.

use qail_core::ast::Qail;
use qail_pg::PgDriver;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

const SOCKET_PATH: &str = "/tmp/qail.sock";
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16MB

// ============================================================================
// IPC Protocol Messages
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    /// Connect to a PostgreSQL database
    Connect {
        host: String,
        port: u16,
        user: String,
        database: String,
        password: Option<String>,
    },
    /// Execute a QAIL GET command (SELECT)
    Get {
        table: String,
        columns: Vec<String>,
        filter: Option<String>,
        limit: Option<i64>,
    },
    /// Execute a batch of GET commands (sequential)
    GetBatch { queries: Vec<GetQuery> },
    /// Execute a batch using PostgreSQL pipeline mode (full results)
    Pipeline { queries: Vec<GetQuery> },
    /// Execute a batch using PostgreSQL pipeline mode (count only - FAST)
    PipelineFast { queries: Vec<GetQuery> },
    /// Prepare a SQL statement (returns handle for reuse)
    Prepare { sql: String },
    /// Execute prepared statement with params batch (FASTEST - like native Rust)
    PreparedPipeline {
        handle: String,
        params_batch: Vec<Vec<String>>, // Each inner vec is params for one query
    },
    /// Close the connection
    Close,
    /// Ping to check if daemon is alive
    Ping,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub filter: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    /// Connection established
    Connected,
    /// Query results
    Results { rows: Vec<Row>, affected: u64 },
    /// Batch results
    BatchResults { results: Vec<QueryResult> },
    /// Count only (fast mode)
    Count { count: usize },
    /// Prepared statement handle (for reuse)
    PreparedHandle { handle: String },
    /// Pong response
    Pong,
    /// Error occurred
    Error { message: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Row {
    pub columns: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub affected: u64,
}

use qail_pg::driver::PreparedStatement;
use std::collections::HashMap;

struct ConnectionState {
    driver: Option<PgDriver>,
    prepared_stmts: HashMap<String, PreparedStatement>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            driver: None,
            prepared_stmts: HashMap::new(),
        }
    }
}

// ============================================================================
// Main Daemon
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("🚀 QAIL Daemon starting...");

    // Remove old socket file if exists
    if Path::new(SOCKET_PATH).exists() {
        std::fs::remove_file(SOCKET_PATH)?;
    }

    // Create Unix socket listener
    let listener = UnixListener::bind(SOCKET_PATH)?;
    info!("📡 Listening on {}", SOCKET_PATH);

    // Accept connections
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                info!("🔌 New client connected");
                tokio::spawn(handle_client(stream));
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

async fn handle_client(mut stream: UnixStream) {
    let state = Arc::new(RwLock::new(ConnectionState::new()));
    let mut buf = vec![0u8; MAX_MESSAGE_SIZE];

    loop {
        // Read message length (4 bytes, big-endian)
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            info!("Client disconnected");
            break;
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        if msg_len > MAX_MESSAGE_SIZE {
            error!("Message too large: {} bytes", msg_len);
            break;
        }

        // Read message
        if stream.read_exact(&mut buf[..msg_len]).await.is_err() {
            error!("Failed to read message");
            break;
        }

        // Decode request (JSON)
        let request: Request = match serde_json::from_slice(&buf[..msg_len]) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to decode request: {}", e);
                let response = Response::Error {
                    message: format!("Invalid request: {}", e),
                };
                send_response(&mut stream, &response).await;
                continue;
            }
        };

        // Handle request
        let response = handle_request(&state, request).await;
        send_response(&mut stream, &response).await;
    }

    // Cleanup
    let mut state = state.write().await;
    state.driver = None;
    info!("🔌 Client cleanup complete");
}

async fn handle_request(state: &Arc<RwLock<ConnectionState>>, request: Request) -> Response {
    match request {
        Request::Ping => Response::Pong,

        Request::Connect {
            host,
            port,
            user,
            database,
            password,
        } => {
            info!("Connecting to {}:{}/{}", host, port, database);
            let result = if let Some(pwd) = password {
                PgDriver::connect_with_password(&host, port, &user, &database, &pwd).await
            } else {
                PgDriver::connect(&host, port, &user, &database).await
            };

            match result {
                Ok(driver) => {
                    let mut state = state.write().await;
                    state.driver = Some(driver);
                    info!("✅ Connected to PostgreSQL");
                    Response::Connected
                }
                Err(e) => {
                    error!("Connection failed: {}", e);
                    Response::Error {
                        message: format!("Connection failed: {}", e),
                    }
                }
            }
        }

        Request::Get {
            table,
            columns,
            filter,
            limit,
        } => {
            let mut state = state.write().await;
            match &mut state.driver {
                Some(driver) => {
                    // Build Qail
                    let mut cmd = Qail::get(&table);
                    for col in &columns {
                        cmd = cmd.column(col);
                    }
                    // Note: filter requires structured params, skip for now
                    let _ = filter;
                    if let Some(l) = limit {
                        cmd = cmd.limit(l);
                    }

                    match driver.fetch_all(&cmd).await {
                        Ok(pg_rows) => {
                            let rows = pg_rows
                                .iter()
                                .map(|r| Row {
                                    columns: r.columns.iter().map(column_to_value).collect(),
                                })
                                .collect();
                            Response::Results { rows, affected: 0 }
                        }
                        Err(e) => Response::Error {
                            message: format!("Query failed: {}", e),
                        },
                    }
                }
                None => Response::Error {
                    message: "Not connected".to_string(),
                },
            }
        }

        Request::GetBatch { queries } => {
            let mut state = state.write().await;
            match &mut state.driver {
                Some(driver) => {
                    let mut results = Vec::with_capacity(queries.len());

                    for q in queries {
                        let mut cmd = Qail::get(&q.table);
                        for col in &q.columns {
                            cmd = cmd.column(col);
                        }
                        // Note: filter requires structured params, skip for now
                        let _ = q.filter;
                        if let Some(l) = q.limit {
                            cmd = cmd.limit(l);
                        }

                        match driver.fetch_all(&cmd).await {
                            Ok(pg_rows) => {
                                let rows = pg_rows
                                    .iter()
                                    .map(|r| Row {
                                        columns: r.columns.iter().map(column_to_value).collect(),
                                    })
                                    .collect();
                                results.push(QueryResult { rows, affected: 0 });
                            }
                            Err(e) => {
                                return Response::Error {
                                    message: format!("Batch query failed: {}", e),
                                };
                            }
                        }
                    }

                    Response::BatchResults { results }
                }
                None => Response::Error {
                    message: "Not connected".to_string(),
                },
            }
        }

        Request::Pipeline { queries } => {
            let mut state = state.write().await;
            match &mut state.driver {
                Some(driver) => {
                    // Build Qail list for pipeline
                    let cmds: Vec<Qail> = queries
                        .iter()
                        .map(|q| {
                            let mut cmd = Qail::get(&q.table);
                            for col in &q.columns {
                                cmd = cmd.column(col);
                            }
                            if let Some(l) = q.limit {
                                cmd = cmd.limit(l);
                            }
                            cmd
                        })
                        .collect();

                    // Use true PostgreSQL pipeline mode with full results
                    match driver.pipeline_fetch(&cmds).await {
                        Ok(all_pg_rows) => {
                            let results: Vec<QueryResult> = all_pg_rows
                                .iter()
                                .map(|pg_rows| QueryResult {
                                    rows: pg_rows
                                        .iter()
                                        .map(|r| Row {
                                            columns: r
                                                .columns
                                                .iter()
                                                .map(column_to_value)
                                                .collect(),
                                        })
                                        .collect(),
                                    affected: 0,
                                })
                                .collect();
                            Response::BatchResults { results }
                        }
                        Err(e) => Response::Error {
                            message: format!("Pipeline failed: {}", e),
                        },
                    }
                }
                None => Response::Error {
                    message: "Not connected".to_string(),
                },
            }
        }

        Request::PipelineFast { queries } => {
            let mut state = state.write().await;
            match &mut state.driver {
                Some(driver) => {
                    // Build Qail list for pipeline
                    let cmds: Vec<Qail> = queries
                        .iter()
                        .map(|q| {
                            let mut cmd = Qail::get(&q.table);
                            for col in &q.columns {
                                cmd = cmd.column(col);
                            }
                            if let Some(l) = q.limit {
                                cmd = cmd.limit(l);
                            }
                            cmd
                        })
                        .collect();

                    // Use FAST pipeline mode (count only, like native Rust benchmark)
                    match driver.pipeline_batch(&cmds).await {
                        Ok(count) => Response::Count { count },
                        Err(e) => Response::Error {
                            message: format!("PipelineFast failed: {}", e),
                        },
                    }
                }
                None => Response::Error {
                    message: "Not connected".to_string(),
                },
            }
        }

        Request::Prepare { sql } => {
            let mut state = state.write().await;
            match &mut state.driver {
                Some(driver) => match driver.prepare(&sql).await {
                    Ok(stmt) => {
                        let handle = stmt.name().to_string();
                        state.prepared_stmts.insert(handle.clone(), stmt);
                        info!("Prepared statement: {}", handle);
                        Response::PreparedHandle { handle }
                    }
                    Err(e) => Response::Error {
                        message: format!("Prepare failed: {}", e),
                    },
                },
                None => Response::Error {
                    message: "Not connected".to_string(),
                },
            }
        }

        Request::PreparedPipeline {
            handle,
            params_batch,
        } => {
            let mut state = state.write().await;

            // First check if we have the prepared statement
            let stmt = match state.prepared_stmts.get(&handle) {
                Some(s) => s.clone(),
                None => {
                    return Response::Error {
                        message: format!("Prepared statement not found: {}", handle),
                    };
                }
            };

            match &mut state.driver {
                Some(driver) => {
                    // Convert String params to Option<Vec<u8>> format
                    let params: Vec<Vec<Option<Vec<u8>>>> = params_batch
                        .iter()
                        .map(|p| p.iter().map(|s| Some(s.as_bytes().to_vec())).collect())
                        .collect();

                    // Use the FASTEST pipeline method (like native Rust benchmark)
                    match driver.pipeline_prepared_fast(&stmt, &params).await {
                        Ok(count) => Response::Count { count },
                        Err(e) => Response::Error {
                            message: format!("PreparedPipeline failed: {}", e),
                        },
                    }
                }
                None => Response::Error {
                    message: "Not connected".to_string(),
                },
            }
        }

        Request::Close => {
            let mut state = state.write().await;
            state.driver = None;
            state.prepared_stmts.clear();
            info!("Connection closed by client");
            Response::Error {
                message: "Connection closed".to_string(),
            }
        }
    }
}

async fn send_response(stream: &mut UnixStream, response: &Response) {
    let data = serde_json::to_vec(response).unwrap_or_default();
    let len = (data.len() as u32).to_be_bytes();

    if stream.write_all(&len).await.is_err() {
        warn!("Failed to send response length");
        return;
    }
    if stream.write_all(&data).await.is_err() {
        warn!("Failed to send response data");
    }
}

// ============================================================================
// Type Conversions
// ============================================================================

fn column_to_value(column: &Option<Vec<u8>>) -> Value {
    match column {
        None => Value::Null,
        Some(bytes) => {
            // Try to interpret as UTF-8 string first
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse as number
                if let Ok(i) = s.parse::<i64>() {
                    return Value::Int(i);
                }
                if let Ok(f) = s.parse::<f64>() {
                    return Value::Float(f);
                }
                if s == "t" || s == "true" {
                    return Value::Bool(true);
                }
                if s == "f" || s == "false" {
                    return Value::Bool(false);
                }
                Value::String(s.to_string())
            } else {
                Value::Bytes(bytes.clone())
            }
        }
    }
}
