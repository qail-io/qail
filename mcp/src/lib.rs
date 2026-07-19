use qail_core::ast::{CageKind, Condition, Qail};
use qail_core::fmt::Formatter;
use qail_core::migrate::schema::{IndexMethod, Schema};
use qail_core::migrate::{FkAction, parse_qail};
use qail_core::transpiler::{Dialect, ToSql, ToSqlParameterized};
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

const PROTOCOL_VERSION: &str = "2025-06-18";
const SERVER_NAME: &str = "qail-mcp";
const SERVER_TITLE: &str = "QAIL AST Kernel MCP";

#[derive(Debug)]
struct McpError {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl McpError {
    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("Method not found: {method}"),
            data: None,
        }
    }
}

/// Run the QAIL MCP server over newline-delimited JSON-RPC stdio.
pub fn run_stdio() -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        if let Some(response) = handle_rpc(&line) {
            writeln!(stdout, "{response}")?;
            stdout.flush()?;
        }
    }

    Ok(())
}

/// Handle one JSON-RPC message and return the serialized response, if any.
///
/// This is the transport-independent entry point: [`run_stdio`] wraps it for
/// newline-delimited stdio, and the `qail-wasm` crate exports it directly so the
/// remote HTTP server runs the exact same dispatch with no reimplementation.
///
/// Returns `None` for notifications (messages without an `id`), which have no
/// response by JSON-RPC definition. Over HTTP that maps to `202 Accepted`.
pub fn handle_rpc(line: &str) -> Option<String> {
    let response = match serde_json::from_str::<Value>(line) {
        Ok(message) => handle_message(message)?,
        Err(err) => error_response(Value::Null, -32700, format!("Parse error: {err}"), None),
    };

    // A JSON-RPC envelope of plain `Value`s cannot fail to serialize.
    Some(serde_json::to_string(&response).unwrap_or_else(|err| {
        format!(
            r#"{{"jsonrpc":"2.0","id":null,"error":{{"code":-32603,"message":"Serialization failed: {err}"}}}}"#
        )
    }))
}

fn handle_message(message: Value) -> Option<Value> {
    let id = message.get("id").cloned();
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return id.map(|id| error_response(id, -32600, "Invalid request: missing method", None));
    };

    if id.is_none() {
        return None;
    }

    let id = id.expect("id checked");

    // JSON-RPC 2.0 requires the version marker on every message. Accepting a
    // message without it (or with "1.0") would answer a request this server
    // does not actually speak.
    match message.get("jsonrpc").and_then(Value::as_str) {
        Some("2.0") => {}
        Some(other) => {
            return Some(error_response(
                id,
                -32600,
                format!("Invalid request: jsonrpc must be \"2.0\", got \"{other}\""),
                None,
            ));
        }
        None => {
            return Some(error_response(
                id,
                -32600,
                "Invalid request: missing \"jsonrpc\": \"2.0\"",
                None,
            ));
        }
    }

    // JSON-RPC restricts id to a string or a number. Objects, arrays and
    // booleans are not valid identifiers, and echoing one back would propagate
    // the client's error rather than reporting it.
    if !(id.is_string() || id.is_number()) {
        return Some(error_response(
            Value::Null,
            -32600,
            "Invalid request: id must be a string or a number",
            None,
        ));
    }

    let params = message.get("params").cloned().unwrap_or_else(|| json!({}));

    let result = match method {
        "initialize" => validate_initialize(&params).map(|()| initialize_result(&params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools() })),
        "tools/call" => handle_tool_call(&params),
        "resources/list" => Ok(json!({ "resources": resources() })),
        "resources/read" => handle_resource_read(&params),
        "resources/templates/list" => Ok(json!({ "resourceTemplates": [] })),
        "prompts/list" => Ok(json!({ "prompts": prompts() })),
        "prompts/get" => handle_prompt_get(&params),
        "shutdown" => Ok(json!({})),
        other => Err(McpError::method_not_found(other)),
    };

    Some(match result {
        Ok(result) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        }),
        Err(err) => error_response(id, err.code, err.message, err.data),
    })
}

/// The lifecycle spec requires `initialize` to carry `protocolVersion`,
/// `capabilities` and `clientInfo`. Answering a bare `initialize` would let a
/// client skip negotiation entirely and still believe a session was
/// established.
fn validate_initialize(params: &Value) -> Result<(), McpError> {
    if !params.is_object() || params.as_object().is_some_and(|o| o.is_empty()) {
        return Err(McpError::invalid_params(
            "initialize requires params with protocolVersion, capabilities and clientInfo",
        ));
    }

    required_str(params, "protocolVersion")?;

    if !params.get("capabilities").is_some_and(Value::is_object) {
        return Err(McpError::invalid_params(
            "initialize requires \"capabilities\" to be an object",
        ));
    }

    let client_info = params
        .get("clientInfo")
        .filter(|v| v.is_object())
        .ok_or_else(|| {
            McpError::invalid_params("initialize requires \"clientInfo\" to be an object")
        })?;
    required_str(client_info, "name")?;

    Ok(())
}

fn initialize_result(params: &Value) -> Value {
    let requested = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(PROTOCOL_VERSION);
    let protocol_version = if requested == PROTOCOL_VERSION {
        requested
    } else {
        PROTOCOL_VERSION
    };

    json!({
        "protocolVersion": protocol_version,
        "capabilities": {
            "tools": {},
            "resources": {},
            "prompts": {}
        },
        "serverInfo": {
            "name": SERVER_NAME,
            "title": SERVER_TITLE,
            "version": env!("CARGO_PKG_VERSION")
        },
        "instructions": "Use this server to learn and inspect QAIL. Start with qail://guide/ast-kernel, then call qail_parse_query or qail_explain_query on concrete QAIL snippets. Tools are read-only and do not connect to a database."
    })
}

fn tools() -> Vec<Value> {
    vec![
        json!({
            "name": "qail_parse_query",
            "title": "Parse QAIL Query",
            "description": "Parse QAIL v2 query syntax into the typed AST and return formatted QAIL plus SQL for LLM inspection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "QAIL v2 query, for example: get users fields id, email where active = true limit 10"
                    },
                    "dialect": {
                        "type": "string",
                        "enum": ["postgres", "sqlite"],
                        "default": "postgres",
                        "description": "SQL dialect used for generated SQL."
                    },
                    "parameterized": {
                        "type": "boolean",
                        "default": true,
                        "description": "Include parameterized SQL and extracted parameters."
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true
            }
        }),
        json!({
            "name": "qail_format_query",
            "title": "Format QAIL Query",
            "description": "Parse and pretty-print a QAIL query using qail-core's formatter.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "QAIL v2 query to format."
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true
            }
        }),
        json!({
            "name": "qail_transpile_query",
            "title": "Transpile QAIL Query",
            "description": "Transpile QAIL v2 query syntax to SQL without executing it.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "QAIL v2 query to transpile."
                    },
                    "dialect": {
                        "type": "string",
                        "enum": ["postgres", "sqlite"],
                        "default": "postgres"
                    },
                    "parameterized": {
                        "type": "boolean",
                        "default": true,
                        "description": "Return parameterized SQL with extracted bind values."
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true
            }
        }),
        json!({
            "name": "qail_explain_query",
            "title": "Explain QAIL Query",
            "description": "Summarize the parsed QAIL AST in terms of action, table, columns, filters, joins, ordering, and limits.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "QAIL v2 query to explain."
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true
            }
        }),
        json!({
            "name": "qail_schema_summary",
            "title": "Summarize QAIL Schema",
            "description": "Parse a schema.qail document and summarize tables, columns, constraints, RLS flags, indexes, and policies.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Contents of a schema.qail document."
                    }
                },
                "required": ["schema"],
                "additionalProperties": false
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true
            }
        }),
        json!({
            "name": "qail_builder_cookbook",
            "title": "QAIL Builder Cookbook",
            "description": "Return focused Rust AST builder examples for the QAIL kernel.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "enum": ["all", "select", "insert", "update", "delete", "joins", "rls", "expressions", "schema"],
                        "default": "all",
                        "description": "Cookbook topic to return."
                    }
                },
                "additionalProperties": false
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true
            }
        }),
    ]
}

fn resources() -> Vec<Value> {
    vec![
        resource_meta(
            "qail://guide/ast-kernel",
            "ast-kernel",
            "QAIL AST Kernel",
            "Core concepts, crate boundaries, and how the AST path should be used.",
        ),
        resource_meta(
            "qail://guide/query-syntax",
            "query-syntax",
            "QAIL Query Syntax",
            "QAIL v2 query examples that parse through qail-core.",
        ),
        resource_meta(
            "qail://guide/schema",
            "schema",
            "schema.qail Guide",
            "Schema source examples, RLS flags, indexes, and policy notes.",
        ),
        resource_meta(
            "qail://guide/llm-usage",
            "llm-usage",
            "LLM Usage Guide",
            "How agents should call this MCP server when learning or writing QAIL.",
        ),
    ]
}

fn resource_meta(uri: &str, name: &str, title: &str, description: &str) -> Value {
    json!({
        "uri": uri,
        "name": name,
        "title": title,
        "description": description,
        "mimeType": "text/markdown",
        "annotations": {
            "audience": ["assistant"],
            "priority": 0.8
        }
    })
}

fn prompts() -> Vec<Value> {
    vec![
        json!({
            "name": "learn_qail_ast",
            "title": "Learn QAIL AST",
            "description": "Guide an LLM through QAIL AST kernel concepts before writing code.",
            "arguments": []
        }),
        json!({
            "name": "explain_qail_query",
            "title": "Explain QAIL Query",
            "description": "Explain a QAIL query with AST and SQL context.",
            "arguments": [
                {
                    "name": "query",
                    "description": "QAIL query to explain.",
                    "required": true
                }
            ]
        }),
        json!({
            "name": "write_qail_builder",
            "title": "Write QAIL Builder",
            "description": "Write Rust code using qail_core::prelude builder APIs.",
            "arguments": [
                {
                    "name": "goal",
                    "description": "Desired database operation.",
                    "required": true
                }
            ]
        }),
    ]
}

/// Validate `arguments` against the tool's advertised inputSchema.
///
/// The schemas declare typed fields and `additionalProperties: false`, so
/// silently coercing a wrong type or ignoring an unknown key would make the
/// advertised contract a fiction — a client sending `"parameterized": "false"`
/// (a string) would get parameterized output and never learn why.
///
/// This is a targeted check against the property table rather than a general
/// JSON Schema implementation: the schemas here are flat objects of strings,
/// booleans and enums, and a full validator would be far more machinery than
/// that shape warrants.
fn validate_tool_args(tool: &Value, args: &Value) -> Result<(), McpError> {
    let Some(schema) = tool.get("inputSchema") else {
        return Ok(());
    };
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return Ok(());
    };
    let Some(provided) = args.as_object() else {
        return Err(McpError::invalid_params("arguments must be an object"));
    };

    for (key, value) in provided {
        let Some(spec) = properties.get(key) else {
            let known: Vec<&str> = properties.keys().map(String::as_str).collect();
            return Err(McpError::invalid_params(format!(
                "Unknown argument \"{key}\". Accepted: {}",
                known.join(", ")
            )));
        };

        let expected = spec.get("type").and_then(Value::as_str).unwrap_or("");
        let matches = match expected {
            "string" => value.is_string(),
            "boolean" => value.is_boolean(),
            "integer" => value.is_i64() || value.is_u64(),
            "number" => value.is_number(),
            "object" => value.is_object(),
            "array" => value.is_array(),
            _ => true,
        };
        if !matches {
            return Err(McpError::invalid_params(format!(
                "Argument \"{key}\" must be a {expected}, got {}",
                json_type_name(value)
            )));
        }

        // Enum members are part of the advertised contract too; an unlisted
        // value would otherwise fall through to a silent default.
        if let Some(allowed) = spec.get("enum").and_then(Value::as_array)
            && !allowed.contains(value)
        {
            let names: Vec<String> = allowed.iter().map(ToString::to_string).collect();
            return Err(McpError::invalid_params(format!(
                "Argument \"{key}\" must be one of: {}",
                names.join(", ")
            )));
        }
    }

    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for key in required.iter().filter_map(Value::as_str) {
            if !provided.contains_key(key) {
                return Err(McpError::invalid_params(format!(
                    "Missing required argument: {key}"
                )));
            }
        }
    }

    Ok(())
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn handle_tool_call(params: &Value) -> Result<Value, McpError> {
    let name = required_str(params, "name")?;
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let Some(tool) = tools().into_iter().find(|t| t["name"] == name) else {
        return Err(McpError::invalid_params(format!("Unknown tool: {name}")));
    };
    validate_tool_args(&tool, &args)?;

    match name {
        "qail_parse_query" => Ok(parse_query_tool(&args)),
        "qail_format_query" => Ok(format_query_tool(&args)),
        "qail_transpile_query" => Ok(transpile_query_tool(&args)),
        "qail_explain_query" => Ok(explain_query_tool(&args)),
        "qail_schema_summary" => Ok(schema_summary_tool(&args)),
        "qail_builder_cookbook" => Ok(builder_cookbook_tool(&args)),
        other => Err(McpError::invalid_params(format!("Unknown tool: {other}"))),
    }
}

fn parse_query_tool(args: &Value) -> Value {
    let query = match required_str(args, "query") {
        Ok(query) => query,
        Err(err) => return tool_error(err.message),
    };
    let dialect = match dialect_arg(args) {
        Ok(dialect) => dialect,
        Err(err) => return tool_error(err),
    };
    let include_parameterized = optional_bool(args, "parameterized", true);

    let cmd = match qail_core::parse(query) {
        Ok(cmd) => cmd,
        Err(err) => return tool_error(err.to_string()),
    };

    let formatted = match Formatter::new().format(&cmd) {
        Ok(formatted) => formatted,
        Err(err) => return tool_error(format!("Format error: {err}")),
    };
    let sql = cmd.to_sql_with_dialect(dialect);
    let mut structured = json!({
        "ok": true,
        "query": query,
        "ast": cmd,
        "formatted": formatted,
        "sql": sql,
        "dialect": dialect_name(dialect)
    });

    if include_parameterized {
        structured["parameterized"] = parameterized_json(&cmd, dialect);
    }

    tool_structured(structured)
}

fn format_query_tool(args: &Value) -> Value {
    let query = match required_str(args, "query") {
        Ok(query) => query,
        Err(err) => return tool_error(err.message),
    };
    let cmd = match qail_core::parse(query) {
        Ok(cmd) => cmd,
        Err(err) => return tool_error(err.to_string()),
    };
    let formatted = match Formatter::new().format(&cmd) {
        Ok(formatted) => formatted,
        Err(err) => return tool_error(format!("Format error: {err}")),
    };

    tool_structured(json!({
        "ok": true,
        "formatted": formatted
    }))
}

fn transpile_query_tool(args: &Value) -> Value {
    let query = match required_str(args, "query") {
        Ok(query) => query,
        Err(err) => return tool_error(err.message),
    };
    let dialect = match dialect_arg(args) {
        Ok(dialect) => dialect,
        Err(err) => return tool_error(err),
    };
    let include_parameterized = optional_bool(args, "parameterized", true);

    let cmd = match qail_core::parse(query) {
        Ok(cmd) => cmd,
        Err(err) => return tool_error(err.to_string()),
    };

    let mut structured = json!({
        "ok": true,
        "dialect": dialect_name(dialect),
        "sql": cmd.to_sql_with_dialect(dialect)
    });

    if include_parameterized {
        structured["parameterized"] = parameterized_json(&cmd, dialect);
    }

    tool_structured(structured)
}

fn explain_query_tool(args: &Value) -> Value {
    let query = match required_str(args, "query") {
        Ok(query) => query,
        Err(err) => return tool_error(err.message),
    };
    let cmd = match qail_core::parse(query) {
        Ok(cmd) => cmd,
        Err(err) => return tool_error(err.to_string()),
    };

    tool_structured(json!({
        "ok": true,
        "explanation": explain_query(&cmd),
        "formatted": Formatter::new().format(&cmd).unwrap_or_default(),
        "sql": cmd.to_sql()
    }))
}

fn schema_summary_tool(args: &Value) -> Value {
    let schema_source = match required_str(args, "schema") {
        Ok(schema) => schema,
        Err(err) => return tool_error(err.message),
    };
    let schema = match parse_qail(schema_source) {
        Ok(schema) => schema,
        Err(err) => return tool_error(paren_dialect_hint(schema_source, &err)),
    };

    tool_structured(schema_summary(&schema))
}

fn builder_cookbook_tool(args: &Value) -> Value {
    let topic = args.get("topic").and_then(Value::as_str).unwrap_or("all");
    let text = cookbook(topic);
    if text.is_empty() {
        return tool_error(format!("Unknown cookbook topic: {topic}"));
    }

    tool_text(text)
}

fn handle_resource_read(params: &Value) -> Result<Value, McpError> {
    let uri = required_str(params, "uri")?;
    let Some(text) = resource_text(uri) else {
        return Err(McpError::invalid_params(format!("Unknown resource: {uri}")));
    };

    Ok(json!({
        "contents": [
            {
                "uri": uri,
                "mimeType": "text/markdown",
                "text": text
            }
        ]
    }))
}

fn handle_prompt_get(params: &Value) -> Result<Value, McpError> {
    let name = required_str(params, "name")?;
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let result = match name {
        "learn_qail_ast" => json!({
            "description": "Teach the QAIL AST Kernel to an LLM.",
            "messages": [
                prompt_user("Read qail://guide/ast-kernel and qail://guide/query-syntax. Then explain the QAIL AST path, the builder API, and when to call qail_parse_query versus qail_builder_cookbook.")
            ]
        }),
        "explain_qail_query" => {
            let query = required_str(&args, "query")?;
            json!({
                "description": "Explain a QAIL query.",
                "messages": [
                    prompt_user(&format!("Call qail_explain_query with this query, then explain the AST and SQL in plain language:\n\n{query}"))
                ]
            })
        }
        "write_qail_builder" => {
            let goal = required_str(&args, "goal")?;
            json!({
                "description": "Write Rust QAIL builder code.",
                "messages": [
                    prompt_user(&format!("Use qail_builder_cookbook for relevant examples, then write Rust using qail_core::prelude::* for this goal:\n\n{goal}\n\nPrefer Qail::get/add/set/del builders and avoid app-side SQL string assembly."))
                ]
            })
        }
        other => return Err(McpError::invalid_params(format!("Unknown prompt: {other}"))),
    };

    Ok(result)
}

fn prompt_user(text: &str) -> Value {
    json!({
        "role": "user",
        "content": {
            "type": "text",
            "text": text
        }
    })
}

fn resource_text(uri: &str) -> Option<&'static str> {
    match uri {
        "qail://guide/ast-kernel" => Some(AST_KERNEL_GUIDE),
        "qail://guide/query-syntax" => Some(QUERY_SYNTAX_GUIDE),
        "qail://guide/schema" => Some(SCHEMA_GUIDE),
        "qail://guide/llm-usage" => Some(LLM_USAGE_GUIDE),
        _ => None,
    }
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str, McpError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| McpError::invalid_params(format!("Missing required string: {key}")))
}

fn optional_bool(value: &Value, key: &str, default: bool) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(default)
}

fn dialect_arg(args: &Value) -> Result<Dialect, String> {
    let dialect = args
        .get("dialect")
        .and_then(Value::as_str)
        .unwrap_or("postgres")
        .to_ascii_lowercase();

    match dialect.as_str() {
        "postgres" | "postgresql" => Ok(Dialect::Postgres),
        "sqlite" => Ok(Dialect::SQLite),
        other => Err(format!("Unsupported dialect: {other}")),
    }
}

fn dialect_name(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Postgres => "postgres",
        Dialect::SQLite => "sqlite",
    }
}

fn parameterized_json(cmd: &Qail, dialect: Dialect) -> Value {
    let result = cmd.to_sql_parameterized_with_dialect(dialect);
    json!({
        "sql": result.sql,
        "params": result.params,
        "namedParams": result.named_params
    })
}

fn tool_structured(structured: Value) -> Value {
    let text = serde_json::to_string_pretty(&structured)
        .unwrap_or_else(|_| "{\"ok\":false,\"error\":\"serialization failed\"}".to_string());

    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": structured,
        "isError": false
    })
}

fn tool_text(text: impl Into<String>) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": text.into()
            }
        ],
        "isError": false
    })
}

fn tool_error(message: impl Into<String>) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": message.into()
            }
        ],
        "isError": true
    })
}

fn error_response(id: Value, code: i64, message: impl Into<String>, data: Option<Value>) -> Value {
    let mut error = json!({
        "code": code,
        "message": message.into()
    });

    if let Some(data) = data {
        error["data"] = data;
    }

    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error
    })
}

fn explain_query(cmd: &Qail) -> Value {
    let filters: Vec<String> = cmd
        .cages
        .iter()
        .filter(|cage| matches!(cage.kind, CageKind::Filter))
        .flat_map(|cage| cage.conditions.iter())
        .map(condition_text)
        .collect();
    let payload: Vec<String> = cmd
        .cages
        .iter()
        .filter(|cage| matches!(cage.kind, CageKind::Payload))
        .flat_map(|cage| cage.conditions.iter())
        .map(condition_text)
        .collect();
    let sorts: Vec<Value> = cmd
        .cages
        .iter()
        .filter_map(|cage| match cage.kind {
            CageKind::Sort(order) => Some(json!({
                "order": format!("{order:?}"),
                "expressions": cage
                    .conditions
                    .iter()
                    .map(|condition| condition.left.to_string())
                    .collect::<Vec<_>>()
            })),
            _ => None,
        })
        .collect();
    let limit = cmd.cages.iter().find_map(|cage| match cage.kind {
        CageKind::Limit(n) => Some(n),
        _ => None,
    });
    let offset = cmd.cages.iter().find_map(|cage| match cage.kind {
        CageKind::Offset(n) => Some(n),
        _ => None,
    });
    let joins: Vec<Value> = cmd
        .joins
        .iter()
        .map(|join| {
            json!({
                "kind": format!("{:?}", join.kind),
                "table": join.table,
                "on": join
                    .on
                    .as_ref()
                    .map(|conditions| conditions.iter().map(condition_text).collect::<Vec<_>>())
                    .unwrap_or_default(),
                "onTrue": join.on_true
            })
        })
        .collect();

    json!({
        "action": cmd.action.to_string(),
        "table": cmd.table,
        "columns": cmd.columns.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "joins": joins,
        "filters": filters,
        "payload": payload,
        "sorts": sorts,
        "limit": limit,
        "offset": offset,
        "returning": cmd.returning.as_ref().map(|exprs| {
            exprs.iter().map(ToString::to_string).collect::<Vec<_>>()
        }),
        "notes": action_notes(cmd)
    })
}

fn condition_text(condition: &Condition) -> String {
    if condition.op.needs_value() {
        format!(
            "{} {} {}",
            condition.left,
            condition.op.sql_symbol(),
            condition.value
        )
    } else {
        format!("{} {}", condition.left, condition.op.sql_symbol())
    }
}

fn action_notes(cmd: &Qail) -> Vec<&'static str> {
    let mut notes = Vec::new();

    match cmd.action.to_string().as_str() {
        "GET" | "CNT" => notes.push("Read path: drivers can validate, rewrite, and encode this AST before execution."),
        "ADD" | "SET" | "DEL" | "PUT" | "UPSERT" | "MERGE" => {
            notes.push("Write path: use explicit columns and RLS context in application code when tenant isolation is required.");
        }
        _ => notes.push("Administrative or DDL path: inspect SQL output before executing in migration workflows."),
    }

    if !cmd.joins.is_empty() {
        notes.push("Join path: relation-safe builders are preferred when generated schema modules are available.");
    }
    if cmd
        .cages
        .iter()
        .any(|cage| matches!(cage.kind, CageKind::Limit(_)))
    {
        notes.push("Bounded result: the AST includes an explicit limit.");
    }

    notes
}

/// Detect the legacy paren schema dialect so a parse failure teaches the caller
/// the canonical form instead of just reporting a syntax error.
///
/// `table users ( ... )` is the legacy dialect still accepted by parts of the
/// CLI. QAIL schemas use brace syntax, which is what `examples/schema/` and the
/// documentation use, and what `parse_qail` accepts.
fn paren_dialect_hint(source: &str, err: &str) -> String {
    let looks_paren = source
        .lines()
        .map(str::trim)
        .any(|line| line.starts_with("table ") && line.ends_with('(') || line.ends_with(") ("));

    if looks_paren {
        format!(
            "{err}\n\nThis looks like the legacy paren schema dialect \
             (`table users ( ... )`). QAIL schemas use brace syntax:\n\n\
             table users {{\n  id uuid primary_key\n  email text unique\n}}\n\n\
             See the qail://guide/schema resource for the canonical form."
        )
    } else {
        err.to_string()
    }
}

fn index_method(method: &IndexMethod) -> &'static str {
    match method {
        IndexMethod::BTree => "btree",
        IndexMethod::Hash => "hash",
        IndexMethod::Gin => "gin",
        IndexMethod::Gist => "gist",
        IndexMethod::Brin => "brin",
        IndexMethod::SpGist => "spgist",
        IndexMethod::Hnsw => "hnsw",
        IndexMethod::IvfFlat => "ivfflat",
    }
}

fn fk_action(action: &FkAction) -> &'static str {
    match action {
        FkAction::NoAction => "no_action",
        FkAction::Restrict => "restrict",
        FkAction::Cascade => "cascade",
        FkAction::SetNull => "set_null",
        FkAction::SetDefault => "set_default",
    }
}

fn schema_summary(schema: &Schema) -> Value {
    // `Schema.tables` is a HashMap; sort so the summary is deterministic.
    let mut table_names: Vec<&String> = schema.tables.keys().collect();
    table_names.sort();

    let tables: Vec<Value> = table_names
        .iter()
        .filter_map(|name| schema.tables.get(*name))
        .map(|table| {
            json!({
                "name": table.name,
                "enableRls": table.enable_rls,
                "forceRls": table.force_rls,
                "columns": table.columns.iter().map(|column| {
                    json!({
                        "name": column.name,
                        "type": column.data_type.to_string(),
                        "nullable": column.nullable,
                        "primaryKey": column.primary_key,
                        "unique": column.unique,
                        "default": column.default,
                        "references": column.foreign_key.as_ref().map(|fk| json!({
                            "table": fk.table,
                            "column": fk.column,
                            "onDelete": fk_action(&fk.on_delete),
                            "onUpdate": fk_action(&fk.on_update)
                        })),
                        "check": column.check.as_ref().map(|c| {
                            c.name.clone().unwrap_or_else(|| "unnamed".to_string())
                        })
                    })
                }).collect::<Vec<_>>()
            })
        })
        .collect();

    json!({
        "ok": true,
        "dialect": "brace",
        "tableCount": schema.tables.len(),
        "policyCount": schema.policies.len(),
        "indexCount": schema.indexes.len(),
        "tables": tables,
        "extensions": schema.extensions.iter().map(|ext| ext.name.clone()).collect::<Vec<_>>(),
        "indexes": schema.indexes.iter().map(|index| {
            json!({
                "name": index.name,
                "table": index.table,
                "columns": index.columns,
                "unique": index.unique,
                "method": index_method(&index.method)
            })
        }).collect::<Vec<_>>(),
        "policies": schema.policies.iter().map(|policy| {
            json!({
                "name": policy.name,
                "table": policy.table,
                "role": policy.role
            })
        }).collect::<Vec<_>>()
    })
}

fn cookbook(topic: &str) -> &'static str {
    match topic {
        "all" => BUILDER_COOKBOOK_ALL,
        "select" => BUILDER_COOKBOOK_SELECT,
        "insert" => BUILDER_COOKBOOK_INSERT,
        "update" => BUILDER_COOKBOOK_UPDATE,
        "delete" => BUILDER_COOKBOOK_DELETE,
        "joins" => BUILDER_COOKBOOK_JOINS,
        "rls" => BUILDER_COOKBOOK_RLS,
        "expressions" => BUILDER_COOKBOOK_EXPRESSIONS,
        "schema" => BUILDER_COOKBOOK_SCHEMA,
        _ => "",
    }
}

const AST_KERNEL_GUIDE: &str = r#"# QAIL AST Kernel

QAIL's kernel lives in `qail-core`. Application code should build database
intent as typed AST values, then let drivers, gateways, validators, and tooling
inspect or encode that structure.

Core pieces:

- `Qail`: command root for get, add, set, del, put, merge, CTEs, DDL, and utility commands.
- `Expr`: expression tree for columns, literals, functions, casts, aggregates, JSON, CASE, and subqueries.
- `Condition`: left expression, operator, value tuple used in filters, joins, payloads, and grouping cages.
- `Cage`: structural clause bucket such as filter, payload, sort, limit, offset, partition, and qualify.
- `RlsContext`: tenant/user/super-admin execution witness used by `.with_rls(&ctx)`.
- `access`: table, operation, and column policy checks before execution.

Use `qail_parse_query` when an LLM sees textual QAIL syntax and needs AST/SQL.
Use `qail_builder_cookbook` when writing Rust builder code.
"#;

const QUERY_SYNTAX_GUIDE: &str = r#"# QAIL Query Syntax

Canonical 1.x application code should prefer Rust builders, but QAIL v2 text is
useful for CLI, tests, examples, and LLM inspection.

Examples:

```qail
get users fields id, email where active = true order by created_at desc limit 10
```

```qail
add users fields email, name values 'alice@example.com', 'Alice'
```

```qail
set users where id = $1 fields name = $2
```

```qail
del sessions where expires_at < now
```

Use `qail_parse_query` to check whether a snippet parses and to see the exact
AST shape that qail-core produced.
"#;

const SCHEMA_GUIDE: &str = r#"# schema.qail Guide

`schema.qail` describes database shape for validation, migration planning, typed
code generation, and relation-aware helpers.

Schemas use **brace** syntax. Columns are newline-separated with no commas, and
table-level flags such as `enable_rls` sit inside the braces:

```qail
extension "pgcrypto"

table users {
  id uuid primary_key default gen_random_uuid()
  tenant_id uuid not_null
  email text unique not_null
  active bool not_null default true
  created_at timestamptz not_null default now()
  enable_rls
}

table posts {
  id uuid primary_key default gen_random_uuid()
  tenant_id uuid not_null
  user_id uuid not_null references users(id) on_delete cascade
  title text not_null
  enable_rls
}

index posts_user_id on posts (user_id)
```

Note `not_null` (underscore), not `not null`.

An older **paren** dialect (`table users ( id uuid primary_key, ... )`, with
commas and trailing `) enable_rls`) still appears in some CLI paths. It is
legacy — do not write it, and do not mix the two. `qail_schema_summary` accepts
only the brace form and will tell you if it detects paren input.

Use `qail_schema_summary` to turn a schema source string into structured tables,
columns, indexes, and policy metadata.
"#;

const LLM_USAGE_GUIDE: &str = r#"# LLM Usage Guide

Recommended flow for agents:

1. Read `qail://guide/ast-kernel` and `qail://guide/query-syntax`.
2. For textual QAIL, call `qail_parse_query` before explaining or rewriting it.
3. For Rust code, call `qail_builder_cookbook` for examples, then use `qail_core::prelude::*`.
4. Avoid inventing old symbolic syntax such as `get::users@id`; current QAIL 1.x code uses builders or v2 keyword syntax.
5. Do not connect to databases through this MCP server. It is intentionally read-only and local.

When tenant isolation matters, prefer:

```rust
use qail_core::prelude::*;
use qail_core::rls::RlsContext;

let ctx = RlsContext::tenant(tenant_id);
let query = Qail::get("orders").columns(["id", "status"]).with_rls(&ctx)?;
```
"#;

const BUILDER_COOKBOOK_ALL: &str = r#"# QAIL Builder Cookbook

Read a focused topic when possible: select, insert, update, delete, joins, rls,
expressions, or schema.

```rust
use qail_core::prelude::*;

let query = Qail::get("users")
    .columns(["id", "email"])
    .eq("active", true)
    .order_desc("created_at")
    .limit(10);
```
"#;

const BUILDER_COOKBOOK_SELECT: &str = r#"# Select

```rust
use qail_core::prelude::*;

let query = Qail::get("orders")
    .columns(["id", "total", "status"])
    .eq("status", "paid")
    .order_desc("created_at")
    .limit(25);
```
"#;

const BUILDER_COOKBOOK_INSERT: &str = r#"# Insert

```rust
use qail_core::prelude::*;

let query = Qail::add("users")
    .columns(["email", "name", "active"])
    .values(vec![
        Value::from("alice@example.com"),
        Value::from("Alice"),
        Value::from(true),
    ])
    .returning(["id", "email"]);
```
"#;

const BUILDER_COOKBOOK_UPDATE: &str = r#"# Update

```rust
use qail_core::prelude::*;

let query = Qail::set("users")
    .set_value("name", "Alice Smith")
    .set_value("updated_at", Value::Function("now()".into()))
    .eq("id", Value::Param(1))
    .returning(["id", "name"]);
```
"#;

const BUILDER_COOKBOOK_DELETE: &str = r#"# Delete

```rust
use qail_core::prelude::*;

let query = Qail::del("sessions")
    .lt("expires_at", Value::Function("now()".into()));
```
"#;

const BUILDER_COOKBOOK_JOINS: &str = r#"# Joins

```rust
use qail_core::prelude::*;

let query = Qail::get("orders")
    .columns(["orders.id", "orders.total", "users.email"])
    .left_join("users", "orders.user_id", "users.id")
    .eq("orders.status", "paid");
```

When generated schema relation metadata is available, prefer relation-aware
helpers such as `join_on("users")?`.
"#;

const BUILDER_COOKBOOK_RLS: &str = r#"# RLS

```rust
use qail_core::prelude::*;
use qail_core::rls::RlsContext;

let ctx = RlsContext::tenant(tenant_id);

let query = Qail::get("bookings")
    .columns(["id", "status", "total"])
    .eq("status", "confirmed")
    .with_rls(&ctx)?;
```

Use `RlsContext::global()` only for shared data and `RlsContext::super_admin`
only for internal privileged flows.
"#;

const BUILDER_COOKBOOK_EXPRESSIONS: &str = r#"# Expressions

```rust
use qail_core::prelude::*;

let query = Qail::get("users")
    .select_expr(count().alias("total_users"))
    .select_expr(count_filter(vec![eq("active", true)]).alias("active_users"))
    .select_expr(now_minus("24 hours").alias("since"));
```
"#;

const BUILDER_COOKBOOK_SCHEMA: &str = r#"# Schema

```qail
table users {
  id uuid primary_key default gen_random_uuid()
  tenant_id uuid not_null
  email text unique not_null
  enable_rls
}

unique index users_email on users (email)
```

Note `not_null` (underscore) and that table-level flags such as `enable_rls`
live inside the braces. The older paren form is legacy — do not write it.

Call `qail_schema_summary` with the schema source to inspect what qail-core
parses.
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_advertises_core_capabilities() {
        let response = handle_message(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "test",
                    "version": "0.0.0"
                }
            }
        }))
        .expect("response");

        assert_eq!(response["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert!(response["result"]["capabilities"]["tools"].is_object());
        assert!(response["result"]["capabilities"]["resources"].is_object());
        assert!(response["result"]["capabilities"]["prompts"].is_object());
    }

    #[test]
    fn parse_tool_returns_ast_and_sql() {
        let response = handle_tool_call(&json!({
            "name": "qail_parse_query",
            "arguments": {
                "query": "get users fields id, email where active = true limit 10"
            }
        }))
        .expect("tool response");

        assert_eq!(response["isError"], false);
        assert_eq!(response["structuredContent"]["ok"], true);
        assert_eq!(response["structuredContent"]["ast"]["table"], "users");
        assert!(
            response["structuredContent"]["sql"]
                .as_str()
                .unwrap()
                .contains("SELECT")
        );
    }

    #[test]
    fn schema_summary_counts_tables() {
        let response = schema_summary_tool(&json!({
            "schema": "table users {\n  id uuid primary_key\n  email text unique not_null\n}\n"
        }));

        assert_eq!(response["isError"], false);
        assert_eq!(response["structuredContent"]["dialect"], "brace");
        assert_eq!(response["structuredContent"]["tableCount"], 1);
        assert_eq!(response["structuredContent"]["tables"][0]["name"], "users");
    }

    #[test]
    fn schema_summary_reads_brace_features() {
        let response = schema_summary_tool(&json!({
            "schema": concat!(
                "extension \"pgcrypto\"\n\n",
                "table tenants {\n  id uuid primary_key\n}\n\n",
                "table users {\n",
                "  id uuid primary_key default gen_random_uuid()\n",
                "  tenant_id uuid not_null references tenants(id) on_delete cascade\n",
                "  enable_rls\n  force_rls\n",
                "}\n"
            )
        }));

        assert_eq!(response["isError"], false);
        let out = &response["structuredContent"];
        assert_eq!(out["tableCount"], 2);
        assert_eq!(out["extensions"][0], "pgcrypto");

        // tables are sorted by name, so `users` follows `tenants`
        let users = &out["tables"][1];
        assert_eq!(users["name"], "users");
        assert_eq!(users["enableRls"], true);
        assert_eq!(users["forceRls"], true);

        let tenant_fk = &users["columns"][1]["references"];
        assert_eq!(tenant_fk["table"], "tenants");
        assert_eq!(tenant_fk["onDelete"], "cascade");
    }

    #[test]
    fn schema_summary_rejects_paren_dialect_with_guidance() {
        // The legacy paren dialect must not silently parse, and the error must
        // teach the canonical brace form rather than just reporting a syntax error.
        let response = schema_summary_tool(&json!({
            "schema": "table users (\n  id uuid primary_key,\n  email text not null\n)\n"
        }));

        assert_eq!(response["isError"], true);
        let text = response["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("legacy paren schema dialect"),
            "paren input should be diagnosed, got: {text}"
        );
        assert!(text.contains("table users {"), "should show brace form");
    }

    /// Every ```qail block this server hands to an agent must parse. Checking
    /// SCHEMA_GUIDE alone was not enough: the paren dialect survived in
    /// BUILDER_COOKBOOK_SCHEMA and shipped, so this sweeps all of them.
    #[test]
    fn every_embedded_schema_example_parses() {
        let sources: [(&str, &str); 3] = [
            ("SCHEMA_GUIDE", SCHEMA_GUIDE),
            ("BUILDER_COOKBOOK_SCHEMA", BUILDER_COOKBOOK_SCHEMA),
            ("BUILDER_COOKBOOK_ALL", BUILDER_COOKBOOK_ALL),
        ];

        for (name, text) in sources {
            for (index, block) in text.split("```qail").skip(1).enumerate() {
                let Some(body) = block.split("```").next() else {
                    continue;
                };
                let body = body.trim();
                // Only schema blocks go through the schema parser; query blocks
                // are covered by the parser's own test suite.
                if !body.contains("table ") {
                    continue;
                }
                assert!(
                    parse_qail(body).is_ok(),
                    "{name} block {} does not parse: {:?}\n{body}",
                    index + 1,
                    parse_qail(body).err()
                );
                // The paren dialect is `table NAME (`. Index declarations
                // legitimately use parens (`index x on t (col)`), so the check
                // is anchored to table declarations specifically.
                let paren_table = body.lines().map(str::trim).any(|line| {
                    line.starts_with("table ") && line.ends_with('(')
                });
                assert!(
                    !paren_table,
                    "{name} block {} uses the legacy paren dialect",
                    index + 1
                );
            }
        }
    }

    #[test]
    fn schema_guide_example_actually_parses() {
        // The guide is what agents copy. If its example stops parsing, the
        // server is teaching invalid schemas — fail the build instead.
        let block = SCHEMA_GUIDE
            .split("```qail")
            .nth(1)
            .and_then(|rest| rest.split("```").next())
            .expect("SCHEMA_GUIDE must contain a ```qail example");

        let schema = parse_qail(block)
            .unwrap_or_else(|err| panic!("SCHEMA_GUIDE example does not parse: {err}"));
        assert!(schema.tables.contains_key("users"));
        assert!(schema.tables.contains_key("posts"));
    }

    fn err_code(response: &Value) -> i64 {
        response["error"]["code"].as_i64().unwrap_or(0)
    }

    #[test]
    fn rejects_wrong_jsonrpc_version() {
        let response =
            handle_message(json!({"jsonrpc": "1.0", "id": 1, "method": "ping"})).expect("responds");
        assert_eq!(err_code(&response), -32600, "{response}");
    }

    #[test]
    fn rejects_missing_jsonrpc_version() {
        let response = handle_message(json!({"id": 1, "method": "ping"})).expect("responds");
        assert_eq!(err_code(&response), -32600, "{response}");
    }

    #[test]
    fn rejects_non_scalar_id() {
        for id in [json!({"a": 1}), json!([1]), json!(true)] {
            let response = handle_message(json!({"jsonrpc": "2.0", "id": id, "method": "ping"}))
                .expect("responds");
            assert_eq!(err_code(&response), -32600, "id {id} should be rejected");
        }
    }

    #[test]
    fn rejects_incomplete_initialize() {
        // No params at all.
        let bare = handle_message(json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
            .expect("responds");
        assert_eq!(err_code(&bare), -32602, "{bare}");

        // Missing clientInfo.
        let partial = handle_message(json!({
            "jsonrpc": "2.0", "id": 2, "method": "initialize",
            "params": {"protocolVersion": PROTOCOL_VERSION, "capabilities": {}}
        }))
        .expect("responds");
        assert_eq!(err_code(&partial), -32602, "{partial}");
    }

    #[test]
    fn rejects_unknown_and_mistyped_tool_arguments() {
        // Unknown field, against additionalProperties: false.
        let unknown = handle_message(json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "qail_parse_query", "arguments": {"query": "get users", "bogus": true}}
        }))
        .expect("responds");
        assert_eq!(err_code(&unknown), -32602, "{unknown}");

        // Right name, wrong type: the schema says boolean.
        let mistyped = handle_message(json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "qail_parse_query", "arguments": {"query": "get users", "parameterized": "false"}}
        }))
        .expect("responds");
        assert_eq!(err_code(&mistyped), -32602, "{mistyped}");

        // Value outside the declared enum.
        let bad_enum = handle_message(json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "qail_parse_query", "arguments": {"query": "get users", "dialect": "oracle"}}
        }))
        .expect("responds");
        assert_eq!(err_code(&bad_enum), -32602, "{bad_enum}");

        // A well-formed call must still succeed.
        let good = handle_message(json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {"name": "qail_parse_query", "arguments": {"query": "get users", "dialect": "postgres", "parameterized": false}}
        }))
        .expect("responds");
        assert_eq!(good["result"]["isError"], false, "{good}");
    }

    #[test]
    fn handle_rpc_returns_none_for_notifications() {
        // Notifications carry no id and have no response. Over HTTP this is 202.
        assert!(handle_rpc(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).is_none());
    }

    #[test]
    fn handle_rpc_reports_parse_errors() {
        let response = handle_rpc("{not json").expect("parse error must respond");
        assert!(response.contains("-32700"), "got: {response}");
    }
}
