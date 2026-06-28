use qail_core::ast::{CageKind, Condition, Qail};
use qail_core::fmt::Formatter;
use qail_core::parser::schema::Schema;
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

        let response = match serde_json::from_str::<Value>(&line) {
            Ok(message) => handle_message(message),
            Err(err) => Some(error_response(
                Value::Null,
                -32700,
                format!("Parse error: {err}"),
                None,
            )),
        };

        if let Some(response) = response {
            writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
            stdout.flush()?;
        }
    }

    Ok(())
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
    let params = message.get("params").cloned().unwrap_or_else(|| json!({}));

    let result = match method {
        "initialize" => Ok(initialize_result(&params)),
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

fn handle_tool_call(params: &Value) -> Result<Value, McpError> {
    let name = required_str(params, "name")?;
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

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
    let schema = match Schema::parse(schema_source) {
        Ok(schema) => schema,
        Err(err) => return tool_error(err),
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

fn schema_summary(schema: &Schema) -> Value {
    let tables: Vec<Value> = schema
        .tables
        .iter()
        .map(|table| {
            json!({
                "name": table.name,
                "enableRls": table.enable_rls,
                "columns": table.columns.iter().map(|column| {
                    json!({
                        "name": column.name,
                        "type": column_type(column),
                        "nullable": column.nullable,
                        "primaryKey": column.primary_key,
                        "unique": column.unique,
                        "references": column.references,
                        "default": column.default_value,
                        "check": column.check,
                        "serial": column.is_serial
                    })
                }).collect::<Vec<_>>()
            })
        })
        .collect();

    json!({
        "ok": true,
        "version": schema.version,
        "tableCount": schema.tables.len(),
        "policyCount": schema.policies.len(),
        "indexCount": schema.indexes.len(),
        "tables": tables,
        "indexes": schema.indexes.iter().map(|index| {
            json!({
                "name": index.name,
                "table": index.table,
                "columns": index.columns,
                "unique": index.unique
            })
        }).collect::<Vec<_>>(),
        "policies": schema.policies.iter().map(|policy| {
            serde_json::to_value(policy).unwrap_or_else(|_| json!({}))
        }).collect::<Vec<_>>()
    })
}

fn column_type(column: &qail_core::parser::schema::ColumnDef) -> String {
    let mut typ = column.typ.clone();
    if let Some(params) = &column.type_params {
        typ.push('(');
        typ.push_str(&params.join(", "));
        typ.push(')');
    }
    if column.is_array {
        typ.push_str("[]");
    }
    typ
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

Example:

```qail
table users (
  id uuid primary_key,
  tenant_id uuid not null,
  email text not null unique,
  active bool default true,
  created_at timestamptz default now()
) enable_rls

table posts (
  id uuid primary_key,
  tenant_id uuid not null,
  user_id uuid references users(id),
  title text not null
) enable_rls

index posts_user_id on posts (user_id)
```

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
table users (
  id uuid primary_key,
  tenant_id uuid not null,
  email text not null unique
) enable_rls

index users_email on users (email) unique
```

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
            "schema": "table users (\n  id uuid primary_key,\n  email text not null\n)\n"
        }));

        assert_eq!(response["isError"], false);
        assert_eq!(response["structuredContent"]["tableCount"], 1);
        assert_eq!(response["structuredContent"]["tables"][0]["name"], "users");
    }
}
