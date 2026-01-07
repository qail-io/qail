//! Fuzz target for AstEncoder::encode_cmd_sql
//!
//! Goal: ensure the AST → SQL encoder never panics on any combination of
//! table names, column names, and filter values. We construct structured
//! Qail queries from fuzzed strings via the builder API, exercising the
//! SQL injection attack surface.
//!
//! We do NOT try to derive Arbitrary on the entire Qail AST (30+ recursive
//! fields). Instead we build realistic query shapes from fuzzed strings.

#![no_main]
use libfuzzer_sys::fuzz_target;
use qail_core::ast::{Operator, Value};
use qail_core::Qail;
use qail_pg::protocol::ast_encoder::AstEncoder;

/// Split fuzz bytes into up to 4 strings for table, column, value, column2.
fn split_strings(data: &[u8]) -> Vec<String> {
    let s = String::from_utf8_lossy(data);
    s.split('\0')
        .take(4)
        .map(|part| part.to_string())
        .collect()
}

fuzz_target!(|data: &[u8]| {
    let parts = split_strings(data);
    if parts.is_empty() { return; }

    let table = parts.get(0).cloned().unwrap_or_default();
    let col1  = parts.get(1).cloned().unwrap_or_else(|| "id".into());
    let val   = parts.get(2).cloned().unwrap_or_else(|| "test".into());
    let col2  = parts.get(3).cloned().unwrap_or_else(|| "name".into());

    // Skip empty table names — the encoder legitimately rejects these
    if table.is_empty() { return; }

    // 1) SELECT with filter — most common gateway path
    let q = Qail::get(&table)
        .columns([&*col1, &*col2])
        .filter(&col1, Operator::Eq, Value::String(val.clone()));
    let _ = AstEncoder::encode_cmd_sql(&q);

    // 2) INSERT with values
    let q = Qail::add(&table)
        .columns([&*col1, &*col2])
        .values([Value::String(val.clone()), Value::String(col2.clone())]);
    let _ = AstEncoder::encode_cmd_sql(&q);

    // 3) UPDATE with filter
    let q = Qail::set(&table)
        .set_value(&col1, Value::String(val.clone()))
        .filter(&col1, Operator::Eq, Value::String(val.clone()));
    let _ = AstEncoder::encode_cmd_sql(&q);

    // 4) DELETE with filter
    let q = Qail::del(&table)
        .filter(&col1, Operator::Eq, Value::String(val.clone()));
    let _ = AstEncoder::encode_cmd_sql(&q);

    // 5) DDL — CREATE TABLE (uses the builder directly)
    let q = Qail::make(&table);
    let _ = AstEncoder::encode_cmd_sql(&q);
});
