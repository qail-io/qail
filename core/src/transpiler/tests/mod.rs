//! Transpiler test modules.
//!
//! Tests are organized by category:
//! - `core`: Basic SELECT, UPDATE, DELETE, INSERT tests
//! - `dialects`: PostgreSQL plus 1.x dialect compatibility checks
//! - `nosql`: Qdrant transpiler tests
//! - `features`: DDL, Upsert, JSON operations, advanced features

mod core;
mod dialects;
mod features;
mod nosql;
