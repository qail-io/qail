//! Transpiler test modules.
//!
//! Tests are organized by category:
//! - `core`: Basic SELECT, UPDATE, DELETE, INSERT tests
//! - `dialects`: SQL dialect-specific tests (MySQL, SQLite, Oracle, etc.)
//! - `nosql`: NoSQL transpiler tests (MongoDB, Redis, DynamoDB, etc.)
//! - `features`: DDL, Upsert, JSON operations, advanced features

mod core;
mod dialects;
mod nosql;
mod features;

