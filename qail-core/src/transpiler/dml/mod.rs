//! DML (Data Manipulation Language) SQL generation.
//!
//! This module contains functions for generating SELECT, INSERT, UPDATE, DELETE,
//! and other DML statements.

pub mod select;
pub mod update;
pub mod delete;
pub mod insert;
pub mod window;
pub mod cte;
pub mod upsert;
pub mod json_table;
