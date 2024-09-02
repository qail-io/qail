//! Core traits for the transformer system

use sqlparser::ast::Statement;
use std::fmt;

/// Error during pattern extraction
#[derive(Debug, Clone)]
pub struct ExtractError {
    pub message: String,
}

impl fmt::Display for ExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ExtractError {}

/// Error during code generation
#[derive(Debug, Clone)]
pub struct TransformError {
    pub message: String,
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TransformError {}

/// Extracted data from a SQL pattern
#[derive(Debug, Clone)]
pub enum PatternData {
    /// SELECT query data
    Select {
        table: String,
        columns: Vec<String>,
        filter: Option<FilterData>,
        order_by: Option<Vec<OrderByData>>,
        limit: Option<u64>,
        joins: Vec<JoinData>,
    },
    /// INSERT query data
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<ValueData>,
        returning: Option<Vec<String>>,
    },
    /// UPDATE query data
    Update {
        table: String,
        set_values: Vec<SetValueData>,
        filter: Option<FilterData>,
        returning: Option<Vec<String>>,
    },
    /// DELETE query data
    Delete {
        table: String,
        filter: Option<FilterData>,
        returning: Option<Vec<String>>,
    },
}

/// Filter condition data
#[derive(Debug, Clone)]
pub struct FilterData {
    pub column: String,
    pub operator: String,
    pub value: ValueData,
}

/// ORDER BY data
#[derive(Debug, Clone)]
pub struct OrderByData {
    pub column: String,
    pub descending: bool,
}

/// JOIN data
#[derive(Debug, Clone)]
pub struct JoinData {
    pub table: String,
    pub on_left: String,
    pub on_right: String,
    pub join_type: JoinType,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Value data (column, literal, or parameter)
#[derive(Debug, Clone)]
pub enum ValueData {
    Column(String),
    Literal(String),
    Param(usize), // $1, $2, etc.
    Null,
}

/// SET clause data for UPDATE
#[derive(Debug, Clone)]
pub struct SetValueData {
    pub column: String,
    pub value: ValueData,
}

/// Context for pattern matching
#[derive(Debug, Default)]
pub struct MatchContext {
    pub binds: Vec<String>,
    pub return_type: Option<String>,
    pub fetch_method: String,
}

/// Context for code generation
#[derive(Debug, Default)]
pub struct TransformContext {
    pub indent: usize,
    pub include_imports: bool,
    pub binds: Vec<String>,
    pub return_type: Option<String>,
}

/// Trait for SQL pattern matching and transformation
pub trait SqlPattern: Send + Sync {
    fn id(&self) -> &'static str;

    fn priority(&self) -> u32 {
        100
    }

    /// Check if this pattern matches the SQL statement
    fn matches(&self, stmt: &Statement, ctx: &MatchContext) -> bool;

    fn extract(&self, stmt: &Statement, ctx: &MatchContext) -> Result<PatternData, ExtractError>;

    /// Generate QAIL code from extracted data
    fn transform(&self, data: &PatternData, ctx: &TransformContext) -> Result<String, TransformError>;
}

/// Trait for target language code generation
pub trait TargetLanguage: Send + Sync {
    fn name(&self) -> &'static str;

    fn emit_import(&self, items: &[&str]) -> String;

    fn emit_variable(&self, name: &str, type_: &str, value: &str) -> String;

    fn emit_method_chain(&self, receiver: &str, methods: &[(String, Vec<String>)]) -> String;

    fn emit_await(&self, expr: &str) -> String;

    fn emit_error_handling(&self, expr: &str) -> String;
}
