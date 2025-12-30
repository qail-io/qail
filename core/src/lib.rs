pub mod analyzer;
pub mod ast;
pub mod error;
pub mod fmt;
pub mod migrate;
pub mod parser;
pub mod schema;
pub mod transformer;
pub mod transpiler;
pub mod validator;

pub use parser::parse;

/// Ergonomic alias for QailCmd - the primary query builder type.
pub type Qail = ast::QailCmd;

pub mod prelude {
    pub use crate::ast::*;
    pub use crate::error::*;
    pub use crate::parser::parse;
    pub use crate::transpiler::ToSql;
    pub use crate::Qail;
}
