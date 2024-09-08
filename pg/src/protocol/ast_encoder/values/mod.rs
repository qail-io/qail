//! Expression and value encoding modules.
//!
//! Modular structure for PostgreSQL expression encoding.
//! Add new expression encoders as separate files when they grow complex.

mod expressions;

// Re-export main encoding functions used externally
pub use expressions::encode_columns;
pub use expressions::encode_conditions;
pub use expressions::encode_expr;
pub use expressions::encode_join_value;
pub use expressions::encode_value;
