//! Expression and value encoding modules.
//!
//! Modular structure for PostgreSQL expression encoding.
//! Add new expression encoders as separate files when they grow complex.

mod expressions;

// Re-export main encoding functions used externally
pub use expressions::encode_call_args_with_params;
#[cfg(test)]
pub use expressions::encode_column_expr;
pub use expressions::encode_columns_with_params;
pub use expressions::encode_conditions;
pub use expressions::encode_expr;
pub use expressions::encode_expr_with_params;
pub use expressions::encode_ref_expr;
pub use expressions::encode_value;
pub(crate) use expressions::{OperandMode, encode_condition};
