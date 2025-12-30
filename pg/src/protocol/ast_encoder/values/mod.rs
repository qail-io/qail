//! Expression and value encoding modules.
//!
//! Modular structure for PostgreSQL expression encoding:
//! - `aggregate`: COUNT, SUM, AVG, etc.
//! - `json`: JSON/JSONB operators (->>, ->, #>)
//! - `cast`: Type casting (::type)
//! - `window`: Window functions (ROW_NUMBER, RANK, etc.)
//! - `expressions`: Core expression encoding

pub mod aggregate;
pub mod cast;
pub mod expressions;
pub mod json;
pub mod window;

// Re-export main encoding functions
#[allow(unused_imports)]
pub use expressions::{
    encode_column_expr, encode_columns, encode_conditions, encode_expr, encode_join_value,
    encode_operator, encode_value, write_value_to_array,
};
