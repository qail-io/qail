//! Compile-time QAIL query macros.
//!
//! Provides `qail!`, `qail_one!`, and `qail_execute!` macros for compile-time
//! validation of QAIL queries against a schema file.
//!
//! # Setup
//!
//! 1. Generate schema file: `qail pull postgres://...`
//! 2. Use the macros:
//!
//! ```ignore
//! use qail_macros::{qail, qail_one, qail_execute};
//!
//! // Fetch all rows
//! let users = qail!(pool, User, "get users where active = :active", active: true).await?;
//!
//! // Fetch one row
//! let user = qail_one!(pool, User, "get users where id = :id", id: user_id).await?;
//!
//! // Execute (no return)
//! qail_execute!(pool, "del users where id = :id", id: user_id).await?;
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

mod schema;
mod input;
mod validation;
mod codegen;
mod include;

use input::{QailQueryInput, QailExecuteInput};
use validation::validate_query;
use codegen::generate_params_code;

// ============================================================================
// Macro Definitions
// ============================================================================

/// Fetch all rows matching a QAIL query.
///
/// # Example
/// ```ignore
/// let users = qail!(pool, User, "get users where active = :active", active: true).await?;
/// ```
#[proc_macro]
pub fn qail(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QailQueryInput);
    
    if let Err(e) = validate_query(&input.query.value(), input.query.span()) {
        return e;
    }
    
    let pool = &input.pool;
    let result_type = &input.result_type;
    let query_lit = &input.query;
    
    let output = if input.params.is_empty() {
        quote! {
            {
                use qail_sqlx::prelude::*;
                (#pool).qail_fetch_all::<#result_type>(#query_lit)
            }
        }
    } else {
        let params_code = generate_params_code(&input.params);
        quote! {
            {
                use qail_sqlx::prelude::*;
                #params_code
                async move {
                    (#pool).qail_fetch_all_with::<#result_type>(#query_lit, &__qail_params).await
                }
            }
        }
    };
    
    output.into()
}

/// Fetch exactly one row matching a QAIL query.
///
/// # Example
/// ```ignore
/// let user = qail_one!(pool, User, "get users where id = :id", id: user_id).await?;
/// ```
#[proc_macro]
pub fn qail_one(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QailQueryInput);
    
    if let Err(e) = validate_query(&input.query.value(), input.query.span()) {
        return e;
    }
    
    let pool = &input.pool;
    let result_type = &input.result_type;
    let query_lit = &input.query;
    
    let output = if input.params.is_empty() {
        quote! {
            {
                use qail_sqlx::prelude::*;
                (#pool).qail_fetch_one::<#result_type>(#query_lit)
            }
        }
    } else {
        let params_code = generate_params_code(&input.params);
        quote! {
            {
                use qail_sqlx::prelude::*;
                #params_code
                async move {
                    (#pool).qail_fetch_one_with::<#result_type>(#query_lit, &__qail_params).await
                }
            }
        }
    };
    
    output.into()
}

/// Fetch an optional row matching a QAIL query.
///
/// # Example
/// ```ignore
/// let user = qail_optional!(pool, User, "get users where id = :id", id: user_id).await?;
/// ```
#[proc_macro]
pub fn qail_optional(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QailQueryInput);
    
    if let Err(e) = validate_query(&input.query.value(), input.query.span()) {
        return e;
    }
    
    let pool = &input.pool;
    let result_type = &input.result_type;
    let query_lit = &input.query;
    
    let output = if input.params.is_empty() {
        quote! {
            {
                use qail_sqlx::prelude::*;
                (#pool).qail_fetch_optional::<#result_type>(#query_lit)
            }
        }
    } else {
        let params_code = generate_params_code(&input.params);
        quote! {
            {
                use qail_sqlx::prelude::*;
                #params_code
                async move {
                    (#pool).qail_fetch_optional_with::<#result_type>(#query_lit, &__qail_params).await
                }
            }
        }
    };
    
    output.into()
}

/// Execute a QAIL query without returning rows (INSERT/UPDATE/DELETE).
///
/// # Example
/// ```ignore
/// qail_execute!(pool, "del users where id = :id", id: user_id).await?;
/// ```
#[proc_macro]
pub fn qail_execute(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QailExecuteInput);
    
    if let Err(e) = validate_query(&input.query.value(), input.query.span()) {
        return e;
    }
    
    let pool = &input.pool;
    let query_lit = &input.query;
    
    let output = if input.params.is_empty() {
        quote! {
            {
                use qail_sqlx::prelude::*;
                (#pool).qail_execute(#query_lit)
            }
        }
    } else {
        let params_code = generate_params_code(&input.params);
        quote! {
            {
                use qail_sqlx::prelude::*;
                #params_code
                async move {
                    (#pool).qail_execute_with(#query_lit, &__qail_params).await
                }
            }
        }
    };
    
    output.into()
}

/// Load a schema from a `.qail` file at compile time.
///
/// # Example
/// ```ignore
/// include_schema!("db/schema.qail");
/// ```
#[proc_macro]
pub fn include_schema(input: TokenStream) -> TokenStream {
    include::include_schema_impl(input)
}

/// Load named queries from a `.qail` file at compile time.
///
/// # Example
/// ```ignore
/// include_queries!("db/queries.qail");
/// ```
#[proc_macro]
pub fn include_queries(input: TokenStream) -> TokenStream {
    include::include_queries_impl(input)
}
