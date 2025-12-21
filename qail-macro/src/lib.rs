//! QAIL Procedural Macros
//!
//! This crate provides the `qail!` macro for compile-time validated QAIL queries.
//!
//! # Usage
//!
//! The macro parses QAIL at compile time and returns the SQL string.
//! Use with any database driver of your choice.
//!
//! ```ignore
//! // Returns a &'static str with the SQL
//! let sql = qail!("get::users:'_[active=true]");
//! // => "SELECT * FROM users WHERE active = true"
//!
//! // Use with your preferred driver
//! pool.query(sql).fetch_all().await?;
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, LitStr};
use qail_core::transpiler::ToSql;

/// Compile-time QAIL to SQL transpilation.
///
/// Parses the QAIL query at compile time and emits the SQL string.
/// If the query has a syntax error, compilation fails with a helpful message.
///
/// # Example
///
/// ```ignore
/// let sql = qail!("get::users:'id'email[active=true]");
/// // sql = "SELECT id, email FROM users WHERE active = true"
/// ```
#[proc_macro]
pub fn qail(input: TokenStream) -> TokenStream {
    let query = parse_macro_input!(input as LitStr);
    let query_str = query.value();

    // Parse QAIL at compile time
    let cmd = match qail_core::parse(&query_str) {
        Ok(cmd) => cmd,
        Err(e) => {
            return syn::Error::new(query.span(), format!("QAIL Parse Error: {}", e))
                .to_compile_error()
                .into();
        }
    };

    // Transpile to SQL
    let sql = cmd.to_sql();

    // Return the SQL string as a &'static str
    let expand = quote! {
        #sql
    };

    TokenStream::from(expand)
}
