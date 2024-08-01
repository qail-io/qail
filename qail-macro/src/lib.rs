//! QAIL Procedural Macros
//!
//! This crate provides compile-time validated QAIL query macros.
//!
//! # Macros
//!
//! - `qail!` — Returns SQL as `&'static str` (works with any driver)
//! - `qail_query!` — Returns `sqlx::Query` with bindings (requires `sqlx` feature)
//! - `qail_query_as!` — Returns `sqlx::QueryAs` with bindings (requires `sqlx` feature)
//!
//! # Usage
//!
//! ```ignore
//! // Basic: returns SQL string
//! let sql = qail!("get::users:'_[active=true]");
//!
//! // With SQLx (requires `sqlx` feature):
//! let users = qail_query!("get::users:'_[id=$1]", user_id)
//!     .fetch_all(&pool).await?;
//!
//! // With SQLx + typed results:
//! let users: Vec<User> = qail_query_as!(User, "get::users:'id'name[active=$1]", true)
//!     .fetch_all(&pool).await?;
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, LitStr};
#[cfg(feature = "sqlx")]
use syn::{Token, Expr, Type};
#[cfg(feature = "sqlx")]
use syn::parse::{Parse, ParseStream};
#[cfg(feature = "sqlx")]
use syn::punctuated::Punctuated;
use qail_core::transpiler::ToSql;

/// Compile-time QAIL to SQL transpilation.
///
/// Returns the SQL as a `&'static str`. Use with any database driver.
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

// ============================================================================
// SQLx Integration (behind `sqlx` feature)
// ============================================================================

/// Input for qail_query!: "query", arg1, arg2, ...
#[cfg(feature = "sqlx")]
struct QueryInput {
    query: LitStr,
    args: Punctuated<Expr, Token![,]>,
}

#[cfg(feature = "sqlx")]
impl Parse for QueryInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let query: LitStr = input.parse()?;
        let args = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            Punctuated::parse_terminated(input)?
        } else {
            Punctuated::new()
        };
        Ok(QueryInput { query, args })
    }
}

/// Input for qail_query_as!: Type, "query", arg1, arg2, ...
#[cfg(feature = "sqlx")]
struct QueryAsInput {
    output_type: Type,
    query: LitStr,
    args: Punctuated<Expr, Token![,]>,
}

#[cfg(feature = "sqlx")]
impl Parse for QueryAsInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let output_type: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let query: LitStr = input.parse()?;
        let args = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            Punctuated::parse_terminated(input)?
        } else {
            Punctuated::new()
        };
        Ok(QueryAsInput { output_type, query, args })
    }
}

/// Compile-time QAIL to SQLx Query with automatic bindings.
///
/// Requires the `sqlx` feature to be enabled.
///
/// # Example
///
/// ```ignore
/// let users = qail_query!("get::users:'_[id=$1]", user_id)
///     .fetch_all(&pool).await?;
/// ```
#[cfg(feature = "sqlx")]
#[proc_macro]
pub fn qail_query(input: TokenStream) -> TokenStream {
    let QueryInput { query, args } = parse_macro_input!(input as QueryInput);
    let query_str = query.value();

    // Parse and transpile
    let cmd = match qail_core::parse(&query_str) {
        Ok(cmd) => cmd,
        Err(e) => {
            return syn::Error::new(query.span(), format!("QAIL Parse Error: {}", e))
                .to_compile_error()
                .into();
        }
    };

    let sql = cmd.to_sql();
    let arg_count = args.len();
    
    // Count placeholders in query
    let placeholder_count = query_str.matches('$').count();
    if arg_count != placeholder_count {
        return syn::Error::new(
            query.span(),
            format!(
                "Argument count mismatch: query has {} placeholder(s) but {} argument(s) provided",
                placeholder_count, arg_count
            )
        )
        .to_compile_error()
        .into();
    }

    // Generate bind chain
    let binds = args.iter().map(|arg| {
        quote! { .bind(#arg) }
    });

    let expand = quote! {
        sqlx::query(#sql)#(#binds)*
    };

    TokenStream::from(expand)
}

/// Compile-time QAIL to SQLx QueryAs with automatic bindings and typed output.
///
/// Requires the `sqlx` feature to be enabled.
///
/// # Example
///
/// ```ignore
/// let users: Vec<User> = qail_query_as!(User, "get::users:'id'name[active=$1]", true)
///     .fetch_all(&pool).await?;
/// ```
#[cfg(feature = "sqlx")]
#[proc_macro]
pub fn qail_query_as(input: TokenStream) -> TokenStream {
    let QueryAsInput { output_type, query, args } = parse_macro_input!(input as QueryAsInput);
    let query_str = query.value();

    // Parse and transpile
    let cmd = match qail_core::parse(&query_str) {
        Ok(cmd) => cmd,
        Err(e) => {
            return syn::Error::new(query.span(), format!("QAIL Parse Error: {}", e))
                .to_compile_error()
                .into();
        }
    };

    let sql = cmd.to_sql();
    let arg_count = args.len();
    
    // Count placeholders in query
    let placeholder_count = query_str.matches('$').count();
    if arg_count != placeholder_count {
        return syn::Error::new(
            query.span(),
            format!(
                "Argument count mismatch: query has {} placeholder(s) but {} argument(s) provided",
                placeholder_count, arg_count
            )
        )
        .to_compile_error()
        .into();
    }

    // Generate bind chain
    let binds = args.iter().map(|arg| {
        quote! { .bind(#arg) }
    });

    let expand = quote! {
        sqlx::query_as::<_, #output_type>(#sql)#(#binds)*
    };

    TokenStream::from(expand)
}
