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
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, LitStr, Ident, Token, Expr};
use syn::parse::{Parse, ParseStream};

// ============================================================================
// Schema Types
// ============================================================================

mod schema {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Schema {
        pub tables: Vec<TableDef>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TableDef {
        pub name: String,
        pub columns: Vec<ColumnDef>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ColumnDef {
        pub name: String,
        #[serde(rename = "type", alias = "typ")]
        pub typ: String,
        #[serde(default)]
        pub nullable: bool,
        #[serde(default)]
        pub primary_key: bool,
    }

    impl Schema {
        pub fn load() -> Option<Self> {
            let paths = [
                "qail.schema.json",
                ".qail/schema.json",
                "../qail.schema.json",
            ];

            for path in paths {
                if let Ok(content) = std::fs::read_to_string(path) {
                    if let Ok(schema) = serde_json::from_str(&content) {
                        return Some(schema);
                    }
                }
            }
            None
        }

        pub fn find_table(&self, name: &str) -> Option<&TableDef> {
            self.tables.iter().find(|t| t.name == name)
        }

        /// Find similar table names for "did you mean" suggestions
        pub fn similar_tables(&self, name: &str) -> Vec<&str> {
            self.tables
                .iter()
                .filter(|t| {
                    levenshtein(&t.name, name) <= 3 || t.name.contains(name) || name.contains(&t.name)
                })
                .map(|t| t.name.as_str())
                .take(5)
                .collect()
        }
    }

    impl TableDef {
        pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
            self.columns.iter().find(|c| c.name == name)
        }

        /// Find similar column names for "did you mean" suggestions
        pub fn similar_columns(&self, name: &str) -> Vec<&str> {
            self.columns
                .iter()
                .filter(|c| {
                    levenshtein(&c.name, name) <= 3 || c.name.contains(name) || name.contains(&c.name)
                })
                .map(|c| c.name.as_str())
                .take(5)
                .collect()
        }
    }

    /// Simple Levenshtein distance for "did you mean" suggestions
    fn levenshtein(a: &str, b: &str) -> usize {
        let a_len = a.chars().count();
        let b_len = b.chars().count();
        
        if a_len == 0 { return b_len; }
        if b_len == 0 { return a_len; }
        
        let mut matrix = vec![vec![0usize; b_len + 1]; a_len + 1];
        
        for i in 0..=a_len { matrix[i][0] = i; }
        for j in 0..=b_len { matrix[0][j] = j; }
        
        for (i, ca) in a.chars().enumerate() {
            for (j, cb) in b.chars().enumerate() {
                let cost = if ca == cb { 0 } else { 1 };
                matrix[i + 1][j + 1] = std::cmp::min(
                    std::cmp::min(matrix[i][j + 1] + 1, matrix[i + 1][j] + 1),
                    matrix[i][j] + cost,
                );
            }
        }
        
        matrix[a_len][b_len]
    }
}

// ============================================================================
// Macro Input Parsing
// ============================================================================

/// Input for qail! and qail_one! - with result type
struct QailQueryInput {
    pool: Expr,
    result_type: Ident,
    query: LitStr,
    params: Vec<(Ident, Expr)>,
}

impl Parse for QailQueryInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let pool: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let result_type: Ident = input.parse()?;
        input.parse::<Token![,]>()?;
        let query: LitStr = input.parse()?;
        let params = parse_params(input)?;
        Ok(Self { pool, result_type, query, params })
    }
}

/// Input for qail_execute! - no result type
struct QailExecuteInput {
    pool: Expr,
    query: LitStr,
    params: Vec<(Ident, Expr)>,
}

impl Parse for QailExecuteInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let pool: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let query: LitStr = input.parse()?;
        let params = parse_params(input)?;
        Ok(Self { pool, query, params })
    }
}

fn parse_params(input: ParseStream) -> syn::Result<Vec<(Ident, Expr)>> {
    let mut params = Vec::new();
    while input.peek(Token![,]) {
        input.parse::<Token![,]>()?;
        if input.is_empty() {
            break;
        }
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let value: Expr = input.parse()?;
        params.push((name, value));
    }
    Ok(params)
}

// ============================================================================
// QAIL Parsing Helpers
// ============================================================================

fn parse_qail_table(query: &str) -> Option<String> {
    let query = query.trim().to_lowercase();
    let words: Vec<&str> = query.split_whitespace().collect();
    
    if words.len() >= 2 && matches!(words[0], "get" | "add" | "set" | "del") {
        return Some(words[1].to_string());
    }
    None
}

fn parse_qail_columns(query: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let query_lower = query.to_lowercase();
    
    if let Some(where_pos) = query_lower.find("where") {
        let after_where = &query[where_pos + 5..];
        for word in after_where.split_whitespace() {
            let word_lower = word.to_lowercase();
            if !matches!(word_lower.as_str(), "and" | "or" | "=" | "!=" | "<" | ">" | 
                         "like" | "ilike" | "in" | "is" | "null" | "not" | "order" | "by" | 
                         "limit" | "offset" | "asc" | "desc" | "set" | "fields" | "true" | "false") 
               && !word.starts_with(':') 
               && !word.starts_with('$')
               && !word.chars().next().map(|c| c.is_numeric()).unwrap_or(false)
               && !word.starts_with('\'')
               && !word.starts_with('"') {
                let clean = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                if !clean.is_empty() && clean.len() > 1 {
                    columns.push(clean.to_string());
                }
            }
        }
    }
    
    columns
}

// ============================================================================
// Validation with "Did you mean?" suggestions
// ============================================================================

fn validate_query(query_str: &str, query_span: proc_macro2::Span) -> Result<(), TokenStream> {
    let schema = match schema::Schema::load() {
        Some(s) => s,
        None => return Ok(()), // No schema = skip validation
    };

    if let Some(table_name) = parse_qail_table(query_str) {
        if schema.find_table(&table_name).is_none() {
            let similar = schema.similar_tables(&table_name);
            let suggestion = if !similar.is_empty() {
                format!("\n\nDid you mean: {:?}?", similar)
            } else {
                String::new()
            };
            
            let error = format!(
                "table '{}' not found in schema.{}",
                table_name, suggestion
            );
            return Err(syn::Error::new(query_span, error).to_compile_error().into());
        }
        
        if let Some(table) = schema.find_table(&table_name) {
            for col_name in parse_qail_columns(query_str) {
                if table.find_column(&col_name).is_none() {
                    let similar = table.similar_columns(&col_name);
                    let suggestion = if !similar.is_empty() {
                        format!("\n\nDid you mean: {:?}?", similar)
                    } else {
                        String::new()
                    };
                    
                    let error = format!(
                        "column '{}' not found in table '{}'.{}",
                        col_name, table_name, suggestion
                    );
                    return Err(syn::Error::new(query_span, error).to_compile_error().into());
                }
            }
        }
    }
    
    Ok(())
}

// ============================================================================
// Code Generation
// ============================================================================

fn generate_params_code(params: &[(Ident, Expr)]) -> TokenStream2 {
    if params.is_empty() {
        return quote! {};
    }

    let param_inserts: Vec<TokenStream2> = params.iter().map(|(name, value)| {
        let name_str = name.to_string();
        quote! {
            __p.insert(#name_str, (#value).to_string());
        }
    }).collect();

    quote! {
        let __qail_params = {
            let mut __p = qail_sqlx::params::QailParams::new();
            #(#param_inserts)*
            __p
        };
    }
}

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
