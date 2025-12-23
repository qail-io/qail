//! Compile-time QAIL query macros.
//!
//! Provides the `qail!` macro for compile-time validation of QAIL queries
//! against a schema file.
//!
//! # Setup
//!
//! 1. Generate schema file: `qail pull postgres://...`
//! 2. Use the macro:
//!
//! ```ignore
//! use qail_macros::qail;
//!
//! let user = qail!(pool, User, "get users where id = :id", id: user_id).await?;
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, LitStr, Ident, Token, Expr};
use syn::parse::{Parse, ParseStream};

/// Schema types (simplified, matches qail-core schema.rs)
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
            // Try to load from multiple possible locations
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
    }

    impl TableDef {
        pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
            self.columns.iter().find(|c| c.name == name)
        }
    }
}

/// Parsed macro input: qail!(pool, Type, "query", params...)
struct QailInput {
    pool: Expr,
    result_type: Ident,
    query: LitStr,
    params: Vec<(Ident, Expr)>,
}

impl Parse for QailInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // Parse: pool
        let pool: Expr = input.parse()?;
        input.parse::<Token![,]>()?;

        // Parse: Type
        let result_type: Ident = input.parse()?;
        input.parse::<Token![,]>()?;

        // Parse: "query"
        let query: LitStr = input.parse()?;

        // Parse optional params: , name: value, ...
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

        Ok(QailInput {
            pool,
            result_type,
            query,
            params,
        })
    }
}

/// Simple QAIL parser for extracting table and column names
fn parse_qail_table(query: &str) -> Option<String> {
    // Pattern: "get TABLE ..." or "add TABLE ..." etc
    let query = query.trim().to_lowercase();
    let words: Vec<&str> = query.split_whitespace().collect();
    
    if words.len() >= 2 {
        // get users, add orders, set products, del items
        if matches!(words[0], "get" | "add" | "set" | "del") {
            return Some(words[1].to_string());
        }
    }
    None
}

/// Extract column names from WHERE clause
fn parse_qail_columns(query: &str) -> Vec<String> {
    let mut columns = Vec::new();
    
    // Simple pattern matching for "column = :param" or "column = value"
    // This is a simplified parser - full parser would use qail-core
    let query_lower = query.to_lowercase();
    
    // Look for patterns like "where col1 = ..." 
    if let Some(where_pos) = query_lower.find("where") {
        let after_where = &query[where_pos + 5..];
        for word in after_where.split_whitespace() {
            // Skip keywords and operators
            let word_lower = word.to_lowercase();
            if !matches!(word_lower.as_str(), "and" | "or" | "=" | "!=" | "<" | ">" | 
                         "like" | "in" | "is" | "null" | "not" | "order" | "by" | 
                         "limit" | "offset" | "asc" | "desc") 
               && !word.starts_with(':') 
               && !word.starts_with('$')
               && !word.chars().next().map(|c| c.is_numeric()).unwrap_or(false)
               && !word.starts_with('\'')
               && !word.starts_with('"') {
                // Potentially a column name
                let clean = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                if !clean.is_empty() {
                    columns.push(clean.to_string());
                }
            }
        }
    }
    
    columns
}

/// The main `qail!` macro.
///
/// # Usage
///
/// ```ignore
/// let user = qail!(pool, User, "get users where id = :id", id: user_id).await?;
/// ```
#[proc_macro]
pub fn qail(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QailInput);
    
    let pool = &input.pool;
    let result_type = &input.result_type;
    let query_str = input.query.value();
    
    // Try to load schema
    let schema = schema::Schema::load();
    
    // Validate if schema is available
    if let Some(ref schema) = schema {
        // Extract table name from query
        if let Some(table_name) = parse_qail_table(&query_str) {
            // Validate table exists
            if schema.find_table(&table_name).is_none() {
                let available: Vec<_> = schema.tables.iter().map(|t| &t.name).collect();
                let error = format!(
                    "table '{}' not found in schema. Available tables: {:?}",
                    table_name, available
                );
                return syn::Error::new(input.query.span(), error)
                    .to_compile_error()
                    .into();
            }
            
            // Validate columns in WHERE clause
            if let Some(table) = schema.find_table(&table_name) {
                for col_name in parse_qail_columns(&query_str) {
                    if table.find_column(&col_name).is_none() {
                        let available: Vec<_> = table.columns.iter().map(|c| &c.name).collect();
                        let error = format!(
                            "column '{}' not found in table '{}'. Available columns: {:?}",
                            col_name, table_name, available
                        );
                        return syn::Error::new(input.query.span(), error)
                            .to_compile_error()
                            .into();
                    }
                }
            }
        }
    }
    
    // Generate code
    let query_lit = &input.query;
    
    // Build params hashmap
    let param_inserts: Vec<TokenStream2> = input.params.iter().map(|(name, value)| {
        let name_str = name.to_string();
        quote! {
            params.insert(#name_str, #value);
        }
    }).collect();
    
    let output = if input.params.is_empty() {
        // No params - use regular qail_fetch_all
        quote! {
            {
                use qail_sqlx::prelude::*;
                (#pool).qail_fetch_all::<#result_type>(#query_lit)
            }
        }
    } else {
        // With params - use qail_params!
        quote! {
            {
                use qail_sqlx::prelude::*;
                use qail_sqlx::qail_params;
                
                let params = qail_params! {
                    #(#param_inserts)*
                };
                (#pool).qail_fetch_all_with::<#result_type>(#query_lit, &params)
            }
        }
    };
    
    output.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_qail_table() {
        assert_eq!(parse_qail_table("get users"), Some("users".to_string()));
        assert_eq!(parse_qail_table("add orders columns x values y"), Some("orders".to_string()));
        assert_eq!(parse_qail_table("set products where id = :id"), Some("products".to_string()));
    }
}
