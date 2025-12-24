//! Include macros for loading .qail files at compile time.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

/// Include a schema from a .qail file
pub fn include_schema_impl(input: TokenStream) -> TokenStream {
    let path_lit = parse_macro_input!(input as syn::LitStr);
    let path = path_lit.value();
    
    // Read the file at compile time
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            return syn::Error::new(
                path_lit.span(),
                format!("Failed to read schema file '{}': {}", path, e),
            ).to_compile_error().into();
        }
    };
    
    // Parse using qail-core's schema parser
    let schema = if content.trim().starts_with('{') {
        // JSON format
        match qail_core::parser::schema::Schema::from_json(&content) {
            Ok(s) => s,
            Err(e) => {
                return syn::Error::new(
                    path_lit.span(),
                    format!("Failed to parse JSON schema: {}", e),
                ).to_compile_error().into();
            }
        }
    } else {
        // QAIL format
        match qail_core::parser::schema::Schema::parse(&content) {
            Ok(s) => s,
            Err(e) => {
                return syn::Error::new(
                    path_lit.span(),
                    format!("Failed to parse QAIL schema: {}", e),
                ).to_compile_error().into();
            }
        }
    };
    
    // Generate code that creates a const with schema JSON
    let json = match serde_json::to_string(&schema) {
        Ok(j) => j,
        Err(e) => {
            return syn::Error::new(
                path_lit.span(),
                format!("Failed to serialize schema: {}", e),
            ).to_compile_error().into();
        }
    };
    
    let output = quote! {
        // Schema loaded from #path at compile time
        const SCHEMA_JSON: &str = #json;
    };
    
    output.into()
}

/// Include queries from a .qail file
pub fn include_queries_impl(input: TokenStream) -> TokenStream {
    let path_lit = parse_macro_input!(input as syn::LitStr);
    let path = path_lit.value();
    
    // Read the file at compile time
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            return syn::Error::new(
                path_lit.span(),
                format!("Failed to read query file '{}': {}", path, e),
            ).to_compile_error().into();
        }
    };
    
    // Parse using qail-core's query file parser
    let query_file = match qail_core::parser::query_file::QueryFile::parse(&content) {
        Ok(qf) => qf,
        Err(e) => {
            return syn::Error::new(
                path_lit.span(),
                format!("Failed to parse query file: {}", e),
            ).to_compile_error().into();
        }
    };
    
    // Generate a function for each query
    let mut functions = Vec::new();
    
    for query_def in &query_file.queries {
        let name = syn::Ident::new(&query_def.name, proc_macro2::Span::call_site());
        let body = &query_def.body;
        
        // Generate parameter list
        let params: Vec<_> = query_def.params.iter().map(|p| {
            let param_name = syn::Ident::new(&p.name, proc_macro2::Span::call_site());
            let param_type = syn::Ident::new(&p.typ, proc_macro2::Span::call_site());
            quote! { #param_name: #param_type }
        }).collect();
        
        // Generate params struct initialization
        let param_inits: Vec<_> = query_def.params.iter().map(|p| {
            let param_name = syn::Ident::new(&p.name, proc_macro2::Span::call_site());
            quote! { __qail_params.insert(stringify!(#param_name), #param_name); }
        }).collect();
        
        let func = if query_def.is_execute {
            quote! {
                /// Generated from query file
                pub async fn #name<'c, E>(executor: E, #(#params),*) -> Result<(), sqlx::Error>
                where
                    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
                {
                    use qail_sqlx::prelude::*;
                    let mut __qail_params = qail_sqlx::Params::new();
                    #(#param_inits)*
                    executor.qail_execute_with(#body, &__qail_params).await?;
                    Ok(())
                }
            }
        } else {
            // Get return type info
            let return_type_str = match &query_def.return_type {
                Some(qail_core::parser::query_file::ReturnType::Single(t)) => t.clone(),
                Some(qail_core::parser::query_file::ReturnType::Vec(t)) => t.clone(),
                Some(qail_core::parser::query_file::ReturnType::Option(t)) => t.clone(),
                None => "()".to_string(),
            };
            let return_type_ident = syn::Ident::new(&return_type_str, proc_macro2::Span::call_site());
            
            match &query_def.return_type {
                Some(qail_core::parser::query_file::ReturnType::Single(_)) => {
                    quote! {
                        /// Generated from query file
                        pub async fn #name<'c, E>(executor: E, #(#params),*) -> Result<#return_type_ident, sqlx::Error>
                        where
                            E: sqlx::Executor<'c, Database = sqlx::Postgres>,
                        {
                            use qail_sqlx::prelude::*;
                            let mut __qail_params = qail_sqlx::Params::new();
                            #(#param_inits)*
                            executor.qail_one_with::<#return_type_ident>(#body, &__qail_params).await
                        }
                    }
                }
                Some(qail_core::parser::query_file::ReturnType::Vec(_)) => {
                    quote! {
                        /// Generated from query file
                        pub async fn #name<'c, E>(executor: E, #(#params),*) -> Result<Vec<#return_type_ident>, sqlx::Error>
                        where
                            E: sqlx::Executor<'c, Database = sqlx::Postgres>,
                        {
                            use qail_sqlx::prelude::*;
                            let mut __qail_params = qail_sqlx::Params::new();
                            #(#param_inits)*
                            executor.qail_with::<#return_type_ident>(#body, &__qail_params).await
                        }
                    }
                }
                Some(qail_core::parser::query_file::ReturnType::Option(_)) => {
                    quote! {
                        /// Generated from query file
                        pub async fn #name<'c, E>(executor: E, #(#params),*) -> Result<Option<#return_type_ident>, sqlx::Error>
                        where
                            E: sqlx::Executor<'c, Database = sqlx::Postgres>,
                        {
                            use qail_sqlx::prelude::*;
                            let mut __qail_params = qail_sqlx::Params::new();
                            #(#param_inits)*
                            executor.qail_optional_with::<#return_type_ident>(#body, &__qail_params).await
                        }
                    }
                }
                None => {
                    quote! {
                        /// Generated from query file
                        pub async fn #name<'c, E>(executor: E, #(#params),*) -> Result<(), sqlx::Error>
                        where
                            E: sqlx::Executor<'c, Database = sqlx::Postgres>,
                        {
                            use qail_sqlx::prelude::*;
                            let mut __qail_params = qail_sqlx::Params::new();
                            #(#param_inits)*
                            executor.qail_execute_with(#body, &__qail_params).await?;
                            Ok(())
                        }
                    }
                }
            }
        };
        
        functions.push(func);
    }
    
    let output = quote! {
        #(#functions)*
    };
    
    output.into()
}
