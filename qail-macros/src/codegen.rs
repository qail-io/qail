//! Code generation helpers.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Ident, Expr};

/// Generate parameter binding code
pub fn generate_params_code(params: &[(Ident, Expr)]) -> TokenStream2 {
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
