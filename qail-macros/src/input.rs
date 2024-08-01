//! Macro input parsing types.

use syn::{LitStr, Ident, Token, Expr};
use syn::parse::{Parse, ParseStream};

/// Input for qail! and qail_one! - with result type
pub struct QailQueryInput {
    pub pool: Expr,
    pub result_type: Ident,
    pub query: LitStr,
    pub params: Vec<(Ident, Expr)>,
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
pub struct QailExecuteInput {
    pub pool: Expr,
    pub query: LitStr,
    pub params: Vec<(Ident, Expr)>,
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

pub fn parse_params(input: ParseStream) -> syn::Result<Vec<(Ident, Expr)>> {
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
