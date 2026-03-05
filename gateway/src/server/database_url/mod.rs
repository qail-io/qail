mod gss;
mod parse;
mod query;
mod rpc_allow_list;

use std::collections::HashSet;
use std::path::Path;

use qail_pg::PoolConfig;

use crate::config::GatewayConfig;
use crate::error::GatewayError;

pub(super) fn parse_database_url(
    url_str: &str,
    gateway_config: &GatewayConfig,
) -> Result<PoolConfig, GatewayError> {
    parse::parse_database_url(url_str, gateway_config)
}

pub(super) fn load_rpc_allow_list(path: &Path) -> Result<HashSet<String>, GatewayError> {
    rpc_allow_list::load_rpc_allow_list(path)
}

fn parse_bool_query(value: &str) -> Option<bool> {
    query::parse_bool_query(value)
}

#[cfg(test)]
mod tests;
