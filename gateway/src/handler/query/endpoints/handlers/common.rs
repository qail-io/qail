use super::*;

pub(super) fn parse_cached_query(
    state: &Arc<GatewayState>,
    query_text: &str,
) -> Result<qail_core::ast::Qail, ApiError> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    query_text.hash(&mut h);
    let key = h.finish();

    if let Some(cached) = state.parse_cache.get(&key) {
        return Ok(cached);
    }

    match qail_core::parser::parse(query_text) {
        Ok(cmd) => {
            state.parse_cache.insert(key, cmd.clone());
            Ok(cmd)
        }
        Err(e) => {
            tracing::warn!("Parse error: {}", e);
            Err(ApiError::parse_error(format!("Parse error: {}", e)))
        }
    }
}
