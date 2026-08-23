//! Scoped LISTEN/NOTIFY channel names.
//!
//! The gateway never lets a WebSocket client LISTEN on a raw channel: a
//! `subscribe` request carries a *fragment* and the gateway derives the real
//! PostgreSQL channel from `(tenant_id, fragment)`. For a producer to reach
//! that subscriber it must emit NOTIFY on the identical derived name, so the
//! derivation lives here — one algorithm, consumed by the gateway listener and
//! by [`crate::Qail::notify_scoped`] — instead of being re-implemented in
//! application SQL.
//!
//! Two namespaces are reserved for the gateway's own live_query wake-ups and
//! are refused for manual subscription: [`LIVE_QUERY_TABLE_PREFIX`] and
//! [`LIVE_QUERY_COMPACT_PREFIX`].

use crate::rls::RlsContext;

/// PostgreSQL identifier limit (`NAMEDATALEN - 1`).
pub const PG_CHANNEL_MAX_BYTES: usize = 63;

/// Prefix of the per-table live_query wake-up channel.
pub const LIVE_QUERY_TABLE_PREFIX: &str = "qail_table_";

/// Prefix of the hashed fallback live_query channel (used when the
/// tenant-scoped name would exceed [`PG_CHANNEL_MAX_BYTES`]).
pub const LIVE_QUERY_COMPACT_PREFIX: &str = "qail_lq_";

/// Validate a client-supplied channel fragment.
///
/// ASCII alphanumeric, underscore and hyphen (UUIDs carry hyphens, and
/// `chat_<user-uuid>` is the canonical per-user pattern), non-empty, and
/// outside the gateway-reserved live_query namespaces. The gateway quotes
/// channel identifiers on LISTEN, so hyphens are safe.
pub fn validate_channel_fragment(fragment: &str) -> Result<(), String> {
    if fragment.is_empty() {
        return Err("Channel name cannot be empty".to_string());
    }
    if !fragment
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(
            "Invalid channel name — ASCII alphanumeric, underscores and hyphens only".to_string(),
        );
    }
    if fragment.starts_with(LIVE_QUERY_TABLE_PREFIX)
        || fragment.starts_with(LIVE_QUERY_COMPACT_PREFIX)
    {
        return Err(format!(
            "Channel name '{}' uses a reserved live_query namespace",
            fragment
        ));
    }
    Ok(())
}

fn validate_tenant_scope(tenant_id: &str) -> Result<(), String> {
    if tenant_id.is_empty() {
        return Err("Tenant identifier is required for scoped channel names".to_string());
    }
    if !tenant_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(
            "Tenant identifier contains unsupported characters for channel scoping".to_string(),
        );
    }
    Ok(())
}

fn ensure_pg_channel_name_limit(channel: &str) -> Result<(), String> {
    if channel.len() <= PG_CHANNEL_MAX_BYTES {
        return Ok(());
    }
    Err(format!(
        "Channel name too long for PostgreSQL LISTEN/NOTIFY ({} bytes > {} bytes)",
        channel.len(),
        PG_CHANNEL_MAX_BYTES
    ))
}

/// Which identity a channel is scoped under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelScope {
    /// Tenant-scoped (`t_` namespace).
    Tenant,
    /// User-scoped (`u_` namespace).
    User,
}

impl ChannelScope {
    fn prefix(self) -> &'static str {
        match self {
            ChannelScope::Tenant => "t",
            ChannelScope::User => "u",
        }
    }
}

/// `{scope}_{len}_{id}_{suffix}` — the scope tag keeps tenant `acme` and
/// user `acme` in different namespaces; the id length is an unambiguous
/// delimiter component (without it `("acme", "eu_orders")` and
/// `("acme_eu", "orders")` flatten to the same string).
fn scoped(scope: ChannelScope, id: &str, suffix: &str) -> Result<String, String> {
    validate_tenant_scope(id)?;
    Ok(format!("{}_{}_{}_{}", scope.prefix(), id.len(), id, suffix))
}

/// The PostgreSQL channel a manual `subscribe` with `fragment` listens on
/// under `tenant_id`. Producers NOTIFY on this exact name.
pub fn scoped_channel(tenant_id: &str, fragment: &str) -> Result<String, String> {
    scoped_channel_in(ChannelScope::Tenant, tenant_id, fragment)
}

/// The channel for `fragment` under an explicit scope type.
pub fn scoped_channel_in(scope: ChannelScope, id: &str, fragment: &str) -> Result<String, String> {
    validate_channel_fragment(fragment)?;
    let channel = scoped(scope, id, fragment)?;
    ensure_pg_channel_name_limit(&channel)?;
    Ok(channel)
}

/// Derive the scoped channel from an [`RlsContext`].
///
/// Tenant-shaped contexts scope on the tenant (`t_…`); user-only contexts
/// scope on the user id (`u_…`), so a consumer app without tenants still gets
/// per-user channels rather than a global one, and the two shapes can never
/// collide on an equal id.
pub fn scoped_channel_for(ctx: &RlsContext, fragment: &str) -> Result<String, String> {
    if ctx.has_tenant() {
        return scoped_channel_in(ChannelScope::Tenant, &ctx.tenant_id, fragment);
    }
    if ctx.has_user() {
        return scoped_channel_in(ChannelScope::User, ctx.user_id(), fragment);
    }
    Err("Scoped channel requires a tenant or user context".to_string())
}

fn stable_channel_hash(input: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// The wake-up channel the gateway's live_query listens on for `table`.
///
/// This is deliberately coarse (per table, per tenant; per table globally
/// when the context carries no tenant): the gateway never forwards its NOTIFY
/// payload to a client — it only triggers a re-fetch under the subscriber's
/// own RLS context, so what a client sees is bounded by its policies, not by
/// the channel. The cost of the coarse channel is fan-out, not leakage.
pub fn live_query_channel(tenant_id: Option<&str>, table: &str) -> Result<String, String> {
    let channel = match tenant_id {
        Some(tid) if !tid.is_empty() => {
            let scoped = scoped(
                ChannelScope::Tenant,
                tid,
                &format!("{}{}", LIVE_QUERY_TABLE_PREFIX, table),
            )?;
            if scoped.len() <= PG_CHANNEL_MAX_BYTES {
                scoped
            } else {
                format!(
                    "{}{:016x}_{:016x}",
                    LIVE_QUERY_COMPACT_PREFIX,
                    stable_channel_hash(tid),
                    stable_channel_hash(table)
                )
            }
        }
        _ => format!("{}{}", LIVE_QUERY_TABLE_PREFIX, table),
    };
    ensure_pg_channel_name_limit(&channel)?;
    Ok(channel)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_channel_is_length_delimited() {
        let a = scoped_channel("acme", "eu_orders").unwrap();
        let b = scoped_channel("acme_eu", "orders").unwrap();
        assert_ne!(a, b);
        assert_eq!(a, "t_4_acme_eu_orders");
    }

    #[test]
    fn reserved_live_query_prefixes_are_refused() {
        assert!(validate_channel_fragment("qail_table_orders").is_err());
        assert!(validate_channel_fragment("qail_lq_abc").is_err());
        assert!(validate_channel_fragment("chat_42").is_ok());
    }

    #[test]
    fn scoped_channel_for_prefers_tenant_then_user() {
        let t = RlsContext::tenant("acme").with_user("u1");
        assert_eq!(scoped_channel_for(&t, "chat").unwrap(), "t_4_acme_chat");
        let u = RlsContext::user("u1");
        assert_eq!(scoped_channel_for(&u, "chat").unwrap(), "u_2_u1_chat");
        assert!(scoped_channel_for(&RlsContext::empty(), "chat").is_err());
    }

    #[test]
    fn tenant_and_user_with_equal_ids_never_share_a_channel() {
        let t = scoped_channel_for(&RlsContext::tenant("acme"), "chat").unwrap();
        let u = scoped_channel_for(&RlsContext::user("acme"), "chat").unwrap();
        assert_ne!(t, u);
        assert!(t.starts_with("t_") && u.starts_with("u_"));
    }

    #[test]
    fn live_query_channel_compacts_when_too_long() {
        let long_tenant = "t".repeat(50);
        let c = live_query_channel(Some(&long_tenant), "orders").unwrap();
        assert!(c.starts_with(LIVE_QUERY_COMPACT_PREFIX));
        assert!(c.len() <= PG_CHANNEL_MAX_BYTES);
        assert_eq!(
            live_query_channel(None, "orders").unwrap(),
            "qail_table_orders"
        );
    }
}
