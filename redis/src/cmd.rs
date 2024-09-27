//! Redis command helpers for QAIL AST.
//!
//! This module provides fluent builder methods for Redis operations.
//! All Redis commands use the unified `Qail` AST from qail-core.
//!
//! # Example
//! ```ignore
//! use qail_core::prelude::*;
//!
//! // SET with TTL
//! let cmd = Qail::redis_set("session:123", b"user_data").redis_ex(3600);
//!
//! // GET
//! let cmd = Qail::redis_get("session:123");
//! ```

use qail_core::ast::Qail;

/// Extension trait for Redis-specific fluent methods on Qail.
pub trait RedisExt {
    /// Add EX (seconds) expiry to SET command.
    fn redis_ex(self, seconds: i64) -> Self;
    
    /// Add PX (milliseconds) expiry to SET command.
    fn redis_px(self, milliseconds: i64) -> Self;
    
    /// Add NX condition (only set if not exists).
    fn redis_nx(self) -> Self;
    
    /// Add XX condition (only set if exists).
    fn redis_xx(self) -> Self;
}

impl RedisExt for Qail {
    fn redis_ex(mut self, seconds: i64) -> Self {
        self.redis_ttl = Some(seconds);
        self
    }
    
    fn redis_px(mut self, milliseconds: i64) -> Self {
        // Store as seconds for simplicity (can be extended)
        self.redis_ttl = Some(milliseconds / 1000);
        self
    }
    
    fn redis_nx(mut self) -> Self {
        self.redis_set_condition = Some("NX".to_string());
        self
    }
    
    fn redis_xx(mut self) -> Self {
        self.redis_set_condition = Some("XX".to_string());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qail_core::ast::Action;

    #[test]
    fn test_redis_get() {
        let cmd = Qail::redis_get("mykey");
        assert_eq!(cmd.action, Action::RedisGet);
        assert_eq!(cmd.table, "mykey");
    }

    #[test]
    fn test_redis_set_with_ex() {
        let cmd = Qail::redis_set("mykey", b"myvalue".to_vec()).redis_ex(3600);
        assert_eq!(cmd.action, Action::RedisSet);
        assert_eq!(cmd.table, "mykey");
        assert_eq!(cmd.redis_ttl, Some(3600));
    }

    #[test]
    fn test_redis_set_nx() {
        let cmd = Qail::redis_set("mykey", b"myvalue".to_vec()).redis_nx();
        assert_eq!(cmd.redis_set_condition, Some("NX".to_string()));
    }
}
