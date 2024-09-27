//! Static constructor methods for Qail.
//!
//! Methods like get(), set(), add(), del(), make(), etc.

use crate::ast::{Action, Qail};

impl Qail {
    pub fn get(table: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn raw_sql(sql: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: sql.into(),
            ..Default::default()
        }
    }

    pub fn set(table: impl Into<String>) -> Self {
        Self {
            action: Action::Set,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn del(table: impl Into<String>) -> Self {
        Self {
            action: Action::Del,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn add(table: impl Into<String>) -> Self {
        Self {
            action: Action::Add,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn put(table: impl Into<String>) -> Self {
        Self {
            action: Action::Put,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn export(table: impl Into<String>) -> Self {
        Self {
            action: Action::Export,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn make(table: impl Into<String>) -> Self {
        Self {
            action: Action::Make,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn truncate(table: impl Into<String>) -> Self {
        Self {
            action: Action::Truncate,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn explain(table: impl Into<String>) -> Self {
        Self {
            action: Action::Explain,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn explain_analyze(table: impl Into<String>) -> Self {
        Self {
            action: Action::ExplainAnalyze,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn lock(table: impl Into<String>) -> Self {
        Self {
            action: Action::Lock,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn create_materialized_view(name: impl Into<String>, query: Qail) -> Self {
        Self {
            action: Action::CreateMaterializedView,
            table: name.into(),
            source_query: Some(Box::new(query)),
            ..Default::default()
        }
    }

    pub fn refresh_materialized_view(name: impl Into<String>) -> Self {
        Self {
            action: Action::RefreshMaterializedView,
            table: name.into(),
            ..Default::default()
        }
    }

    pub fn drop_materialized_view(name: impl Into<String>) -> Self {
        Self {
            action: Action::DropMaterializedView,
            table: name.into(),
            ..Default::default()
        }
    }

    // ========== Redis Operations ==========
    // "Redis stores time — QAIL decides."

    /// Create a Redis GET command.
    /// 
    /// ```ignore
    /// Qail::redis_get("session:123")
    /// ```
    pub fn redis_get(key: impl Into<String>) -> Self {
        Self {
            action: Action::RedisGet,
            table: key.into(),
            ..Default::default()
        }
    }

    /// Create a Redis SET command.
    /// Use `.redis_ex()` or `.redis_px()` to add TTL.
    /// 
    /// ```ignore
    /// Qail::redis_set("key", "value").redis_ex(3600)
    /// ```
    pub fn redis_set(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            action: Action::RedisSet,
            table: key.into(),
            raw_value: Some(value.into()),
            ..Default::default()
        }
    }

    /// Create a Redis DEL command.
    pub fn redis_del(key: impl Into<String>) -> Self {
        Self {
            action: Action::RedisDel,
            table: key.into(),
            ..Default::default()
        }
    }

    /// Create a Redis INCR command.
    pub fn redis_incr(key: impl Into<String>) -> Self {
        Self {
            action: Action::RedisIncr,
            table: key.into(),
            ..Default::default()
        }
    }

    /// Create a Redis DECR command.
    pub fn redis_decr(key: impl Into<String>) -> Self {
        Self {
            action: Action::RedisDecr,
            table: key.into(),
            ..Default::default()
        }
    }

    /// Create a Redis TTL command.
    pub fn redis_ttl(key: impl Into<String>) -> Self {
        Self {
            action: Action::RedisTtl,
            table: key.into(),
            ..Default::default()
        }
    }

    /// Create a Redis EXPIRE command.
    pub fn redis_expire(key: impl Into<String>, seconds: i64) -> Self {
        Self {
            action: Action::RedisExpire,
            table: key.into(),
            redis_ttl: Some(seconds),
            ..Default::default()
        }
    }

    /// Create a Redis EXISTS command.
    pub fn redis_exists(key: impl Into<String>) -> Self {
        Self {
            action: Action::RedisExists,
            table: key.into(),
            ..Default::default()
        }
    }

    /// Create a Redis PING command.
    pub fn redis_ping() -> Self {
        Self {
            action: Action::RedisPing,
            ..Default::default()
        }
    }
}
