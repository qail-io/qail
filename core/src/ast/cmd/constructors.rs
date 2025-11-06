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

    // PostgreSQL Pub/Sub (LISTEN/NOTIFY)
    
    /// Create a LISTEN command to subscribe to a channel.
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = Qail::listen("orders");
    /// // Generates: LISTEN orders
    /// ```
    pub fn listen(channel: impl Into<String>) -> Self {
        Self {
            action: Action::Listen,
            channel: Some(channel.into()),
            ..Default::default()
        }
    }

    /// Create an UNLISTEN command to unsubscribe from a channel.
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = Qail::unlisten("orders");
    /// // Generates: UNLISTEN orders
    /// ```
    pub fn unlisten(channel: impl Into<String>) -> Self {
        Self {
            action: Action::Unlisten,
            channel: Some(channel.into()),
            ..Default::default()
        }
    }

    /// Create a NOTIFY command to send a message to a channel.
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = Qail::notify("orders", "new_order:123");
    /// // Generates: NOTIFY orders, 'new_order:123'
    /// ```
    pub fn notify(channel: impl Into<String>, payload: impl Into<String>) -> Self {
        Self {
            action: Action::Notify,
            channel: Some(channel.into()),
            payload: Some(payload.into()),
            ..Default::default()
        }
    }
}
