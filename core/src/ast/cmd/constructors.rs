//! Static constructor methods for Qail.
//!
//! Methods like get(), set(), add(), del(), make(), etc.

use crate::ast::{Action, Qail};

impl Qail {
    /// SELECT — query rows.
    pub fn get(table: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Raw SQL pass-through.
    pub fn raw_sql(sql: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: sql.into(),
            ..Default::default()
        }
    }

    /// UPDATE — modify rows.
    pub fn set(table: impl Into<String>) -> Self {
        Self {
            action: Action::Set,
            table: table.into(),
            ..Default::default()
        }
    }

    /// DELETE — remove rows.
    pub fn del(table: impl Into<String>) -> Self {
        Self {
            action: Action::Del,
            table: table.into(),
            ..Default::default()
        }
    }

    /// INSERT — add rows.
    pub fn add(table: impl Into<String>) -> Self {
        Self {
            action: Action::Add,
            table: table.into(),
            ..Default::default()
        }
    }

    /// UPSERT — insert or update.
    pub fn put(table: impl Into<String>) -> Self {
        Self {
            action: Action::Put,
            table: table.into(),
            ..Default::default()
        }
    }

    /// COPY … TO — export data.
    pub fn export(table: impl Into<String>) -> Self {
        Self {
            action: Action::Export,
            table: table.into(),
            ..Default::default()
        }
    }

    /// CREATE TABLE.
    pub fn make(table: impl Into<String>) -> Self {
        Self {
            action: Action::Make,
            table: table.into(),
            ..Default::default()
        }
    }

    /// TRUNCATE — empty a table.
    pub fn truncate(table: impl Into<String>) -> Self {
        Self {
            action: Action::Truncate,
            table: table.into(),
            ..Default::default()
        }
    }

    /// EXPLAIN — show query plan.
    pub fn explain(table: impl Into<String>) -> Self {
        Self {
            action: Action::Explain,
            table: table.into(),
            ..Default::default()
        }
    }

    /// EXPLAIN ANALYZE — show query plan with execution stats.
    pub fn explain_analyze(table: impl Into<String>) -> Self {
        Self {
            action: Action::ExplainAnalyze,
            table: table.into(),
            ..Default::default()
        }
    }

    /// LOCK TABLE.
    pub fn lock(table: impl Into<String>) -> Self {
        Self {
            action: Action::Lock,
            table: table.into(),
            ..Default::default()
        }
    }

    /// CREATE MATERIALIZED VIEW.
    pub fn create_materialized_view(name: impl Into<String>, query: Qail) -> Self {
        Self {
            action: Action::CreateMaterializedView,
            table: name.into(),
            source_query: Some(Box::new(query)),
            ..Default::default()
        }
    }

    /// REFRESH MATERIALIZED VIEW.
    pub fn refresh_materialized_view(name: impl Into<String>) -> Self {
        Self {
            action: Action::RefreshMaterializedView,
            table: name.into(),
            ..Default::default()
        }
    }

    /// DROP MATERIALIZED VIEW.
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

    // PostgreSQL Procedural Commands

    /// Create a CALL command to invoke a stored procedure.
    ///
    /// # Example
    /// ```ignore
    /// let cmd = Qail::call("refresh_materialized_views()");
    /// // Generates: CALL refresh_materialized_views()
    /// ```
    pub fn call(procedure: impl Into<String>) -> Self {
        Self {
            action: Action::Call,
            table: procedure.into(),
            ..Default::default()
        }
    }

    /// Create a DO command to execute an anonymous code block.
    ///
    /// # Example
    /// ```ignore
    /// let cmd = Qail::do_block("BEGIN RAISE NOTICE 'hello'; END;", "plpgsql");
    /// // Generates: DO $$ BEGIN RAISE NOTICE 'hello'; END; $$ LANGUAGE plpgsql
    /// ```
    pub fn do_block(body: impl Into<String>, language: impl Into<String>) -> Self {
        Self {
            action: Action::Do,
            payload: Some(body.into()),
            table: language.into(),
            ..Default::default()
        }
    }

    // PostgreSQL Session Commands

    /// Create a SET command for session variables.
    ///
    /// # Example
    /// ```ignore
    /// let cmd = Qail::session_set("statement_timeout", "5000");
    /// // Generates: SET statement_timeout = '5000'
    /// ```
    pub fn session_set(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            action: Action::SessionSet,
            table: key.into(),
            payload: Some(value.into()),
            ..Default::default()
        }
    }

    /// Create a SHOW command to inspect a session variable.
    ///
    /// # Example
    /// ```ignore
    /// let cmd = Qail::session_show("statement_timeout");
    /// // Generates: SHOW statement_timeout
    /// ```
    pub fn session_show(key: impl Into<String>) -> Self {
        Self {
            action: Action::SessionShow,
            table: key.into(),
            ..Default::default()
        }
    }

    /// Create a RESET command to restore a session variable to default.
    ///
    /// # Example
    /// ```ignore
    /// let cmd = Qail::session_reset("statement_timeout");
    /// // Generates: RESET statement_timeout
    /// ```
    pub fn session_reset(key: impl Into<String>) -> Self {
        Self {
            action: Action::SessionReset,
            table: key.into(),
            ..Default::default()
        }
    }
}
