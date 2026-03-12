//! DDL (Data Definition Language) encoders.
//!
//! CREATE TABLE, CREATE INDEX, DROP, ALTER statements.

use bytes::BytesMut;
use qail_core::ast::{Constraint, Expr, Qail, TableConstraint};

/// Map QAIL types to PostgreSQL types.
#[inline]
pub fn map_type(t: &str) -> &'static str {
    match t {
        "str" | "text" | "string" | "TEXT" => "TEXT",
        "int" | "i32" | "INT" | "INTEGER" => "INT",
        "bigint" | "i64" | "BIGINT" => "BIGINT",
        "uuid" | "UUID" => "UUID",
        "bool" | "boolean" | "BOOLEAN" => "BOOLEAN",
        "dec" | "decimal" | "DECIMAL" => "DECIMAL",
        "float" | "f64" | "DOUBLE PRECISION" => "DOUBLE PRECISION",
        "serial" | "SERIAL" => "SERIAL",
        "bigserial" | "BIGSERIAL" => "BIGSERIAL",
        "timestamp" | "time" | "TIMESTAMP" => "TIMESTAMP",
        "timestamptz" | "TIMESTAMPTZ" => "TIMESTAMPTZ",
        "date" | "DATE" => "DATE",
        "json" | "jsonb" | "JSON" | "JSONB" => "JSONB",
        "varchar" | "VARCHAR" => "VARCHAR(255)",
        _ => "TEXT",
    }
}

/// Encode CREATE TABLE statement.
pub fn encode_make(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"CREATE TABLE ");
    buf.extend_from_slice(cmd.table.as_bytes());
    buf.extend_from_slice(b" (");

    let mut first = true;
    for col in &cmd.columns {
        if let Expr::Def {
            name,
            data_type,
            constraints,
        } = col
        {
            if !first {
                buf.extend_from_slice(b", ");
            }
            first = false;

            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(map_type(data_type).as_bytes());

            // Default to NOT NULL unless Nullable
            if !constraints.contains(&Constraint::Nullable) {
                buf.extend_from_slice(b" NOT NULL");
            }

            // DEFAULT
            for constraint in constraints {
                if let Constraint::Default(val) = constraint {
                    buf.extend_from_slice(b" DEFAULT ");
                    let sql_default = match val.as_str() {
                        "uuid()" => "gen_random_uuid()",
                        "now()" => "NOW()",
                        other => other,
                    };
                    buf.extend_from_slice(sql_default.as_bytes());
                }
            }

            // PRIMARY KEY
            if constraints.contains(&Constraint::PrimaryKey) {
                buf.extend_from_slice(b" PRIMARY KEY");
            }

            // UNIQUE
            if constraints.contains(&Constraint::Unique) {
                buf.extend_from_slice(b" UNIQUE");
            }

            // REFERENCES (foreign key)
            for constraint in constraints {
                if let Constraint::References(target) = constraint {
                    buf.extend_from_slice(b" REFERENCES ");
                    buf.extend_from_slice(target.as_bytes());
                }
            }

            // CHECK constraint
            for constraint in constraints {
                if let Constraint::Check(vals) = constraint {
                    buf.extend_from_slice(b" CHECK (");
                    buf.extend_from_slice(name.as_bytes());
                    buf.extend_from_slice(b" IN (");
                    for (i, v) in vals.iter().enumerate() {
                        if i > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        buf.extend_from_slice(b"'");
                        buf.extend_from_slice(v.as_bytes());
                        buf.extend_from_slice(b"'");
                    }
                    buf.extend_from_slice(b"))");
                }
            }
        }
    }

    // Table constraints
    for tc in &cmd.table_constraints {
        buf.extend_from_slice(b", ");
        match tc {
            TableConstraint::Unique(cols) => {
                buf.extend_from_slice(b"UNIQUE (");
                for (i, col) in cols.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    buf.extend_from_slice(col.as_bytes());
                }
                buf.extend_from_slice(b")");
            }
            TableConstraint::PrimaryKey(cols) => {
                buf.extend_from_slice(b"PRIMARY KEY (");
                for (i, col) in cols.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    buf.extend_from_slice(col.as_bytes());
                }
                buf.extend_from_slice(b")");
            }
        }
    }

    buf.extend_from_slice(b")");
}

/// Encode CREATE INDEX statement.
pub fn encode_index(cmd: &Qail, buf: &mut BytesMut) {
    if let Some(idx) = &cmd.index_def {
        if idx.unique {
            buf.extend_from_slice(b"CREATE UNIQUE INDEX ");
        } else {
            buf.extend_from_slice(b"CREATE INDEX ");
        }
        buf.extend_from_slice(idx.name.as_bytes());
        buf.extend_from_slice(b" ON ");
        buf.extend_from_slice(idx.table.as_bytes());
        buf.extend_from_slice(b" (");
        for (i, col) in idx.columns.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            buf.extend_from_slice(col.as_bytes());
        }
        buf.extend_from_slice(b")");
    }
}

/// Encode DROP TABLE statement.
pub fn encode_drop_table(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP TABLE IF EXISTS ");
    buf.extend_from_slice(cmd.table.as_bytes());
}

/// Encode DROP INDEX statement.
pub fn encode_drop_index(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP INDEX IF EXISTS ");
    buf.extend_from_slice(cmd.table.as_bytes());
}

/// Encode ALTER TABLE ADD COLUMN statement.
pub fn encode_alter_add_column(cmd: &Qail, buf: &mut BytesMut) {
    for col in &cmd.columns {
        if let Expr::Def {
            name,
            data_type,
            constraints,
        } = col
        {
            buf.extend_from_slice(b"ALTER TABLE ");
            buf.extend_from_slice(cmd.table.as_bytes());
            buf.extend_from_slice(b" ADD COLUMN ");
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(map_type(data_type).as_bytes());

            if !constraints.contains(&Constraint::Nullable) {
                buf.extend_from_slice(b" NOT NULL");
            }

            for constraint in constraints {
                if let Constraint::Default(val) = constraint {
                    buf.extend_from_slice(b" DEFAULT ");
                    let sql_default = match val.as_str() {
                        "uuid()" => "gen_random_uuid()",
                        "now()" => "NOW()",
                        other => other,
                    };
                    buf.extend_from_slice(sql_default.as_bytes());
                }
            }
        }
    }
}

/// Encode ALTER TABLE DROP COLUMN statement.
pub fn encode_alter_drop_column(cmd: &Qail, buf: &mut BytesMut) {
    for col in &cmd.columns {
        let col_name = match col {
            Expr::Named(n) => n.clone(),
            Expr::Def { name, .. } => name.clone(),
            _ => continue,
        };
        buf.extend_from_slice(b"ALTER TABLE ");
        buf.extend_from_slice(cmd.table.as_bytes());
        buf.extend_from_slice(b" DROP COLUMN ");
        buf.extend_from_slice(col_name.as_bytes());
    }
}

/// Encode ALTER TABLE ALTER COLUMN TYPE statement.
pub fn encode_alter_column_type(cmd: &Qail, buf: &mut BytesMut) {
    for col in &cmd.columns {
        if let Expr::Def {
            name, data_type, ..
        } = col
        {
            buf.extend_from_slice(b"ALTER TABLE ");
            buf.extend_from_slice(cmd.table.as_bytes());
            buf.extend_from_slice(b" ALTER COLUMN ");
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(b" TYPE ");
            buf.extend_from_slice(map_type(data_type).as_bytes());
        }
    }
}

/// Encode ALTER TABLE RENAME COLUMN statement.
/// The `Mod` action stores renames as `Expr::Named("old_name -> new_name")`.
pub fn encode_rename_column(cmd: &Qail, buf: &mut BytesMut) {
    for col in &cmd.columns {
        if let Expr::Named(rename_str) = col {
            // Parse "old_name -> new_name" format
            if let Some((old, new)) = rename_str.split_once(" -> ") {
                buf.extend_from_slice(b"ALTER TABLE ");
                buf.extend_from_slice(cmd.table.as_bytes());
                buf.extend_from_slice(b" RENAME COLUMN ");
                buf.extend_from_slice(old.trim().as_bytes());
                buf.extend_from_slice(b" TO ");
                buf.extend_from_slice(new.trim().as_bytes());
            }
        }
    }
}

/// Encode CREATE VIEW statement.
/// CREATE VIEW name AS SELECT ...
pub fn encode_create_view(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
    buf.extend_from_slice(b"CREATE VIEW ");
    buf.extend_from_slice(cmd.table.as_bytes());
    buf.extend_from_slice(b" AS ");

    // The source_query contains the SELECT statement for the view
    if let Some(ref source) = cmd.source_query {
        super::dml::encode_select(source, buf, params)?;
    }
    Ok(())
}

/// Encode DROP VIEW statement.
pub fn encode_drop_view(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP VIEW IF EXISTS ");
    buf.extend_from_slice(cmd.table.as_bytes());
}

/// Encode ALTER TABLE ALTER COLUMN SET NOT NULL.
pub fn encode_alter_set_not_null(cmd: &Qail, buf: &mut BytesMut) {
    if let Some(Expr::Named(col)) = cmd.columns.first() {
        buf.extend_from_slice(b"ALTER TABLE ");
        buf.extend_from_slice(cmd.table.as_bytes());
        buf.extend_from_slice(b" ALTER COLUMN ");
        buf.extend_from_slice(col.as_bytes());
        buf.extend_from_slice(b" SET NOT NULL");
    }
}

/// Encode ALTER TABLE ALTER COLUMN DROP NOT NULL.
pub fn encode_alter_drop_not_null(cmd: &Qail, buf: &mut BytesMut) {
    if let Some(Expr::Named(col)) = cmd.columns.first() {
        buf.extend_from_slice(b"ALTER TABLE ");
        buf.extend_from_slice(cmd.table.as_bytes());
        buf.extend_from_slice(b" ALTER COLUMN ");
        buf.extend_from_slice(col.as_bytes());
        buf.extend_from_slice(b" DROP NOT NULL");
    }
}

/// Encode ALTER TABLE ALTER COLUMN SET DEFAULT.
pub fn encode_alter_set_default(cmd: &Qail, buf: &mut BytesMut) {
    if let Some(Expr::Named(col)) = cmd.columns.first() {
        buf.extend_from_slice(b"ALTER TABLE ");
        buf.extend_from_slice(cmd.table.as_bytes());
        buf.extend_from_slice(b" ALTER COLUMN ");
        buf.extend_from_slice(col.as_bytes());
        buf.extend_from_slice(b" SET DEFAULT ");
        let default_expr = cmd.payload.as_deref().unwrap_or("NULL");
        buf.extend_from_slice(default_expr.as_bytes());
    }
}

/// Encode ALTER TABLE ALTER COLUMN DROP DEFAULT.
pub fn encode_alter_drop_default(cmd: &Qail, buf: &mut BytesMut) {
    if let Some(Expr::Named(col)) = cmd.columns.first() {
        buf.extend_from_slice(b"ALTER TABLE ");
        buf.extend_from_slice(cmd.table.as_bytes());
        buf.extend_from_slice(b" ALTER COLUMN ");
        buf.extend_from_slice(col.as_bytes());
        buf.extend_from_slice(b" DROP DEFAULT");
    }
}

/// Encode ALTER TABLE ENABLE ROW LEVEL SECURITY.
pub fn encode_alter_enable_rls(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"ALTER TABLE ");
    buf.extend_from_slice(cmd.table.as_bytes());
    buf.extend_from_slice(b" ENABLE ROW LEVEL SECURITY");
}

/// Encode ALTER TABLE DISABLE ROW LEVEL SECURITY.
pub fn encode_alter_disable_rls(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"ALTER TABLE ");
    buf.extend_from_slice(cmd.table.as_bytes());
    buf.extend_from_slice(b" DISABLE ROW LEVEL SECURITY");
}

/// Encode ALTER TABLE FORCE ROW LEVEL SECURITY.
pub fn encode_alter_force_rls(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"ALTER TABLE ");
    buf.extend_from_slice(cmd.table.as_bytes());
    buf.extend_from_slice(b" FORCE ROW LEVEL SECURITY");
}

/// Encode ALTER TABLE NO FORCE ROW LEVEL SECURITY.
pub fn encode_alter_no_force_rls(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"ALTER TABLE ");
    buf.extend_from_slice(cmd.table.as_bytes());
    buf.extend_from_slice(b" NO FORCE ROW LEVEL SECURITY");
}

// ── Session & procedural commands ──────────────────────────────────

/// Encode CALL procedure_name.
pub fn encode_call(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"CALL ");
    buf.extend_from_slice(cmd.table.as_bytes());
}

/// Encode DO $$ body $$ LANGUAGE lang.
pub fn encode_do(cmd: &Qail, buf: &mut BytesMut) {
    let body = cmd.payload.as_deref().unwrap_or("");
    let lang = if cmd.table.is_empty() {
        "plpgsql"
    } else {
        &cmd.table
    };
    buf.extend_from_slice(b"DO $$ ");
    buf.extend_from_slice(body.as_bytes());
    buf.extend_from_slice(b" $$ LANGUAGE ");
    buf.extend_from_slice(lang.as_bytes());
}

/// Encode SET key = 'value'.
///
/// The value is escaped: `'` → `''` to prevent SQL injection.
pub fn encode_session_set(cmd: &Qail, buf: &mut BytesMut) {
    let value = cmd.payload.as_deref().unwrap_or("");
    let escaped = value.replace('\'', "''");
    buf.extend_from_slice(b"SET ");
    buf.extend_from_slice(cmd.table.as_bytes());
    buf.extend_from_slice(b" = '");
    buf.extend_from_slice(escaped.as_bytes());
    buf.extend_from_slice(b"'");
}

/// Encode SHOW key.
pub fn encode_session_show(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"SHOW ");
    buf.extend_from_slice(cmd.table.as_bytes());
}

/// Encode RESET key.
pub fn encode_session_reset(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"RESET ");
    buf.extend_from_slice(cmd.table.as_bytes());
}

/// Encode CREATE DATABASE name.
pub fn encode_create_database(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"CREATE DATABASE ");
    buf.extend_from_slice(cmd.table.as_bytes());
}

/// Encode DROP DATABASE IF EXISTS name.
pub fn encode_drop_database(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP DATABASE IF EXISTS ");
    buf.extend_from_slice(cmd.table.as_bytes());
}

// ── Pub/Sub commands ───────────────────────────────────────────────

/// Encode LISTEN "channel".
pub fn encode_listen(cmd: &Qail, buf: &mut BytesMut) {
    let channel = cmd.channel.as_deref().unwrap_or("");
    buf.extend_from_slice(b"LISTEN \"");
    // Escape double-quotes in channel name
    buf.extend_from_slice(channel.replace('"', "\"\"").as_bytes());
    buf.extend_from_slice(b"\"");
}

/// Encode UNLISTEN "channel".
pub fn encode_unlisten(cmd: &Qail, buf: &mut BytesMut) {
    let channel = cmd.channel.as_deref().unwrap_or("");
    buf.extend_from_slice(b"UNLISTEN \"");
    buf.extend_from_slice(channel.replace('"', "\"\"").as_bytes());
    buf.extend_from_slice(b"\"");
}

/// Encode NOTIFY "channel", 'payload'.
pub fn encode_notify(cmd: &Qail, buf: &mut BytesMut) {
    let channel = cmd.channel.as_deref().unwrap_or("");
    buf.extend_from_slice(b"NOTIFY \"");
    buf.extend_from_slice(channel.replace('"', "\"\"").as_bytes());
    buf.extend_from_slice(b"\"");
    if let Some(ref payload) = cmd.payload {
        buf.extend_from_slice(b", '");
        buf.extend_from_slice(payload.replace('\'', "''").as_bytes());
        buf.extend_from_slice(b"'");
    }
}
