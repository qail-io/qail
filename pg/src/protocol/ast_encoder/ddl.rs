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
        if let Expr::Def { name, data_type, constraints } = col {
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
        if let Expr::Def { name, data_type, constraints } = col {
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
        if let Expr::Def { name, data_type, .. } = col {
            buf.extend_from_slice(b"ALTER TABLE ");
            buf.extend_from_slice(cmd.table.as_bytes());
            buf.extend_from_slice(b" ALTER COLUMN ");
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(b" TYPE ");
            buf.extend_from_slice(map_type(data_type).as_bytes());
        }
    }
}
