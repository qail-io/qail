//! ALTER TABLE Operations (AST-native)
//!
//! All ALTER TABLE operations as typed enums - no raw SQL!
//!
//! # Example
//! ```ignore
//! use qail_core::migrate::alter::{AlterTable, AlterOp};
//!
//! let alter = AlterTable::new("users")
//!     .add_column(Column::new("bio", ColumnType::Text))
//!     .drop_column("legacy_field")
//!     .rename_column("username", "handle");
//! ```

use super::schema::{CheckExpr, Column};
use super::types::ColumnType;

/// ALTER TABLE operation
#[derive(Debug, Clone)]
pub enum AlterOp {
    /// ADD COLUMN.
    AddColumn(Column),
    /// DROP COLUMN \[CASCADE\].
    DropColumn {
        /// Column name.
        name: String,
        /// Whether to CASCADE.
        cascade: bool,
    },
    /// RENAME COLUMN old TO new.
    RenameColumn {
        /// Original column name.
        from: String,
        /// New column name.
        to: String,
    },
    /// ALTER COLUMN TYPE [USING expr].
    AlterType {
        /// Column name.
        column: String,
        /// New data type.
        new_type: ColumnType,
        /// Optional USING expression for type conversion.
        using: Option<String>,
    },
    /// ALTER COLUMN SET NOT NULL.
    SetNotNull(String),
    /// ALTER COLUMN DROP NOT NULL.
    DropNotNull(String),
    /// ALTER COLUMN SET DEFAULT expr.
    SetDefault {
        /// Column name.
        column: String,
        /// Default expression.
        expr: String,
    },
    /// ALTER COLUMN DROP DEFAULT.
    DropDefault(String),
    /// ADD CONSTRAINT.
    AddConstraint {
        /// Constraint name.
        name: String,
        /// Constraint definition.
        constraint: TableConstraint,
    },
    /// DROP CONSTRAINT \[CASCADE\].
    DropConstraint {
        /// Constraint name.
        name: String,
        /// Whether to CASCADE.
        cascade: bool,
    },
    /// RENAME TO new_name.
    RenameTable(String),
    /// SET SCHEMA new_schema.
    SetSchema(String),
    /// ENABLE / DISABLE ROW LEVEL SECURITY.
    SetRowLevelSecurity(bool),
    /// FORCE / NO FORCE ROW LEVEL SECURITY.
    ForceRowLevelSecurity(bool),
}

/// Table-level constraints
#[derive(Debug, Clone)]
pub enum TableConstraint {
    /// PRIMARY KEY (columns).
    PrimaryKey(Vec<String>),
    /// UNIQUE (columns).
    Unique(Vec<String>),
    /// CHECK (expr).
    Check(CheckExpr),
    /// FOREIGN KEY (cols) REFERENCES table(ref_cols)
    ForeignKey {
        /// Source columns.
        columns: Vec<String>,
        /// Referenced table.
        ref_table: String,
        /// Referenced columns.
        ref_columns: Vec<String>,
    },
    /// EXCLUDE USING method (...)
    Exclude {
        /// Index method.
        method: String,
        /// Exclusion elements.
        elements: Vec<String>,
    },
}

/// Fluent builder for ALTER TABLE statements
#[derive(Debug, Clone)]
pub struct AlterTable {
    /// Target table.
    pub table: String,
    /// Queued operations.
    pub ops: Vec<AlterOp>,
    /// ALTER TABLE ONLY.
    pub only: bool,
    /// IF EXISTS.
    pub if_exists: bool,
}

impl AlterTable {
    /// Create a new ALTER TABLE builder
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            ops: Vec::new(),
            only: false,
            if_exists: false,
        }
    }

    /// ALTER TABLE ONLY (no child tables)
    pub fn only(mut self) -> Self {
        self.only = true;
        self
    }

    /// ALTER TABLE IF EXISTS
    pub fn if_exists(mut self) -> Self {
        self.if_exists = true;
        self
    }

    /// ADD COLUMN
    pub fn add_column(mut self, col: Column) -> Self {
        self.ops.push(AlterOp::AddColumn(col));
        self
    }

    /// DROP COLUMN
    pub fn drop_column(mut self, name: impl Into<String>) -> Self {
        self.ops.push(AlterOp::DropColumn {
            name: name.into(),
            cascade: false,
        });
        self
    }

    /// DROP COLUMN CASCADE
    pub fn drop_column_cascade(mut self, name: impl Into<String>) -> Self {
        self.ops.push(AlterOp::DropColumn {
            name: name.into(),
            cascade: true,
        });
        self
    }

    /// RENAME COLUMN old TO new
    pub fn rename_column(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.ops.push(AlterOp::RenameColumn {
            from: from.into(),
            to: to.into(),
        });
        self
    }

    /// ALTER COLUMN TYPE.
    pub fn set_type(mut self, column: impl Into<String>, new_type: ColumnType) -> Self {
        self.ops.push(AlterOp::AlterType {
            column: column.into(),
            new_type,
            using: None,
        });
        self
    }

    /// ALTER COLUMN TYPE … USING expression.
    pub fn set_type_using(
        mut self,
        column: impl Into<String>,
        new_type: ColumnType,
        using: impl Into<String>,
    ) -> Self {
        self.ops.push(AlterOp::AlterType {
            column: column.into(),
            new_type,
            using: Some(using.into()),
        });
        self
    }

    /// ALTER COLUMN SET NOT NULL
    pub fn set_not_null(mut self, column: impl Into<String>) -> Self {
        self.ops.push(AlterOp::SetNotNull(column.into()));
        self
    }

    /// ALTER COLUMN DROP NOT NULL
    pub fn drop_not_null(mut self, column: impl Into<String>) -> Self {
        self.ops.push(AlterOp::DropNotNull(column.into()));
        self
    }

    /// ALTER COLUMN SET DEFAULT.
    pub fn set_default(mut self, column: impl Into<String>, expr: impl Into<String>) -> Self {
        self.ops.push(AlterOp::SetDefault {
            column: column.into(),
            expr: expr.into(),
        });
        self
    }

    /// ALTER COLUMN DROP DEFAULT.
    pub fn drop_default(mut self, column: impl Into<String>) -> Self {
        self.ops.push(AlterOp::DropDefault(column.into()));
        self
    }

    /// ADD CONSTRAINT.
    pub fn add_constraint(mut self, name: impl Into<String>, constraint: TableConstraint) -> Self {
        self.ops.push(AlterOp::AddConstraint {
            name: name.into(),
            constraint,
        });
        self
    }

    /// DROP CONSTRAINT.
    pub fn drop_constraint(mut self, name: impl Into<String>) -> Self {
        self.ops.push(AlterOp::DropConstraint {
            name: name.into(),
            cascade: false,
        });
        self
    }

    /// DROP CONSTRAINT CASCADE.
    pub fn drop_constraint_cascade(mut self, name: impl Into<String>) -> Self {
        self.ops.push(AlterOp::DropConstraint {
            name: name.into(),
            cascade: true,
        });
        self
    }

    /// RENAME TABLE TO.
    pub fn rename_to(mut self, name: impl Into<String>) -> Self {
        self.ops.push(AlterOp::RenameTable(name.into()));
        self
    }

    /// SET SCHEMA.
    pub fn set_schema(mut self, schema: impl Into<String>) -> Self {
        self.ops.push(AlterOp::SetSchema(schema.into()));
        self
    }

    /// ENABLE ROW LEVEL SECURITY.
    pub fn enable_rls(mut self) -> Self {
        self.ops.push(AlterOp::SetRowLevelSecurity(true));
        self
    }

    /// DISABLE ROW LEVEL SECURITY.
    pub fn disable_rls(mut self) -> Self {
        self.ops.push(AlterOp::SetRowLevelSecurity(false));
        self
    }

    /// FORCE ROW LEVEL SECURITY — policies apply even to table owner.
    pub fn force_rls(mut self) -> Self {
        self.ops.push(AlterOp::ForceRowLevelSecurity(true));
        self
    }

    /// NO FORCE ROW LEVEL SECURITY — owner bypasses policies (default).
    pub fn no_force_rls(mut self) -> Self {
        self.ops.push(AlterOp::ForceRowLevelSecurity(false));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::types::ColumnType;

    #[test]
    fn test_alter_table_builder() {
        let alter = AlterTable::new("users")
            .add_column(Column::new("bio", ColumnType::Text))
            .drop_column("legacy")
            .rename_column("username", "handle")
            .set_not_null("email");

        assert_eq!(alter.table, "users");
        assert_eq!(alter.ops.len(), 4);
    }

    #[test]
    fn test_alter_type_with_using() {
        let alter = AlterTable::new("users").set_type_using("age", ColumnType::Int, "age::integer");

        match &alter.ops[0] {
            AlterOp::AlterType { column, using, .. } => {
                assert_eq!(column, "age");
                assert_eq!(using.as_ref().unwrap(), "age::integer");
            }
            _ => panic!("Expected AlterType"),
        }
    }

    #[test]
    fn test_add_constraint() {
        let alter = AlterTable::new("users")
            .add_constraint("pk_users", TableConstraint::PrimaryKey(vec!["id".into()]));

        assert_eq!(alter.ops.len(), 1);
    }
}
