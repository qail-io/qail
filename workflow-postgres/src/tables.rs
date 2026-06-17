use qail_core::ast::{Constraint, Expr, Qail, TableConstraint};

/// Table names used by [`crate::PgWorkflowStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgWorkflowTables {
    /// Persisted workflow context table.
    pub states: String,
    /// Per-workflow lease table.
    pub leases: String,
    /// Workflow operation idempotency table.
    pub operations: String,
    /// Side-effect idempotency table.
    pub side_effects: String,
}

impl Default for PgWorkflowTables {
    fn default() -> Self {
        Self {
            states: "qail_workflow_states".to_string(),
            leases: "qail_workflow_leases".to_string(),
            operations: "qail_workflow_operations".to_string(),
            side_effects: "qail_workflow_side_effects".to_string(),
        }
    }
}

impl PgWorkflowTables {
    /// Return QAIL AST commands that create the workflow storage tables.
    ///
    /// These commands are intentionally plain `CREATE TABLE` ASTs. Run them in
    /// a migration or fresh database bootstrap path; existing installations
    /// should use normal QAIL migrations for schema changes.
    pub fn schema_commands(&self) -> Vec<Qail> {
        vec![
            states_schema(&self.states),
            leases_schema(&self.leases),
            operations_schema(&self.operations),
            side_effects_schema(&self.side_effects),
        ]
    }
}

fn states_schema(table: &str) -> Qail {
    Qail::make(table).columns_expr([
        def_pk("workflow_id", "text"),
        def_nullable("definition_name", "text"),
        def_nullable("definition_version", "text"),
        def("current_state", "text"),
        def("context", "jsonb"),
        def_nullable("wait_event", "text"),
        def_nullable("wait_deadline_at", "text"),
        def_nullable("timeout_claimed_until", "text"),
        def("created_at", "text"),
        def("updated_at", "text"),
    ])
}

fn leases_schema(table: &str) -> Qail {
    Qail::make(table).columns_expr([
        def_pk("workflow_id", "text"),
        def("owner", "text"),
        def("expires_at", "text"),
        def("updated_at", "text"),
    ])
}

fn operations_schema(table: &str) -> Qail {
    let mut cmd = Qail::make(table).columns_expr([
        def("workflow_name", "text"),
        def("workflow_id", "text"),
        def("idempotency_key", "text"),
        def("kind", "text"),
        def("status", "text"),
        def_nullable("state", "text"),
        def_nullable("error", "text"),
        def("created_at", "text"),
        def("updated_at", "text"),
    ]);
    cmd.table_constraints.push(TableConstraint::PrimaryKey(vec![
        "workflow_id".to_string(),
        "idempotency_key".to_string(),
    ]));
    cmd
}

fn side_effects_schema(table: &str) -> Qail {
    Qail::make(table).columns_expr([
        def_pk("operation_id", "text"),
        def("workflow_id", "text"),
        def("state", "text"),
        def("step_path", "text"),
        def("kind", "text"),
        def("status", "text"),
        def_nullable("result", "jsonb"),
        def("created_at", "text"),
        def("updated_at", "text"),
    ])
}

fn def(name: &str, data_type: &str) -> Expr {
    Expr::Def {
        name: name.to_string(),
        data_type: data_type.to_string(),
        constraints: Vec::new(),
    }
}

fn def_pk(name: &str, data_type: &str) -> Expr {
    Expr::Def {
        name: name.to_string(),
        data_type: data_type.to_string(),
        constraints: vec![Constraint::PrimaryKey],
    }
}

fn def_nullable(name: &str, data_type: &str) -> Expr {
    Expr::Def {
        name: name.to_string(),
        data_type: data_type.to_string(),
        constraints: vec![Constraint::Nullable],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_commands_cover_runtime_tables() {
        let tables = PgWorkflowTables::default();
        let commands = tables.schema_commands();

        assert_eq!(commands.len(), 4);
        assert_eq!(commands[0].table, "qail_workflow_states");
        assert_eq!(commands[1].table, "qail_workflow_leases");
        assert_eq!(commands[2].table, "qail_workflow_operations");
        assert_eq!(commands[3].table, "qail_workflow_side_effects");
    }

    #[test]
    fn operations_schema_uses_composite_operation_key() {
        let tables = PgWorkflowTables::default();
        let commands = tables.schema_commands();

        assert_eq!(
            commands[2].table_constraints,
            vec![TableConstraint::PrimaryKey(vec![
                "workflow_id".to_string(),
                "idempotency_key".to_string()
            ])]
        );
    }

    #[test]
    fn schema_commands_encode_through_pg_ast_encoder() {
        for cmd in PgWorkflowTables::default().schema_commands() {
            qail_pg::protocol::AstEncoder::encode_cmd_sql(&cmd)
                .expect("workflow schema command must encode through the AST path");
        }
    }
}
