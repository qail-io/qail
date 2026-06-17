//! QAIL Flow Ledger.
//!
//! PostgreSQL storage backend for `qail-workflow`.
//!
//! This crate keeps application side effects in the app executor while backing
//! workflow state, leases, operation idempotency, side-effect replay, and
//! timeout discovery with PostgreSQL tables. All database operations are built
//! as QAIL AST commands and executed through `qail-pg`; the crate does not use
//! raw SQL execution paths.
//!
//! Default tables:
//!
//! - `qail_workflow_states`
//! - `qail_workflow_leases`
//! - `qail_workflow_operations`
//! - `qail_workflow_side_effects`

#![deny(warnings)]
#![deny(clippy::all)]
#![deny(unused_imports)]
#![deny(dead_code)]

mod executor;
mod store;
mod tables;
mod util;

pub use executor::PgWorkflowExecutor;
pub use store::PgWorkflowStore;
pub use tables::PgWorkflowTables;

#[cfg(test)]
mod tests {
    #[test]
    fn crate_does_not_call_raw_sql_execution_paths() {
        let source = concat!(
            include_str!("executor.rs"),
            include_str!("store.rs"),
            include_str!("tables.rs"),
            include_str!("util.rs")
        );
        let forbidden = [concat!("execute", "_simple("), concat!("simple", "_query(")];

        for needle in forbidden {
            assert!(
                !source.contains(needle),
                "qail-workflow-postgres must use QAIL AST operations, found {needle}"
            );
        }
    }
}
