//! Semantic invariant property tests.
//!
//! Tests that ensure the transpiler and other components
//! maintain expected properties under random inputs.

use crate::ast::*;
use crate::proptest::arbitrary::*;
use crate::transpiler::ToSql;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// ToSql should never panic for valid Qail GET queries
    #[test]
    fn to_sql_doesnt_panic(qail in arb_qail_get()) {
        let _ = qail.to_sql();
    }

    /// ToSql output should be non-empty for GET queries
    #[test]
    fn to_sql_produces_output(qail in arb_qail_get()) {
        let sql = qail.to_sql();
        prop_assert!(!sql.is_empty(), "SQL output should not be empty");
    }

    /// Generated identifiers should be properly escaped (no SQL injection)
    #[test]
    fn identifiers_are_safe(ident in arb_identifier()) {
        // Valid identifiers shouldn't contain dangerous characters
        prop_assert!(!ident.contains(';'), "Identifier should not contain semicolon");
        prop_assert!(!ident.contains('\''), "Identifier should not contain single quote");
        prop_assert!(!ident.contains('"'), "Identifier should not contain double quote");
        prop_assert!(!ident.contains('\0'), "Identifier should not contain null byte");
    }

    /// Operator needs_value should be consistent with sql_symbol
    #[test]
    fn operator_needs_value_consistency(op in arb_operator()) {
        let symbol = op.sql_symbol();
        let needs_val = op.needs_value();
        
        // IS NULL, IS NOT NULL shouldn't need values
        if symbol == "IS NULL" || symbol == "IS NOT NULL" {
            prop_assert!(!needs_val, "{} should not need a value", symbol);
        }
    }

    /// Aggregate functions should have valid Display output
    #[test]
    fn aggregate_func_has_sql_name(func in arb_aggregate_func()) {
        let name = func.to_string();
        prop_assert!(!name.is_empty(), "Aggregate function name should not be empty");
        prop_assert!(name.chars().all(|c| c.is_ascii_uppercase() || c == '_'), 
            "Aggregate function name should be uppercase: {}", name);
    }

    /// Qail with Get action should produce SELECT SQL
    #[test]
    fn get_action_produces_select(
        table in arb_identifier(),
        columns in proptest::collection::vec(arb_expr_leaf(), 1..3),
    ) {
        let qail = Qail {
            action: Action::Get,
            table,
            columns,
            ..Default::default()
        };
        let sql = qail.to_sql();
        prop_assert!(sql.starts_with("SELECT"), "GET should produce SELECT, got: {}", sql);
    }

    /// Qail with Del action should produce DELETE SQL
    #[test]
    fn del_action_produces_delete(table in arb_identifier()) {
        let qail = Qail {
            action: Action::Del,
            table,
            ..Default::default()
        };
        let sql = qail.to_sql();
        prop_assert!(sql.starts_with("DELETE"), "DEL should produce DELETE, got: {}", sql);
    }
}
