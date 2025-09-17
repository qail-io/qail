//! Roundtrip property tests.
//!
//! Tests that serialization/display operations are reversible
//! and don't lose information.

use crate::ast::*;
use crate::proptest::arbitrary::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Value Display should never panic
    #[test]
    fn value_display_doesnt_panic(val in arb_value()) {
        let _ = val.to_string();
    }

    /// Value should survive JSON roundtrip (no floats due to precision)
    #[test]
    fn value_serde_roundtrip(val in arb_value_no_float()) {
        let json = serde_json::to_string(&val).unwrap();
        let parsed: Value = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(val, parsed);
    }

    /// Operator Display should never panic
    #[test]
    fn operator_display_doesnt_panic(op in arb_operator()) {
        let _ = op.sql_symbol();
    }

    /// Operator should survive JSON roundtrip
    #[test]
    fn operator_serde_roundtrip(op in arb_operator()) {
        let json = serde_json::to_string(&op).unwrap();
        let parsed: Operator = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(op, parsed);
    }

    /// Expr Display should never panic
    #[test]
    fn expr_display_doesnt_panic(expr in arb_expr()) {
        let _ = expr.to_string();
    }

    /// Expr should survive JSON roundtrip (no floats due to precision)
    #[test]
    fn expr_serde_roundtrip(expr in arb_expr_no_float()) {
        let json = serde_json::to_string(&expr).unwrap();
        let parsed: Expr = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(expr, parsed);
    }

    /// IntervalUnit Display should never panic
    #[test]
    fn interval_unit_display_doesnt_panic(unit in arb_interval_unit()) {
        let _ = unit.to_string();
    }

    /// SortOrder should survive JSON roundtrip
    #[test]
    fn sort_order_serde_roundtrip(order in arb_sort_order()) {
        let json = serde_json::to_string(&order).unwrap();
        let parsed: SortOrder = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(order, parsed);
    }

    /// Action should survive JSON roundtrip
    #[test]
    fn action_serde_roundtrip(action in arb_action()) {
        let json = serde_json::to_string(&action).unwrap();
        let parsed: Action = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(action, parsed);
    }

    /// Condition should survive JSON roundtrip (no floats due to precision)
    #[test]
    fn condition_serde_roundtrip(cond in arb_condition_no_float()) {
        let json = serde_json::to_string(&cond).unwrap();
        let parsed: Condition = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(cond, parsed);
    }

    /// Cage should survive JSON roundtrip (no floats due to precision)
    #[test]
    fn cage_serde_roundtrip(cage in arb_cage_no_float()) {
        let json = serde_json::to_string(&cage).unwrap();
        let parsed: Cage = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(cage, parsed);
    }
}
