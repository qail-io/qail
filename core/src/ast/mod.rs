/// Aggregate call shape: arguments, local ORDER BY, WITHIN GROUP.
pub mod aggregate;
/// Condition builders for WHERE clauses.
pub mod builders;
/// Constraint cages (filter, sort, limit, etc.).
pub mod cages;
/// Command builders and AST root.
pub mod cmd;
/// Condition types.
pub mod conditions;
/// Expression AST nodes.
pub mod expr;
/// JOIN clause types.
pub mod joins;
/// SQL operators and actions.
pub mod operators;
/// Value types for parameters and literals.
pub mod values;
/// INSERT/UPDATE payload rules shared by the transpiler and native encoder.
pub mod write_payload;

pub use self::aggregate::{
    aggregate_sort_cage, aggregate_sort_key, check_aggregate_shape, sort_order_sql,
};
pub use self::cages::{Cage, CageKind};
pub use self::cmd::Qail;
pub use self::cmd::{
    CTEDef, ConflictAction, CteCycle, CteMaterialization, CteSearch, CteSearchOrder, GroupByClause,
    Merge, MergeAction, MergeClause, MergeMatchKind, MergeSource, OnConflict,
};
pub use self::conditions::Condition;
pub use self::expr::{
    BinaryOp, ColumnGeneration, Constraint, Expr, FrameBound, FrameExclusion, FunctionDef,
    IndexDef, JsonPathSegment, TableConstraint, TriggerDef, TriggerEvent, TriggerTiming,
    WindowFrame,
};
pub use self::joins::Join;
pub use self::operators::{
    Action, AggregateFunc, Distance, GroupByMode, JoinKind, LockMode, LogicalOp, ModKind, Operator,
    OverridingKind, SampleMethod, SetOp, SortOrder,
};
pub use self::values::Value;
// PostgreSQL 18 RETURNING aliases, typed FROM sources, temporal keys.
pub use self::cmd::{FromSource, ReturningAliases};
pub use self::expr::validate_function_args;
