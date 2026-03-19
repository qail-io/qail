//! Query normalization and optimization support.
//!
//! This module defines canonical representations for rewrite-safe subsets of
//! QAIL `SELECT` and mutation (`ADD`/`SET`/`DEL`) queries.

mod nested_batch;
mod normalized_mutation;
mod normalized_select;
mod passes;

pub use nested_batch::{
    BatchPlanError, NestedBatchPlan, NestedRelationKind, plan_nested_batch_fetch,
};
pub use normalized_mutation::{
    MutationClause, NormalizeMutationError, NormalizedMutation, normalize_mutation,
};
pub use normalized_select::{
    FilterClause, NormalizeError, NormalizedJoin, NormalizedSelect, OrderByItem, normalize_select,
};
pub use passes::{cleanup_mutation, cleanup_select};
