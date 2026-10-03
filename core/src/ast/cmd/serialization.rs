//! Applied scope cannot be an optional extension that older readers ignore.
use super::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

// The remote derive checks this field list against Qail at compile time. Keep
// its raw serializer private: only the envelope may transport applied scope.
#[derive(Serialize, Deserialize)]
#[serde(remote = "Qail", deny_unknown_fields)]
struct Fields {
    action: Action,
    table: String,
    // Absent in payloads written before typed FROM sources existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    from_source: Option<FromSource>,
    columns: Vec<Expr>,
    joins: Vec<Join>,
    cages: Vec<Cage>,
    distinct: bool,
    index_def: Option<IndexDef>,
    table_constraints: Vec<TableConstraint>,
    set_ops: Vec<(SetOp, Box<Qail>)>,
    having: Vec<Condition>,
    group_by_mode: GroupByMode,
    ctes: Vec<CTEDef>,
    distinct_on: Vec<Expr>,
    returning: Option<Vec<Expr>>,
    // Absent in payloads written before PG 18 RETURNING aliases existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    returning_aliases: Option<ReturningAliases>,
    on_conflict: Option<OnConflict>,
    #[serde(skip)]
    conflict_update_scope: Vec<Condition>,
    #[serde(default)]
    merge: Option<Merge>,
    source_query: Option<Box<Qail>>,
    channel: Option<String>,
    payload: Option<String>,
    savepoint_name: Option<String>,
    from_tables: Vec<String>,
    using_tables: Vec<String>,
    lock_mode: Option<LockMode>,
    skip_locked: bool,
    // Absent in payloads written before NOWAIT / OF locks existed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    lock_nowait: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    lock_of: Vec<String>,
    fetch: Option<(u64, bool)>,
    default_values: bool,
    overriding: Option<OverridingKind>,
    sample: Option<(SampleMethod, f64, Option<u64>)>,
    only_table: bool,
    vector: Option<Vec<f32>>,
    score_threshold: Option<f32>,
    vector_name: Option<String>,
    with_vector: bool,
    vector_size: Option<u64>,
    distance: Option<Distance>,
    on_disk: Option<bool>,
    function_def: Option<crate::ast::FunctionDef>,
    trigger_def: Option<crate::ast::TriggerDef>,
    policy_def: Option<crate::migrate::policy::RlsPolicy>,
    #[serde(default)]
    view_security_invoker: bool,
}

#[derive(Serialize)]
struct ScopedRef<'a> {
    qail_ast_version: u32,
    conflict_update_scope: &'a [Condition],
    #[serde(with = "Fields")]
    command: &'a Qail,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scoped {
    qail_ast_version: u32,
    conflict_update_scope: Vec<Condition>,
    #[serde(with = "Fields")]
    command: Qail,
}

#[derive(Deserialize)]
struct Plain(#[serde(with = "Fields")] Qail);

#[derive(Deserialize)]
#[serde(untagged)]
enum Input {
    Scoped(Scoped),
    Plain(Plain),
}

impl Serialize for Qail {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.conflict_update_scope.is_empty() {
            Fields::serialize(self, serializer)
        } else {
            ScopedRef {
                qail_ast_version: 2,
                conflict_update_scope: &self.conflict_update_scope,
                command: self,
            }
            .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Qail {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Input::deserialize(deserializer)? {
            Input::Plain(Plain(cmd)) => Ok(cmd),
            Input::Scoped(scoped) => {
                if scoped.qail_ast_version != 2 {
                    return Err(D::Error::custom(
                        "unsupported Qail applied-scope AST version",
                    ));
                }
                if scoped.conflict_update_scope.is_empty() {
                    return Err(D::Error::custom(
                        "scoped Qail AST requires nonempty applied scope",
                    ));
                }
                let mut cmd = scoped.command;
                cmd.conflict_update_scope = scoped.conflict_update_scope;
                Ok(cmd)
            }
        }
    }
}
