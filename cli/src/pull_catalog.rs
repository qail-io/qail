//! Catalog facts that `information_schema` flattens or omits.
//!
//! Shared by `qail pull` (`introspection.rs`) and live-schema introspection
//! (`shadow.rs`: drift checks and post-apply verify), so both read the same
//! type identity, generation kind, collation, and sequence settings.

use anyhow::{Result, anyhow};
use qail_core::ast::{
    Condition, Expr, FunctionOptions, IdentityOptions, JoinKind, Operator, Qail, Value,
};
use qail_core::migrate::ColumnType;
use qail_pg::driver::PgDriver;
use std::collections::{BTreeMap, HashMap, HashSet};

// pg_depend.classid values (OIDs of the catalogs themselves; fixed by initdb).
const PG_CLASS_RELID: &str = "1259";
const PG_TYPE_RELID: &str = "1247";
const PG_PROC_RELID: &str = "1255";

fn join_eq(left: &str, right: &str) -> Condition {
    Condition {
        left: Expr::Named(left.to_string()),
        op: Operator::Eq,
        value: Value::Column(right.to_string()),
        is_array_unnest: false,
    }
}

fn named(column: &str) -> Expr {
    Expr::Named(column.to_string())
}

fn catalog_call(name: &str, args: &[&str]) -> Expr {
    Expr::FunctionCall {
        name: format!("pg_catalog.{name}"),
        args: args.iter().map(|arg| named(arg)).collect(),
        alias: None,
    }
}

fn text_list(values: &[&str]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|value| Value::String((*value).to_string()))
            .collect(),
    )
}

fn is_system_schema(name: &str) -> bool {
    name == "information_schema" || name.starts_with("pg_")
}

/// Parse PostgreSQL text-array output (`{a,"b c",d}`).
pub(crate) fn parse_text_array(raw: &str) -> Vec<String> {
    let Some(inner) = raw
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
    else {
        return Vec::new();
    };
    if inner.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' if in_quotes => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => out.push(std::mem::take(&mut current)),
            _ => current.push(ch),
        }
    }
    out.push(current);
    out
}

/// Extension-member object OIDs per catalog (pg_depend deptype 'e').
async fn extension_members(driver: &mut PgDriver) -> Result<HashMap<String, HashSet<String>>> {
    let cmd = Qail::get("pg_catalog.pg_depend")
        .columns(["classid", "objid"])
        .filter("deptype", Operator::Eq, "e");
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to query extension members: {}", e))?;
    let mut members: HashMap<String, HashSet<String>> = HashMap::new();
    for row in rows {
        members.entry(row.text(0)).or_default().insert(row.text(1));
    }
    Ok(members)
}

// ── Columns ─────────────────────────────────────────────────────────────

/// Per-column catalog facts.
#[derive(Debug, Clone, Default)]
pub(crate) struct ColumnCatalog {
    /// `format_type(atttypid, atttypmod)` — exact declared type.
    pub formatted_type: String,
    /// `pg_attribute.attgenerated`: empty, `s` (stored) or `v` (virtual).
    pub attgenerated: String,
    /// Column collation when it differs from the type's default collation.
    pub collation: Option<String>,
    /// The collation is not a built-in (`pg_catalog`) collation.
    pub custom_collation: bool,
    /// `schema.type` when the column type (or array element type) lives in a
    /// non-system schema other than the pulled one and is not extension-owned.
    pub foreign_type: Option<String>,
}

/// Read per-column type identity, generation kind, and collation for every
/// table column in `namespace_oid`, keyed by `(table, column)`.
pub(crate) async fn fetch_column_catalog(
    driver: &mut PgDriver,
    namespace_oid: &str,
) -> Result<HashMap<(String, String), ColumnCatalog>> {
    let cmd = Qail::get("pg_catalog.pg_attribute")
        .table_alias("a")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class c",
            "c.oid",
            "a.attrelid",
        )
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_type t",
            "t.oid",
            "a.atttypid",
        )
        .left_join_conds(
            "pg_catalog.pg_collation co",
            vec![join_eq("co.oid", "a.attcollation")],
        )
        .left_join_conds(
            "pg_catalog.pg_namespace cn",
            vec![join_eq("cn.oid", "co.collnamespace")],
        )
        .columns_expr([
            named("c.relname"),
            named("a.attname"),
            catalog_call("format_type", &["a.atttypid", "a.atttypmod"]),
            named("a.attgenerated"),
            named("a.attcollation"),
            named("t.typcollation"),
            named("co.collname"),
            named("cn.nspname"),
            named("a.atttypid"),
            named("t.typelem"),
            named("t.typcategory"),
        ])
        .filter("c.relnamespace", Operator::Eq, namespace_oid.to_string())
        .filter("c.relkind", Operator::In, text_list(&["r", "p"]))
        .filter("a.attnum", Operator::Gt, 0)
        .filter("a.attisdropped", Operator::Eq, false);
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to query column catalog: {}", e))?;

    let foreign_types = foreign_type_names(driver, namespace_oid).await?;

    let mut out = HashMap::new();
    for row in rows {
        let formatted_type = row.get_string(2).ok_or_else(|| {
            anyhow!(
                "format_type returned NULL for {}.{}",
                row.text(0),
                row.text(1)
            )
        })?;
        let attcollation = row.text(4);
        let collation = (attcollation != "0" && attcollation != row.text(5))
            .then(|| row.get_string(6))
            .flatten();
        let custom_collation = collation.is_some() && row.text(7) != "pg_catalog";
        let effective_type = if row.text(10) == "A" && row.text(9) != "0" {
            row.text(9)
        } else {
            row.text(8)
        };
        out.insert(
            (row.text(0), row.text(1)),
            ColumnCatalog {
                formatted_type,
                attgenerated: row.text(3),
                collation,
                custom_collation,
                foreign_type: foreign_types.get(&effective_type).cloned(),
            },
        );
    }
    Ok(out)
}

/// `oid -> schema.type` for types in non-system schemas other than the
/// pulled one, excluding extension members.
async fn foreign_type_names(
    driver: &mut PgDriver,
    namespace_oid: &str,
) -> Result<HashMap<String, String>> {
    let cmd = Qail::get("pg_catalog.pg_type")
        .table_alias("t")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_namespace n",
            "n.oid",
            "t.typnamespace",
        )
        .columns(["t.oid", "n.nspname", "t.typname"])
        .filter("t.typnamespace", Operator::Ne, namespace_oid.to_string());
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to query type namespaces: {}", e))?;
    let members = extension_members(driver).await?;
    let extension_types = members.get(PG_TYPE_RELID);
    let mut out = HashMap::new();
    for row in rows {
        let oid = row.text(0);
        let schema = row.text(1);
        if is_system_schema(&schema) || extension_types.is_some_and(|set| set.contains(&oid)) {
            continue;
        }
        out.insert(oid, format!("{}.{}", schema, row.text(2)));
    }
    Ok(out)
}

/// Resolve a pulled column type against `format_type` output.
///
/// Returns the type to emit and whether it is the exact declared type. When
/// the information_schema mapping normalized the type (json→jsonb, char→varchar,
/// lost typmods, ...) the exact spelling is parsed back; when no `ColumnType`
/// reproduces the declared type, the mapping is kept and `false` is returned
/// so the caller reports the loss instead of hiding it.
pub(crate) fn exact_column_type(mapped: ColumnType, formatted: &str) -> (ColumnType, bool) {
    let target = normalize_type_text(formatted);
    if canonical_pg_type(&mapped) == target {
        return (mapped, true);
    }
    if let Ok(parsed) = formatted.parse::<ColumnType>()
        && canonical_pg_type(&parsed) == target
        && parsed
            .to_pg_type()
            .parse::<ColumnType>()
            .is_ok_and(|again| again == parsed)
    {
        return (parsed, true);
    }
    (mapped, false)
}

fn normalize_type_text(raw: &str) -> String {
    let mut base = raw.trim();
    let mut dims = 0usize;
    while let Some(stripped) = base.strip_suffix("[]") {
        base = stripped.trim_end();
        dims += 1;
    }
    let mut out = match base
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
    {
        // Quoted identifiers are case-sensitive.
        Some(inner) if !inner.is_empty() => inner.replace("\"\"", "\""),
        _ => base
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase(),
    };
    for _ in 0..dims {
        out.push_str("[]");
    }
    out
}

/// `format_type` spelling of a `ColumnType`.
pub(crate) fn canonical_pg_type(ty: &ColumnType) -> String {
    match ty {
        ColumnType::Uuid => "uuid".to_string(),
        ColumnType::Text => "text".to_string(),
        ColumnType::Varchar(None) => "character varying".to_string(),
        ColumnType::Varchar(Some(len)) => format!("character varying({len})"),
        ColumnType::Int | ColumnType::Serial => "integer".to_string(),
        ColumnType::BigInt | ColumnType::BigSerial => "bigint".to_string(),
        ColumnType::Bool => "boolean".to_string(),
        ColumnType::Float => "double precision".to_string(),
        ColumnType::Decimal(None) => "numeric".to_string(),
        ColumnType::Decimal(Some((p, s))) => format!("numeric({p},{s})"),
        ColumnType::Jsonb => "jsonb".to_string(),
        ColumnType::Timestamp => "timestamp without time zone".to_string(),
        ColumnType::Timestamptz => "timestamp with time zone".to_string(),
        ColumnType::Date => "date".to_string(),
        ColumnType::Time => "time without time zone".to_string(),
        ColumnType::Bytea => "bytea".to_string(),
        ColumnType::Array(inner) => format!("{}[]", canonical_pg_type(inner)),
        ColumnType::Enum { name, .. } => name.clone(),
        ColumnType::Range(raw) => canonical_raw_type(raw),
        ColumnType::Interval => "interval".to_string(),
        ColumnType::Cidr => "cidr".to_string(),
        ColumnType::Inet => "inet".to_string(),
        ColumnType::MacAddr => "macaddr".to_string(),
    }
}

fn canonical_raw_type(raw: &str) -> String {
    let lower = raw
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let with_args = |prefix: &str, replacement: &str, suffix: &str| -> Option<String> {
        let args = lower.strip_prefix(prefix)?.strip_prefix('(')?;
        Some(format!("{replacement}({args}{suffix}"))
    };
    match lower.as_str() {
        "timetz" => return "time with time zone".to_string(),
        "varbit" => return "bit varying".to_string(),
        _ => {}
    }
    if let Some(out) = with_args("varchar", "character varying", "") {
        return out;
    }
    if let Some(out) = with_args("varbit", "bit varying", "") {
        return out;
    }
    if let Some(out) = with_args("decimal", "numeric", "") {
        return out;
    }
    // `TIMESTAMP(3)` → `timestamp(3) without time zone`; the precision group
    // is the whole argument list for these types.
    for (prefix, replacement, zone) in [
        ("timestamptz", "timestamp", " with time zone"),
        ("timestamp", "timestamp", " without time zone"),
        ("timetz", "time", " with time zone"),
        ("time", "time", " without time zone"),
    ] {
        if let Some(args) = lower
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_prefix('('))
            .and_then(|rest| rest.strip_suffix(')'))
        {
            return format!("{replacement}({args}){zone}");
        }
    }
    lower
}

// ── Sequences ───────────────────────────────────────────────────────────

/// One sequence in the pulled namespace with its settings and owner.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SequenceCatalog {
    pub name: String,
    /// `smallint`, `integer`, or `bigint`.
    pub type_name: &'static str,
    pub start: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cache: i64,
    pub cycle: bool,
    /// Owning `(table, column)` via OWNED BY or identity, in the pulled namespace.
    pub owner: Option<(String, String)>,
    /// Owner table outside the pulled namespace (`schema.table.column`).
    pub foreign_owner: Option<String>,
    /// Identity sequence (pg_depend deptype 'i').
    pub identity: bool,
}

fn sequence_type_bounds(type_name: &str) -> (i64, i64) {
    match type_name {
        "smallint" => (i16::MIN as i64, i16::MAX as i64),
        "integer" => (i32::MIN as i64, i32::MAX as i64),
        _ => (i64::MIN, i64::MAX),
    }
}

impl SequenceCatalog {
    /// Options that differ from PostgreSQL's defaults for this type and increment.
    pub(crate) fn non_default_options(&self) -> IdentityOptions {
        let (type_min, type_max) = sequence_type_bounds(self.type_name);
        let ascending = self.increment > 0;
        let default_min = if ascending { 1 } else { type_min };
        let default_max = if ascending { type_max } else { -1 };
        let default_start = if ascending {
            self.min_value
        } else {
            self.max_value
        };
        IdentityOptions {
            start: (self.start != default_start).then_some(self.start),
            increment: (self.increment != 1).then_some(self.increment),
            min_value: (self.min_value != default_min).then_some(self.min_value),
            max_value: (self.max_value != default_max).then_some(self.max_value),
            cache: (self.cache != 1).then_some(self.cache),
            cycle: self.cycle,
        }
    }

    /// The sequence `serial`/`bigserial` creates for `column_type`
    /// (`integer`/`bigint`): default name, settings, and OWNED BY its column.
    pub(crate) fn is_plain_serial_for(&self, column_type: &str) -> bool {
        let Some((table, column)) = &self.owner else {
            return false;
        };
        !self.identity
            && matches!(column_type, "integer" | "bigint")
            && self.type_name == column_type
            && self.name == format!("{table}_{column}_seq")
            && self.non_default_options().is_empty()
    }
}

/// Read every sequence in `namespace_oid` with settings and ownership.
pub(crate) async fn fetch_sequence_catalog(
    driver: &mut PgDriver,
    namespace_oid: &str,
) -> Result<Vec<SequenceCatalog>> {
    let seq_cmd = Qail::get("pg_catalog.pg_sequence")
        .table_alias("q")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class s",
            "s.oid",
            "q.seqrelid",
        )
        .columns([
            "s.relname",
            "s.oid",
            "q.seqtypid",
            "q.seqstart",
            "q.seqincrement",
            "q.seqmax",
            "q.seqmin",
            "q.seqcache",
            "q.seqcycle",
        ])
        .filter("s.relnamespace", Operator::Eq, namespace_oid.to_string());
    let seq_rows = driver
        .fetch_all(&seq_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query sequence settings: {}", e))?;

    let dep_cmd = Qail::get("pg_catalog.pg_depend")
        .table_alias("d")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class t",
            "t.oid",
            "d.refobjid",
        )
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_namespace tn",
            "tn.oid",
            "t.relnamespace",
        )
        .inner_join_conds(
            "pg_catalog.pg_attribute a",
            vec![
                join_eq("a.attrelid", "d.refobjid"),
                join_eq("a.attnum", "d.refobjsubid"),
            ],
        )
        .columns([
            "d.objid",
            "d.deptype",
            "t.relname",
            "t.relnamespace",
            "a.attname",
            "tn.nspname",
        ])
        .filter("d.classid", Operator::Eq, PG_CLASS_RELID)
        .filter("d.refclassid", Operator::Eq, PG_CLASS_RELID)
        .filter("d.deptype", Operator::In, text_list(&["a", "i"]))
        .filter("d.refobjsubid", Operator::Gt, 0);
    let dep_rows = driver
        .fetch_all(&dep_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query sequence ownership: {}", e))?;
    let mut owners: HashMap<String, (String, String, String, String, String)> = HashMap::new();
    for row in dep_rows {
        owners.insert(
            row.text(0),
            (
                row.text(1),
                row.text(2),
                row.text(3),
                row.text(4),
                row.text(5),
            ),
        );
    }

    let parse_i64 = |raw: String, label: &str| -> Result<i64> {
        raw.trim()
            .parse::<i64>()
            .map_err(|e| anyhow!("Invalid pg_sequence.{label} {:?}: {}", raw, e))
    };
    let mut out = Vec::new();
    for row in seq_rows {
        let name = row.text(0);
        let type_name = match row.text(2).as_str() {
            "21" => "smallint",
            "23" => "integer",
            "20" => "bigint",
            other => {
                return Err(anyhow!(
                    "Sequence {} has unsupported data type oid {}",
                    name,
                    other
                ));
            }
        };
        let (owner, foreign_owner, identity) = match owners.get(&row.text(1)) {
            Some((deptype, table, table_ns, column, table_schema)) => {
                if table_ns == namespace_oid {
                    (Some((table.clone(), column.clone())), None, deptype == "i")
                } else {
                    (
                        None,
                        Some(format!("{table_schema}.{table}.{column}")),
                        deptype == "i",
                    )
                }
            }
            None => (None, None, false),
        };
        out.push(SequenceCatalog {
            name,
            type_name,
            start: parse_i64(row.text(3), "seqstart")?,
            increment: parse_i64(row.text(4), "seqincrement")?,
            max_value: parse_i64(row.text(5), "seqmax")?,
            min_value: parse_i64(row.text(6), "seqmin")?,
            cache: parse_i64(row.text(7), "seqcache")?,
            cycle: row.text(8) == "t",
            owner,
            foreign_owner,
            identity,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Sequence name inside a `nextval('name'::regclass)` default, unquoted.
pub(crate) fn nextval_sequence_name(default: &str) -> Option<String> {
    let inner = default
        .trim()
        .strip_prefix("nextval('")?
        .strip_suffix("'::regclass)")?;
    let unquoted = inner
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .map(|s| s.replace("\"\"", "\""))
        .unwrap_or_else(|| inner.to_string());
    Some(unquoted)
}

// ── Functions ───────────────────────────────────────────────────────────

/// Execution properties of one routine, keyed by information_schema `specific_name`.
#[derive(Debug, Clone)]
pub(crate) struct FunctionCatalog {
    pub volatility: Option<String>,
    /// `pg_get_function_result` (keeps SETOF / TABLE / exact type names).
    pub returns: String,
    pub options: FunctionOptions,
    /// OUT / INOUT / VARIADIC parameters exist (the pull keeps IN arguments only).
    pub has_non_in_args: bool,
}

/// `SET` clauses from `pg_get_functiondef` output, as `name TO value`.
pub(crate) fn function_set_clauses(def: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in def.lines() {
        // The header ends where the body starts.
        if line.starts_with("AS ")
            || line.starts_with("BEGIN ATOMIC")
            || line.starts_with(" RETURN ")
        {
            break;
        }
        if let Some(rest) = line.strip_prefix(" SET ") {
            out.push(rest.trim().to_string());
        }
    }
    out
}

/// Read execution properties for every plain function in `namespace_oid`,
/// keyed by `proname_oid` (information_schema `specific_name`).
pub(crate) async fn fetch_function_catalog(
    driver: &mut PgDriver,
    namespace_oid: &str,
) -> Result<HashMap<String, FunctionCatalog>> {
    let cmd = Qail::get("pg_catalog.pg_proc")
        .table_alias("p")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_language l",
            "l.oid",
            "p.prolang",
        )
        .columns_expr([
            named("p.oid"),
            named("p.proname"),
            named("p.provolatile"),
            named("p.prosecdef"),
            named("p.proisstrict"),
            named("p.proleakproof"),
            named("p.proparallel"),
            named("p.procost"),
            named("p.prorows"),
            named("p.proretset"),
            named("l.lanname"),
            named("p.proargmodes"),
            catalog_call("pg_get_function_result", &["p.oid"]),
            catalog_call("pg_get_functiondef", &["p.oid"]),
        ])
        .filter("p.pronamespace", Operator::Eq, namespace_oid.to_string())
        .filter("p.prokind", Operator::Eq, "f");
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to query function properties: {}", e))?;

    let mut out = HashMap::new();
    for row in rows {
        let specific_name = format!("{}_{}", row.text(1), row.text(0));
        let volatility = match row.text(2).as_str() {
            "i" => Some("immutable".to_string()),
            "s" => Some("stable".to_string()),
            _ => None,
        };
        let lanname = row.text(10);
        let default_cost = if matches!(lanname.as_str(), "c" | "internal") {
            1.0
        } else {
            100.0
        };
        let cost_text = row.text(7);
        let cost = cost_text
            .parse::<f64>()
            .map_err(|e| anyhow!("Invalid pg_proc.procost {:?}: {}", cost_text, e))?;
        let returns_set = row.text(9) == "t";
        let rows_text = row.text(8);
        let rows_estimate = rows_text
            .parse::<f64>()
            .map_err(|e| anyhow!("Invalid pg_proc.prorows {:?}: {}", rows_text, e))?;
        let options = FunctionOptions {
            strict: row.text(4) == "t",
            security_definer: row.text(3) == "t",
            leakproof: row.text(5) == "t",
            parallel: match row.text(6).as_str() {
                "s" => Some("safe".to_string()),
                "r" => Some("restricted".to_string()),
                _ => None,
            },
            cost: (cost != default_cost).then_some(cost_text),
            rows: (returns_set && rows_estimate != 1000.0).then_some(rows_text),
            config: function_set_clauses(&row.text(13)),
        };
        let has_non_in_args = parse_text_array(&row.text(11))
            .iter()
            .any(|mode| matches!(mode.as_str(), "o" | "b" | "v"));
        out.insert(
            specific_name,
            FunctionCatalog {
                volatility,
                returns: row.text(12),
                options,
                has_non_in_args,
            },
        );
    }
    Ok(out)
}

// ── Triggers ────────────────────────────────────────────────────────────

/// pg_trigger facts information_schema does not expose.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TriggerCatalog {
    /// CONSTRAINT trigger (deferrable firing).
    pub constraint: bool,
    /// Number of trigger function arguments.
    pub nargs: i64,
}

/// Non-internal triggers in `namespace_oid`, keyed by `(trigger, table)`.
pub(crate) async fn fetch_trigger_catalog(
    driver: &mut PgDriver,
    namespace_oid: &str,
) -> Result<HashMap<(String, String), TriggerCatalog>> {
    let cmd = Qail::get("pg_catalog.pg_trigger")
        .table_alias("tg")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class c",
            "c.oid",
            "tg.tgrelid",
        )
        .columns(["tg.tgname", "c.relname", "tg.tgconstraint", "tg.tgnargs"])
        .filter("c.relnamespace", Operator::Eq, namespace_oid.to_string())
        .filter("tg.tgisinternal", Operator::Eq, false);
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to query trigger catalog: {}", e))?;
    let mut out = HashMap::new();
    for row in rows {
        let nargs_text = row.text(3);
        let nargs = nargs_text
            .parse::<i64>()
            .map_err(|e| anyhow!("Invalid pg_trigger.tgnargs {:?}: {}", nargs_text, e))?;
        out.insert(
            (row.text(0), row.text(1)),
            TriggerCatalog {
                constraint: row.text(2) != "0",
                nargs,
            },
        );
    }
    Ok(out)
}

// ── Indexes ─────────────────────────────────────────────────────────────

/// Index storage parameters (`reloptions`) keyed by index name.
pub(crate) async fn fetch_index_storage_params(
    driver: &mut PgDriver,
    namespace_oid: &str,
) -> Result<HashMap<String, Vec<String>>> {
    let cmd = Qail::get("pg_catalog.pg_class")
        .columns(["relname", "reloptions"])
        .filter("relkind", Operator::In, text_list(&["i", "I"]))
        .filter("relnamespace", Operator::Eq, namespace_oid.to_string());
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to query index storage parameters: {}", e))?;
    let mut out = HashMap::new();
    for row in rows {
        let params = row
            .get_string(1)
            .map(|raw| parse_text_array(&raw))
            .unwrap_or_default();
        if !params.is_empty() {
            out.insert(row.text(0), params);
        }
    }
    Ok(out)
}

// ── Pull scope (one namespace) ──────────────────────────────────────────

/// Foreign keys from the pulled namespace to tables in another namespace,
/// as `table.constraint -> schema.table`.
pub(crate) async fn foreign_keys_leaving_namespace(
    driver: &mut PgDriver,
    namespace_oid: &str,
) -> Result<Vec<String>> {
    let cmd = Qail::get("pg_catalog.pg_constraint")
        .table_alias("con")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class src",
            "src.oid",
            "con.conrelid",
        )
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class ref",
            "ref.oid",
            "con.confrelid",
        )
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_namespace rn",
            "rn.oid",
            "ref.relnamespace",
        )
        .columns(["src.relname", "con.conname", "rn.nspname", "ref.relname"])
        .filter("con.contype", Operator::Eq, "f")
        .filter("con.connamespace", Operator::Eq, namespace_oid.to_string())
        .filter("ref.relnamespace", Operator::Ne, namespace_oid.to_string());
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to query cross-schema foreign keys: {}", e))?;
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{}.{} -> {}.{}",
                row.text(0),
                row.text(1),
                row.text(2),
                row.text(3)
            )
        })
        .collect();
    out.sort();
    Ok(out)
}

/// Non-system schemas other than the pulled one that hold user objects
/// (relations, types, functions; extension members excluded), with counts.
pub(crate) async fn schemas_outside_pull(
    driver: &mut PgDriver,
    namespace_oid: &str,
) -> Result<Vec<String>> {
    let ns_cmd = Qail::get("pg_catalog.pg_namespace").columns(["oid", "nspname"]);
    let ns_rows = driver
        .fetch_all(&ns_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query namespaces: {}", e))?;
    let candidates: BTreeMap<String, String> = ns_rows
        .iter()
        .map(|row| (row.text(0), row.text(1)))
        .filter(|(oid, name)| oid != namespace_oid && !is_system_schema(name))
        .collect();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let oids = Value::Array(
        candidates
            .keys()
            .map(|oid| Value::String(oid.clone()))
            .collect(),
    );
    let members = extension_members(driver).await?;

    let mut counts: BTreeMap<String, [usize; 3]> = BTreeMap::new();
    let queries = [
        (
            Qail::get("pg_catalog.pg_class")
                .columns(["relnamespace", "oid"])
                .filter(
                    "relkind",
                    Operator::In,
                    text_list(&["r", "p", "v", "m", "S", "f"]),
                )
                .filter("relnamespace", Operator::In, oids.clone()),
            PG_CLASS_RELID,
        ),
        (
            Qail::get("pg_catalog.pg_type")
                .columns(["typnamespace", "oid"])
                .filter("typtype", Operator::In, text_list(&["e", "d", "r", "m"]))
                .filter("typnamespace", Operator::In, oids.clone()),
            PG_TYPE_RELID,
        ),
        (
            Qail::get("pg_catalog.pg_proc")
                .columns(["pronamespace", "oid"])
                .filter("pronamespace", Operator::In, oids),
            PG_PROC_RELID,
        ),
    ];
    for (slot, (cmd, classid)) in queries.into_iter().enumerate() {
        let rows = driver
            .fetch_all(&cmd)
            .await
            .map_err(|e| anyhow!("Failed to count objects outside the pulled schema: {}", e))?;
        let extension_objects = members.get(classid);
        for row in rows {
            if extension_objects.is_some_and(|set| set.contains(&row.text(1))) {
                continue;
            }
            if let Some(name) = candidates.get(&row.text(0)) {
                counts.entry(name.clone()).or_default()[slot] += 1;
            }
        }
    }
    Ok(counts
        .into_iter()
        .map(|(schema, [relations, types, functions])| {
            format!(
                "schema '{schema}' ({relations} relation(s), {types} type(s), {functions} function(s)) is not pulled: pull reads the public schema only"
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_type_restores_identity_information_schema_normalizes() {
        let cases = [
            (ColumnType::Jsonb, "json", "JSON"),
            (ColumnType::Float, "real", "REAL"),
            (ColumnType::Varchar(Some(3)), "character(3)", "CHARACTER(3)"),
            (
                ColumnType::Timestamp,
                "timestamp(3) without time zone",
                "TIMESTAMP(3)",
            ),
            (
                ColumnType::Timestamptz,
                "timestamp(0) with time zone",
                "TIMESTAMPTZ(0)",
            ),
            (ColumnType::Time, "time(2) without time zone", "TIME(2)"),
            (
                ColumnType::Range("TIMETZ".to_string()),
                "time with time zone",
                "TIMETZ",
            ),
            (ColumnType::Range("BIT".to_string()), "bit(8)", "BIT(8)"),
            (
                ColumnType::Range("VARBIT".to_string()),
                "bit varying(16)",
                "VARBIT(16)",
            ),
            (
                ColumnType::Array(Box::new(ColumnType::Varchar(None))),
                "character varying(20)[]",
                "VARCHAR(20)[]",
            ),
            (
                ColumnType::Interval,
                "interval day to second(3)",
                "INTERVAL DAY TO SECOND(3)",
            ),
        ];
        for (mapped, formatted, expected) in cases {
            let (resolved, exact) = exact_column_type(mapped, formatted);
            assert!(exact, "{formatted} should resolve exactly");
            assert_eq!(resolved.to_pg_type(), expected, "{formatted}");
        }
    }

    #[test]
    fn exact_type_keeps_identical_mappings() {
        let cases = [
            (ColumnType::Jsonb, "jsonb"),
            (ColumnType::Float, "double precision"),
            (ColumnType::Varchar(Some(20)), "character varying(20)"),
            (ColumnType::Serial, "integer"),
            (ColumnType::Decimal(Some((10, 2))), "numeric(10,2)"),
            (ColumnType::Timestamptz, "timestamp with time zone"),
            (ColumnType::Range("SMALLINT".to_string()), "smallint"),
            (
                ColumnType::Enum {
                    name: "Mood".to_string(),
                    values: vec![],
                },
                "\"Mood\"",
            ),
        ];
        for (mapped, formatted) in cases {
            let (resolved, exact) = exact_column_type(mapped.clone(), formatted);
            assert!(exact, "{formatted}");
            assert_eq!(resolved, mapped, "{formatted}");
        }
    }

    #[test]
    fn exact_type_reports_unrepresentable_types() {
        let (resolved, exact) = exact_column_type(ColumnType::Text, "email_address");
        assert!(!exact, "a domain over text must not pass as text");
        assert_eq!(resolved, ColumnType::Text);
        let (_, exact) = exact_column_type(ColumnType::Range("VECTOR".to_string()), "vector(1536)");
        assert!(!exact);
    }

    #[test]
    fn sequence_defaults_are_omitted_and_settings_kept() {
        let seq = SequenceCatalog {
            name: "t_id_seq".to_string(),
            type_name: "integer",
            start: 1,
            increment: 1,
            min_value: 1,
            max_value: i32::MAX as i64,
            cache: 1,
            cycle: false,
            owner: Some(("t".to_string(), "id".to_string())),
            foreign_owner: None,
            identity: false,
        };
        assert!(seq.non_default_options().is_empty());
        assert!(seq.is_plain_serial_for("integer"));
        assert!(!seq.is_plain_serial_for("bigint"));

        let custom = SequenceCatalog {
            start: 100,
            increment: 5,
            min_value: 50,
            max_value: 100_000,
            cache: 10,
            cycle: true,
            ..seq.clone()
        };
        let opts = custom.non_default_options();
        assert_eq!(opts.start, Some(100));
        assert_eq!(opts.increment, Some(5));
        assert_eq!(opts.min_value, Some(50));
        assert_eq!(opts.max_value, Some(100_000));
        assert_eq!(opts.cache, Some(10));
        assert!(opts.cycle);
        assert!(!custom.is_plain_serial_for("integer"));

        let renamed = SequenceCatalog {
            name: "owner_seq".to_string(),
            ..seq
        };
        assert!(!renamed.is_plain_serial_for("integer"));
    }

    #[test]
    fn function_set_clauses_stop_at_body() {
        let def = "CREATE OR REPLACE FUNCTION public.f()\n RETURNS integer\n LANGUAGE plpgsql\n STABLE SECURITY DEFINER\n SET search_path TO 'public', 'pg_temp'\n SET work_mem TO '64MB'\nAS $function$\n SET x = 1;\n$function$\n";
        assert_eq!(
            function_set_clauses(def),
            vec![
                "search_path TO 'public', 'pg_temp'".to_string(),
                "work_mem TO '64MB'".to_string()
            ]
        );
    }

    #[test]
    fn text_array_parsing_handles_quotes() {
        assert_eq!(parse_text_array("{fillfactor=70}"), vec!["fillfactor=70"]);
        assert_eq!(parse_text_array("{i,o,\"a,b\"}"), vec!["i", "o", "a,b"]);
        assert!(parse_text_array("{}").is_empty());
    }

    #[test]
    fn nextval_default_names_are_unquoted() {
        assert_eq!(
            nextval_sequence_name("nextval('owner_seq'::regclass)").as_deref(),
            Some("owner_seq")
        );
        assert_eq!(
            nextval_sequence_name("nextval('\"Odd\"\"Seq\"'::regclass)").as_deref(),
            Some("Odd\"Seq")
        );
        assert_eq!(nextval_sequence_name("now()"), None);
    }
}
