use crate::ast::aggregate::sort_keys_text;
use crate::ast::{AggregateFunc, Cage, Condition, ModKind, Value};

/// Binary operators for expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BinaryOp {
    // Arithmetic
    /// String concatenation `||`.
    Concat,
    /// Addition `+`.
    Add,
    /// Subtraction `-`.
    Sub,
    /// Multiplication `*`.
    Mul,
    /// Division `/`.
    Div,
    /// Modulo (%)
    Rem,
    // Logical
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
    // Comparison
    /// Equals `=`.
    Eq,
    /// Not equals `<>`.
    Ne,
    /// Greater than `>`.
    Gt,
    /// Greater than or equal `>=`.
    Gte,
    /// Less than `<`.
    Lt,
    /// Less than or equal `<=`.
    Lte,
    // Null checks (unary but represented as binary with null right)
    /// IS NULL.
    IsNull,
    /// IS NOT NULL.
    IsNotNull,
    /// Null-safe inequality `IS DISTINCT FROM`.
    IsDistinctFrom,
    /// Null-safe equality `IS NOT DISTINCT FROM`.
    IsNotDistinctFrom,
    // Boolean tests (unary, like the null checks)
    /// IS TRUE.
    IsTrue,
    /// IS NOT TRUE.
    IsNotTrue,
    /// IS FALSE.
    IsFalse,
    /// IS NOT FALSE.
    IsNotFalse,
    /// IS UNKNOWN.
    IsUnknown,
    /// IS NOT UNKNOWN.
    IsNotUnknown,
}

impl BinaryOp {
    /// Unary postfix tests (`IS NULL`, `IS TRUE`, ...): `right` is a
    /// placeholder that must not be rendered.
    pub fn is_postfix(&self) -> bool {
        matches!(
            self,
            BinaryOp::IsNull
                | BinaryOp::IsNotNull
                | BinaryOp::IsTrue
                | BinaryOp::IsNotTrue
                | BinaryOp::IsFalse
                | BinaryOp::IsNotFalse
                | BinaryOp::IsUnknown
                | BinaryOp::IsNotUnknown
        )
    }
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOp::Concat => write!(f, "||"),
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Sub => write!(f, "-"),
            BinaryOp::Mul => write!(f, "*"),
            BinaryOp::Div => write!(f, "/"),
            BinaryOp::Rem => write!(f, "%"),
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::Ne => write!(f, "<>"),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::Gte => write!(f, ">="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::Lte => write!(f, "<="),
            BinaryOp::IsNull => write!(f, "IS NULL"),
            BinaryOp::IsNotNull => write!(f, "IS NOT NULL"),
            BinaryOp::IsDistinctFrom => write!(f, "IS DISTINCT FROM"),
            BinaryOp::IsNotDistinctFrom => write!(f, "IS NOT DISTINCT FROM"),
            BinaryOp::IsTrue => write!(f, "IS TRUE"),
            BinaryOp::IsNotTrue => write!(f, "IS NOT TRUE"),
            BinaryOp::IsFalse => write!(f, "IS FALSE"),
            BinaryOp::IsNotFalse => write!(f, "IS NOT FALSE"),
            BinaryOp::IsUnknown => write!(f, "IS UNKNOWN"),
            BinaryOp::IsNotUnknown => write!(f, "IS NOT UNKNOWN"),
        }
    }
}

/// Operand of one JSON `->` / `->>` step.
///
/// PostgreSQL overloads these operators: a text operand selects an object
/// key and an integer operand selects an array position (negative counts
/// from the end), so `doc->'0'` and `doc->0` read different values.
///
/// Serde form: an index is the decimal string (`"0"`), a key whose text is
/// not an `i64` literal is the plain string (`"name"`), and a key whose text
/// is an `i64` literal is `{"key": "0"}`. A plain string therefore keeps the
/// meaning it had when segments were bare strings, and a bare JSON integer
/// also decodes as an index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JsonPathSegment {
    /// Object key, rendered as a quoted text operand (`->'0'`).
    Key(String),
    /// Array position, rendered as an integer operand (`->0`).
    Index(i64),
}

impl JsonPathSegment {
    /// Classify a segment written as plain path text: an `i64` literal is an
    /// array position, anything else an object key.
    ///
    /// String-path builders (`json_path`, `.path("items.0.name")`) and
    /// plain-string serde segments use this rule.
    pub fn from_path_text(text: &str) -> Self {
        match text.parse::<i64>() {
            Ok(index) => Self::Index(index),
            Err(_) => Self::Key(text.to_string()),
        }
    }

    /// Object key text, if this segment is a key.
    pub fn as_key(&self) -> Option<&str> {
        match self {
            Self::Key(key) => Some(key),
            Self::Index(_) => None,
        }
    }
}

impl From<&str> for JsonPathSegment {
    fn from(key: &str) -> Self {
        Self::Key(key.to_string())
    }
}

impl From<String> for JsonPathSegment {
    fn from(key: String) -> Self {
        Self::Key(key)
    }
}

impl From<i64> for JsonPathSegment {
    fn from(index: i64) -> Self {
        Self::Index(index)
    }
}

/// Renders the SQL operand: `'key'` with quotes doubled, or the bare integer.
impl std::fmt::Display for JsonPathSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Key(key) => write!(f, "'{}'", key.replace('\'', "''")),
            Self::Index(index) => write!(f, "{index}"),
        }
    }
}

impl serde::Serialize for JsonPathSegment {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Self::Index(index) => serializer.serialize_str(&index.to_string()),
            Self::Key(key) if key.parse::<i64>().is_ok() => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("key", key)?;
                map.end()
            }
            Self::Key(key) => serializer.serialize_str(key),
        }
    }
}

impl<'de> serde::Deserialize<'de> for JsonPathSegment {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SegmentVisitor;

        impl<'de> serde::de::Visitor<'de> for SegmentVisitor {
            type Value = JsonPathSegment;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON path string, integer index, or {\"key\": text}")
            }

            fn visit_i64<E: serde::de::Error>(self, index: i64) -> Result<Self::Value, E> {
                Ok(JsonPathSegment::Index(index))
            }

            fn visit_u64<E: serde::de::Error>(self, index: u64) -> Result<Self::Value, E> {
                i64::try_from(index)
                    .map(JsonPathSegment::Index)
                    .map_err(|_| E::custom("JSON path index out of i64 range"))
            }

            fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Self::Value, E> {
                Ok(JsonPathSegment::from_path_text(text))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                use serde::de::Error;
                let Some(tag) = map.next_key::<String>()? else {
                    return Err(A::Error::custom("empty JSON path segment object"));
                };
                let segment = match tag.as_str() {
                    "key" => JsonPathSegment::Key(map.next_value()?),
                    "index" => JsonPathSegment::Index(map.next_value()?),
                    other => {
                        return Err(A::Error::unknown_field(other, &["key", "index"]));
                    }
                };
                if map.next_key::<String>()?.is_some() {
                    return Err(A::Error::custom(
                        "JSON path segment object takes exactly one field",
                    ));
                }
                Ok(segment)
            }
        }

        deserializer.deserialize_any(SegmentVisitor)
    }
}

/// An expression node in the AST.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    /// All columns (*)
    Star,
    /// A named column or identifier.
    Named(String),
    /// An aliased expression (expr AS alias)
    Aliased {
        /// Expression name.
        name: String,
        /// Alias.
        alias: String,
    },
    /// An aggregate function (COUNT(col)) with optional FILTER and DISTINCT
    ///
    /// Shape rules, checked by [`check_aggregate_shape`](crate::ast::check_aggregate_shape)
    /// in both the transpiler and the native encoder: `col` and `args` are
    /// never both set; `order_by` and `within_group` are never both set;
    /// `within_group` is set exactly for ordered-set functions; STRING_AGG
    /// takes `args: [value, delimiter]`.
    Aggregate {
        /// Column to aggregate (`*`, `col`, `t.col`) when `args` is empty.
        /// Must be empty when `args` is set.
        col: String,
        /// Aggregate function.
        func: AggregateFunc,
        /// Whether DISTINCT is applied.
        distinct: bool,
        /// PostgreSQL FILTER (WHERE ...) clause for aggregates
        filter: Option<Vec<Condition>>,
        /// Optional alias.
        alias: Option<String>,
        /// Argument expressions, in call order (`SUM(price * quantity)`,
        /// `STRING_AGG(status, ',')`; the direct arguments of an ordered-set
        /// aggregate). Empty means the one argument is `col`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<Expr>,
        /// Aggregate-local ORDER BY inside the call: `ARRAY_AGG(x ORDER BY y)`.
        /// One `CageKind::Sort` cage per key; the key is its single condition's `left`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        order_by: Vec<Cage>,
        /// `WITHIN GROUP (ORDER BY ...)` of an ordered-set aggregate, same cage shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        within_group: Vec<Cage>,
    },
    /// Type cast expression (expr::type)
    Cast {
        /// Expression to cast.
        expr: Box<Expr>,
        /// Target SQL type.
        target_type: String,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Column definition (name, type, constraints).
    Def {
        /// Column name.
        name: String,
        /// SQL data type.
        data_type: String,
        /// Column constraints.
        constraints: Vec<Constraint>,
    },
    /// ALTER TABLE modify (ADD/DROP column).
    Mod {
        /// Modification kind.
        kind: ModKind,
        /// Column expression.
        col: Box<Expr>,
    },
    /// Window Function Definition
    Window {
        /// Window name/alias.
        name: String,
        /// Window function name.
        func: String,
        /// Function arguments as expressions (e.g., for SUM(amount), use Expr::Named("amount"))
        params: Vec<Expr>,
        /// Aggregate FILTER (WHERE ...) applied before OVER.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<Vec<Condition>>,
        /// PARTITION BY columns.
        partition: Vec<String>,
        /// ORDER BY clauses.
        order: Vec<Cage>,
        /// Frame specification.
        frame: Option<WindowFrame>,
    },
    /// CASE WHEN expression
    Case {
        /// WHEN condition THEN expr pairs (Expr allows functions, values, identifiers)
        when_clauses: Vec<(Condition, Box<Expr>)>,
        /// ELSE expr (optional)
        else_value: Option<Box<Expr>>,
        /// Optional alias
        alias: Option<String>,
    },
    /// JSON accessor (data->>'key' or data->'key' or chained data->'a'->0->>'b')
    JsonAccess {
        /// Base column name
        column: String,
        /// JSON path segments: (operand, as_text)
        /// as_text: true for ->> (extract as text), false for -> (extract as JSON)
        /// For chained access like x->'a'->0->>'b', this is
        /// [(Key("a"), false), (Index(0), false), (Key("b"), true)]
        path_segments: Vec<(JsonPathSegment, bool)>,
        /// Optional alias
        alias: Option<String>,
    },
    /// Function call expression (COALESCE, NULLIF, etc.)
    FunctionCall {
        /// Function name (coalesce, nullif, etc.)
        name: String,
        /// Arguments to the function (now supports nested expressions)
        args: Vec<Expr>,
        /// Optional alias
        alias: Option<String>,
    },
    /// Special SQL function with keyword arguments (SUBSTRING, EXTRACT, TRIM, etc.)
    /// e.g., SUBSTRING(expr FROM pos [FOR len]), EXTRACT(YEAR FROM date)
    SpecialFunction {
        /// Function name (SUBSTRING, EXTRACT, TRIM, etc.)
        name: String,
        /// Arguments as (optional_keyword, expr) pairs
        /// e.g., [(None, col), (Some("FROM"), 2), (Some("FOR"), 5)]
        args: Vec<(Option<String>, Box<Expr>)>,
        /// Optional alias
        alias: Option<String>,
    },
    /// Binary expression (left op right)
    Binary {
        /// Left operand.
        left: Box<Expr>,
        /// Binary operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<Expr>,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Literal value (string, number) for use in expressions
    /// e.g., '62', 0, 'active'
    Literal(Value),
    /// Array constructor: ARRAY[expr1, expr2, ...]
    ArrayConstructor {
        /// Array elements.
        elements: Vec<Expr>,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Row constructor: ROW(expr1, expr2, ...) or (expr1, expr2, ...)
    RowConstructor {
        /// Row elements.
        elements: Vec<Expr>,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Array/string subscript: `arr[index]`.
    Subscript {
        /// Base expression.
        expr: Box<Expr>,
        /// Index expression.
        index: Box<Expr>,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Array slice: `arr[lower:upper]`; an omitted bound is open (`arr[2:]`, `arr[:]`).
    ArraySlice {
        /// Base expression.
        expr: Box<Expr>,
        /// Lower bound, inclusive.
        lower: Option<Box<Expr>>,
        /// Upper bound, inclusive.
        upper: Option<Box<Expr>>,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Collation: expr COLLATE "collation_name"
    Collate {
        /// Expression.
        expr: Box<Expr>,
        /// Collation name.
        collation: String,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Field selection from composite: (row).field
    FieldAccess {
        /// Composite expression.
        expr: Box<Expr>,
        /// Field name.
        field: String,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Scalar subquery: (SELECT ... LIMIT 1)
    /// Used in COALESCE, comparisons, etc.
    Subquery {
        /// Inner query.
        query: Box<super::Qail>,
        /// Optional alias.
        alias: Option<String>,
    },
    /// EXISTS subquery: EXISTS(SELECT ...)
    Exists {
        /// Inner query.
        query: Box<super::Qail>,
        /// Whether this is NOT EXISTS.
        negated: bool,
        /// Optional alias.
        alias: Option<String>,
    },
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Star => write!(f, "*"),
            Expr::Named(name) => write!(f, "{}", name),
            Expr::Aliased { name, alias } => write!(f, "{} AS {}", name, alias),
            Expr::Aggregate {
                col,
                func,
                distinct,
                filter,
                alias,
                args,
                order_by,
                within_group,
            } => {
                write!(f, "{}(", func)?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                if args.is_empty() {
                    write!(f, "{}", col)?;
                } else {
                    for (i, arg) in args.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", arg)?;
                    }
                }
                if !order_by.is_empty() {
                    write!(f, " ORDER BY {}", sort_keys_text(order_by))?;
                }
                write!(f, ")")?;
                if !within_group.is_empty() {
                    write!(
                        f,
                        " WITHIN GROUP (ORDER BY {})",
                        sort_keys_text(within_group)
                    )?;
                }
                if let Some(conditions) = filter {
                    write!(
                        f,
                        " FILTER (WHERE {})",
                        conditions
                            .iter()
                            .map(|c| c.to_string())
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    )?;
                }
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Cast {
                expr,
                target_type,
                alias,
            } => {
                write!(f, "{}::{}", expr, target_type)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Def {
                name,
                data_type,
                constraints,
            } => {
                write!(f, "{}:{}", name, data_type)?;
                for c in constraints {
                    write!(f, "^{}", c)?;
                }
                Ok(())
            }
            Expr::Mod { kind, col } => match kind {
                ModKind::Add => write!(f, "+{}", col),
                ModKind::Drop => write!(f, "-{}", col),
            },
            Expr::Window {
                name,
                func,
                params,
                filter,
                partition,
                order,
                frame,
            } => {
                write!(f, "{}:{}(", name, func)?;
                for (i, p) in params.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", p)?;
                }
                write!(f, ")")?;
                if let Some(conditions) = filter {
                    write!(
                        f,
                        " FILTER (WHERE {})",
                        conditions
                            .iter()
                            .map(|c| c.to_string())
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    )?;
                }

                // Print partitions if any
                if !partition.is_empty() {
                    write!(f, "{{Part=")?;
                    for (i, p) in partition.iter().enumerate() {
                        if i > 0 {
                            write!(f, ",")?;
                        }
                        write!(f, "{}", p)?;
                    }
                    if let Some(fr) = frame {
                        write!(f, ", Frame={:?}", fr)?; // Debug format for now
                    }
                    write!(f, "}}")?;
                } else if let Some(fr) = frame {
                    write!(f, "{{Frame={:?}}}", fr)?;
                }

                // Print order cages
                for _cage in order {
                    // Order cages are sort cages - display format TBD
                }
                Ok(())
            }
            Expr::Case {
                when_clauses,
                else_value,
                alias,
            } => {
                write!(f, "CASE")?;
                for (cond, val) in when_clauses {
                    // The whole predicate: printing only `cond.left` would
                    // turn `status = 'paid'` into a bare `status`.
                    write!(f, " WHEN {} {}", cond.left, cond.op.sql_symbol())?;
                    match (&cond.op, &cond.value) {
                        (crate::ast::Operator::IsNull | crate::ast::Operator::IsNotNull, _) => {}
                        (
                            crate::ast::Operator::Between | crate::ast::Operator::NotBetween,
                            Value::Array(bounds),
                        ) if bounds.len() == 2 => {
                            write!(f, " {} AND {}", bounds[0], bounds[1])?;
                        }
                        (_, value) => write!(f, " {}", value)?,
                    }
                    write!(f, " THEN {}", val)?;
                }
                if let Some(e) = else_value {
                    write!(f, " ELSE {}", e)?;
                }
                write!(f, " END")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::JsonAccess {
                column,
                path_segments,
                alias,
            } => {
                write!(f, "{}", column)?;
                for (segment, as_text) in path_segments {
                    let op = if *as_text { "->>" } else { "->" };
                    write!(f, "{}{}", op, segment)?;
                }
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::FunctionCall { name, args, alias } => {
                let args_str: Vec<String> = args.iter().map(|a| a.to_string()).collect();
                write!(f, "{}({})", name.to_uppercase(), args_str.join(", "))?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::SpecialFunction { name, args, alias } => {
                write!(f, "{}(", name.to_uppercase())?;
                for (i, (keyword, expr)) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    if let Some(kw) = keyword {
                        write!(f, "{} ", kw)?;
                    }
                    write!(f, "{}", expr)?;
                }
                write!(f, ")")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Binary {
                left,
                op,
                right,
                alias,
            } => {
                if op.is_postfix() {
                    write!(f, "({} {})", left, op)?;
                } else {
                    write!(f, "({} {} {})", left, op, right)?;
                }
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Literal(value) => write!(f, "{}", value),
            Expr::ArrayConstructor { elements, alias } => {
                write!(f, "ARRAY[")?;
                for (i, elem) in elements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "]")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::RowConstructor { elements, alias } => {
                write!(f, "ROW(")?;
                for (i, elem) in elements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, ")")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Subscript { expr, index, alias } => {
                if expr.needs_parens_for_subscript() {
                    write!(f, "({})[{}]", expr, index)?;
                } else {
                    write!(f, "{}[{}]", expr, index)?;
                }
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::ArraySlice {
                expr,
                lower,
                upper,
                alias,
            } => {
                if expr.needs_parens_for_subscript() {
                    write!(f, "({})[", expr)?;
                } else {
                    write!(f, "{}[", expr)?;
                }
                if let Some(lower) = lower {
                    write!(f, "{}", lower)?;
                }
                write!(f, ":")?;
                if let Some(upper) = upper {
                    write!(f, "{}", upper)?;
                }
                write!(f, "]")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Collate {
                expr,
                collation,
                alias,
            } => {
                write!(f, "{} COLLATE \"{}\"", expr, collation)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::FieldAccess { expr, field, alias } => {
                write!(f, "({}).{}", expr, field)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Subquery { query, alias } => {
                write!(f, "({})", query)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Exists {
                query,
                negated,
                alias,
            } => {
                if *negated {
                    write!(f, "NOT ")?;
                }
                write!(f, "EXISTS ({})", query)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
        }
    }
}

impl Expr {
    /// The output alias already attached to this expression, if any
    /// (`Window` reports its output name).
    pub fn alias_name(&self) -> Option<&str> {
        match self {
            Expr::Aliased { alias, .. } => Some(alias),
            Expr::Window { name, .. } => Some(name),
            Expr::Aggregate { alias, .. }
            | Expr::Cast { alias, .. }
            | Expr::Case { alias, .. }
            | Expr::JsonAccess { alias, .. }
            | Expr::FunctionCall { alias, .. }
            | Expr::SpecialFunction { alias, .. }
            | Expr::Binary { alias, .. }
            | Expr::ArrayConstructor { alias, .. }
            | Expr::RowConstructor { alias, .. }
            | Expr::Subscript { alias, .. }
            | Expr::ArraySlice { alias, .. }
            | Expr::Collate { alias, .. }
            | Expr::FieldAccess { alias, .. }
            | Expr::Subquery { alias, .. }
            | Expr::Exists { alias, .. } => alias.as_deref(),
            Expr::Star
            | Expr::Named(_)
            | Expr::Def { .. }
            | Expr::Mod { .. }
            | Expr::Literal(_) => None,
        }
    }

    /// Attach `alias` to any expression that can carry one; a `Named`
    /// column becomes `Aliased`, and a `Window` takes it as its output name.
    ///
    /// Returns `false` and leaves the expression unchanged when it has no
    /// alias slot (`Star`, `Literal`, an already `Aliased` name, DDL nodes),
    /// so callers can refuse instead of silently dropping the column name.
    pub fn set_alias(&mut self, alias: impl Into<String>) -> bool {
        let alias = alias.into();
        let slot = match self {
            Expr::Named(name) => {
                let name = std::mem::take(name);
                *self = Expr::Aliased { name, alias };
                return true;
            }
            Expr::Window { name, .. } => {
                *name = alias;
                return true;
            }
            Expr::Aggregate { alias, .. }
            | Expr::Cast { alias, .. }
            | Expr::Case { alias, .. }
            | Expr::JsonAccess { alias, .. }
            | Expr::FunctionCall { alias, .. }
            | Expr::SpecialFunction { alias, .. }
            | Expr::Binary { alias, .. }
            | Expr::ArrayConstructor { alias, .. }
            | Expr::RowConstructor { alias, .. }
            | Expr::Subscript { alias, .. }
            | Expr::ArraySlice { alias, .. }
            | Expr::Collate { alias, .. }
            | Expr::FieldAccess { alias, .. }
            | Expr::Subquery { alias, .. }
            | Expr::Exists { alias, .. } => alias,
            Expr::Star
            | Expr::Aliased { .. }
            | Expr::Def { .. }
            | Expr::Mod { .. }
            | Expr::Literal(_) => return false,
        };
        *slot = Some(alias);
        true
    }

    /// Whether this expression must be wrapped in parentheses before `[...]`.
    ///
    /// PostgreSQL subscripts only a column reference, a positional parameter,
    /// or a parenthesized expression: `array_append(a, 2)[1]` is a syntax
    /// error and must be written `(array_append(a, 2))[1]`. Chained
    /// subscripts (`m[1][2]`) are already in subscriptable form.
    pub fn needs_parens_for_subscript(&self) -> bool {
        match self {
            Expr::Named(name) => !is_subscriptable_name(name),
            Expr::Subscript { .. } | Expr::ArraySlice { .. } => false,
            _ => true,
        }
    }
}

fn is_subscriptable_name(name: &str) -> bool {
    if let Some(digits) = name.strip_prefix('$') {
        return !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit());
    }
    !name.is_empty()
        && name.split('.').all(|part| {
            let mut chars = part.chars();
            matches!(chars.next(), Some(ch) if ch.is_alphabetic() || ch == '_' || ch == '"')
                && chars.all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '"')
        })
}

/// Column constraint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Constraint {
    /// PRIMARY KEY.
    PrimaryKey,
    /// UNIQUE.
    Unique,
    /// NULL / nullable.
    Nullable,
    /// DEFAULT value.
    Default(String),
    /// CHECK constraint.
    Check(Vec<String>),
    /// COMMENT ON COLUMN.
    Comment(String),
    /// REFERENCES foreign key.
    References(String),
    /// GENERATED column.
    Generated(ColumnGeneration),
}

/// Generated column type (STORED or VIRTUAL)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ColumnGeneration {
    /// GENERATED ALWAYS AS (expr) STORED - computed and stored
    Stored(String),
    /// GENERATED ALWAYS AS (expr) - computed at query time (default in Postgres 18+)
    Virtual(String),
}

/// Window frame definition for window functions
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowFrame {
    /// ROWS BETWEEN start AND end
    Rows {
        /// Frame start bound.
        start: FrameBound,
        /// Frame end bound.
        end: FrameBound,
        /// Frame exclusion.
        #[serde(default, skip_serializing_if = "FrameExclusion::is_no_others")]
        exclude: FrameExclusion,
    },
    /// RANGE BETWEEN start AND end
    Range {
        /// Frame start bound.
        start: FrameBound,
        /// Frame end bound.
        end: FrameBound,
        /// Frame exclusion.
        #[serde(default, skip_serializing_if = "FrameExclusion::is_no_others")]
        exclude: FrameExclusion,
    },
    /// GROUPS BETWEEN start AND end (offsets count peer groups; needs ORDER BY)
    Groups {
        /// Frame start bound.
        start: FrameBound,
        /// Frame end bound.
        end: FrameBound,
        /// Frame exclusion.
        #[serde(default, skip_serializing_if = "FrameExclusion::is_no_others")]
        exclude: FrameExclusion,
    },
}

impl WindowFrame {
    /// Mode keyword, bounds and exclusion of this frame.
    pub fn parts(&self) -> (&'static str, &FrameBound, &FrameBound, FrameExclusion) {
        match self {
            WindowFrame::Rows {
                start,
                end,
                exclude,
            } => ("ROWS", start, end, *exclude),
            WindowFrame::Range {
                start,
                end,
                exclude,
            } => ("RANGE", start, end, *exclude),
            WindowFrame::Groups {
                start,
                end,
                exclude,
            } => ("GROUPS", start, end, *exclude),
        }
    }

    /// SQL text `MODE BETWEEN start AND end [EXCLUDE ...]`, or the reason
    /// PostgreSQL would reject the frame.
    ///
    /// PostgreSQL restricts offsets: ROWS and GROUPS take a non-negative
    /// integer, RANGE takes a non-negative value of the ORDER BY column's
    /// type (an interval for date/time columns).
    pub fn to_sql(&self) -> Result<String, &'static str> {
        let (mode, start, end, exclude) = self.parts();
        for bound in [start, end] {
            match bound {
                FrameBound::Preceding(n) | FrameBound::Following(n) if *n < 0 => {
                    return Err("frame offset must not be negative");
                }
                FrameBound::IntervalPreceding { amount, .. }
                | FrameBound::IntervalFollowing { amount, .. } => {
                    if mode != "RANGE" {
                        return Err("interval frame offsets require RANGE");
                    }
                    if *amount < 0 {
                        return Err("frame offset must not be negative");
                    }
                }
                _ => {}
            }
        }
        if matches!(start, FrameBound::UnboundedFollowing) {
            return Err("frame start cannot be UNBOUNDED FOLLOWING");
        }
        if matches!(end, FrameBound::UnboundedPreceding) {
            return Err("frame end cannot be UNBOUNDED PRECEDING");
        }
        let mut sql = format!("{mode} BETWEEN {} AND {}", start.to_sql(), end.to_sql());
        if let Some(exclusion) = exclude.sql_suffix() {
            sql.push(' ');
            sql.push_str(exclusion);
        }
        Ok(sql)
    }
}

/// Window frame boundary
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FrameBound {
    /// UNBOUNDED PRECEDING.
    UnboundedPreceding,
    /// n PRECEDING.
    Preceding(i32),
    /// CURRENT ROW.
    CurrentRow,
    /// n FOLLOWING.
    Following(i32),
    /// UNBOUNDED FOLLOWING.
    UnboundedFollowing,
    /// `INTERVAL 'amount unit' PRECEDING` (RANGE frames over date/time columns).
    IntervalPreceding {
        /// Non-negative interval amount.
        amount: i64,
        /// Interval unit.
        unit: crate::ast::values::IntervalUnit,
    },
    /// `INTERVAL 'amount unit' FOLLOWING` (RANGE frames over date/time columns).
    IntervalFollowing {
        /// Non-negative interval amount.
        amount: i64,
        /// Interval unit.
        unit: crate::ast::values::IntervalUnit,
    },
}

impl FrameBound {
    /// SQL text of this bound.
    pub fn to_sql(&self) -> String {
        match self {
            FrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
            FrameBound::Preceding(n) => format!("{n} PRECEDING"),
            FrameBound::CurrentRow => "CURRENT ROW".to_string(),
            FrameBound::Following(n) => format!("{n} FOLLOWING"),
            FrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
            FrameBound::IntervalPreceding { amount, unit } => {
                format!("INTERVAL '{amount} {unit}' PRECEDING")
            }
            FrameBound::IntervalFollowing { amount, unit } => {
                format!("INTERVAL '{amount} {unit}' FOLLOWING")
            }
        }
    }
}

/// Window frame exclusion (`EXCLUDE ...`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum FrameExclusion {
    /// `EXCLUDE NO OTHERS`, the default; rendered as nothing.
    #[default]
    NoOthers,
    /// `EXCLUDE CURRENT ROW`.
    CurrentRow,
    /// `EXCLUDE GROUP`: the current row and its ORDER BY peers.
    Group,
    /// `EXCLUDE TIES`: the current row's peers, keeping the row itself.
    Ties,
}

impl FrameExclusion {
    /// Serde helper: the default exclusion is omitted from payloads.
    pub fn is_no_others(&self) -> bool {
        matches!(self, FrameExclusion::NoOthers)
    }

    /// `EXCLUDE ...` text, or `None` for the default.
    pub fn sql_suffix(self) -> Option<&'static str> {
        match self {
            FrameExclusion::NoOthers => None,
            FrameExclusion::CurrentRow => Some("EXCLUDE CURRENT ROW"),
            FrameExclusion::Group => Some("EXCLUDE GROUP"),
            FrameExclusion::Ties => Some("EXCLUDE TIES"),
        }
    }
}

impl std::fmt::Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constraint::PrimaryKey => write!(f, "pk"),
            Constraint::Unique => write!(f, "uniq"),
            Constraint::Nullable => write!(f, "?"),
            Constraint::Default(val) => write!(f, "={}", val),
            Constraint::Check(vals) => write!(f, "check({})", vals.join(",")),
            Constraint::Comment(text) => write!(f, "comment(\"{}\")", text),
            Constraint::References(target) => write!(f, "ref({})", target),
            Constraint::Generated(generation) => match generation {
                ColumnGeneration::Stored(expr) => write!(f, "gen({})", expr),
                ColumnGeneration::Virtual(expr) => write!(f, "vgen({})", expr),
            },
        }
    }
}

/// Index definition for CREATE INDEX
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct IndexDef {
    /// Index name
    pub name: String,
    /// Target table
    pub table: String,
    /// Columns to index (ordered)
    pub columns: Vec<String>,
    /// Whether the index is unique.
    pub unique: bool,
    /// Index type (e.g., "keyword", "integer", "float", "geo", "text")
    pub index_type: Option<String>,
    /// INCLUDE columns for covering indexes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    /// Whether to create the index concurrently.
    #[serde(default)]
    pub concurrently: bool,
    /// Optional partial-index predicate (`WHERE ...` body without the keyword).
    pub where_clause: Option<String>,
}

/// Table-level constraints for composite keys
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableConstraint {
    /// Composite UNIQUE constraint.
    Unique(Vec<String>),
    /// Composite PRIMARY KEY.
    PrimaryKey(Vec<String>),
    /// Composite FOREIGN KEY constraint.
    ForeignKey {
        /// Optional constraint name.
        name: Option<String>,
        /// Source columns.
        columns: Vec<String>,
        /// Referenced table.
        ref_table: String,
        /// Referenced columns.
        ref_columns: Vec<String>,
        /// Optional ON DELETE action.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_delete: Option<String>,
        /// Optional ON UPDATE action.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_update: Option<String>,
        /// Optional DEFERRABLE clause.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deferrable: Option<String>,
    },
}

// ==================== From Implementations for Ergonomic API ====================

impl From<&str> for Expr {
    /// Convert a string reference to a Named expression.
    /// Enables: `.select(["id", "name"])` instead of `.select([col("id"), col("name")])`
    fn from(s: &str) -> Self {
        Expr::Named(s.to_string())
    }
}

impl From<String> for Expr {
    fn from(s: String) -> Self {
        Expr::Named(s)
    }
}

impl From<&String> for Expr {
    fn from(s: &String) -> Self {
        Expr::Named(s.clone())
    }
}

// ==================== Function and Trigger Definitions ====================

/// PostgreSQL function definition
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FunctionDef {
    /// Function name.
    pub name: String,
    /// Function arguments (e.g., "v int", "tenant uuid").
    pub args: Vec<String>,
    /// Return type (e.g., "trigger", "integer", "void").
    pub returns: String,
    /// Function body (PL/pgSQL code).
    pub body: String,
    /// Language (default: plpgsql).
    pub language: Option<String>,
    /// Volatility modifier (IMMUTABLE/STABLE/VOLATILE), if specified.
    pub volatility: Option<String>,
}

/// Trigger timing (BEFORE or AFTER)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerTiming {
    /// BEFORE.
    Before,
    /// AFTER.
    After,
    /// INSTEAD OF.
    InsteadOf,
}

/// Trigger event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerEvent {
    /// INSERT.
    Insert,
    /// UPDATE.
    Update,
    /// DELETE.
    Delete,
    /// TRUNCATE.
    Truncate,
}

/// PostgreSQL trigger definition
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TriggerDef {
    /// Trigger name.
    pub name: String,
    /// Target table.
    pub table: String,
    /// Timing (BEFORE, AFTER, INSTEAD OF).
    pub timing: TriggerTiming,
    /// Events that fire the trigger.
    pub events: Vec<TriggerEvent>,
    /// Optional column list for `UPDATE OF` triggers.
    pub update_columns: Vec<String>,
    /// Whether the trigger fires FOR EACH ROW.
    pub for_each_row: bool,
    /// Function to execute.
    pub execute_function: String,
}
