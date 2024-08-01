use crate::ast::*;
use super::traits::SqlGenerator;
use super::ToSql;

/// Context for parameterized query building.
#[derive(Debug, Default)]
pub struct ParamContext {
    /// Current parameter index (1-based for Postgres $1, $2, etc.)
    pub index: usize,
    /// Collected parameter values in order
    pub params: Vec<Value>,
    /// Names of named parameters in order (for :name → $n mapping)
    pub named_params: Vec<String>,
}

impl ParamContext {
    pub fn new() -> Self {
        Self { index: 0, params: Vec::new(), named_params: Vec::new() }
    }

    /// Add a value and return the placeholder for it.
    pub fn add_param(&mut self, value: Value, generator: &dyn SqlGenerator) -> String {
        self.index += 1;
        self.params.push(value);
        generator.placeholder(self.index)
    }

    /// Add a named parameter and return the placeholder for it.
    pub fn add_named_param(&mut self, name: String, generator: &dyn SqlGenerator) -> String {
        self.index += 1;
        self.named_params.push(name);
        generator.placeholder(self.index)
    }
}

/// Helper to resolve a column identifier that might be a JSON path or a JOIN reference.
/// 
/// Heuristic:
/// 1. Split by '.'
/// 2. If single part -> quote_identifier
/// 3. If multiple parts:
///    - If first part matches table name or any join alias -> Treat as "Table"."Col".
///    - Else -> Treat as "Col"->"Field" (JSON).
fn resolve_col_syntax(col: &str, cmd: &QailCmd, generator: &dyn SqlGenerator) -> String {
    // Check for raw SQL syntax { ... }
    if col.starts_with('{') && col.ends_with('}') {
        return col[1..col.len()-1].to_string();
    }

    let parts: Vec<&str> = col.split('.').collect();
    if parts.len() <= 1 {
        return generator.quote_identifier(col);
    }
    
    let first = parts[0];
    
    // Check main table matches
    if first == cmd.table {
        // table.col
        return format!("{}.{}", generator.quote_identifier(first), generator.quote_identifier(parts[1]));
    }
    
    // Check joins matches
    for join in &cmd.joins {
        if first == join.table {
             // join_table.col
             return format!("{}.{}", generator.quote_identifier(first), generator.quote_identifier(parts[1]));
        }
    }
    
    // Default: treated as JSON access on the first part
    let col_name = parts[0];
    let path = &parts[1..];
    generator.json_access(col_name, path)
}

pub trait ConditionToSql {
    fn to_sql(&self, generator: &Box<dyn SqlGenerator>, context: Option<&QailCmd>) -> String;
    fn to_value_sql(&self, generator: &Box<dyn SqlGenerator>) -> String;
    
    /// Convert condition to SQL with parameterized values.
    /// Returns the SQL fragment and updates the ParamContext with extracted values.
    fn to_sql_parameterized(
        &self, 
        generator: &Box<dyn SqlGenerator>, 
        context: Option<&QailCmd>,
        params: &mut ParamContext
    ) -> String;
}

impl ConditionToSql for Condition {
    /// Convert condition to SQL string.
    fn to_sql(&self, generator: &Box<dyn SqlGenerator>, context: Option<&QailCmd>) -> String {
        let col = match &self.left {
            Expr::Named(name) => {
                // Handle raw SQL {content} first - context-independent
                if name.starts_with('{') && name.ends_with('}') {
                    name[1..name.len()-1].to_string()
                } else if let Some(cmd) = context {
                    resolve_col_syntax(name, cmd, generator.as_ref())
                } else {
                    generator.quote_identifier(name)
                }
            },
            Expr::JsonAccess { column, path, as_text, .. } => {
                let op = if *as_text { "->>" } else { "->" };
                format!("{}{}'{}'", generator.quote_identifier(column), op, path)
            },
            expr => expr.to_string(),
        };

        // Handle array unnest conditions
        if self.is_array_unnest {
             let inner_condition = match self.op {
                Operator::Eq => format!("_el = {}", self.to_value_sql(generator)),
                Operator::Ne => format!("_el != {}", self.to_value_sql(generator)),
                Operator::Gt => format!("_el > {}", self.to_value_sql(generator)),
                Operator::Gte => format!("_el >= {}", self.to_value_sql(generator)),
                Operator::Lt => format!("_el < {}", self.to_value_sql(generator)),
                Operator::Lte => format!("_el <= {}", self.to_value_sql(generator)),
                Operator::Fuzzy => {
                    let val = match &self.value {
                        Value::String(s) => format!("'%{}%'", s),
                        Value::Param(n) => {
                             let p = generator.placeholder(*n);
                             generator.string_concat(&["'%'", &p, "'%'"])
                        },
                         v => format!("'%{}%'", v),
                    };
                    format!("_el {} {}", generator.fuzzy_operator(), val)
                }
                _ => format!("_el = {}", self.to_value_sql(generator)),
            };
            return format!(
                "EXISTS (SELECT 1 FROM unnest({}) _el WHERE {})",
                col, inner_condition
            );
        }
        
        // Normal conditions
        // Simple binary operators use sql_symbol() for unified handling
        if self.op.is_simple_binary() {
            return format!("{} {} {}", col, self.op.sql_symbol(), self.to_value_sql(generator));
        }
        
        // Special operators that need custom handling
        match self.op {
            Operator::Fuzzy => {
                let val = match &self.value {
                    Value::String(s) => format!("'%{}%'", s),
                    Value::Param(n) => {
                        let p = generator.placeholder(*n);
                        generator.string_concat(&["'%'", &p, "'%'"])
                    },
                    v => format!("'%{}%'", v),
                };
                format!("{} {} {}", col, generator.fuzzy_operator(), val)
            }
            Operator::In => format!("{} = ANY({})", col, self.value), // TODO: ANY() is Postgres specific, move to generator?
            Operator::NotIn => format!("{} != ALL({})", col, self.value),
            Operator::IsNull => format!("{} IS NULL", col),
            Operator::IsNotNull => format!("{} IS NOT NULL", col),
            Operator::Contains => generator.json_contains(&col, &self.to_value_sql(generator)),
            Operator::KeyExists => generator.json_key_exists(&col, &self.to_value_sql(generator)),
            // Postgres 17+ SQL/JSON standard functions
            Operator::JsonExists => {
                let path = self.to_value_sql(generator);
                generator.json_exists(&col, &path.trim_matches('\''))
            }
            Operator::JsonQuery => {
                let path = self.to_value_sql(generator);
                generator.json_query(&col, &path.trim_matches('\''))
            }
            Operator::JsonValue => {
                let path = self.to_value_sql(generator);
                format!("{} = {}", generator.json_value(&col, &path.trim_matches('\'')), self.to_value_sql(generator))
            }
            Operator::Between => {
                // Value is Array with 2 elements [min, max]
                if let Value::Array(vals) = &self.value {
                    if vals.len() >= 2 {
                        return format!("{} BETWEEN {} AND {}", col, vals[0], vals[1]);
                    }
                }
                format!("{} BETWEEN {}", col, self.value)
            }
            Operator::NotBetween => {
                if let Value::Array(vals) = &self.value {
                    if vals.len() >= 2 {
                        return format!("{} NOT BETWEEN {} AND {}", col, vals[0], vals[1]);
                    }
                }
                format!("{} NOT BETWEEN {}", col, self.value)
            }
            Operator::Exists => {
                // EXISTS takes subquery, col is ignored
                if let Value::Subquery(cmd) = &self.value {
                    let subquery_sql = cmd.to_sql();
                    format!("EXISTS ({})", subquery_sql)
                } else {
                    format!("EXISTS ({})", self.value)
                }
            }
            Operator::NotExists => {
                if let Value::Subquery(cmd) = &self.value {
                    let subquery_sql = cmd.to_sql();
                    format!("NOT EXISTS ({})", subquery_sql)
                } else {
                    format!("NOT EXISTS ({})", self.value)
                }
            }
            // Simple binary operators are handled above by is_simple_binary()
            _ => format!("{} {} {}", col, self.op.sql_symbol(), self.to_value_sql(generator)),
        }
    }

    fn to_value_sql(&self, generator: &Box<dyn SqlGenerator>) -> String {
        match &self.value {
            Value::Param(n) => generator.placeholder(*n),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bool(b) => generator.bool_literal(*b),
            Value::Subquery(cmd) => {
                // Use ToSql trait to generate subquery SQL
                use crate::transpiler::ToSql;
                format!("({})", cmd.to_sql())
            }
            Value::Column(col) => {
                 // Determine if it's "table"."col" or just "col"
                 // Use resolve_col_syntax logic? Or simply quote?
                 // Usually Join ON RHS is just an identifier, but transpiler logic in resolve_col_syntax
                 // requires a QailCmd context which we don't have here efficiently (we have context: Option<&QailCmd> in other methods but strictly to_value_sql signature is fixed?).
                 // Wait, to_value_sql signature is: fn to_value_sql(&self, generator: &Box<dyn SqlGenerator>) -> String
                 // We don't have context here.
                 // However, we can use a basic split check or just quote full string.
                 // If col is "users.id", generator.quote_identifier("users.id") might quote the whole thing which is wrong for Postgres ("users.id" vs "users"."id").
                 // We should manually split if dot is present.
                 if col.contains('.') {
                     let parts: Vec<&str> = col.split('.').collect();
                     parts.iter().map(|p| generator.quote_identifier(p)).collect::<Vec<String>>().join(".")
                 } else {
                     generator.quote_identifier(col)
                 }
            }
            v => v.to_string(), 
        }
    }

    fn to_sql_parameterized(
        &self, 
        generator: &Box<dyn SqlGenerator>, 
        context: Option<&QailCmd>,
        params: &mut ParamContext
    ) -> String {
        let col = match &self.left {
            Expr::Named(name) => {
                // Handle raw SQL {content} first - context-independent
                if name.starts_with('{') && name.ends_with('}') {
                    name[1..name.len()-1].to_string()
                } else if let Some(cmd) = context {
                    resolve_col_syntax(name, cmd, generator.as_ref())
                } else {
                    generator.quote_identifier(name)
                }
            },
            Expr::JsonAccess { column, path, as_text, .. } => {
                let op = if *as_text { "->>" } else { "->" };
                format!("{}{}'{}'", generator.quote_identifier(column), op, path)
            },
            expr => expr.to_string(),
        };

        // Helper to convert value to placeholder
        let value_placeholder = |v: &Value, p: &mut ParamContext| -> String {
            match v {
                Value::Param(n) => generator.placeholder(*n), // Already a placeholder
                Value::NamedParam(name) => p.add_named_param(name.clone(), generator.as_ref()),
                Value::Null => "NULL".to_string(),
                other => p.add_param(other.clone(), generator.as_ref()),
            }
        };

        match self.op {
            Operator::Eq => {
                // Raw conditions ({...}, op=Eq, value=Null) are now handled at col resolution
                if matches!(self.value, Value::Null) {
                    if let Expr::Named(name) = &self.left {
                        if name.starts_with('{') && name.ends_with('}') {
                            return col; // col already contains raw SQL content
                        }
                    }
                }
                format!("{} = {}", col, value_placeholder(&self.value, params))
            },
            Operator::Fuzzy => {
                // For LIKE, we need to wrap in wildcards
                let placeholder = value_placeholder(&self.value, params);
                format!("{} {} {}", col, generator.fuzzy_operator(), placeholder)
            }
            Operator::IsNull => format!("{} IS NULL", col),
            Operator::IsNotNull => format!("{} IS NOT NULL", col),
            Operator::In => format!("{} = ANY({})", col, value_placeholder(&self.value, params)),
            Operator::NotIn => format!("{} != ALL({})", col, value_placeholder(&self.value, params)),
            Operator::Contains => generator.json_contains(&col, &value_placeholder(&self.value, params)),
            Operator::KeyExists => generator.json_key_exists(&col, &value_placeholder(&self.value, params)),
            Operator::JsonExists => {
                let path = value_placeholder(&self.value, params);
                generator.json_exists(&col, &path)
            }
            Operator::JsonQuery => {
                let path = value_placeholder(&self.value, params);
                generator.json_query(&col, &path)
            }
            Operator::JsonValue => {
                let path = value_placeholder(&self.value, params);
                format!("{} = {}", generator.json_value(&col, &path), value_placeholder(&self.value, params))
            }
            Operator::Between => {
                if let Value::Array(vals) = &self.value {
                    if vals.len() >= 2 {
                        return format!("{} BETWEEN {} AND {}", col, vals[0], vals[1]);
                    }
                }
                format!("{} BETWEEN {}", col, self.value)
            }
            Operator::NotBetween => {
                if let Value::Array(vals) = &self.value {
                    if vals.len() >= 2 {
                        return format!("{} NOT BETWEEN {} AND {}", col, vals[0], vals[1]);
                    }
                }
                format!("{} NOT BETWEEN {}", col, self.value)
            }
            Operator::Exists => {
                if let Value::Subquery(cmd) = &self.value {
                    let subquery_sql = cmd.to_sql();
                    format!("EXISTS ({})", subquery_sql)
                } else {
                    format!("EXISTS ({})", self.value)
                }
            }
            Operator::NotExists => {
                if let Value::Subquery(cmd) = &self.value {
                    let subquery_sql = cmd.to_sql();
                    format!("NOT EXISTS ({})", subquery_sql)
                } else {
                    format!("NOT EXISTS ({})", self.value)
                }
            }
            // Simple operators (Ne, Gt, Gte, Lt, Lte, Like, NotLike, ILike, NotILike) use sql_symbol()
            _ => format!("{} {} {}", col, self.op.sql_symbol(), value_placeholder(&self.value, params)),
        }
    }
}
