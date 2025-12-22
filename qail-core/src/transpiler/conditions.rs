use crate::ast::*;
use super::traits::SqlGenerator;

/// Context for parameterized query building.
#[derive(Debug, Default)]
pub struct ParamContext {
    /// Current parameter index (1-based for Postgres $1, $2, etc.)
    pub index: usize,
    /// Collected parameter values in order
    pub params: Vec<Value>,
}

impl ParamContext {
    pub fn new() -> Self {
        Self { index: 0, params: Vec::new() }
    }

    /// Add a value and return the placeholder for it.
    pub fn add_param(&mut self, value: Value, generator: &dyn SqlGenerator) -> String {
        self.index += 1;
        self.params.push(value);
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
        let col = if let Some(cmd) = context {
            resolve_col_syntax(&self.column, cmd, generator.as_ref())
        } else {
            generator.quote_identifier(&self.column)
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
        match self.op {
            Operator::Eq => format!("{} = {}", col, self.to_value_sql(generator)),
            Operator::Ne => format!("{} != {}", col, self.to_value_sql(generator)),
            Operator::Gt => format!("{} > {}", col, self.to_value_sql(generator)),
            Operator::Gte => format!("{} >= {}", col, self.to_value_sql(generator)),
            Operator::Lt => format!("{} < {}", col, self.to_value_sql(generator)),
            Operator::Lte => format!("{} <= {}", col, self.to_value_sql(generator)),
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
        let col = if let Some(cmd) = context {
            resolve_col_syntax(&self.column, cmd, generator.as_ref())
        } else {
            generator.quote_identifier(&self.column)
        };

        // Helper to convert value to placeholder
        let value_placeholder = |v: &Value, p: &mut ParamContext| -> String {
            match v {
                Value::Param(n) => generator.placeholder(*n), // Already a placeholder
                Value::Null => "NULL".to_string(),
                other => p.add_param(other.clone(), generator.as_ref()),
            }
        };

        match self.op {
            Operator::Eq => format!("{} = {}", col, value_placeholder(&self.value, params)),
            Operator::Ne => format!("{} != {}", col, value_placeholder(&self.value, params)),
            Operator::Gt => format!("{} > {}", col, value_placeholder(&self.value, params)),
            Operator::Gte => format!("{} >= {}", col, value_placeholder(&self.value, params)),
            Operator::Lt => format!("{} < {}", col, value_placeholder(&self.value, params)),
            Operator::Lte => format!("{} <= {}", col, value_placeholder(&self.value, params)),
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
        }
    }
}
