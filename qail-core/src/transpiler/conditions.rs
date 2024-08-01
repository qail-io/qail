use crate::ast::*;
use super::traits::SqlGenerator;

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
            v => v.to_string(), 
        }
    }
}
