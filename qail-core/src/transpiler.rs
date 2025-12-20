//! SQL Transpiler for QAIL AST.
//!
//! Converts parsed QAIL commands into executable SQL strings.

use crate::ast::*;

/// Trait for converting AST nodes to SQL.
pub trait ToSql {
    /// Convert this node to a SQL string.
    fn to_sql(&self) -> String;
}

impl ToSql for QailCmd {
    fn to_sql(&self) -> String {
        match self.action {
            Action::Get => self.to_select_sql(),
            Action::Set => self.to_update_sql(),
            Action::Del => self.to_delete_sql(),
            Action::Add => self.to_insert_sql(),
            Action::Gen => format!("-- gen::{}  (generates Rust struct, not SQL)", self.table),
            Action::Make => self.to_create_table_sql(),
            Action::Mod => self.to_alter_table_sql(),
            Action::Over => self.to_window_sql(),
            Action::With => self.to_cte_sql(),
        }
    }
}

impl QailCmd {
    /// Generate SELECT SQL.
    fn to_select_sql(&self) -> String {
        let mut sql = String::from("SELECT ");

        // Columns
        if self.columns.is_empty() {
            sql.push('*');
        } else {
            let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
            sql.push_str(&cols.join(", "));
        }

        // FROM
        sql.push_str(" FROM ");
        sql.push_str(&self.table);

        // JOINS
        for join in &self.joins {
            let kind = match join.kind {
                JoinKind::Inner => "INNER",
                JoinKind::Left => "LEFT",
                JoinKind::Right => "RIGHT",
            };
            // Heuristic: target.source_singular_id = source.id
            // e.g. users -> posts => posts.user_id = users.id
            let source_singular = self.table.trim_end_matches('s');
            sql.push_str(&format!(
                " {} JOIN {} ON {}.{}_id = {}.id",
                kind, join.table, join.table, source_singular, self.table
            ));
        }
        
        // Prepare for GROUP BY check
        let has_aggregates = self.columns.iter().any(|c| matches!(c, Column::Aggregate { .. }));
        let mut non_aggregated_cols = Vec::new();
        if has_aggregates {
             for col in &self.columns {
                 if let Column::Named(name) = col {
                     non_aggregated_cols.push(name.clone());
                 }
             }
        }

        // Process cages
        let mut where_groups: Vec<String> = Vec::new();
        let mut order_by: Option<String> = None;
        let mut limit: Option<usize> = None;
        let mut offset: Option<usize> = None;

        for cage in &self.cages {
            match &cage.kind {
                CageKind::Filter => {
                    if !cage.conditions.is_empty() {
                        let joiner = match cage.logical_op {
                            LogicalOp::And => " AND ",
                            LogicalOp::Or => " OR ",
                        };
                        let conditions: Vec<String> = cage.conditions.iter().map(|c| c.to_sql()).collect();
                        let group = conditions.join(joiner);
                        // Wrap OR groups in parentheses for correct precedence
                        if cage.logical_op == LogicalOp::Or && cage.conditions.len() > 1 {
                            where_groups.push(format!("({})", group));
                        } else {
                            where_groups.push(group);
                        }
                    }
                }
                CageKind::Sort(order) => {
                    if let Some(cond) = cage.conditions.first() {
                        let dir = match order {
                            SortOrder::Asc => "ASC",
                            SortOrder::Desc => "DESC",
                        };
                        order_by = Some(format!("{} {}", cond.column, dir));
                    }
                }
                CageKind::Limit(n) => {
                    limit = Some(*n);
                }
                CageKind::Offset(n) => {
                    offset = Some(*n);
                }
                CageKind::Payload => {
                    // Not used in SELECT
                }
            }
        }

        // WHERE - each cage group is joined with AND
        if !where_groups.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_groups.join(" AND "));
        }

        // GROUP BY
        if !non_aggregated_cols.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&non_aggregated_cols.join(", "));
        }

        // ORDER BY
        if let Some(order) = order_by {
            sql.push_str(" ORDER BY ");
            sql.push_str(&order);
        }

        // LIMIT
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {}", n));
        }

        // OFFSET
        if let Some(n) = offset {
            sql.push_str(&format!(" OFFSET {}", n));
        }

        sql
    }

    /// Generate UPDATE SQL.
    fn to_update_sql(&self) -> String {
        let mut sql = String::from("UPDATE ");
        sql.push_str(&self.table);

        // For SET queries, first cage is payload, rest are filters
        let mut set_clauses: Vec<String> = Vec::new();
        let mut where_clauses: Vec<String> = Vec::new();
        let mut is_first_filter = true;

        for cage in &self.cages {
            if let CageKind::Filter = cage.kind {
                if is_first_filter {
                    fn map_type(t: &str) -> &str {
                        match t {
                            "str" | "text" | "string" => "VARCHAR(255)",
                            "int" | "i32" => "INT",
                            "bigint" | "i64" => "BIGINT",
                            "uuid" => "UUID",
                            "bool" | "boolean" => "BOOLEAN",
                            "dec" | "decimal" => "DECIMAL",
                            "float" | "f64" => "DOUBLE PRECISION",
                            "serial" => "SERIAL",
                            "timestamp" | "time" => "TIMESTAMP",
                            "json" | "jsonb" => "JSONB",
                            _ => t, // usage of raw types if unknown
                        }
                    }
                    // First filter cage is the SET payload
                    for cond in &cage.conditions {
                        set_clauses.push(format!("{} = {}", cond.column, cond.value));
                    }
                    is_first_filter = false;
                } else {
                    // Subsequent filter cages are WHERE conditions
                    for cond in &cage.conditions {
                        where_clauses.push(cond.to_sql());
                    }
                }
            }
        }

        // SET clause
        if !set_clauses.is_empty() {
            sql.push_str(" SET ");
            sql.push_str(&set_clauses.join(", "));
        }

        // WHERE clause
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql
    }

    /// Generate DELETE SQL.
    fn to_delete_sql(&self) -> String {
        let mut sql = String::from("DELETE FROM ");
        sql.push_str(&self.table);

        // Process WHERE clauses
        let mut where_clauses: Vec<String> = Vec::new();

        for cage in &self.cages {
            if let CageKind::Filter = cage.kind {
                for cond in &cage.conditions {
                    where_clauses.push(cond.to_sql());
                }
            }
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql
    }

    /// Generate INSERT SQL.
    fn to_insert_sql(&self) -> String {
        let mut sql = String::from("INSERT INTO ");
        sql.push_str(&self.table);

        // For ADD queries, we use columns and first cage contains values
        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        
        if !cols.is_empty() {
            sql.push_str(" (");
            sql.push_str(&cols.join(", "));
            sql.push(')');
        }

        // Values from first payload/filter cage
        if let Some(cage) = self.cages.first() {
            let values: Vec<String> = cage.conditions.iter().map(|c| c.value.to_string()).collect();
            
            if !values.is_empty() {
                sql.push_str(" VALUES (");
                sql.push_str(&values.join(", "));
                sql.push(')');
            }
        }

        sql
    }

    /// Generate CREATE TABLE SQL.
    fn to_create_table_sql(&self) -> String {
        let mut sql = String::new();
        sql.push_str("CREATE TABLE ");
        sql.push_str(&self.table);
        sql.push_str(" (\n");

        let mut defs = Vec::new();
        for col in &self.columns {
            if let Column::Def {
                name,
                data_type,
                constraints,
            } = col
            {
                let sql_type = map_type(data_type);
                let mut line = format!("    {} {}", name, sql_type);

                // Default to NOT NULL unless Nullable (?) constraint is present
                let is_nullable = constraints.contains(&Constraint::Nullable);
                if !is_nullable {
                    line.push_str(" NOT NULL");
                }

                if constraints.contains(&Constraint::PrimaryKey) {
                    line.push_str(" PRIMARY KEY");
                }
                if constraints.contains(&Constraint::Unique) {
                    line.push_str(" UNIQUE");
                }
                defs.push(line);
            }
        }
        sql.push_str(&defs.join(",\n"));
        sql.push_str("\n)");
        sql
    }

    /// Generate ALTER TABLE SQL.
    fn to_alter_table_sql(&self) -> String {
        let mut stmts = Vec::new();
        for col in &self.columns {
            match col {
                Column::Mod { kind, col } => {
                    match kind {
                        ModKind::Add => {
                            if let Column::Def { name, data_type, constraints } = col.as_ref() {
                                let sql_type = map_type(data_type);
                                let mut line = format!("ALTER TABLE {} ADD COLUMN {} {}", self.table, name, sql_type);
                                
                                let is_nullable = constraints.contains(&Constraint::Nullable);
                                if !is_nullable {
                                    line.push_str(" NOT NULL");
                                }

                                if constraints.contains(&Constraint::Unique) {
                                    line.push_str(" UNIQUE");
                                }
                                stmts.push(line);
                            }
                        }
                        ModKind::Drop => {
                            if let Column::Named(name) = col.as_ref() {
                                stmts.push(format!("ALTER TABLE {} DROP COLUMN {}", self.table, name));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        stmts.join(";\n")
    }

    /// Generate Window Function SQL (Pillar 8).
    fn to_window_sql(&self) -> String {
        // Build SELECT with window function columns
        let mut sql = String::from("SELECT ");

        let cols: Vec<String> = self.columns.iter().map(|c| {
            match c {
                Column::Window { name, func, params, partition, order } => {
                    let params_str = if params.is_empty() {
                        String::new()
                    } else {
                        params.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")
                    };
                    
                    let mut over_clause = String::from("OVER (");
                    if !partition.is_empty() {
                        over_clause.push_str("PARTITION BY ");
                        over_clause.push_str(&partition.join(", "));
                        if !order.is_empty() {
                            over_clause.push(' ');
                        }
                    }
                    if !order.is_empty() {
                        over_clause.push_str("ORDER BY ");
                        let order_parts: Vec<String> = order.iter().map(|cage| {
                            match &cage.kind {
                                CageKind::Sort(SortOrder::Asc) => {
                                    if let Some(cond) = cage.conditions.first() {
                                        format!("{} ASC", cond.column)
                                    } else {
                                        String::new()
                                    }
                                }
                                CageKind::Sort(SortOrder::Desc) => {
                                    if let Some(cond) = cage.conditions.first() {
                                        format!("{} DESC", cond.column)
                                    } else {
                                        String::new()
                                    }
                                }
                                _ => String::new(),
                            }
                        }).filter(|s| !s.is_empty()).collect();
                        over_clause.push_str(&order_parts.join(", "));
                    }
                    over_clause.push(')');
                    
                    format!("{}({}) {} AS {}", func, params_str, over_clause, name)
                }
                _ => c.to_string(),
            }
        }).collect();

        sql.push_str(&cols.join(", "));
        sql.push_str(" FROM ");
        sql.push_str(&self.table);

        // Handle cages (WHERE, LIMIT, etc.)
        let where_clauses: Vec<String> = self.cages.iter()
            .filter(|c| matches!(c.kind, CageKind::Filter))
            .flat_map(|c| c.conditions.iter().map(|cond| cond.to_sql()))
            .collect();

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql
    }

    /// Generate CTE SQL (Pillar 8).
    fn to_cte_sql(&self) -> String {
        // For WITH, the table is the CTE name. We generate: WITH cte_name AS (...) SELECT * FROM cte_name
        // The inner query comes from cages (filter/limit). This is a stub—full impl needs chained queries.
        let mut sql = String::from("WITH ");
        sql.push_str(&self.table);
        sql.push_str(" AS (");

        // Generate inner SELECT from columns and cages
        sql.push_str("SELECT ");
        if self.columns.is_empty() {
            sql.push('*');
        } else {
            let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
            sql.push_str(&cols.join(", "));
        }
        sql.push_str(" FROM ");
        sql.push_str(&self.table);

        // Cages for inner query
        let where_clauses: Vec<String> = self.cages.iter()
            .filter(|c| matches!(c.kind, CageKind::Filter))
            .flat_map(|c| c.conditions.iter().map(|cond| cond.to_sql()))
            .collect();
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        
        for cage in &self.cages {
            if let CageKind::Limit(n) = cage.kind {
                sql.push_str(&format!(" LIMIT {}", n));
            }
        }

        sql.push_str(") SELECT * FROM ");
        sql.push_str(&self.table);

        sql
    }
}

fn map_type(t: &str) -> &str {
    match t {
        "str" | "text" | "string" => "VARCHAR(255)",
        "int" | "i32" => "INT",
        "bigint" | "i64" => "BIGINT",
        "uuid" => "UUID",
        "bool" | "boolean" => "BOOLEAN",
        "dec" | "decimal" => "DECIMAL",
        "float" | "f64" => "DOUBLE PRECISION",
        "serial" => "SERIAL",
        "timestamp" | "time" => "TIMESTAMP",
        "json" | "jsonb" => "JSONB",
        _ => t,
    }
}

impl Condition {
    /// Convert condition to SQL string.
    fn to_sql(&self) -> String {
        // Handle array unnest conditions: EXISTS (SELECT 1 FROM unnest(col) _el WHERE _el OP val)
        if self.is_array_unnest {
            let inner_condition = match self.op {
                Operator::Eq => format!("_el = {}", self.value),
                Operator::Ne => format!("_el != {}", self.value),
                Operator::Gt => format!("_el > {}", self.value),
                Operator::Gte => format!("_el >= {}", self.value),
                Operator::Lt => format!("_el < {}", self.value),
                Operator::Lte => format!("_el <= {}", self.value),
                Operator::Fuzzy => {
                    let val = match &self.value {
                        Value::String(s) => format!("'%{}%'", s),
                        Value::Param(n) => format!("'%' || ${} || '%'", n),
                        v => format!("'%{}%'", v),
                    };
                    format!("_el ILIKE {}", val)
                }
                _ => format!("_el = {}", self.value),
            };
            return format!(
                "EXISTS (SELECT 1 FROM unnest({}) _el WHERE {})",
                self.column, inner_condition
            );
        }
        
        // Normal conditions
        match self.op {
            Operator::Eq => format!("{} = {}", self.column, self.value),
            Operator::Ne => format!("{} != {}", self.column, self.value),
            Operator::Gt => format!("{} > {}", self.column, self.value),
            Operator::Gte => format!("{} >= {}", self.column, self.value),
            Operator::Lt => format!("{} < {}", self.column, self.value),
            Operator::Lte => format!("{} <= {}", self.column, self.value),
            Operator::Fuzzy => {
                // For fuzzy match, wrap the value in %...%
                let val = match &self.value {
                    Value::String(s) => format!("'%{}%'", s),
                    Value::Param(n) => format!("'%' || ${} || '%'", n),
                    v => format!("'%{}%'", v),
                };
                format!("{} ILIKE {}", self.column, val)
            }
            Operator::In => format!("{} = ANY({})", self.column, self.value),
            Operator::NotIn => format!("{} != ALL({})", self.column, self.value),
            Operator::IsNull => format!("{} IS NULL", self.column),
            Operator::IsNotNull => format!("{} IS NOT NULL", self.column),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    #[test]
    fn test_simple_select() {
        let cmd = parse("get::users•@*").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users");
    }

    #[test]
    fn test_select_columns() {
        let cmd = parse("get::users•@id@email@role").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT id, email, role FROM users");
    }

    #[test]
    fn test_select_with_where() {
        let cmd = parse("get::users•@*[active=true]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE active = true");
    }

    #[test]
    fn test_select_with_limit() {
        let cmd = parse("get::users•@*[lim=10]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users LIMIT 10");
    }

    #[test]
    fn test_select_with_order() {
        let cmd = parse("get::users•@*[^!created_at]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users ORDER BY created_at DESC");
    }

    #[test]
    fn test_select_complex() {
        let cmd = parse("get::users•@id@email[active=true][^!created_at][lim=10]").unwrap();
        assert_eq!(
            cmd.to_sql(),
            "SELECT id, email FROM users WHERE active = true ORDER BY created_at DESC LIMIT 10"
        );
    }

    #[test]
    fn test_update() {
        let cmd = parse("set::users•[verified=true][id=$1]").unwrap();
        assert_eq!(cmd.to_sql(), "UPDATE users SET verified = true WHERE id = $1");
    }

    #[test]
    fn test_delete() {
        let cmd = parse("del::sessions•[expired_at<now]").unwrap();
        // Note: 'now' is parsed as a function
        assert_eq!(cmd.to_sql(), "DELETE FROM sessions WHERE expired_at < now()");
    }

    #[test]
    fn test_fuzzy_match() {
        let cmd = parse("get::users•@*[name~$1]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE name ILIKE '%' || $1 || '%'");
    }

    #[test]
    fn test_or_conditions() {
        let cmd = parse("get::users•@*[role=admin|role=mod]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE (role = 'admin' OR role = 'mod')");
    }

    #[test]
    fn test_mixed_and_or() {
        let cmd = parse("get::users•@*[active=true][role=admin|role=mod]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE active = true AND (role = 'admin' OR role = 'mod')");
    }

    #[test]
    fn test_array_unnest() {
        let cmd = parse("get::posts•@*[tags[*]~$1]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM posts WHERE EXISTS (SELECT 1 FROM unnest(tags) _el WHERE _el ILIKE '%' || $1 || '%')");
    }

    #[test]
    fn test_complex_array_search() {
        let cmd = parse("get::kb•@*[active=true][keywords[*]~$1|question~$1]").unwrap();
        assert_eq!(
            cmd.to_sql(),
            "SELECT * FROM kb WHERE active = true AND (EXISTS (SELECT 1 FROM unnest(keywords) _el WHERE _el ILIKE '%' || $1 || '%') OR question ILIKE '%' || $1 || '%')"
        );
    }
}
