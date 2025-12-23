use std::fmt::{Result, Write};
use crate::ast::{QailCmd, Expr, Join, Cage, CageKind, Condition, Operator, Value, LogicalOp, SortOrder, Action};

#[cfg(test)]
mod tests;

pub struct Formatter {
    indent_level: usize,
    buffer: String,
}

impl Formatter {
    pub fn new() -> Self {
        Self {
            indent_level: 0,
            buffer: String::new(),
        }
    }

    pub fn format(mut self, cmd: &QailCmd) -> std::result::Result<String, std::fmt::Error> {
        self.visit_cmd(cmd)?;
        Ok(self.buffer)
    }

    fn indent(&mut self) -> Result {
        for _ in 0..self.indent_level {
            write!(self.buffer, "  ")?;
        }
        Ok(())
    }

    fn visit_cmd(&mut self, cmd: &QailCmd) -> Result {
        // Handle CTEs first
        for cte in &cmd.ctes {
            write!(self.buffer, "with {} = ", cte.name)?;
            self.indent_level += 1;
            writeln!(self.buffer)?;
            self.indent()?;
            self.visit_cmd(&cte.base_query)?;
            
            // Handle recursive part if present
            if cte.recursive {
                if let Some(ref recursive_query) = cte.recursive_query {
                    writeln!(self.buffer)?;
                    self.indent()?;
                    writeln!(self.buffer, "union all")?;
                    self.indent()?;
                    self.visit_cmd(recursive_query)?;
                }
            }
            
            self.indent_level -= 1;
            writeln!(self.buffer)?;
        }

        // Action and Table
        match cmd.action {
            Action::Get => write!(self.buffer, "get {}", cmd.table)?,
            Action::Set => write!(self.buffer, "set {}", cmd.table)?,
            Action::Del => write!(self.buffer, "del {}", cmd.table)?,
            Action::Add => write!(self.buffer, "add {}", cmd.table)?,
            _ => write!(self.buffer, "{} {}", cmd.action, cmd.table)?, // Fallback for others
        }
        writeln!(self.buffer)?;
        
        // self.indent_level += 1; // Removed: Clauses should act at same level as command
        
        // Cages: Group By (if any "by" equivalent exists? No, "by" is usually implicit in AST or explicit in group_by_mode?)
        // The proposal example shows "by phone_number".
        // In AST `cmd.rs`, there isn't a direct "Group By" list, usually inferred or group_by_mode.
        // Wait, where is `by phone_number` stored in AST? 
        // Checking `ast/cmd.rs`: `group_by_mode: GroupByMode`.
        // Usually group by is inferred from aggregates or explicit. 
        // If the AST doesn't have explicit group by columns, we might need to derive it or it's in `cages`?
        // Let's check `cages.rs` again. `CageKind` has `Filter`, `Sort`, `Limit`... no `GroupBy`.
        // Maybe it's implied by non-aggregated columns in a `Get` with aggregates? 
        // For now, I will skip "by" unless I find it in AST.
        
        // Columns (Fields)
        if !cmd.columns.is_empty() {
             // Check if all are Star, then maybe skip fields block? 
             // But proposal says "Canonical". 
             // "get table" implies "get table fields *" usually?
             // If manual explicit columns:
            if !(cmd.columns.len() == 1 && matches!(cmd.columns[0], Expr::Star)) {
                self.indent()?;
                writeln!(self.buffer, "fields")?;
                self.indent_level += 1;
                for (i, col) in cmd.columns.iter().enumerate() {
                    self.indent()?;
                    self.format_column(col)?;
                    if i < cmd.columns.len() - 1 {
                        writeln!(self.buffer, ",")?;
                    } else {
                        writeln!(self.buffer)?;
                    }
                }
                self.indent_level -= 1;
            }
        }

        // Joins
        for join in &cmd.joins {
            self.indent()?;
            self.format_join(join)?;
            writeln!(self.buffer)?;
        }

        // Where (Filter Cages)
        let filters: Vec<&Cage> = cmd.cages.iter().filter(|c| matches!(c.kind, CageKind::Filter)).collect();
        if !filters.is_empty() {
            // We need to merge them or print them?
            // Proposal says: "where rn = 1"
            self.indent()?;
            write!(self.buffer, "where ")?;
            for (i, cage) in filters.iter().enumerate() {
                if i > 0 {
                    write!(self.buffer, " and ")?; // Assuming AND between cages for now
                }
                self.format_conditions(&cage.conditions, cage.logical_op)?;
            }
            writeln!(self.buffer)?;
        }

        // Order By (Sort Cages)
        let sorts: Vec<&Cage> = cmd.cages.iter().filter(|c| matches!(c.kind, CageKind::Sort(_))).collect();
        if !sorts.is_empty() {
            self.indent()?;
            writeln!(self.buffer, "order by")?;
            self.indent_level += 1;
            for (i, cage) in sorts.iter().enumerate() {
                if let CageKind::Sort(order) = cage.kind {
                     for (j, cond) in cage.conditions.iter().enumerate() {
                        self.indent()?;
                        write!(self.buffer, "{}", cond.left)?;
                        self.format_sort_order(order)?;
                        if i < sorts.len() - 1 || j < cage.conditions.len() - 1 {
                             writeln!(self.buffer, ",")?;
                        } else {
                             writeln!(self.buffer)?;
                        }
                     }
                }
            }
            self.indent_level -= 1;
        }

        // Limit / Offset
        for cage in &cmd.cages {
             match cage.kind {
                 CageKind::Limit(n) => {
                     self.indent()?;
                     writeln!(self.buffer, "limit {}", n)?;
                 },
                 CageKind::Offset(n) => {
                     self.indent()?;
                     writeln!(self.buffer, "offset {}", n)?;
                 },
                 _ => {}
             }
        }
        
        
        // self.indent_level -= 1; // Removed matching decrement
        Ok(())
    }

    fn format_column(&mut self, col: &Expr) -> Result {
        match col {
            Expr::Star => write!(self.buffer, "*")?,
            Expr::Named(name) => write!(self.buffer, "{}", name)?,
            Expr::Aliased { name, alias } => write!(self.buffer, "{} as {}", name, alias)?,
            Expr::Aggregate { col, func } => {
                 let func_name = match func {
                     crate::ast::AggregateFunc::Count => "count",
                     crate::ast::AggregateFunc::Sum => "sum",
                     crate::ast::AggregateFunc::Avg => "avg",
                     crate::ast::AggregateFunc::Min => "min",
                     crate::ast::AggregateFunc::Max => "max",
                 };
                 write!(self.buffer, "{}({})", func_name, col)?
            },
            Expr::FunctionCall { name, args, alias } => {
                let args_str: Vec<String> = args.iter().map(|a| a.to_string()).collect();
                write!(self.buffer, "{}({})", name, args_str.join(", "))?;
                if let Some(a) = alias {
                    write!(self.buffer, " as {}", a)?;
                }
            }
            // TODO: Handle Window, Case, JsonAccess
            _ => write!(self.buffer, "/* TODO: {:?} */", col)?, 
        }
        Ok(())
    }

    fn format_join(&mut self, join: &Join) -> Result {
        match join.kind {
            crate::ast::JoinKind::Inner => write!(self.buffer, "join {}", join.table)?,
            crate::ast::JoinKind::Left => write!(self.buffer, "left join {}", join.table)?,
            crate::ast::JoinKind::Right => write!(self.buffer, "right join {}", join.table)?,
            crate::ast::JoinKind::Full => write!(self.buffer, "full join {}", join.table)?,
            crate::ast::JoinKind::Cross => write!(self.buffer, "cross join {}", join.table)?,
            crate::ast::JoinKind::Lateral => write!(self.buffer, "lateral join {}", join.table)?,
        }

        if let Some(conditions) = &join.on {
            if !conditions.is_empty() {
                write!(self.buffer, "\n")?;
                self.indent_level += 1;
                self.indent()?;
                write!(self.buffer, "on ")?;
                self.format_conditions(conditions, LogicalOp::And)?;
                self.indent_level -= 1;
            }
        }
        Ok(())
    }

    fn format_conditions(&mut self, conditions: &[Condition], logical_op: LogicalOp) -> Result {
        for (i, cond) in conditions.iter().enumerate() {
             if i > 0 {
                 match logical_op {
                     LogicalOp::And => write!(self.buffer, " and ")?,
                     LogicalOp::Or => write!(self.buffer, " or ")?,
                 }
             }
             
             write!(self.buffer, "{}", cond.left)?;
             
             match cond.op {
                 Operator::Eq => write!(self.buffer, " = ")?,
                 Operator::Ne => write!(self.buffer, " != ")?,
                 Operator::Gt => write!(self.buffer, " > ")?,
                 Operator::Gte => write!(self.buffer, " >= ")?,
                 Operator::Lt => write!(self.buffer, " < ")?,
                 Operator::Lte => write!(self.buffer, " <= ")?,
                 Operator::Fuzzy => write!(self.buffer, " ~ ")?, // ILIKE
                 Operator::In => write!(self.buffer, " in ")?,
                 Operator::NotIn => write!(self.buffer, " not in ")?,
                 Operator::IsNull => write!(self.buffer, " is null")?,
                 Operator::IsNotNull => write!(self.buffer, " is not null")?,
                 Operator::Contains => write!(self.buffer, " @> ")?,
                 Operator::KeyExists => write!(self.buffer, " ? ")?,
                 _ => write!(self.buffer, " {:?} ", cond.op)?,
             }

             // Some operators like IsNull don't need a value printed
             if !matches!(cond.op, Operator::IsNull | Operator::IsNotNull) {
                 self.format_value(&cond.value)?;
             }
        }
        Ok(())
    }
    
    fn format_value(&mut self, val: &Value) -> Result {
        match val {
            Value::Null => write!(self.buffer, "null")?,
            Value::Bool(b) => write!(self.buffer, "{}", b)?,
            Value::Int(n) => write!(self.buffer, "{}", n)?,
            Value::Float(n) => write!(self.buffer, "{}", n)?,
            Value::Param(n) => write!(self.buffer, "${}", n)?,
            Value::Function(f) => write!(self.buffer, "{}", f)?,
            Value::Column(c) => write!(self.buffer, "{}", c)?,
            Value::String(s) => write!(self.buffer, "'{}'", s)?, // Simple quoting, might need escaping
            // Value::Date and Value::Interval are not in AST, likely Strings
            // Value::Date(d) => write!(self.buffer, "'{}'", d)?,
            // Value::Interval(i) => write!(self.buffer, "interval '{}'", i)?,
            Value::Array(arr) => {
                write!(self.buffer, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 { write!(self.buffer, ", ")?; }
                    self.format_value(v)?;
                }
                write!(self.buffer, "]")?;
            }
            // TODO: Handle others
             _ => write!(self.buffer, "{:?}", val)?,
        }
        Ok(())
    }

    fn format_sort_order(&mut self, order: SortOrder) -> Result {
        match order {
            SortOrder::Asc => {},
            SortOrder::Desc => write!(self.buffer, " desc")?,
            SortOrder::AscNullsFirst => write!(self.buffer, " nulls first")?,
            SortOrder::AscNullsLast => write!(self.buffer, " nulls last")?,
            SortOrder::DescNullsFirst => write!(self.buffer, " desc nulls first")?,
            SortOrder::DescNullsLast => write!(self.buffer, " desc nulls last")?,
        }
        Ok(())
    }
}
