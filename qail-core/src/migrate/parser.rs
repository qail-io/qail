//! QAIL Schema Parser
//!
//! Parses .qail text format into Schema AST.
//!
//! ## Grammar
//! ```
//! schema = { table_def | index_def | migration_hint }*
//! 
//! table_def = "table" IDENT "{" column_def* "}"
//! column_def = IDENT TYPE constraint*
//! constraint = "primary_key" | "not_null" | "nullable" | "unique" | "default" VALUE
//!
//! index_def = ["unique"] "index" IDENT "on" IDENT "(" IDENT+ ")"
//!
//! migration_hint = "rename" PATH "->" PATH
//!                | "transform" EXPR "->" PATH
//!                | "drop" PATH ["confirm"]
//! ```

use super::schema::{Schema, Table, Column, Index, MigrationHint};

/// Parse a .qail file into a Schema.
pub fn parse_qail(input: &str) -> Result<Schema, String> {
    let mut schema = Schema::new();
    let mut lines = input.lines().peekable();
    
    while let Some(line) = lines.next() {
        let line = line.trim();
        
        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        // Parse table definition
        if line.starts_with("table ") {
            let (table, consumed) = parse_table(line, &mut lines)?;
            schema.add_table(table);
            // consumed lines already processed
            let _ = consumed;
        }
        // Parse index definition
        else if line.starts_with("unique index ") || line.starts_with("index ") {
            let index = parse_index(line)?;
            schema.add_index(index);
        }
        // Parse migration hints
        else if line.starts_with("rename ") {
            let hint = parse_rename(line)?;
            schema.add_hint(hint);
        }
        else if line.starts_with("transform ") {
            let hint = parse_transform(line)?;
            schema.add_hint(hint);
        }
        else if line.starts_with("drop ") {
            let hint = parse_drop(line)?;
            schema.add_hint(hint);
        }
        else {
            return Err(format!("Unknown statement: {}", line));
        }
    }
    
    Ok(schema)
}

/// Parse a table definition with columns.
fn parse_table<'a, I>(first_line: &str, lines: &mut std::iter::Peekable<I>) -> Result<(Table, usize), String>
where
    I: Iterator<Item = &'a str>,
{
    // Parse: table users {
    let rest = first_line.strip_prefix("table ").unwrap();
    let name = rest.trim_end_matches('{').trim().to_string();
    
    if name.is_empty() {
        return Err("Table name required".to_string());
    }
    
    let mut table = Table::new(&name);
    let mut consumed = 0;
    
    // Parse columns until }
    while let Some(line) = lines.next() {
        consumed += 1;
        let line = line.trim();
        
        if line == "}" || line.starts_with('}') {
            break;
        }
        
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        let col = parse_column(line)?;
        table.columns.push(col);
    }
    
    Ok((table, consumed))
}

/// Parse a column definition.
fn parse_column(line: &str) -> Result<Column, String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    
    if parts.len() < 2 {
        return Err(format!("Invalid column: {}", line));
    }
    
    let name = parts[0].to_string();
    let data_type = parts[1].to_string();
    
    let mut col = Column::new(&name, &data_type);
    
    // Parse constraints
    let mut i = 2;
    while i < parts.len() {
        match parts[i] {
            "primary_key" => {
                col.primary_key = true;
                col.nullable = false;
            }
            "not_null" => {
                col.nullable = false;
            }
            "nullable" => {
                col.nullable = true;
            }
            "unique" => {
                col.unique = true;
            }
            "default" => {
                if i + 1 < parts.len() {
                    col.default = Some(parts[i + 1].to_string());
                    i += 1;
                }
            }
            _ => {
                // Unknown constraint, might be part of default value
            }
        }
        i += 1;
    }
    
    Ok(col)
}

/// Parse an index definition.
fn parse_index(line: &str) -> Result<Index, String> {
    let is_unique = line.starts_with("unique ");
    let rest = if is_unique {
        line.strip_prefix("unique index ").unwrap()
    } else {
        line.strip_prefix("index ").unwrap()
    };
    
    // Parse: idx_name on table_name (col1, col2)
    let parts: Vec<&str> = rest.splitn(2, " on ").collect();
    if parts.len() != 2 {
        return Err(format!("Invalid index: {}", line));
    }
    
    let name = parts[0].trim().to_string();
    let rest = parts[1];
    
    // Parse table (columns)
    let paren_start = rest.find('(').ok_or("Missing ( in index")?;
    let paren_end = rest.find(')').ok_or("Missing ) in index")?;
    
    let table = rest[..paren_start].trim().to_string();
    let cols_str = &rest[paren_start + 1..paren_end];
    let columns: Vec<String> = cols_str.split(',').map(|s| s.trim().to_string()).collect();
    
    let mut index = Index::new(&name, &table, columns);
    if is_unique {
        index.unique = true;
    }
    
    Ok(index)
}

/// Parse a rename hint.
fn parse_rename(line: &str) -> Result<MigrationHint, String> {
    // rename users.username -> users.name
    let rest = line.strip_prefix("rename ").unwrap();
    let parts: Vec<&str> = rest.split(" -> ").collect();
    
    if parts.len() != 2 {
        return Err(format!("Invalid rename: {}", line));
    }
    
    Ok(MigrationHint::Rename {
        from: parts[0].trim().to_string(),
        to: parts[1].trim().to_string(),
    })
}

/// Parse a transform hint.
fn parse_transform(line: &str) -> Result<MigrationHint, String> {
    // transform age * 12 -> age_months
    let rest = line.strip_prefix("transform ").unwrap();
    let parts: Vec<&str> = rest.split(" -> ").collect();
    
    if parts.len() != 2 {
        return Err(format!("Invalid transform: {}", line));
    }
    
    Ok(MigrationHint::Transform {
        expression: parts[0].trim().to_string(),
        target: parts[1].trim().to_string(),
    })
}

/// Parse a drop hint.
fn parse_drop(line: &str) -> Result<MigrationHint, String> {
    // drop temp_table confirm
    let rest = line.strip_prefix("drop ").unwrap();
    let confirmed = rest.ends_with(" confirm");
    let target = if confirmed {
        rest.strip_suffix(" confirm").unwrap().trim().to_string()
    } else {
        rest.trim().to_string()
    };
    
    Ok(MigrationHint::Drop { target, confirmed })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_table() {
        let input = r#"
table users {
  id serial primary_key
  name text not_null
  email text nullable unique
}
"#;
        let schema = parse_qail(input).unwrap();
        assert!(schema.tables.contains_key("users"));
        let table = &schema.tables["users"];
        assert_eq!(table.columns.len(), 3);
        assert!(table.columns[0].primary_key);
        assert!(!table.columns[1].nullable);
        assert!(table.columns[2].unique);
    }

    #[test]
    fn test_parse_index() {
        let input = "unique index idx_users_email on users (email)";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.indexes.len(), 1);
        assert!(schema.indexes[0].unique);
        assert_eq!(schema.indexes[0].name, "idx_users_email");
    }

    #[test]
    fn test_parse_rename() {
        let input = "rename users.username -> users.name";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.migrations.len(), 1);
        assert!(matches!(&schema.migrations[0], MigrationHint::Rename { from, to } 
            if from == "users.username" && to == "users.name"));
    }

    #[test]
    fn test_parse_full_schema() {
        let input = r#"
# User table
table users {
  id serial primary_key
  name text not_null
  email text unique
  created_at timestamptz default now()
}

unique index idx_users_email on users (email)

rename users.username -> users.name
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.indexes.len(), 1);
        assert_eq!(schema.migrations.len(), 1);
    }
}
