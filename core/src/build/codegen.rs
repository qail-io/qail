//! Typed schema code generation.
//!
//! Generates Rust modules from `schema.qail` for compile-time type safety.

use std::fs;

use crate::migrate::types::ColumnType;

use super::schema::Schema;

fn qail_type_to_rust(col_type: &ColumnType) -> &'static str {
    match col_type {
        ColumnType::Uuid => "uuid::Uuid",
        ColumnType::Text | ColumnType::Varchar(_) => "String",
        ColumnType::Int | ColumnType::Serial => "i32",
        ColumnType::BigInt | ColumnType::BigSerial => "i64",
        ColumnType::Bool => "bool",
        ColumnType::Float => "f32",
        ColumnType::Decimal(_) => "rust_decimal::Decimal",
        ColumnType::Jsonb => "serde_json::Value",
        ColumnType::Timestamp | ColumnType::Timestamptz => "chrono::DateTime<chrono::Utc>",
        ColumnType::Date => "chrono::NaiveDate",
        ColumnType::Time => "chrono::NaiveTime",
        ColumnType::Bytea => "Vec<u8>",
        ColumnType::Array(_) => "Vec<serde_json::Value>",
        ColumnType::Enum { .. } => "String",
        ColumnType::Range(_) => "String",
        ColumnType::Interval => "String",
        ColumnType::Cidr | ColumnType::Inet => "String",
        ColumnType::MacAddr => "String",
    }
}

/// Convert table/column names to valid Rust identifiers
fn to_rust_ident(name: &str) -> String {
    // Handle Rust keywords
    let name = match name {
        "type" => "r#type",
        "match" => "r#match",
        "ref" => "r#ref",
        "self" => "r#self",
        "mod" => "r#mod",
        "use" => "r#use",
        _ => name,
    };
    name.to_string()
}

/// Convert table name to PascalCase struct name
fn to_struct_name(name: &str) -> String {
    name.chars()
        .next()
        .map(|c| c.to_uppercase().collect::<String>() + &name[1..])
        .unwrap_or_default()
}

/// Generate typed Rust module from schema.
///
/// # Usage in consumer's build.rs:
/// ```ignore
/// fn main() {
///     let out_dir = std::env::var("OUT_DIR").unwrap();
///     qail_core::build::generate_typed_schema("schema.qail", &format!("{}/schema.rs", out_dir)).unwrap();
///     println!("cargo:rerun-if-changed=schema.qail");
/// }
/// ```
///
/// Then in the consumer's lib.rs:
/// ```ignore
/// include!(concat!(env!("OUT_DIR"), "/schema.rs"));
/// ```
pub fn generate_typed_schema(schema_path: &str, output_path: &str) -> Result<(), String> {
    let schema = Schema::parse_file(schema_path)?;
    let code = generate_schema_code(&schema);

    fs::write(output_path, code)
        .map_err(|e| format!("Failed to write schema module to '{}': {}", output_path, e))?;

    Ok(())
}

/// Generate typed Rust code from schema (does not write to file)
pub fn generate_schema_code(schema: &Schema) -> String {
    let mut code = String::new();

    // Header
    code.push_str("//! Auto-generated typed schema from schema.qail\n");
    code.push_str("//! Do not edit manually - regenerate with `cargo build`\n\n");
    code.push_str("#![allow(dead_code, non_upper_case_globals)]\n\n");
    code.push_str("use qail_core::typed::{Table, TypedColumn, RelatedTo, Public, Protected};\n\n");

    // Sort tables for deterministic output
    let mut tables: Vec<_> = schema.tables.values().collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));

    for table in &tables {
        let mod_name = to_rust_ident(&table.name);
        let struct_name = to_struct_name(&table.name);

        code.push_str(&format!("/// Typed schema for `{}` table\n", table.name));
        code.push_str(&format!("pub mod {} {{\n", mod_name));
        code.push_str("    use super::*;\n\n");

        // Table struct implementing Table trait
        code.push_str(&format!("    /// Table marker for `{}`\n", table.name));
        code.push_str("    #[derive(Debug, Clone, Copy)]\n");
        code.push_str(&format!("    pub struct {};\n\n", struct_name));

        code.push_str(&format!("    impl Table for {} {{\n", struct_name));
        code.push_str(&format!(
            "        fn table_name() -> &'static str {{ \"{}\" }}\n",
            table.name
        ));
        code.push_str("    }\n\n");

        code.push_str(&format!("    impl From<{}> for String {{\n", struct_name));
        code.push_str(&format!(
            "        fn from(_: {}) -> String {{ \"{}\".to_string() }}\n",
            struct_name, table.name
        ));
        code.push_str("    }\n\n");

        code.push_str(&format!("    impl AsRef<str> for {} {{\n", struct_name));
        code.push_str(&format!(
            "        fn as_ref(&self) -> &str {{ \"{}\" }}\n",
            table.name
        ));
        code.push_str("    }\n\n");

        // Table constant for convenience
        code.push_str(&format!("    /// The `{}` table\n", table.name));
        code.push_str(&format!(
            "    pub const table: {} = {};\n\n",
            struct_name, struct_name
        ));

        // Sort columns for deterministic output
        let mut columns: Vec<_> = table.columns.iter().collect();
        columns.sort_by(|a, b| a.0.cmp(b.0));

        // Column constants
        for (col_name, col_type) in columns {
            let rust_type = qail_type_to_rust(col_type);
            let col_ident = to_rust_ident(col_name);
            let policy = table
                .policies
                .get(col_name)
                .map(|s| s.as_str())
                .unwrap_or("Public");
            let rust_policy = if policy == "Protected" {
                "Protected"
            } else {
                "Public"
            };

            code.push_str(&format!(
                "    /// Column `{}.{}` ({}) - {}\n",
                table.name,
                col_name,
                col_type.to_pg_type(),
                policy
            ));
            code.push_str(&format!(
                "    pub const {}: TypedColumn<{}, {}> = TypedColumn::new(\"{}\", \"{}\");\n",
                col_ident, rust_type, rust_policy, table.name, col_name
            ));
        }

        code.push_str("}\n\n");
    }

    // ==========================================================================
    // Generate RelatedTo impls for compile-time relationship checking
    // ==========================================================================

    code.push_str(
        "// =============================================================================\n",
    );
    code.push_str("// Compile-Time Relationship Safety (RelatedTo impls)\n");
    code.push_str(
        "// =============================================================================\n\n",
    );

    for table in &tables {
        for fk in &table.foreign_keys {
            // table.column refs ref_table.ref_column
            // This means: table is related TO ref_table (forward)
            // AND: ref_table is related FROM table (reverse - parent has many children)

            let from_mod = to_rust_ident(&table.name);
            let from_struct = to_struct_name(&table.name);
            let to_mod = to_rust_ident(&fk.ref_table);
            let to_struct = to_struct_name(&fk.ref_table);

            // Forward: From table (child) -> Referenced table (parent)
            // Example: posts -> users (posts.user_id -> users.id)
            code.push_str(&format!(
                "/// {} has a foreign key to {} via {}.{}\n",
                table.name, fk.ref_table, table.name, fk.column
            ));
            code.push_str(&format!(
                "impl RelatedTo<{}::{}> for {}::{} {{\n",
                to_mod, to_struct, from_mod, from_struct
            ));
            code.push_str(&format!(
                "    fn join_columns() -> (&'static str, &'static str) {{ (\"{}\", \"{}\") }}\n",
                fk.column, fk.ref_column
            ));
            code.push_str("}\n\n");

            // Reverse: Referenced table (parent) -> From table (child)
            // Example: users -> posts (users.id -> posts.user_id)
            // This allows: Qail::get(users::table).join_related(posts::table)
            code.push_str(&format!(
                "/// {} is referenced by {} via {}.{}\n",
                fk.ref_table, table.name, table.name, fk.column
            ));
            code.push_str(&format!(
                "impl RelatedTo<{}::{}> for {}::{} {{\n",
                from_mod, from_struct, to_mod, to_struct
            ));
            code.push_str(&format!(
                "    fn join_columns() -> (&'static str, &'static str) {{ (\"{}\", \"{}\") }}\n",
                fk.ref_column, fk.column
            ));
            code.push_str("}\n\n");
        }
    }

    code
}

#[cfg(test)]
mod codegen_tests {
    use super::*;

    #[test]
    fn test_generate_schema_code() {
        let schema_content = r#"
table users {
    id UUID primary_key
    email TEXT not_null
    age INT
}

table posts {
    id UUID primary_key
    user_id UUID ref:users.id
    title TEXT
}
"#;

        let schema = Schema::parse(schema_content).unwrap();
        let code = generate_schema_code(&schema);

        // Verify module structure
        assert!(code.contains("pub mod users {"));
        assert!(code.contains("pub mod posts {"));

        // Verify table structs
        assert!(code.contains("pub struct Users;"));
        assert!(code.contains("pub struct Posts;"));

        // Verify columns
        assert!(code.contains("pub const id: TypedColumn<uuid::Uuid, Public>"));
        assert!(code.contains("pub const email: TypedColumn<String, Public>"));
        assert!(code.contains("pub const age: TypedColumn<i32, Public>"));

        // Verify RelatedTo impls for compile-time relationship checking
        assert!(code.contains("impl RelatedTo<users::Users> for posts::Posts"));
        assert!(code.contains("impl RelatedTo<posts::Posts> for users::Users"));
    }

    #[test]
    fn test_generate_protected_column() {
        let schema_content = r#"
table secrets {
    id UUID primary_key
    token TEXT protected
}
"#;
        let schema = Schema::parse(schema_content).unwrap();
        let code = generate_schema_code(&schema);

        // Verify Protected policy
        assert!(code.contains("pub const token: TypedColumn<String, Protected>"));
    }
}

#[cfg(test)]
mod migration_parser_tests {
    use super::*;

    #[test]
    fn test_agent_contracts_migration_parses_all_columns() {
        let sql = r#"
CREATE TABLE agent_contracts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    operator_id UUID NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    pricing_model VARCHAR(20) NOT NULL CHECK (pricing_model IN ('commission', 'static_markup', 'net_rate')),
    commission_percent DECIMAL(5,2),
    static_markup DECIMAL(10,2),
    is_active BOOLEAN DEFAULT true,
    valid_from DATE,
    valid_until DATE,
    approved_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    UNIQUE(agent_id, operator_id)
);
"#;

        let mut schema = Schema::default();
        schema.parse_sql_migration(sql);

        let table = schema
            .tables
            .get("agent_contracts")
            .expect("agent_contracts table should exist");

        for col in &[
            "id",
            "agent_id",
            "operator_id",
            "pricing_model",
            "commission_percent",
            "static_markup",
            "is_active",
            "valid_from",
            "valid_until",
            "approved_by",
            "created_at",
            "updated_at",
        ] {
            assert!(
                table.columns.contains_key(*col),
                "Missing column: '{}'. Found: {:?}",
                col,
                table.columns.keys().collect::<Vec<_>>()
            );
        }
    }

    /// Regression test: column names that START with SQL keywords must parse correctly.
    /// e.g., created_at starts with CREATE, primary_contact starts with PRIMARY, etc.
    #[test]
    fn test_keyword_prefixed_column_names_are_not_skipped() {
        let sql = r#"
CREATE TABLE edge_cases (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    created_by UUID,
    primary_contact VARCHAR(255),
    check_status VARCHAR(20),
    unique_code VARCHAR(50),
    foreign_ref UUID,
    constraint_name VARCHAR(100),
    PRIMARY KEY (id),
    CHECK (check_status IN ('pending', 'active')),
    UNIQUE (unique_code),
    CONSTRAINT fk_ref FOREIGN KEY (foreign_ref) REFERENCES other(id)
);
"#;

        let mut schema = Schema::default();
        schema.parse_sql_migration(sql);

        let table = schema
            .tables
            .get("edge_cases")
            .expect("edge_cases table should exist");

        // These column names start with SQL keywords — all must be found
        for col in &[
            "created_at",
            "created_by",
            "primary_contact",
            "check_status",
            "unique_code",
            "foreign_ref",
            "constraint_name",
        ] {
            assert!(
                table.columns.contains_key(*col),
                "Column '{}' should NOT be skipped just because it starts with a SQL keyword. Found: {:?}",
                col,
                table.columns.keys().collect::<Vec<_>>()
            );
        }

        // These are constraint keywords, not columns — must NOT appear
        // (PRIMARY KEY, CHECK, UNIQUE, CONSTRAINT lines should be skipped)
        assert!(
            !table.columns.contains_key("primary"),
            "Constraint keyword 'PRIMARY' should not be treated as a column"
        );
    }
}
