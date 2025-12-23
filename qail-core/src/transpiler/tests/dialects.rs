//! SQL Dialect tests (MySQL, SQLite, Oracle, SQL Server, etc.)

use crate::parser::parse;
use crate::transpiler::{ToSql, Dialect};

#[test]
fn test_mysql_dialect() {
    let cmd = parse("get users fields * where name ~ $1 and active = true").unwrap();
    let sql = cmd.to_sql_with_dialect(Dialect::MySQL);
    assert_eq!(
        sql, 
        "SELECT * FROM `users` WHERE `name` LIKE CONCAT('%', ?, '%') AND `active` = 1"
    );
}

#[test]
fn test_sqlite_dialect() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(cmd.to_sql_with_dialect(Dialect::SQLite), "SELECT * FROM \"users\" WHERE \"active\" = 1");

    let cmd_fuzzy = parse("get users fields * where name ~ $1").unwrap();
    assert_eq!(cmd_fuzzy.to_sql_with_dialect(Dialect::SQLite), "SELECT * FROM \"users\" WHERE \"name\" LIKE '%' || ? || '%'");
}

#[test]
fn test_sqlserver_dialect() {
    let cmd = parse("get users fields * limit 10 offset 5").unwrap();
    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::SqlServer), 
        "SELECT * FROM [users] OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"
    );
    
    let cmd_quotes = parse("get users fields id").unwrap();
    assert_eq!(
        cmd_quotes.to_sql_with_dialect(Dialect::SqlServer),
        "SELECT [id] FROM [users]"
    );

    let cmd_params = parse("get users fields * where id = $1").unwrap();
    assert_eq!(
        cmd_params.to_sql_with_dialect(Dialect::SqlServer),
        "SELECT * FROM [users] WHERE [id] = @p1"
    );
}

#[test]
fn test_oracle_dialect() {
    let cmd = parse("get users fields * limit 10 offset 5").unwrap();
    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::Oracle), 
        "SELECT * FROM \"users\" OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"
    );
    
    let cmd_params = parse("get users fields * where id = $1").unwrap();
    assert_eq!(
        cmd_params.to_sql_with_dialect(Dialect::Oracle),
        "SELECT * FROM \"users\" WHERE \"id\" = :1"
    );

    let cmd_fuzzy = parse("get users fields * where name ~ $1").unwrap();
    assert_eq!(
        cmd_fuzzy.to_sql_with_dialect(Dialect::Oracle),
        "SELECT * FROM \"users\" WHERE \"name\" LIKE '%' || :1 || '%'"
    );
}

#[test]
fn test_duckdb_dialect() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(cmd.to_sql_with_dialect(Dialect::DuckDB), "SELECT * FROM \"users\" WHERE \"active\" = true");

    let cmd_fuzzy = parse("get users fields * where name ~ $1").unwrap();
    assert_eq!(cmd_fuzzy.to_sql_with_dialect(Dialect::DuckDB), "SELECT * FROM \"users\" WHERE \"name\" ILIKE '%' || $1 || '%'");
}

#[test]
fn test_mariadb_dialect() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(cmd.to_sql_with_dialect(Dialect::MariaDB), "SELECT * FROM `users` WHERE `active` = 1");
}

#[test]
fn test_snowflake_dialect() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(cmd.to_sql_with_dialect(Dialect::Snowflake), "SELECT * FROM \"users\" WHERE \"active\" = true");
}

#[test]
fn test_redshift_dialect() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(cmd.to_sql_with_dialect(Dialect::Redshift), "SELECT * FROM \"users\" WHERE \"active\" = true");
}

#[test]
fn test_bigquery_dialect() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(cmd.to_sql_with_dialect(Dialect::BigQuery), "SELECT * FROM `users` WHERE `active` = true");
}

#[test]
fn test_influx_ql() {
    let cmd = parse("get cpu fields * where host = 'server1'").unwrap();
    let sql = cmd.to_sql_with_dialect(Dialect::InfluxDB);
    // InfluxQL uses single quotes for strings, double for identifiers
    assert!(sql.contains("cpu"));
    assert!(sql.contains("host"));
}
