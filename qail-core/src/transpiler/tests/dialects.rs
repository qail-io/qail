//! SQL Dialect tests.

use crate::parser::parse;
use crate::transpiler::{Dialect, ToSql};

#[test]
fn test_sqlite_dialect() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::SQLite),
        "SELECT * FROM \"users\" WHERE \"active\" = 1"
    );

    let cmd_fuzzy = parse("get users fields * where name ~ $1").unwrap();
    assert_eq!(
        cmd_fuzzy.to_sql_with_dialect(Dialect::SQLite),
        "SELECT * FROM \"users\" WHERE \"name\" LIKE '%' || ? || '%'"
    );
}
