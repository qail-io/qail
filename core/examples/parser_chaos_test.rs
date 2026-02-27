//! Core Parser Chaos Test
//!
//! Throws every conceivable edge case at the QAIL parser:
//! - Unicode/emoji in identifiers and values
//! - SQL injection attempts
//! - Deeply nested filters
//! - Boundary values (limits, offsets)
//! - Malformed/incomplete queries
//! - Every action variant
//! - Stress-level nesting

use qail_core::parser::parse;

#[allow(unused_variables, unused_assignments)]
fn main() {
    let mut pass = 0u32;
    let mut fail = 0u32;
    let mut panics = 0u32;

    println!("═══════════════════════════════════════════════════");
    println!("  🧪 CORE PARSER CHAOS TEST");
    println!("═══════════════════════════════════════════════════");

    // ═══════════════════════════════════════════════════
    // 1. Valid Queries — must parse without panic
    // ═══════════════════════════════════════════════════
    println!("\n── 1. Valid Queries (must parse Ok) ──");
    let valid_queries = vec![
        // Basic CRUD
        ("get users", "basic get"),
        ("get users fields id,name", "get with fields"),
        ("get users fields id,name limit 10", "get with limit"),
        (
            "get users fields id,name limit 10 offset 20",
            "get with limit+offset",
        ),
        ("count users", "count"),
        ("cnt users", "cnt alias"),
        ("add users", "add"),
        ("set users", "set"),
        ("del users", "del"),
        // Filters
        ("get users[name = 'Alice']", "filter eq string"),
        ("get users[age > 18]", "filter gt number"),
        ("get users[age >= 18]", "filter gte"),
        ("get users[age < 100]", "filter lt"),
        ("get users[active = true]", "filter bool"),
        ("get users[email = null]", "filter null"),
        ("get users[name != 'Bob']", "filter neq"),
        ("get users[name = 'Alice'][age > 18]", "chained filters"),
        // Sorting
        ("get users sort name:asc", "sort asc"),
        ("get users sort name:desc", "sort desc"),
        ("get users sort name:asc,age:desc", "multi sort"),
        // Aggregates
        ("get users fields count(id)", "aggregate count"),
        ("get users fields sum(age)", "aggregate sum"),
        ("get users fields avg(age)", "aggregate avg"),
        ("get users fields min(age),max(age)", "aggregate min/max"),
        // Joins
        (
            "get users join orders on users.id = orders.user_id",
            "inner join",
        ),
        (
            "get users left join orders on users.id = orders.user_id",
            "left join",
        ),
        // Parameters
        ("get users[id = $1]", "positional param"),
        ("get users[id = :user_id]", "named param"),
        // JSON values
        ("add users values {\"name\": \"test\"}", "json value"),
        // Interval
        ("get events[created_at > 24h]", "interval hours"),
        ("get events[created_at > 7d]", "interval days"),
        // DDL
        ("make users columns id:uuid,name:text", "make table"),
        ("drop users", "drop table"),
        // Transaction
        ("begin", "begin txn"),
        ("commit", "commit txn"),
        ("rollback", "rollback txn"),
        // Count with filter
        ("count users[active = true]", "count with filter"),
        // Limit edge cases
        ("get users limit 1", "limit 1"),
        ("get users limit 0", "limit 0"),
        ("get users limit 999999999", "huge limit"),
        ("get users offset 0", "offset 0"),
        // Case insensitivity
        ("GET users", "uppercase GET"),
        ("Get users", "mixed case Get"),
        ("COUNT users", "uppercase COUNT"),
    ];

    for (query, label) in &valid_queries {
        match std::panic::catch_unwind(|| parse(query)) {
            Ok(Ok(_)) => {
                pass += 1;
            }
            Ok(Err(e)) => {
                println!("  ❌ {} — parse error: {}", label, e);
                fail += 1;
            }
            Err(_) => {
                println!("  💀 {} — PANIC!", label);
                panics += 1;
            }
        }
    }
    println!(
        "  ✅ {}/{} valid queries parsed OK",
        pass,
        valid_queries.len()
    );

    // ═══════════════════════════════════════════════════
    // 2. Invalid Queries — must not panic (parse error is OK)
    // ═══════════════════════════════════════════════════
    println!("\n── 2. Invalid Queries (must not panic) ──");
    let invalid_start = pass + fail + panics;
    let invalid_queries = vec![
        // Empty / whitespace
        ("", "empty string"),
        ("   ", "whitespace only"),
        ("\n\t\r", "whitespace chars"),
        ("\0", "null byte"),
        ("\0\0\0", "triple null bytes"),
        // No action
        ("users", "missing action"),
        ("users fields id", "missing action with fields"),
        // SQL injection
        ("get users; DROP TABLE users;--", "SQL injection semicolon"),
        ("get users' OR 1=1 --", "SQL injection single quote"),
        ("get users\" OR 1=1 --", "SQL injection double quote"),
        ("get users UNION SELECT * FROM passwords", "union injection"),
        ("get users; DELETE FROM users WHERE 1=1", "delete injection"),
        (
            "get users[id = '1; DROP TABLE users']",
            "injection in filter value",
        ),
        // Malformed filters
        ("get users[", "unclosed bracket"),
        ("get users]", "orphan close bracket"),
        ("get users[=]", "filter missing field"),
        ("get users[name =]", "filter missing value"),
        ("get users[= 'val']", "filter missing field 2"),
        ("get users[]", "empty filter"),
        ("get users[name]", "filter missing operator"),
        // Deeply nested nonsense
        ("get users[[name = 'a']]", "double brackets"),
        ("get users[name = 'a']]", "extra close bracket"),
        // Invalid actions
        ("SELECT users", "SQL SELECT keyword"),
        ("INSERT INTO users", "SQL INSERT keyword"),
        ("UPDATE users SET name = 'x'", "SQL UPDATE keyword"),
        ("DELETE FROM users", "SQL DELETE keyword"),
        ("DROP TABLE users CASCADE", "SQL DROP TABLE"),
        ("ALTER TABLE users", "SQL ALTER TABLE"),
        ("GRANT ALL ON users", "SQL GRANT"),
        ("REVOKE ALL ON users", "SQL REVOKE"),
        ("CREATE FUNCTION evil()", "SQL CREATE FUNCTION"),
        // Extreme lengths
    ];
    let long_ident = "a".repeat(10000);
    let long_table = format!("get {}", "a".repeat(10000));
    let many_cols = format!(
        "get users fields {}",
        (0..500)
            .map(|i| format!("col{}", i))
            .collect::<Vec<_>>()
            .join(",")
    );
    let many_filters = format!("get users{}", "[name = 'x']".repeat(100));
    let invalid_extras: Vec<(&str, &str)> = vec![
        (&long_ident, "10K character identifier"),
        (&long_table, "10K table name"),
        (&many_cols, "500 columns"),
        (&many_filters, "100 chained filters"),
        // Unicode chaos
        ("get 用户", "Chinese table name"),
        ("get مستخدمين", "Arabic table name"),
        ("get 🏴‍☠️", "emoji table name"),
        ("get users[name = '🚀🎯💀']", "emoji filter value"),
        ("get users[name = '中文测试']", "Chinese filter value"),
        ("get users[name = 'مرحبا']", "Arabic filter value"),
        ("get users[name = '']", "empty string value"),
        ("get users[name = 'A\0B']", "null byte in value"),
        // Unicode normalization attacks
        (
            "get users[name = '\u{0041}\u{0300}']",
            "combining char (À via A+grave)",
        ),
        ("get users[name = '\u{FEFF}admin']", "BOM prefix"),
        (
            "get users[name = 'admin\u{200B}']",
            "zero-width space suffix",
        ),
        ("get users[name = '\u{202E}nimda']", "RTL override"),
        // Numerical edge cases
        ("get users limit -1", "negative limit"),
        ("get users limit 0.5", "float limit"),
        (
            "get users limit 9999999999999999999999",
            "i64 overflow limit",
        ),
        ("get users offset -100", "negative offset"),
        ("get users limit NaN", "NaN limit"),
        ("get users limit Infinity", "Infinity limit"),
        // Repeated keywords
        ("get get users", "double action"),
        ("get users limit 10 limit 20", "double limit"),
        ("get users fields id fields name", "double fields"),
        ("get users sort name:asc sort age:desc", "double sort"),
        // Incomplete
        ("get", "action only no table"),
        ("get ", "action space no table"),
        ("count", "count only"),
        ("add", "add only"),
        ("set", "set only"),
        ("del", "del only"),
        // Tab/newline in query
        ("get\tusers", "tab between action and table"),
        ("get\nusers", "newline between action and table"),
        ("get users\nfields id", "newline before fields"),
        // Mixed valid/invalid
        ("get users limit abc", "non-numeric limit"),
        ("get users offset xyz", "non-numeric offset"),
        ("get users sort :invalid", "invalid sort syntax"),
    ];

    let mut invalid_pass = 0u32;
    let mut invalid_panic = 0u32;
    for (query, label) in invalid_queries.iter().chain(invalid_extras.iter()) {
        match std::panic::catch_unwind(|| parse(query)) {
            Ok(_) => {
                // Parse succeeded or returned error — both fine
                invalid_pass += 1;
                pass += 1;
            }
            Err(_) => {
                println!("  💀 {} — PANIC!", label);
                invalid_panic += 1;
                panics += 1;
            }
        }
    }
    println!(
        "  ✅ {}/{} invalid queries handled without panic",
        invalid_pass,
        invalid_queries.len()
    );
    if invalid_panic > 0 {
        println!("  💀 {} PANICS!", invalid_panic);
    }

    // ═══════════════════════════════════════════════════
    // 3. Roundtrip: parse → Display → parse
    // ═══════════════════════════════════════════════════
    println!("\n── 3. Parse-Display Roundtrip ──");
    let roundtrip_queries = vec![
        "get users",
        "get users fields id,name",
        "get users fields id,name limit 10",
        "count users",
        "get users[name = 'Alice']",
        "get users[age > 18]",
        "del users[id = $1]",
        "get users sort name:asc",
    ];

    let mut rt_pass = 0u32;
    let mut rt_fail = 0u32;
    for query in &roundtrip_queries {
        match parse(query) {
            Ok(ast) => {
                let displayed = ast.to_string();
                match parse(&displayed) {
                    Ok(ast2) => {
                        // Compare the ASTs
                        let d1 = format!("{:?}", ast);
                        let d2 = format!("{:?}", ast2);
                        if d1 == d2 {
                            rt_pass += 1;
                            pass += 1;
                        } else {
                            println!("  ❌ Roundtrip mismatch: {:?}", query);
                            println!(
                                "     Original:   {}",
                                d1.chars().take(100).collect::<String>()
                            );
                            println!(
                                "     Roundtrip:  {}",
                                d2.chars().take(100).collect::<String>()
                            );
                            rt_fail += 1;
                            fail += 1;
                        }
                    }
                    Err(e) => {
                        println!(
                            "  ❌ Roundtrip re-parse failed: {} → {} → {:?}",
                            query, displayed, e
                        );
                        rt_fail += 1;
                        fail += 1;
                    }
                }
            }
            Err(_) => {
                // Some queries might not parse, that's OK
            }
        }
    }
    println!("  ✅ {}/{} roundtrips OK", rt_pass, roundtrip_queries.len());

    // ═══════════════════════════════════════════════════
    // 4. Fuzz-like random strings
    // ═══════════════════════════════════════════════════
    println!("\n── 4. Random String Fuzzing (1000 inputs) ──");
    let mut fuzz_panic = 0u32;
    let fuzz_inputs: Vec<String> = (0..1000)
        .map(|i| match i % 20 {
            0 => format!("get t{}", i),
            1 => format!("get users[f{} = {}]", i, i),
            2 => format!("get users limit {}", i * i * i),
            3 => format!("count t{}", i),
            4 => format!("del t{}[id = {}]", i, i),
            5 => format!("set t{}", i),
            6 => format!("add t{}", i),
            7 => (0..i % 50).map(|_| 'A').collect::<String>(),
            8 => format!(
                "get {}",
                (0..i % 100 + 1)
                    .map(|j| format!("[f{} = {}]", j, j))
                    .collect::<String>()
            ),
            9 => format!(
                "get users fields {}",
                (0..i % 50 + 1)
                    .map(|j| format!("c{}", j))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            10 => format!(
                "get users sort {}",
                (0..i % 10 + 1)
                    .map(|j| format!("c{}:asc", j))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            11 => String::from_utf8(vec![i as u8; (i % 100 + 1) as usize]).unwrap_or_default(),
            12 => format!("get users[name = '{}']", "x".repeat((i % 500 + 1) as usize)),
            13 => "get users[a > 1][b < 2][c = 3][d != 4][e >= 5]".to_string(),
            14 => format!("make t{} columns id:uuid,name:text,age:int,active:bool", i),
            15 => format!("get users join t{} on users.id = t{}.user_id", i, i),
            16 => format!(
                "get users left join t{} on users.id = t{}.uid limit 5",
                i, i
            ),
            17 => "\x00\x01\x02\x03\x04\x05".to_string(),
            18 => format!("get users[id = ${}]", i),
            _ => format!("get users[name = :param{}]", i),
        })
        .collect();

    for input in &fuzz_inputs {
        match std::panic::catch_unwind(|| parse(input)) {
            Ok(_) => { /* ok */ }
            Err(_) => {
                fuzz_panic += 1;
                panics += 1;
                println!(
                    "  💀 PANIC on input: {:?}",
                    &input[..std::cmp::min(input.len(), 80)]
                );
            }
        }
    }
    println!(
        "  ✅ {}/1000 fuzz inputs handled without panic",
        1000 - fuzz_panic
    );

    // ═══════════════════════════════════════════════════
    // 5. All Action Keywords
    // ═══════════════════════════════════════════════════
    println!("\n── 5. All Action Keywords ──");
    let action_keywords = vec![
        "get",
        "count",
        "cnt",
        "set",
        "del",
        "add",
        "make",
        "drop",
        "alter",
        "index",
        "export",
        "explain",
        "lock",
        "begin",
        "commit",
        "rollback",
        "savepoint",
        "listen",
        "notify",
        "unlisten",
        "truncate",
        "upsert",
    ];
    let mut action_pass = 0u32;
    for kw in &action_keywords {
        let query = format!("{} test_table", kw);
        match std::panic::catch_unwind(|| parse(&query)) {
            Ok(_) => {
                action_pass += 1;
                pass += 1;
            }
            Err(_) => {
                println!("  💀 PANIC on action '{}'", kw);
                panics += 1;
            }
        }
    }
    println!(
        "  ✅ {}/{} action keywords handled",
        action_pass,
        action_keywords.len()
    );

    // ═══════════════════════════════════════════════════
    // SUMMARY
    // ═══════════════════════════════════════════════════
    let total = pass + fail + panics;
    println!("\n═══════════════════════════════════════════════════");
    println!("  Parser Chaos Results: {} total tests", total);
    println!("  ✅ {} passed", pass);
    println!("  ❌ {} failed (parse errors on valid input)", fail);
    println!("  💀 {} PANICS", panics);
    println!("═══════════════════════════════════════════════════");

    if panics > 0 {
        std::process::exit(1);
    }
}
