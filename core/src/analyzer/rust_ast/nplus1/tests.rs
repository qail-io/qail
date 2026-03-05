//! N+1 detection tests.

use super::*;

    #[test]
    fn n1001_query_in_single_loop() {
        let source = r#"
fn process(items: Vec<Item>, conn: &mut Conn) {
    for item in &items {
        let rows = conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty(), "Expected N1-001 diagnostic");
        assert_eq!(diags[0].code, NPlusOneCode::N1001);
        assert_eq!(diags[0].severity, NPlusOneSeverity::Warning);
    }

    #[test]
    fn n1004_query_in_nested_loop() {
        let source = r#"
fn process(groups: Vec<Vec<Item>>, conn: &mut Conn) {
    for group in &groups {
        for item in group {
            conn.execute(&cmd).await.unwrap();
        }
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty(), "Expected N1-004 diagnostic");
        assert_eq!(diags[0].code, NPlusOneCode::N1004);
        assert_eq!(diags[0].severity, NPlusOneSeverity::Error);
    }

    #[test]
    fn n1002_inline_chain_catches_loop_var() {
        // Loop var directly in the execution expression chain → N1-002
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        conn.fetch_all(&Qail::get("users").eq("id", id)).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(
            n1002.is_some(),
            "Inline chain with loop var should trigger N1-002, got: {:?}",
            diags.iter().map(|d| d.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn n1002_two_statement_binding_is_captured() {
        // Loop var in separate `let` binding should still be N1-002 because
        // the executed binding is derived from loop variable data.
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let cmd = Qail::get("users").eq("id", id);
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty());
        assert_eq!(
            diags[0].code,
            NPlusOneCode::N1002,
            "Two-statement loop-derived binding should be N1-002"
        );
    }

    #[test]
    fn n1002_transitive_binding_chain_is_captured() {
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let uid = *id;
        let cmd = Qail::get("users").eq("id", uid);
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(
            n1002.is_some(),
            "Expected N1-002 for transitive loop-derived binding chain"
        );
    }

    #[test]
    fn unrelated_loop_var_does_not_upgrade() {
        // Loop var used for logging only, query is static → N1-001, not N1-002
        let source = r#"
fn process(items: Vec<Item>, conn: &mut Conn) {
    for item in &items {
        println!("{}", item.name);
        conn.fetch_all(&static_query).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty());
        assert_eq!(
            diags[0].code,
            NPlusOneCode::N1001,
            "Unrelated loop var should NOT upgrade to N1-002"
        );
    }

    #[test]
    fn n1003_function_with_query_called_in_loop() {
        let source = r#"
async fn load_user(conn: &mut Conn, id: Uuid) -> User {
    let cmd = Qail::get("users").eq("id", id);
    conn.fetch_one(&cmd).await.unwrap()
}

async fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let user = load_user(conn, *id).await;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_some(),
            "Expected N1-003 for indirect query call, got: {:?}",
            diags
        );
    }

    #[test]
    fn suppression_disables_diagnostic() {
        let source = r#"
fn process(items: Vec<Item>, conn: &mut Conn) {
    for item in &items {
        // qail-lint:disable-next-line N1-001
        let rows = conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1001 = diags
            .iter()
            .filter(|d| d.code == NPlusOneCode::N1001)
            .count();
        assert_eq!(n1001, 0, "Suppressed N1-001 should not appear");
    }

    #[test]
    fn no_diagnostic_outside_loop() {
        let source = r#"
fn process(conn: &mut Conn) {
    let rows = conn.fetch_all(&cmd).await.unwrap();
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "No diagnostic expected outside loop, got: {:?}",
            diags
        );
    }

    #[test]
    fn while_loop_detected() {
        let source = r#"
fn process(conn: &mut Conn) {
    while has_more() {
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty(), "Expected diagnostic in while loop");
        assert_eq!(diags[0].code, NPlusOneCode::N1001);
    }

    #[test]
    fn while_let_tracks_loop_var_for_n1002() {
        let source = r#"
fn process(mut it: Iter<Item>, conn: &mut Conn) {
    while let Some(item) = it.next() {
        conn.fetch_all(&Qail::get("users").eq("id", item.id)).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(
            n1002.is_some(),
            "Expected N1-002 for while-let loop var usage"
        );
    }

    #[test]
    fn loop_keyword_detected() {
        let source = r#"
fn process(conn: &mut Conn) {
    loop {
        conn.execute(&cmd).await.unwrap();
        break;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty(), "Expected diagnostic in loop keyword");
        assert_eq!(diags[0].code, NPlusOneCode::N1001);
    }

    #[test]
    fn iterator_for_each_detected_as_loop() {
        let source = r#"
fn process(items: Vec<Item>, conn: &mut Conn) {
    items.iter().for_each(|item| {
        let _ = conn.fetch_all(&static_query);
    });
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            !diags.is_empty(),
            "Expected diagnostic in iterator for_each"
        );
        assert_eq!(diags[0].code, NPlusOneCode::N1001);
    }

    #[test]
    fn iterator_for_each_loop_var_upgrades_to_n1002() {
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    ids.iter().for_each(|id| {
        let _ = conn.fetch_all(&Qail::get("users").eq("id", id));
    });
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(n1002.is_some(), "Expected N1-002 in iterator for_each");
    }

    #[test]
    fn nested_for_and_for_each_becomes_n1004() {
        let source = r#"
fn process(groups: Vec<Vec<Item>>, conn: &mut Conn) {
    for group in groups {
        group.iter().for_each(|item| {
            let _ = conn.execute(&cmd);
        });
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1004 = diags.iter().find(|d| d.code == NPlusOneCode::N1004);
        assert!(n1004.is_some(), "Expected N1-004 for nested loop pattern");
    }

    #[test]
    fn n1003_self_method_call_in_loop() {
        let source = r#"
impl Repo {
    async fn load_user(&self, conn: &mut Conn, id: Uuid) -> User {
        conn.fetch_one(&cmd).await.unwrap()
    }

    async fn process(&self, conn: &mut Conn, ids: Vec<Uuid>) {
        for id in ids {
            let _u = self.load_user(conn, id).await;
        }
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_some(),
            "Expected N1-003 for self method call in loop"
        );
    }

    #[test]
    fn n1003_qualified_function_call_in_loop() {
        let source = r#"
mod helpers {
    pub async fn load_user(conn: &mut Conn, id: Uuid) -> User {
        conn.fetch_one(&cmd).await.unwrap()
    }
}

async fn process(conn: &mut Conn, ids: Vec<Uuid>) {
    for id in ids {
        let _u = helpers::load_user(conn, id).await;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_some(),
            "Expected N1-003 for qualified function call in loop"
        );
    }

    #[test]
    fn inline_suppression_works() {
        let source = r#"
fn process(conn: &mut Conn) {
    for item in &items {
        conn.fetch_all(&cmd).await.unwrap(); // qail-lint:disable-line N1-001
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1001 = diags
            .iter()
            .filter(|d| d.code == NPlusOneCode::N1001)
            .count();
        assert_eq!(n1001, 0, "Inline suppression should work");
    }

    // --- New tests for quality gaps ---

    #[test]
    fn qail_constructor_alone_not_flagged() {
        // Qail::get() is a builder — no DB round-trip without fetch_*/execute
        let source = r#"
fn process(items: Vec<Item>) {
    for item in &items {
        let cmd = Qail::get("users").eq("id", item.id);
        commands.push(cmd);
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "Qail::get without execution should NOT be flagged, got: {:?}",
            diags
        );
    }

    #[test]
    fn sqlx_query_builder_not_flagged() {
        // sqlx::query() is a builder, not execution
        let source = r#"
fn process(items: Vec<Item>) {
    for item in &items {
        let q = sqlx::query("SELECT 1");
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "sqlx::query() builder should NOT be flagged, got: {:?}",
            diags
        );
    }

    #[test]
    fn end_column_matches_method_name() {
        let source = r#"
fn process(conn: &mut Conn) {
    for item in &items {
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(!diags.is_empty());
        let d = &diags[0];
        assert_eq!(
            d.end_column - d.column,
            "fetch_all".len(),
            "end_column should be column + method name length"
        );
    }

    #[test]
    fn n1003_transitive_wrapper_call_in_loop() {
        let source = r#"
async fn load_user_leaf(conn: &mut Conn, id: Uuid) -> User {
    conn.fetch_one(&Qail::get("users").eq("id", id)).await.unwrap()
}

async fn load_user_wrapper(conn: &mut Conn, id: Uuid) -> User {
    load_user_leaf(conn, id).await
}

async fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let _u = load_user_wrapper(conn, *id).await;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_some(),
            "Expected N1-003 for transitive wrapper call, got: {:?}",
            diags
        );
    }

    #[test]
    fn iterator_map_collect_detected_as_loop() {
        let source = r#"
fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    let _futs = ids
        .iter()
        .map(|id| conn.fetch_all(&Qail::get("users").eq("id", id)))
        .collect::<Vec<_>>();
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(n1002.is_some(), "Expected N1-002 for iterator map closure");
    }

    #[test]
    fn option_map_not_treated_as_loop() {
        let source = r#"
fn process(opt_id: Option<Uuid>, conn: &mut Conn) {
    let _ = opt_id.map(|id| conn.fetch_all(&Qail::get("users").eq("id", id)));
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "Option::map is not iterator loop semantics, got: {:?}",
            diags
        );
    }

    #[test]
    fn bounded_inline_vec_loop_not_flagged() {
        let source = r#"
async fn process(conn: &mut Conn) {
    for (market, pax) in &vec![
        ("dom", "adult"),
        ("dom", "child"),
        ("dom", "infant"),
        ("intl", "adult"),
        ("intl", "child"),
        ("intl", "infant"),
    ] {
        conn.fetch_all(&Qail::get("pricing").eq("market", market).eq("pax", pax))
            .await
            .unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "Small bounded inline vec loop should not trigger N+1 diagnostics, got: {:?}",
            diags
        );
    }

    #[test]
    fn bounded_binding_loop_not_flagged() {
        let source = r#"
async fn process(conn: &mut Conn) {
    let tier_pairs = vec![
        ("dom", "adult"),
        ("dom", "child"),
        ("dom", "infant"),
        ("intl", "adult"),
        ("intl", "child"),
        ("intl", "infant"),
    ];
    for (market, pax) in &tier_pairs {
        conn.fetch_all(&Qail::get("pricing").eq("market", market).eq("pax", pax))
            .await
            .unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "Small bounded binding loop should not trigger N+1 diagnostics, got: {:?}",
            diags
        );
    }

    #[test]
    fn bounded_inner_loop_inside_unbounded_loop_still_flags() {
        let source = r#"
async fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    let tier_pairs = vec![
        ("dom", "adult"),
        ("dom", "child"),
        ("dom", "infant"),
        ("intl", "adult"),
        ("intl", "child"),
        ("intl", "infant"),
    ];
    for id in &ids {
        for (market, pax) in &tier_pairs {
            conn.fetch_all(
                &Qail::get("pricing")
                    .eq("user_id", id)
                    .eq("market", market)
                    .eq("pax", pax),
            )
            .await
            .unwrap();
        }
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "Outer unbounded loop should still trigger N1-002, got: {:?}",
            diags
        );
    }

    #[test]
    fn event_loop_with_wait_point_is_not_flagged_as_n_plus_one() {
        let source = r#"
async fn process(mut rx: Rx, conn: &mut Conn) {
    loop {
        let _msg = rx.recv().await;
        conn.fetch_all(&static_query).await.unwrap();
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        assert!(
            diags.is_empty(),
            "Event loop should not trigger N+1 diagnostics, got: {:?}",
            diags
        );
    }

    #[test]
    fn event_loop_with_inner_work_loop_still_flags_inner_n_plus_one() {
        let source = r#"
async fn process(mut rx: Rx, ids: Vec<Uuid>, conn: &mut Conn) {
    loop {
        let _msg = rx.recv().await;
        for id in &ids {
            conn.fetch_all(&Qail::get("users").eq("id", id)).await.unwrap();
        }
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1002 = diags.iter().find(|d| d.code == NPlusOneCode::N1002);
        assert!(
            n1002.is_some(),
            "Inner work loop should still trigger N1-002, got: {:?}",
            diags
        );
    }

    #[test]
    fn n1003_does_not_propagate_by_short_name() {
        let source = r#"
mod helpers {
    pub async fn new(conn: &mut Conn) {
        conn.fetch_one(&Qail::get("users")).await.unwrap();
    }
}

pub async fn new(_conn: &mut Conn) {}

async fn process(conn: &mut Conn, ids: Vec<Uuid>) {
    for _id in &ids {
        new(conn).await;
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_none(),
            "Short-name-only propagation should not trigger N1-003, got: {:?}",
            diags
        );
    }

    #[test]
    fn n1003_method_resolution_is_impl_qualified() {
        let source = r#"
struct A;
struct B;

impl A {
    async fn new(&self, conn: &mut Conn) {
        conn.fetch_one(&Qail::get("users")).await.unwrap();
    }
}

impl B {
    async fn new(&self, _conn: &mut Conn) {}

    async fn process(&self, conn: &mut Conn, ids: Vec<Uuid>) {
        for _id in &ids {
            self.new(conn).await;
        }
    }
}
"#;
        let diags = detect_n_plus_one_in_file("test.rs", source);
        let n1003 = diags.iter().find(|d| d.code == NPlusOneCode::N1003);
        assert!(
            n1003.is_none(),
            "Impl-qualified method resolution should avoid cross-type N1-003, got: {:?}",
            diags
        );
    }

    #[test]
    fn cross_file_n1003_propagates_via_module_index() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "qail_nplus1_cross_file_{}_{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&root).unwrap();

        let helpers = r#"
pub async fn load_user(conn: &mut Conn, id: Uuid) -> User {
    conn.fetch_one(&Qail::get("users").eq("id", id)).await.unwrap()
}
"#;
        let main = r#"
mod helpers;

async fn process(ids: Vec<Uuid>, conn: &mut Conn) {
    for id in &ids {
        let _ = helpers::load_user(conn, *id).await;
    }
}
"#;
        std::fs::write(root.join("helpers.rs"), helpers).unwrap();
        std::fs::write(root.join("main.rs"), main).unwrap();

        let diags = detect_n_plus_one_in_dir(&root);
        let _ = std::fs::remove_dir_all(&root);

        let has_n1003 = diags.iter().any(|d| d.code == NPlusOneCode::N1003);
        assert!(
            has_n1003,
            "Expected cross-file N1-003 via module index, got: {:?}",
            diags
        );
    }
