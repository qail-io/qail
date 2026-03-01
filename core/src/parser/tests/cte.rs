//! Tests for CTE parsing — strict recursive CTE pipeline.
//!
//! Covers: valid recursive parse, missing UNION ALL, multiple top-level unions,
//! UNION (distinct) rejected, base self-reference rejected, recursive
//! non-self-reference rejected, quoted/commented UNION ALL ignored,
//! nested paren UNION ALL ignored.

use crate::parser::parse;

// ─── Valid recursive CTE ──────────────────────────────────────────────

#[test]
fn test_recursive_cte_valid() {
    // Base: get nodes (no reference to "tree")
    // Recursive: get nodes join tree ... (references "tree")
    // QAIL syntax: joins come before fields
    let input = "WITH RECURSIVE tree(id, parent_id, depth) AS (\
        get nodes fields id, parent_id where parent_id is null \
        UNION ALL \
        get nodes join tree on parent_id = id fields id, parent_id\
    ) get tree";

    let result = parse(input);
    assert!(result.is_ok(), "parse failed: {:?}", result.err());

    let cmd = result.unwrap();
    assert_eq!(cmd.ctes.len(), 1);

    let cte = &cmd.ctes[0];
    assert_eq!(cte.name, "tree");
    assert!(cte.recursive);
    assert_eq!(cte.columns, vec!["id", "parent_id", "depth"]);
    assert!(
        cte.recursive_query.is_some(),
        "recursive_query must be populated"
    );

    // Base query should reference 'nodes', not 'tree'
    assert_eq!(cte.base_query.table, "nodes");
    // Recursive query table should reference 'nodes' with join to 'tree'
    let rq = cte.recursive_query.as_ref().unwrap();
    assert_eq!(rq.table, "nodes");
    assert!(!rq.joins.is_empty(), "recursive query must have a JOIN");
}

// ─── Missing UNION ALL → error ───────────────────────────────────────

#[test]
fn test_recursive_cte_missing_union_all() {
    let input = "WITH RECURSIVE tree AS (\
        get nodes where parent_id is null\
    ) get tree";

    let result = parse(input);
    assert!(
        result.is_err(),
        "should fail: no UNION ALL in recursive CTE"
    );
}

// ─── Multiple top-level UNION ALL → error ────────────────────────────

#[test]
fn test_recursive_cte_multiple_union_all() {
    let input = "WITH RECURSIVE tree AS (\
        get a UNION ALL get b UNION ALL get c\
    ) get tree";

    let result = parse(input);
    assert!(result.is_err(), "should fail: multiple top-level UNION ALL");
}

// ─── Bare UNION (without ALL) → error ────────────────────────────────

#[test]
fn test_recursive_cte_bare_union_rejected() {
    let input = "WITH RECURSIVE tree AS (\
        get nodes where parent_id is null \
        UNION \
        get nodes join tree on parent_id = id\
    ) get tree";

    let result = parse(input);
    assert!(result.is_err(), "should fail: bare UNION not allowed");
}

// ─── Base references CTE name → error ────────────────────────────────

#[test]
fn test_recursive_cte_base_self_reference() {
    // The base part (before UNION ALL) references 'tree' — not allowed
    let input = "WITH RECURSIVE tree AS (\
        get tree where parent_id is null \
        UNION ALL \
        get nodes join tree on parent_id = id\
    ) get tree";

    let result = parse(input);
    assert!(result.is_err(), "should fail: base references CTE name");
}

// ─── Recursive part doesn't reference CTE name → error ──────────────

#[test]
fn test_recursive_cte_no_self_reference_in_recursive() {
    // The recursive part (after UNION ALL) doesn't reference 'tree'
    let input = "WITH RECURSIVE tree AS (\
        get nodes where parent_id is null \
        UNION ALL \
        get nodes where parent_id is not null\
    ) get tree";

    let result = parse(input);
    assert!(
        result.is_err(),
        "should fail: recursive part doesn't reference CTE name"
    );
}

// ─── UNION ALL inside quotes → ignored ───────────────────────────────

#[test]
fn test_union_all_inside_quotes_ignored() {
    use crate::parser::grammar::cte::split_top_level_union_all;

    let body = "get t1 where name = 'UNION ALL' UNION ALL get t2";
    let result = split_top_level_union_all(body);
    assert!(result.is_ok(), "should find exactly one real UNION ALL");

    let (base, recursive) = result.unwrap();
    assert!(
        base.contains("'UNION ALL'"),
        "base should contain quoted UNION ALL"
    );
    assert!(
        recursive.trim().starts_with("get"),
        "recursive should be the second query"
    );
}

// ─── UNION ALL inside comments → ignored ─────────────────────────────

#[test]
fn test_union_all_inside_comment_ignored() {
    use crate::parser::grammar::cte::split_top_level_union_all;

    let body = "get t1 -- UNION ALL this is a comment\n UNION ALL get t2";
    let result = split_top_level_union_all(body);
    assert!(result.is_ok(), "should find exactly one real UNION ALL");
}

#[test]
fn test_union_all_inside_block_comment_ignored() {
    use crate::parser::grammar::cte::split_top_level_union_all;

    let body = "get t1 /* UNION ALL inside block */ UNION ALL get t2";
    let result = split_top_level_union_all(body);
    assert!(result.is_ok(), "should find exactly one real UNION ALL");
}

// ─── UNION ALL inside nested parens → ignored ────────────────────────

#[test]
fn test_union_all_inside_nested_parens_ignored() {
    use crate::parser::grammar::cte::split_top_level_union_all;

    let body = "get t1 where id in (SELECT 1 UNION ALL SELECT 2) UNION ALL get t2";
    let result = split_top_level_union_all(body);
    assert!(
        result.is_ok(),
        "should find exactly one top-level UNION ALL"
    );

    let (base, _) = result.unwrap();
    assert!(
        base.contains("UNION ALL SELECT 2)"),
        "nested UNION ALL should remain in base"
    );
}

// ─── Non-recursive CTE is strict QAIL-only ───────────────────────────

#[test]
fn test_non_recursive_cte_raw_sql_rejected() {
    // Non-recursive CTE with raw SQL body — strict mode should reject
    let input = "WITH summary AS (SELECT id, count(*) as cnt FROM orders GROUP BY id) get summary";

    let result = parse(input);
    assert!(result.is_err(), "non-recursive raw SQL must be rejected");
}

#[test]
fn test_non_recursive_cte_qail_valid() {
    let input = "WITH summary AS (get orders fields id, total) get summary";
    let result = parse(input);
    assert!(
        result.is_ok(),
        "non-recursive QAIL CTE should parse: {:?}",
        result.err()
    );

    let cmd = result.unwrap();
    assert_eq!(cmd.ctes.len(), 1);
    assert_eq!(cmd.ctes[0].name, "summary");
    assert_eq!(cmd.ctes[0].base_query.table, "orders");
    assert!(!cmd.ctes[0].base_query.is_raw_sql());
}

// ─── Recursive with column aliases ───────────────────────────────────

#[test]
fn test_recursive_cte_with_columns() {
    let input = "WITH RECURSIVE ancestors(id, name, level) AS (\
        get people fields id, name where parent_id is null \
        UNION ALL \
        get people join ancestors on parent_id = id fields id, name\
    ) get ancestors";

    let result = parse(input);
    assert!(result.is_ok(), "failed: {:?}", result.err());

    let cmd = result.unwrap();
    let cte = &cmd.ctes[0];
    assert_eq!(cte.columns, vec!["id", "name", "level"]);
    assert!(cte.recursive_query.is_some());
}

// ─── Case-insensitive UNION ALL detection ────────────────────────────

#[test]
fn test_union_all_case_insensitive() {
    use crate::parser::grammar::cte::split_top_level_union_all;

    let body = "get t1 union all get t2";
    let result = split_top_level_union_all(body);
    assert!(result.is_ok(), "should match case-insensitive UNION ALL");

    let body2 = "get t1 Union All get t2";
    let result2 = split_top_level_union_all(body2);
    assert!(result2.is_ok(), "should match mixed-case UNION ALL");
}

// ─── Self-reference check is case-insensitive ────────────────────────

#[test]
fn test_contains_ident_case_insensitive() {
    use crate::parser::grammar::cte::contains_ident_outside_quotes_comments;

    assert!(contains_ident_outside_quotes_comments(
        "get nodes join Tree on parent_id = id",
        "tree"
    ));
    assert!(contains_ident_outside_quotes_comments(
        "get nodes join TREE on parent_id = id",
        "tree"
    ));
    // Should not match inside quotes
    assert!(!contains_ident_outside_quotes_comments(
        "get nodes where label = 'tree'",
        "tree"
    ));
    // Should not match as substring
    assert!(!contains_ident_outside_quotes_comments("get trees", "tree"));
}
