use crate::ast::*;
use crate::parser::parse;

#[test]
fn test_parse_merge_update_insert() {
    let cmd = parse(
        "merge users as u using staging_users as s on u.id = s.id \
         when matched and u.name != s.name then update set name = s.name, email = s.email \
         when not matched then insert (id, name, email) values (s.id, s.name, s.email)",
    )
    .unwrap();

    assert_eq!(cmd.action, Action::Merge);
    assert_eq!(cmd.table, "users");

    let merge = cmd.merge.unwrap();
    assert_eq!(merge.target_alias.as_deref(), Some("u"));
    assert_eq!(
        merge.source,
        MergeSource::Table {
            name: "staging_users".to_string(),
            alias: Some("s".to_string()),
        }
    );
    assert_eq!(merge.on.len(), 1);
    assert_eq!(merge.on[0].left, Expr::Named("u.id".to_string()));
    assert_eq!(merge.on[0].value, Value::Column("s.id".to_string()));
    assert_eq!(merge.clauses.len(), 2);

    assert_eq!(merge.clauses[0].match_kind, MergeMatchKind::Matched);
    assert_eq!(merge.clauses[0].condition.len(), 1);
    match &merge.clauses[0].action {
        MergeAction::Update { assignments } => {
            assert_eq!(assignments.len(), 2);
            assert_eq!(assignments[0].0, "name");
            assert_eq!(assignments[0].1, Expr::Named("s.name".to_string()));
        }
        other => panic!("expected update action, got {other:?}"),
    }

    assert_eq!(
        merge.clauses[1].match_kind,
        MergeMatchKind::NotMatchedByTarget
    );
    match &merge.clauses[1].action {
        MergeAction::Insert { columns, values } => {
            assert_eq!(columns, &["id", "name", "email"]);
            assert_eq!(values[2], Expr::Named("s.email".to_string()));
        }
        other => panic!("expected insert action, got {other:?}"),
    }
}

#[test]
fn test_parse_merge_by_source_delete() {
    let cmd = parse(
        "merge users using staging_users on users.id = staging_users.id \
         when not matched by source then delete",
    )
    .unwrap();

    let merge = cmd.merge.unwrap();
    assert_eq!(
        merge.clauses[0].match_kind,
        MergeMatchKind::NotMatchedBySource
    );
    assert_eq!(merge.clauses[0].action, MergeAction::Delete);
}
