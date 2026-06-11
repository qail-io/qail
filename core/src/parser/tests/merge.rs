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

#[test]
fn test_merge_rejects_malformed_identifiers() {
    for query in [
        "merge .users using staging_users on users.id = staging_users.id when matched then delete",
        "merge users. using staging_users on users.id = staging_users.id when matched then delete",
        "merge users as 1u using staging_users on users.id = staging_users.id when matched then delete",
        "merge users using .staging_users on users.id = staging_users.id when matched then delete",
        "merge users using staging_users as s. on users.id = s.id when matched then delete",
        "merge users using staging_users on .users.id = staging_users.id when matched then delete",
        "merge users using staging_users on users.id = .staging_users.id when matched then delete",
        "merge users using staging_users on users.id = staging_users.id when matched then update set .name = staging_users.name",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (.id) values (staging_users.id)",
    ] {
        assert!(
            parse(query).is_err(),
            "malformed MERGE identifier parsed: {query}"
        );
    }
}

#[test]
fn test_merge_rejects_invalid_native_shape() {
    for query in [
        "merge users as public.u using staging_users on u.id = staging_users.id when matched then delete",
        "merge users public.u using staging_users on u.id = staging_users.id when matched then delete",
        "merge users as u.part using staging_users on u.id = staging_users.id when matched then delete",
        "merge users u.part using staging_users on u.id = staging_users.id when matched then delete",
        "merge users using staging_users as staging.s on users.id = s.id when matched then delete",
        "merge users using staging_users staging.s on users.id = s.id when matched then delete",
        "merge users using (get staging_users fields id) as staging.s on users.id = s.id when matched then delete",
        "merge users using (get staging_users fields id) staging.s on users.id = s.id when matched then delete",
        "merge users using staging_users on users.id = staging_users.id when matched then update set users.name = staging_users.name",
        "merge users using staging_users on users.id = staging_users.id when matched then update set name. = staging_users.name",
        "merge users using staging_users on users.id = staging_users.id when matched then update set profile.name = staging_users.name",
        "merge users using staging_users on users.id = staging_users.id when matched then update set name = staging_users.name, name = staging_users.display_name",
        "merge users using staging_users on users.id = staging_users.id when matched then update set name = staging_users.name, email = staging_users.email, name = staging_users.display_name",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (users.id) values (staging_users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (id.) values (staging_users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (profile.id) values (staging_users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (id, id) values (staging_users.id, staging_users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (id, email, id) values (staging_users.id, staging_users.email, staging_users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (id, email) values (staging_users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (id) values (staging_users.id, staging_users.email)",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (id, email, name) values (staging_users.id, staging_users.email)",
        "merge users using staging_users on users.id = staging_users.id when not matched then insert (id, email) values (staging_users.id, staging_users.email, staging_users.name)",
        "merge users using staging_users on users.id = staging_users.id when matched then insert (id) values (staging_users.id)",
        "merge users using staging_users on users.id = staging_users.id when matched and users.active = true then insert (id) values (staging_users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched then update set name = staging_users.name",
        "merge users using staging_users on users.id = staging_users.id when not matched by target then update set name = staging_users.name",
        "merge users using staging_users on users.id = staging_users.id when not matched by target and staging_users.active = true then update set name = staging_users.name",
        "merge users using staging_users on users.id = staging_users.id when not matched then delete",
        "merge users using staging_users on users.id = staging_users.id when not matched by target then delete",
        "merge users using staging_users on users.id = staging_users.id when not matched by source then insert (id) values (users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched by source and users.active = false then insert (id) values (users.id)",
        "merge users using staging_users on users.id = staging_users.id when not matched by source then insert values (users.id)",
        "merge users using (set staging_users values active = true) as s on users.id = s.id when matched then delete",
        "merge users using (del staging_users where active = false) as s on users.id = s.id when matched then delete",
        "merge users using (add staging_users values 1) as s on users.id = s.id when matched then delete",
        "merge users using (make staging_users id:uuid) as s on users.id = s.id when matched then delete",
        "merge users using (index idx_staging_users_id on staging_users id) as s on users.id = s.id when matched then delete",
        "merge users using (begin) as s on users.id = s.id when matched then delete",
        "merge users using (cnt staging_users) as s on users.id = s.id when matched then delete",
        "merge users using (drop staging_users) as s on users.id = s.id when matched then delete",
    ] {
        assert!(parse(query).is_err(), "invalid MERGE shape parsed: {query}");
    }
}
