use super::*;
use crate::ast::*;

// ========================================================================
// Syntax Tests
// ========================================================================

#[test]
fn test_v2_simple_get() {
    let cmd = parse("get::users:'_").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns, vec![Column::Star]);
}

#[test]
fn test_v2_get_with_columns() {
    let cmd = parse("get::users:'id'email").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(
        cmd.columns,
        vec![
            Column::Named("id".to_string()),
            Column::Named("email".to_string()),
        ]
    );
}

#[test]
fn test_v2_get_with_filter() {
    let cmd = parse("get::users:'_ [ 'active == true ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Filter);
    assert_eq!(cmd.cages[0].conditions.len(), 1);
    assert_eq!(cmd.cages[0].conditions[0].column, "active");
    assert_eq!(cmd.cages[0].conditions[0].op, Operator::Eq);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::Bool(true));
}

#[test]
fn test_v2_get_with_range_limit() {
    let cmd = parse("get::users:'_ [ 0..10 ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Limit(10));
}

#[test]
fn test_v2_get_with_range_offset() {
    let cmd = parse("get::users:'_ [ 20..30 ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    // Range 20..30 = LIMIT 10 with offset 20
    assert_eq!(cmd.cages[0].kind, CageKind::Limit(10));
    // Offset stored in conditions as workaround
    assert_eq!(cmd.cages[0].conditions[0].column, "__offset__");
    assert_eq!(cmd.cages[0].conditions[0].value, Value::Int(20));
}

#[test]
fn test_v2_get_with_sort_desc() {
    let cmd = parse("get::users:'_ [ -created_at ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Sort(SortOrder::Desc));
    assert_eq!(cmd.cages[0].conditions[0].column, "created_at");
}

#[test]
fn test_v2_get_with_sort_asc() {
    let cmd = parse("get::users:'_ [ +id ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Sort(SortOrder::Asc));
    assert_eq!(cmd.cages[0].conditions[0].column, "id");
}

#[test]
fn test_v2_fuzzy_match() {
    let cmd = parse("get::users:'id [ 'name ~ \"john\" ]").unwrap();
    assert_eq!(cmd.cages[0].conditions[0].op, Operator::Fuzzy);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::String("john".to_string()));
}

#[test]
fn test_v2_param_in_filter() {
    let cmd = parse("get::users:'id [ 'email == $1 ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::Param(1));
}

#[test]
fn test_v2_left_join() {
    // Joins come directly after table name, not after columns
    let cmd = parse("get::users<-posts:'id'title").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "posts");
    assert_eq!(cmd.joins[0].kind, JoinKind::Left);
}

#[test]
fn test_v2_inner_join() {
    let cmd = parse("get::users->posts:'id'title").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "posts");
    assert_eq!(cmd.joins[0].kind, JoinKind::Inner);
}

#[test]
fn test_v2_right_join() {
    let cmd = parse("get::orders->>customers:'_").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "customers");
    assert_eq!(cmd.joins[0].kind, JoinKind::Right);
}



#[test]
fn test_set_command() {
    let cmd = parse("set::users:[verified=true][id=$1]").unwrap();
    assert_eq!(cmd.action, Action::Set);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.cages.len(), 2);
}

#[test]
fn test_del_command() {
    let cmd = parse("del::sessions:[expired_at<now]").unwrap();
    assert_eq!(cmd.action, Action::Del);
    assert_eq!(cmd.table, "sessions");
}



#[test]
fn test_param_in_update() {
    let cmd = parse("set::users:[verified=true][id=$1]").unwrap();
    assert_eq!(cmd.action, Action::Set);
    assert_eq!(cmd.cages.len(), 2);
    assert_eq!(cmd.cages[1].conditions[0].value, Value::Param(1));
}

// ========================================================================
// Schema v0.7.0 Tests (DEFAULT, CHECK)
// ========================================================================

#[test]
fn test_make_with_default_uuid() {
    let cmd = parse("make::users:'id:uuid^pk = uuid()").unwrap();
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns.len(), 1);
    if let Column::Def { name, data_type, constraints } = &cmd.columns[0] {
        assert_eq!(name, "id");
        assert_eq!(data_type, "uuid");
        assert!(constraints.contains(&Constraint::PrimaryKey));
        assert!(constraints.iter().any(|c| matches!(c, Constraint::Default(v) if v == "uuid()")));
    } else {
        panic!("Expected Column::Def");
    }
}

#[test]
fn test_make_with_default_numeric() {
    let cmd = parse("make::stats:'count:bigint = 0").unwrap();
    assert_eq!(cmd.action, Action::Make);
    if let Column::Def { constraints, .. } = &cmd.columns[0] {
        assert!(constraints.iter().any(|c| matches!(c, Constraint::Default(v) if v == "0")));
    } else {
        panic!("Expected Column::Def");
    }
}

#[test]
fn test_make_with_check_constraint() {
    let cmd = parse(r#"make::orders:'status:varchar^check("pending","paid","cancelled")"#).unwrap();
    assert_eq!(cmd.action, Action::Make);
    if let Column::Def { name, constraints, .. } = &cmd.columns[0] {
        assert_eq!(name, "status");
        let check = constraints.iter().find(|c| matches!(c, Constraint::Check(_)));
        assert!(check.is_some());
        if let Some(Constraint::Check(vals)) = check {
            assert_eq!(vals, &vec!["pending".to_string(), "paid".to_string(), "cancelled".to_string()]);
        }
    } else {
        panic!("Expected Column::Def");
    }
}

// ========================================================================
// INDEX Tests (v0.7.0)
// ========================================================================

#[test]
fn test_index_basic() {
    let cmd = parse("index::idx_users_email^on(users:'email)").unwrap();
    assert_eq!(cmd.action, Action::Index);
    let idx = cmd.index_def.expect("index_def should be Some");
    assert_eq!(idx.name, "idx_users_email");
    assert_eq!(idx.table, "users");
    assert_eq!(idx.columns, vec!["email".to_string()]);
    assert!(!idx.unique);
}

#[test]
fn test_index_composite() {
    let cmd = parse("index::idx_lookup^on(orders:'user_id-created_at)").unwrap();
    assert_eq!(cmd.action, Action::Index);
    let idx = cmd.index_def.expect("index_def should be Some");
    assert_eq!(idx.name, "idx_lookup");
    assert_eq!(idx.table, "orders");
    assert_eq!(idx.columns, vec!["user_id".to_string(), "created_at".to_string()]);
}

#[test]
fn test_index_unique() {
    let cmd = parse("index::idx_phone^on(users:'phone)^unique").unwrap();
    assert_eq!(cmd.action, Action::Index);
    let idx = cmd.index_def.expect("index_def should be Some");
    assert_eq!(idx.name, "idx_phone");
    assert!(idx.unique);
}

// ========================================================================
// Composite Table Constraints Tests (v0.7.0)
// ========================================================================

#[test]
fn test_make_composite_unique() {
    let cmd = parse("make::bookings:'user_id:uuid'schedule_id:uuid^unique(user_id, schedule_id)").unwrap();
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table_constraints.len(), 1);
    if let TableConstraint::Unique(cols) = &cmd.table_constraints[0] {
        assert_eq!(cols, &vec!["user_id".to_string(), "schedule_id".to_string()]);
    } else {
        panic!("Expected TableConstraint::Unique");
    }
}

#[test]
fn test_make_composite_pk() {
    let cmd = parse("make::order_items:'order_id:uuid'product_id:uuid^pk(order_id, product_id)").unwrap();
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table_constraints.len(), 1);
    if let TableConstraint::PrimaryKey(cols) = &cmd.table_constraints[0] {
        assert_eq!(cols, &vec!["order_id".to_string(), "product_id".to_string()]);
    } else {
        panic!("Expected TableConstraint::PrimaryKey");
    }
}

#[test]
fn test_txn_commands() {
    let cmd = parse("txn::start").unwrap();
    assert_eq!(cmd.action, Action::TxnStart);
    
    let cmd = parse("txn::commit").unwrap();
    assert_eq!(cmd.action, Action::TxnCommit);
    
    let cmd = parse("txn::rollback").unwrap();
    assert_eq!(cmd.action, Action::TxnRollback);
}

#[test]
fn test_ddl_commands() {
    let cmd = parse("put::users:[id=1][name=John]").unwrap();
    assert_eq!(cmd.action, Action::Put);
    assert_eq!(cmd.table, "users");
    
    let cmd = parse("drop::users:password").unwrap();
    assert_eq!(cmd.action, Action::DropCol);
    assert_eq!(cmd.table, "users");
    if let Column::Named(n) = &cmd.columns[0] {
        assert_eq!(n, "password");
    }
    
    let cmd = parse("rename::users:oldname").unwrap();
    assert_eq!(cmd.action, Action::RenameCol);
}

#[test]
fn test_nested_identifiers() {
    use crate::parser::tokens::parse_identifier;
    
    // 1. Basic Nested
    let (_, id) = parse_identifier("metadata.theme").unwrap();
    assert_eq!(id, "metadata.theme");

    // 2. In Context (Cage)
    let (_, cmd) = parse_qail_cmd("get::users [metadata.theme='dark']").unwrap();
    if let CageKind::Filter = cmd.cages[0].kind {
        assert_eq!(cmd.cages[0].conditions[0].column, "metadata.theme");
        match &cmd.cages[0].conditions[0].value {
            Value::String(s) => assert_eq!(s, "dark"),
            _ => panic!("Expected string value"),
        }
    } else {
        panic!("Expected filter cage");
    }
}

#[test]
fn test_lsp_snippet_identifier() {
    use crate::parser::tokens::parse_identifier;
    
    // 1. Check identifier parsing directly
    let (_, id) = parse_identifier("${1:table}").unwrap();
    assert_eq!(id, "${1:table}");

    // 2. Check within a command
    let cmd = parse("get::${1:table}:'${2:column}").unwrap();
    assert_eq!(cmd.table, "${1:table}");
    if let Column::Named(n) = &cmd.columns[0] {
        assert_eq!(n, "${2:column}");
    } else {
        panic!("Expected Column::Named");
    }
}
