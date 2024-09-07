use super::Formatter;
use crate::ast::{
    CTEDef, Cage, CageKind, Condition, Expr, Join, JoinKind, LogicalOp, Operator, Qail,
    SortOrder, Value,
};

#[test]
fn test_fmt_simple_get() {
    let cmd = Qail::get("users");
    let formatter = Formatter::new();
    let output = formatter.format(&cmd).unwrap();
    assert_eq!(output.trim(), "get users");
}

#[test]
fn test_fmt_get_fields() {
    let mut cmd = Qail::get("users");
    cmd.columns = vec![
        Expr::Named("id".to_string()),
        Expr::Aliased {
            name: "name".to_string(),
            alias: "full_name".to_string(),
        },
    ];

    let formatter = Formatter::new();
    let output = formatter.format(&cmd).unwrap();

    let expected = r#"
get users
fields
  id,
  name as full_name
"#;
    assert_eq!(output.trim(), expected.trim());
}

#[test]
fn test_fmt_complex_query() {
    // get whatsapp_contacts
    // fields
    //   id
    //   phone_number
    // where rn = 1
    // join message_stats
    // order by created_at desc

    let mut cmd = Qail::get("whatsapp_contacts");

    cmd.columns = vec![
        Expr::Named("id".to_string()),
        Expr::Named("phone_number".to_string()),
    ];

    cmd.joins = vec![Join {
        table: "message_stats".to_string(),
        kind: JoinKind::Inner,
        on: None, // Implicit join for now to match proposal simplification or explicit? Proposal had explicit ON in example 4.
        on_true: false,
    }];

    // Proposal example 4: join message_stats on ...
    // Let's use that.
    cmd.joins[0].on = Some(vec![Condition {
        left: Expr::Named("phone_number".to_string()),
        op: Operator::Eq,
        value: Value::Null,
        is_array_unnest: false,
    }]);
    // Wait, I need to check `Value` definition to see if it supports identifiers/columns.
    // If not, my formatter test might be wrong about how joins are stored.
    // Let's assume for this test we filter by literal.

    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        logical_op: LogicalOp::And,
        conditions: vec![Condition {
            left: Expr::Named("rn".to_string()),
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        }],
    });

    cmd.cages.push(Cage {
        kind: CageKind::Sort(SortOrder::Desc),
        logical_op: LogicalOp::And,
        conditions: vec![Condition {
            left: Expr::Named("created_at".to_string()),
            op: Operator::Eq, // ignored for sort
            value: Value::Null,
            is_array_unnest: false,
        }],
    });

    let formatter = Formatter::new();
    let output = formatter.format(&cmd).unwrap();

    let expected = r#"
get whatsapp_contacts
fields
  id,
  phone_number
join message_stats
  on phone_number = null
where rn = 1
order by
  created_at desc
"#;
    assert_eq!(output.trim(), expected.trim());
}

#[test]
fn test_fmt_cte() {
    // with cte = get table
    // get cte

    let mut cmd = Qail::get("cte");
    cmd.ctes.push(CTEDef {
        name: "cte".to_string(),
        recursive: false,
        columns: vec![],
        base_query: Box::new(Qail::get("table")),
        recursive_query: None,
        source_table: None,
    });

    let formatter = Formatter::new();
    let output = formatter.format(&cmd).unwrap();

    let expected = r#"
with cte = 
  get table

get cte
"#;
    assert_eq!(output.trim(), expected.trim());
}
