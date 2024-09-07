/// WhatsApp Insights Query - QAIL Builder API Example
///
/// This demonstrates how to build the complex WhatsApp insights query
/// using QAIL's programmatic builder API with all the new features:
/// - COUNT(DISTINCT col)
/// - COUNT(*) FILTER (WHERE ...)  
/// - CASE WHEN ... THEN ... END
/// - Type casting (::float8)
use qail_core::ast::*;
use qail_core::transpiler::ToSql;

fn main() {
    // Build the stats CTE subquery
    let mut stats_query = Qail::get("whatsapp_messages");

    // COUNT(DISTINCT phone_number) AS total_contacts
    stats_query.columns.push(Expr::Aggregate {
        col: "phone_number".to_string(),
        func: AggregateFunc::Count,
        distinct: true,
        filter: None,
        alias: Some("total_contacts".to_string()),
    });

    // COUNT(*) AS total_messages
    stats_query.columns.push(Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: Some("total_messages".to_string()),
    });

    // COUNT(*) FILTER (WHERE direction = 'outbound' AND created_at > NOW() - INTERVAL '24 hours') AS messages_sent_24h
    stats_query.columns.push(Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: Some(vec![
            Condition {
                left: Expr::Named("direction".to_string()),
                op: Operator::Eq,
                value: Value::String("outbound".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("created_at".to_string()),
                op: Operator::Gt,
                value: Value::Function("NOW() - INTERVAL '24 hours'".to_string()),
                is_array_unnest: false,
            },
        ]),
        alias: Some("messages_sent_24h".to_string()),
    });

    // COUNT(*) FILTER (WHERE direction = 'inbound' AND created_at > NOW() - INTERVAL '24 hours') AS messages_received_24h
    stats_query.columns.push(Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: Some(vec![
            Condition {
                left: Expr::Named("direction".to_string()),
                op: Operator::Eq,
                value: Value::String("inbound".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("created_at".to_string()),
                op: Operator::Gt,
                value: Value::Function("NOW() - INTERVAL '24 hours'".to_string()),
                is_array_unnest: false,
            },
        ]),
        alias: Some("messages_received_24h".to_string()),
    });

    // COUNT(*) FILTER (WHERE direction = 'inbound' AND status = 'received') AS unread_messages
    stats_query.columns.push(Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: Some(vec![
            Condition {
                left: Expr::Named("direction".to_string()),
                op: Operator::Eq,
                value: Value::String("inbound".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("status".to_string()),
                op: Operator::Eq,
                value: Value::String("received".to_string()),
                is_array_unnest: false,
            },
        ]),
        alias: Some("unread_messages".to_string()),
    });

    // COUNT(*) FILTER (WHERE direction = 'outbound' AND created_at > NOW() - INTERVAL '24 hours' AND status IN ('delivered', 'read')) AS successful_deliveries_24h
    stats_query.columns.push(Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: Some(vec![
            Condition {
                left: Expr::Named("direction".to_string()),
                op: Operator::Eq,
                value: Value::String("outbound".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("created_at".to_string()),
                op: Operator::Gt,
                value: Value::Function("NOW() - INTERVAL '24 hours'".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("status".to_string()),
                op: Operator::In,
                value: Value::Array(vec![
                    Value::String("delivered".to_string()),
                    Value::String("read".to_string()),
                ]),
                is_array_unnest: false,
            },
        ]),
        alias: Some("successful_deliveries_24h".to_string()),
    });

    // Create CTE definition (using new API)
    let stats_cte = stats_query.to_cte("stats");

    // Build final SELECT from CTE with CASE expression for delivery_rate
    let mut final_query = Qail::get("stats").with_cte(stats_cte);
    final_query.columns = vec![
        Expr::Named("total_contacts".to_string()),
        Expr::Named("total_messages".to_string()),
        Expr::Named("messages_sent_24h".to_string()),
        Expr::Named("messages_received_24h".to_string()),
        Expr::Named("unread_messages".to_string()),
        Expr::Case {
            when_clauses: vec![(
                Condition {
                    left: Expr::Named("messages_sent_24h".to_string()),
                    op: Operator::Gt,
                    value: Value::Int(0),
                    is_array_unnest: false,
                },
                Box::new(Expr::Binary {
                    left: Box::new(Expr::Binary {
                        left: Box::new(Expr::Cast {
                            expr: Box::new(Expr::Named("successful_deliveries_24h".to_string())),
                            target_type: "float8".to_string(),
                            alias: None,
                        }),
                        op: BinaryOp::Div,
                        right: Box::new(Expr::Cast {
                            expr: Box::new(Expr::Named("messages_sent_24h".to_string())),
                            target_type: "float8".to_string(),
                            alias: None,
                        }),
                        alias: None,
                    }),
                    op: BinaryOp::Mul,
                    right: Box::new(Expr::Named("100.0".to_string())),
                    alias: None,
                }),
            )],
            else_value: Some(Box::new(Expr::Named("0.0".to_string()))),
            alias: Some("delivery_rate_24h".to_string()),
        },
    ];

    // Generate SQL
    let sql = final_query.to_sql();
    println!("Generated SQL:\n{}", sql);
}
