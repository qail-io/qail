use bytes::BytesMut;
use qail_core::ast::Qail;
use qail_core::ast::builders::ExprExt;
use qail_core::ast::builders::{col, subquery};

fn main() {
    let last_message = subquery(
        Qail::get("app_chat_messages")
            .column("content")
            .eq("session_id", col("app_chat_sessions.id"))
            .order_desc("created_at")
            .limit(1),
    )
    .with_alias("last_message");

    let unread_count = subquery(
        Qail::get("app_chat_messages")
            .column("count(*)")
            .eq("session_id", col("app_chat_sessions.id"))
            .eq("sender_type", "user")
            .ne("status", "read"),
    )
    .with_alias("unread_count");

    let cmd = Qail::get("app_chat_sessions")
        .columns(["app_chat_sessions.id", "app_chat_sessions.status"])
        .select_expr(last_message)
        .select_expr(unread_count)
        .left_join_as("users", "u", "u.id", "app_chat_sessions.user_id")
        .ne("app_chat_sessions.status", "closed")
        .order_desc("app_chat_sessions.updated_at")
        .limit(20)
        .offset(0);

    // AST encoder (parameterized)
    let mut sql_buf = BytesMut::new();
    let mut params: Vec<Option<Vec<u8>>> = Vec::new();
    qail_pg::protocol::AstEncoder::encode_select_sql(&cmd, &mut sql_buf, &mut params);

    println!("SQL: {}", std::str::from_utf8(&sql_buf).unwrap_or("ERR"));
    println!("\nParams ({})", params.len());
    for (i, p) in params.iter().enumerate() {
        match p {
            Some(v) => println!(
                "  ${}: {}",
                i + 1,
                std::str::from_utf8(v).unwrap_or("binary")
            ),
            None => println!("  ${}: NULL", i + 1),
        }
    }
}
