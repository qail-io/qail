use qail_core::ast::Qail;
use qail_core::ast::builders::ExprExt;
use qail_core::ast::builders::{coalesce, col, concat, subquery, text};
use qail_core::transpiler::ToSql;

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

    let user_name = coalesce([
        col("u.display_name"),
        concat([col("u.first_name"), text(" "), col("u.last_name")]).build(),
    ])
    .alias("user_name");

    let cmd = Qail::get("app_chat_sessions")
        .columns(["app_chat_sessions.id", "app_chat_sessions.status"])
        .column("u.email AS user_email")
        .select_expr(user_name)
        .select_expr(last_message)
        .select_expr(unread_count)
        .left_join_as("users", "u", "u.id", "app_chat_sessions.user_id")
        .ne("app_chat_sessions.status", "closed")
        .order_desc("app_chat_sessions.updated_at")
        .limit(20)
        .offset(0);

    // Use Display which calls the Formatter
    println!("=== Display (Formatter) ===");
    println!("{}", cmd);

    // Core-only SQL transpile
    println!("\n=== Transpiled SQL ===");
    println!("{}", cmd.to_sql());
}
