# Debugging Status Report: Login 400 Bad Request on Staging (engine.qail.io)

**Date:** 2026-02-07
**Objective:** Resolve "Bad request: no rows returned by a query that expected to return at least one row" error during login.

## 1. Root Cause Analysis
- **Error Source:** The error occurs in the **Auth Service** during the 2FA update step (`UPDATE users ... RETURNING *`).
- **Mechanism:**
    - The `users` table on Staging (`qail-engine-db`) has `FORCE ROW LEVEL SECURITY` enabled.
    - The `users_update_policy` requires one of the following session variables to be set:
        - `app.is_super_admin = 'true'`
        - `app.current_operator_id` matching the user's `operator_id`
    - The current `sqlx::PgPool` (used by the engine) does **not** set these session variables.
    - Consequently, the RLS policy blocks the UPDATE operation, causing it to return **0 rows**, triggering the `fetch_one` error.
- **Verification:**
    - Confirmed `engine.qail.io` routes to Staging (port 8081).
    - Verified 28 tables have `FORCE ROW LEVEL SECURITY`.
    - Confirmed `qail-pg` driver correctly implements `set_rls_context` (SQL generation).

## 2. Proposed Solutions

### Immediate Hotfix (Rejected)
- Add an `after_connect` hook to the raw `sqlx::PgPool` to execute `SET app.is_super_admin = 'true'` on every connection.
- **Pros:** Unblocks login immediately with minimal code changes.
- **Cons:** Security concern (global bypass for the app).

### Preferred Solution (Qail Native)
- **Migrate the Auth Repository to use `qail-pg` driver.**
- **Why:** The Qail driver natively supports `RlsContext`. It can set the session variables per-request (or per-connection scope) cleanly.
- **Plan:**
    1. Create a Qail-native version of `auth_repository` and `user_repository`.
    2. Leverage `RlsContext::super_admin()` for system operations like login.
    3. Test against the real Staging DB or a local replica to ensure RLS policies are satisfied.

## 3. Next Steps
- Implement the Qail-native Auth repository.
- Verify that `PgDriver::set_rls_context` correctly sets the session variables required by `users_update_policy`.
- Deploy and validate on Staging.
