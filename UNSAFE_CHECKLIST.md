# Unsafe Connection Patterns

Any call to `acquire_raw()` MUST include a `// SAFETY:` comment explaining
why raw acquisition is justified (i.e., what sets the RLS context or why
no tenant context is needed).

## CI Check

```bash
# Should return empty — every acquire_raw() call must have a SAFETY comment
grep -rn "acquire_raw()" pg/src/ | grep -v "// SAFETY:" | grep -v "pub(crate) async fn acquire_raw" | grep -v "///\|//!" | grep -v "^$"
```

## Current Justified Call Sites

| File | Line | Justification |
|------|------|---------------|
| `pool.rs` | `acquire_with_rls()` | RLS context set immediately after via `context_to_sql()` |
| `pool.rs` | `acquire_with_rls_timeout()` | RLS context + timeout set immediately after via `context_to_sql_with_timeout()` |
| `pool.rs` | `acquire_with_branch()` | Branch context set immediately after via `branch_context_sql()` |

## Rules

1. **Never expose `acquire_raw()` as `pub`** — it is `pub(crate)` by design.
2. **Never call `acquire_raw()` in handler code** — handlers must use `acquire_with_rls()` or `acquire_system()`.
3. **Every new call site** must be reviewed for RLS context setup.
