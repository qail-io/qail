# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.28.0)

- Redundant compatibility APIs were removed: `try_with_rls`, `try_join_on`, `try_encode_cmd_binary`, `AnalysisMode::Regex`, `validate_against_schema`, and legacy `i32` cancel-key wrappers.
- Builder and encoder runtime paths now keep malformed payloads, ambiguous relation metadata, and size-limit failures on structured fallible APIs.
- Gateway and driver docs now consistently describe tenant-first runtime scope through `tenant_id`, `current_user_id`, `current_agent_id`, and `is_super_admin`.
- Workspace crates and docs references were bumped to `0.28.0`.
