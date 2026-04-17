# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.27.8)

- Malformed `X-Branch-ID` values now fail closed with `INVALID_BRANCH_NAME` (instead of silently defaulting to `main`), and `main` matching is case-insensitive.
- REST tenant scope enforcement now resolves the scope column per table (`tenant_id` with legacy `operator_id` fallback) and applies consistently across CRUD/nested/aggregate handlers.
- `search_columns` now accepts documented CSV input such as `name,description` with strict identifier validation.
- `qail migrate shadow` now correctly quotes hyphenated database names when creating/dropping shadow databases.
- Security dependency update: `rustls-webpki` upgraded to `0.103.12`.
