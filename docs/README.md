# QAIL Documentation Source

This directory contains the source for the official QAIL documentation (mdBook).

## Current Status (~80% Production Ready)

| Feature | Status |
|---------|--------|
| SSL/TLS, SCRAM-SHA-256 | ✅ |
| Connection Pooling | ✅ |
| Query Plan Caching | ✅ |
| Transactions | ✅ |
| CTEs, Subqueries | ✅ |
| Window Functions | ✅ |
| JSON/JSONB, Arrays | ✅ |
| COPY Protocol | ✅ |
| UPSERT, RETURNING | ✅ |
| LATERAL JOIN | ✅ |
| Savepoints | ✅ |

## Quick Commands

```bash
# Schema operations
qail pull postgres://...           # Extract schema from DB
qail diff old.qail new.qail        # Compare schemas
qail lint schema.qail              # Check best practices

# Migration operations
qail migrate create add_users      # Create named migration
qail migrate plan old:new          # Preview SQL
qail migrate up old:new postgres:  # Apply migrations
```

## Editing Documentation

1. Edit markdown files in `src/`
2. Update `src/SUMMARY.md` if adding new pages

## Deployment

```bash
mdbook build
# Copy to public/docs so it gets built by Astro
cp -r book/* /Users/orion/qail-web/public/docs/
# Build and deploy (npm run deploy = astro build && wrangler pages deploy dist)
cd /Users/orion/qail-web && npm run deploy
```
