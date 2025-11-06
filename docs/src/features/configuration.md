# Centralized Configuration

Qail uses a single `qail.toml` file as the source of truth for all components. Configuration follows a layered priority:

**Environment Variables > `qail.toml` > Hardcoded Defaults**

## Quick Start

Generate a config with `qail init`, or create `qail.toml` manually:

```toml
[project]
name = "my-app"
mode = "postgres"

[postgres]
url = "postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST:-localhost}:5432/mydb"
max_connections = 20
min_connections = 2
idle_timeout_secs = 600
acquire_timeout_secs = 30
connect_timeout_secs = 10
test_on_acquire = false

[postgres.rls]
default_role = "app_user"
super_admin_role = "super_admin"

[redis]
host = "127.0.0.1"
port = 6379
max_connections = 10
password = "${REDIS_PASSWORD:-}"

[qdrant]
url = "http://localhost:6333"
grpc = "localhost:6334"
max_connections = 10

[gateway]
bind = "0.0.0.0:8080"
cors = true
policy = "policies.yaml"
max_result_rows = 10000
statement_timeout_ms = 30000
explain_max_cost = 100000.0
explain_max_rows = 1000000
max_expand_depth = 3
tenant_max_concurrent = 10

# Per-role guard overrides — override global defaults for specific roles.
# Any field not specified here falls back to the global [gateway] default.
[gateway.role_overrides.reporting]
max_result_rows = 100000
statement_timeout_ms = 120000
explain_max_cost = 500000.0

[gateway.role_overrides.admin]
max_expand_depth = 5

[gateway.cache]
enabled = true
max_entries = 1000
ttl_secs = 60

[[sync]]
source_table = "products"
trigger_column = "description"
target_collection = "products_search"
embedding_model = "candle:bert-base"
```

## Environment Variable Expansion

Use `${VAR}` syntax inside TOML values to reference environment variables:

| Syntax | Behavior |
|--------|----------|
| `${VAR}` | **Required** — errors if `VAR` is not set |
| `${VAR:-default}` | **Optional** — uses `default` if `VAR` is not set |
| `$$` | Literal `$` character |

Example:
```toml
[postgres]
url = "postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST:-localhost}:5432/mydb"
```

Set `DB_USER` and `DB_PASSWORD` in your `.env` or shell. `DB_HOST` falls back to `localhost` if unset.

## Environment Variable Overrides

These env vars always override their TOML counterparts, regardless of `${VAR}` expansion:

| Env Var | Overrides |
|---------|-----------|
| `DATABASE_URL` | `[postgres].url` |
| `REDIS_URL` | `[redis].host` + `port` |
| `QDRANT_URL` | `[qdrant].url` |
| `QAIL_BIND` | `[gateway].bind` |

This lets you keep one `qail.toml` across dev/staging/prod and switch databases purely via env vars.

## Usage in Rust

### PostgreSQL Pool (one-liner)

```rust
use qail_pg::driver::pool::PgPool;

let pool = PgPool::from_config().await?;
```

### Manual Config Loading

```rust
use qail_core::config::QailConfig;
use qail_pg::driver::pool::PoolConfig;

let qail = QailConfig::load()?;                       // reads ./qail.toml
let pg_config = PoolConfig::from_qail_config(&qail)?;  // parse postgres section
let pool = PgPool::connect(pg_config).await?;
```

### Redis

```rust
use qail_redis::pool::PoolConfig as RedisPoolConfig;

let qail = QailConfig::load()?;
if let Some(config) = RedisPoolConfig::from_qail_config(&qail) {
    let pool = RedisPool::new(config).await?;
}
```

### Qdrant

```rust
use qail_qdrant::pool::PoolConfig as QdrantPoolConfig;

let qail = QailConfig::load()?;
if let Some(config) = QdrantPoolConfig::from_qail_config(&qail) {
    let pool = QdrantPool::new(config).await?;
}
```

### Gateway

```rust
use qail_gateway::config::GatewayConfig;

let qail = QailConfig::load()?;
let gw = GatewayConfig::from_qail_config(&qail);
```

## Section Reference

| Section | Required | Description |
|---------|----------|-------------|
| `[project]` | Yes | Project name, mode (`postgres`/`qdrant`/`hybrid`), schema path |
| `[postgres]` | Yes | Database URL and pool tuning |
| `[postgres.rls]` | No | RLS role names |
| `[redis]` | No | Redis connection settings |
| `[qdrant]` | No | Qdrant REST + gRPC endpoints |
| `[gateway]` | No | HTTP server bind, CORS, cache |
| `[[sync]]` | No | Vector sync rules (hybrid mode) |

## Generated Files

`qail init` creates:
- **`qail.toml`** — project config with commented-out optional sections
- **`.env.example`** — documents all supported env var overrides
