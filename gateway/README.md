# 🌐 QAIL Gateway

> **The Native Data Layer — Replace REST/GraphQL with Binary AST Protocol**

## Vision

```
┌─────────────────────────────────────────────────────┐
│  Client (Web/Mobile/CLI)                            │
│  └── qail-js / qail-swift / qail-rs                 │
├─────────────────────────────────────────────────────┤
│  QAIL Gateway (this crate)                          │
│  ├── HTTP/WebSocket endpoint                        │
│  ├── AST validation against schema.qail             │
│  ├── Row-level security policies                    │
│  └── Direct Postgres binary protocol                │
├─────────────────────────────────────────────────────┤
│  PostgreSQL / Qdrant / Redis                        │
└─────────────────────────────────────────────────────┘
```

## Status: 🚧 Draft

This crate is in early design phase. See `DESIGN.md` for architecture decisions.

## Key Differentiators

| Feature | REST | GraphQL | QAIL Gateway |
|---------|------|---------|--------------|
| Wire format | JSON | JSON | Binary AST |
| Latency | ~10ms | ~10ms | ~1ms |
| Client SDK | - | 50KB+ | ~5KB |
| Row security | Manual | Manual | Built-in |

## Architectural Decisions

1. **Binary Format**: Custom QAIL AST (native), with FlatBuffers export later
2. **Subscriptions**: Hybrid (LISTEN/NOTIFY → Redis Pub/Sub → WebSocket)
3. **Federation**: Explicit steps with prefixes (`postgres.`, `qdrant.`, `redis.`)

## Usage (Future)

```rust
use qail_gateway::Gateway;

#[tokio::main]
async fn main() {
    let gateway = Gateway::builder()
        .schema("schema.qail")
        .database("postgres://localhost/mydb")
        .policy("policies.qail")
        .build()
        .await?;

    gateway.serve("0.0.0.0:8080").await?;
}
```

## Client Retry Guidance (503 Backpressure)

When the database acquire queue is saturated, gateway may return:

- HTTP `503 Service Unavailable`
- JSON error code: `POOL_BACKPRESSURE`
- Headers:
  - `Retry-After` (seconds)
  - `X-Qail-Backpressure-Scope` (`global`, `tenant`, `tenant_map`, `unknown`)
  - `X-Qail-Backpressure-Reason` (`global_waiters_exceeded`, `tenant_waiters_exceeded`, `tenant_tracker_saturated`, `queue_saturated`)

Recommended retry policy:

1. Read `Retry-After` and treat it as a floor delay.
2. Apply exponential backoff with full jitter.
3. Cap max delay and retry budget to avoid infinite retry storms.

Example (full-jitter):

```text
base_ms = max(retry_after_seconds * 1000, 200)
cap_ms = 30000
exp_ms = min(base_ms * (2^attempt), cap_ms)
sleep_ms = random(0, exp_ms)
```

JavaScript (fetch):

```js
async function qailRequestWithBackoff(url, init = {}, maxRetries = 6) {
  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    const res = await fetch(url, init);
    if (res.status !== 503) return res;

    let body = null;
    try { body = await res.json(); } catch {}
    if (body?.code !== "POOL_BACKPRESSURE") return res;

    if (attempt === maxRetries) return res;

    const retryAfterSec = Number(res.headers.get("retry-after") || "0");
    const scope = res.headers.get("x-qail-backpressure-scope") || "unknown";
    const reason = res.headers.get("x-qail-backpressure-reason") || "queue_saturated";
    console.warn("qail backpressure", { scope, reason, attempt });

    const baseMs = Math.max(retryAfterSec * 1000, 200);
    const capMs = 30_000;
    const expMs = Math.min(baseMs * (2 ** attempt), capMs);
    const sleepMs = Math.floor(Math.random() * (expMs + 1)); // full jitter
    await new Promise((r) => setTimeout(r, sleepMs));
  }
  throw new Error("unreachable");
}
```

Rust (reqwest + tokio):

```rust
use reqwest::Client;
use serde_json::Value;
use tokio::time::{Duration, sleep};

pub async fn qail_json_with_backoff(
    client: &Client,
    make_req: impl Fn() -> reqwest::RequestBuilder,
    max_retries: u32,
) -> reqwest::Result<Value> {
    for attempt in 0..=max_retries {
        let res = make_req().send().await?;
        let status = res.status();
        let headers = res.headers().clone();
        let body: Value = res.json().await.unwrap_or(Value::Null);
        if status != 503 {
            return Ok(body);
        }

        if body.get("code").and_then(|v| v.as_str()) != Some("POOL_BACKPRESSURE") {
            // Non-backpressure 503: let caller decide.
            return Ok(body);
        }

        if attempt == max_retries {
            return Ok(body);
        }

        let retry_after_sec = headers
            .get("retry-after")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let base_ms = (retry_after_sec * 1000).max(200);
        let cap_ms = 30_000u64;
        let multiplier = 1u64.checked_shl(attempt.min(20)).unwrap_or(u64::MAX);
        let exp_ms = base_ms.saturating_mul(multiplier).min(cap_ms);
        let sleep_ms = rand::random::<u64>() % (exp_ms + 1); // full jitter
        sleep(Duration::from_millis(sleep_ms)).await;
    }
    unreachable!()
}
```

## Security Policies (Future)

```qail
# In policies.qail
policy users_own_data {
  on: users
  filter: id = $auth.user_id
  allow: read, update
}

policy admin_full_access {
  on: *
  when: $auth.role = 'admin'
  allow: *
}
```

## Roadmap

- [ ] Phase 1: HTTP endpoint accepting QAIL text
- [ ] Phase 2: Binary wire protocol
- [ ] Phase 3: Row-level security
- [ ] Phase 4: WebSocket subscriptions
- [ ] Phase 5: Client SDKs

---

*Long-term vision: 2026-2027*
