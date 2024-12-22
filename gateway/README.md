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
