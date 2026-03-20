# Row-Level Security (RLS)

> **New in v0.15** — The first Rust PostgreSQL driver with built-in multi-tenant data isolation

QAIL injects tenant context at the **AST level**, ensuring every query is automatically scoped to the correct tenant, agent, or user — without manual `WHERE` clauses.

## The Problem

Every multi-tenant SaaS app needs data isolation. Traditional solutions are fragile:

```rust
// ❌ Manual WHERE clauses — easy to forget, impossible to audit
let sql = "SELECT * FROM bookings WHERE tenant_id = $1";

// ❌ Every query must remember to add the filter
let sql = "SELECT * FROM invoices"; // BUG: leaks ALL tenant data
```

One missed `WHERE` clause = cross-tenant data leak. In a codebase with 200+ queries, this is a ticking time bomb.

## The Solution: AST-Level RLS

QAIL solves this at the driver level:

```rust
use qail_core::rls::{RlsContext, SuperAdminToken};

// Create context from authenticated session
let ctx = RlsContext::tenant(tenant_id);

// Every query is automatically scoped
let query = Qail::get("bookings")
    .columns(["id", "customer", "status"])
    .with_rls(&ctx);  // ← RLS injected at AST level

// Generated SQL: SELECT ... FROM bookings
// But the connection sets app.current_tenant_id/app.current_agent_id
// PostgreSQL RLS policy handles the rest
```

## RlsContext Constructors

| Constructor | Scope | Use Case |
|-------------|-------|----------|
| `RlsContext::tenant(id)` | Single tenant | Tenant dashboard |
| `RlsContext::agent(id)` | Single agent | Agent portal |
| `RlsContext::tenant_and_agent(t, ag)` | Both | Agent within tenant |
| `RlsContext::operator(id)` | Legacy alias | Backward compatibility |
| `RlsContext::global()` | Shared/global rows (`tenant_id IS NULL`) | Public/reference data |
| `RlsContext::super_admin(token)` | Bypasses RLS | Internal platform-only ops |

## Query Methods

```rust
let ctx = RlsContext::tenant_and_agent(tenant_id, agent_id);

ctx.has_tenant();     // true
ctx.has_agent();      // true
ctx.bypasses_rls();   // false

let global = RlsContext::global();
global.is_global();   // true
global.bypasses_rls();// false

let token = SuperAdminToken::for_system_process("admin_task");
let admin = RlsContext::super_admin(token);
admin.bypasses_rls(); // true
```

## How It Works

```
┌─────────────────────────────────────────────────┐
│  Application Code                                │
│                                                   │
│  Qail::get("bookings").with_rls(&ctx)            │
│       ↓                                           │
│  AST Builder adds RLS context to query            │
│       ↓                                           │
│  PgDriver::execute()                              │
│  ├─ set_config('app.current_tenant_id', '<uuid>', true)    │
│  ├─ set_config('app.current_operator_id', '<uuid>', true)  │
│  ├─ set_config('app.current_agent_id', '<uuid>', true)     │
│  └─ Execute query on SAME connection                       │
│       ↓                                           │
│  PostgreSQL RLS Policy                            │
│  CREATE POLICY tenant_isolation ON bookings       │
│    USING (tenant_id = current_setting(            │
│           'app.current_tenant_id')::uuid)         │
│       ↓                                           │
│  Only matching rows returned                      │
└─────────────────────────────────────────────────┘
```

> Compatibility note: gateway/driver still writes legacy operator GUCs and accepts `operator_id` in JWTs while tenant-first naming is rolled out.

## Why AST-Level?

| Approach | Reliability | Audit | Performance |
|----------|-------------|-------|-------------|
| Manual `WHERE` | ❌ Easy to forget | ❌ Grep every query | ✅ Fast |
| ORM middleware | ⚠️ Can be bypassed | ⚠️ Framework-specific | ⚠️ Overhead |
| **QAIL RLS** | ✅ Structural | ✅ Single entry point | ✅ Native PG |

QAIL's approach is **structural** — the RLS context is part of the query pipeline, not an afterthought bolted onto SQL strings.

## Combined with TypedQail

RLS works with the typed API too:

```rust
use schema::{bookings, users};

let query = Qail::typed(bookings::table)
    .join_related(users::table)       // Compile-time safe join
    .typed_column(bookings::id())
    .typed_column(bookings::status())
    .with_rls(&ctx)                   // Multi-tenant isolation
    .build();
```

## Comparison with Other Drivers

| Feature | QAIL | sqlx | Diesel | SeaORM |
|---------|------|------|--------|--------|
| Built-in RLS | ✅ | ❌ | ❌ | ❌ |
| AST-level injection | ✅ | N/A | N/A | N/A |
| `with_rls()` API | ✅ | N/A | N/A | N/A |
| Session variable management | ✅ Auto | Manual | Manual | Manual |
| Connection-scoped context | ✅ | Manual | Manual | Manual |

> **QAIL is the only Rust PostgreSQL driver with built-in Row-Level Security support.**
