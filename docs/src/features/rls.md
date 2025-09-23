# Row-Level Security (RLS)

> **New in v0.15.6** — The first Rust PostgreSQL driver with built-in multi-tenant data isolation

QAIL injects tenant context at the **AST level**, ensuring every query is automatically scoped to the correct operator, agent, or user — without manual `WHERE` clauses.

## The Problem

Every multi-tenant SaaS app needs data isolation. Traditional solutions are fragile:

```rust
// ❌ Manual WHERE clauses — easy to forget, impossible to audit
let sql = "SELECT * FROM bookings WHERE operator_id = $1";

// ❌ Every query must remember to add the filter
let sql = "SELECT * FROM invoices"; // BUG: leaks ALL operator data
```

One missed `WHERE` clause = cross-tenant data leak. In a codebase with 200+ queries, this is a ticking time bomb.

## The Solution: AST-Level RLS

QAIL solves this at the driver level:

```rust
use qail_core::rls::RlsContext;

// Create context from authenticated session
let ctx = RlsContext::operator(operator_id);

// Every query is automatically scoped
let query = Qail::get("bookings")
    .columns(["id", "customer", "status"])
    .with_rls(&ctx);  // ← RLS injected at AST level

// Generated SQL: SELECT ... FROM bookings
// But the connection has: SET app.operator_id = '<uuid>'
// PostgreSQL RLS policy handles the rest
```

## RlsContext Constructors

| Constructor | Scope | Use Case |
|-------------|-------|----------|
| `RlsContext::operator(id)` | Single operator | Operator dashboard |
| `RlsContext::agent(id)` | Single agent | Agent portal |
| `RlsContext::operator_and_agent(op, ag)` | Both | Agent within operator |
| `RlsContext::super_admin()` | Bypasses RLS | Platform admin |

## Query Methods

```rust
let ctx = RlsContext::operator_and_agent(op_id, agent_id);

ctx.has_operator();   // true
ctx.has_agent();      // true
ctx.bypasses_rls();   // false

let admin = RlsContext::super_admin();
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
│  ├─ SET app.operator_id = '<uuid>'               │
│  ├─ SET app.agent_id = '<uuid>'                  │
│  └─ Execute query on SAME connection             │
│       ↓                                           │
│  PostgreSQL RLS Policy                            │
│  CREATE POLICY tenant_isolation ON bookings       │
│    USING (operator_id = current_setting(          │
│           'app.operator_id')::uuid)              │
│       ↓                                           │
│  Only matching rows returned                      │
└─────────────────────────────────────────────────┘
```

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
