# Qail Multi-Operator & Agent Architecture

> **Status**: Schema & Migrations Ready — RLS Enforcement via qail-pg (In Design) | **Last Updated**: 2026-02-07

---

## Vision

Qail is a **Wix/Shopify-style SaaS platform** for ferry/charter booking. Each operator gets a complete suite:
- Booking engine
- WABA (WhatsApp Business)
- Payment gateway integration
- SEO controls
- Agent management

---

## Hierarchy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ LEVEL 0: QAIL PLATFORM (engine.qail.io)                                     │
│ master@qail.io - Administrator                                              │
│ • Full access to ALL data, bypasses RLS                                     │
│ • FinanceAdmin - read-only all transactions                                 │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
        ┌─────────────┴─────────────┐
        ▼                           ▼
┌───────────────────────┐   ┌───────────────────────┐
│ LEVEL 1: OPERATOR     │   │ LEVEL 1: OPERATOR     │
│ Scoot (vessel owner)  │   │ (Other operators...)  │
│                       │   │                       │
│ • SuperAdmin          │   │ Owns:                 │
│ • Admin               │   │ • Inventory           │
│                       │   │ • Vessels             │
│ Contract ───────────────► │ • Pricing             │
└───────────┬───────────┘   │ • WABA                │
            │               └───────────────────────┘
            ▼
┌───────────────────────┐
│ LEVEL 2: AGENT        │
│ ExampleApp (reseller)    │
│ • 10% commission      │
│ • Public site         │
│   example.com         │
└───────────────────────┘
```

---

## RLS Enforcement Strategy: Defense-in-Depth

> **Core Principle**: Engine code stays clean. RLS complexity lives in qail-pg.

### Layer 1: PostgreSQL RLS Policies (Vault Door) ✅ Written

Database-level `CREATE POLICY` on 28+ tables enforcing `operator_id = current_setting('app.current_operator_id')`. This is the **last line of defense** — even raw SQL or direct `psql` access respects these policies.

**Status**: 16 migration files written, not yet applied to staging.

### Layer 2: qail-pg Driver-Level RLS (Front Door) 🔵 To Build

RLS context is set **inside the driver**, not in application code. Engine repos call `driver.fetch_all()` — they don't know or care about RLS.

```rust
// In qail-pg — driver handles everything
impl PgDriver {
    /// Set RLS context on this connection.
    /// All subsequent queries will be scoped to this operator.
    pub async fn set_rls_context(&mut self, ctx: RlsContext) -> PgResult<()> {
        self.execute_raw("SELECT set_config('app.current_operator_id', $1, false)", &[&ctx.operator_id]).await?;
        self.execute_raw("SELECT set_config('app.current_agent_id', $1, false)", &[&ctx.agent_id]).await?;
        self.execute_raw("SELECT set_config('app.is_super_admin', $1, false)", &[&ctx.is_super_admin.to_string()]).await?;
        self.rls_context = Some(ctx);
        Ok(())
    }
}

impl PgPool {
    /// Acquire a connection with RLS context pre-configured.
    /// The returned connection is scoped to the given operator.
    pub async fn acquire_with_rls(&self, ctx: RlsContext) -> PgResult<PooledConnection> {
        let mut conn = self.acquire().await?;
        conn.set_rls_context(ctx).await?;
        Ok(conn)
    }
}
```

**Why this is bulletproof**: `PgDriver` takes `&mut self` for every query. Rust's borrow checker guarantees exclusive access. `set_config` and queries are physically on the same connection — no pool race condition possible.

### Layer 3: AST-Level Query Injection (Nuclear Option) ⚪ Future

Build `operator_id` filtering directly into qail-core's query transpiler:

```rust
// Future: every Qail::get() auto-injects WHERE operator_id = $current
let query = Qail::get("orders").filter_cond(eq("status", "confirmed"));
// Transpiles to: SELECT * FROM orders WHERE status = 'confirmed' AND operator_id = $current_operator
```

This is the **Hasura approach** — data isolation at the query builder level. Makes PostgreSQL RLS policies a safety net rather than the primary mechanism.

### How It All Fits Together

```
Request → Middleware → set_rls_context() on PgDriver
                            ↓
                       PgDriver (qail-pg)
                       • &mut self = one connection
                       • set_config already called
                       • All queries scoped
                            ↓
                       PostgreSQL
                       • RLS policies = backup check
                       • Even if driver bug, DB blocks it
```

**Engine code has ZERO RLS logic.** Repos just call `fetch_all()`.

---

## Implementation Status

### ✅ Schema & Migrations (Written, Not Applied)

16 migration files ready in `migrations/20260206*` – `20260207*`:

| Migration | Purpose |
|-----------|---------|
| `000001_enable_rls_policies` | RLS on orders, users |
| `000002_extend_rls_coverage` | Odyssey, WABA, Promotions, Email Templates |
| `000003_extend_rls_phase3` | Odyssey chain, Charters, WhatsApp, AI |
| `000004_user_permissions` | Configurable Admin permissions table |
| `000005_agents_and_contracts` | Agents, agent_contracts tables |
| `000006_promotion_apply_to_agents` | Agent parity for promotions |
| `000007_fix_users_rls_for_auth` | Fix auth blocking from users RLS |
| `000008_user_role_hierarchy` | Multi-tenant user roles |
| `000009_standardize_vendor_to_operator` | Rename vendor_id → operator_id |
| `20260207_000001_saas_content_isolation` | Blog, SEO, Marketing scoping |
| `000002_pricing_engine_settlements` | Commission tracking, settlements |
| `000003_remaining_table_scoping` | payment_sessions, holds, broadcasts |
| `000004_analytics_logging_isolation` | Search stats, webhook logs |
| `000005_assign_scoot_operator` | Test data for Scoot operator |
| `000006_create_app_user_role` | Non-superuser `app_user` role |
| `000007_demote_app_user` | Demote `sailtix` from superuser |

### ✅ Domain Models (Written, Not Committed)

| Module | Files | Status |
|--------|-------|--------|
| Agent domain | `src/domain/agent/mod.rs` | ✅ Written, untracked |
| Settlement domain | `src/domain/settlement/mod.rs` | ✅ Written, untracked |
| Agent repository | `src/repository/agent/postgres_impl.rs` | ✅ Written, untracked |
| Settlement repository | `src/repository/settlement/postgres_impl.rs` | ✅ Written, untracked |
| Agent API handlers | `src/api/agent/handlers.rs` | ✅ Written, untracked |
| Settlement API handlers | `src/api/settlement/handlers.rs` | ✅ Written, untracked |
| Agent router | `src/api/routers/agent.rs` | ✅ Written, untracked |

### ❌ Not Started

| Item | Priority | Notes |
|------|----------|-------|
| **qail-pg `RlsContext`** | 🔴 High | Core of driver-level RLS |
| **qail-pg `set_rls_context()`** | 🔴 High | Sets session vars on connection |
| **qail-pg `acquire_with_rls()`** | 🔴 High | Pool-level RLS acquisition |
| **Engine middleware integration** | 🔴 High | Calls `set_rls_context()` per request |
| **Apply migrations to staging** | 🟡 Medium | 16 migration files ready |
| **Repo migration to qail-pg** | 🟡 Medium | Incremental, start with payment |
| **Cloudflare custom domains** | 🟢 Low | Phase 6 |
| **AST-level injection** | 🟢 Low | Layer 3, future |

---

## Database Schema

### 💎 Database Scoping Strategy (SaaS Isolation)

For "SaaS Features" (Blog, Chat, SEO, Marketing) that belong to the **site owner**, we use a **Tenant Isolation** pattern:

**Pattern: Dual Nullable Foreign Keys**
```sql
ALTER TABLE articles ADD COLUMN operator_id UUID REFERENCES operators(id);
ALTER TABLE articles ADD COLUMN agent_id UUID REFERENCES agents(id);
ALTER TABLE articles ADD CONSTRAINT check_owner 
  CHECK (
    (operator_id IS NOT NULL AND agent_id IS NULL) OR 
    (operator_id IS NULL AND agent_id IS NOT NULL)
  );
```
*Why?* Strong foreign key integrity + simpler RLS than polymorphic types.

| Feature | Scope |
|:---|:---|
| **Inventory/Vessels** | Always `operator_id` (Agents can't own boats) |
| **Orders** | Both `operator_id` (vendor) AND `agent_id` (seller) |
| **Blog/SEO/Chat** | **Tenant Scoped** (Either Operator OR Agent) |

### Core Tables

```sql
-- Agents (resellers/OTAs)
CREATE TABLE agents (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL,
    contact_email VARCHAR(255),
    api_key_hash VARCHAR(255),  -- For API access
    is_active BOOLEAN DEFAULT true
);

-- Agent-Operator Contracts
CREATE TABLE agent_contracts (
    agent_id UUID REFERENCES agents(id),
    operator_id UUID REFERENCES operators(id),
    pricing_model VARCHAR(20),  -- 'commission' | 'static_markup' | 'net_rate'
    commission_percent DECIMAL(5,2),
    static_markup DECIMAL(10,2),
    is_active BOOLEAN DEFAULT true,
    UNIQUE(agent_id, operator_id)
);

-- Orders track both operator and agent
ALTER TABLE orders ADD COLUMN operator_id UUID;
ALTER TABLE orders ADD COLUMN agent_id UUID REFERENCES agents(id);
```

### RLS-Scoped Tables (28+)

| Module | Tables | operator_id | RLS Policy |
|--------|--------|:-----------:|:----------:|
| **Users** | users | ✅ | ✅ 4 policies |
| **Odysseys** | odysseys, odyssey_pricing_* | ✅ | ✅ |
| **Vessels** | vessels | ✅ | ✅ |
| **Pricing** | pricing_plans, tiers, fees | ✅ | ✅ |
| **WABA** | waba_contacts, messages, sessions | ✅ | ✅ |
| **Payment** | payment_gateways, sessions | ✅ | ✅ |
| **Promotions** | promotions | ✅ | ✅ |
| **Email** | email_templates | ✅ | ✅ |
| **Orders** | orders | ✅ | ✅ |
| **Charters** | 5 charter tables | ✅ | ✅ |
| **Agents** | agent_contracts | ✅ | ✅ |
| **Blog/SEO** | articles, seo_poros | ✅ dual | ✅ |
| **Marketing** | marketing_config | ✅ dual | ✅ |
| **Settlements** | settlements, settlement_items | ✅ | ✅ |
| **Analytics** | search_stats, webhook_logs | ✅ dual | ✅ |

### 🟢 Shared Global (No RLS)

| Table | Reason |
|-------|--------|
| **users** (auth) | Single identity across platform |
| **user_payment_methods** | "Shop Pay" style |
| **saved_passengers** | CRM |
| **reservations** | User history |

### ⚪ Platform-Only (Admin Access)

Harbors, Destinations, SSL Certificates, System Settings

---

## User Roles

| Role | operator_id | Access | RLS Behavior |
|------|:-----------:|--------|--------------:|
| Administrator | NULL | ALL | Bypasses RLS |
| FinanceAdmin | NULL | All payments (R/O) | Bypasses RLS |
| SuperAdmin | UUID | Own operator | operator_id = current |
| Admin | UUID | Configurable | Same + permissions |
| Customer | UUID | Own bookings | user_id = current |

### Test Users (Staging)

| Email | Role | Operator |
|-------|------|----------|
| master@qail.io | Administrator | Platform |
| scootsuperadmin@qail.io | SuperAdmin | Scoot |
| scootadmin@qail.io | Admin | Scoot |
| hello@example.com | SuperAdmin | ExampleApp |

---

## Payment Flow

### Agent Commission (Auto-Split via Gateway)

```
Customer pays $100 → Qail Gateway (Xendit/Doku)
                          │
                          ├─ $90 → Operator sub-account
                          └─ $10 → Agent sub-account (10% commission)
```

---

## Execution Roadmap

### Phase A: qail-pg Driver-Level RLS 🔴 Next
*Build RLS into qail-pg so engine code stays clean.*

- [ ] Add `RlsContext` struct to `qail-pg/src/driver/`
- [ ] Add `set_rls_context()` to `PgDriver`
- [ ] Add `acquire_with_rls()` to `PgPool`
- [ ] Add `after_acquire` hook for safe defaults on pool connections
- [ ] Integration tests: cross-tenant query blocked

### Phase B: Apply Migrations & Wire Up 🟡 After A
*Deploy the 16 migration files and connect middleware.*

- [ ] Apply migrations to staging via `sqlx migrate run`
- [ ] Demote `sailtix` role from superuser
- [ ] Update engine middleware to call `set_rls_context()`
- [ ] Commit agent/settlement domain + repos + handlers
- [ ] Verify with `test_rls.sh`

### Phase C: Incremental Repo Migration 🟡 Ongoing
*Migrate sqlx repos to qail-pg one module at a time.*

Priority order (by data sensitivity):
1. Payment repos → qail-pg
2. Order repos → qail-pg
3. Vessel/Odyssey repos → qail-pg
4. WABA repos → already qail-pg ✅
5. Blog/SEO/Marketing repos → qail-pg
6. Remaining repos → qail-pg

### Phase D: Cloudflare Custom Domains 🟢 Future

- [ ] Cloudflare for SaaS setup
- [ ] `custom_domains` table + migration
- [ ] Domain verification flow
- [ ] Next.js middleware for domain routing

### Phase E: AST-Level Query Injection 🟢 Future

- [ ] qail-core transpiler auto-injects `WHERE operator_id = $current`
- [ ] Makes PostgreSQL RLS policies optional backup
- [ ] Full Hasura-style data isolation in code

---

## Custom Domain Architecture (Cloudflare for SaaS)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ CLOUDFLARE FOR SAAS                                                         │
│                                                                             │
│  Customer's Domain          Cloudflare Edge           Qail Platform         │
│  ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐   │
│  │ scoot.travel    │──────►│ SSL termination │──────►│ *.qail.io       │   │
│  │ book.example.com│       │ CNAME flattening│       │ Next.js app     │   │
│  │ ferry.agent.id  │       │ Origin routing  │       │ (domain lookup) │   │
│  └─────────────────┘       └─────────────────┘       └─────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Decisions

| Question | Answer |
|----------|--------|
| RLS enforcement | **qail-pg driver** (primary) + PostgreSQL policies (backup) |
| Agent registration | Admin-only (no self-service) |
| Refund handling | Operator initiates |
| Multi-operator agents | Yes, one agent → many operators |
| ExampleApp role | Currently both operator + agent |
| Naming convention | All tables use `operator_id` |
| Custom domains | Via Cloudflare for SaaS |
| Repo migration | Incremental sqlx → qail-pg |

---

## Related Files

- User Roles: `src/domain/auth/auth.rs`
- Permissions: `src/domain/auth/permissions.rs`
- RLS Middleware: `src/api/auth/middleware/rls_middleware.rs` (untracked)
- Agent Domain: `src/domain/agent/mod.rs` (untracked)
- Order Domain: `src/domain/payment/mod.rs`
- Settlement Domain: `src/domain/settlement/mod.rs` (untracked)
- Settlement API: `src/api/settlement/handlers.rs` (untracked)
- Migrations: `migrations/20260206000001` – `20260207000007` (untracked)
- RLS Test: `test_rls.sh` (untracked)
- qail-pg Driver: `/Users/orion/qail.rs/pg/`
- qail-pg Pool: `/Users/orion/qail.rs/pg/src/driver/pool.rs`
