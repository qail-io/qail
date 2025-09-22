# QAIL Roadmap 2026: The "Dream" Backend

## 1. First-Class Relations

### Phase 1: Runtime Registry ✅ (v0.15.0)
- [x] `ref:` syntax in `schema.qail`
- [x] `RelationRegistry` for runtime lookup
- [x] `Qail::join_on("table")` — string-based API

### Phase 2: Fully Typed Codegen ✅ (v0.16.0)
**Goal:** Zero strings. Compile-time type-safe tables and columns.

```rust
// Generated from schema.qail by build.rs
mod schema {
    pub struct Users;
    pub struct Posts;
    pub struct UserId;
    pub struct PostUserId;
}

// The Ultimate Dream:
Qail::get(schema::Users)
    .join_on(schema::Posts)
    .column(schema::UserId)
```

#### Implementation
- [x] `build.rs` generates `schema_gen.rs` with typed structs
- [x] Table structs implement `Table` trait
- [x] `TypedColumn<T>` with Rust type mapping
- [x] `Qail::get()` accepts typed table markers — `AsRef<str>` generated on table structs
- [x] `typed_column()` accepts `TypedColumn<T>` — `builders/typed.rs`

### Phase 3: Logic-Safe Relations ✅ (v0.16.0)
**Goal:** Compile-time error if joining unrelated tables.

```rust
// Generated from ref: annotations
impl RelatedTo<posts::Posts> for users::Users {
    fn join_columns() -> (&'static str, &'static str) { ("id", "user_id") }
}

// Compiles ✓
Qail::typed(users::table).join_related(posts::table)

// Compile ERROR: "Users: RelatedTo<Products> is not satisfied"
Qail::typed(users::table).join_related(products::table)
```

#### Implementation
- [x] `RelatedTo<T>` trait in `typed.rs`
- [x] Codegen generates bidirectional `RelatedTo` impls
- [x] `TypedQail<T>` wrapper + `join_related()` with trait bound

### Phase 4: Compile-Time Data Governance 🔒 ✅ (v0.16.0)
**Goal:** Prevent data leaks at the compiler level. Sensitive columns require a "Capability Witness" to select.

```qail
table users {
    username TEXT
    password_hash TEXT protected  // <--- New keyword
}
```

```rust
// ❌ Compile Error: "ProtectedColumn cannot be selected without capability"
Qail::get(users::table).with_cap(&NoCap).column_protected(users::password_hash)

// ✅ Compiles - AdminCap proves authorization
Qail::get(users::table)
    .with_cap(&CapabilityProvider::mint_admin())
    .column_protected(users::password_hash)
```

#### Implementation
- [x] `protected` keyword in `schema.qail`
- [x] `TypedColumn<T, P>` with `Policy` generic
- [x] `Public` and `Protected` marker traits
- [x] `CapQuery::column_protected()` with `PolicyAllowedBy<C>` check
- [x] Compile-time failure verified via type system (`test_cap_query_builder`)

---

## 2. SaaS Multi-Tenant Isolation (RLS)

**Goal:** Driver-level data isolation for multi-operator SaaS. Application code has ZERO RLS awareness — the driver owns all isolation complexity.

> **Architecture:**
> ```
> Request → Middleware → set_rls_context() on PgDriver
>                             ↓
>                        PgDriver (qail-pg)
>                        • &mut self = one connection
>                        • set_config already called
>                        • All queries scoped
>                             ↓
>                        PostgreSQL
>                        • RLS policies = backup check
>                        • Defense-in-depth
> ```

### Phase 1: Driver-Level Context ✅ (v0.14.21)

**Goal:** `PgDriver` can set PostgreSQL session variables for RLS enforcement.

#### Implementation
- [x] `RlsContext` struct in `core/src/rls.rs` — operator_id, agent_id, is_super_admin
- [x] `PgDriver.set_rls_context(ctx)` — calls `execute_raw("SELECT set_config(...)")`
- [x] `PgDriver.clear_rls_context()` — resets to safe defaults
- [x] `PgDriver.rls_context()` — getter, returns `Option<&RlsContext>`
- [x] Unit tests pass

### Phase 2: Pool-Level RLS Acquisition ✅ (v0.15.6)

**Goal:** Pooled connections auto-clear RLS context on return. No stale tenant leaks.

#### Implementation
- [x] `PgPool.acquire_with_rls(ctx)` — acquire + set context in one call
- [x] `PooledConnection` auto-clears RLS context on `Drop` via `rls_dirty` flag
- [x] Unit tests pass (53/53 qail-pg)

### Phase 3: Policy Definition API ✅ (v0.15.6)

**Goal:** Define RLS policies programmatically using the QAIL AST, not raw SQL.

```rust
let policy = RlsPolicy::new("orders_isolation", "orders")
    .for_all()
    .using(tenant_check("operator_id", "app.current_operator_id", "uuid"));
let sql = rls_setup_sql(&policy); // ENABLE + FORCE + CREATE POLICY
```

#### Implementation
- [x] `RlsPolicy` builder in `core/src/migrate/policy.rs`
- [x] `AlterOp::ForceRowLevelSecurity` + `force_rls()` / `no_force_rls()` builders
- [x] SQL transpiler in `core/src/transpiler/policy.rs`
- [x] `rls_setup_sql()` convenience — ENABLE + FORCE + CREATE POLICY in one call
- [x] Supports `FOR SELECT`, `FOR INSERT`, `FOR UPDATE`, `FOR DELETE`, `FOR ALL`

### Phase 4: AST-Level Query Injection ✅ (v0.15.6)

**Goal:** Query builder auto-injects `WHERE operator_id = $current` into every query. Data isolation at the AST level — PostgreSQL RLS policies become a backup safety net.

```rust
// Developer writes:
let ctx = RlsContext::operator("op-uuid");
Qail::get("orders").filter("status", Operator::Eq, "active").with_rls(&ctx)
// → SELECT * FROM orders WHERE status = 'active' AND operator_id = 'op-uuid'

Qail::add("orders").set_value("total", 100).with_rls(&ctx)
// → INSERT INTO orders (total, operator_id) VALUES (100, 'op-uuid')
```

#### Implementation
- [x] `TenantRegistry` + `TENANT_TABLES` global in `core/src/rls/tenant.rs`
- [x] `register_tenant_table()`, `lookup_tenant_column()`, `load_tenant_tables()`
- [x] Auto-detect via `from_build_schema()` — tables with `operator_id` auto-register
- [x] `Qail::with_rls(ctx)` — GET/SET/DEL → filter, ADD/Upsert → payload
- [x] Super admins + unregistered tables bypass injection
- [x] 13 unit tests pass

---

## 3. Native Versioning (Data Virtualization)
**Goal:** "GitHub for Database" — Branching at the Application Layer.

- [ ] Gateway Middleware with `X-Branch-ID` header
- [ ] Row-Level branching (`WHERE _branch_id = ?`)
- [ ] Copy-on-Write strategy for writes
- [ ] CLI: `qail branch create <name>`, `qail checkout <name>`

---

## 4. Infrastructure-Aware Compiler
**Goal:** Verify external resources at compile time.

- [ ] `schema.qail` extensions: `bucket`, `queue`, `topic`
- [ ] `build.rs` validates resources exist in Terraform/AWS

---

## Current Status (Feb 7, 2026)

### SaaS Isolation (RLS) — Full Stack Complete ✅ v0.15.6
- [x] **Phase 1:** `PgDriver::set_rls_context()` — driver-level session variables
- [x] **Phase 2:** `PooledConnection` auto-clears RLS on Drop — no stale tenant leaks
- [x] **Phase 3:** `RlsPolicy` builder + SQL transpiler — AST-native policy creation
- [x] **Phase 4:** `Qail::with_rls(ctx)` — AST-level tenant injection (primary mechanism)
- [x] **Repository Migration:** `QailOrderRepository` and `User/Customer` handlers migrated
- [x] **Staging Verified:** `users`, `customers`, `orders` endpoints confirmed
- [ ] **Migration Pending:** Move 'ExampleApp' from Operator role to Agent role (DB migration)