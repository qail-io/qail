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
- [ ] `Qail::get()` accepts typed table marker (Optional lift)
- [ ] `columns()` accepts typed column markers (Optional lift)

### Phase 3: Logic-Safe Relations ✅ (v0.16.0)
**Goal:** Compile-time error if joining unrelated tables.

```rust
// Generated from ref: annotations
impl RelatedTo<posts::Posts> for users::Users {
    fn join_columns() -> (&'static str, &'static str) { ("id", "user_id") }
}

// Compiles ✓
Qail::get(users::table).join_related(posts::table)

// Compile ERROR: "Users: RelatedTo<Products> is not satisfied"
Qail::get(users::table).join_related(products::table)
```

#### Implementation
- [x] `RelatedTo<T>` trait in `typed.rs`
- [x] Codegen generates bidirectional `RelatedTo` impls
- [ ] Add `join_related<T>()` method with trait bound

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
Qail::get(users::table).column(users::password_hash)

// ✅ Compiles - AdminCap proves authorization
Qail::get(users::table)
    .column_secured(users::password_hash, AdminCap)
```

#### Implementation
- [x] `protected` keyword in `schema.qail`
- [x] `TypedColumn<T, P>` with `Policy` generic
- [x] `Public` and `Protected` marker traits
- [ ] Implement `column_secured()` with capability check
- [ ] Verify compile-time failure for unauthorized access

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

### Phase 1: Driver-Level Context 🔴 Next

**Goal:** `PgDriver` can set PostgreSQL session variables for RLS enforcement. `&mut self` guarantees `set_config` and queries run on the same connection.

**Building blocks already present:**
- `PgDriver.execute_raw(sql)` ✅ (v0.14.9)
- `PgDriver` takes `&mut self` ✅ (borrow checker = same-connection guarantee)
- `PgPool.acquire()` ✅ (returns `PooledConnection`)

#### Implementation
- [ ] `RlsContext` struct in `pg/src/driver/rls.rs`
  ```rust
  pub struct RlsContext {
      pub operator_id: String,
      pub agent_id: String,
      pub is_super_admin: bool,
  }
  ```
- [ ] `PgDriver.set_rls_context(ctx)` — calls `execute_raw("SELECT set_config(...)")`
- [ ] `PgDriver.clear_rls_context()` — resets to safe defaults
- [ ] `PgDriver.rls_context()` — getter, returns `Option<&RlsContext>`
- [ ] Unit test: set context → query → verify scoped results

### Phase 2: Pool-Level RLS Acquisition

**Goal:** Get a pooled connection pre-configured with tenant context. Connection returns to pool with context cleared.

#### Implementation
- [ ] `PgPool.acquire_with_rls(ctx)` — acquire + set context in one call
- [ ] `PooledConnection` auto-clears RLS context on `Drop` (defense-in-depth)
- [ ] `PgPool.after_connect` hook — set safe defaults on every new connection
  ```rust
  // Every new pool connection starts with: is_super_admin=false, operator_id=''
  // This prevents stale context from leaking between requests
  ```
- [ ] Integration test: concurrent requests with different operator_ids

### Phase 3: Policy Definition API (qail-core)

**Goal:** Define RLS policies programmatically using the QAIL AST, not raw SQL.

```rust
// qail-core migration API
Migration::new("enable_orders_rls")
    .alter("orders", |t| t.enable_rls())
    .create_policy("orders_operator_isolation")
        .on("orders")
        .for_all()  // SELECT, INSERT, UPDATE, DELETE
        .using("operator_id = current_setting('app.current_operator_id')::uuid")
        .with_check("operator_id = current_setting('app.current_operator_id')::uuid")
```

#### Implementation
- [ ] `CreatePolicy` builder in `core/src/migrate/`
- [ ] `DropPolicy` builder
- [ ] `AlterPolicy` builder
- [ ] SQL transpiler for `CREATE POLICY ... ON ... USING (...) WITH CHECK (...)`
- [ ] Support `FOR SELECT`, `FOR INSERT`, `FOR UPDATE`, `FOR DELETE`, `FOR ALL`

### Phase 4: AST-Level Query Injection (Nuclear Option) ⚪ Future

**Goal:** Query builder auto-injects `WHERE operator_id = $current` into every query. Data isolation at the AST level — no PostgreSQL RLS policies needed.

```rust
// Developer writes:
Qail::get("orders").filter_cond(eq("status", "confirmed"))

// qail-core transparently transpiles to:
// SELECT * FROM orders WHERE status = 'confirmed' AND operator_id = $current_operator
```

This is the **Hasura approach**. Makes PostgreSQL RLS policies a backup rather than primary mechanism.

#### Implementation
- [ ] `Qail::with_tenant_scope(operator_id)` — injects filter into AST before encoding
- [ ] Schema-aware: only inject on tables that have `operator_id` column
- [ ] `schema.qail` syntax: `table orders { operator_id UUID tenant }` — `tenant` keyword
- [ ] Bypass for platform admin queries

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
