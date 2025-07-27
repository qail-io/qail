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

## 2. Native Versioning (Data Virtualization)
**Goal:** "GitHub for Database" — Branching at the Application Layer.

- [ ] Gateway Middleware with `X-Branch-ID` header
- [ ] Row-Level branching (`WHERE _branch_id = ?`)
- [ ] Copy-on-Write strategy for writes
- [ ] CLI: `qail branch create <name>`, `qail checkout <name>`

---

## 3. Infrastructure-Aware Compiler
**Goal:** Verify external resources at compile time.

- [ ] `schema.qail` extensions: `bucket`, `queue`, `topic`
- [ ] `build.rs` validates resources exist in Terraform/AWS

