# Typed Codegen

QAIL can generate fully typed Rust modules from your `schema.qail` file, enabling compile-time checked table and column references.

## Setup

### 1. Add build script

Create or update your `build.rs`:

```rust
fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    qail_core::build::generate_typed_schema(
        "schema.qail",
        &format!("{}/schema.rs", out_dir)
    ).unwrap();
    
    println!("cargo:rerun-if-changed=schema.qail");
}
```

### 2. Include generated module

In your `lib.rs` or `main.rs`:

```rust
include!(concat!(env!("OUT_DIR"), "/schema.rs"));
```

## Usage

### Generated Structure

From this schema:
```qail
table users {
    id UUID primary_key
    email TEXT not_null
    age INT
}

table posts {
    id UUID primary_key
    user_id UUID ref:users.id
    title TEXT
}
```

QAIL generates:
```rust
pub mod users {
    pub struct Users;
    impl Table for Users { ... }
    
    pub const table: Users = Users;
    pub const id: TypedColumn<uuid::Uuid> = ...;
    pub const email: TypedColumn<String> = ...;
    pub const age: TypedColumn<i32> = ...;
}

pub mod posts {
    pub struct Posts;
    pub const table: Posts = Posts;
    pub const user_id: TypedColumn<uuid::Uuid> = ...;
}
```

### Using Typed References

```rust
use schema::{users, posts};

// Tables and columns are type-safe
Qail::get(users::table)
    .columns([users::id, users::email])
    .join_on(posts::table)
```

## Type Mapping

| QAIL Type | Rust Type |
|-----------|-----------|
| `UUID` | `uuid::Uuid` |
| `TEXT`, `VARCHAR` | `String` |
| `INT`, `INTEGER` | `i32` |
| `BIGINT` | `i64` |
| `FLOAT`, `REAL` | `f32` |
| `DOUBLE` | `f64` |
| `BOOL` | `bool` |
| `TIMESTAMP` | `chrono::DateTime<Utc>` |
| `JSON`, `JSONB` | `serde_json::Value` |

## Logic-Safe Relations (Scenario B)

QAIL codegen now generates **compile-time relationship checking** using the `RelatedTo<T>` trait.

### How It Works

When schema.qail contains foreign key references:
```qail
table posts {
    user_id UUID ref:users.id
}
```

The codegen produces:
```rust
// Forward: child -> parent
impl RelatedTo<users::Users> for posts::Posts {
    fn join_columns() -> (&'static str, &'static str) { ("user_id", "id") }
}

// Reverse: parent -> children
impl RelatedTo<posts::Posts> for users::Users {
    fn join_columns() -> (&'static str, &'static str) { ("id", "user_id") }
}
```

### Compile-Time Safety

This enables "logic-safe" joins that fail at compile time:

```rust
// ✅ Compiles - tables are related
Qail::get(users::table).join_related(posts::table)

// ❌ Compile Error: "Users: RelatedTo<Products> is not satisfied"
Qail::get(users::table).join_related(products::table)
```

## Data Access Policies (Phase 4)

QAIL now supports **compile-time data governance** using the `protected` keyword.

### Schema Definition

Mark sensitive columns with `protected`:
```qail
table users {
    id UUID primary_key
    email TEXT not_null
    password_hash TEXT protected
    two_factor_secret TEXT protected
}
```

### Generated Types

Protected columns get `TypedColumn<T, Protected>` instead of `TypedColumn<T, Public>`:
```rust
// Public - accessible by default
pub const email: TypedColumn<String, Public> = ...;

// Protected - requires capability witness
pub const password_hash: TypedColumn<String, Protected> = ...;
```

### Policy Hierarchy

| Policy | Description | Use Case |
|--------|-------------|----------|
| `Public` | Default, no restrictions | Normal data |
| `Protected` | Requires `AdminCap` witness | Passwords, secrets |
| `Restricted` | Requires `SystemCap` witness | Audit-critical data |

### Capability Witness API

Access protected columns using the **builder pattern**:

```rust
use qail_core::typed::{CapabilityProvider, WithCap};    

// In your auth middleware (Root of Trust):
let admin_cap = CapabilityProvider::mint_admin();  // After JWT verification

// Build query with typed table reference (no strings!)
let query = Qail::get(users::table)                 // ✓ Typed, not string
    .with_cap(&admin_cap)                           // Prove authorization
    .column(users::email)                           // Public - always allowed
    .column_protected(users::password_hash)         // Protected - now allowed!
    .build();
```

### Root of Trust

> [!IMPORTANT]
> `AdminCap` and `SystemCap` have **sealed constructors** (private fields).
> They can only be minted via `CapabilityProvider::mint_*()`.
> Place this in a single, auditable auth layer.

```rust
// In your AuthService (the ONLY place that can mint capabilities):
impl AuthService {
    pub fn verify_admin(&self, token: &str) -> Result<AdminCap, AuthError> {
        let claims = self.verify_jwt(token)?;
        if claims.role == "admin" {
            Ok(CapabilityProvider::mint_admin())
        } else {
            Err(AuthError::Forbidden)
        }
    }
}
```

### Compile-Time Enforcement

Attempting to access protected columns without capability **fails at compile time**:

```rust
// ❌ Compile Error: Protected: PolicyAllowedBy<NoCap> is not satisfied
Qail::get(users::table)
    .with_cap(&NoCap)
    .column_protected(users::password_hash)
```

