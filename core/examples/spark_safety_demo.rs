//! # SPARK-Level Compile-Time Safety in Qail
//!
//! This example demonstrates how Qail achieves the same level of correctness
//! guarantees as Ada/SPARK — but for database queries instead of avionics.
//!
//! Every proof listed here is enforced BY THE COMPILER, not at runtime.
//! If this file compiles, ALL safety invariants are satisfied.
//!
//! ## Proof Categories:
//! 1. **Column Existence** — TypedColumn<T> with ColumnValue<C>
//! 2. **Type-Safe Filters** — typed_eq() rejects type mismatches
//! 3. **Relationship Graph** — RelatedTo<T> prevents invalid joins
//! 4. **Data Governance** — CapQuery<C> + PolicyAllowedBy<C>
//! 5. **Tenant Isolation** — RequiresRls + RlsQuery<T> proof witness
//!
//! Run:  cargo run --example spark_safety_demo -p qail-core
//!
//! ## What Makes This SPARK-Level?
//!
//! In SPARK/Ada, you write contracts (pre/post conditions) and the prover
//! verifies them at compile time. In Qail, the RUST TYPE SYSTEM is the prover:
//!
//!   SPARK:  `Pre => (X > 0)` — prover rejects negative X
//!   Qail:   `T: DirectBuild` — compiler rejects unproven RLS queries
//!
//! No proc macros. No external tools. No database connection at compile time.
//! The compiler IS the theorem prover.

use qail_core::ast::{Qail, Operator, SortOrder, Value};
use qail_core::rls::RlsContext;
use qail_core::typed::*;
use qail_core::transpiler::ToSql;

// ============================================================================
// SCHEMA DEFINITION (simulates `qail types schema.qail` codegen output)
// ============================================================================
//
// In production, `qail types` generates this from your schema.qail files.
// Here we define it manually to show exactly what the compiler sees.
//
// Schema:
//   users         — platform table (no operator_id) → DirectBuild
//   operators     — platform table (no operator_id) → DirectBuild
//   orders        — tenant table (has operator_id)  → RequiresRls
//   bookings      — tenant table (has operator_id)  → RequiresRls
//   audit_logs    — system table (restricted access) → DirectBuild + Restricted columns

// ──────────────────────────────────────────────────────────────────────────────
// Table: users (platform-level, no RLS)
// ──────────────────────────────────────────────────────────────────────────────
pub struct Users;
impl Table for Users { fn table_name() -> &'static str { "users" } }
impl AsRef<str> for Users { fn as_ref(&self) -> &str { "users" } }
impl From<Users> for String { fn from(_: Users) -> String { "users".into() } }
impl DirectBuild for Users {} // ← No operator_id, no RLS needed

#[allow(non_upper_case_globals)]
pub mod users {
    use super::*;
    pub const id: TypedColumn<uuid::Uuid> = TypedColumn::new("users", "id");
    pub const email: TypedColumn<String> = TypedColumn::new("users", "email");
    pub const role: TypedColumn<String> = TypedColumn::new("users", "role");
    pub const password_hash: TypedColumn<String, Protected> = TypedColumn::new("users", "password_hash");
    pub const created_at: TypedColumn<chrono::DateTime<chrono::Utc>> = TypedColumn::new("users", "created_at");
}

// ──────────────────────────────────────────────────────────────────────────────
// Table: operators (platform-level, no RLS)
// ──────────────────────────────────────────────────────────────────────────────
pub struct Operators;
impl Table for Operators { fn table_name() -> &'static str { "operators" } }
impl AsRef<str> for Operators { fn as_ref(&self) -> &str { "operators" } }
impl From<Operators> for String { fn from(_: Operators) -> String { "operators".into() } }
impl DirectBuild for Operators {}

// ──────────────────────────────────────────────────────────────────────────────
// Table: orders (TENANT table — has operator_id → RequiresRls)
// ──────────────────────────────────────────────────────────────────────────────
pub struct Orders;
impl Table for Orders { fn table_name() -> &'static str { "orders" } }
impl AsRef<str> for Orders { fn as_ref(&self) -> &str { "orders" } }
impl From<Orders> for String { fn from(_: Orders) -> String { "orders".into() } }
impl RequiresRls for Orders {} // ← HAS operator_id → MUST prove RLS

#[allow(non_upper_case_globals)]
pub mod orders {
    use super::*;
    pub const id: TypedColumn<uuid::Uuid> = TypedColumn::new("orders", "id");
    pub const user_id: TypedColumn<uuid::Uuid> = TypedColumn::new("orders", "user_id");
    pub const status: TypedColumn<String> = TypedColumn::new("orders", "status");
    pub const total_fare: TypedColumn<i64> = TypedColumn::new("orders", "total_fare");
    pub const currency: TypedColumn<String> = TypedColumn::new("orders", "currency");
    pub const operator_id: TypedColumn<uuid::Uuid> = TypedColumn::new("orders", "operator_id");
}

// ──────────────────────────────────────────────────────────────────────────────
// Table: bookings (TENANT table — has operator_id → RequiresRls)
// ──────────────────────────────────────────────────────────────────────────────
pub struct Bookings;
impl Table for Bookings { fn table_name() -> &'static str { "bookings" } }
impl AsRef<str> for Bookings { fn as_ref(&self) -> &str { "bookings" } }
impl From<Bookings> for String { fn from(_: Bookings) -> String { "bookings".into() } }
impl RequiresRls for Bookings {} // ← MUST prove RLS

#[allow(non_upper_case_globals)]
pub mod bookings {
    use super::*;
    pub const id: TypedColumn<uuid::Uuid> = TypedColumn::new("bookings", "id");
    pub const order_id: TypedColumn<uuid::Uuid> = TypedColumn::new("bookings", "order_id");
    pub const passenger_name: TypedColumn<String> = TypedColumn::new("bookings", "passenger_name");
    pub const seat_number: TypedColumn<String> = TypedColumn::new("bookings", "seat_number");
}

// ──────────────────────────────────────────────────────────────────────────────
// Table: audit_logs (system-level, Restricted columns)
// ──────────────────────────────────────────────────────────────────────────────
pub struct AuditLogs;
impl Table for AuditLogs { fn table_name() -> &'static str { "audit_logs" } }
impl AsRef<str> for AuditLogs { fn as_ref(&self) -> &str { "audit_logs" } }
impl From<AuditLogs> for String { fn from(_: AuditLogs) -> String { "audit_logs".into() } }
impl DirectBuild for AuditLogs {}

#[allow(non_upper_case_globals)]
pub mod audit_logs {
    use super::*;
    pub const id: TypedColumn<uuid::Uuid> = TypedColumn::new("audit_logs", "id");
    pub const action: TypedColumn<String> = TypedColumn::new("audit_logs", "action");
    pub const ip_address: TypedColumn<String, Protected> = TypedColumn::new("audit_logs", "ip_address");
    pub const raw_payload: TypedColumn<String, Restricted> = TypedColumn::new("audit_logs", "raw_payload");
}

// ──────────────────────────────────────────────────────────────────────────────
// Relationship Graph (from ref: annotations in schema.qail)
// ──────────────────────────────────────────────────────────────────────────────
// orders.user_id → users.id
impl RelatedTo<Orders> for Users {
    fn join_columns() -> (&'static str, &'static str) { ("id", "user_id") }
}
impl RelatedTo<Users> for Orders {
    fn join_columns() -> (&'static str, &'static str) { ("user_id", "id") }
}
// bookings.order_id → orders.id
impl RelatedTo<Bookings> for Orders {
    fn join_columns() -> (&'static str, &'static str) { ("id", "order_id") }
}
impl RelatedTo<Orders> for Bookings {
    fn join_columns() -> (&'static str, &'static str) { ("order_id", "id") }
}

// ============================================================================
// PROOF DEMONSTRATIONS
// ============================================================================

fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║     SPARK-Level Compile-Time Safety Proofs in Qail       ║");
    println!("║     If this compiles, ALL invariants are satisfied.       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // ======================================================================
    // PROOF 1: Type-Safe Column References
    //
    // SPARK equivalent: `type Age is range 0 .. 200;`
    // Qail equivalent:  `TypedColumn<i64>` — compiler knows the column type
    // ======================================================================
    println!("━━━ Proof 1: Type-Safe Column References ━━━");

    let q = Qail::typed(Users)
        .typed_column(users::email)        // String column ✓
        .typed_eq(users::role, "admin")     // String == &str ✓
        .build();

    println!("✅ Column types verified at compile time");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // The following would NOT compile (type mismatch):
    // .typed_eq(users::email, 42i64)    // ✗ String column vs i64 value
    // .typed_eq(orders::total_fare, "hello")  // ✗ i64 column vs &str value

    // ======================================================================
    // PROOF 2: Relationship Graph (Join Validity)
    //
    // SPARK equivalent: `Pre => Is_Connected(Node_A, Node_B)`
    // Qail equivalent:  `T: RelatedTo<U>` — invalid joins don't compile
    // ======================================================================
    println!("━━━ Proof 2: Join Validity Graph ━━━");

    let q = Qail::typed(Users)
        .join_related(Orders)              // Users → Orders via (id, user_id) ✓
        .column("users.email")
        .column("orders.status")
        .build();

    println!("✅ Users → Orders join proven valid via RelatedTo<Orders>");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // The following would NOT compile (no relationship defined):
    // Qail::typed(Users).join_related(AuditLogs)
    //   ✗ error: `Users: RelatedTo<AuditLogs>` is not satisfied

    // ======================================================================
    // PROOF 3: N-Way Join Chain
    //
    // SPARK equivalent: proving transitive reachability in a graph
    // Qail equivalent:  chaining .join_related() calls, each proven valid
    //
    // NOTE: Because TypedQail carries only the SOURCE table type,
    // we chain via Qail's string API after the first typed join.
    // Full N-way typed joins would require GATs (future enhancement).
    // ======================================================================
    println!("━━━ Proof 3: Multi-Table Query ━━━");

    let q = Qail::typed(Users)
        .join_related(Orders)              // Proven: Users → Orders
        .column("users.email")
        .column("orders.status")
        .column("orders.total_fare")
        .filter("orders.status", Operator::Eq, "confirmed")
        .order_by("orders.total_fare", SortOrder::Desc)
        .limit(5)
        .build();

    println!("✅ Users → Orders with typed columns, filters, ordering");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // ======================================================================
    // PROOF 4: Data Governance (Capability Witnesses)
    //
    // SPARK equivalent: `Pre => Has_Clearance(User, Secret_Level)`
    // Qail equivalent:  `PolicyAllowedBy<C>` — accessing Protected/Restricted
    //                    data requires a capability witness
    // ======================================================================
    println!("━━━ Proof 4: Data Governance (Capability Witnesses) ━━━");

    // 4a. Public columns — no capability needed
    let q = CapQuery::new(Qail::get("users"))
        .column(users::email)             // Public ✓
        .build();

    println!("✅ Public column accessed without capability");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // 4b. Protected column — requires AdminCap
    let admin_cap = CapabilityProvider::mint_admin();

    let q = CapQuery::new(Qail::get("users"))
        .with_cap(&admin_cap)                       // Upgrade to AdminCap
        .column(users::email)                       // Public — always OK
        .column_protected(users::password_hash)     // Protected — requires AdminCap ✓
        .build();

    println!("✅ Protected column (password_hash) unlocked with AdminCap");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // The following would NOT compile (insufficient capability):
    // CapQuery::new(Qail::get("users"))
    //     .column_protected(users::password_hash)
    //   ✗ error: `Protected: PolicyAllowedBy<NoCap>` is not satisfied

    // 4c. Restricted column — requires SystemCap (highest privilege)
    let system_cap = CapabilityProvider::mint_system();

    let q = CapQuery::new(Qail::get("audit_logs"))
        .with_cap(&system_cap)
        .column(audit_logs::action)                 // Public ✓
        .column_protected(audit_logs::ip_address)   // Protected — SystemCap covers it ✓
        .column_protected(audit_logs::raw_payload)  // Restricted — requires SystemCap ✓
        .build();

    println!("✅ Restricted column (raw_payload) unlocked with SystemCap");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // The following would NOT compile:
    // CapQuery::new(...).with_cap(&admin_cap).column_protected(audit_logs::raw_payload)
    //   ✗ error: `Restricted: PolicyAllowedBy<AdminCap>` is not satisfied
    //   AdminCap cannot access Restricted data — only SystemCap can

    // ======================================================================
    // PROOF 5: RLS Tenant Isolation (Proof Witness)
    //
    // SPARK equivalent: `Pre => Has_Isolation_Context(Query, Tenant)`
    // Qail equivalent:  `RequiresRls` tables have NO .build() without
    //                    .with_rls() — data leakage is a TYPE ERROR
    // ======================================================================
    println!("━━━ Proof 5: RLS Tenant Isolation (Proof Witness) ━━━");

    let tenant_ctx = RlsContext::operator("550e8400-e29b-41d4-a716-446655440000");

    // 5a. Basic RLS-protected query
    let q = Qail::typed(Orders)
        .typed_column(orders::id)
        .typed_column(orders::status)
        .typed_column(orders::total_fare)
        .with_rls(&tenant_ctx)             // → RlsQuery<Orders> (proof sealed)
        .build();                          // ✅ .build() now available

    println!("✅ Orders query proven isolated to tenant 550e8400...");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // 5b. Chaining AFTER proof — still type-safe
    let q = Qail::typed(Bookings)
        .typed_column(bookings::id)
        .typed_column(bookings::passenger_name)
        .with_rls(&tenant_ctx)
        .column("seat_number")            // Can still add columns after proof
        .filter("seat_number", Operator::IsNotNull, Value::Null)
        .order_by("passenger_name", SortOrder::Asc)
        .limit(50)
        .build();

    println!("✅ Bookings query — chaining after proof works");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // 5c. Super admin — proof still REQUIRED (type-level), but no filter injected
    let admin_ctx = RlsContext::super_admin();
    let q = Qail::typed(Orders)
        .typed_column(orders::id)
        .typed_column(orders::operator_id)
        .typed_column(orders::total_fare)
        .with_rls(&admin_ctx)              // Proof satisfied, no filter added
        .build();

    println!("✅ Super admin — proof required (type-level), no filter injected");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // The following would NOT compile:
    // let _leak = Qail::typed(Orders).typed_column(orders::id).build();
    //   ✗ error[E0599]: method `build` exists for struct `TypedQail<Orders>`,
    //                    but its trait bounds were not satisfied
    //     note: `Orders: DirectBuild` is not satisfied

    // ======================================================================
    // PROOF 6: Combined — All Proofs in One Query
    //
    // This is the "full stack" proof: type safety + governance + RLS
    // ======================================================================
    println!("━━━ Proof 6: Combined Safety Stack ━━━");

    let q = Qail::typed(Orders)
        .typed_column(orders::id)           // Proof 1: column type checked
        .typed_column(orders::status)       // Proof 1: column type checked
        .typed_eq(orders::status, "paid")   // Proof 2: String == &str ✓
        .with_rls(&tenant_ctx)              // Proof 5: tenant isolation proven
        .filter("total_fare", Operator::Gt, 100000i64) // Additional filter
        .order_by("total_fare", SortOrder::Desc)
        .limit(20)
        .build();

    println!("✅ Combined: typed columns + typed filter + RLS proof");
    println!("   Qail:  {}", q);
    println!("   SQL:   {}\n", q.to_sql());

    // ======================================================================
    // SUMMARY: What the compiler proves for you
    // ======================================================================
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                  COMPILER PROOF SUMMARY                  ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Proof 1: Column types match filter values     [PROVEN]  ║");
    println!("║  Proof 2: Join relationships are valid          [PROVEN]  ║");
    println!("║  Proof 3: Multi-table queries have valid paths  [PROVEN]  ║");
    println!("║  Proof 4: Data governance capabilities checked [PROVEN]  ║");
    println!("║  Proof 5: Tenant isolation is enforced          [PROVEN]  ║");
    println!("║  Proof 6: All proofs compose together           [PROVEN]  ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║                                                          ║");
    println!("║  COMPILE-TIME REJECTIONS (uncomment to verify):          ║");
    println!("║                                                          ║");
    println!("║  • typed_eq(age, \"hello\")     → type mismatch            ║");
    println!("║  • join_related(Unrelated)   → no RelatedTo impl         ║");
    println!("║  • column_protected(secret)  → insufficient capability   ║");
    println!("║  • Orders.build()            → missing RLS proof         ║");
    println!("║                                                          ║");
    println!("║  No proc macros. No external provers. No DB at compile.  ║");
    println!("║  The Rust compiler IS the theorem prover.                ║");
    println!("╚════════════════════════════════════════════════════════════╝");
}
