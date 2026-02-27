//! RLS Proof Witness Demo
//!
//! Proves that the type system enforces tenant isolation at COMPILE TIME.
//!
//! Run: cargo run --example rls_proof_demo
//!
//! To see the compile error, uncomment the line marked "UNCOMMENT TO SEE COMPILE ERROR"

use qail_core::ast::Qail;
use qail_core::rls::RlsContext;
use qail_core::transpiler::ToSql;
use qail_core::typed::{DirectBuild, RequiresRls, Table};

// ============================================================
// Simulated codegen output from `qail types schema.qail`
// ============================================================

// Table WITHOUT operator_id → gets DirectBuild
pub struct Migrations;
impl Table for Migrations {
    fn table_name() -> &'static str {
        "migrations"
    }
}
impl AsRef<str> for Migrations {
    fn as_ref(&self) -> &str {
        "migrations"
    }
}
impl From<Migrations> for String {
    fn from(_: Migrations) -> String {
        "migrations".into()
    }
}
impl DirectBuild for Migrations {} // ← No RLS needed

// Table WITH operator_id → gets RequiresRls
pub struct Orders;
impl Table for Orders {
    fn table_name() -> &'static str {
        "orders"
    }
}
impl AsRef<str> for Orders {
    fn as_ref(&self) -> &str {
        "orders"
    }
}
impl From<Orders> for String {
    fn from(_: Orders) -> String {
        "orders".into()
    }
}
impl RequiresRls for Orders {} // ← RLS REQUIRED

// Table WITH operator_id → gets RequiresRls
pub struct Bookings;
impl Table for Bookings {
    fn table_name() -> &'static str {
        "bookings"
    }
}
impl AsRef<str> for Bookings {
    fn as_ref(&self) -> &str {
        "bookings"
    }
}
impl From<Bookings> for String {
    fn from(_: Bookings) -> String {
        "bookings".into()
    }
}
impl RequiresRls for Bookings {} // ← RLS REQUIRED

fn main() {
    println!("=== RLS Proof Witness Demo ===\n");

    // ============================================================
    // 1. Non-RLS table: .build() works directly
    // ============================================================
    let query = Qail::typed(Migrations)
        .column("id")
        .column("name")
        .column("applied_at")
        .build(); // ✅ Compiles — Migrations has DirectBuild

    println!("✅ Non-RLS table (Migrations):");
    println!("   {}\n", query.to_sql());

    // ============================================================
    // 2. RLS table WITH proof: .with_rls() → .build() works
    // ============================================================
    let ctx = RlsContext::operator("550e8400-e29b-41d4-a716-446655440000");

    let query = Qail::typed(Orders)
        .column("id")
        .column("status")
        .column("total_fare")
        .with_rls(&ctx) // Returns RlsQuery<Orders> — proof provided
        .build(); // ✅ Compiles — RlsQuery has .build()

    println!("✅ RLS table with proof (Orders):");
    println!("   {}\n", query.to_sql());

    // ============================================================
    // 3. RLS table: chaining after .with_rls() still works
    // ============================================================
    let query = Qail::typed(Bookings)
        .column("id")
        .with_rls(&ctx)
        .column("passenger_name")
        .column("booking_number")
        .filter("status", qail_core::ast::Operator::Eq, "confirmed")
        .order_by("created_at", qail_core::ast::SortOrder::Desc)
        .limit(10)
        .build(); // ✅ Compiles

    println!("✅ RLS table with chaining (Bookings):");
    println!("   {}\n", query.to_sql());

    // ============================================================
    // 4. Super admin: still needs proof (type-level), but filter is no-op
    // ============================================================
    let token = qail_core::rls::SuperAdminToken::for_system_process("demo_super_admin");
    let admin_ctx = RlsContext::super_admin(token);
    let query = Qail::typed(Orders)
        .column("id")
        .column("operator_id")
        .with_rls(&admin_ctx) // Proof satisfied, but no filter injected
        .build();

    println!("✅ Super admin (no tenant filter, but proof still required):");
    println!("   {}\n", query.to_sql());

    // ============================================================
    // 5. COMPILE ERROR — uncomment to prove type safety
    // ============================================================
    // UNCOMMENT TO SEE COMPILE ERROR:
    // let _leak = Qail::typed(Orders).column("id").build();
    //
    // Error: no method named `build` found for struct `TypedQail<Orders>`
    //        in the current scope
    //        method not found in `TypedQail<Orders>`
    //        note: `Orders` does not implement `DirectBuild`

    println!("✅ All proofs satisfied. Data leakage is a type error!");
    println!("\n   Try uncommenting the line marked 'UNCOMMENT TO SEE COMPILE ERROR'");
    println!("   to witness the compiler reject an unproven query.");
}
