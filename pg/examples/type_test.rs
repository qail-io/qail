//! Comprehensive Type Mapping Tests
//!
//! Tests all type conversions using simulated PostgreSQL data.
//!
//! Run: cargo test -p qail-pg types --release

use qail_pg::protocol::types::oid;
use qail_pg::{Date, FromPg, Json, Numeric, Time, Timestamp, Uuid};

fn main() {
    println!("🧪 QAIL TYPE MAPPING COMPREHENSIVE TEST");
    println!("=========================================\n");

    let mut passed = 0;
    let mut failed = 0;

    macro_rules! test_type {
        ($name:expr, $bytes:expr, $oid:expr, $format:expr, $parse:ty) => {{
            print!("  {} ... ", $name);
            match <$parse>::from_pg($bytes, $oid, $format) {
                Ok(val) => {
                    println!("✅ {:?}", val);
                    passed += 1;
                }
                Err(e) => {
                    println!("❌ {:?}", e);
                    failed += 1;
                }
            }
        }};
    }

    println!("📦 BASIC TYPES (Text Format):");
    test_type!("String", b"Hello, QAIL!", oid::TEXT, 0, String);
    test_type!("i32", b"42", oid::INT4, 0, i32);
    test_type!("i64", b"9223372036854775807", oid::INT8, 0, i64);
    test_type!("f64", b"3.14159", oid::FLOAT8, 0, f64);
    test_type!("bool (true)", b"t", oid::BOOL, 0, bool);
    test_type!("bool (false)", b"f", oid::BOOL, 0, bool);

    println!("\n📦 BASIC TYPES (Binary Format):");
    test_type!("i32 (binary)", &42i32.to_be_bytes(), oid::INT4, 1, i32);
    test_type!(
        "i64 (binary)",
        &9223372036854775807i64.to_be_bytes(),
        oid::INT8,
        1,
        i64
    );
    test_type!(
        "f64 (binary)",
        &3.14159f64.to_be_bytes(),
        oid::FLOAT8,
        1,
        f64
    );
    test_type!("bool (binary true)", &[1u8], oid::BOOL, 1, bool);
    test_type!("bool (binary false)", &[0u8], oid::BOOL, 1, bool);

    println!("\n📦 UUID:");
    // Binary format: 16 bytes
    let uuid_bytes: [u8; 16] = [
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ];
    test_type!("UUID (binary)", &uuid_bytes, oid::UUID, 1, Uuid);
    test_type!(
        "UUID (text)",
        b"550e8400-e29b-41d4-a716-446655440000",
        oid::UUID,
        0,
        Uuid
    );

    println!("\n📦 JSON/JSONB:");
    // JSONB binary format: version byte + json
    let jsonb_bytes = [&[1u8][..], b"{\"key\":\"value\"}".as_slice()].concat();
    test_type!("JSONB (binary)", &jsonb_bytes, oid::JSONB, 1, Json);
    test_type!(
        "JSON (text)",
        b"{\"nested\":{\"array\":[1,2,3]}}",
        oid::JSON,
        0,
        Json
    );

    println!("\n📦 NUMERIC:");
    test_type!("NUMERIC", b"12345.6789", oid::NUMERIC, 0, Numeric);
    test_type!(
        "NUMERIC (large)",
        b"99999999999999999999.123456789",
        oid::NUMERIC,
        0,
        Numeric
    );
    test_type!("NUMERIC (negative)", b"-999.99", oid::NUMERIC, 0, Numeric);

    println!("\n📦 TEMPORAL TYPES:");
    // Binary: 8 bytes for timestamp (microseconds since 2000-01-01)
    let ts_usec: i64 = 789_012_345_678_900; // ~25 years after 2000
    test_type!(
        "TIMESTAMP (binary)",
        &ts_usec.to_be_bytes(),
        oid::TIMESTAMP,
        1,
        Timestamp
    );
    test_type!(
        "TIMESTAMP (text)",
        b"2024-12-25 17:30:00",
        oid::TIMESTAMP,
        0,
        Timestamp
    );

    // Binary: 4 bytes for date (days since 2000-01-01)
    let date_days: i32 = 9125; // ~25 years
    test_type!(
        "DATE (binary)",
        &date_days.to_be_bytes(),
        oid::DATE,
        1,
        Date
    );
    test_type!("DATE (text)", b"2024-12-25", oid::DATE, 0, Date);

    // Binary: 8 bytes for time (microseconds since midnight)
    let time_usec: i64 = 12 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000;
    test_type!(
        "TIME (binary)",
        &time_usec.to_be_bytes(),
        oid::TIME,
        1,
        Time
    );
    test_type!("TIME (text)", b"14:30:45.123456", oid::TIME, 0, Time);

    println!("\n📦 ARRAYS (Text Format):");
    test_type!("TEXT[]", b"{a,b,c}", oid::TEXT_ARRAY, 0, Vec<String>);
    test_type!(
        "TEXT[] (quoted)",
        b"{\"hello, world\",foo}",
        oid::TEXT_ARRAY,
        0,
        Vec<String>
    );
    test_type!("INT[]", b"{1,2,3,4,5}", oid::INT4_ARRAY, 0, Vec<i64>);
    test_type!("INT[] (empty)", b"{}", oid::INT4_ARRAY, 0, Vec<i64>);

    println!("\n📦 BYTEA:");
    test_type!("BYTEA", b"\\xDEADBEEF", oid::BYTEA, 0, Vec<u8>);

    // Summary
    println!("\n=========================================");
    println!("📊 RESULTS: {} passed, {} failed", passed, failed);

    if failed == 0 {
        println!("✅ ALL TESTS PASSED!");
    } else {
        println!("❌ SOME TESTS FAILED");
        std::process::exit(1);
    }
}
