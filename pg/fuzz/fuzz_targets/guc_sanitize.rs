//! Fuzz target for sanitize_guc_value — the RLS injection barrier.
//!
//! Goal: for ANY input string, the output must NEVER contain characters
//! that could break out of a SQL string literal or inject SQL.
//!
//! Dangerous characters that must never appear in output:
//! - Single quote (') — SQL string escape
//! - Backslash (\) — escape character
//! - Semicolon (;) — statement separator
//! - Dollar sign ($) — $$-style quoting
//! - NUL byte (\0) — C string truncation
//! - Any control character — unpredictable behavior

#![no_main]
use libfuzzer_sys::fuzz_target;
use qail_pg::driver::rls::sanitize_guc_value;

fuzz_target!(|data: &[u8]| {
    // Convert arbitrary bytes to a string (lossy — replaces invalid UTF-8)
    let input = String::from_utf8_lossy(data);
    let sanitized = sanitize_guc_value(&input);

    // INVARIANT: output must never contain dangerous characters
    for ch in sanitized.chars() {
        assert!(ch >= ' ' && ch <= '~', "Non-printable ASCII in output: {:?}", ch);
        assert_ne!(ch, '\'', "Single quote in sanitized output!");
        assert_ne!(ch, '\\', "Backslash in sanitized output!");
        assert_ne!(ch, ';', "Semicolon in sanitized output!");
        assert_ne!(ch, '$', "Dollar sign in sanitized output!");
        assert_ne!(ch, '\0', "NUL byte in sanitized output!");
    }

    // INVARIANT: output length must never exceed input length (filter-only)
    assert!(sanitized.len() <= input.len(),
        "Sanitized output longer than input: {} > {}", sanitized.len(), input.len());
});
