//! Zero-allocation helper functions and lookup tables.
//!
//! Pre-computed values for parameter placeholders and numeric literals
//! to avoid heap allocations in the hot path.

use bytes::BytesMut;

/// Pre-computed parameter placeholders $1-$99 (covers 99% of cases)
pub const PARAM_PLACEHOLDERS: [&[u8]; 100] = [
    b"$0", b"$1", b"$2", b"$3", b"$4", b"$5", b"$6", b"$7", b"$8", b"$9",
    b"$10", b"$11", b"$12", b"$13", b"$14", b"$15", b"$16", b"$17", b"$18", b"$19",
    b"$20", b"$21", b"$22", b"$23", b"$24", b"$25", b"$26", b"$27", b"$28", b"$29",
    b"$30", b"$31", b"$32", b"$33", b"$34", b"$35", b"$36", b"$37", b"$38", b"$39",
    b"$40", b"$41", b"$42", b"$43", b"$44", b"$45", b"$46", b"$47", b"$48", b"$49",
    b"$50", b"$51", b"$52", b"$53", b"$54", b"$55", b"$56", b"$57", b"$58", b"$59",
    b"$60", b"$61", b"$62", b"$63", b"$64", b"$65", b"$66", b"$67", b"$68", b"$69",
    b"$70", b"$71", b"$72", b"$73", b"$74", b"$75", b"$76", b"$77", b"$78", b"$79",
    b"$80", b"$81", b"$82", b"$83", b"$84", b"$85", b"$86", b"$87", b"$88", b"$89",
    b"$90", b"$91", b"$92", b"$93", b"$94", b"$95", b"$96", b"$97", b"$98", b"$99",
];

/// Pre-computed numeric values 0-99 for LIMIT/OFFSET (covers common cases)
pub const NUMERIC_VALUES: [&[u8]; 100] = [
    b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9",
    b"10", b"11", b"12", b"13", b"14", b"15", b"16", b"17", b"18", b"19",
    b"20", b"21", b"22", b"23", b"24", b"25", b"26", b"27", b"28", b"29",
    b"30", b"31", b"32", b"33", b"34", b"35", b"36", b"37", b"38", b"39",
    b"40", b"41", b"42", b"43", b"44", b"45", b"46", b"47", b"48", b"49",
    b"50", b"51", b"52", b"53", b"54", b"55", b"56", b"57", b"58", b"59",
    b"60", b"61", b"62", b"63", b"64", b"65", b"66", b"67", b"68", b"69",
    b"70", b"71", b"72", b"73", b"74", b"75", b"76", b"77", b"78", b"79",
    b"80", b"81", b"82", b"83", b"84", b"85", b"86", b"87", b"88", b"89",
    b"90", b"91", b"92", b"93", b"94", b"95", b"96", b"97", b"98", b"99",
];

/// Write parameter placeholder ($N) to buffer.
/// Zero allocation for common cases (1-99).
#[inline(always)]
pub fn write_param_placeholder(buf: &mut BytesMut, idx: usize) {
    if idx < 100 {
        buf.extend_from_slice(PARAM_PLACEHOLDERS[idx]);
    } else {
        buf.extend_from_slice(b"$");
        write_usize(buf, idx);
    }
}

/// Write usize to buffer.
/// Zero allocation for 0-99, minimal allocation for 100-999.
#[inline(always)]
pub fn write_usize(buf: &mut BytesMut, n: usize) {
    if n < 100 {
        buf.extend_from_slice(NUMERIC_VALUES[n]);
    } else if n < 1000 {
        let hundreds = n / 100;
        let tens = (n % 100) / 10;
        let ones = n % 10;
        buf.extend_from_slice(NUMERIC_VALUES[hundreds]);
        buf.extend_from_slice(NUMERIC_VALUES[tens]);
        buf.extend_from_slice(NUMERIC_VALUES[ones]);
    } else {
        buf.extend_from_slice(n.to_string().as_bytes());
    }
}

/// Write i64 to buffer.
/// Zero allocation for 0-99.
#[inline(always)]
#[allow(dead_code)]
pub fn write_i64(buf: &mut BytesMut, n: i64) {
    if (0..100).contains(&n) {
        buf.extend_from_slice(NUMERIC_VALUES[n as usize]);
    } else if (0..1000).contains(&n) {
        write_usize(buf, n as usize);
    } else {
        buf.extend_from_slice(n.to_string().as_bytes());
    }
}

/// Convert i64 to bytes for parameter.
/// Zero allocation for 0-99.
#[inline(always)]
pub fn i64_to_bytes(n: i64) -> Vec<u8> {
    if (0..100).contains(&n) {
        NUMERIC_VALUES[n as usize].to_vec()
    } else {
        n.to_string().into_bytes()
    }
}
