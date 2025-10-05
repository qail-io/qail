//! Lightweight timestamp formatting — replaces the `chrono` crate.
//!
//! Provides local-time formatting using only the standard library.

use std::process::Command;

/// Format the current local time as `YYYYMMDDHHMMSS` (for migration versions).
pub fn timestamp_version() -> String {
    // Use the `date` command for local time — avoids pulling in chrono
    let output = Command::new("date")
        .arg("+%Y%m%d%H%M%S")
        .output()
        .expect("failed to run `date`");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Format the current local time as `YYYYMMDD_HHMMSS` (for backup filenames).
pub fn timestamp_filename() -> String {
    let output = Command::new("date")
        .arg("+%Y%m%d_%H%M%S")
        .output()
        .expect("failed to run `date`");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Format the current local time as `HH:MM:SS` (for watch mode logging).
pub fn timestamp_short() -> String {
    let output = Command::new("date")
        .arg("+%H:%M:%S")
        .output()
        .expect("failed to run `date`");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Format the current local time as RFC 3339 (for metadata).
pub fn timestamp_rfc3339() -> String {
    let output = Command::new("date")
        .arg("-u")
        .arg("+%Y-%m-%dT%H:%M:%SZ")
        .output()
        .expect("failed to run `date`");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Simple MD5 hash — replaces the `md5` crate.
/// Only used for migration checksums, not cryptographic security.
pub fn md5_hex(input: &str) -> String {
    // MD5 constants
    const S: [u32; 64] = [
        7,12,17,22,7,12,17,22,7,12,17,22,7,12,17,22,
        5,9,14,20,5,9,14,20,5,9,14,20,5,9,14,20,
        4,11,16,23,4,11,16,23,4,11,16,23,4,11,16,23,
        6,10,15,21,6,10,15,21,6,10,15,21,6,10,15,21,
    ];
    const K: [u32; 64] = [
        0xd76aa478,0xe8c7b756,0x242070db,0xc1bdceee,
        0xf57c0faf,0x4787c62a,0xa8304613,0xfd469501,
        0x698098d8,0x8b44f7af,0xffff5bb1,0x895cd7be,
        0x6b901122,0xfd987193,0xa679438e,0x49b40821,
        0xf61e2562,0xc040b340,0x265e5a51,0xe9b6c7aa,
        0xd62f105d,0x02441453,0xd8a1e681,0xe7d3fbc8,
        0x21e1cde6,0xc33707d6,0xf4d50d87,0x455a14ed,
        0xa9e3e905,0xfcefa3f8,0x676f02d9,0x8d2a4c8a,
        0xfffa3942,0x8771f681,0x6d9d6122,0xfde5380c,
        0xa4beea44,0x4bdecfa9,0xf6bb4b60,0xbebfbc70,
        0x289b7ec6,0xeaa127fa,0xd4ef3085,0x04881d05,
        0xd9d4d039,0xe6db99e5,0x1fa27cf8,0xc4ac5665,
        0xf4292244,0x432aff97,0xab9423a7,0xfc93a039,
        0x655b59c3,0x8f0ccc92,0xffeff47d,0x85845dd1,
        0x6fa87e4f,0xfe2ce6e0,0xa3014314,0x4e0811a1,
        0xf7537e82,0xbd3af235,0x2ad7d2bb,0xeb86d391,
    ];

    let mut msg = input.as_bytes().to_vec();
    let bit_len = (msg.len() as u64) * 8;
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    let (mut a0, mut b0, mut c0, mut d0): (u32, u32, u32, u32) =
        (0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476);

    for chunk in msg.chunks(64) {
        let mut m = [0u32; 16];
        for (i, c) in chunk.chunks(4).enumerate() {
            m[i] = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        }
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let temp = d;
            d = c;
            c = b;
            b = b.wrapping_add(
                (a.wrapping_add(f).wrapping_add(K[i]).wrapping_add(m[g]))
                    .rotate_left(S[i]),
            );
            a = temp;
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    format!(
        "{:08x}{:08x}{:08x}{:08x}",
        a0.swap_bytes(),
        b0.swap_bytes(),
        c0.swap_bytes(),
        d0.swap_bytes()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5_empty() {
        assert_eq!(md5_hex(""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn test_md5_hello() {
        assert_eq!(md5_hex("hello"), "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn test_md5_migration_sql() {
        // Realistic test: a SQL migration string
        let sql = "CREATE TABLE users (id UUID PRIMARY KEY);";
        let hash = md5_hex(sql);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_timestamp_version_format() {
        let ts = timestamp_version();
        assert_eq!(ts.len(), 14);
        assert!(ts.chars().all(|c| c.is_ascii_digit()));
    }
}
