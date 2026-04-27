//! Lightweight timestamp formatting — replaces the `chrono` crate.
//!
//! Provides local-time formatting using only the standard library.

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static LAST_MIGRATION_TICK_MS: AtomicU64 = AtomicU64::new(0);

/// Format the current local time as `YYYYMMDDHHMMSSmmm` (for migration versions).
pub fn timestamp_version() -> String {
    let base = date_output(&["+%Y%m%d%H%M%S"]).unwrap_or_else(|| {
        let (year, month, day, hour, minute, second) = utc_now_parts();
        format!("{year:04}{month:02}{day:02}{hour:02}{minute:02}{second:02}")
    });
    let ms_suffix = monotonic_epoch_ms() % 1000;
    format!("{base}{ms_suffix:03}")
}

/// Format the current local time as `YYYYMMDD_HHMMSS` (for backup filenames).
pub fn timestamp_filename() -> String {
    date_output(&["+%Y%m%d_%H%M%S"]).unwrap_or_else(|| {
        let (year, month, day, hour, minute, second) = utc_now_parts();
        format!("{year:04}{month:02}{day:02}_{hour:02}{minute:02}{second:02}")
    })
}

/// Format the current local time as `HH:MM:SS` (for watch mode logging).
pub fn timestamp_short() -> String {
    date_output(&["+%H:%M:%S"]).unwrap_or_else(|| {
        let (_, _, _, hour, minute, second) = utc_now_parts();
        format!("{hour:02}:{minute:02}:{second:02}")
    })
}

/// Format the current local time as RFC 3339 (for metadata).
pub fn timestamp_rfc3339() -> String {
    date_output(&["-u", "+%Y-%m-%dT%H:%M:%SZ"]).unwrap_or_else(|| {
        let (year, month, day, hour, minute, second) = utc_now_parts();
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    })
}

fn date_output(args: &[&str]) -> Option<String> {
    let output = Command::new("date").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }

    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}

fn utc_now_parts() -> (i64, u32, u32, u32, u32, u32) {
    utc_parts(monotonic_epoch_ms() / 1000)
}

fn utc_parts(epoch_secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (epoch_secs / 86_400) as i64;
    let secs_of_day = epoch_secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = (secs_of_day / 3_600) as u32;
    let minute = ((secs_of_day % 3_600) / 60) as u32;
    let second = (secs_of_day % 60) as u32;
    (year, month, day, hour, minute, second)
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}

fn monotonic_epoch_ms() -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    loop {
        let last = LAST_MIGRATION_TICK_MS.load(Ordering::Relaxed);
        let next = if now_ms > last { now_ms } else { last + 1 };
        if LAST_MIGRATION_TICK_MS
            .compare_exchange(last, next, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            return next;
        }
    }
}

/// Simple MD5 hash — replaces the `md5` crate.
/// Only used for migration checksums, not cryptographic security.
pub fn md5_hex(input: &str) -> String {
    // MD5 constants
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
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
                (a.wrapping_add(f).wrapping_add(K[i]).wrapping_add(m[g])).rotate_left(S[i]),
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
        assert_eq!(ts.len(), 17);
        assert!(ts.chars().all(|c| c.is_ascii_digit()));
    }
}
