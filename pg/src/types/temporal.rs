//! Timestamp type conversions for PostgreSQL.
//!
//! PostgreSQL timestamps are stored as microseconds since 2000-01-01 00:00:00 UTC.

use super::{FromPg, ToPg, TypeError};
use crate::protocol::types::oid;

/// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
/// Difference from Unix epoch (1970-01-01) in microseconds
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;
const USEC_PER_DAY: i64 = 86_400_000_000;

/// Timestamp without timezone (microseconds since 2000-01-01)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    /// Microseconds since PostgreSQL epoch (2000-01-01 00:00:00)
    pub usec: i64,
}

impl Timestamp {
    /// Create from microseconds since PostgreSQL epoch
    pub fn from_pg_usec(usec: i64) -> Self {
        Self { usec }
    }

    /// Create from Unix timestamp (seconds since 1970-01-01)
    pub fn from_unix_secs(secs: i64) -> Self {
        Self {
            usec: secs
                .saturating_mul(1_000_000)
                .saturating_sub(PG_EPOCH_OFFSET_USEC),
        }
    }

    /// Convert to Unix timestamp (seconds since 1970-01-01)
    pub fn to_unix_secs(&self) -> i64 {
        self.to_unix_usec() / 1_000_000
    }

    /// Convert to Unix timestamp with microseconds
    pub fn to_unix_usec(&self) -> i64 {
        self.usec.saturating_add(PG_EPOCH_OFFSET_USEC)
    }
}

impl FromPg for Timestamp {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::TIMESTAMP && oid_val != oid::TIMESTAMPTZ {
            return Err(TypeError::UnexpectedOid {
                expected: "timestamp",
                got: oid_val,
            });
        }

        if format == 1 {
            // Binary: 8 bytes, microseconds since 2000-01-01
            if bytes.len() != 8 {
                return Err(TypeError::InvalidData(
                    "Expected 8 bytes for timestamp".to_string(),
                ));
            }
            let usec = i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            Ok(Timestamp::from_pg_usec(usec))
        } else {
            // Text format: parse ISO 8601
            let s =
                std::str::from_utf8(bytes).map_err(|e| TypeError::InvalidData(e.to_string()))?;
            parse_timestamp_text(s)
        }
    }
}

impl ToPg for Timestamp {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.usec.to_be_bytes().to_vec(), oid::TIMESTAMP, 1)
    }
}

#[cfg(feature = "chrono")]
impl FromPg for chrono::DateTime<chrono::Utc> {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::TIMESTAMP && oid_val != oid::TIMESTAMPTZ {
            return Err(TypeError::UnexpectedOid {
                expected: "timestamp",
                got: oid_val,
            });
        }

        if format == 1 {
            if bytes.len() != 8 {
                return Err(TypeError::InvalidData(
                    "Expected 8 bytes for timestamp".to_string(),
                ));
            }
            let pg_usec = i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            let unix_usec = pg_usec.saturating_add(PG_EPOCH_OFFSET_USEC);
            chrono::DateTime::<chrono::Utc>::from_timestamp_micros(unix_usec).ok_or_else(|| {
                TypeError::InvalidData(format!("Timestamp out of range: {}", unix_usec))
            })
        } else {
            let s =
                std::str::from_utf8(bytes).map_err(|e| TypeError::InvalidData(e.to_string()))?;

            if oid_val == oid::TIMESTAMPTZ {
                chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
                    .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%#z"))
                    .or_else(|_| chrono::DateTime::parse_from_rfc3339(s))
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| TypeError::InvalidData(format!("Invalid timestamptz: {}", e)))
            } else {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                    .map(|naive| {
                        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                            naive,
                            chrono::Utc,
                        )
                    })
                    .map_err(|e| TypeError::InvalidData(format!("Invalid timestamp: {}", e)))
            }
        }
    }
}

#[cfg(feature = "chrono")]
impl ToPg for chrono::DateTime<chrono::Utc> {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        let unix_usec = self.timestamp_micros();
        let pg_usec = unix_usec.saturating_sub(PG_EPOCH_OFFSET_USEC);
        (pg_usec.to_be_bytes().to_vec(), oid::TIMESTAMPTZ, 1)
    }
}

/// Parse PostgreSQL text timestamp format
fn parse_timestamp_text(s: &str) -> Result<Timestamp, TypeError> {
    // Format: "2024-12-25 17:30:00" or "2024-12-25 17:30:00.123456"
    // This is a simplified parser - production would use chrono or time crate

    let parts: Vec<&str> = s.splitn(2, &[' ', 'T'][..]).collect();
    if parts.len() != 2 {
        return Err(TypeError::InvalidData(format!("Invalid timestamp: {}", s)));
    }

    let (year, month, day) = parse_date_components(parts[0])?;
    let (time_str, timezone_offset_usec) = split_timezone_suffix(parts[1])?;
    let (hour, minute, second, usec) = parse_time_components(time_str)?;
    let days_since_epoch = days_from_ymd_checked(year, month, day)?;

    let total_usec = days_since_epoch as i64 * 86_400_000_000
        + hour as i64 * 3_600_000_000
        + minute as i64 * 60_000_000
        + second as i64 * 1_000_000
        + usec;

    Ok(Timestamp::from_pg_usec(total_usec - timezone_offset_usec))
}

fn parse_date_components(s: &str) -> Result<(i32, i32, i32), TypeError> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(TypeError::InvalidData(format!("Invalid date: {}", s)));
    }
    let year = parse_i32_part(parts[0], "year")?;
    let month = parse_i32_part(parts[1], "month")?;
    let day = parse_i32_part(parts[2], "day")?;
    validate_ymd(year, month, day)?;
    Ok((year, month, day))
}

fn split_timezone_suffix(s: &str) -> Result<(&str, i64), TypeError> {
    let s = s.trim_end();
    if let Some(stripped) = s.strip_suffix('Z') {
        return Ok((stripped, 0));
    }
    if let Some(idx) = s
        .char_indices()
        .skip(1)
        .find_map(|(idx, c)| (c == '+' || c == '-').then_some(idx))
    {
        let offset = parse_timezone_offset_usec(&s[idx..]).ok_or_else(|| {
            TypeError::InvalidData(format!("Invalid timezone offset: {}", &s[idx..]))
        })?;
        Ok((&s[..idx], offset))
    } else {
        Ok((s, 0))
    }
}

fn parse_timezone_offset_usec(s: &str) -> Option<i64> {
    let sign = match s.as_bytes().first()? {
        b'+' => 1_i64,
        b'-' => -1_i64,
        _ => return None,
    };
    let raw = &s[1..];
    if raw.is_empty() {
        return None;
    }

    let (hours, minutes) = if let Some((hours, minutes)) = raw.split_once(':') {
        (hours, minutes)
    } else if raw.len() == 4 {
        (&raw[..2], &raw[2..])
    } else {
        (raw, "0")
    };

    if hours.is_empty() || minutes.is_empty() {
        return None;
    }
    let hours = hours.parse::<i64>().ok()?;
    let minutes = minutes.parse::<i64>().ok()?;
    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return None;
    }

    Some(sign * ((hours * 3_600 + minutes * 60) * 1_000_000))
}

fn parse_time_components(s: &str) -> Result<(i32, i32, i32, i64), TypeError> {
    let parts: Vec<&str> = s.split(':').collect();
    if !(2..=3).contains(&parts.len()) {
        return Err(TypeError::InvalidData(format!("Invalid time: {}", s)));
    }

    let hour = parse_i32_part(parts[0], "hour")?;
    let minute = parse_i32_part(parts[1], "minute")?;
    let (second, usec) = if let Some(second_part) = parts.get(2) {
        parse_second_usec(second_part)?
    } else {
        (0, 0)
    };

    validate_time_components(hour, minute, second, usec)?;
    Ok((hour, minute, second, usec))
}

fn parse_second_usec(s: &str) -> Result<(i32, i64), TypeError> {
    let (second, fraction) = match s.split_once('.') {
        Some((second, fraction)) => (second, Some(fraction)),
        None => (s, None),
    };
    let second = parse_i32_part(second, "second")?;
    let usec = match fraction {
        Some(fraction) => parse_usec_fraction(fraction)?,
        None => 0,
    };
    Ok((second, usec))
}

fn parse_usec_fraction(s: &str) -> Result<i64, TypeError> {
    if s.is_empty() || s.len() > 6 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(TypeError::InvalidData(
            "Invalid microsecond fraction".to_string(),
        ));
    }
    let padded = format!("{:0<6}", s);
    padded
        .parse::<i64>()
        .map_err(|_| TypeError::InvalidData("Invalid microsecond fraction".to_string()))
}

fn parse_i32_part(s: &str, label: &str) -> Result<i32, TypeError> {
    if s.is_empty() {
        return Err(TypeError::InvalidData(format!("Invalid {}", label)));
    }
    s.parse()
        .map_err(|_| TypeError::InvalidData(format!("Invalid {}", label)))
}

fn validate_ymd(year: i32, month: i32, day: i32) -> Result<(), TypeError> {
    if !(1..=12).contains(&month) {
        return Err(TypeError::InvalidData("Invalid month".to_string()));
    }
    let max_day = days_in_month(year, month);
    if !(1..=max_day).contains(&day) {
        return Err(TypeError::InvalidData("Invalid day".to_string()));
    }
    Ok(())
}

fn validate_time_components(
    hour: i32,
    minute: i32,
    second: i32,
    usec: i64,
) -> Result<(), TypeError> {
    if !(0..=23).contains(&hour) {
        return Err(TypeError::InvalidData("Invalid hour".to_string()));
    }
    if !(0..=59).contains(&minute) {
        return Err(TypeError::InvalidData("Invalid minute".to_string()));
    }
    if !(0..=59).contains(&second) {
        return Err(TypeError::InvalidData("Invalid second".to_string()));
    }
    if !(0..=999_999).contains(&usec) {
        return Err(TypeError::InvalidData("Invalid microsecond".to_string()));
    }
    Ok(())
}

fn validate_time_usec(usec: i64) -> Result<(), TypeError> {
    if !(0..USEC_PER_DAY).contains(&usec) {
        return Err(TypeError::InvalidData(format!(
            "Time out of range: {} microseconds",
            usec
        )));
    }
    Ok(())
}

fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Calculate days since 2000-01-01.
fn days_from_ymd_checked(year: i32, month: i32, day: i32) -> Result<i32, TypeError> {
    validate_ymd(year, month, day)?;
    let epoch_days = days_from_civil(2000, 1, 1);
    let days = days_from_civil(year, month, day)
        .checked_sub(epoch_days)
        .ok_or_else(|| TypeError::InvalidData("Date out of range".to_string()))?;
    i32::try_from(days).map_err(|_| TypeError::InvalidData("Date out of range".to_string()))
}

fn days_from_civil(year: i32, month: i32, day: i32) -> i64 {
    let mut year = year as i64;
    let month = month as i64;
    let day = day as i64;
    year -= (month <= 2) as i64;
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_adjusted = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_adjusted + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Date type (days since 2000-01-01)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date {
    /// Days since PostgreSQL epoch (2000-01-01). Negative values represent dates before the epoch.
    pub days: i32,
}

impl FromPg for Date {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::DATE {
            return Err(TypeError::UnexpectedOid {
                expected: "date",
                got: oid_val,
            });
        }

        if format == 1 {
            // Binary: 4 bytes, days since 2000-01-01
            if bytes.len() != 4 {
                return Err(TypeError::InvalidData(
                    "Expected 4 bytes for date".to_string(),
                ));
            }
            let days = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            Ok(Date { days })
        } else {
            // Text format: YYYY-MM-DD
            let s =
                std::str::from_utf8(bytes).map_err(|e| TypeError::InvalidData(e.to_string()))?;
            let (year, month, day) = parse_date_components(s)?;
            Ok(Date {
                days: days_from_ymd_checked(year, month, day)?,
            })
        }
    }
}

impl ToPg for Date {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.days.to_be_bytes().to_vec(), oid::DATE, 1)
    }
}

/// Time type (microseconds since midnight)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Time {
    /// Microseconds since midnight
    pub usec: i64,
}

impl Time {
    /// Create from hours, minutes, seconds, microseconds.
    ///
    /// Invalid components return midnight instead of panicking. Use
    /// [`Time::try_new`] when invalid input must be reported to the caller.
    ///
    /// # Arguments
    ///
    /// * `hour` — Hour component (0–23).
    /// * `minute` — Minute component (0–59).
    /// * `second` — Second component (0–59).
    /// * `usec` — Microseconds within the current second.
    pub fn new(hour: u8, minute: u8, second: u8, usec: u32) -> Self {
        Self::try_new(hour, minute, second, usec).unwrap_or(Time { usec: 0 })
    }

    /// Fallible constructor from hours, minutes, seconds, microseconds.
    pub fn try_new(hour: u8, minute: u8, second: u8, usec: u32) -> Result<Self, TypeError> {
        validate_time_components(hour as i32, minute as i32, second as i32, usec as i64)?;
        Ok(Self {
            usec: hour as i64 * 3_600_000_000
                + minute as i64 * 60_000_000
                + second as i64 * 1_000_000
                + usec as i64,
        })
    }

    /// Get hours component (0-23)
    pub fn hour(&self) -> u8 {
        ((self.usec / 3_600_000_000) % 24) as u8
    }

    /// Get minutes component (0-59)
    pub fn minute(&self) -> u8 {
        ((self.usec / 60_000_000) % 60) as u8
    }

    /// Get seconds component (0-59)
    pub fn second(&self) -> u8 {
        ((self.usec / 1_000_000) % 60) as u8
    }

    /// Get microseconds component (0-999999)
    pub fn microsecond(&self) -> u32 {
        (self.usec % 1_000_000) as u32
    }
}

impl FromPg for Time {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::TIME {
            return Err(TypeError::UnexpectedOid {
                expected: "time",
                got: oid_val,
            });
        }

        if format == 1 {
            // Binary: 8 bytes, microseconds since midnight
            if bytes.len() != 8 {
                return Err(TypeError::InvalidData(
                    "Expected 8 bytes for time".to_string(),
                ));
            }
            let usec = i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            validate_time_usec(usec)?;
            Ok(Time { usec })
        } else {
            // Text format: HH:MM:SS or HH:MM:SS.ffffff
            let s =
                std::str::from_utf8(bytes).map_err(|e| TypeError::InvalidData(e.to_string()))?;
            parse_time_text(s)
        }
    }
}

impl ToPg for Time {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.usec.to_be_bytes().to_vec(), oid::TIME, 1)
    }
}

/// Parse PostgreSQL text time format
fn parse_time_text(s: &str) -> Result<Time, TypeError> {
    let (hour, minute, second, usec) = parse_time_components(s)?;

    Ok(Time {
        usec: hour as i64 * 3_600_000_000
            + minute as i64 * 60_000_000
            + second as i64 * 1_000_000
            + usec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "chrono")]
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_timestamp_unix_conversion() {
        // 2024-01-01 00:00:00 UTC
        let ts = Timestamp::from_unix_secs(1704067200);
        let back = ts.to_unix_secs();
        assert_eq!(back, 1704067200);
    }

    #[test]
    fn test_timestamp_extreme_conversion_saturates() {
        assert_eq!(
            Timestamp::from_unix_secs(i64::MAX).usec,
            i64::MAX.saturating_sub(PG_EPOCH_OFFSET_USEC)
        );
        assert_eq!(Timestamp::from_pg_usec(i64::MAX).to_unix_usec(), i64::MAX);
    }

    #[test]
    fn test_timestamp_from_pg_binary() {
        // Some arbitrary timestamp in binary
        let usec: i64 = 789_012_345_678_900; // ~25 years after 2000
        let bytes = usec.to_be_bytes();
        let ts = Timestamp::from_pg(&bytes, oid::TIMESTAMP, 1).unwrap();
        assert_eq!(ts.usec, usec);
    }

    #[test]
    fn test_date_from_pg_binary() {
        // 2024-01-01 = 8766 days since 2000-01-01
        let days: i32 = 8766;
        let bytes = days.to_be_bytes();
        let date = Date::from_pg(&bytes, oid::DATE, 1).unwrap();
        assert_eq!(date.days, days);
    }

    #[test]
    fn test_time_from_pg_binary() {
        // 12:30:45.123456 = 45045123456 microseconds
        let usec: i64 = 12 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000 + 123456;
        let bytes = usec.to_be_bytes();
        let time = Time::from_pg(&bytes, oid::TIME, 1).unwrap();
        assert_eq!(time.hour(), 12);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 45);
        assert_eq!(time.microsecond(), 123456);
    }

    #[test]
    fn test_time_from_pg_binary_rejects_out_of_range_values() {
        assert!(Time::from_pg(&(-1i64).to_be_bytes(), oid::TIME, 1).is_err());
        assert!(Time::from_pg(&USEC_PER_DAY.to_be_bytes(), oid::TIME, 1).is_err());
    }

    #[test]
    fn test_time_new_rejects_invalid_components() {
        assert!(Time::try_new(24, 0, 0, 0).is_err());
        assert!(Time::try_new(23, 60, 0, 0).is_err());
        assert!(Time::try_new(23, 59, 60, 0).is_err());
        assert!(Time::try_new(23, 59, 59, 1_000_000).is_err());
        assert_eq!(Time::new(24, 0, 0, 0), Time { usec: 0 });
    }

    #[test]
    fn test_time_from_pg_text() {
        let time = parse_time_text("14:30:00").unwrap();
        assert_eq!(time.hour(), 14);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 0);
    }

    #[test]
    fn test_timestamp_from_pg_text_preserves_time_components() {
        let ts = parse_timestamp_text("2024-12-25 17:30:45.123456").unwrap();
        let expected_days = days_from_ymd_checked(2024, 12, 25).unwrap() as i64;
        let expected_usec = expected_days * 86_400_000_000
            + 17 * 3_600_000_000
            + 30 * 60_000_000
            + 45 * 1_000_000
            + 123_456;
        assert_eq!(ts.usec, expected_usec);
    }

    #[test]
    fn test_timestamp_from_pg_text_rejects_invalid_components() {
        assert!(parse_timestamp_text("2024-12-25 xx:30:00").is_err());
        assert!(parse_timestamp_text("2024-12-25 17:bad:00").is_err());
        assert!(parse_timestamp_text("2024-12-25 17:30:bad").is_err());
        assert!(parse_timestamp_text("2024-13-25 17:30:00").is_err());
        assert!(parse_timestamp_text("2024-02-30 17:30:00").is_err());
        assert!(parse_timestamp_text("2024-12-25 17:30:00+bad").is_err());
        assert!(parse_timestamp_text("2024-12-25 17:30:00+25:00").is_err());
    }

    #[test]
    fn test_timestamp_from_pg_text_ignores_timezone_suffix_without_trimming_time() {
        let ts = parse_timestamp_text("2024-12-25 17:30:45+00").unwrap();
        let expected_days = days_from_ymd_checked(2024, 12, 25).unwrap() as i64;
        let expected_usec =
            expected_days * 86_400_000_000 + 17 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000;
        assert_eq!(ts.usec, expected_usec);
    }

    #[test]
    fn test_timestamp_from_pg_text_applies_timezone_offset() {
        let ts = parse_timestamp_text("2024-12-25 17:30:45+02:30").unwrap();
        let expected_days = days_from_ymd_checked(2024, 12, 25).unwrap() as i64;
        let expected_usec = expected_days * 86_400_000_000 + 15 * 3_600_000_000 + 45 * 1_000_000;
        assert_eq!(ts.usec, expected_usec);

        let negative = parse_timestamp_text("2024-12-25 17:30:45-0330").unwrap();
        let negative_expected =
            expected_days * 86_400_000_000 + 21 * 3_600_000_000 + 45 * 1_000_000;
        assert_eq!(negative.usec, negative_expected);
    }

    #[test]
    fn test_date_from_pg_text_rejects_invalid_components() {
        assert!(Date::from_pg(b"2024-13-01", oid::DATE, 0).is_err());
        assert!(Date::from_pg(b"2024-aa-01", oid::DATE, 0).is_err());
        assert!(Date::from_pg(b"2024-02-30", oid::DATE, 0).is_err());
    }

    #[test]
    fn test_time_from_pg_text_rejects_invalid_components() {
        assert!(parse_time_text("24:00:00").is_err());
        assert!(parse_time_text("14:60:00").is_err());
        assert!(parse_time_text("14:30:bad").is_err());
        assert!(parse_time_text("14:30:00.bad").is_err());
        assert!(parse_time_text("14:30:00.1234567").is_err());
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn test_chrono_datetime_from_pg_binary() {
        // PostgreSQL binary timestamp at Unix epoch.
        let pg_usec = -PG_EPOCH_OFFSET_USEC;
        let bytes = pg_usec.to_be_bytes();
        let dt = chrono::DateTime::<chrono::Utc>::from_pg(&bytes, oid::TIMESTAMPTZ, 1).unwrap();
        assert_eq!(dt.timestamp(), 0);
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn test_chrono_datetime_from_pg_text_timestamptz() {
        let dt = chrono::DateTime::<chrono::Utc>::from_pg(
            b"2024-12-25 17:30:00+00",
            oid::TIMESTAMPTZ,
            0,
        )
        .unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 12);
        assert_eq!(dt.day(), 25);
        assert_eq!(dt.hour(), 17);
        assert_eq!(dt.minute(), 30);
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn test_chrono_datetime_to_pg_binary() {
        let dt =
            chrono::DateTime::<chrono::Utc>::from_timestamp(1_704_067_200, 123_456_000).unwrap();
        let (bytes, oid_val, format) = dt.to_pg();
        assert_eq!(oid_val, oid::TIMESTAMPTZ);
        assert_eq!(format, 1);
        assert_eq!(bytes.len(), 8);
    }
}
