//! Timestamp type conversions for PostgreSQL.
//!
//! PostgreSQL timestamps are stored as microseconds since 2000-01-01 00:00:00 UTC.

use super::{FromPg, ToPg, TypeError};
use crate::protocol::types::oid;

/// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
/// Difference from Unix epoch (1970-01-01) in microseconds
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;

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
            usec: secs * 1_000_000 - PG_EPOCH_OFFSET_USEC,
        }
    }
    
    /// Convert to Unix timestamp (seconds since 1970-01-01)
    pub fn to_unix_secs(&self) -> i64 {
        (self.usec + PG_EPOCH_OFFSET_USEC) / 1_000_000
    }
    
    /// Convert to Unix timestamp with microseconds
    pub fn to_unix_usec(&self) -> i64 {
        self.usec + PG_EPOCH_OFFSET_USEC
    }
}

impl FromPg for Timestamp {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::TIMESTAMP && oid_val != oid::TIMESTAMPTZ {
            return Err(TypeError::UnexpectedOid { expected: "timestamp", got: oid_val });
        }
        
        if format == 1 {
            // Binary: 8 bytes, microseconds since 2000-01-01
            if bytes.len() != 8 {
                return Err(TypeError::InvalidData("Expected 8 bytes for timestamp".to_string()));
            }
            let usec = i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            Ok(Timestamp::from_pg_usec(usec))
        } else {
            // Text format: parse ISO 8601
            let s = std::str::from_utf8(bytes)
                .map_err(|e| TypeError::InvalidData(e.to_string()))?;
            parse_timestamp_text(s)
        }
    }
}

impl ToPg for Timestamp {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.usec.to_be_bytes().to_vec(), oid::TIMESTAMP, 1)
    }
}

/// Parse PostgreSQL text timestamp format
fn parse_timestamp_text(s: &str) -> Result<Timestamp, TypeError> {
    // Format: "2024-12-25 17:30:00" or "2024-12-25 17:30:00.123456"
    // This is a simplified parser - production would use chrono or time crate
    
    let parts: Vec<&str> = s.split(&[' ', 'T'][..]).collect();
    if parts.len() < 2 {
        return Err(TypeError::InvalidData(format!("Invalid timestamp: {}", s)));
    }
    
    let date_parts: Vec<i32> = parts[0]
        .split('-')
        .filter_map(|p| p.parse().ok())
        .collect();
    
    if date_parts.len() != 3 {
        return Err(TypeError::InvalidData(format!("Invalid date: {}", parts[0])));
    }
    
    let time_str = parts[1].trim_end_matches(|c: char| c == '+' || c == '-' || c.is_ascii_digit() || c == ':');
    let time_parts: Vec<&str> = time_str.split(':').collect();
    
    let hour: i32 = time_parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
    let minute: i32 = time_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let second_str = time_parts.get(2).unwrap_or(&"0");
    let sec_parts: Vec<&str> = second_str.split('.').collect();
    let second: i32 = sec_parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
    let usec: i64 = sec_parts.get(1)
        .map(|s| {
            let padded = format!("{:0<6}", s);
            padded[..6].parse::<i64>().unwrap_or(0)
        })
        .unwrap_or(0);
    
    // Calculate days since 2000-01-01
    let year = date_parts[0];
    let month = date_parts[1];
    let day = date_parts[2];
    
    // Simplified calculation (not accounting for all leap years correctly)
    let days_since_epoch = days_from_ymd(year, month, day);
    
    let total_usec = days_since_epoch as i64 * 86_400_000_000
        + hour as i64 * 3_600_000_000
        + minute as i64 * 60_000_000
        + second as i64 * 1_000_000
        + usec;
    
    Ok(Timestamp::from_pg_usec(total_usec))
}

/// Calculate days since 2000-01-01
fn days_from_ymd(year: i32, month: i32, day: i32) -> i32 {
    // Days from 2000-01-01 to given date
    let mut days = 0;
    
    // Years
    for y in 2000..year {
        days += if is_leap_year(y) { 366 } else { 365 };
    }
    for y in year..2000 {
        days -= if is_leap_year(y) { 366 } else { 365 };
    }
    
    // Months
    let days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..month {
        days += days_in_month[(m - 1) as usize];
        if m == 2 && is_leap_year(year) {
            days += 1;
        }
    }
    
    // Days
    days += day - 1;
    
    days
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Date type (days since 2000-01-01)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date {
    pub days: i32,
}

impl FromPg for Date {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::DATE {
            return Err(TypeError::UnexpectedOid { expected: "date", got: oid_val });
        }
        
        if format == 1 {
            // Binary: 4 bytes, days since 2000-01-01
            if bytes.len() != 4 {
                return Err(TypeError::InvalidData("Expected 4 bytes for date".to_string()));
            }
            let days = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            Ok(Date { days })
        } else {
            // Text format: YYYY-MM-DD
            let s = std::str::from_utf8(bytes)
                .map_err(|e| TypeError::InvalidData(e.to_string()))?;
            let parts: Vec<i32> = s.split('-').filter_map(|p| p.parse().ok()).collect();
            if parts.len() != 3 {
                return Err(TypeError::InvalidData(format!("Invalid date: {}", s)));
            }
            Ok(Date { days: days_from_ymd(parts[0], parts[1], parts[2]) })
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
    /// Create from hours, minutes, seconds, microseconds
    pub fn new(hour: u8, minute: u8, second: u8, usec: u32) -> Self {
        Self {
            usec: hour as i64 * 3_600_000_000
                + minute as i64 * 60_000_000
                + second as i64 * 1_000_000
                + usec as i64,
        }
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
            return Err(TypeError::UnexpectedOid { expected: "time", got: oid_val });
        }

        if format == 1 {
            // Binary: 8 bytes, microseconds since midnight
            if bytes.len() != 8 {
                return Err(TypeError::InvalidData("Expected 8 bytes for time".to_string()));
            }
            let usec = i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            Ok(Time { usec })
        } else {
            // Text format: HH:MM:SS or HH:MM:SS.ffffff
            let s = std::str::from_utf8(bytes)
                .map_err(|e| TypeError::InvalidData(e.to_string()))?;
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
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() < 2 {
        return Err(TypeError::InvalidData(format!("Invalid time: {}", s)));
    }

    let hour: i64 = parts[0].parse().map_err(|_| TypeError::InvalidData("Invalid hour".to_string()))?;
    let minute: i64 = parts[1].parse().map_err(|_| TypeError::InvalidData("Invalid minute".to_string()))?;

    let (second, usec) = if parts.len() > 2 {
        let sec_parts: Vec<&str> = parts[2].split('.').collect();
        let sec: i64 = sec_parts[0].parse().unwrap_or(0);
        let us: i64 = sec_parts.get(1)
            .map(|s| {
                let padded = format!("{:0<6}", s);
                padded[..6].parse::<i64>().unwrap_or(0)
            })
            .unwrap_or(0);
        (sec, us)
    } else {
        (0, 0)
    };

    Ok(Time {
        usec: hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + usec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_unix_conversion() {
        // 2024-01-01 00:00:00 UTC
        let ts = Timestamp::from_unix_secs(1704067200);
        let back = ts.to_unix_secs();
        assert_eq!(back, 1704067200);
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
    fn test_time_from_pg_text() {
        let time = parse_time_text("14:30:00").unwrap();
        assert_eq!(time.hour(), 14);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 0);
    }
}

