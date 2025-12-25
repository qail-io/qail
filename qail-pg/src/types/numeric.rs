//! NUMERIC/DECIMAL type support for PostgreSQL.
//!
//! PostgreSQL NUMERIC is a variable-precision type stored in a complex binary format.
//! For simplicity, we use String representation and convert on demand.

use super::{FromPg, ToPg, TypeError};
use crate::protocol::types::oid;

/// NUMERIC/DECIMAL type (stored as string for precision)
#[derive(Debug, Clone, PartialEq)]
pub struct Numeric(pub String);

impl Numeric {
    /// Create from string representation
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Parse as f64 (may lose precision for very large numbers)
    pub fn to_f64(&self) -> Result<f64, std::num::ParseFloatError> {
        self.0.parse()
    }

    /// Parse as i64 (truncates decimal part)
    pub fn to_i64(&self) -> Result<i64, std::num::ParseIntError> {
        // Remove decimal part if present
        let int_part = self.0.split('.').next().unwrap_or("0");
        int_part.parse()
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromPg for Numeric {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::NUMERIC {
            return Err(TypeError::UnexpectedOid { expected: "numeric", got: oid_val });
        }

        if format == 1 {
            // Binary format: complex packed decimal format
            // For now, we don't support binary NUMERIC - it requires unpacking
            // the PostgreSQL packed decimal format (ndigits, weight, sign, dscale, digits)
            decode_numeric_binary(bytes)
        } else {
            // Text format: just the string
            let s = std::str::from_utf8(bytes)
                .map_err(|e| TypeError::InvalidData(e.to_string()))?;
            Ok(Numeric(s.to_string()))
        }
    }
}

impl ToPg for Numeric {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        // Send as text for simplicity
        (self.0.as_bytes().to_vec(), oid::NUMERIC, 0)
    }
}

/// Decode PostgreSQL binary NUMERIC format
fn decode_numeric_binary(bytes: &[u8]) -> Result<Numeric, TypeError> {
    if bytes.len() < 8 {
        return Err(TypeError::InvalidData("NUMERIC too short".to_string()));
    }

    // PostgreSQL NUMERIC binary format:
    // 2 bytes: ndigits (number of base-10000 digits)
    // 2 bytes: weight (position of first digit relative to decimal point)
    // 2 bytes: sign (0=pos, 0x4000=neg, 0xC000=NaN)
    // 2 bytes: dscale (number of decimal digits after decimal point)
    // ndigits * 2 bytes: digits (each 0-9999)

    let ndigits = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = u16::from_be_bytes([bytes[6], bytes[7]]) as usize;

    if bytes.len() < 8 + ndigits * 2 {
        return Err(TypeError::InvalidData("NUMERIC truncated".to_string()));
    }

    // Handle special cases
    if sign == 0xC000 {
        return Ok(Numeric("NaN".to_string()));
    }

    if ndigits == 0 {
        return Ok(Numeric("0".to_string()));
    }

    // Extract digits
    let mut digits = Vec::with_capacity(ndigits);
    for i in 0..ndigits {
        let d = u16::from_be_bytes([bytes[8 + i * 2], bytes[9 + i * 2]]);
        digits.push(d);
    }

    // Build string representation
    let mut result = String::new();
    if sign == 0x4000 {
        result.push('-');
    }

    // Integer part
    let int_digits = (weight + 1) as usize;
    for i in 0..int_digits.min(ndigits) {
        if i == 0 {
            result.push_str(&digits[i].to_string());
        } else {
            result.push_str(&format!("{:04}", digits[i]));
        }
    }
    // Pad with zeros if weight > ndigits
    for _ in ndigits..int_digits {
        result.push_str("0000");
    }

    if result.is_empty() || result == "-" {
        result.push('0');
    }

    // Decimal part
    if dscale > 0 {
        result.push('.');
        let start = int_digits.max(0);
        let mut decimal_digits = 0;
        for i in start..ndigits {
            let s = format!("{:04}", digits[i]);
            for c in s.chars() {
                if decimal_digits >= dscale {
                    break;
                }
                result.push(c);
                decimal_digits += 1;
            }
        }
        // Pad with zeros if needed
        while decimal_digits < dscale {
            result.push('0');
            decimal_digits += 1;
        }
    }

    Ok(Numeric(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_from_text() {
        let n = Numeric::from_pg(b"123.456", oid::NUMERIC, 0).unwrap();
        assert_eq!(n.0, "123.456");
        assert!((n.to_f64().unwrap() - 123.456).abs() < 0.0001);
    }

    #[test]
    fn test_numeric_to_i64() {
        let n = Numeric::new("12345.67");
        assert_eq!(n.to_i64().unwrap(), 12345);
    }

    #[test]
    fn test_numeric_negative() {
        let n = Numeric::new("-999.99");
        assert_eq!(n.to_f64().unwrap(), -999.99);
    }
}
