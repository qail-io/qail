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

    /// Parse as i64 only when the numeric value has no non-zero decimal part.
    pub fn to_i64_exact(&self) -> Result<i64, std::num::ParseIntError> {
        if !self.is_integral() {
            return self.0.parse();
        }
        self.0.split('.').next().unwrap_or("0").parse::<i64>()
    }

    /// Whether the decimal representation has no non-zero fractional digits.
    pub fn is_integral(&self) -> bool {
        let Some((_, fractional)) = self.0.split_once('.') else {
            return true;
        };
        !fractional.is_empty() && fractional.bytes().all(|b| b == b'0')
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromPg for Numeric {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::NUMERIC {
            return Err(TypeError::UnexpectedOid {
                expected: "numeric",
                got: oid_val,
            });
        }

        if format == 1 {
            // Binary format: complex packed decimal format
            // For now, we don't support binary NUMERIC - it requires unpacking
            // the PostgreSQL packed decimal format (ndigits, weight, sign, dscale, digits)
            decode_numeric_binary(bytes)
        } else {
            // Text format: just the string
            let s =
                std::str::from_utf8(bytes).map_err(|e| TypeError::InvalidData(e.to_string()))?;
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

    if sign == 0xC000 {
        return Ok(Numeric("NaN".to_string()));
    }
    if !matches!(sign, 0 | 0x4000) {
        return Err(TypeError::InvalidData(format!(
            "NUMERIC sign out of range: {sign:#06x}"
        )));
    }

    if ndigits == 0 {
        return Ok(Numeric("0".to_string()));
    }

    let mut digits = Vec::with_capacity(ndigits);
    for i in 0..ndigits {
        let d = u16::from_be_bytes([bytes[8 + i * 2], bytes[9 + i * 2]]);
        if d > 9999 {
            return Err(TypeError::InvalidData(format!(
                "NUMERIC digit out of range: {}",
                d
            )));
        }
        digits.push(d);
    }

    let mut result = String::new();
    if sign == 0x4000 {
        result.push('-');
    }

    // Integer part. A negative weight means every stored base-10000 digit is
    // fractional; do not cast it to usize or it wraps to a huge group count.
    let int_digits = i32::from(weight) + 1;
    if int_digits > 0 {
        let int_digits = int_digits as usize;
        for i in 0..int_digits {
            let digit = digits.get(i).copied().unwrap_or(0);
            if i == 0 {
                result.push_str(&digit.to_string());
            } else {
                result.push_str(&format!("{:04}", digit));
            }
        }
    }

    if result.is_empty() || result == "-" {
        result.push('0');
    }

    // Decimal part
    if dscale > 0 {
        result.push('.');
        let mut fractional = String::new();
        if int_digits < 0 {
            for _ in 0..(-int_digits) {
                fractional.push_str("0000");
            }
        }

        let start = int_digits.max(0) as usize;
        for digit in digits.iter().skip(start) {
            fractional.push_str(&format!("{:04}", digit));
        }

        if fractional.len() < dscale {
            fractional.extend(std::iter::repeat_n('0', dscale - fractional.len()));
        } else {
            fractional.truncate(dscale);
        }
        result.push_str(&fractional);
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
    fn test_numeric_to_i64_exact_rejects_fractional_values() {
        assert_eq!(Numeric::new("12345.00").to_i64_exact().unwrap(), 12345);
        assert!(Numeric::new("12345.67").to_i64_exact().is_err());
    }

    #[test]
    fn test_numeric_negative() {
        let n = Numeric::new("-999.99");
        assert_eq!(n.to_f64().unwrap(), -999.99);
    }

    #[test]
    fn test_numeric_binary_decodes_negative_weight() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u16.to_be_bytes()); // ndigits
        bytes.extend_from_slice(&(-2i16).to_be_bytes()); // weight
        bytes.extend_from_slice(&0u16.to_be_bytes()); // sign
        bytes.extend_from_slice(&8u16.to_be_bytes()); // dscale
        bytes.extend_from_slice(&1u16.to_be_bytes()); // digit

        let n = Numeric::from_pg(&bytes, oid::NUMERIC, 1).unwrap();
        assert_eq!(n.as_str(), "0.00000001");
    }

    #[test]
    fn test_numeric_binary_rejects_out_of_range_digits() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u16.to_be_bytes()); // ndigits
        bytes.extend_from_slice(&0i16.to_be_bytes()); // weight
        bytes.extend_from_slice(&0u16.to_be_bytes()); // sign
        bytes.extend_from_slice(&0u16.to_be_bytes()); // dscale
        bytes.extend_from_slice(&10000u16.to_be_bytes()); // invalid base-10000 digit

        let err = Numeric::from_pg(&bytes, oid::NUMERIC, 1).unwrap_err();
        assert!(matches!(err, TypeError::InvalidData(msg) if msg.contains("out of range")));
    }

    #[test]
    fn test_numeric_binary_rejects_unknown_sign_code() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u16.to_be_bytes()); // ndigits
        bytes.extend_from_slice(&0i16.to_be_bytes()); // weight
        bytes.extend_from_slice(&0x2000u16.to_be_bytes()); // invalid sign
        bytes.extend_from_slice(&0u16.to_be_bytes()); // dscale
        bytes.extend_from_slice(&123u16.to_be_bytes()); // digit

        let err = Numeric::from_pg(&bytes, oid::NUMERIC, 1).unwrap_err();
        assert!(matches!(err, TypeError::InvalidData(msg) if msg.contains("sign out of range")));
    }
}
