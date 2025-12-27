//! MySQL authentication helpers.
//!
//! Implements:
//! - mysql_native_password (SHA1-based, legacy)
//! - caching_sha2_password (SHA256-based, MySQL 8+)

use sha1::{Sha1, Digest as Sha1Digest};
use sha2::{Sha256, Digest as Sha256Digest};

/// Compute mysql_native_password authentication response (legacy).
///
/// Formula: SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
pub fn mysql_native_password(password: &[u8], scramble: &[u8]) -> [u8; 20] {
    let hash1 = Sha1::digest(password);
    let hash2 = Sha1::digest(&hash1);
    
    let mut hasher = Sha1::new();
    hasher.update(scramble);
    hasher.update(&hash2);
    let hash3 = hasher.finalize();
    
    let mut result = [0u8; 20];
    for i in 0..20 {
        result[i] = hash1[i] ^ hash3[i];
    }
    result
}

/// Compute caching_sha2_password authentication response (MySQL 8+).
///
/// Formula: SHA256(password) XOR SHA256(SHA256(SHA256(password)) + scramble)
pub fn caching_sha2_password(password: &[u8], scramble: &[u8]) -> [u8; 32] {
    // SHA256(password)
    let hash1 = Sha256::digest(password);
    
    // SHA256(SHA256(password))
    let hash2 = Sha256::digest(&hash1);
    
    // SHA256(SHA256(SHA256(password)) + scramble)
    let mut hasher = Sha256::new();
    hasher.update(&hash2);
    hasher.update(scramble);
    let hash3 = hasher.finalize();
    
    // XOR
    let mut result = [0u8; 32];
    for i in 0..32 {
        result[i] = hash1[i] ^ hash3[i];
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_native_password() {
        let result = mysql_native_password(b"password", b"12345678901234567890");
        assert_eq!(result.len(), 20);
    }
    
    #[test]
    fn test_caching_sha2_password() {
        let result = caching_sha2_password(b"password", b"12345678901234567890");
        assert_eq!(result.len(), 32);
    }
}
