//! SCRAM-SHA-256 Authentication
//!
//! Implements the SASL SCRAM-SHA-256 authentication mechanism for PostgreSQL.
//! Reference: RFC 5802, PostgreSQL SASL documentation.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, Mac};
use sha2::{Sha256, Digest};
use rand::Rng;

type HmacSha256 = Hmac<Sha256>;

/// SCRAM-SHA-256 client state machine.
pub struct ScramClient {
    /// Username (normalized)
    username: String,
    /// Password
    password: String,
    /// Client nonce (random)
    client_nonce: String,
    /// Combined nonce (client + server)
    combined_nonce: Option<String>,
    /// Salt from server
    salt: Option<Vec<u8>>,
    /// Iteration count from server
    iterations: Option<u32>,
    /// Auth message for signature verification
    auth_message: Option<String>,
    /// Salted password (cached for verification)
    salted_password: Option<Vec<u8>>,
}

impl ScramClient {
    /// Create a new SCRAM client for authentication.
    pub fn new(username: &str, password: &str) -> Self {
        // Generate random nonce
        let mut rng = rand::rng();
        let nonce: String = (0..24)
            .map(|_| {
                let idx = rng.random_range(0..62);
                let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                chars[idx] as char
            })
            .collect();

        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: nonce,
            combined_nonce: None,
            salt: None,
            iterations: None,
            auth_message: None,
            salted_password: None,
        }
    }

    /// Generate the client-first-message.
    ///
    /// Format: `n,,n=<user>,r=<nonce>`
    pub fn client_first_message(&self) -> Vec<u8> {
        // GS2 header: n,, (no channel binding)
        // client-first-message-bare: n=<user>,r=<nonce>
        let msg = format!("n,,n={},r={}", self.username, self.client_nonce);
        msg.into_bytes()
    }

    /// Get the client-first-message-bare (for auth message construction).
    fn client_first_message_bare(&self) -> String {
        format!("n={},r={}", self.username, self.client_nonce)
    }

    /// Process the server-first-message and generate client-final-message.
    ///
    /// Server-first-message format: `r=<nonce>,s=<salt>,i=<iterations>`
    /// Returns the client-final-message.
    pub fn process_server_first(&mut self, server_msg: &[u8]) -> Result<Vec<u8>, String> {
        let server_str = std::str::from_utf8(server_msg)
            .map_err(|_| "Invalid UTF-8 in server message")?;

        // Parse server-first-message
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;

        for part in server_str.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(BASE64.decode(value).map_err(|_| "Invalid salt base64")?);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = Some(value.parse::<u32>().map_err(|_| "Invalid iteration count")?);
            }
        }

        let nonce = nonce.ok_or("Missing nonce in server message")?;
        let salt = salt.ok_or("Missing salt in server message")?;
        let iterations = iterations.ok_or("Missing iterations in server message")?;

        // Verify nonce starts with our client nonce
        if !nonce.starts_with(&self.client_nonce) {
            return Err("Server nonce doesn't contain client nonce".to_string());
        }

        self.combined_nonce = Some(nonce.clone());
        self.salt = Some(salt.clone());
        self.iterations = Some(iterations);

        // Derive salted password using PBKDF2
        let salted_password = self.derive_salted_password(&salt, iterations);
        self.salted_password = Some(salted_password.clone());

        // Compute keys
        let client_key = self.hmac(&salted_password, b"Client Key");
        let stored_key = Self::sha256(&client_key);

        // Build auth message
        let client_first_bare = self.client_first_message_bare();
        let client_final_without_proof = format!("c=biws,r={}", nonce); // biws = base64("n,,")
        let auth_message = format!("{},{},{}", client_first_bare, server_str, client_final_without_proof);
        self.auth_message = Some(auth_message.clone());

        // Compute proof
        let client_signature = self.hmac(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key.iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        // Build client-final-message
        let proof_b64 = BASE64.encode(&client_proof);
        let client_final = format!("{},p={}", client_final_without_proof, proof_b64);

        Ok(client_final.into_bytes())
    }

    /// Verify the server-final-message (server signature).
    pub fn verify_server_final(&self, server_msg: &[u8]) -> Result<(), String> {
        let server_str = std::str::from_utf8(server_msg)
            .map_err(|_| "Invalid UTF-8 in server final message")?;

        // Parse verifier
        let verifier = server_str.strip_prefix("v=")
            .ok_or("Missing verifier in server final message")?;

        let expected_signature = BASE64.decode(verifier)
            .map_err(|_| "Invalid base64 in server signature")?;

        // Compute expected server signature
        let salted_password = self.salted_password.as_ref()
            .ok_or("Missing salted password")?;
        let auth_message = self.auth_message.as_ref()
            .ok_or("Missing auth message")?;

        let server_key = self.hmac(salted_password, b"Server Key");
        let computed_signature = self.hmac(&server_key, auth_message.as_bytes());

        if computed_signature != expected_signature {
            return Err("Server signature verification failed".to_string());
        }

        Ok(())
    }

    /// Derive salted password using PBKDF2-SHA256.
    fn derive_salted_password(&self, salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut output = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            self.password.as_bytes(),
            salt,
            iterations,
            &mut output,
        );
        output.to_vec()
    }

    /// Compute HMAC-SHA256.
    fn hmac(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Compute SHA-256 hash.
    fn sha256(data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_first_message() {
        let client = ScramClient::new("user", "password");
        let msg = client.client_first_message();
        let msg_str = String::from_utf8(msg).unwrap();
        
        assert!(msg_str.starts_with("n,,n=user,r="));
    }

    #[test]
    fn test_scram_flow() {
        let mut client = ScramClient::new("testuser", "testpass");
        
        // Client sends first message
        let first = client.client_first_message();
        assert!(String::from_utf8(first).unwrap().contains("n=testuser"));
        
        // Simulate server response (in real scenario, server generates this)
        // Format: r=<combined_nonce>,s=<salt_base64>,i=<iterations>
        let server_nonce = format!("{}ServerPart", client.client_nonce);
        let salt_b64 = BASE64.encode(b"randomsalt");
        let server_first = format!("r={},s={},i=4096", server_nonce, salt_b64);
        
        // Client processes and generates final message
        let final_msg = client.process_server_first(server_first.as_bytes()).unwrap();
        let final_str = String::from_utf8(final_msg).unwrap();
        
        assert!(final_str.starts_with("c=biws,r="));
        assert!(final_str.contains(",p="));
    }
}
