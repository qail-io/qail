//! FrontendMessage encoder — client-to-server wire format.

use super::types::*;

impl FrontendMessage {
    #[inline]
    fn has_nul(s: &str) -> bool {
        s.as_bytes().contains(&0)
    }

    #[inline]
    fn content_len_to_wire_len(content_len: usize) -> Result<i32, FrontendEncodeError> {
        let total = content_len
            .checked_add(4)
            .ok_or(FrontendEncodeError::MessageTooLarge(usize::MAX))?;
        i32::try_from(total).map_err(|_| FrontendEncodeError::MessageTooLarge(total))
    }

    /// Fallible encoder that returns explicit reason on invalid input.
    pub fn encode_checked(&self) -> Result<Vec<u8>, FrontendEncodeError> {
        match self {
            FrontendMessage::Startup {
                user,
                database,
                protocol_version,
                startup_params,
            } => {
                if Self::has_nul(user) {
                    return Err(FrontendEncodeError::InteriorNul("user"));
                }
                if Self::has_nul(database) {
                    return Err(FrontendEncodeError::InteriorNul("database"));
                }
                let mut seen_startup_keys = std::collections::HashSet::new();
                let mut buf = Vec::new();
                buf.extend_from_slice(&protocol_version.to_be_bytes());
                buf.extend_from_slice(b"user\0");
                buf.extend_from_slice(user.as_bytes());
                buf.push(0);
                buf.extend_from_slice(b"database\0");
                buf.extend_from_slice(database.as_bytes());
                buf.push(0);
                for (key, value) in startup_params {
                    let key_trimmed = key.trim();
                    if key_trimmed.is_empty() {
                        return Err(FrontendEncodeError::InvalidStartupParam(
                            "key must not be empty".to_string(),
                        ));
                    }
                    let key_lc = key_trimmed.to_ascii_lowercase();
                    if key_lc == "user" || key_lc == "database" {
                        return Err(FrontendEncodeError::InvalidStartupParam(format!(
                            "reserved key '{}'",
                            key_trimmed
                        )));
                    }
                    if !seen_startup_keys.insert(key_lc) {
                        return Err(FrontendEncodeError::InvalidStartupParam(format!(
                            "duplicate key '{}'",
                            key_trimmed
                        )));
                    }
                    if Self::has_nul(key) {
                        return Err(FrontendEncodeError::InteriorNul("startup_param_key"));
                    }
                    if Self::has_nul(value) {
                        return Err(FrontendEncodeError::InteriorNul("startup_param_value"));
                    }
                    buf.extend_from_slice(key.as_bytes());
                    buf.push(0);
                    buf.extend_from_slice(value.as_bytes());
                    buf.push(0);
                }
                buf.push(0);

                let len = Self::content_len_to_wire_len(buf.len())?;
                let mut result = len.to_be_bytes().to_vec();
                result.extend(buf);
                Ok(result)
            }
            FrontendMessage::Query(sql) => {
                if Self::has_nul(sql) {
                    return Err(FrontendEncodeError::InteriorNul("sql"));
                }
                let mut buf = Vec::new();
                buf.push(b'Q');
                let mut content = Vec::with_capacity(sql.len() + 1);
                content.extend_from_slice(sql.as_bytes());
                content.push(0);
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Terminate => Ok(vec![b'X', 0, 0, 0, 4]),
            FrontendMessage::SASLInitialResponse { mechanism, data } => {
                if Self::has_nul(mechanism) {
                    return Err(FrontendEncodeError::InteriorNul("mechanism"));
                }
                if data.len() > i32::MAX as usize {
                    return Err(FrontendEncodeError::MessageTooLarge(data.len()));
                }
                let mut buf = Vec::new();
                buf.push(b'p');

                let mut content = Vec::new();
                content.extend_from_slice(mechanism.as_bytes());
                content.push(0);
                let data_len = i32::try_from(data.len())
                    .map_err(|_| FrontendEncodeError::MessageTooLarge(data.len()))?;
                content.extend_from_slice(&data_len.to_be_bytes());
                content.extend_from_slice(data);

                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::SASLResponse(data) | FrontendMessage::GSSResponse(data) => {
                if data.len() > i32::MAX as usize {
                    return Err(FrontendEncodeError::MessageTooLarge(data.len()));
                }
                let mut buf = Vec::new();
                buf.push(b'p');
                let len = Self::content_len_to_wire_len(data.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(data);
                Ok(buf)
            }
            FrontendMessage::PasswordMessage(password) => {
                if Self::has_nul(password) {
                    return Err(FrontendEncodeError::InteriorNul("password"));
                }
                let mut buf = Vec::new();
                buf.push(b'p');
                let mut content = Vec::with_capacity(password.len() + 1);
                content.extend_from_slice(password.as_bytes());
                content.push(0);
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Parse {
                name,
                query,
                param_types,
            } => {
                if Self::has_nul(name) {
                    return Err(FrontendEncodeError::InteriorNul("name"));
                }
                if Self::has_nul(query) {
                    return Err(FrontendEncodeError::InteriorNul("query"));
                }
                if param_types.len() > i16::MAX as usize {
                    return Err(FrontendEncodeError::TooManyParams(param_types.len()));
                }
                let mut buf = Vec::new();
                buf.push(b'P');

                let mut content = Vec::new();
                content.extend_from_slice(name.as_bytes());
                content.push(0);
                content.extend_from_slice(query.as_bytes());
                content.push(0);
                let param_count = i16::try_from(param_types.len())
                    .map_err(|_| FrontendEncodeError::TooManyParams(param_types.len()))?;
                content.extend_from_slice(&param_count.to_be_bytes());
                for oid in param_types {
                    content.extend_from_slice(&oid.to_be_bytes());
                }

                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Bind {
                portal,
                statement,
                params,
            } => {
                if Self::has_nul(portal) {
                    return Err(FrontendEncodeError::InteriorNul("portal"));
                }
                if Self::has_nul(statement) {
                    return Err(FrontendEncodeError::InteriorNul("statement"));
                }
                if params.len() > i16::MAX as usize {
                    return Err(FrontendEncodeError::TooManyParams(params.len()));
                }
                if let Some(too_large) = params
                    .iter()
                    .flatten()
                    .find(|p| p.len() > i32::MAX as usize)
                {
                    return Err(FrontendEncodeError::MessageTooLarge(too_large.len()));
                }

                let mut buf = Vec::new();
                buf.push(b'B');

                let mut content = Vec::new();
                content.extend_from_slice(portal.as_bytes());
                content.push(0);
                content.extend_from_slice(statement.as_bytes());
                content.push(0);
                content.extend_from_slice(&0i16.to_be_bytes());
                let param_count = i16::try_from(params.len())
                    .map_err(|_| FrontendEncodeError::TooManyParams(params.len()))?;
                content.extend_from_slice(&param_count.to_be_bytes());
                for param in params {
                    match param {
                        Some(data) => {
                            let data_len = i32::try_from(data.len())
                                .map_err(|_| FrontendEncodeError::MessageTooLarge(data.len()))?;
                            content.extend_from_slice(&data_len.to_be_bytes());
                            content.extend_from_slice(data);
                        }
                        None => content.extend_from_slice(&(-1i32).to_be_bytes()),
                    }
                }
                content.extend_from_slice(&0i16.to_be_bytes());

                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Execute { portal, max_rows } => {
                if Self::has_nul(portal) {
                    return Err(FrontendEncodeError::InteriorNul("portal"));
                }
                if *max_rows < 0 {
                    return Err(FrontendEncodeError::InvalidMaxRows(*max_rows));
                }
                let mut buf = Vec::new();
                buf.push(b'E');
                let mut content = Vec::new();
                content.extend_from_slice(portal.as_bytes());
                content.push(0);
                content.extend_from_slice(&max_rows.to_be_bytes());
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Sync => Ok(vec![b'S', 0, 0, 0, 4]),
            FrontendMessage::CopyFail(msg) => {
                if Self::has_nul(msg) {
                    return Err(FrontendEncodeError::InteriorNul("copy_fail"));
                }
                let mut buf = Vec::new();
                buf.push(b'f');
                let mut content = Vec::with_capacity(msg.len() + 1);
                content.extend_from_slice(msg.as_bytes());
                content.push(0);
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Close { is_portal, name } => {
                if Self::has_nul(name) {
                    return Err(FrontendEncodeError::InteriorNul("name"));
                }
                let mut buf = Vec::new();
                buf.push(b'C');
                let type_byte = if *is_portal { b'P' } else { b'S' };
                let mut content = vec![type_byte];
                content.extend_from_slice(name.as_bytes());
                content.push(0);
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
        }
    }
}
