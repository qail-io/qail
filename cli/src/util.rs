//! Utility functions for qail-cli

use anyhow::Result;
use url::Url;

/// Projection used by existence probes (`SELECT 1 ... LIMIT 1`).
pub fn qail_exists_projection() -> qail_core::ast::Expr {
    qail_core::prelude::int(1)
}

/// Parse a PostgreSQL URL into (host, port, user, password, database).
///
/// Handles: `postgres://user:pass@host:port/database`
///
/// # Arguments
///
/// * `url` — Full connection string starting with `postgres://` or `postgresql://`.
pub fn parse_pg_url(url: &str) -> Result<(String, u16, String, Option<String>, String)> {
    let parsed = Url::parse(url).map_err(|e| anyhow::anyhow!("Invalid PostgreSQL URL: {e}"))?;
    if !matches!(parsed.scheme(), "postgres" | "postgresql") {
        return Err(anyhow::anyhow!(
            "URL must start with postgres:// or postgresql://"
        ));
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Missing host in URL"))?
        .to_string();
    let port = parsed.port().unwrap_or(5432);
    let has_userinfo = url
        .split_once("://")
        .and_then(|(_, rest)| rest.split('/').next())
        .is_some_and(|authority| authority.contains('@'));
    let user = if parsed.username().is_empty() {
        if has_userinfo {
            anyhow::bail!("Missing user in URL");
        }
        "postgres".to_string()
    } else {
        percent_decode(parsed.username())?
    };
    let password = parsed.password().map(percent_decode).transpose()?;
    let database = percent_decode(parsed.path().trim_start_matches('/'))?;
    if database.is_empty() {
        return Err(anyhow::anyhow!("Missing database in URL"));
    }

    Ok((host, port, user, password, database))
}

/// Parse a generic URL into (scheme, host, port, path).
///
/// Used by `exec.rs` for SSH tunnel URL rewriting.
///
/// # Arguments
///
/// * `url` — URL string containing `scheme://[userinfo@]host[:port]/path`.
pub fn parse_url_parts(url: &str) -> Result<(String, String, u16, String)> {
    let parsed = Url::parse(url).map_err(|e| anyhow::anyhow!("Invalid URL: {e}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Missing host in URL"))?
        .to_string();
    let port = parsed
        .port_or_known_default()
        .unwrap_or_else(|| default_port(parsed.scheme()));
    let mut path = parsed.path().to_string();
    if path.is_empty() {
        path.push('/');
    }
    if let Some(query) = parsed.query() {
        path.push('?');
        path.push_str(query);
    }

    Ok((parsed.scheme().to_string(), host, port, path))
}

/// Rewrite a URL to point at a different host:port (for SSH tunneling).
pub fn rewrite_url_host(url: &str, new_host: &str, new_port: u16) -> Result<String> {
    let mut parsed = Url::parse(url).map_err(|e| anyhow::anyhow!("Invalid URL: {e}"))?;
    parsed
        .set_host(Some(new_host))
        .map_err(|_| anyhow::anyhow!("Invalid replacement host: {new_host}"))?;
    parsed
        .set_port(Some(new_port))
        .map_err(|_| anyhow::anyhow!("Invalid replacement port: {new_port}"))?;

    Ok(parsed.to_string())
}

/// Redact the password from a credentialed URL.
///
/// `postgres://user:secret@host:5432/db` → `postgres://user:***@host:5432/db`
/// Returns the original string unchanged if there is no password.
pub fn redact_url(url: &str) -> String {
    if let Ok(mut parsed) = Url::parse(url)
        && parsed.password().is_some()
        && parsed.set_password(Some("***")).is_ok()
    {
        return parsed.to_string();
    }

    // Find the scheme separator
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    // Check for userinfo@host
    let Some((userinfo, hostpart)) = rest.split_once('@') else {
        return url.to_string(); // no @ → no credentials
    };
    // Check for user:password
    if let Some((user, _password)) = userinfo.split_once(':') {
        format!("{}://{}:***@{}", scheme, user, hostpart)
    } else {
        url.to_string() // no password in userinfo
    }
}

fn default_port(scheme: &str) -> u16 {
    match scheme {
        "http" => 80,
        "https" => 443,
        "postgres" | "postgresql" => 5432,
        _ => 5432,
    }
}

fn percent_decode(s: &str) -> Result<String> {
    fn hex_value(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                anyhow::bail!("Invalid percent encoding: '%' must be followed by two hex digits");
            }
            let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) else {
                anyhow::bail!("Invalid percent encoding: '%' must be followed by two hex digits");
            };
            decoded.push((hi << 4) | lo);
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }

    String::from_utf8(decoded)
        .map_err(|_| anyhow::anyhow!("Invalid percent encoding: decoded value is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qail_exists_projection_encodes_as_literal_select_one() {
        let cmd = qail_core::prelude::Qail::get("users")
            .column_expr(qail_exists_projection())
            .limit(1);
        let (sql, params) = qail_pg::protocol::AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert_eq!(sql, "SELECT 1 FROM users LIMIT 1");
        assert!(params.is_empty());
    }

    #[test]
    fn test_redact_url_with_password() {
        assert_eq!(
            redact_url("postgres://admin:s3cret@db.example.com:5432/mydb"),
            "postgres://admin:***@db.example.com:5432/mydb"
        );
    }

    #[test]
    fn test_redact_url_no_password() {
        assert_eq!(
            redact_url("postgres://admin@localhost/mydb"),
            "postgres://admin@localhost/mydb"
        );
    }

    #[test]
    fn test_redact_url_no_userinfo() {
        assert_eq!(
            redact_url("postgres://localhost/mydb"),
            "postgres://localhost/mydb"
        );
    }

    #[test]
    fn test_redact_url_preserves_query_params() {
        assert_eq!(
            redact_url("postgres://user:pass@host:5432/db?max_connections=10"),
            "postgres://user:***@host:5432/db?max_connections=10"
        );
    }

    #[test]
    fn test_redact_url_hides_percent_encoded_password() {
        let redacted = redact_url("postgresql://us%40er:p%40ss%2Fword@db.example.com/app");

        assert_eq!(redacted, "postgresql://us%40er:***@db.example.com/app");
        assert!(!redacted.contains("p%40ss"));
        assert!(!redacted.contains("word"));
    }

    #[test]
    fn test_parse_pg_url_basic() {
        let (host, port, user, password, database) =
            parse_pg_url("postgres://admin:pass@localhost:5432/testdb").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 5432);
        assert_eq!(user, "admin");
        assert_eq!(password, Some("pass".to_string()));
        assert_eq!(database, "testdb");
    }

    #[test]
    fn test_parse_pg_url_rejects_empty_userinfo_user() {
        let err = parse_pg_url("postgres://@db.example.com/app")
            .expect_err("empty URL userinfo user must fail");

        assert!(err.to_string().contains("Missing user"));
    }

    #[test]
    fn test_parse_pg_url_decodes_credentials_and_database() {
        let (host, port, user, password, database) =
            parse_pg_url("postgres://us%40er:p%40ss%2Fword@db.example.com/my%2Fdb").unwrap();
        assert_eq!(host, "db.example.com");
        assert_eq!(port, 5432);
        assert_eq!(user, "us@er");
        assert_eq!(password, Some("p@ss/word".to_string()));
        assert_eq!(database, "my/db");
    }

    #[test]
    fn test_parse_pg_url_decodes_utf8_percent_encoding() {
        let (host, _port, user, password, database) =
            parse_pg_url("postgres://caf%C3%A9:p%C3%A9ss@db.example.com/app_%E2%9C%93").unwrap();

        assert_eq!(host, "db.example.com");
        assert_eq!(user, "café");
        assert_eq!(password, Some("péss".to_string()));
        assert_eq!(database, "app_✓");
    }

    #[test]
    fn test_parse_pg_url_rejects_malformed_percent_encoding() {
        let err = parse_pg_url("postgres://user:bad%ZZ@db.example.com/app")
            .expect_err("malformed percent escape must fail");
        assert!(err.to_string().contains("two hex digits"));

        let err = parse_pg_url("postgres://user:bad%@db.example.com/app")
            .expect_err("trailing percent escape must fail");
        assert!(err.to_string().contains("two hex digits"));
    }

    #[test]
    fn test_parse_pg_url_rejects_invalid_percent_encoded_utf8() {
        let err = parse_pg_url("postgres://user:%FF@db.example.com/app")
            .expect_err("invalid decoded UTF-8 must fail");
        assert!(err.to_string().contains("not valid UTF-8"));
    }

    #[test]
    fn test_parse_pg_url_supports_ipv6() {
        let (host, port, user, password, database) =
            parse_pg_url("postgres://admin:pass@[::1]:5544/testdb").unwrap();
        assert_eq!(host, "[::1]");
        assert_eq!(port, 5544);
        assert_eq!(user, "admin");
        assert_eq!(password, Some("pass".to_string()));
        assert_eq!(database, "testdb");
    }

    #[test]
    fn test_parse_pg_url_rejects_invalid_port() {
        let err = parse_pg_url("postgres://admin:pass@localhost:notaport/testdb")
            .expect_err("invalid port must not silently fall back to 5432");
        assert!(err.to_string().contains("Invalid PostgreSQL URL"));
    }

    #[test]
    fn test_parse_url_parts_preserves_query_and_rejects_bad_port() {
        let (scheme, host, port, path) =
            parse_url_parts("postgres://user:pass@db.example.com:15432/app?sslmode=require")
                .unwrap();
        assert_eq!(scheme, "postgres");
        assert_eq!(host, "db.example.com");
        assert_eq!(port, 15432);
        assert_eq!(path, "/app?sslmode=require");

        assert!(
            parse_url_parts("postgres://user:pass@db.example.com:bad/app").is_err(),
            "bad port must fail before SSH tunnel setup"
        );
    }

    #[test]
    fn test_rewrite_url_host_preserves_credentials_path_and_query() {
        assert_eq!(
            rewrite_url_host(
                "postgres://user:p%40ss@db.example.com:15432/app?sslmode=require",
                "127.0.0.1",
                6543
            )
            .unwrap(),
            "postgres://user:p%40ss@127.0.0.1:6543/app?sslmode=require"
        );
    }
}
