/// SECURITY (E4): Validate webhook URL to prevent SSRF attacks.
///
/// Rejects:
/// - Non-HTTP(S) schemes (e.g., `file://`, `gopher://`)
/// - Localhost and loopback addresses (127.x.x.x, ::1)
/// - Private network ranges (RFC 1918 / link-local)
/// - Cloud metadata endpoints (169.254.169.254)
/// - Zero/unspecified addresses (0.0.0.0, ::)
/// - URLs with embedded credentials (user:pass@host)
/// - Hostnames containing suspicious keywords
pub(super) fn validate_webhook_url(url: &str) -> Result<(), String> {
    let parsed = url::Url::parse(url).map_err(|e| format!("Invalid URL: {}", e))?;

    // Only allow http and https schemes
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => return Err(format!("Disallowed scheme: {}", scheme)),
    }

    // Reject URLs with embedded credentials (user:pass@host SSRF vector)
    if parsed.username() != "" || parsed.password().is_some() {
        return Err("URL credentials not allowed".to_string());
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "No host in URL".to_string())?;

    // Reject localhost (case-insensitive)
    let lower_host = host.to_ascii_lowercase();
    if lower_host == "localhost"
        || lower_host == "127.0.0.1"
        || lower_host == "::1"
        || lower_host == "[::1]"
        || lower_host == "0.0.0.0"
    {
        return Err("Loopback/unspecified address rejected".to_string());
    }

    // Reject hostnames that look like internal service discovery
    // (e.g., metadata.google.internal, instance-data.ec2.internal)
    for keyword in &["metadata", ".internal", "instance-data"] {
        if lower_host.contains(keyword) {
            return Err(format!(
                "Hostname contains suspicious keyword '{}': {}",
                keyword, host
            ));
        }
    }

    // Reject private and link-local IPs
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        reject_private_ip(ip)?;
    }

    // Also check when url::Url parsed it as a bracketed IPv6 (e.g., [::ffff:127.0.0.1])
    if let Some(url::Host::Ipv4(v4)) = parsed.host() {
        reject_private_ip(std::net::IpAddr::V4(v4))?;
    }
    if let Some(url::Host::Ipv6(v6)) = parsed.host() {
        reject_private_ip(std::net::IpAddr::V6(v6))?;

        // Check IPv6-mapped IPv4 (::ffff:127.0.0.1)
        if let Some(mapped_v4) = v6.to_ipv4_mapped() {
            reject_private_ip(std::net::IpAddr::V4(mapped_v4))?;
        }
    }

    Ok(())
}

/// Reject private, loopback, link-local, and cloud metadata IPs.
pub(super) fn reject_private_ip(ip: std::net::IpAddr) -> Result<(), String> {
    let is_bad = match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()                                  // 127.0.0.0/8
            || v4.is_private()                                // 10/8, 172.16/12, 192.168/16
            || v4.is_link_local()                             // 169.254.0.0/16
            || v4.is_unspecified()                            // 0.0.0.0
            || v4.octets()[0] == 169 && v4.octets()[1] == 254 // link-local (redundant but explicit)
            || v4.octets()[0] == 0                            // current network (0.x.x.x)
            || v4.is_broadcast() // 255.255.255.255
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()                                  // ::1
            || v6.is_unspecified()                            // ::
            || (v6.segments()[0] & 0xfe00) == 0xfc00          // unique local (fc00::/7)
            || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local (fe80::/10)
        }
    };
    if is_bad {
        return Err(format!("Private/reserved IP rejected: {}", ip));
    }
    Ok(())
}
