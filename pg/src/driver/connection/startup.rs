//! Startup handshake — authentication, parameter negotiation, prepared stmt mgmt.

use super::helpers::{generate_gss_token, md5_password_message, select_scram_mechanism};
use super::types::{GSS_SESSION_COUNTER, PgConnection, StartupAuthFlow};
use crate::driver::stream::PgStream;
use crate::driver::{
    AuthSettings, EnterpriseAuthMechanism, GssTokenProvider, GssTokenProviderEx, PgError, PgResult,
};
use crate::protocol::{BackendMessage, FrontendMessage, ScramClient, TransactionStatus};
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;

impl PgConnection {
    /// Handle startup sequence (auth + params).
    pub(super) async fn handle_startup(
        &mut self,
        user: &str,
        password: Option<&str>,
        auth_settings: AuthSettings,
        gss_token_provider: Option<GssTokenProvider>,
        gss_token_provider_ex: Option<GssTokenProviderEx>,
    ) -> PgResult<()> {
        let mut scram_client: Option<ScramClient> = None;
        let mut startup_auth_flow: Option<StartupAuthFlow> = None;
        let mut saw_auth_ok = false;
        let gss_session_id = GSS_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut gss_roundtrips: u32 = 0;
        const MAX_GSS_ROUNDTRIPS: u32 = 32;

        loop {
            let msg = self.recv().await?;
            if saw_auth_ok
                && matches!(
                    &msg,
                    BackendMessage::AuthenticationOk
                        | BackendMessage::AuthenticationKerberosV5
                        | BackendMessage::AuthenticationGSS
                        | BackendMessage::AuthenticationSCMCredential
                        | BackendMessage::AuthenticationGSSContinue(_)
                        | BackendMessage::AuthenticationSSPI
                        | BackendMessage::AuthenticationCleartextPassword
                        | BackendMessage::AuthenticationMD5Password(_)
                        | BackendMessage::AuthenticationSASL(_)
                        | BackendMessage::AuthenticationSASLContinue(_)
                        | BackendMessage::AuthenticationSASLFinal(_)
                )
            {
                return Err(PgError::Protocol(
                    "Received authentication challenge after AuthenticationOk".to_string(),
                ));
            }
            match msg {
                BackendMessage::AuthenticationOk => {
                    if let Some(StartupAuthFlow::Scram {
                        server_final_seen: false,
                    }) = startup_auth_flow
                    {
                        return Err(PgError::Protocol(
                            "Received AuthenticationOk before AuthenticationSASLFinal".to_string(),
                        ));
                    }
                    saw_auth_ok = true;
                }
                BackendMessage::AuthenticationKerberosV5 => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationKerberosV5 while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::EnterpriseGss {
                        mechanism: EnterpriseAuthMechanism::KerberosV5,
                    });

                    if !auth_settings.allow_kerberos_v5 {
                        return Err(PgError::Auth(
                            "Server requested Kerberos V5 authentication, but Kerberos V5 is disabled by AuthSettings".to_string(),
                        ));
                    }

                    if gss_token_provider.is_none() && gss_token_provider_ex.is_none() {
                        return Err(PgError::Auth(
                            "Kerberos V5 authentication requested but no GSS token provider is configured. Set ConnectOptions.gss_token_provider or ConnectOptions.gss_token_provider_ex.".to_string(),
                        ));
                    }

                    let token = generate_gss_token(
                        gss_session_id,
                        EnterpriseAuthMechanism::KerberosV5,
                        None,
                        gss_token_provider,
                        gss_token_provider_ex.as_ref(),
                    )
                    .map_err(|e| {
                        PgError::Auth(format!("Kerberos V5 token generation failed: {}", e))
                    })?;

                    self.send(FrontendMessage::GSSResponse(token)).await?;
                }
                BackendMessage::AuthenticationGSS => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationGSS while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::EnterpriseGss {
                        mechanism: EnterpriseAuthMechanism::GssApi,
                    });

                    if !auth_settings.allow_gssapi {
                        return Err(PgError::Auth(
                            "Server requested GSSAPI authentication, but GSSAPI is disabled by AuthSettings".to_string(),
                        ));
                    }

                    if gss_token_provider.is_none() && gss_token_provider_ex.is_none() {
                        return Err(PgError::Auth(
                            "GSSAPI authentication requested but no GSS token provider is configured. Set ConnectOptions.gss_token_provider or ConnectOptions.gss_token_provider_ex.".to_string(),
                        ));
                    }

                    let token = generate_gss_token(
                        gss_session_id,
                        EnterpriseAuthMechanism::GssApi,
                        None,
                        gss_token_provider,
                        gss_token_provider_ex.as_ref(),
                    )
                    .map_err(|e| {
                        PgError::Auth(format!("GSSAPI initial token generation failed: {}", e))
                    })?;

                    self.send(FrontendMessage::GSSResponse(token)).await?;
                }
                BackendMessage::AuthenticationSCMCredential => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationSCMCredential while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    return Err(PgError::Auth(
                        "Server requested SCM credential authentication (auth code 6). This driver currently does not support Unix-socket credential passing; use SCRAM, GSS/SSPI, or password auth for this connection."
                            .to_string(),
                    ));
                }
                BackendMessage::AuthenticationSSPI => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationSSPI while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::EnterpriseGss {
                        mechanism: EnterpriseAuthMechanism::Sspi,
                    });

                    if !auth_settings.allow_sspi {
                        return Err(PgError::Auth(
                            "Server requested SSPI authentication, but SSPI is disabled by AuthSettings".to_string(),
                        ));
                    }

                    if gss_token_provider.is_none() && gss_token_provider_ex.is_none() {
                        return Err(PgError::Auth(
                            "SSPI authentication requested but no GSS token provider is configured. Set ConnectOptions.gss_token_provider or ConnectOptions.gss_token_provider_ex.".to_string(),
                        ));
                    }

                    let token = generate_gss_token(
                        gss_session_id,
                        EnterpriseAuthMechanism::Sspi,
                        None,
                        gss_token_provider,
                        gss_token_provider_ex.as_ref(),
                    )
                    .map_err(|e| {
                        PgError::Auth(format!("SSPI initial token generation failed: {}", e))
                    })?;

                    self.send(FrontendMessage::GSSResponse(token)).await?;
                }
                BackendMessage::AuthenticationGSSContinue(server_token) => {
                    gss_roundtrips += 1;
                    if gss_roundtrips > MAX_GSS_ROUNDTRIPS {
                        return Err(PgError::Auth(format!(
                            "GSS handshake exceeded {} roundtrips — aborting",
                            MAX_GSS_ROUNDTRIPS
                        )));
                    }

                    let mechanism = match startup_auth_flow {
                        Some(StartupAuthFlow::EnterpriseGss { mechanism }) => mechanism,
                        Some(flow) => {
                            return Err(PgError::Protocol(format!(
                                "Received AuthenticationGSSContinue while {} authentication is in progress",
                                flow.label()
                            )));
                        }
                        None => {
                            return Err(PgError::Auth(
                                "Received GSSContinue without AuthenticationGSS/SSPI/KerberosV5 init"
                                    .to_string(),
                            ));
                        }
                    };

                    if gss_token_provider.is_none() && gss_token_provider_ex.is_none() {
                        return Err(PgError::Auth(
                            "Received GSSContinue but no GSS token provider is configured. Set ConnectOptions.gss_token_provider or ConnectOptions.gss_token_provider_ex.".to_string(),
                        ));
                    }

                    let token = generate_gss_token(
                        gss_session_id,
                        mechanism,
                        Some(&server_token),
                        gss_token_provider,
                        gss_token_provider_ex.as_ref(),
                    )
                    .map_err(|e| {
                        PgError::Auth(format!("GSS continue token generation failed: {}", e))
                    })?;

                    // Only send the response if there is actually a token to
                    // send.  When gss_init_sec_context returns GSS_S_COMPLETE
                    // on the final round, the token may be empty.  Sending an
                    // empty GSSResponse ('p') after the server already
                    // considers auth complete trips the "invalid frontend
                    // message type 112" FATAL in PostgreSQL.
                    if !token.is_empty() {
                        self.send(FrontendMessage::GSSResponse(token)).await?;
                    }
                }
                BackendMessage::AuthenticationCleartextPassword => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationCleartextPassword while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::CleartextPassword);

                    if !auth_settings.allow_cleartext_password {
                        return Err(PgError::Auth(
                            "Server requested cleartext authentication, but cleartext is disabled by AuthSettings"
                                .to_string(),
                        ));
                    }
                    let password = password.ok_or_else(|| {
                        PgError::Auth("Password required for cleartext authentication".to_string())
                    })?;
                    self.send(FrontendMessage::PasswordMessage(password.to_string()))
                        .await?;
                }
                BackendMessage::AuthenticationMD5Password(salt) => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationMD5Password while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::Md5Password);

                    if !auth_settings.allow_md5_password {
                        return Err(PgError::Auth(
                            "Server requested MD5 authentication, but MD5 is disabled by AuthSettings"
                                .to_string(),
                        ));
                    }
                    let password = password.ok_or_else(|| {
                        PgError::Auth("Password required for MD5 authentication".to_string())
                    })?;
                    let md5_password = md5_password_message(user, password, salt);
                    self.send(FrontendMessage::PasswordMessage(md5_password))
                        .await?;
                }
                BackendMessage::AuthenticationSASL(mechanisms) => {
                    if let Some(flow) = startup_auth_flow {
                        return Err(PgError::Protocol(format!(
                            "Received AuthenticationSASL while {} authentication is in progress",
                            flow.label()
                        )));
                    }
                    startup_auth_flow = Some(StartupAuthFlow::Scram {
                        server_final_seen: false,
                    });

                    if !auth_settings.allow_scram_sha_256 {
                        return Err(PgError::Auth(
                            "Server requested SCRAM authentication, but SCRAM is disabled by AuthSettings"
                                .to_string(),
                        ));
                    }
                    let password = password.ok_or_else(|| {
                        PgError::Auth("Password required for SCRAM authentication".to_string())
                    })?;

                    let tls_binding = self.tls_server_end_point_channel_binding();
                    let (mechanism, channel_binding_data) = select_scram_mechanism(
                        &mechanisms,
                        tls_binding,
                        auth_settings.channel_binding,
                    )
                    .map_err(PgError::Auth)?;

                    let client = if let Some(binding_data) = channel_binding_data {
                        ScramClient::new_with_tls_server_end_point(user, password, binding_data)
                    } else {
                        ScramClient::new(user, password)
                    };
                    let first_message = client.client_first_message();

                    self.send(FrontendMessage::SASLInitialResponse {
                        mechanism,
                        data: first_message,
                    })
                    .await?;

                    scram_client = Some(client);
                }
                BackendMessage::AuthenticationSASLContinue(server_data) => {
                    match startup_auth_flow {
                        Some(StartupAuthFlow::Scram {
                            server_final_seen: false,
                        }) => {}
                        Some(StartupAuthFlow::Scram {
                            server_final_seen: true,
                        }) => {
                            return Err(PgError::Protocol(
                                "Received AuthenticationSASLContinue after AuthenticationSASLFinal"
                                    .to_string(),
                            ));
                        }
                        Some(flow) => {
                            return Err(PgError::Protocol(format!(
                                "Received AuthenticationSASLContinue while {} authentication is in progress",
                                flow.label()
                            )));
                        }
                        None => {
                            return Err(PgError::Auth(
                                "Received SASL Continue without SASL init".to_string(),
                            ));
                        }
                    }

                    let client = scram_client.as_mut().ok_or_else(|| {
                        PgError::Auth("Received SASL Continue without SASL init".to_string())
                    })?;

                    let final_message = client
                        .process_server_first(&server_data)
                        .map_err(|e| PgError::Auth(format!("SCRAM error: {}", e)))?;

                    self.send(FrontendMessage::SASLResponse(final_message))
                        .await?;
                }
                BackendMessage::AuthenticationSASLFinal(server_signature) => {
                    match startup_auth_flow {
                        Some(StartupAuthFlow::Scram {
                            server_final_seen: false,
                        }) => {
                            startup_auth_flow = Some(StartupAuthFlow::Scram {
                                server_final_seen: true,
                            });
                        }
                        Some(StartupAuthFlow::Scram {
                            server_final_seen: true,
                        }) => {
                            return Err(PgError::Protocol(
                                "Received duplicate AuthenticationSASLFinal".to_string(),
                            ));
                        }
                        Some(flow) => {
                            return Err(PgError::Protocol(format!(
                                "Received AuthenticationSASLFinal while {} authentication is in progress",
                                flow.label()
                            )));
                        }
                        None => {
                            return Err(PgError::Auth(
                                "Received SASL Final without SASL init".to_string(),
                            ));
                        }
                    }

                    let client = scram_client.as_ref().ok_or_else(|| {
                        PgError::Auth("Received SASL Final without SASL init".to_string())
                    })?;
                    client
                        .verify_server_final(&server_signature)
                        .map_err(|e| PgError::Auth(format!("Server verification failed: {}", e)))?;
                }
                BackendMessage::ParameterStatus { .. } => {
                    if !saw_auth_ok {
                        return Err(PgError::Protocol(
                            "Received ParameterStatus before AuthenticationOk".to_string(),
                        ));
                    }
                }
                BackendMessage::NegotiateProtocolVersion {
                    newest_minor_supported,
                    unrecognized_protocol_options,
                } => {
                    if saw_auth_ok {
                        return Err(PgError::Protocol(
                            "Received NegotiateProtocolVersion after AuthenticationOk".to_string(),
                        ));
                    }
                    let negotiated = u16::try_from(newest_minor_supported).map_err(|_| {
                        PgError::Protocol(format!(
                            "Invalid NegotiateProtocolVersion newest_minor_supported: {}",
                            newest_minor_supported
                        ))
                    })?;
                    if negotiated > self.requested_protocol_minor {
                        return Err(PgError::Protocol(format!(
                            "Server negotiated protocol minor {} above requested {}",
                            negotiated, self.requested_protocol_minor
                        )));
                    }
                    self.negotiated_protocol_minor = negotiated;
                    if !unrecognized_protocol_options.is_empty() {
                        tracing::debug!(
                            negotiated_minor = negotiated,
                            unrecognized_count = unrecognized_protocol_options.len(),
                            "startup_negotiate_protocol_version"
                        );
                    }
                }
                BackendMessage::BackendKeyData {
                    process_id,
                    secret_key,
                } => {
                    if !saw_auth_ok {
                        return Err(PgError::Protocol(
                            "Received BackendKeyData before AuthenticationOk".to_string(),
                        ));
                    }
                    self.process_id = process_id;
                    self.cancel_key_bytes = secret_key;
                    self.secret_key = if self.cancel_key_bytes.len() == 4 {
                        i32::from_be_bytes([
                            self.cancel_key_bytes[0],
                            self.cancel_key_bytes[1],
                            self.cancel_key_bytes[2],
                            self.cancel_key_bytes[3],
                        ])
                    } else {
                        0
                    };
                }
                BackendMessage::ReadyForQuery(TransactionStatus::Idle)
                | BackendMessage::ReadyForQuery(TransactionStatus::InBlock)
                | BackendMessage::ReadyForQuery(TransactionStatus::Failed) => {
                    if !saw_auth_ok {
                        return Err(PgError::Protocol(
                            "Startup completed without AuthenticationOk".to_string(),
                        ));
                    }
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Connection(err.message));
                }
                BackendMessage::NoticeResponse(_) => {}
                _ => {
                    return Err(PgError::Protocol(
                        "Unexpected backend message during startup".to_string(),
                    ));
                }
            }
        }
    }

    /// Build SCRAM `tls-server-end-point` channel-binding bytes from the server leaf cert.
    ///
    /// PostgreSQL expects the hash of the peer certificate DER for
    /// `SCRAM-SHA-256-PLUS` channel binding. We currently use SHA-256 here.
    fn tls_server_end_point_channel_binding(&self) -> Option<Vec<u8>> {
        let PgStream::Tls(tls) = &self.stream else {
            return None;
        };

        let (_, conn) = tls.get_ref();
        let certs = conn.peer_certificates()?;
        let leaf_cert = certs.first()?;

        let mut hasher = Sha256::new();
        hasher.update(leaf_cert.as_ref());
        Some(hasher.finalize().to_vec())
    }

    /// Gracefully close the connection by sending a Terminate message.
    /// This tells the server we're done and allows proper cleanup.
    pub async fn close(mut self) -> PgResult<()> {
        use crate::protocol::PgEncoder;

        // Send Terminate packet ('X')
        let terminate = PgEncoder::encode_terminate();
        self.write_all_with_timeout(&terminate, "stream write")
            .await?;
        self.flush_with_timeout("stream flush").await?;

        Ok(())
    }

    /// Maximum prepared statements per connection before LRU eviction kicks in.
    ///
    /// This prevents memory spikes from dynamic batch filters generating
    /// thousands of unique SQL shapes within a single request. Using LRU
    /// eviction instead of nuclear `.clear()` preserves hot statements.
    pub(crate) const MAX_PREPARED_PER_CONN: usize = 128;

    /// Evict the least-recently-used prepared statement if at capacity.
    ///
    /// Called before every new statement registration to enforce
    /// `MAX_PREPARED_PER_CONN`. Both `stmt_cache` (LRU ordering) and
    /// `prepared_statements` (name→SQL map) are kept in sync.
    pub(crate) fn evict_prepared_if_full(&mut self) {
        if self.prepared_statements.len() >= Self::MAX_PREPARED_PER_CONN {
            // Pop the LRU entry from the cache
            if let Some((evicted_hash, evicted_name)) = self.stmt_cache.pop_lru() {
                self.prepared_statements.remove(&evicted_name);
                self.column_info_cache.remove(&evicted_hash);
                self.pending_statement_closes.push(evicted_name);
            } else {
                // stmt_cache is empty but prepared_statements is full —
                // shouldn't happen in normal flow, but handle defensively
                // by clearing the oldest entry from the HashMap.
                if let Some(key) = self.prepared_statements.keys().next().cloned() {
                    self.prepared_statements.remove(&key);
                    self.pending_statement_closes.push(key);
                }
            }
        }
    }

    /// Clear all local prepared-statement state for this connection.
    ///
    /// Used by one-shot self-heal paths when server-side statement state
    /// becomes invalid after DDL or failover.
    pub(crate) fn clear_prepared_statement_state(&mut self) {
        self.stmt_cache.clear();
        self.prepared_statements.clear();
        self.column_info_cache.clear();
        self.pending_statement_closes.clear();
    }
}
