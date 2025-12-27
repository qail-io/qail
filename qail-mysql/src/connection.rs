//! MySQL connection with TLS and read-only query support.

use std::sync::Arc;
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use rustls::ClientConfig;

use crate::auth::{mysql_native_password, caching_sha2_password};
use crate::protocol::{
    InitialHandshake, ColumnDef, encode_handshake_response, encode_query,
    encode_ssl_request, read_len_enc_string, HEADER_SIZE,
};
use crate::{MySqlError, MySqlResult};

/// MySQL stream wrapper supporting both plain and TLS connections.
enum MysqlStream {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl MysqlStream {
    async fn read_exact(&mut self, buf: &mut [u8]) -> MySqlResult<()> {
        match self {
            MysqlStream::Plain(s) => {
                s.read_exact(buf).await?;
            }
            MysqlStream::Tls(s) => {
                s.read_exact(buf).await?;
            }
        }
        Ok(())
    }
    
    async fn write_all(&mut self, buf: &[u8]) -> MySqlResult<()> {
        match self {
            MysqlStream::Plain(s) => {
                s.write_all(buf).await?;
            }
            MysqlStream::Tls(s) => {
                s.write_all(buf).await?;
            }
        }
        Ok(())
    }
}

/// Read-only MySQL connection with TLS support.
pub struct MySqlConnection {
    stream: MysqlStream,
    sequence_id: u8,
    read_buf: Vec<u8>,
}

impl MySqlConnection {
    /// Connect to MySQL server with TLS and password authentication.
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
    ) -> MySqlResult<Self> {
        let addr = format!("{}:{}", host, port);
        let mut stream = TcpStream::connect(&addr).await?;
        
        // Read initial handshake
        let mut header = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header).await?;
        let packet_len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let mut sequence_id = header[3];
        
        let mut packet = vec![0u8; packet_len];
        stream.read_exact(&mut packet).await?;
        
        let handshake = InitialHandshake::parse(&packet)
            .ok_or_else(|| MySqlError::Protocol("Failed to parse handshake".into()))?;
        
        // Check if server supports SSL
        let ssl_supported = (handshake.capability_flags & 0x00000800) != 0;
        let is_caching_sha2 = handshake.auth_plugin_name.contains("caching_sha2_password");
        
        // If caching_sha2_password, we need TLS for first-time auth
        let mut mysql_stream = if ssl_supported && is_caching_sha2 {
            // Send SSL request
            sequence_id = sequence_id.wrapping_add(1);
            let ssl_request = encode_ssl_request(handshake.character_set);
            Self::send_packet_raw(&mut stream, sequence_id, &ssl_request).await?;
            
            // Upgrade to TLS
            let tls_config = ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::from_iter(
                    webpki_roots::TLS_SERVER_ROOTS.iter().cloned()
                ))
                .with_no_client_auth();
            
            // For local MySQL, skip certificate verification
            let mut config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
                .with_no_client_auth();
            
            let connector = TlsConnector::from(Arc::new(config));
            let domain = rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|_| MySqlError::Protocol("Invalid hostname".into()))?;
            
            let tls_stream = connector.connect(domain, stream).await
                .map_err(|e| MySqlError::Protocol(format!("TLS error: {}", e)))?;
            
            MysqlStream::Tls(tls_stream)
        } else {
            MysqlStream::Plain(stream)
        };
        
        // Compute auth response
        let auth_response = if password.is_empty() {
            Vec::new()
        } else if is_caching_sha2 {
            caching_sha2_password(password.as_bytes(), &handshake.auth_plugin_data).to_vec()
        } else {
            mysql_native_password(password.as_bytes(), &handshake.auth_plugin_data).to_vec()
        };
        
        // Send handshake response
        sequence_id = sequence_id.wrapping_add(1);
        let auth_plugin = if is_caching_sha2 { "caching_sha2_password" } else { "mysql_native_password" };
        let response = encode_handshake_response(
            user,
            &auth_response,
            database,
            handshake.character_set,
            auth_plugin,
        );
        
        Self::send_packet(&mut mysql_stream, sequence_id, &response).await?;
        
        // Read auth result
        let (packet, new_seq) = Self::read_packet_internal(&mut mysql_stream).await?;
        sequence_id = new_seq;
        
        // Handle auth response
        match packet.first() {
            Some(0x00) => {} // OK - auth complete
            Some(0x01) if is_caching_sha2 => {
                // AuthMoreData
                if packet.get(1) == Some(&0x03) {
                    // Fast auth succeeded, read OK packet
                    let (ok_packet, new_seq) = Self::read_packet_internal(&mut mysql_stream).await?;
                    sequence_id = new_seq;
                    if ok_packet.first() != Some(&0x00) && ok_packet.first() != Some(&0xfe) {
                        let msg = String::from_utf8_lossy(&ok_packet[3..]).to_string();
                        return Err(MySqlError::Auth(msg));
                    }
                } else if packet.get(1) == Some(&0x04) {
                    // Full auth required - send password in cleartext over TLS
                    sequence_id = sequence_id.wrapping_add(1);
                    let mut pwd = password.as_bytes().to_vec();
                    pwd.push(0); // null terminate
                    Self::send_packet(&mut mysql_stream, sequence_id, &pwd).await?;
                    
                    // Read OK
                    let (ok_packet, new_seq) = Self::read_packet_internal(&mut mysql_stream).await?;
                    sequence_id = new_seq;
                    if ok_packet.first() != Some(&0x00) {
                        let msg = String::from_utf8_lossy(&ok_packet).to_string();
                        return Err(MySqlError::Auth(format!("Full auth failed: {}", msg)));
                    }
                }
            }
            Some(0xfe) => {
                // AuthSwitchRequest - server wants to use a different auth method
                // Just send empty response - we already authenticated with caching_sha2
                sequence_id = sequence_id.wrapping_add(1);
                Self::send_packet(&mut mysql_stream, sequence_id, &[]).await?;
                
                // Read OK
                let (ok_packet, _new_seq) = Self::read_packet_internal(&mut mysql_stream).await?;
                if ok_packet.first() != Some(&0x00) {
                    let msg = String::from_utf8_lossy(&ok_packet).to_string();
                    return Err(MySqlError::Auth(format!("Auth switch failed: {}", msg)));
                }
            }
            Some(0xff) => {
                let msg = String::from_utf8_lossy(&packet[3..]).to_string();
                return Err(MySqlError::Auth(msg));
            }
            other => {
                return Err(MySqlError::Protocol(format!("Unexpected auth response: {:?}, packet len: {}", other, packet.len())));
            }
        }
        
        Ok(Self {
            stream: mysql_stream,
            sequence_id,
            read_buf: Vec::with_capacity(65536),
        })
    }
    
    /// Send a packet to the server (raw TcpStream).
    async fn send_packet_raw(stream: &mut TcpStream, seq: u8, data: &[u8]) -> MySqlResult<()> {
        let len = data.len();
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + len);
        buf.extend_from_slice(&[(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq]);
        buf.extend_from_slice(data);
        stream.write_all(&buf).await?;
        Ok(())
    }
    
    /// Send a packet to the server (MysqlStream).
    async fn send_packet(stream: &mut MysqlStream, seq: u8, data: &[u8]) -> MySqlResult<()> {
        let len = data.len();
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + len);
        buf.extend_from_slice(&[(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq]);
        buf.extend_from_slice(data);
        stream.write_all(&buf).await?;
        Ok(())
    }
    
    /// Read a packet from MysqlStream (internal helper).
    async fn read_packet_internal(stream: &mut MysqlStream) -> MySqlResult<(Vec<u8>, u8)> {
        let mut header = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header).await?;
        
        let packet_len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let sequence_id = header[3];
        
        let mut packet = vec![0u8; packet_len];
        stream.read_exact(&mut packet).await?;
        
        Ok((packet, sequence_id))
    }
    
    /// Execute a SELECT query and stream rows to callback.
    pub async fn query_stream<F>(
        &mut self,
        sql: &str,
        mut callback: F,
    ) -> MySqlResult<u64>
    where
        F: FnMut(&[Vec<u8>]),
    {
        // Send COM_QUERY
        self.sequence_id = 0;
        let query = encode_query(sql);
        Self::send_packet(&mut self.stream, self.sequence_id, &query).await?;
        
        // Read column count
        let packet = self.read_packet().await?;
        if packet.first() == Some(&0xff) {
            let msg = String::from_utf8_lossy(&packet[3..]).to_string();
            return Err(MySqlError::Protocol(msg));
        }
        
        let mut buf = &packet[..];
        let column_count = crate::protocol::read_len_enc_int(&mut buf) as usize;
        
        if column_count == 0 {
            return Ok(0);
        }
        
        // Read column definitions
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let packet = self.read_packet().await?;
            if let Some(col) = ColumnDef::parse(&packet) {
                columns.push(col);
            }
        }
        
        // Read EOF packet
        let _eof = self.read_packet().await?;
        
        // Read rows
        let mut row_count = 0u64;
        let mut row_values: Vec<Vec<u8>> = vec![Vec::new(); column_count];
        
        loop {
            let packet = self.read_packet().await?;
            
            match packet.first() {
                Some(0xfe) if packet.len() < 9 => {
                    // EOF packet - end of result set
                    break;
                }
                Some(0xff) => {
                    let msg = String::from_utf8_lossy(&packet[3..]).to_string();
                    return Err(MySqlError::Protocol(msg));
                }
                _ => {
                    let mut buf = &packet[..];
                    for i in 0..column_count {
                        row_values[i] = read_len_enc_string(&mut buf);
                    }
                    callback(&row_values);
                    row_count += 1;
                }
            }
        }
        
        Ok(row_count)
    }
    
    /// Read a single packet from the stream.
    async fn read_packet(&mut self) -> MySqlResult<Vec<u8>> {
        let mut header = [0u8; HEADER_SIZE];
        self.stream.read_exact(&mut header).await?;
        
        let packet_len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        self.sequence_id = header[3];
        
        if self.read_buf.len() < packet_len {
            self.read_buf.resize(packet_len, 0);
        }
        self.stream.read_exact(&mut self.read_buf[..packet_len]).await?;
        
        Ok(self.read_buf[..packet_len].to_vec())
    }
    
    /// Execute query and collect all rows into TSV format (for COPY).
    pub async fn query_to_tsv(&mut self, sql: &str) -> MySqlResult<Vec<u8>> {
        let mut buffer = Vec::with_capacity(1024 * 1024);
        
        self.query_stream(sql, |row| {
            for (i, col) in row.iter().enumerate() {
                if i > 0 {
                    buffer.push(b'\t');
                }
                if col.is_empty() {
                    buffer.extend_from_slice(b"\\N");
                } else {
                    buffer.extend_from_slice(col);
                }
            }
            buffer.push(b'\n');
        }).await?;
        
        Ok(buffer)
    }
}

/// Certificate verifier that accepts any certificate (for local MySQL).
#[derive(Debug)]
struct NoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}
