//! PostgreSQL Encoder (Visitor Pattern)
//!
//! Compiles Qail AST into PostgreSQL wire protocol bytes.
//! This is pure, synchronous computation - no I/O, no async.
//!
//! # Architecture
//!
//! Layer 2 of the QAIL architecture:
//! - Input: Qail (AST)
//! - Output: BytesMut (ready to send over the wire)
//!
//! The async I/O layer (Layer 3) consumes these bytes.

use super::EncodeError;
use bytes::BytesMut;

/// Takes a Qail and produces wire protocol bytes.
/// This is the "Visitor" in the visitor pattern.
pub struct PgEncoder;

impl PgEncoder {
    /// Wire format code for text columns.
    pub const FORMAT_TEXT: i16 = 0;
    /// Wire format code for binary columns.
    pub const FORMAT_BINARY: i16 = 1;

    #[inline(always)]
    fn param_format_wire_len(param_format: i16) -> usize {
        if param_format == Self::FORMAT_TEXT {
            2 // parameter format count = 0 (server default text)
        } else {
            4 // parameter format count = 1 + one format code for all parameters
        }
    }

    #[inline(always)]
    fn encode_param_formats_vec(content: &mut Vec<u8>, param_format: i16) {
        if param_format == Self::FORMAT_TEXT {
            content.extend_from_slice(&0i16.to_be_bytes());
        } else {
            content.extend_from_slice(&1i16.to_be_bytes());
            content.extend_from_slice(&param_format.to_be_bytes());
        }
    }

    #[inline(always)]
    fn encode_param_formats_bytesmut(buf: &mut BytesMut, param_format: i16) {
        if param_format == Self::FORMAT_TEXT {
            buf.extend_from_slice(&0i16.to_be_bytes());
        } else {
            buf.extend_from_slice(&1i16.to_be_bytes());
            buf.extend_from_slice(&param_format.to_be_bytes());
        }
    }

    #[inline(always)]
    fn result_format_wire_len(result_format: i16) -> usize {
        if result_format == Self::FORMAT_TEXT {
            2 // result format count = 0
        } else {
            4 // result format count = 1 + one format code
        }
    }

    #[inline(always)]
    fn encode_result_formats_vec(content: &mut Vec<u8>, result_format: i16) {
        if result_format == Self::FORMAT_TEXT {
            content.extend_from_slice(&0i16.to_be_bytes());
        } else {
            content.extend_from_slice(&1i16.to_be_bytes());
            content.extend_from_slice(&result_format.to_be_bytes());
        }
    }

    #[inline(always)]
    fn encode_result_formats_bytesmut(buf: &mut BytesMut, result_format: i16) {
        if result_format == Self::FORMAT_TEXT {
            buf.extend_from_slice(&0i16.to_be_bytes());
        } else {
            buf.extend_from_slice(&1i16.to_be_bytes());
            buf.extend_from_slice(&result_format.to_be_bytes());
        }
    }

    #[inline(always)]
    fn content_len_to_wire_len(content_len: usize) -> Result<i32, EncodeError> {
        let total = content_len
            .checked_add(4)
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
        i32::try_from(total).map_err(|_| EncodeError::MessageTooLarge(total))
    }

    #[inline(always)]
    fn usize_to_i16(n: usize) -> Result<i16, EncodeError> {
        i16::try_from(n).map_err(|_| EncodeError::TooManyParameters(n))
    }

    #[inline(always)]
    fn usize_to_i32(n: usize) -> Result<i32, EncodeError> {
        i32::try_from(n).map_err(|_| EncodeError::MessageTooLarge(n))
    }

    #[inline(always)]
    fn has_nul(s: &str) -> bool {
        s.as_bytes().contains(&0)
    }

    /// Fallible simple-query encoder.
    pub fn try_encode_query_string(sql: &str) -> Result<BytesMut, EncodeError> {
        if Self::has_nul(sql) {
            return Err(EncodeError::NullByte);
        }

        let mut buf = BytesMut::new();
        let content_len = sql.len() + 1; // +1 for null terminator
        let total_len = Self::content_len_to_wire_len(content_len)?;

        buf.extend_from_slice(b"Q");
        buf.extend_from_slice(&total_len.to_be_bytes());
        buf.extend_from_slice(sql.as_bytes());
        buf.extend_from_slice(&[0]);
        Ok(buf)
    }

    /// Encode a Terminate message to close the connection.
    pub fn encode_terminate() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[b'X', 0, 0, 0, 4]);
        buf
    }

    /// Encode a Sync message (end of pipeline in extended query protocol).
    pub fn encode_sync() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);
        buf
    }

    // ==================== Extended Query Protocol ====================

    /// Fallible Parse message encoder.
    pub fn try_encode_parse(
        name: &str,
        sql: &str,
        param_types: &[u32],
    ) -> Result<BytesMut, EncodeError> {
        if Self::has_nul(name) || Self::has_nul(sql) {
            return Err(EncodeError::NullByte);
        }
        if param_types.len() > i16::MAX as usize {
            return Err(EncodeError::TooManyParameters(param_types.len()));
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"P");

        let mut content = Vec::new();
        content.extend_from_slice(name.as_bytes());
        content.push(0);
        content.extend_from_slice(sql.as_bytes());
        content.push(0);
        let param_count = Self::usize_to_i16(param_types.len())?;
        content.extend_from_slice(&param_count.to_be_bytes());
        for &oid in param_types {
            content.extend_from_slice(&oid.to_be_bytes());
        }

        let len = Self::content_len_to_wire_len(content.len())?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&content);
        Ok(buf)
    }

    /// Encode a Bind message (bind parameters to a prepared statement).
    /// Wire format:
    /// - 'B' (1 byte) - message type
    /// - length (4 bytes)
    /// - portal name (null-terminated)
    /// - statement name (null-terminated)
    /// - format code section (2-4 bytes) - default path uses 0 (all text)
    /// - parameter count (2 bytes)
    /// - for each parameter: length (4 bytes, -1 for NULL), data
    /// - result format count + codes
    ///
    /// # Arguments
    ///
    /// * `portal` — Destination portal name (empty string for unnamed).
    /// * `statement` — Source prepared statement name (empty string for unnamed).
    /// * `params` — Parameter values; `None` entries encode as SQL NULL.
    pub fn encode_bind(
        portal: &str,
        statement: &str,
        params: &[Option<Vec<u8>>],
    ) -> Result<BytesMut, EncodeError> {
        Self::encode_bind_with_result_format(portal, statement, params, Self::FORMAT_TEXT)
    }

    /// Encode a Bind message with explicit result-column format.
    ///
    /// `result_format` is PostgreSQL wire format code: `0 = text`, `1 = binary`.
    /// For `0`, this encodes "result format count = 0" (server default text).
    /// For non-zero codes, this encodes one explicit result format code.
    pub fn encode_bind_with_result_format(
        portal: &str,
        statement: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> Result<BytesMut, EncodeError> {
        Self::encode_bind_with_formats(portal, statement, params, Self::FORMAT_TEXT, result_format)
    }

    /// Encode a Bind message with explicit parameter and result format codes.
    ///
    /// `param_format` / `result_format` are PostgreSQL wire format codes:
    /// `0 = text`, `1 = binary`.
    ///
    /// For `param_format = 0`, this encodes "parameter format count = 0"
    /// (server default text). For non-zero, this encodes one explicit format
    /// code applied to all parameters.
    pub fn encode_bind_with_formats(
        portal: &str,
        statement: &str,
        params: &[Option<Vec<u8>>],
        param_format: i16,
        result_format: i16,
    ) -> Result<BytesMut, EncodeError> {
        if Self::has_nul(portal) || Self::has_nul(statement) {
            return Err(EncodeError::NullByte);
        }
        if params.len() > i16::MAX as usize {
            return Err(EncodeError::TooManyParameters(params.len()));
        }

        let mut buf = BytesMut::new();

        // Message type 'B'
        buf.extend_from_slice(b"B");

        let mut content = Vec::new();

        // Portal name (null-terminated)
        content.extend_from_slice(portal.as_bytes());
        content.push(0);

        // Statement name (null-terminated)
        content.extend_from_slice(statement.as_bytes());
        content.push(0);

        // Parameter format codes
        Self::encode_param_formats_vec(&mut content, param_format);

        // Parameter count
        let param_count = Self::usize_to_i16(params.len())?;
        content.extend_from_slice(&param_count.to_be_bytes());

        // Parameters
        for param in params {
            match param {
                None => {
                    // NULL: length = -1
                    content.extend_from_slice(&(-1i32).to_be_bytes());
                }
                Some(data) => {
                    let data_len = Self::usize_to_i32(data.len())?;
                    content.extend_from_slice(&data_len.to_be_bytes());
                    content.extend_from_slice(data);
                }
            }
        }

        // Result format codes: default text (count=0) or explicit code.
        Self::encode_result_formats_vec(&mut content, result_format);

        // Length
        let len = Self::content_len_to_wire_len(content.len())?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&content);

        Ok(buf)
    }

    /// Fallible Execute message encoder.
    pub fn try_encode_execute(portal: &str, max_rows: i32) -> Result<BytesMut, EncodeError> {
        if Self::has_nul(portal) {
            return Err(EncodeError::NullByte);
        }
        if max_rows < 0 {
            return Err(EncodeError::InvalidMaxRows(max_rows));
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"E");

        let mut content = Vec::new();
        content.extend_from_slice(portal.as_bytes());
        content.push(0);
        content.extend_from_slice(&max_rows.to_be_bytes());

        let len = Self::content_len_to_wire_len(content.len())?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&content);
        Ok(buf)
    }

    /// Fallible Describe message encoder.
    pub fn try_encode_describe(is_portal: bool, name: &str) -> Result<BytesMut, EncodeError> {
        if Self::has_nul(name) {
            return Err(EncodeError::NullByte);
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"D");

        let mut content = Vec::new();
        content.push(if is_portal { b'P' } else { b'S' });
        content.extend_from_slice(name.as_bytes());
        content.push(0);

        let len = Self::content_len_to_wire_len(content.len())?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&content);
        Ok(buf)
    }

    /// Encode a complete extended query pipeline (OPTIMIZED).
    /// This combines Parse + Bind + Execute + Sync in a single buffer.
    /// Zero intermediate allocations - writes directly to pre-sized BytesMut.
    pub fn encode_extended_query(
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> Result<BytesMut, EncodeError> {
        Self::encode_extended_query_with_result_format(sql, params, Self::FORMAT_TEXT)
    }

    /// Encode a complete extended query pipeline with explicit result format.
    ///
    /// `result_format` is PostgreSQL wire format code: `0 = text`, `1 = binary`.
    pub fn encode_extended_query_with_result_format(
        sql: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> Result<BytesMut, EncodeError> {
        Self::encode_extended_query_with_formats(sql, params, Self::FORMAT_TEXT, result_format)
    }

    /// Encode a complete extended query pipeline with explicit parameter and result formats.
    ///
    /// `param_format` / `result_format` are PostgreSQL wire format codes:
    /// `0 = text`, `1 = binary`.
    pub fn encode_extended_query_with_formats(
        sql: &str,
        params: &[Option<Vec<u8>>],
        param_format: i16,
        result_format: i16,
    ) -> Result<BytesMut, EncodeError> {
        if Self::has_nul(sql) {
            return Err(EncodeError::NullByte);
        }
        if params.len() > i16::MAX as usize {
            return Err(EncodeError::TooManyParameters(params.len()));
        }

        // Calculate total size upfront to avoid reallocations
        // Bind: 1 + 4 + 1 + 1 + param_formats + 2 + params_data + result_formats
        // Execute: 1 + 4 + 1 + 4 = 10
        // Sync: 5
        let params_size = params.iter().try_fold(0usize, |acc, p| {
            let field_size = 4usize
                .checked_add(p.as_ref().map_or(0usize, |v| v.len()))
                .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
            acc.checked_add(field_size)
                .ok_or(EncodeError::MessageTooLarge(usize::MAX))
        })?;
        let param_formats_size = Self::param_format_wire_len(param_format);
        let result_formats_size = Self::result_format_wire_len(result_format);
        let total_size = 9usize
            .checked_add(sql.len())
            .and_then(|v| v.checked_add(9))
            .and_then(|v| v.checked_add(params_size))
            .and_then(|v| v.checked_add(param_formats_size))
            .and_then(|v| v.checked_add(result_formats_size))
            .and_then(|v| v.checked_add(10))
            .and_then(|v| v.checked_add(5))
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;

        let mut buf = BytesMut::with_capacity(total_size);

        // ===== PARSE =====
        buf.extend_from_slice(b"P");
        let parse_content_len = 1usize
            .checked_add(sql.len())
            .and_then(|v| v.checked_add(1))
            .and_then(|v| v.checked_add(2))
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
        let parse_len = Self::content_len_to_wire_len(parse_content_len)?;
        buf.extend_from_slice(&parse_len.to_be_bytes());
        buf.extend_from_slice(&[0]); // Unnamed statement
        buf.extend_from_slice(sql.as_bytes());
        buf.extend_from_slice(&[0]); // Null terminator
        buf.extend_from_slice(&0i16.to_be_bytes()); // No param types (infer)

        // ===== BIND =====
        buf.extend_from_slice(b"B");
        let bind_content_len = 1usize
            .checked_add(1)
            .and_then(|v| v.checked_add(2))
            .and_then(|v| v.checked_add(param_formats_size))
            .and_then(|v| v.checked_add(params_size))
            .and_then(|v| v.checked_add(result_formats_size))
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
        let bind_len = Self::content_len_to_wire_len(bind_content_len)?;
        buf.extend_from_slice(&bind_len.to_be_bytes());
        buf.extend_from_slice(&[0]); // Unnamed portal
        buf.extend_from_slice(&[0]); // Unnamed statement
        Self::encode_param_formats_bytesmut(&mut buf, param_format);
        let param_count = Self::usize_to_i16(params.len())?;
        buf.extend_from_slice(&param_count.to_be_bytes());
        for param in params {
            match param {
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(data) => {
                    let data_len = Self::usize_to_i32(data.len())?;
                    buf.extend_from_slice(&data_len.to_be_bytes());
                    buf.extend_from_slice(data);
                }
            }
        }
        Self::encode_result_formats_bytesmut(&mut buf, result_format);

        // ===== EXECUTE =====
        buf.extend_from_slice(b"E");
        buf.extend_from_slice(&9i32.to_be_bytes()); // len = 4 + 1 + 4
        buf.extend_from_slice(&[0]); // Unnamed portal
        buf.extend_from_slice(&0i32.to_be_bytes()); // Unlimited rows

        // ===== SYNC =====
        buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);

        Ok(buf)
    }

    /// Fallible CopyFail encoder.
    pub fn try_encode_copy_fail(reason: &str) -> Result<BytesMut, EncodeError> {
        if Self::has_nul(reason) {
            return Err(EncodeError::NullByte);
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"f");
        let content_len = reason.len() + 1; // +1 for null terminator
        let len = Self::content_len_to_wire_len(content_len)?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(reason.as_bytes());
        buf.extend_from_slice(&[0]);
        Ok(buf)
    }

    /// Fallible Close encoder.
    pub fn try_encode_close(is_portal: bool, name: &str) -> Result<BytesMut, EncodeError> {
        if Self::has_nul(name) {
            return Err(EncodeError::NullByte);
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"C");
        let content_len = 1 + name.len() + 1; // type + name + null
        let len = Self::content_len_to_wire_len(content_len)?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&[if is_portal { b'P' } else { b'S' }]);
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&[0]);
        Ok(buf)
    }
}

// ==================== ULTRA-OPTIMIZED Hot Path Encoders ====================
//
// These encoders are designed to beat C:
// - Direct integer writes (no temp arrays, no bounds checks)
// - Borrowed slice params (zero-copy)
// - Single store instructions via BufMut
//

use bytes::BufMut;

/// Zero-copy parameter for ultra-fast encoding.
/// Uses borrowed slices to avoid any allocation or copy.
pub enum Param<'a> {
    /// SQL NULL value.
    Null,
    /// Non-null parameter as a borrowed byte slice.
    Bytes(&'a [u8]),
}

impl PgEncoder {
    /// Direct i32 write - no temp array, no bounds check.
    /// LLVM emits a single store instruction.
    #[inline(always)]
    fn put_i32_be(buf: &mut BytesMut, v: i32) {
        buf.put_i32(v);
    }

    #[inline(always)]
    fn put_i16_be(buf: &mut BytesMut, v: i16) {
        buf.put_i16(v);
    }

    /// Encode Bind message - ULTRA OPTIMIZED.
    /// - Direct integer writes (no temp arrays)
    /// - Borrowed params (zero-copy)
    /// - Single allocation check
    #[inline]
    pub fn encode_bind_ultra<'a>(
        buf: &mut BytesMut,
        statement: &str,
        params: &[Param<'a>],
    ) -> Result<(), EncodeError> {
        Self::encode_bind_ultra_with_result_format(buf, statement, params, Self::FORMAT_TEXT)
    }

    /// Encode Bind message with explicit result-column format.
    #[inline]
    pub fn encode_bind_ultra_with_result_format<'a>(
        buf: &mut BytesMut,
        statement: &str,
        params: &[Param<'a>],
        result_format: i16,
    ) -> Result<(), EncodeError> {
        Self::encode_bind_ultra_with_formats(
            buf,
            statement,
            params,
            Self::FORMAT_TEXT,
            result_format,
        )
    }

    /// Encode Bind message with explicit parameter and result format codes.
    #[inline]
    pub fn encode_bind_ultra_with_formats<'a>(
        buf: &mut BytesMut,
        statement: &str,
        params: &[Param<'a>],
        param_format: i16,
        result_format: i16,
    ) -> Result<(), EncodeError> {
        if Self::has_nul(statement) {
            return Err(EncodeError::NullByte);
        }
        if params.len() > i16::MAX as usize {
            return Err(EncodeError::TooManyParameters(params.len()));
        }

        // Calculate content length upfront
        let params_size = params.iter().try_fold(0usize, |acc, p| {
            let field_size = match p {
                Param::Null => 4usize,
                Param::Bytes(b) => 4usize
                    .checked_add(b.len())
                    .ok_or(EncodeError::MessageTooLarge(usize::MAX))?,
            };
            acc.checked_add(field_size)
                .ok_or(EncodeError::MessageTooLarge(usize::MAX))
        })?;
        let param_formats_size = Self::param_format_wire_len(param_format);
        let result_formats_size = Self::result_format_wire_len(result_format);
        let content_len = 1usize
            .checked_add(statement.len())
            .and_then(|v| v.checked_add(1))
            .and_then(|v| v.checked_add(2))
            .and_then(|v| v.checked_add(param_formats_size))
            .and_then(|v| v.checked_add(params_size))
            .and_then(|v| v.checked_add(result_formats_size))
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
        let wire_len = Self::content_len_to_wire_len(content_len)?;

        // Single reserve - no more allocations
        buf.reserve(1 + 4 + content_len);

        // Message type 'B'
        buf.put_u8(b'B');

        // Length (includes itself) - DIRECT WRITE
        Self::put_i32_be(buf, wire_len);

        // Portal name (empty, null-terminated)
        buf.put_u8(0);

        // Statement name (null-terminated)
        buf.extend_from_slice(statement.as_bytes());
        buf.put_u8(0);

        // Parameter format codes
        Self::encode_param_formats_bytesmut(buf, param_format);

        // Parameter count
        let param_count = Self::usize_to_i16(params.len())?;
        Self::put_i16_be(buf, param_count);

        // Parameters - ZERO COPY from borrowed slices
        for param in params {
            match param {
                Param::Null => Self::put_i32_be(buf, -1),
                Param::Bytes(data) => {
                    let data_len = Self::usize_to_i32(data.len())?;
                    Self::put_i32_be(buf, data_len);
                    buf.extend_from_slice(data);
                }
            }
        }

        // Result format codes
        Self::encode_result_formats_bytesmut(buf, result_format);
        Ok(())
    }

    /// Encode Execute message - ULTRA OPTIMIZED.
    #[inline(always)]
    pub fn encode_execute_ultra(buf: &mut BytesMut) {
        // Execute: 'E' + len(9) + portal("") + max_rows(0)
        // = 'E' 00 00 00 09 00 00 00 00 00
        buf.extend_from_slice(&[b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0]);
    }

    /// Encode Sync message - ULTRA OPTIMIZED.
    #[inline(always)]
    pub fn encode_sync_ultra(buf: &mut BytesMut) {
        buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);
    }

    // Keep the original methods for compatibility

    /// Encode Bind message directly into existing buffer (ZERO ALLOCATION).
    /// This is the hot path optimization - no intermediate Vec allocation.
    #[inline]
    pub fn encode_bind_to(
        buf: &mut BytesMut,
        statement: &str,
        params: &[Option<Vec<u8>>],
    ) -> Result<(), EncodeError> {
        Self::encode_bind_to_with_result_format(buf, statement, params, Self::FORMAT_TEXT)
    }

    /// Encode Bind into existing buffer with explicit result-column format.
    #[inline]
    pub fn encode_bind_to_with_result_format(
        buf: &mut BytesMut,
        statement: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> Result<(), EncodeError> {
        Self::encode_bind_to_with_formats(buf, statement, params, Self::FORMAT_TEXT, result_format)
    }

    /// Encode Bind into existing buffer with explicit parameter and result formats.
    #[inline]
    pub fn encode_bind_to_with_formats(
        buf: &mut BytesMut,
        statement: &str,
        params: &[Option<Vec<u8>>],
        param_format: i16,
        result_format: i16,
    ) -> Result<(), EncodeError> {
        if Self::has_nul(statement) {
            return Err(EncodeError::NullByte);
        }
        if params.len() > i16::MAX as usize {
            return Err(EncodeError::TooManyParameters(params.len()));
        }

        // Calculate content length upfront
        // portal(1) + statement(len+1) + param_formats + param_count(2)
        // + params_data + result_formats(2 or 4)
        let params_size = params.iter().try_fold(0usize, |acc, p| {
            let field_size = 4usize
                .checked_add(p.as_ref().map_or(0usize, |v| v.len()))
                .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
            acc.checked_add(field_size)
                .ok_or(EncodeError::MessageTooLarge(usize::MAX))
        })?;
        let param_formats_size = Self::param_format_wire_len(param_format);
        let result_formats_size = Self::result_format_wire_len(result_format);
        let content_len = 1usize
            .checked_add(statement.len())
            .and_then(|v| v.checked_add(1))
            .and_then(|v| v.checked_add(2))
            .and_then(|v| v.checked_add(param_formats_size))
            .and_then(|v| v.checked_add(params_size))
            .and_then(|v| v.checked_add(result_formats_size))
            .ok_or(EncodeError::MessageTooLarge(usize::MAX))?;
        let wire_len = Self::content_len_to_wire_len(content_len)?;

        buf.reserve(1 + 4 + content_len);

        // Message type 'B'
        buf.put_u8(b'B');

        // Length (includes itself) - DIRECT WRITE
        Self::put_i32_be(buf, wire_len);

        // Portal name (empty, null-terminated)
        buf.put_u8(0);

        // Statement name (null-terminated)
        buf.extend_from_slice(statement.as_bytes());
        buf.put_u8(0);

        // Parameter format codes
        Self::encode_param_formats_bytesmut(buf, param_format);

        // Parameter count
        let param_count = Self::usize_to_i16(params.len())?;
        Self::put_i16_be(buf, param_count);

        // Parameters
        for param in params {
            match param {
                None => Self::put_i32_be(buf, -1),
                Some(data) => {
                    let data_len = Self::usize_to_i32(data.len())?;
                    Self::put_i32_be(buf, data_len);
                    buf.extend_from_slice(data);
                }
            }
        }

        // Result format codes
        Self::encode_result_formats_bytesmut(buf, result_format);
        Ok(())
    }

    /// Encode Execute message directly into existing buffer (ZERO ALLOCATION).
    #[inline]
    pub fn encode_execute_to(buf: &mut BytesMut) {
        // Content: portal(1) + max_rows(4) = 5 bytes
        buf.extend_from_slice(&[b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0]);
    }

    /// Encode Sync message directly into existing buffer (ZERO ALLOCATION).
    #[inline]
    pub fn encode_sync_to(buf: &mut BytesMut) {
        buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: test_encode_simple_query removed - use AstEncoder instead
    #[test]
    fn test_encode_query_string() {
        let sql = "SELECT 1";
        let bytes = PgEncoder::try_encode_query_string(sql).unwrap();

        // Message type
        assert_eq!(bytes[0], b'Q');

        // Length: 4 (length field) + 8 (query) + 1 (null) = 13
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(len, 13);

        // Query content
        assert_eq!(&bytes[5..13], b"SELECT 1");

        // Null terminator
        assert_eq!(bytes[13], 0);
    }

    #[test]
    fn test_encode_terminate() {
        let bytes = PgEncoder::encode_terminate();
        assert_eq!(bytes.as_ref(), &[b'X', 0, 0, 0, 4]);
    }

    #[test]
    fn test_encode_sync() {
        let bytes = PgEncoder::encode_sync();
        assert_eq!(bytes.as_ref(), &[b'S', 0, 0, 0, 4]);
    }

    #[test]
    fn test_encode_parse() {
        let bytes = PgEncoder::try_encode_parse("", "SELECT $1", &[]).unwrap();

        // Message type 'P'
        assert_eq!(bytes[0], b'P');

        // Content should include query
        let content = String::from_utf8_lossy(&bytes[5..]);
        assert!(content.contains("SELECT $1"));
    }

    #[test]
    fn test_encode_bind() {
        let params = vec![
            Some(b"42".to_vec()),
            None, // NULL
        ];
        let bytes = PgEncoder::encode_bind("", "", &params).unwrap();

        // Message type 'B'
        assert_eq!(bytes[0], b'B');

        // Should have proper length
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert!(len > 4); // At least header
    }

    #[test]
    fn test_encode_bind_binary_result_format() {
        let bytes =
            PgEncoder::encode_bind_with_result_format("", "", &[], PgEncoder::FORMAT_BINARY)
                .unwrap();

        // B + len + portal + statement + param formats + param count + result formats
        // Result format section for binary should be: count=1, format=1.
        assert_eq!(&bytes[11..15], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_bind_binary_param_and_result_format() {
        let bytes = PgEncoder::encode_bind_with_formats(
            "",
            "",
            &[],
            PgEncoder::FORMAT_BINARY,
            PgEncoder::FORMAT_BINARY,
        )
        .unwrap();

        // portal, statement, param formats(count+code), param count, result formats(count+code)
        assert_eq!(&bytes[7..11], &[0, 1, 0, 1]);
        assert_eq!(&bytes[11..13], &[0, 0]);
        assert_eq!(&bytes[13..17], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_execute() {
        let bytes = PgEncoder::try_encode_execute("", 0).unwrap();

        // Message type 'E'
        assert_eq!(bytes[0], b'E');

        // Length: 4 + 1 (null) + 4 (max_rows) = 9
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(len, 9);
    }

    #[test]
    fn test_encode_execute_negative_max_rows_returns_error() {
        let err = PgEncoder::try_encode_execute("", -1).expect_err("must reject negative max_rows");
        assert_eq!(err, EncodeError::InvalidMaxRows(-1));
    }

    #[test]
    fn test_encode_extended_query() {
        let params = vec![Some(b"hello".to_vec())];
        let bytes = PgEncoder::encode_extended_query("SELECT $1", &params).unwrap();

        // Should contain all 4 message types: P, B, E, S
        assert!(bytes.windows(1).any(|w| w == [b'P']));
        assert!(bytes.windows(1).any(|w| w == [b'B']));
        assert!(bytes.windows(1).any(|w| w == [b'E']));
        assert!(bytes.windows(1).any(|w| w == [b'S']));
    }

    #[test]
    fn test_encode_extended_query_binary_result_format() {
        let bytes = PgEncoder::encode_extended_query_with_result_format(
            "SELECT 1",
            &[],
            PgEncoder::FORMAT_BINARY,
        )
        .unwrap();

        let parse_len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        let bind_start = 1 + parse_len;
        assert_eq!(bytes[bind_start], b'B');

        let bind_len = i32::from_be_bytes([
            bytes[bind_start + 1],
            bytes[bind_start + 2],
            bytes[bind_start + 3],
            bytes[bind_start + 4],
        ]);
        assert_eq!(bind_len, 14);

        let bind_content = &bytes[bind_start + 5..bind_start + 1 + bind_len as usize];
        assert_eq!(&bind_content[6..10], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_extended_query_binary_param_and_result_format() {
        let bytes = PgEncoder::encode_extended_query_with_formats(
            "SELECT 1",
            &[],
            PgEncoder::FORMAT_BINARY,
            PgEncoder::FORMAT_BINARY,
        )
        .unwrap();

        let parse_len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        let bind_start = 1 + parse_len;
        let bind_len = i32::from_be_bytes([
            bytes[bind_start + 1],
            bytes[bind_start + 2],
            bytes[bind_start + 3],
            bytes[bind_start + 4],
        ]);
        assert_eq!(bind_len, 16);

        let bind_content = &bytes[bind_start + 5..bind_start + 1 + bind_len as usize];
        assert_eq!(&bind_content[2..6], &[0, 1, 0, 1]);
        assert_eq!(&bind_content[6..8], &[0, 0]);
        assert_eq!(&bind_content[8..12], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_copy_fail() {
        let bytes = PgEncoder::try_encode_copy_fail("bad data").unwrap();
        assert_eq!(bytes[0], b'f');
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(len as usize, 4 + "bad data".len() + 1);
        assert_eq!(&bytes[5..13], b"bad data");
        assert_eq!(bytes[13], 0);
    }

    #[test]
    fn test_encode_close_statement() {
        let bytes = PgEncoder::try_encode_close(false, "my_stmt").unwrap();
        assert_eq!(bytes[0], b'C');
        assert_eq!(bytes[5], b'S'); // Statement type
        assert_eq!(&bytes[6..13], b"my_stmt");
        assert_eq!(bytes[13], 0);
    }

    #[test]
    fn test_encode_close_portal() {
        let bytes = PgEncoder::try_encode_close(true, "").unwrap();
        assert_eq!(bytes[0], b'C');
        assert_eq!(bytes[5], b'P'); // Portal type
        assert_eq!(bytes[6], 0); // Empty name null terminator
    }

    #[test]
    fn test_encode_parse_too_many_param_types_returns_error() {
        let param_types = vec![0u32; (i16::MAX as usize) + 1];
        let err =
            PgEncoder::try_encode_parse("s", "SELECT 1", &param_types).expect_err("must reject");
        assert_eq!(err, EncodeError::TooManyParameters(param_types.len()));
    }

    #[test]
    fn test_encode_bind_to_binary_result_format() {
        let mut buf = BytesMut::new();
        PgEncoder::encode_bind_to_with_result_format(&mut buf, "", &[], PgEncoder::FORMAT_BINARY)
            .unwrap();

        assert_eq!(&buf[11..15], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_bind_to_binary_param_and_result_format() {
        let mut buf = BytesMut::new();
        PgEncoder::encode_bind_to_with_formats(
            &mut buf,
            "",
            &[],
            PgEncoder::FORMAT_BINARY,
            PgEncoder::FORMAT_BINARY,
        )
        .unwrap();

        assert_eq!(&buf[7..11], &[0, 1, 0, 1]);
        assert_eq!(&buf[11..13], &[0, 0]);
        assert_eq!(&buf[13..17], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_bind_ultra_binary_result_format() {
        let mut buf = BytesMut::new();
        PgEncoder::encode_bind_ultra_with_result_format(
            &mut buf,
            "",
            &[],
            PgEncoder::FORMAT_BINARY,
        )
        .unwrap();

        assert_eq!(&buf[11..15], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_bind_ultra_binary_param_and_result_format() {
        let mut buf = BytesMut::new();
        PgEncoder::encode_bind_ultra_with_formats(
            &mut buf,
            "",
            &[],
            PgEncoder::FORMAT_BINARY,
            PgEncoder::FORMAT_BINARY,
        )
        .unwrap();

        assert_eq!(&buf[7..11], &[0, 1, 0, 1]);
        assert_eq!(&buf[11..13], &[0, 0]);
        assert_eq!(&buf[13..17], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_query_string_with_nul_returns_empty() {
        let err =
            PgEncoder::try_encode_query_string("select 1\0select 2").expect_err("must reject NUL");
        assert_eq!(err, EncodeError::NullByte);
    }

    #[test]
    fn test_encode_parse_with_nul_returns_empty() {
        let err = PgEncoder::try_encode_parse("s", "SELECT 1\0", &[]).expect_err("must reject");
        assert_eq!(err, EncodeError::NullByte);
    }

    #[test]
    fn test_encode_bind_with_nul_rejected() {
        let err = PgEncoder::encode_bind_with_result_format("\0", "", &[], PgEncoder::FORMAT_TEXT)
            .expect_err("bind with NUL portal must fail");
        assert_eq!(err, EncodeError::NullByte);
    }

    #[test]
    fn test_encode_extended_query_with_nul_rejected() {
        let err = PgEncoder::encode_extended_query_with_result_format(
            "SELECT 1\0UNION SELECT 2",
            &[],
            PgEncoder::FORMAT_TEXT,
        )
        .expect_err("extended query with NUL SQL must fail");
        assert_eq!(err, EncodeError::NullByte);
    }

    #[test]
    fn test_encode_copy_fail_with_nul_returns_empty() {
        let err = PgEncoder::try_encode_copy_fail("bad\0data").expect_err("must reject");
        assert_eq!(err, EncodeError::NullByte);
    }
}
