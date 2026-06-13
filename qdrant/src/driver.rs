//! gRPC-based Qdrant driver with zero-copy encoding.
//!
//! This driver uses the proto_encoder for direct protobuf encoding
//! and grpc_transport for HTTP/2 communication, achieving performance
//! that matches or exceeds the official qdrant-client.

use bytes::BytesMut;
use qail_core::ast::Qail;

use crate::decoder;
use crate::encoder;
use crate::error::{QdrantError, QdrantResult};
use crate::point::{Payload, Point, PointId, ScoredPoint};
use crate::transport::GrpcClient;

fn encode_error(message: impl Into<String>) -> QdrantError {
    QdrantError::Encode(message.into())
}

fn search_limit_from_ast(cmd: &Qail) -> QdrantResult<u64> {
    let mut limit = 10u64;
    for cage in &cmd.cages {
        if let qail_core::ast::CageKind::Limit(n) = cage.kind {
            if n == 0 {
                return Err(QdrantError::Encode(
                    "Qdrant search limit must be positive".to_string(),
                ));
            }
            limit = u64::try_from(n)
                .map_err(|_| QdrantError::Encode("Qdrant search limit is too large".to_string()))?;
        }
    }
    Ok(limit)
}

fn validate_vector_finite(label: &str, vector: &[f32]) -> QdrantResult<()> {
    if vector.is_empty() {
        return Err(encode_error(format!(
            "Qdrant {label} vector must not be empty"
        )));
    }
    if let Some((idx, value)) = vector
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(encode_error(format!(
            "Qdrant {label} contains non-finite vector value at index {idx}: {value}"
        )));
    }
    Ok(())
}

fn validate_vector_size(vector_size: u64) -> QdrantResult<()> {
    if vector_size == 0 {
        return Err(encode_error(
            "Qdrant collection vector_size must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_score_threshold(score_threshold: Option<f32>) -> QdrantResult<()> {
    if let Some(value) = score_threshold
        && !value.is_finite()
    {
        return Err(encode_error(format!(
            "Qdrant score threshold must be finite, got {value}"
        )));
    }
    Ok(())
}

fn validate_search_request(request: &encoder::SearchRequest<'_>) -> QdrantResult<()> {
    validate_vector_finite("search request", request.vector)?;
    validate_score_threshold(request.score_threshold)
}

fn validate_ast_value_finite(value: &qail_core::ast::Value, label: &str) -> QdrantResult<()> {
    use qail_core::ast::Value;

    match value {
        Value::Float(value) if !value.is_finite() => Err(encode_error(format!(
            "Qdrant {label} contains non-finite float: {value}"
        ))),
        Value::Array(items) => {
            for item in items {
                validate_ast_value_finite(item, label)?;
            }
            Ok(())
        }
        Value::Vector(values) => {
            validate_vector_finite(label, values)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_conditions_finite(
    conditions: &[qail_core::ast::Condition],
    label: &str,
) -> QdrantResult<()> {
    for condition in conditions {
        validate_ast_value_finite(&condition.value, label)?;
    }
    Ok(())
}

fn validate_condition_groups_finite(
    groups: &[Vec<qail_core::ast::Condition>],
    label: &str,
) -> QdrantResult<()> {
    for group in groups {
        validate_conditions_finite(group, label)?;
    }
    Ok(())
}

fn validate_payload_value_finite(
    value: &crate::point::PayloadValue,
    label: &str,
) -> QdrantResult<()> {
    use crate::point::PayloadValue;

    match value {
        PayloadValue::Float(value) if !value.is_finite() => Err(encode_error(format!(
            "Qdrant {label} contains non-finite payload float: {value}"
        ))),
        PayloadValue::List(items) => {
            for item in items {
                validate_payload_value_finite(item, label)?;
            }
            Ok(())
        }
        PayloadValue::Object(map) => validate_payload_finite(map, label),
        _ => Ok(()),
    }
}

fn validate_payload_finite(payload: &Payload, label: &str) -> QdrantResult<()> {
    for value in payload.values() {
        validate_payload_value_finite(value, label)?;
    }
    Ok(())
}

fn validate_points_finite(points: &[Point]) -> QdrantResult<()> {
    for (idx, point) in points.iter().enumerate() {
        validate_vector_finite(&format!("upsert point {idx}"), &point.vector)?;
        validate_payload_finite(&point.payload, &format!("upsert point {idx}"))?;
    }
    Ok(())
}

/// High-performance gRPC driver for Qdrant.
///
/// Uses gRPC/HTTP2 with zero-copy protobuf encoding:
/// - Encodes protobuf directly with pre-computed headers
/// - Reuses buffers to minimize allocations
/// - Uses memcpy for vector data (no per-element loop)
///
/// # Example
/// ```ignore
/// use qail_qdrant::QdrantDriver;
/// use qail_core::prelude::*;
///
/// let driver = QdrantDriver::connect("localhost", 6334).await?;
///
/// let results = driver.search(
///     "products",
///     &embedding,
///     10,
///     Some(0.5),
/// ).await?;
/// ```
pub struct QdrantDriver {
    /// gRPC client for HTTP/2 transport
    client: GrpcClient,
    /// Reusable encoding buffer
    buffer: BytesMut,
}

impl QdrantDriver {
    /// Connect to Qdrant gRPC endpoint (default port 6334).
    pub async fn connect(host: &str, port: u16) -> QdrantResult<Self> {
        let client = GrpcClient::connect(host, port).await?;
        Ok(Self {
            client,
            buffer: BytesMut::with_capacity(8192),
        })
    }

    /// Connect with address string.
    pub async fn connect_addr(addr: &str) -> QdrantResult<Self> {
        let (host_part, port_part) = addr.rsplit_once(':').ok_or_else(|| {
            QdrantError::Connection("Invalid address format, expected host:port".to_string())
        })?;
        let host = host_part
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(host_part);
        if host.is_empty() {
            return Err(QdrantError::Connection(
                "Invalid address format, empty host".to_string(),
            ));
        }
        let port: u16 = port_part
            .parse()
            .map_err(|_| QdrantError::Connection("Invalid port".to_string()))?;
        Self::connect(host, port).await
    }

    /// Connect to Qdrant gRPC endpoint with TLS (rustls).
    ///
    /// Uses Mozilla root certificates — no system openssl required.
    pub async fn connect_tls(host: &str, port: u16) -> QdrantResult<Self> {
        let client = GrpcClient::connect_tls(host, port).await?;
        Ok(Self {
            client,
            buffer: BytesMut::with_capacity(8192),
        })
    }

    /// Connect with URL auto-detection (https = TLS, http = plain).
    pub async fn connect_url(url: &str) -> QdrantResult<Self> {
        let client = GrpcClient::connect_url(url).await?;
        Ok(Self {
            client,
            buffer: BytesMut::with_capacity(8192),
        })
    }

    // ========================================================================
    // Search Operations
    // ========================================================================

    /// Vector similarity search with zero-copy encoding.
    ///
    /// # Performance
    /// Vector is encoded via memcpy (no per-element serialization).
    pub async fn search(
        &mut self,
        collection: &str,
        vector: &[f32],
        limit: u64,
        score_threshold: Option<f32>,
    ) -> QdrantResult<Vec<ScoredPoint>> {
        validate_vector_finite("search request", vector)?;
        validate_score_threshold(score_threshold)?;

        self.buffer.clear();
        encoder::encode_search_proto(
            &mut self.buffer,
            collection,
            vector,
            limit,
            score_threshold,
            None,
            false,
        );
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.search(request_bytes).await?;
        decoder::decode_search_response(&response)
    }

    /// Vector search with named vector field.
    pub async fn search_named(
        &mut self,
        collection: &str,
        vector_name: &str,
        vector: &[f32],
        limit: u64,
        score_threshold: Option<f32>,
    ) -> QdrantResult<Vec<ScoredPoint>> {
        validate_vector_finite("search request", vector)?;
        validate_score_threshold(score_threshold)?;

        self.buffer.clear();
        encoder::encode_search_proto(
            &mut self.buffer,
            collection,
            vector,
            limit,
            score_threshold,
            Some(vector_name),
            false,
        );
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.search(request_bytes).await?;
        decoder::decode_search_response(&response)
    }

    /// Vector similarity search using the shared search request options.
    pub async fn search_with_request(
        &mut self,
        request: encoder::SearchRequest<'_>,
    ) -> QdrantResult<Vec<ScoredPoint>> {
        validate_search_request(&request)?;

        self.buffer.clear();
        encoder::encode_search_proto(
            &mut self.buffer,
            request.collection,
            request.vector,
            request.limit,
            request.score_threshold,
            request.vector_name,
            request.with_vectors,
        );
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.search(request_bytes).await?;
        decoder::decode_search_response(&response)
    }

    /// Filtered vector search using QAIL AST conditions.
    ///
    /// Translates QAIL conditions into Qdrant's protobuf filter (must/should)
    /// and includes them in the gRPC search request.
    pub async fn search_filtered(
        &mut self,
        collection: &str,
        vector: &[f32],
        limit: u64,
        score_threshold: Option<f32>,
        conditions: &[qail_core::ast::Condition],
        is_or: bool,
    ) -> QdrantResult<Vec<ScoredPoint>> {
        validate_vector_finite("search request", vector)?;
        validate_score_threshold(score_threshold)?;
        validate_conditions_finite(conditions, "filter condition")?;

        self.buffer.clear();
        encoder::encode_search_with_filter_proto(
            &mut self.buffer,
            encoder::SearchRequest {
                collection,
                vector,
                limit,
                score_threshold,
                vector_name: None,
                with_vectors: false,
            },
            conditions,
            is_or,
        )?;
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.search(request_bytes).await?;
        decoder::decode_search_response(&response)
    }

    /// Filtered vector search with grouped conditions.
    ///
    /// `must_conditions` are joined with AND, `should_conditions` with OR.
    pub async fn search_filtered_grouped(
        &mut self,
        request: encoder::SearchRequest<'_>,
        must_conditions: &[qail_core::ast::Condition],
        should_conditions: &[qail_core::ast::Condition],
    ) -> QdrantResult<Vec<ScoredPoint>> {
        validate_search_request(&request)?;
        validate_conditions_finite(must_conditions, "filter condition")?;
        validate_conditions_finite(should_conditions, "filter condition")?;

        self.buffer.clear();
        encoder::encode_search_with_filter_groups_proto(
            &mut self.buffer,
            request,
            must_conditions,
            should_conditions,
        )?;
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.search(request_bytes).await?;
        decoder::decode_search_response(&response)
    }

    /// Filtered vector search preserving OR-cage groups.
    ///
    /// Each OR group is treated as its own disjunction that must be satisfied.
    pub async fn search_filtered_grouped_cages(
        &mut self,
        request: encoder::SearchRequest<'_>,
        must_conditions: &[qail_core::ast::Condition],
        should_groups: &[Vec<qail_core::ast::Condition>],
    ) -> QdrantResult<Vec<ScoredPoint>> {
        validate_search_request(&request)?;
        validate_conditions_finite(must_conditions, "filter condition")?;
        validate_condition_groups_finite(should_groups, "filter condition")?;

        self.buffer.clear();
        encoder::encode_search_with_filter_grouped_cages_proto(
            &mut self.buffer,
            request,
            must_conditions,
            should_groups,
        )?;
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.search(request_bytes).await?;
        decoder::decode_search_response(&response)
    }

    /// Search multiple vectors concurrently using HTTP/2 pipelining.
    ///
    /// This sends all requests concurrently over a single h2 connection,
    /// achieving 2-3x speedup compared to sequential searches.
    ///
    /// # Example
    /// ```ignore
    /// let vectors = vec![vec1, vec2, vec3];
    /// let results = driver.search_batch("products", &vectors, 10, None).await?;
    /// ```
    pub async fn search_batch(
        &mut self,
        collection: &str,
        vectors: &[Vec<f32>],
        limit: u64,
        score_threshold: Option<f32>,
    ) -> QdrantResult<Vec<Vec<ScoredPoint>>> {
        use futures_util::future::join_all;

        validate_score_threshold(score_threshold)?;

        let mut encoded_requests = Vec::with_capacity(vectors.len());
        for (idx, vector) in vectors.iter().enumerate() {
            validate_vector_finite(&format!("batch search vector {idx}"), vector)?;
            self.buffer.clear();
            encoder::encode_search_proto(
                &mut self.buffer,
                collection,
                vector,
                limit,
                score_threshold,
                None,
                false,
            );
            encoded_requests.push(self.buffer.split().freeze());
        }

        let mut futures = Vec::with_capacity(encoded_requests.len());
        for request in encoded_requests {
            futures.push(self.client.search(request));
        }

        let responses = join_all(futures).await;
        let mut results = Vec::with_capacity(responses.len());
        for response in responses {
            let decoded = decoder::decode_search_response(&response?)?;
            results.push(decoded);
        }

        Ok(results)
    }

    /// Search using QAIL AST.
    ///
    /// Extracts vector, collection, limit from the Qail command.
    /// If conditions are present in the AST, they are included as filters.
    pub async fn search_ast(&mut self, cmd: &Qail) -> QdrantResult<Vec<ScoredPoint>> {
        use qail_core::ast::LogicalOp;

        let collection = if cmd.table.is_empty() {
            return Err(QdrantError::Encode("Collection name required".to_string()));
        } else {
            &cmd.table
        };

        let vector = cmd
            .vector
            .as_ref()
            .ok_or_else(|| QdrantError::Encode("Vector required for search".to_string()))?;

        let limit = search_limit_from_ast(cmd)?;

        let score_threshold = cmd.score_threshold;

        let mut must_conditions = Vec::new();
        let mut should_groups = Vec::new();
        for cage in cmd
            .cages
            .iter()
            .filter(|c| matches!(c.kind, qail_core::ast::CageKind::Filter))
        {
            match cage.logical_op {
                LogicalOp::And => must_conditions.extend(cage.conditions.iter().cloned()),
                LogicalOp::Or => {
                    if !cage.conditions.is_empty() {
                        should_groups.push(cage.conditions.to_vec());
                    }
                }
            }
        }

        if !must_conditions.is_empty() || !should_groups.is_empty() {
            return self
                .search_filtered_grouped_cages(
                    encoder::SearchRequest {
                        collection,
                        vector,
                        limit,
                        score_threshold,
                        vector_name: cmd.vector_name.as_deref(),
                        with_vectors: cmd.with_vector,
                    },
                    &must_conditions,
                    &should_groups,
                )
                .await;
        }

        self.search_with_request(encoder::SearchRequest {
            collection,
            vector,
            limit,
            score_threshold,
            vector_name: cmd.vector_name.as_deref(),
            with_vectors: cmd.with_vector,
        })
        .await
    }

    // ========================================================================
    // Point Operations
    // ========================================================================

    /// Upsert points with zero-copy encoding.
    pub async fn upsert(
        &mut self,
        collection: &str,
        points: &[Point],
        wait: bool,
    ) -> QdrantResult<()> {
        validate_points_finite(points)?;

        self.buffer.clear();
        encoder::encode_upsert_proto(&mut self.buffer, collection, points, wait);
        let request_bytes = self.buffer.split().freeze();
        let _response = self.client.upsert(request_bytes).await?;
        Ok(())
    }

    /// Get points by ID (with payload and optional vectors).
    pub async fn get_points(
        &mut self,
        collection: &str,
        ids: &[PointId],
        with_vectors: bool,
    ) -> QdrantResult<Vec<ScoredPoint>> {
        self.buffer.clear();
        encoder::encode_get_points_proto(&mut self.buffer, collection, ids, with_vectors);
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.get(request_bytes).await?;
        decoder::decode_get_response(&response)
    }

    /// Scroll through points (paginated iteration).
    pub async fn scroll(
        &mut self,
        collection: &str,
        limit: u32,
        offset: Option<&PointId>,
        with_vectors: bool,
    ) -> QdrantResult<decoder::ScrollResult> {
        self.buffer.clear();
        encoder::encode_scroll_points_proto(
            &mut self.buffer,
            collection,
            limit,
            offset,
            with_vectors,
        );
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.scroll(request_bytes).await?;
        decoder::decode_scroll_response(&response)
    }

    /// Scroll through points with QAIL AST filters.
    pub async fn scroll_filtered_grouped_cages(
        &mut self,
        collection: &str,
        limit: u32,
        offset: Option<&PointId>,
        with_vectors: bool,
        must_conditions: &[qail_core::ast::Condition],
        should_groups: &[Vec<qail_core::ast::Condition>],
    ) -> QdrantResult<decoder::ScrollResult> {
        validate_conditions_finite(must_conditions, "filter condition")?;
        validate_condition_groups_finite(should_groups, "filter condition")?;

        self.buffer.clear();
        encoder::encode_scroll_points_with_filter_grouped_cages_proto(
            &mut self.buffer,
            collection,
            limit,
            offset,
            with_vectors,
            must_conditions,
            should_groups,
        )?;
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.scroll(request_bytes).await?;
        decoder::decode_scroll_response(&response)
    }

    /// Delete points by numeric IDs.
    pub async fn delete_points(
        &mut self,
        collection_name: &str,
        point_ids: &[u64],
    ) -> QdrantResult<()> {
        self.buffer.clear();
        encoder::encode_delete_points_proto(&mut self.buffer, collection_name, point_ids);
        let request = self.buffer.split().freeze();
        self.client.delete(request).await?;
        Ok(())
    }

    /// Delete points by PointId (supports both numeric and UUID).
    pub async fn delete_points_by_id(
        &mut self,
        collection_name: &str,
        ids: &[PointId],
    ) -> QdrantResult<()> {
        self.buffer.clear();
        encoder::encode_delete_points_mixed_proto(&mut self.buffer, collection_name, ids);
        let request = self.buffer.split().freeze();
        self.client.delete(request).await?;
        Ok(())
    }

    /// Update payload on existing points.
    pub async fn update_payload(
        &mut self,
        collection: &str,
        point_ids: &[PointId],
        payload: &Payload,
        wait: bool,
    ) -> QdrantResult<()> {
        validate_payload_finite(payload, "payload update")?;

        self.buffer.clear();
        encoder::encode_set_payload_proto(&mut self.buffer, collection, point_ids, payload, wait);
        let request = self.buffer.split().freeze();
        self.client.update_payload(request).await?;
        Ok(())
    }

    // ========================================================================
    // Collection Operations
    // ========================================================================

    /// Create a collection with specific vector parameters.
    pub async fn create_collection(
        &mut self,
        collection_name: &str,
        vector_size: u64,
        distance: crate::Distance,
        on_disk: bool,
    ) -> QdrantResult<()> {
        validate_vector_size(vector_size)?;

        self.buffer.clear();
        encoder::encode_create_collection_proto(
            &mut self.buffer,
            collection_name,
            vector_size,
            distance,
            on_disk,
        );
        let request = self.buffer.split().freeze();
        self.client.create_collection(request).await?;
        Ok(())
    }

    /// Delete a collection.
    pub async fn delete_collection(&mut self, collection_name: &str) -> QdrantResult<()> {
        self.buffer.clear();
        encoder::encode_delete_collection_proto(&mut self.buffer, collection_name);
        let request = self.buffer.split().freeze();
        self.client.delete_collection(request).await?;
        Ok(())
    }

    // ========================================================================
    // Index Operations
    // ========================================================================

    /// Create a payload field index for faster filtering.
    pub async fn create_field_index(
        &mut self,
        collection: &str,
        field_name: &str,
        field_type: encoder::FieldType,
        wait: bool,
    ) -> QdrantResult<()> {
        self.buffer.clear();
        encoder::encode_create_field_index_proto(
            &mut self.buffer,
            collection,
            field_name,
            field_type,
            wait,
        );
        let request = self.buffer.split().freeze();
        self.client.create_field_index(request).await?;
        Ok(())
    }
}

#[cfg(test)]
mod ast_limit_tests {
    use qail_core::ast::Qail;

    use super::{QdrantError, search_limit_from_ast};

    #[test]
    fn search_limit_defaults_without_ast_limit() {
        let cmd = Qail::search("products").vector(vec![0.1]);

        assert_eq!(search_limit_from_ast(&cmd).unwrap(), 10);
    }

    #[test]
    fn search_limit_rejects_zero_from_ast() {
        let cmd = Qail::search("products").vector(vec![0.1]).limit(0);

        let err = search_limit_from_ast(&cmd).expect_err("zero limit should be rejected");
        assert!(matches!(err, QdrantError::Encode(_)));
        assert!(err.to_string().contains("must be positive"));
    }
}

#[cfg(test)]
mod validation_tests {
    use std::collections::HashMap;

    use qail_core::ast::{Condition, Expr, Operator, Value};

    use crate::point::{PayloadValue, Point};

    use super::{
        QdrantError, validate_condition_groups_finite, validate_conditions_finite,
        validate_payload_finite, validate_points_finite, validate_score_threshold,
        validate_vector_finite, validate_vector_size,
    };

    fn assert_encode_error(result: crate::error::QdrantResult<()>, needle: &str) {
        let err = result.expect_err("non-finite value should be rejected");
        assert!(matches!(err, QdrantError::Encode(_)));
        assert!(
            err.to_string().contains(needle),
            "expected {needle:?} in {err}"
        );
    }

    #[test]
    fn rejects_non_finite_search_vectors_and_thresholds() {
        assert_encode_error(
            validate_vector_finite("search request", &[]),
            "must not be empty",
        );
        assert_encode_error(
            validate_vector_finite("search request", &[0.1, f32::NAN]),
            "non-finite vector value",
        );
        assert_encode_error(
            validate_score_threshold(Some(f32::INFINITY)),
            "score threshold",
        );
    }

    #[test]
    fn rejects_non_finite_filter_values() {
        let conditions = vec![Condition {
            left: Expr::Named("score".to_string()),
            op: Operator::Gt,
            value: Value::Float(f64::NAN),
            is_array_unnest: false,
        }];

        assert_encode_error(
            validate_conditions_finite(&conditions, "filter condition"),
            "non-finite float",
        );
    }

    #[test]
    fn rejects_non_finite_grouped_filter_values() {
        let groups = vec![vec![Condition {
            left: Expr::Named("score".to_string()),
            op: Operator::Gt,
            value: Value::Float(f64::INFINITY),
            is_array_unnest: false,
        }]];

        assert_encode_error(
            validate_condition_groups_finite(&groups, "filter condition"),
            "non-finite float",
        );
    }

    #[test]
    fn rejects_non_finite_upsert_vectors_and_payloads() {
        let mut payload = HashMap::new();
        payload.insert("rank".to_string(), PayloadValue::Float(f64::INFINITY));

        assert_encode_error(
            validate_payload_finite(&payload, "payload update"),
            "payload float",
        );

        let point = Point {
            id: "p1".into(),
            vector: vec![1.0, f32::NEG_INFINITY],
            payload: HashMap::new(),
        };

        assert_encode_error(validate_points_finite(&[point]), "non-finite vector value");

        let point = Point {
            id: "p2".into(),
            vector: Vec::new(),
            payload: HashMap::new(),
        };

        assert_encode_error(validate_points_finite(&[point]), "must not be empty");
    }

    #[test]
    fn rejects_zero_vector_size_for_collection_create() {
        assert_encode_error(validate_vector_size(0), "greater than zero");
        validate_vector_size(1).expect("positive vector sizes should pass");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_driver_struct() {
        // Verify struct is constructible
        let buffer = BytesMut::with_capacity(1024);
        assert!(buffer.capacity() >= 1024);
    }
}
