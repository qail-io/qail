//! Qdrant driver - main client interface.
//!
//! AST-native driver for Qdrant vector database using REST API.

use crate::error::{QdrantError, QdrantResult};
use crate::point::{Point, PointId, ScoredPoint};
use crate::protocol;
use qail_core::ast::{CageKind, Qail, Value};
use reqwest::Client;

/// Qdrant driver for vector database operations.
pub struct QdrantDriver {
    base_url: String,
    client: Client,
}

impl QdrantDriver {
    /// Connect to Qdrant server.
    ///
    /// # Example
    /// ```ignore
    /// let driver = QdrantDriver::connect("localhost", 6333).await?;
    /// ```
    pub async fn connect(host: &str, port: u16) -> QdrantResult<Self> {
        let base_url = format!("http://{}:{}", host, port);
        let client = Client::new();
        
        // Test connection by listing collections
        let url = format!("{}/collections", base_url);
        client
            .get(&url)
            .send()
            .await
            .map_err(|e| QdrantError::Connection(e.to_string()))?;
        
        Ok(Self { base_url, client })
    }

    /// Connect with address string.
    pub async fn connect_addr(addr: &str) -> QdrantResult<Self> {
        let parts: Vec<&str> = addr.split(':').collect();
        let host = parts.first().unwrap_or(&"localhost");
        let port: u16 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(6333);
        Self::connect(host, port).await
    }

    /// Vector similarity search using QAIL AST.
    ///
    /// Extracts collection, vector, limit, offset from the Qail command.
    ///
    /// # Example
    /// ```ignore
    /// let results = driver.search(
    ///     &Qail::search("products")
    ///         .vector(embedding)
    ///         .limit(10)
    /// ).await?;
    /// ```
    pub async fn search(&self, cmd: &Qail) -> QdrantResult<Vec<ScoredPoint>> {
        let collection = &cmd.table;
        
        // Extract vector from Qail (new field) or fallback to cages
        let vector = cmd.vector.clone()
            .or_else(|| self.extract_vector_from_cages(cmd).ok())
            .ok_or_else(|| QdrantError::Encode("No vector found in search command".to_string()))?;
        
        // Extract limit and offset from cages
        let (limit, offset) = self.extract_limit_offset(cmd);
        
        // Extract filter conditions from cages (excluding Limit/Offset/Vector cages)
        let filter_conditions = self.extract_filter_conditions(cmd);
        
        // Build body - with or without filter
        let body = if filter_conditions.is_empty() {
            protocol::encode_search_request(
                &vector,
                limit,
                offset,
                cmd.score_threshold,
                cmd.with_vector,
            )
        } else {
            let filter = protocol::encode_conditions_to_filter(&filter_conditions, false);
            protocol::encode_search_request_with_filter(
                &vector,
                limit,
                offset,
                cmd.score_threshold,
                cmd.with_vector,
                filter,
            )
        };
        
        // Make HTTP request
        let url = format!("{}/collections/{}/points/search", self.base_url, collection);
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        let bytes = response.bytes().await
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        protocol::decode_search_response(&bytes)
    }

    /// Upsert points (insert or update).
    ///
    /// # Example
    /// ```ignore
    /// driver.upsert("products", &[
    ///     Point::new("id1", vec![0.1, 0.2, 0.3]).with_payload("name", "Product 1"),
    /// ]).await?;
    /// ```
    pub async fn upsert(&self, collection: &str, points: &[Point]) -> QdrantResult<()> {
        let body = protocol::encode_upsert_request(points);
        
        let url = format!("{}/collections/{}/points?wait=true", self.base_url, collection);
        let response = self.client
            .put(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(QdrantError::Grpc(format!("Upsert failed: {}", text)));
        }
        
        Ok(())
    }

    /// Delete points by ID.
    pub async fn delete(&self, collection: &str, ids: &[PointId]) -> QdrantResult<()> {
        let body = protocol::encode_delete_request(ids);
        
        let url = format!("{}/collections/{}/points/delete?wait=true", self.base_url, collection);
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(QdrantError::Grpc(format!("Delete failed: {}", text)));
        }
        
        Ok(())
    }

    /// Create a new collection.
    pub async fn create_collection(
        &self,
        name: &str,
        vector_size: u64,
        distance: Distance,
    ) -> QdrantResult<()> {
        let distance_str = match distance {
            Distance::Cosine => "Cosine",
            Distance::Euclidean => "Euclid",
            Distance::Dot => "Dot",
        };
        
        let body = protocol::encode_create_collection_request(vector_size, distance_str);
        
        let url = format!("{}/collections/{}", self.base_url, name);
        let response = self.client
            .put(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(QdrantError::Grpc(format!("Create collection failed: {}", text)));
        }
        
        Ok(())
    }

    /// Delete a collection.
    pub async fn delete_collection(&self, name: &str) -> QdrantResult<()> {
        let url = format!("{}/collections/{}", self.base_url, name);
        let response = self.client
            .delete(&url)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(QdrantError::Grpc(format!("Delete collection failed: {}", text)));
        }
        
        Ok(())
    }

    /// List all collections.
    pub async fn list_collections(&self) -> QdrantResult<Vec<String>> {
        let url = format!("{}/collections", self.base_url);
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        let bytes = response.bytes().await
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        let json: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        let collections = json["result"]["collections"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        
        Ok(collections)
    }

    /// Scroll through all points in a collection (paginated).
    ///
    /// Returns points with optional filter, for iterating large datasets.
    pub async fn scroll(
        &self,
        collection: &str,
        limit: u64,
        offset: Option<PointId>,
        filter: Option<serde_json::Value>,
    ) -> QdrantResult<(Vec<ScoredPoint>, Option<PointId>)> {
        let mut request = serde_json::json!({
            "limit": limit,
            "with_payload": true,
            "with_vector": false,
        });
        
        if let Some(off) = offset {
            request["offset"] = match off {
                PointId::Uuid(s) => serde_json::json!(s),
                PointId::Num(n) => serde_json::json!(n),
            };
        }
        
        if let Some(f) = filter {
            request["filter"] = f;
        }
        
        let url = format!("{}/collections/{}/points/scroll", self.base_url, collection);
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        let bytes = response.bytes().await
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        let json: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        let points = json["result"]["points"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let id = protocol::parse_point_id(&p["id"])?;
                        let payload = protocol::parse_payload(&p["payload"]);
                        Some(ScoredPoint {
                            id,
                            score: 0.0, // scroll doesn't return scores
                            payload,
                            vector: None,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        
        let next_offset = json["result"]["next_page_offset"]
            .as_str()
            .map(|s| PointId::Uuid(s.to_string()))
            .or_else(|| json["result"]["next_page_offset"].as_u64().map(PointId::Num));
        
        Ok((points, next_offset))
    }

    /// Recommend similar points based on existing point IDs.
    ///
    /// # Example
    /// ```ignore
    /// // Find products similar to items the user liked
    /// let similar = driver.recommend(
    ///     "products",
    ///     &[PointId::Num(1), PointId::Num(2)],  // positive examples
    ///     &[PointId::Num(5)],                    // negative examples
    ///     10,
    /// ).await?;
    /// ```
    pub async fn recommend(
        &self,
        collection: &str,
        positive: &[PointId],
        negative: &[PointId],
        limit: u64,
    ) -> QdrantResult<Vec<ScoredPoint>> {
        let positive_json: Vec<serde_json::Value> = positive.iter().map(|id| {
            match id {
                PointId::Uuid(s) => serde_json::json!(s),
                PointId::Num(n) => serde_json::json!(n),
            }
        }).collect();
        
        let negative_json: Vec<serde_json::Value> = negative.iter().map(|id| {
            match id {
                PointId::Uuid(s) => serde_json::json!(s),
                PointId::Num(n) => serde_json::json!(n),
            }
        }).collect();
        
        let request = serde_json::json!({
            "positive": positive_json,
            "negative": negative_json,
            "limit": limit,
            "with_payload": true,
        });
        
        let url = format!("{}/collections/{}/points/recommend", self.base_url, collection);
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        let bytes = response.bytes().await
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        protocol::decode_search_response(&bytes)
    }

    /// Get specific points by ID.
    pub async fn get_points(
        &self,
        collection: &str,
        ids: &[PointId],
    ) -> QdrantResult<Vec<ScoredPoint>> {
        let ids_json: Vec<serde_json::Value> = ids.iter().map(|id| {
            match id {
                PointId::Uuid(s) => serde_json::json!(s),
                PointId::Num(n) => serde_json::json!(n),
            }
        }).collect();
        
        let request = serde_json::json!({
            "ids": ids_json,
            "with_payload": true,
            "with_vector": false,
        });
        
        let url = format!("{}/collections/{}/points", self.base_url, collection);
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        let bytes = response.bytes().await
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        let json: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        let points = json["result"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let id = protocol::parse_point_id(&p["id"])?;
                        let payload = protocol::parse_payload(&p["payload"]);
                        Some(ScoredPoint {
                            id,
                            score: 0.0,
                            payload,
                            vector: None,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        
        Ok(points)
    }

    /// Count points in a collection (with optional filter).
    pub async fn count(
        &self,
        collection: &str,
        filter: Option<serde_json::Value>,
        exact: bool,
    ) -> QdrantResult<u64> {
        let mut request = serde_json::json!({
            "exact": exact,
        });
        
        if let Some(f) = filter {
            request["filter"] = f;
        }
        
        let url = format!("{}/collections/{}/points/count", self.base_url, collection);
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        let bytes = response.bytes().await
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        let json: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        json["result"]["count"]
            .as_u64()
            .ok_or_else(|| QdrantError::Decode("Missing count".to_string()))
    }

    /// Collection info (vector count, config, etc.).
    pub async fn collection_info(&self, name: &str) -> QdrantResult<serde_json::Value> {
        let url = format!("{}/collections/{}", self.base_url, name);
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| QdrantError::Grpc(e.to_string()))?;
        
        let bytes = response.bytes().await
            .map_err(|e| QdrantError::Decode(e.to_string()))?;
        
        serde_json::from_slice(&bytes)
            .map_err(|e| QdrantError::Decode(e.to_string()))
    }

    /// Extract vector from Qail cages (fallback for backward compatibility).
    fn extract_vector_from_cages(&self, cmd: &Qail) -> QdrantResult<Vec<f32>> {
        // Look for Value::Vector in cages conditions
        for cage in &cmd.cages {
            for cond in &cage.conditions {
                if let Value::Vector(v) = &cond.value {
                    return Ok(v.clone());
                }
            }
        }
        
        Err(QdrantError::Encode("No vector found in cages".to_string()))
    }

    /// Extract limit and offset from Qail cages.
    fn extract_limit_offset(&self, cmd: &Qail) -> (u64, Option<u64>) {
        let mut limit = 10u64;
        let mut offset = None;
        
        for cage in &cmd.cages {
            match cage.kind {
                CageKind::Limit(n) => limit = n as u64,
                CageKind::Offset(n) => offset = Some(n as u64),
                _ => {}
            }
        }
        
        (limit, offset)
    }

    /// Extract filter conditions from Qail cages.
    /// Only includes Filter cages, excludes Limit/Offset/Sort/Payload.
    fn extract_filter_conditions(&self, cmd: &Qail) -> Vec<qail_core::ast::Condition> {
        cmd.cages
            .iter()
            .filter(|cage| matches!(cage.kind, CageKind::Filter))
            .flat_map(|cage| cage.conditions.clone())
            .collect()
    }
}

/// Distance metric for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Distance {
    Cosine,
    Euclidean,
    Dot,
}
