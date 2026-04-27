//! Point and payload types for Qdrant.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Point ID - either UUID string or integer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PointId {
    /// UUID string identifier.
    Uuid(String),
    /// Numeric identifier.
    Num(u64),
}

impl From<&str> for PointId {
    fn from(s: &str) -> Self {
        PointId::Uuid(s.to_string())
    }
}

impl From<String> for PointId {
    fn from(s: String) -> Self {
        PointId::Uuid(s)
    }
}

impl From<u64> for PointId {
    fn from(n: u64) -> Self {
        PointId::Num(n)
    }
}

/// Payload value types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PayloadValue {
    /// UTF-8 string value.
    String(String),
    /// Signed 64-bit integer.
    Integer(i64),
    /// 64-bit floating-point number.
    Float(f64),
    /// Boolean value.
    Bool(bool),
    /// Ordered list of nested payload values.
    List(Vec<PayloadValue>),
    /// Nested key-value object.
    Object(HashMap<String, PayloadValue>),
    /// Explicit null / absent value.
    Null,
}

/// Payload - key-value metadata attached to points.
pub type Payload = HashMap<String, PayloadValue>;

/// A point in Qdrant - vector + payload + id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// Unique point identifier (UUID string or integer).
    pub id: PointId,
    /// Dense embedding vector.
    pub vector: Vec<f32>,
    /// Key-value metadata payload.
    #[serde(default)]
    pub payload: Payload,
}

impl Point {
    /// Create a new point with a string/UUID ID.
    pub fn new(id: impl Into<PointId>, vector: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            vector,
            payload: HashMap::new(),
        }
    }

    /// Create a new point with a numeric ID.
    pub fn new_num(id: u64, vector: Vec<f32>) -> Self {
        Self {
            id: PointId::Num(id),
            vector,
            payload: HashMap::new(),
        }
    }

    /// Add payload field.
    pub fn with_payload(mut self, key: impl Into<String>, value: impl Into<PayloadValue>) -> Self {
        self.payload.insert(key.into(), value.into());
        self
    }
}

/// Sparse vector - only non-zero indices and their values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SparseVector {
    /// Indices of non-zero elements.
    pub indices: Vec<u32>,
    /// Values at those indices.
    pub values: Vec<f32>,
}

impl SparseVector {
    /// Create a sparse vector from indices and values.
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Self {
        Self { indices, values }
    }

    /// Create from a dense vector (only keeps non-zero elements).
    pub fn from_dense(dense: &[f32], threshold: f32) -> Self {
        let mut indices = Vec::new();
        let mut values = Vec::new();
        for (i, &v) in dense.iter().enumerate() {
            if v.abs() > threshold {
                indices.push(i as u32);
                values.push(v);
            }
        }
        Self { indices, values }
    }
}

/// Vector type that can be named or anonymous.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VectorData {
    /// Single unnamed vector.
    Single(Vec<f32>),
    /// Named vectors (for multi-vector collections).
    Named(HashMap<String, Vec<f32>>),
    /// Sparse vector.
    Sparse {
        /// Indices of non-zero elements.
        indices: Vec<u32>,
        /// Values at those indices.
        values: Vec<f32>,
    },
}

/// A point with named vector support (for multi-vector collections).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MultiVectorPoint {
    /// Unique point identifier.
    pub id: PointId,
    /// Named vectors — key is vector name, value is the embedding.
    pub vectors: HashMap<String, Vec<f32>>,
    /// Key-value metadata payload.
    #[serde(default)]
    pub payload: Payload,
}

impl MultiVectorPoint {
    /// Create a new multi-vector point.
    pub fn new(id: impl Into<PointId>) -> Self {
        Self {
            id: id.into(),
            vectors: HashMap::new(),
            payload: HashMap::new(),
        }
    }

    /// Add a named vector.
    pub fn with_vector(mut self, name: impl Into<String>, vector: Vec<f32>) -> Self {
        self.vectors.insert(name.into(), vector);
        self
    }

    /// Add payload field.
    pub fn with_payload(mut self, key: impl Into<String>, value: impl Into<PayloadValue>) -> Self {
        self.payload.insert(key.into(), value.into());
        self
    }
}

/// Search result - point with similarity score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoredPoint {
    /// Point identifier.
    pub id: PointId,
    /// Similarity score (higher = more similar for cosine/dot, lower for euclidean).
    pub score: f32,
    /// Key-value metadata payload.
    #[serde(default)]
    pub payload: Payload,
    /// Optional dense vector (returned when `with_vectors = true`).
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
}

// Convenient From implementations for PayloadValue
impl From<String> for PayloadValue {
    fn from(s: String) -> Self {
        PayloadValue::String(s)
    }
}

impl From<&str> for PayloadValue {
    fn from(s: &str) -> Self {
        PayloadValue::String(s.to_string())
    }
}

impl From<i64> for PayloadValue {
    fn from(n: i64) -> Self {
        PayloadValue::Integer(n)
    }
}

impl From<f64> for PayloadValue {
    fn from(n: f64) -> Self {
        PayloadValue::Float(n)
    }
}

impl From<bool> for PayloadValue {
    fn from(b: bool) -> Self {
        PayloadValue::Bool(b)
    }
}
