//! Point and payload types for Qdrant.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Point ID - either UUID string or integer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PointId {
    Uuid(String),
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
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    List(Vec<PayloadValue>),
    Object(HashMap<String, PayloadValue>),
    Null,
}

/// Payload - key-value metadata attached to points.
pub type Payload = HashMap<String, PayloadValue>;

/// A point in Qdrant - vector + payload + id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub id: PointId,
    pub vector: Vec<f32>,
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
    Sparse { indices: Vec<u32>, values: Vec<f32> },
}

/// A point with named vector support (for multi-vector collections).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MultiVectorPoint {
    pub id: PointId,
    /// Named vectors - key is vector name, value is the embedding.
    pub vectors: HashMap<String, Vec<f32>>,
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
    pub id: PointId,
    pub score: f32,
    #[serde(default)]
    pub payload: Payload,
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
