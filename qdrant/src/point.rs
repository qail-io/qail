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
    /// Create a new point with a vector.
    pub fn new(id: impl Into<PointId>, vector: Vec<f32>) -> Self {
        Self {
            id: id.into(),
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
