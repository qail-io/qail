use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

const DEFAULT_MAX_JSON_DEPTH: usize = 32;
const DEFAULT_MAX_JSON_ARRAY_ITEMS: usize = 4096;
const DEFAULT_MAX_JSON_OBJECT_KEYS: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct JsonInputLimits {
    pub(crate) max_depth: usize,
    pub(crate) max_array_items: usize,
    pub(crate) max_object_keys: usize,
}

impl Default for JsonInputLimits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_JSON_DEPTH,
            max_array_items: DEFAULT_MAX_JSON_ARRAY_ITEMS,
            max_object_keys: DEFAULT_MAX_JSON_OBJECT_KEYS,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum JsonInputError {
    #[error("JSON nesting depth {actual} exceeds maximum {max}")]
    MaxDepth { actual: usize, max: usize },
    #[error("JSON array contains {actual} items, maximum is {max}")]
    MaxArrayItems { actual: usize, max: usize },
    #[error("JSON object contains {actual} keys, maximum is {max}")]
    MaxObjectKeys { actual: usize, max: usize },
    #[error("Invalid JSON body: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("Invalid JSON shape: {0}")]
    Shape(serde_json::Error),
}

pub(crate) fn decode_value(bytes: &[u8], limits: JsonInputLimits) -> Result<Value, JsonInputError> {
    validate_lexical_depth(bytes, limits.max_depth)?;
    let value = serde_json::from_slice::<Value>(bytes)?;
    validate_value_shape(&value, limits, 0)?;
    Ok(value)
}

pub(crate) fn decode_typed<T: DeserializeOwned>(
    bytes: &[u8],
    limits: JsonInputLimits,
) -> Result<T, JsonInputError> {
    let value = decode_value(bytes, limits)?;
    serde_json::from_value(value).map_err(JsonInputError::Shape)
}

fn validate_lexical_depth(bytes: &[u8], max_depth: usize) -> Result<(), JsonInputError> {
    let mut in_string = false;
    let mut escaped = false;
    let mut depth = 0usize;

    for byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match *byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth.checked_add(1).ok_or(JsonInputError::MaxDepth {
                    actual: usize::MAX,
                    max: max_depth,
                })?;
                if depth > max_depth {
                    return Err(JsonInputError::MaxDepth {
                        actual: depth,
                        max: max_depth,
                    });
                }
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }

    Ok(())
}

fn validate_value_shape(
    value: &Value,
    limits: JsonInputLimits,
    depth: usize,
) -> Result<(), JsonInputError> {
    match value {
        Value::Array(items) => {
            let container_depth = depth + 1;
            if container_depth > limits.max_depth {
                return Err(JsonInputError::MaxDepth {
                    actual: container_depth,
                    max: limits.max_depth,
                });
            }
            if items.len() > limits.max_array_items {
                return Err(JsonInputError::MaxArrayItems {
                    actual: items.len(),
                    max: limits.max_array_items,
                });
            }
            for item in items {
                validate_value_shape(item, limits, container_depth)?;
            }
        }
        Value::Object(map) => {
            let container_depth = depth + 1;
            if container_depth > limits.max_depth {
                return Err(JsonInputError::MaxDepth {
                    actual: container_depth,
                    max: limits.max_depth,
                });
            }
            if map.len() > limits.max_object_keys {
                return Err(JsonInputError::MaxObjectKeys {
                    actual: map.len(),
                    max: limits.max_object_keys,
                });
            }
            for value in map.values() {
                validate_value_shape(value, limits, container_depth)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{JsonInputError, JsonInputLimits, decode_typed, decode_value};
    use serde::Deserialize;

    fn tiny_limits() -> JsonInputLimits {
        JsonInputLimits {
            max_depth: 2,
            max_array_items: 2,
            max_object_keys: 2,
        }
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Payload {
        name: String,
    }

    #[test]
    fn decode_typed_accepts_valid_payload() {
        let payload: Payload =
            decode_typed(br#"{"name":"qail"}"#, JsonInputLimits::default()).unwrap();

        assert_eq!(
            payload,
            Payload {
                name: "qail".to_string()
            }
        );
    }

    #[test]
    fn rejects_excessive_lexical_depth_before_full_decode() {
        let err = decode_value(br#"{"a":{"b":{"c":1}}}"#, tiny_limits()).unwrap_err();

        assert!(matches!(
            err,
            JsonInputError::MaxDepth { actual: 3, max: 2 }
        ));
    }

    #[test]
    fn rejects_oversized_array() {
        let err = decode_value(br#"[1,2,3]"#, tiny_limits()).unwrap_err();

        assert!(matches!(
            err,
            JsonInputError::MaxArrayItems { actual: 3, max: 2 }
        ));
    }

    #[test]
    fn rejects_oversized_object() {
        let err = decode_value(br#"{"a":1,"b":2,"c":3}"#, tiny_limits()).unwrap_err();

        assert!(matches!(
            err,
            JsonInputError::MaxObjectKeys { actual: 3, max: 2 }
        ));
    }

    #[test]
    fn ignores_braces_inside_json_strings_for_depth_precheck() {
        let value = decode_value(br#"{"text":"{{{{{{{{{{"}"#, tiny_limits()).unwrap();

        assert_eq!(value["text"], "{{{{{{{{{{");
    }
}
