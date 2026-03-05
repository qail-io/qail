use super::super::is_safe_ident_segment;
use super::types::{is_json_value_compatible_with_pg_type, variadic_element_type};
use crate::middleware::ApiError;
use crate::server::RpcCallableSignature;
use serde_json::Value;

pub(in super::super::super) fn matches_positional_signature(
    signature: &RpcCallableSignature,
    values: &[Value],
) -> bool {
    let provided = values.len();
    let min_required = if signature.variadic && signature.total_args > 0 {
        signature
            .required_args()
            .min(signature.total_args.saturating_sub(1))
    } else {
        signature.required_args()
    };
    let max_allowed = if signature.variadic {
        usize::MAX
    } else {
        signature.total_args
    };

    if provided < min_required || provided > max_allowed {
        return false;
    }

    if signature.total_args == 0 {
        return provided == 0;
    }

    for (idx, value) in values.iter().enumerate() {
        let expected_type = if signature.variadic && idx >= signature.total_args.saturating_sub(1) {
            signature
                .arg_types
                .last()
                .map(|t| variadic_element_type(t))
                .unwrap_or("anyelement")
        } else {
            signature
                .arg_types
                .get(idx)
                .map(String::as_str)
                .unwrap_or("anyelement")
        };

        if !is_json_value_compatible_with_pg_type(value, expected_type) {
            return false;
        }
    }

    true
}

fn matches_named_signature(
    signature: &RpcCallableSignature,
    named_args: &serde_json::Map<String, Value>,
) -> bool {
    if named_args.len() > signature.total_args {
        return false;
    }

    let mut normalized_args: std::collections::HashMap<String, &Value> =
        std::collections::HashMap::with_capacity(named_args.len());
    for (raw_key, value) in named_args {
        if !is_safe_ident_segment(raw_key) {
            return false;
        }
        let normalized_key = raw_key.to_ascii_lowercase();
        if normalized_args.insert(normalized_key, value).is_some() {
            return false;
        }
    }

    let mut name_to_index = std::collections::HashMap::with_capacity(signature.arg_names.len());
    for (idx, maybe_name) in signature.arg_names.iter().enumerate() {
        if let Some(name) = maybe_name {
            name_to_index.insert(name.as_str(), idx);
        }
    }

    for idx in 0..signature.required_args().min(signature.total_args) {
        let Some(required_name) = signature.arg_names.get(idx).and_then(|v| v.as_ref()) else {
            return false;
        };
        if !normalized_args.contains_key(required_name) {
            return false;
        }
    }

    for (normalized_key, value) in &normalized_args {
        let Some(idx) = name_to_index.get(normalized_key.as_str()) else {
            return false;
        };
        let expected_type = signature
            .arg_types
            .get(*idx)
            .map(String::as_str)
            .unwrap_or("anyelement");
        if !is_json_value_compatible_with_pg_type(value, expected_type) {
            return false;
        }
    }

    true
}

pub(in super::super::super) fn signature_matches_call(
    signature: &RpcCallableSignature,
    args: Option<&Value>,
) -> bool {
    match args {
        None => matches_positional_signature(signature, &[]),
        Some(Value::Object(map)) => matches_named_signature(signature, map),
        Some(Value::Array(values)) => matches_positional_signature(signature, values),
        Some(single) => matches_positional_signature(signature, std::slice::from_ref(single)),
    }
}

pub(super) fn format_signature_brief(signature: &RpcCallableSignature) -> String {
    let identity = if signature.identity_args.is_empty() {
        "".to_string()
    } else {
        signature.identity_args.clone()
    };
    if signature.result_type.is_empty() {
        format!("({})", identity)
    } else {
        format!("({}) -> {}", identity, signature.result_type)
    }
}

pub(in super::super::super) fn select_matching_rpc_signature<'a>(
    function_name: &str,
    signatures: &'a [RpcCallableSignature],
    args: Option<&Value>,
) -> Result<&'a RpcCallableSignature, ApiError> {
    let matches: Vec<&RpcCallableSignature> = signatures
        .iter()
        .filter(|sig| signature_matches_call(sig, args))
        .collect();

    if matches.is_empty() {
        let available = signatures
            .iter()
            .map(format_signature_brief)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::parse_error(format!(
            "RPC arguments do not match any overload for '{}'. Available overloads: {}",
            function_name, available
        )));
    }

    if matches.len() > 1 {
        let matched = matches
            .iter()
            .map(|sig| format_signature_brief(sig))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::parse_error(format!(
            "RPC call is ambiguous for '{}'. Matching overloads: {}",
            function_name, matched
        )));
    }

    Ok(matches[0])
}
