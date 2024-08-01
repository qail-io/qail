//! Extension traits for Expr.
//!
//! Adds fluent methods to Expr for more ergonomic query building.

use crate::ast::Expr;
use super::json::JsonBuilder;

/// Extension trait to add fluent methods to Expr
pub trait ExprExt {
    /// Add an alias to this expression.
    /// 
    /// # Example
    /// ```ignore
    /// col("name").as_alias("user_name")
    /// ```
    fn as_alias(self, alias: &str) -> Expr;
    
    /// COALESCE with a default value.
    /// 
    /// # Example
    /// ```ignore
    /// col("name").or_default("Unknown")  // COALESCE(name, 'Unknown')
    /// ```
    fn or_default(self, default: impl Into<Expr>) -> Expr;
    
    /// JSON text extraction (column->>'key').
    /// 
    /// # Example
    /// ```ignore
    /// col("contact_info").json("phone")  // contact_info->>'phone'
    /// ```
    fn json(self, key: &str) -> JsonBuilder;
    
    /// JSON path extraction with dot notation.
    /// 
    /// # Example
    /// ```ignore
    /// col("metadata").path("vessel.0.port")  // metadata->'vessel'->0->>'port'
    /// ```
    fn path(self, dotted_path: &str) -> JsonBuilder;
}

impl ExprExt for Expr {
    fn as_alias(self, alias: &str) -> Expr {
        match self {
            Expr::Named(name) => Expr::Aliased { name, alias: alias.to_string() },
            Expr::Aggregate { col, func, distinct, filter, .. } => {
                Expr::Aggregate { col, func, distinct, filter, alias: Some(alias.to_string()) }
            }
            Expr::Cast { expr, target_type, .. } => {
                Expr::Cast { expr, target_type, alias: Some(alias.to_string()) }
            }
            Expr::Case { when_clauses, else_value, .. } => {
                Expr::Case { when_clauses, else_value, alias: Some(alias.to_string()) }
            }
            Expr::FunctionCall { name, args, .. } => {
                Expr::FunctionCall { name, args, alias: Some(alias.to_string()) }
            }
            Expr::Binary { left, op, right, .. } => {
                Expr::Binary { left, op, right, alias: Some(alias.to_string()) }
            }
            Expr::JsonAccess { column, path_segments, .. } => {
                Expr::JsonAccess { column, path_segments, alias: Some(alias.to_string()) }
            }
            Expr::SpecialFunction { name, args, .. } => {
                Expr::SpecialFunction { name, args, alias: Some(alias.to_string()) }
            }
            other => other,  // Star, Aliased, Literal, etc. - return as-is
        }
    }
    
    fn or_default(self, default: impl Into<Expr>) -> Expr {
        Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![self, default.into()],
            alias: None,
        }
    }
    
    fn json(self, key: &str) -> JsonBuilder {
        let column = match self {
            Expr::Named(name) => name,
            _ => panic!("json() can only be called on column references"),
        };
        JsonBuilder {
            column,
            path_segments: vec![(key.to_string(), true)],  // true = text extraction (->>)
            alias: None,
        }
    }
    
    fn path(self, dotted_path: &str) -> JsonBuilder {
        let column = match self {
            Expr::Named(name) => name,
            _ => panic!("path() can only be called on column references"),
        };
        
        let segments: Vec<&str> = dotted_path.split('.').collect();
        let len = segments.len();
        let path_segments: Vec<(String, bool)> = segments
            .into_iter()
            .enumerate()
            .map(|(i, segment)| (segment.to_string(), i == len - 1))  // Last segment as text
            .collect();
        
        JsonBuilder {
            column,
            path_segments,
            alias: None,
        }
    }
}

// Implement ExprExt for &str to enable: "col_name".or_default("X")
impl ExprExt for &str {
    fn as_alias(self, alias: &str) -> Expr {
        Expr::Aliased { name: self.to_string(), alias: alias.to_string() }
    }
    
    fn or_default(self, default: impl Into<Expr>) -> Expr {
        Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![Expr::Named(self.to_string()), default.into()],
            alias: None,
        }
    }
    
    fn json(self, key: &str) -> JsonBuilder {
        JsonBuilder {
            column: self.to_string(),
            path_segments: vec![(key.to_string(), true)],
            alias: None,
        }
    }
    
    fn path(self, dotted_path: &str) -> JsonBuilder {
        let segments: Vec<&str> = dotted_path.split('.').collect();
        let len = segments.len();
        let path_segments: Vec<(String, bool)> = segments
            .into_iter()
            .enumerate()
            .map(|(i, segment)| (segment.to_string(), i == len - 1))
            .collect();
        
        JsonBuilder {
            column: self.to_string(),
            path_segments,
            alias: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::builders::col;
    
    #[test]
    fn test_or_default() {
        let expr = col("name").or_default("Unknown");
        assert!(matches!(expr, Expr::FunctionCall { name, .. } if name == "COALESCE"));
    }
    
    #[test]
    fn test_json_fluent() {
        let expr: Expr = col("info").json("phone").into();
        assert!(matches!(expr, Expr::JsonAccess { column, .. } if column == "info"));
    }
    
    #[test]
    fn test_path_fluent() {
        let expr: Expr = col("metadata").path("vessel.0.port").into();
        if let Expr::JsonAccess { path_segments, .. } = expr {
            assert_eq!(path_segments.len(), 3);
            assert_eq!(path_segments[0], ("vessel".to_string(), false));  // JSON
            assert_eq!(path_segments[1], ("0".to_string(), false));       // JSON
            assert_eq!(path_segments[2], ("port".to_string(), true));     // Text
        } else {
            panic!("Expected JsonAccess");
        }
    }
    
    #[test]
    fn test_str_or_default() {
        let expr = "name".or_default("N/A");
        assert!(matches!(expr, Expr::FunctionCall { name, .. } if name == "COALESCE"));
    }
}
