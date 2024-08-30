//! Extension traits for Expr.
//!

use super::json::JsonBuilder;
use crate::ast::Expr;

/// Extension trait to add fluent methods to Expr
pub trait ExprExt {
    /// Add an alias to this expression.
    ///
    /// # Example
    /// ```ignore
    /// col("name").with_alias("user_name")
    /// ```
    fn with_alias(self, alias: &str) -> Expr;

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

    /// Cast to a type: CAST(expr AS type)
    ///
    /// # Example
    /// ```ignore
    /// col("value").cast("int")  // CAST(value AS int)
    /// ```
    fn cast(self, target_type: &str) -> Expr;

    /// UPPER(expr)
    fn upper(self) -> Expr;

    /// LOWER(expr)
    fn lower(self) -> Expr;

    /// TRIM(expr)
    fn trim(self) -> Expr;

    /// LENGTH(expr)
    fn length(self) -> Expr;

    /// ABS(expr)
    fn abs(self) -> Expr;
}

impl ExprExt for Expr {
    fn with_alias(self, alias: &str) -> Expr {
        match self {
            Expr::Named(name) => Expr::Aliased {
                name,
                alias: alias.to_string(),
            },
            Expr::Aggregate {
                col,
                func,
                distinct,
                filter,
                ..
            } => Expr::Aggregate {
                col,
                func,
                distinct,
                filter,
                alias: Some(alias.to_string()),
            },
            Expr::Cast {
                expr, target_type, ..
            } => Expr::Cast {
                expr,
                target_type,
                alias: Some(alias.to_string()),
            },
            Expr::Case {
                when_clauses,
                else_value,
                ..
            } => Expr::Case {
                when_clauses,
                else_value,
                alias: Some(alias.to_string()),
            },
            Expr::FunctionCall { name, args, .. } => Expr::FunctionCall {
                name,
                args,
                alias: Some(alias.to_string()),
            },
            Expr::Binary {
                left, op, right, ..
            } => Expr::Binary {
                left,
                op,
                right,
                alias: Some(alias.to_string()),
            },
            Expr::JsonAccess {
                column,
                path_segments,
                ..
            } => Expr::JsonAccess {
                column,
                path_segments,
                alias: Some(alias.to_string()),
            },
            Expr::SpecialFunction { name, args, .. } => Expr::SpecialFunction {
                name,
                args,
                alias: Some(alias.to_string()),
            },
            other => other, // Star, Aliased, Literal, etc. - return as-is
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
            path_segments: vec![(key.to_string(), true)], // true = text extraction (->>)
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
            .map(|(i, segment)| (segment.to_string(), i == len - 1)) // Last segment as text
            .collect();

        JsonBuilder {
            column,
            path_segments,
            alias: None,
        }
    }

    fn cast(self, target_type: &str) -> Expr {
        Expr::Cast {
            expr: Box::new(self),
            target_type: target_type.to_string(),
            alias: None,
        }
    }

    fn upper(self) -> Expr {
        Expr::FunctionCall {
            name: "UPPER".to_string(),
            args: vec![self],
            alias: None,
        }
    }

    fn lower(self) -> Expr {
        Expr::FunctionCall {
            name: "LOWER".to_string(),
            args: vec![self],
            alias: None,
        }
    }

    fn trim(self) -> Expr {
        Expr::FunctionCall {
            name: "TRIM".to_string(),
            args: vec![self],
            alias: None,
        }
    }

    fn length(self) -> Expr {
        Expr::FunctionCall {
            name: "LENGTH".to_string(),
            args: vec![self],
            alias: None,
        }
    }

    fn abs(self) -> Expr {
        Expr::FunctionCall {
            name: "ABS".to_string(),
            args: vec![self],
            alias: None,
        }
    }
}

// Implement ExprExt for &str to enable: "col_name".or_default("X")
impl ExprExt for &str {
    fn with_alias(self, alias: &str) -> Expr {
        Expr::Aliased {
            name: self.to_string(),
            alias: alias.to_string(),
        }
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

    fn cast(self, target_type: &str) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::Named(self.to_string())),
            target_type: target_type.to_string(),
            alias: None,
        }
    }

    fn upper(self) -> Expr {
        Expr::FunctionCall {
            name: "UPPER".to_string(),
            args: vec![Expr::Named(self.to_string())],
            alias: None,
        }
    }

    fn lower(self) -> Expr {
        Expr::FunctionCall {
            name: "LOWER".to_string(),
            args: vec![Expr::Named(self.to_string())],
            alias: None,
        }
    }

    fn trim(self) -> Expr {
        Expr::FunctionCall {
            name: "TRIM".to_string(),
            args: vec![Expr::Named(self.to_string())],
            alias: None,
        }
    }

    fn length(self) -> Expr {
        Expr::FunctionCall {
            name: "LENGTH".to_string(),
            args: vec![Expr::Named(self.to_string())],
            alias: None,
        }
    }

    fn abs(self) -> Expr {
        Expr::FunctionCall {
            name: "ABS".to_string(),
            args: vec![Expr::Named(self.to_string())],
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
            assert_eq!(path_segments[0], ("vessel".to_string(), false)); // JSON
            assert_eq!(path_segments[1], ("0".to_string(), false)); // JSON
            assert_eq!(path_segments[2], ("port".to_string(), true)); // Text
        } else {
            panic!("Expected JsonAccess");
        }
    }

    #[test]
    fn test_str_or_default() {
        let expr = "name".or_default("N/A");
        assert!(matches!(expr, Expr::FunctionCall { name, .. } if name == "COALESCE"));
    }

    #[test]
    fn test_cast_fluent() {
        let expr = col("value").cast("int4");
        assert!(matches!(expr, Expr::Cast { target_type, .. } if target_type == "int4"));
    }

    #[test]
    fn test_upper_fluent() {
        let expr = col("name").upper();
        assert!(matches!(expr, Expr::FunctionCall { name, .. } if name == "UPPER"));
    }

    #[test]
    fn test_lower_fluent() {
        let expr = "email".lower();
        assert!(matches!(expr, Expr::FunctionCall { name, .. } if name == "LOWER"));
    }

    #[test]
    fn test_trim_fluent() {
        let expr = col("text").trim();
        assert!(matches!(expr, Expr::FunctionCall { name, .. } if name == "TRIM"));
    }
}
