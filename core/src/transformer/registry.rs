//! Pattern registry for SQL to QAIL transformation

use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::traits::*;
use super::patterns::*;

/// Registry of SQL patterns
pub struct PatternRegistry {
    patterns: Vec<Box<dyn SqlPattern>>,
}

impl Default for PatternRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PatternRegistry {
    /// Create a new registry with default patterns
    pub fn new() -> Self {
        let mut registry = Self {
            patterns: Vec::new(),
        };

        // Register patterns in priority order
        registry.register(Box::new(SelectPattern));
        registry.register(Box::new(InsertPattern));
        registry.register(Box::new(UpdatePattern));
        registry.register(Box::new(DeletePattern));

        registry
    }

    /// Register a new pattern
    pub fn register(&mut self, pattern: Box<dyn SqlPattern>) {
        self.patterns.push(pattern);
        // Sort by priority (descending)
        self.patterns.sort_by_key(|p| std::cmp::Reverse(p.priority()));
    }

    /// Find matching pattern for SQL
    pub fn find_pattern(&self, stmt: &Statement, ctx: &MatchContext) -> Option<&dyn SqlPattern> {
        for pattern in &self.patterns {
            if pattern.matches(stmt, ctx) {
                return Some(pattern.as_ref());
            }
        }
        None
    }

    /// Transform SQL to QAIL
    pub fn transform_sql(&self, sql: &str, ctx: &TransformContext) -> Result<String, String> {
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| format!("Parse error: {}", e))?;

        if ast.is_empty() {
            return Err("Empty SQL".to_string());
        }

        let stmt = &ast[0];
        let match_ctx = MatchContext::default();

        let pattern = self
            .find_pattern(stmt, &match_ctx)
            .ok_or_else(|| "No matching pattern found".to_string())?;

        let data = pattern
            .extract(stmt, &match_ctx)
            .map_err(|e| e.to_string())?;

        pattern
            .transform(&data, ctx)
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_select() {
        let registry = PatternRegistry::new();
        let ctx = TransformContext {
            include_imports: true,
            ..Default::default()
        };

        let result = registry.transform_sql(
            "SELECT id, name FROM users WHERE id = $1",
            &ctx,
        );

        assert!(result.is_ok());
        let code = result.unwrap();
        assert!(code.contains("Qail::get"));
        assert!(code.contains("users"));
    }
}
