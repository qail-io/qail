//! Row-level security policy engine
//!
//! Parses and evaluates security policies defined in policies.yaml.
//! Injects filters into QAIL queries based on user context.

use crate::auth::AuthContext;
use crate::error::GatewayError;
use qail_core::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};
use serde::{Deserialize, Serialize};
use std::fs;

/// Policy configuration loaded from YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    pub policies: Vec<PolicyDef>,
}

/// A security policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDef {
    pub name: String,
    pub table: String,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub operations: Vec<OperationType>,
    /// Column-level permissions: only these columns are visible (whitelist).
    /// If empty, all columns are allowed.
    #[serde(default)]
    pub allowed_columns: Vec<String>,
    /// Column-level permissions: these columns are hidden (blacklist).
    /// Applied after allowed_columns.
    #[serde(default)]
    pub denied_columns: Vec<String>,
}

/// Operations a policy can allow
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationType {
    Read,
    Create,
    Update,
    Delete,
}

impl OperationType {
    pub fn from_action(action: Action) -> Option<Self> {
        match action {
            Action::Get => Some(OperationType::Read),
            Action::Add => Some(OperationType::Create),
            Action::Set => Some(OperationType::Update),
            Action::Del => Some(OperationType::Delete),
            _ => None,
        }
    }
}

/// Policy engine that evaluates access control and injects filters
#[derive(Debug, Default)]
pub struct PolicyEngine {
    policies: Vec<PolicyDef>,
}

impl PolicyEngine {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn load_from_file(&mut self, path: &str) -> Result<(), GatewayError> {
        let content = fs::read_to_string(path)
            .map_err(|e| GatewayError::Config(format!("Failed to read policy file: {}", e)))?;
        
        let config: PolicyConfig = serde_yaml::from_str(&content)
            .map_err(|e| GatewayError::Config(format!("Failed to parse policy file: {}", e)))?;
        
        self.policies = config.policies;
        tracing::info!("Loaded {} policies from {}", self.policies.len(), path);
        
        for policy in &self.policies {
            tracing::debug!(
                "Policy '{}': table={}, filter={:?}, role={:?}",
                policy.name, policy.table, policy.filter, policy.role
            );
        }
        
        Ok(())
    }
    
    pub fn add_policy(&mut self, policy: PolicyDef) {
        self.policies.push(policy);
    }
    
    pub fn apply_policies(&self, auth: &AuthContext, cmd: &mut Qail) -> Result<(), GatewayError> {
        let op = OperationType::from_action(cmd.action);
        
        let mut filters_to_inject: Vec<(String, String)> = Vec::new();
        
        for policy in &self.policies {
            if policy.table != "*" && policy.table != cmd.table {
                continue;
            }
            
            if let Some(ref required_role) = policy.role {
                if &auth.role != required_role {
                    continue;
                }
            }
            
            if let Some(operation) = op {
                if !policy.operations.is_empty() && !policy.operations.contains(&operation) {
                    return Err(GatewayError::AccessDenied(format!(
                        "Operation {:?} not allowed on table '{}' by policy '{}'",
                        operation, cmd.table, policy.name
                    )));
                }
            }
            
            if let Some(ref filter_template) = policy.filter {
                let filter = self.expand_filter(filter_template, auth);
                filters_to_inject.push((policy.name.clone(), filter));
            }
        }
        
        for (policy_name, filter) in filters_to_inject {
            self.inject_filter(cmd, &filter)?;
            tracing::debug!("Applied policy '{}' filter: {}", policy_name, filter);
        }

        // Apply column-level permissions
        for policy in &self.policies {
            if policy.table != "*" && policy.table != cmd.table {
                continue;
            }
            if let Some(ref required_role) = policy.role {
                if &auth.role != required_role {
                    continue;
                }
            }

            // Whitelist: restrict to allowed columns only
            if !policy.allowed_columns.is_empty() {
                self.apply_column_whitelist(cmd, &policy.allowed_columns);
                tracing::debug!(
                    "Policy '{}' restricts columns to: {:?}",
                    policy.name, policy.allowed_columns
                );
            }

            // Blacklist: strip denied columns
            if !policy.denied_columns.is_empty() {
                self.apply_column_blacklist(cmd, &policy.denied_columns);
                tracing::debug!(
                    "Policy '{}' denies columns: {:?}",
                    policy.name, policy.denied_columns
                );
            }
        }
        
        Ok(())
    }
    
    /// Expand filter template with auth context values
    fn expand_filter(&self, template: &str, auth: &AuthContext) -> String {
        let mut result = template.to_string();
        result = result.replace("$user_id", &format!("'{}'", auth.user_id));
        result = result.replace("$role", &format!("'{}'", auth.role));
        
        for (key, value) in &auth.claims {
            let placeholder = format!("${}", key);
            let replacement = match value {
                serde_json::Value::String(s) => format!("'{}'", s),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => format!("'{}'", value),
            };
            result = result.replace(&placeholder, &replacement);
        }
        
        result
    }
    
    /// Inject a filter expression into the query
    fn inject_filter(&self, cmd: &mut Qail, filter_expr: &str) -> Result<(), GatewayError> {
        let parts: Vec<&str> = if filter_expr.contains(" = ") {
            filter_expr.splitn(2, " = ").collect()
        } else if filter_expr.contains(" != ") {
            filter_expr.splitn(2, " != ").collect()
        } else {
            return Err(GatewayError::Config(format!(
                "Unsupported filter expression: {}. Use 'column = value' format.",
                filter_expr
            )));
        };
        
        if parts.len() != 2 {
            return Err(GatewayError::Config(format!(
                "Invalid filter expression: {}",
                filter_expr
            )));
        }
        
        let column = parts[0].trim();
        let value_str = parts[1].trim();
        let is_not_equal = filter_expr.contains(" != ");
        
        let value = if value_str.starts_with('\'') && value_str.ends_with('\'') {
            Value::String(value_str[1..value_str.len()-1].to_string())
        } else if value_str == "true" {
            Value::Bool(true)
        } else if value_str == "false" {
            Value::Bool(false)
        } else if let Ok(n) = value_str.parse::<i64>() {
            Value::Int(n)
        } else {
            Value::String(value_str.to_string())
        };
        
        let condition = Condition {
            left: Expr::Named(column.to_string()),
            op: if is_not_equal { Operator::Ne } else { Operator::Eq },
            value,
            is_array_unnest: false,
        };
        
        // Inject as a filter cage
        cmd.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![condition],
            logical_op: LogicalOp::And,
        });
        
        Ok(())
    }
    
    /// Apply column whitelist: replace SELECT columns with only the allowed set
    fn apply_column_whitelist(&self, cmd: &mut Qail, allowed: &[String]) {
        if cmd.columns.is_empty() || cmd.columns == vec![Expr::Star] {
            // SELECT * → restrict to allowed columns
            cmd.columns = allowed.iter().map(|c| Expr::Named(c.clone())).collect();
        } else {
            // Filter existing columns to only allowed ones
            cmd.columns.retain(|expr| {
                match expr {
                    Expr::Named(name) => allowed.iter().any(|a| a == name),
                    Expr::Aliased { name, .. } => allowed.iter().any(|a| a == name),
                    _ => true, // Keep aggregates, casts, etc.
                }
            });
        }
    }
    
    /// Apply column blacklist: remove denied columns from SELECT
    fn apply_column_blacklist(&self, cmd: &mut Qail, denied: &[String]) {
        if cmd.columns.is_empty() || cmd.columns == vec![Expr::Star] {
            // Can't strip from *, leave as-is (blacklist is best-effort with SELECT *)
            // The caller should use allowed_columns for strict enforcement
            return;
        }
        cmd.columns.retain(|expr| {
            match expr {
                Expr::Named(name) => !denied.iter().any(|d| d == name),
                Expr::Aliased { name, .. } => !denied.iter().any(|d| d == name),
                _ => true,
            }
        });
    }
    
    /// Check if any policy denies access (before filter injection)
    pub fn check_access(&self, auth: &AuthContext, table: &str, action: Action) -> Result<(), GatewayError> {
        let op = OperationType::from_action(action);
        
        if self.policies.is_empty() {
            return Ok(());
        }
        
        for policy in &self.policies {
            if policy.table != "*" && policy.table != table {
                continue;
            }
            
            // Check role
            if let Some(ref required_role) = policy.role {
                if &auth.role != required_role {
                    continue;
                }
            }
            
            if let Some(operation) = op {
                if policy.operations.is_empty() || policy.operations.contains(&operation) {
                    return Ok(()); // Found a matching policy that allows
                }
            }
        }
        
        // No matching policy found - deny (secure by default)
        Err(GatewayError::AccessDenied(format!(
            "No policy allows {:?} on table '{}'",
            op, table
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_policy_expands_user_id() {
        let engine = PolicyEngine::new();
        let auth = AuthContext {
            user_id: "user123".to_string(),
            role: "user".to_string(),
            tenant_id: None,
            claims: std::collections::HashMap::new(),
        };
        
        let result = engine.expand_filter("user_id = $user_id", &auth);
        assert_eq!(result, "user_id = 'user123'");
    }
    
    #[test]
    fn test_policy_injects_filter() {
        let engine = PolicyEngine::new();
        let mut cmd = Qail::get("orders").columns(["id", "total"]);
        
        engine.inject_filter(&mut cmd, "user_id = 'user123'").unwrap();
        
        assert_eq!(cmd.cages.len(), 1);
        assert!(matches!(cmd.cages[0].kind, CageKind::Filter));
        assert_eq!(cmd.cages[0].conditions.len(), 1);
    }
    
    #[test]
    fn test_apply_policies_adds_filter() {
        let mut engine = PolicyEngine::new();
        engine.add_policy(PolicyDef {
            name: "tenant_isolation".to_string(),
            table: "orders".to_string(),
            filter: Some("user_id = $user_id".to_string()),
            role: None,
            operations: vec![OperationType::Read],
            allowed_columns: vec![],
            denied_columns: vec![],
        });
        
        let auth = AuthContext {
            user_id: "user456".to_string(),
            role: "user".to_string(),
            tenant_id: None,
            claims: std::collections::HashMap::new(),
        };
        
        let mut cmd = Qail::get("orders").columns(["id"]);
        engine.apply_policies(&auth, &mut cmd).unwrap();
        
        // Check that filter was added
        assert_eq!(cmd.cages.len(), 1);
        let condition = &cmd.cages[0].conditions[0];
        assert_eq!(condition.left, Expr::Named("user_id".to_string()));
        assert_eq!(condition.value, Value::String("user456".to_string()));
    }
    
    #[test]
    fn test_column_whitelist() {
        let mut engine = PolicyEngine::new();
        engine.add_policy(PolicyDef {
            name: "hide_sensitive".to_string(),
            table: "users".to_string(),
            filter: None,
            role: None,
            operations: vec![],
            allowed_columns: vec!["id".into(), "name".into(), "email".into()],
            denied_columns: vec![],
        });
        
        let auth = AuthContext {
            user_id: "user1".to_string(),
            role: "user".to_string(),
            tenant_id: None,
            claims: std::collections::HashMap::new(),
        };
        
        // SELECT * should be restricted
        let mut cmd = Qail::get("users");
        engine.apply_policies(&auth, &mut cmd).unwrap();
        assert_eq!(cmd.columns.len(), 3);
        assert!(cmd.columns.contains(&Expr::Named("id".to_string())));
        assert!(cmd.columns.contains(&Expr::Named("name".to_string())));
        assert!(cmd.columns.contains(&Expr::Named("email".to_string())));
    }
    
    #[test]
    fn test_column_blacklist() {
        let mut engine = PolicyEngine::new();
        engine.add_policy(PolicyDef {
            name: "hide_password".to_string(),
            table: "users".to_string(),
            filter: None,
            role: None,
            operations: vec![],
            allowed_columns: vec![],
            denied_columns: vec!["password_hash".into(), "secret_key".into()],
        });
        
        let auth = AuthContext {
            user_id: "user1".to_string(),
            role: "user".to_string(),
            tenant_id: None,
            claims: std::collections::HashMap::new(),
        };
        
        let mut cmd = Qail::get("users").columns(["id", "name", "password_hash", "secret_key"]);
        engine.apply_policies(&auth, &mut cmd).unwrap();
        assert_eq!(cmd.columns.len(), 2);
        assert!(cmd.columns.contains(&Expr::Named("id".to_string())));
        assert!(cmd.columns.contains(&Expr::Named("name".to_string())));
    }
}
