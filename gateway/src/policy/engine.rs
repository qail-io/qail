use std::fs;

use qail_core::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

use crate::auth::AuthContext;
use crate::error::GatewayError;

use super::{OperationType, PolicyConfig, PolicyDef, PolicyEngine};

impl PolicyEngine {
    /// Create an empty policy engine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load policies from a YAML configuration file.
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
                policy.name,
                policy.table,
                policy.filter,
                policy.role
            );
        }

        Ok(())
    }

    /// Register an additional policy definition.
    pub fn add_policy(&mut self, policy: PolicyDef) {
        self.policies.push(policy);
    }

    /// Evaluate all matching policies for a given auth context and command,
    /// injecting filters and column restrictions into the AST.
    ///
    /// # Arguments
    ///
    /// * `auth` — Authenticated user context (role, operator, agent).
    /// * `cmd` — Mutable Qail AST command to inject policy filters into.
    pub fn apply_policies(&self, auth: &AuthContext, cmd: &mut Qail) -> Result<(), GatewayError> {
        if self.policies.is_empty() {
            return Ok(());
        }

        let op = OperationType::from_action(cmd.action);
        let mut matched_policy_names: Vec<String> = Vec::new();
        let mut applicable_policies: Vec<&PolicyDef> = Vec::new();

        for policy in &self.policies {
            if policy.table != "*" && policy.table != cmd.table {
                continue;
            }

            if let Some(ref required_role) = policy.role
                && &auth.role != required_role
            {
                continue;
            }

            matched_policy_names.push(policy.name.clone());

            let op_allowed = if let Some(operation) = op {
                policy.operations.is_empty() || policy.operations.contains(&operation)
            } else {
                true
            };

            if op_allowed {
                applicable_policies.push(policy);
            }
        }

        if let Some(operation) = op
            && !matched_policy_names.is_empty()
            && applicable_policies.is_empty()
        {
            return Err(GatewayError::AccessDenied(format!(
                "Operation {:?} not allowed on table '{}' by matching policies {:?}",
                operation, cmd.table, matched_policy_names
            )));
        }

        if applicable_policies.is_empty() {
            return Err(GatewayError::AccessDenied(format!(
                "No policy allows {:?} on table '{}'",
                op, cmd.table
            )));
        }

        let mut filters_to_inject: Vec<(String, String)> = Vec::new();
        for policy in &applicable_policies {
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
        for policy in &applicable_policies {
            // Whitelist: restrict to allowed columns only
            if !policy.allowed_columns.is_empty() {
                self.apply_column_whitelist(cmd, &policy.allowed_columns);
                tracing::debug!(
                    "Policy '{}' restricts columns to: {:?}",
                    policy.name,
                    policy.allowed_columns
                );
            }

            // Blacklist: strip denied columns
            if !policy.denied_columns.is_empty() {
                self.apply_column_blacklist(cmd, &policy.denied_columns);
                tracing::debug!(
                    "Policy '{}' denies columns: {:?}",
                    policy.name,
                    policy.denied_columns
                );
            }
        }

        Ok(())
    }

    /// Expand filter template with auth context values.
    ///
    /// SECURITY: All string values are SQL-escaped (single quotes doubled)
    /// before interpolation to prevent injection via crafted JWT claims.
    pub(super) fn expand_filter(&self, template: &str, auth: &AuthContext) -> String {
        let mut result = template.to_string();
        result = result.replace(
            "$user_id",
            &format!("'{}'", auth.user_id.replace('\'', "''")),
        );
        result = result.replace("$role", &format!("'{}'", auth.role.replace('\'', "''")));

        // SECURITY (H1): Expand $tenant_id for tenant isolation policies.
        // Without this, policies like `filter: "tenant_id = $tenant_id"` stay literal.
        if let Some(ref tid) = auth.tenant_id {
            result = result.replace("$tenant_id", &format!("'{}'", tid.replace('\'', "''")));
        }

        for (key, value) in &auth.claims {
            let placeholder = format!("${}", key);
            let replacement = match value {
                serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => format!("'{}'", value.to_string().replace('\'', "''")),
            };
            result = result.replace(&placeholder, &replacement);
        }

        result
    }

    /// Inject a filter expression into the query
    pub(super) fn inject_filter(
        &self,
        cmd: &mut Qail,
        filter_expr: &str,
    ) -> Result<(), GatewayError> {
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
            Value::String(value_str[1..value_str.len() - 1].to_string())
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
            op: if is_not_equal {
                Operator::Ne
            } else {
                Operator::Eq
            },
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
            cmd.columns.retain(|expr| match expr {
                Expr::Named(name) => allowed.iter().any(|a| a == name),
                Expr::Aliased { name, .. } => allowed.iter().any(|a| a == name),
                _ => true, // Keep aggregates, casts, etc.
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
        cmd.columns.retain(|expr| match expr {
            Expr::Named(name) => !denied.iter().any(|d| d == name),
            Expr::Aliased { name, .. } => !denied.iter().any(|d| d == name),
            _ => true,
        });
    }

    /// Check if any policy denies access (before filter injection).
    ///
    /// # Arguments
    ///
    /// * `auth` — Authenticated user context.
    /// * `table` — Target table name.
    /// * `action` — The CRUD action being performed.
    pub fn check_access(
        &self,
        auth: &AuthContext,
        table: &str,
        action: Action,
    ) -> Result<(), GatewayError> {
        let op = OperationType::from_action(action);

        if self.policies.is_empty() {
            return Ok(());
        }

        for policy in &self.policies {
            if policy.table != "*" && policy.table != table {
                continue;
            }

            // Check role
            if let Some(ref required_role) = policy.role
                && &auth.role != required_role
            {
                continue;
            }

            if let Some(operation) = op
                && (policy.operations.is_empty() || policy.operations.contains(&operation))
            {
                return Ok(()); // Found a matching policy that allows
            }
        }

        // No matching policy found - deny (secure by default)
        Err(GatewayError::AccessDenied(format!(
            "No policy allows {:?} on table '{}'",
            op, table
        )))
    }
}
