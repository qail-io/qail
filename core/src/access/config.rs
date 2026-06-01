use std::collections::BTreeMap;
use std::path::Path;

use super::AccessPolicy;
use super::error::AccessPolicyLoadError;
use super::ident::normalize_table_ref;
use super::model::{AccessDecision, TableAccessPolicy};

impl AccessPolicy {
    /// Deny-by-default policy set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow-by-default policy set for trusted/internal use.
    pub fn allow_by_default() -> Self {
        Self {
            default_decision: AccessDecision::Allow,
            tables: BTreeMap::new(),
        }
    }

    /// Add or replace a table policy.
    pub fn with_table(mut self, table: impl Into<String>, policy: TableAccessPolicy) -> Self {
        self.tables
            .insert(normalize_table_ref(&table.into()), policy);
        self
    }

    /// Parse an access policy from TOML.
    pub fn from_toml_str(input: &str) -> Result<Self, AccessPolicyLoadError> {
        toml::from_str::<Self>(input)
            .map(Self::normalize_table_keys)
            .map_err(AccessPolicyLoadError::Toml)
    }

    /// Parse an access policy from JSON.
    pub fn from_json_str(input: &str) -> Result<Self, AccessPolicyLoadError> {
        serde_json::from_str::<Self>(input)
            .map(Self::normalize_table_keys)
            .map_err(AccessPolicyLoadError::Json)
    }

    /// Load an access policy from a `.toml` or `.json` file.
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, AccessPolicyLoadError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(AccessPolicyLoadError::Read)?;
        match path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("toml") => Self::from_toml_str(&raw),
            Some("json") => Self::from_json_str(&raw),
            other => Err(AccessPolicyLoadError::UnsupportedExtension(
                other.unwrap_or_default().to_string(),
            )),
        }
    }

    /// Mutably access a table policy, creating an empty policy if needed.
    pub fn table_mut(&mut self, table: impl Into<String>) -> &mut TableAccessPolicy {
        self.tables
            .entry(normalize_table_ref(&table.into()))
            .or_default()
    }

    fn normalize_table_keys(mut self) -> Self {
        self.tables = self
            .tables
            .into_iter()
            .map(|(table, policy)| (normalize_table_ref(&table), policy))
            .collect();
        self
    }
}

impl Default for AccessPolicy {
    fn default() -> Self {
        Self {
            default_decision: AccessDecision::Deny,
            tables: BTreeMap::new(),
        }
    }
}
