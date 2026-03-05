use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct YamlColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct YamlTableSchema {
    pub name: String,
    pub columns: Vec<YamlColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct YamlSchemaConfig {
    pub tables: Vec<YamlTableSchema>,
}
