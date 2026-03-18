use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Execution manifest that bundles Substrait plan with data source mappings
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionManifest {
    /// Version of the manifest format
    pub version: String,
    
    /// Original SQL query (for reference/debugging)
    pub sql: String,
    
    /// Base64-encoded Substrait plan bytes
    pub substrait_plan: String,
    
    /// Map of table names to their Parquet file paths
    /// Key: table name referenced in the query (e.g., "orders", "customers")
    /// Value: absolute or relative path to Parquet file
    pub tables: HashMap<String, TableSource>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSource {
    /// Path to Parquet file (can be relative to manifest or absolute)
    pub path: String,
    
    /// Optional: file format (default: parquet)
    #[serde(default = "default_format")]
    pub format: String,
    
    /// Optional: schema information for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Vec<ColumnInfo>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

fn default_format() -> String {
    "parquet".to_string()
}

impl ExecutionManifest {
    pub fn new(sql: String, substrait_bytes: Vec<u8>) -> Self {
        Self {
            version: "1.0".to_string(),
            sql,
            substrait_plan: base64::encode(&substrait_bytes),
            tables: HashMap::new(),
        }
    }
    
    pub fn add_table(&mut self, name: String, path: PathBuf) {
        self.tables.insert(
            name,
            TableSource {
                path: path.to_string_lossy().to_string(),
                format: "parquet".to_string(),
                schema: None,
            },
        );
    }
    
    pub fn get_substrait_bytes(&self) -> Result<Vec<u8>, base64::DecodeError> {
        base64::decode(&self.substrait_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_roundtrip() {
        let mut manifest = ExecutionManifest::new(
            "SELECT * FROM orders".to_string(),
            vec![1, 2, 3, 4],
        );
        manifest.add_table("orders".to_string(), PathBuf::from("/data/orders.parquet"));
        
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let parsed: ExecutionManifest = serde_json::from_str(&json).unwrap();
        
        assert_eq!(manifest.sql, parsed.sql);
        assert_eq!(manifest.tables.len(), parsed.tables.len());
    }
}
