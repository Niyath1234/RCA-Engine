//! SQL Engine Tool Module
//! 
//! Provides SQL execution capability using DuckDB CLI (installed via Homebrew).
//! DuckDB supports full SQL including scientific notation (1e6, 1e7), window functions, CTEs, etc.
//! 
//! This enables the "Traverse → Test → Observe → Decide" pattern:
//! - Agent chooses a node (Table, Join, Filter, Rule, Metric)
//! - Runs a small SQL probe at that node
//! - Observes the result
//! - Decides next step dynamically

use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use serde::{Deserialize, Serialize};
use tracing::{info, debug, warn};

/// SQL Engine for executing queries using DuckDB CLI
pub struct SqlEngine {
    metadata: Metadata,
    data_dir: PathBuf,
}

/// Result of a SQL probe query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProbeResult {
    /// Number of rows returned
    pub row_count: usize,
    
    /// Sample rows (first N rows)
    pub sample_rows: Vec<HashMap<String, serde_json::Value>>,
    
    /// Column names
    pub columns: Vec<String>,
    
    /// Summary statistics (if applicable)
    pub summary: Option<ProbeSummary>,
    
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    
    /// Any warnings or issues
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeSummary {
    /// Distinct count of key columns
    pub distinct_keys: Option<usize>,
    
    /// Null counts per column
    pub null_counts: HashMap<String, usize>,
    
    /// Value ranges (min/max for numeric columns)
    pub value_ranges: HashMap<String, ValueRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueRange {
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
}

/// Result of a direct SQL query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlQueryResult {
    /// Column names
    pub columns: Vec<String>,
    
    /// All rows returned
    pub rows: Vec<HashMap<String, serde_json::Value>>,
}

impl SqlEngine {
    /// Create a new SQL engine
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self {
            metadata,
            data_dir,
        }
    }
    
    /// Build SQL script to register all tables and execute query
    fn build_sql_script(&self, query: &str) -> Result<String> {
        let mut script = String::new();
        
        // Register all tables from metadata
        for table in &self.metadata.tables {
            let table_path = self.data_dir.join(&table.path);
            let table_path_str = table_path.to_str()
                .ok_or_else(|| RcaError::Execution(format!("Invalid path for table {}", table.name)))?;
            
            // Create view from parquet/csv file
            if table_path_str.ends_with(".parquet") {
                script.push_str(&format!("CREATE VIEW {} AS SELECT * FROM read_parquet('{}');\n", table.name, table_path_str));
            } else if table_path_str.ends_with(".csv") {
                script.push_str(&format!("CREATE VIEW {} AS SELECT * FROM read_csv_auto('{}');\n", table.name, table_path_str));
            }
            
            // Also register with just the table name (without schema prefix) if it contains a dot
            if table.name.contains('.') {
                let parts: Vec<&str> = table.name.split('.').collect();
                let base_name = parts.last().unwrap_or(&"");
                if table_path_str.ends_with(".parquet") {
                    script.push_str(&format!("CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}');\n", base_name, table_path_str));
                } else if table_path_str.ends_with(".csv") {
                    script.push_str(&format!("CREATE OR REPLACE VIEW {} AS SELECT * FROM read_csv_auto('{}');\n", base_name, table_path_str));
                }
            }
        }
        
        // Add the actual query
        script.push_str(query);
        if !query.trim().ends_with(';') {
            script.push(';');
        }
        
        Ok(script)
    }
    
    /// Execute a direct SQL query using DuckDB CLI
    /// Returns results in a simple format suitable for display
    pub async fn execute_sql(&self, sql: &str) -> Result<SqlQueryResult> {
        info!("🔍 Executing SQL with DuckDB CLI: {}", sql);
        
        let script = self.build_sql_script(sql)?;
        
        // Execute DuckDB with the script
        let output = Command::new("duckdb")
            .arg(":memory:") // Use in-memory database
            .arg("-json") // Output as JSON
            .arg("-c") // Execute command
            .arg(&script)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| RcaError::Execution(format!("Failed to execute DuckDB: {}. Is DuckDB installed? Try: brew install duckdb", e)))?;
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RcaError::Execution(format!("DuckDB execution failed: {}", stderr)));
        }
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        
        // Parse JSON output
        // DuckDB outputs JSON as an array of objects
        let json_rows: Vec<HashMap<String, serde_json::Value>> = serde_json::from_str(&stdout)
            .map_err(|e| RcaError::Execution(format!("Failed to parse DuckDB JSON output: {}. Output: {}", e, stdout)))?;
        
        // Extract column names from first row
        let columns = if let Some(first_row) = json_rows.first() {
            first_row.keys().cloned().collect()
        } else {
            vec![]
        };
        
        info!("✅ SQL query completed, returned {} rows", json_rows.len());
        
        Ok(SqlQueryResult {
            columns,
            rows: json_rows,
        })
    }
    
    /// Execute a probe query at a specific table
    /// Returns a small sample of rows and basic statistics
    pub async fn execute_probe(
        &self,
        table_name: &str,
        limit: usize,
    ) -> Result<SqlProbeResult> {
        let start = std::time::Instant::now();
        
        info!("🔍 Probing table: {} (limit: {})", table_name, limit);
        
        // Simple SELECT * with limit
        let sql = format!("SELECT * FROM {} LIMIT {}", table_name, limit);
        let result = self.execute_sql(&sql).await?;
        
        let execution_time_ms = start.elapsed().as_millis() as u64;
        
        Ok(SqlProbeResult {
            row_count: result.rows.len(),
            sample_rows: result.rows,
            columns: result.columns,
            summary: None,
            execution_time_ms,
            warnings: vec![],
        })
    }
    
    /// Execute a probe with a filter condition
    pub async fn probe_filter(
        &self,
        table_name: &str,
        filter: &str,
        limit: usize,
    ) -> Result<SqlProbeResult> {
        let start = std::time::Instant::now();
        
        info!("🔍 Probing table: {} with filter: {} (limit: {})", table_name, filter, limit);
        
        let sql = format!("SELECT * FROM {} WHERE {} LIMIT {}", table_name, filter, limit);
        let result = self.execute_sql(&sql).await?;
        
        let execution_time_ms = start.elapsed().as_millis() as u64;
        
        Ok(SqlProbeResult {
            row_count: result.rows.len(),
            sample_rows: result.rows,
            columns: result.columns,
            summary: None,
            execution_time_ms,
            warnings: vec![],
        })
    }
    
    /// Execute a join probe between two tables
    pub async fn probe_join(
        &self,
        left_table: &str,
        right_table: &str,
        join_condition: &str,
        limit: usize,
    ) -> Result<SqlProbeResult> {
        let start = std::time::Instant::now();
        
        info!("🔍 Probing join: {} ⟕ {} on {}", left_table, right_table, join_condition);
        
        let sql = format!(
            "SELECT * FROM {} LEFT JOIN {} ON {} LIMIT {}",
            left_table, right_table, join_condition, limit
        );
        let result = self.execute_sql(&sql).await?;
        
        let execution_time_ms = start.elapsed().as_millis() as u64;
        
        Ok(SqlProbeResult {
            row_count: result.rows.len(),
            sample_rows: result.rows,
            columns: result.columns,
            summary: None,
            execution_time_ms,
            warnings: vec![],
        })
    }
    
    /// Execute an aggregation probe
    pub async fn probe_aggregate(
        &self,
        table_name: &str,
        group_by: &[String],
        aggregates: &[String],
        limit: usize,
    ) -> Result<SqlProbeResult> {
        let start = std::time::Instant::now();
        
        let group_by_clause = if group_by.is_empty() {
            String::new()
        } else {
            format!("GROUP BY {}", group_by.join(", "))
        };
        
        let select_clause = if group_by.is_empty() {
            aggregates.join(", ")
        } else {
            format!("{}, {}", group_by.join(", "), aggregates.join(", "))
        };
        
        info!("🔍 Probing aggregate on {}: {}", table_name, select_clause);
        
        let sql = format!(
            "SELECT {} FROM {} {} LIMIT {}",
            select_clause, table_name, group_by_clause, limit
        );
        let result = self.execute_sql(&sql).await?;
        
        let execution_time_ms = start.elapsed().as_millis() as u64;
        
        Ok(SqlProbeResult {
            row_count: result.rows.len(),
            sample_rows: result.rows,
            columns: result.columns,
            summary: None,
            execution_time_ms,
            warnings: vec![],
        })
    }
    
    /// Query the Knowledge Register (virtual table)
    pub async fn query_knowledge_register(&self, search_term: &str) -> Result<SqlQueryResult> {
        info!("🔍 Querying Knowledge Register for: {}", search_term);
        
        // Load knowledge pages from node_registry/knowledge/
        let knowledge_dir = PathBuf::from("node_registry/knowledge");
        let mut rows = Vec::new();
        
        if knowledge_dir.exists() {
            for entry in std::fs::read_dir(&knowledge_dir)
                .map_err(|e| RcaError::Execution(format!("Failed to read knowledge directory: {}", e)))? {
                let entry = entry.map_err(|e| RcaError::Execution(format!("Failed to read entry: {}", e)))?;
                let path = entry.path();
                
                if path.extension().and_then(|s| s.to_str()) == Some("md") {
                    let content = std::fs::read_to_string(&path)
                        .map_err(|e| RcaError::Execution(format!("Failed to read file: {}", e)))?;
                    
                    // Simple search: check if content contains search term (case-insensitive)
                    if search_term.is_empty() || content.to_lowercase().contains(&search_term.to_lowercase()) {
                        let mut row = HashMap::new();
                        row.insert("id".to_string(), serde_json::json!(path.file_stem().unwrap().to_str().unwrap()));
                        row.insert("title".to_string(), serde_json::json!(path.file_stem().unwrap().to_str().unwrap()));
                        row.insert("content".to_string(), serde_json::json!(content));
                        rows.push(row);
                    }
                }
            }
        }
        
        Ok(SqlQueryResult {
            columns: vec!["id".to_string(), "title".to_string(), "content".to_string()],
            rows,
        })
    }
    
    /// Query the Metadata Register (virtual table)
    pub async fn query_metadata_register(&self, search_term: &str) -> Result<SqlQueryResult> {
        info!("🔍 Querying Metadata Register for: {}", search_term);
        
        // Load metadata pages from node_registry/metadata/
        let metadata_dir = PathBuf::from("node_registry/metadata");
        let mut rows = Vec::new();
        
        if metadata_dir.exists() {
            for entry in std::fs::read_dir(&metadata_dir)
                .map_err(|e| RcaError::Execution(format!("Failed to read metadata directory: {}", e)))? {
                let entry = entry.map_err(|e| RcaError::Execution(format!("Failed to read entry: {}", e)))?;
                let path = entry.path();
                
                if path.extension().and_then(|s| s.to_str()) == Some("json") {
                    let content = std::fs::read_to_string(&path)
                        .map_err(|e| RcaError::Execution(format!("Failed to read file: {}", e)))?;
                    
                    // Simple search: check if content contains search term (case-insensitive)
                    if search_term.is_empty() || content.to_lowercase().contains(&search_term.to_lowercase()) {
                        let mut row = HashMap::new();
                        row.insert("id".to_string(), serde_json::json!(path.file_stem().unwrap().to_str().unwrap()));
                        row.insert("metadata".to_string(), serde_json::json!(content));
                        rows.push(row);
                    }
                }
            }
        }
        
        Ok(SqlQueryResult {
            columns: vec!["id".to_string(), "metadata".to_string()],
            rows,
        })
    }
}
