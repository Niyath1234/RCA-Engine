///! Simplified Intent Compiler - Enhanced to auto-detect systems from table names
///! 
///! This module extends the existing intent compiler to:
///! 1. Auto-detect systems from table names mentioned in the question
///! 2. Use table registry to infer system membership
///! 3. Generate metadata on-the-fly from uploaded tables

use crate::intent_compiler::{IntentSpec, TaskType};
use crate::table_upload::TableRegistry;
use crate::llm::LlmClient;
use serde_json::json;

/// Enhanced intent compiler that auto-detects systems
pub struct SimplifiedIntentCompiler {
    pub table_registry: TableRegistry,
    pub llm_client: Option<LlmClient>,
}

impl SimplifiedIntentCompiler {
    pub fn new(table_registry: TableRegistry, llm_client: Option<LlmClient>) -> Self {
        Self {
            table_registry,
            llm_client,
        }
    }
    
    /// Compile intent with automatic system detection
    /// 
    /// Example:
    /// Query: "TOS recon between khatabook and TB"
    /// Auto-detects: systems = ["khatabook", "tb"]
    pub async fn compile_with_auto_detection(
        &self,
        query: &str,
    ) -> Result<SimplifiedIntent, Box<dyn std::error::Error>> {
        // Step 1: Detect systems from the question
        let detected_systems = self.table_registry.detect_systems_from_question(query);
        
        if detected_systems.is_empty() {
            return Err("Could not detect any systems from the question. Please mention table names like 'khatabook' or 'TB'.".into());
        }
        
        if detected_systems.len() < 2 {
            return Err(format!(
                "Only detected {} system(s): {}. Reconciliation requires at least 2 systems.",
                detected_systems.len(),
                detected_systems.join(", ")
            ).into());
        }
        
        // Step 2: Extract metric name from question
        let metric_name = self.extract_metric_name(query).await?;
        
        // Step 3: Find all tables for each system
        let mut system_tables = std::collections::HashMap::new();
        for system in &detected_systems {
            let tables = self.table_registry.find_tables_by_prefix(system);
            system_tables.insert(system.clone(), tables);
        }
        
        // Step 4: Generate default rules for this metric
        let suggested_rules = self.table_registry.generate_default_rules(&metric_name);
        
        // Step 5: Create simplified intent
        Ok(SimplifiedIntent {
            query: query.to_string(),
            metric_name,
            detected_systems,
            system_tables: system_tables.into_iter()
                .map(|(k, v)| (k, v.into_iter().map(|t| t.upload.table_name.clone()).collect()))
                .collect(),
            suggested_rules,
        })
    }
    
    /// Extract metric name from question using LLM or pattern matching
    async fn extract_metric_name(&self, query: &str) -> Result<String, Box<dyn std::error::Error>> {
        let query_lower = query.to_lowercase();
        
        // Pattern matching for common metrics
        if query_lower.contains("tos") || query_lower.contains("outstanding") {
            return Ok("total_outstanding".to_string());
        } else if query_lower.contains("recovery") {
            return Ok("recovery".to_string());
        } else if query_lower.contains("disbursement") {
            return Ok("disbursement".to_string());
        } else if query_lower.contains("recon") {
            // Generic reconciliation - try to extract metric from "X recon"
            if let Some(pos) = query_lower.find("recon") {
                let before = &query_lower[..pos].trim();
                if let Some(word_start) = before.rfind(' ') {
                    let metric = &before[word_start..].trim();
                    return Ok(metric.to_string());
                }
            }
            return Ok("amount".to_string()); // Default
        }
        
        // If we have LLM, use it to extract metric
        if let Some(ref llm) = self.llm_client {
            let prompt = format!(
                r#"Extract the metric name being reconciled from this question:

Question: {}

Common metrics:
- total_outstanding (TOS)
- recovery
- disbursement  
- balance
- amount

Return only the metric name, nothing else."#,
                query
            );
            
            // Use the LLM's call_llm method
            match llm.call_llm(&prompt).await {
                Ok(response) => {
                    let metric = response.trim().to_lowercase();
                    if !metric.is_empty() {
                        return Ok(metric);
                    }
                }
                Err(_) => {}
            }
        }
        
        // Default
        Ok("amount".to_string())
    }
    
    /// Generate full metadata JSON from table registry
    /// This creates the metadata needed by the RCA engine
    pub fn generate_metadata(&self) -> Result<String, Box<dyn std::error::Error>> {
        self.table_registry.generate_full_metadata()
    }
}

/// Simplified intent structure
#[derive(Debug, Clone)]
pub struct SimplifiedIntent {
    /// Original query
    pub query: String,
    
    /// Detected metric name
    pub metric_name: String,
    
    /// Auto-detected systems (e.g., ["khatabook", "tb"])
    pub detected_systems: Vec<String>,
    
    /// Tables for each system
    pub system_tables: std::collections::HashMap<String, Vec<String>>,
    
    /// Auto-generated business rules suggestions
    pub suggested_rules: Vec<String>,
}

impl SimplifiedIntent {
    /// Convert to full IntentSpec for RCA engine
    pub fn to_intent_spec(&self) -> IntentSpec {
        IntentSpec {
            task_type: TaskType::RCA,
            systems: self.detected_systems.clone(),
            target_metrics: vec![self.metric_name.clone()],
            entities: vec![], // Will be inferred by task grounder
            grain: vec![], // Will be inferred from tables
            constraints: vec![], // Can be extracted from query if needed
            time_scope: None,
            validation_constraint: None,
        }
    }
    
    /// Display human-readable summary
    pub fn summary(&self) -> String {
        format!(
            r#"Detected Intent:
- Metric: {}
- Systems: {}
- Tables:
{}
- Suggested Rules:
{}"#,
            self.metric_name,
            self.detected_systems.join(" vs "),
            self.system_tables.iter()
                .map(|(sys, tables)| format!("  {}: {}", sys, tables.join(", ")))
                .collect::<Vec<_>>()
                .join("\n"),
            self.suggested_rules.iter()
                .map(|r| format!("  - {}", r))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_upload::SimpleTableUpload;
    use std::path::PathBuf;
    use std::collections::HashMap;
    
    #[test]
    fn test_system_detection() {
        let mut registry = TableRegistry::new();
        
        // Manually create tables (in real use, would call register_table)
        let tables = vec![
            ("khatabook_customers", "khatabook"),
            ("khatabook_loans", "khatabook"),
            ("tb_customer_data", "tb"),
            ("tb_loan_details", "tb"),
        ];
        
        for (name, prefix) in tables {
            registry.tables.push(crate::table_upload::RegisteredTable {
                upload: SimpleTableUpload {
                    table_name: name.to_string(),
                    csv_path: PathBuf::from("test.csv"),
                    primary_keys: vec!["id".to_string()],
                    column_descriptions: HashMap::new(),
                },
                schema: crate::table_upload::TableSchema { columns: vec![] },
                table_prefix: Some(prefix.to_string()),
                row_count: 0,
            });
        }
        
        let systems = registry.detect_systems_from_question("TOS recon between khatabook and TB");
        assert_eq!(systems.len(), 2);
        assert!(systems.contains(&"khatabook".to_string()));
        assert!(systems.contains(&"tb".to_string()));
    }
}

