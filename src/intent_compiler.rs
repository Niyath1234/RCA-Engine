//! Intent Compiler - LLM as Compiler
//! 
//! Compiles natural language queries into strict JSON specifications.
//! The LLM acts as a compiler, not a thinker - outputs only validated JSON.

use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, debug};

/// Intent specification compiled from natural language
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentSpec {
    /// Task type: "RCA" or "DV"
    pub task_type: TaskType,
    
    /// Target metrics (for RCA)
    pub target_metrics: Vec<String>,
    
    /// Entities involved
    pub entities: Vec<String>,
    
    /// Constraints (filters, conditions)
    pub constraints: Vec<ConstraintSpec>,
    
    /// Required grain level
    pub grain: Vec<String>,
    
    /// Time scope (as_of_date, date_range, etc.)
    pub time_scope: Option<TimeScope>,
    
    /// Systems involved (for RCA)
    pub systems: Vec<String>,
    
    /// Validation constraint (for DV)
    pub validation_constraint: Option<ValidationConstraintSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskType {
    RCA,
    DV,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintSpec {
    pub column: Option<String>,
    pub operator: Option<String>,
    pub value: Option<serde_json::Value>,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeScope {
    pub as_of_date: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub time_grain: Option<String>, // "daily", "monthly", etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConstraintSpec {
    pub constraint_type: String,
    pub description: String,
    pub details: serde_json::Value,
}

/// Intent Compiler - Uses LLM to compile natural language to strict JSON
pub struct IntentCompiler {
    llm: LlmClient,
    max_retries: usize,
}

impl IntentCompiler {
    pub fn new(llm: LlmClient) -> Self {
        Self {
            llm,
            max_retries: 2,
        }
    }

    /// Compile natural language query to IntentSpec
    pub async fn compile(&self, query: &str) -> Result<IntentSpec> {
        info!("Compiling intent from query: {}", query);
        
        let schema_prompt = self.get_schema_prompt();
        let user_prompt = format!("Query: {}\n\nCompile this query into the IntentSpec JSON schema.", query);
        
        for attempt in 0..=self.max_retries {
            debug!("Compilation attempt {}", attempt + 1);
            
            match compile_intent_helper(&self.llm, &schema_prompt, &user_prompt).await {
                Ok(json_str) => {
                    match self.parse_and_validate(&json_str) {
                        Ok(spec) => {
                            info!("Successfully compiled intent: {:?}", spec.task_type);
                            return Ok(spec);
                        }
                        Err(e) => {
                            warn!("Failed to parse/validate JSON on attempt {}: {}", attempt + 1, e);
                            if attempt < self.max_retries {
                                continue;
                            } else {
                                return Err(RcaError::Llm(format!(
                                    "Failed to compile intent after {} attempts: {}",
                                    self.max_retries + 1, e
                                )));
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("LLM call failed on attempt {}: {}", attempt + 1, e);
                    if attempt < self.max_retries {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        
        Err(RcaError::Llm("Failed to compile intent after all retries".to_string()))
    }

    fn parse_and_validate(&self, json_str: &str) -> Result<IntentSpec> {
        // Extract JSON from markdown code blocks if present
        let json_str = self.extract_json(json_str);
        
        // Parse JSON
        let spec: IntentSpec = serde_json::from_str(&json_str)
            .map_err(|e| RcaError::Llm(format!("Invalid JSON: {}", e)))?;
        
        // Validate schema
        self.validate_schema(&spec)?;
        
        Ok(spec)
    }

    fn extract_json(&self, text: &str) -> String {
        // Remove markdown code blocks if present
        let text = text.trim();
        if text.starts_with("```json") {
            text.strip_prefix("```json")
                .or_else(|| text.strip_prefix("```"))
                .and_then(|s| s.strip_suffix("```"))
                .map(|s| s.trim())
                .unwrap_or(text)
                .to_string()
        } else if text.starts_with("```") {
            text.strip_prefix("```")
                .and_then(|s| s.strip_suffix("```"))
                .map(|s| s.trim())
                .unwrap_or(text)
                .to_string()
        } else {
            text.to_string()
        }
    }

    fn validate_schema(&self, spec: &IntentSpec) -> Result<()> {
        // Validate task type
        match spec.task_type {
            TaskType::RCA => {
                if spec.systems.is_empty() {
                    return Err(RcaError::Llm("RCA task requires at least one system".to_string()));
                }
                if spec.target_metrics.is_empty() {
                    return Err(RcaError::Llm("RCA task requires at least one target metric".to_string()));
                }
            }
            TaskType::DV => {
                if spec.validation_constraint.is_none() {
                    return Err(RcaError::Llm("DV task requires validation_constraint".to_string()));
                }
            }
        }
        
        // Validate grain is not empty
        if spec.grain.is_empty() {
            return Err(RcaError::Llm("Grain cannot be empty".to_string()));
        }
        
        Ok(())
    }

    fn get_schema_prompt(&self) -> String {
        r#"You are an Intent Compiler. Your job is to compile natural language queries into strict JSON specifications.

You MUST output ONLY valid JSON matching this exact schema:

{
  "task_type": "RCA" | "DV",
  "target_metrics": ["metric1", "metric2"],
  "entities": ["entity1", "entity2"],
  "constraints": [
    {
      "column": "column_name",
      "operator": "=" | ">" | "<" | ">=" | "<=" | "!=" | "in" | "contains",
      "value": <json_value>,
      "description": "human readable description"
    }
  ],
  "grain": ["grain_column1", "grain_column2"],
  "time_scope": {
    "as_of_date": "YYYY-MM-DD" | null,
    "start_date": "YYYY-MM-DD" | null,
    "end_date": "YYYY-MM-DD" | null,
    "time_grain": "daily" | "monthly" | "yearly" | null
  } | null,
  "systems": ["system1", "system2"],
  "validation_constraint": {
    "constraint_type": "value" | "range" | "set" | "uniqueness" | "nullability" | "referential" | "aggregation" | "cross_column" | "format" | "drift" | "volume" | "freshness" | "schema" | "cardinality" | "composition",
    "description": "human readable description",
    "details": { <any json object with constraint-specific details> }
  } | null
}

Rules:
- For RCA: systems and target_metrics are required
- For DV: validation_constraint is required
- grain is always required (cannot be empty)
- IMPORTANT: grain should be entity-level keys (e.g., ["loan_id"], ["customer_id"], ["account_id"])
- DO NOT use filter values as grain (e.g., if query says "for PERSONAL loans", grain should be ["loan_id"], NOT ["loan_type"])
- If query mentions a filter like "for PERSONAL loans", add it as a constraint with column="loan_type", operator="=", value="PERSONAL"
- Output ONLY the JSON object, no markdown, no explanation, no code blocks
- If uncertain about a field, use null or empty array
- Be precise and extract all relevant information from the query"#.to_string()
    }
}

// Helper function to compile intent using LlmClient
async fn compile_intent_helper(
    llm: &LlmClient,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<String> {
    // Use existing call_llm method with combined prompt
    let combined_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
    
    // Check for mock mode by trying to call and checking response
    let response = llm.call_llm(&combined_prompt).await?;
    
    // If response looks like mock, return it; otherwise it's real
    Ok(response)
}

fn mock_compile_intent(query: &str) -> String {
    // Mock implementation for testing
    let query_lower = query.to_lowercase();
    
    if query_lower.contains("~dv") || query_lower.contains("validation") || query_lower.contains("must") || query_lower.contains("cannot") {
        // Data Validation task
        r#"{
  "task_type": "DV",
  "target_metrics": [],
  "entities": ["loan"],
  "constraints": [],
  "grain": ["loan_id"],
  "time_scope": null,
  "systems": [],
  "validation_constraint": {
    "constraint_type": "value",
    "description": "Mock validation constraint",
    "details": {}
  }
}"#.to_string()
    } else {
        // RCA task
        r#"{
  "task_type": "RCA",
  "target_metrics": ["tos"],
  "entities": ["loan"],
  "constraints": [],
  "grain": ["loan_id"],
  "time_scope": {
    "as_of_date": "2025-12-31",
    "start_date": null,
    "end_date": null,
    "time_grain": null
  },
  "systems": ["khatabook", "tb"],
  "validation_constraint": null
}"#.to_string()
    }
}

