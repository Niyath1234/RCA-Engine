use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInterpretation {
    pub system_a: String,
    pub system_b: String,
    pub metric: String,
    pub as_of_date: Option<String>,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbiguityQuestion {
    pub question: String,
    pub options: Vec<AmbiguityOption>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbiguityOption {
    pub id: String,
    pub label: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbiguityResolution {
    pub questions: Vec<AmbiguityQuestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Explanation {
    pub summary: String,
    pub details: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvAnalysis {
    pub grain_column: String,
    pub metric_column: Option<String>,
    pub aggregation_type: String, // "count", "sum", "avg", "max", "min"
    pub filters: Vec<CsvFilter>,
    pub metric_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvFilter {
    pub column: String,
    pub operator: String, // "=", "!=", ">", "<", ">=", "<=", "in", "contains"
    pub value: serde_json::Value, // Can be string, number, array, etc.
}

#[derive(Clone)]
pub struct LlmClient {
    api_key: String,
    base_url: String,
    model: String,
}

impl LlmClient {
    pub fn new(api_key: String, model: String, base_url: String) -> Self {
        Self {
            api_key,
            base_url,
            model,
        }
    }
    
    pub async fn interpret_query(
        &self,
        query: &str,
        business_labels: &crate::metadata::BusinessLabelObject,
        _metrics: &[crate::metadata::Metric],
    ) -> Result<QueryInterpretation> {
        // Token-optimized: Build compact context lists (only IDs and primary aliases)
        let systems: Vec<String> = business_labels.systems.iter()
            .map(|s| {
                // Only include first alias if exists, otherwise just ID
                let alias_hint = s.aliases.first().map(|a| format!(" or {}", a)).unwrap_or_default();
                format!("{}{}", s.system_id, alias_hint)
            })
            .collect();
        
        let metrics: Vec<String> = business_labels.metrics.iter()
            .map(|m| {
                let alias_hint = m.aliases.first().map(|a| format!(" or {}", a)).unwrap_or_default();
                format!("{}{}", m.metric_id, alias_hint)
            })
            .collect();
        
        // Token-optimized prompt: concise, no repetition, minimal example
        let prompt = format!(
            r#"Extract from query and return JSON only:
Query: "{}"
Systems: {}
Metrics: {}
Format: {{"system_a":"id","system_b":"id","metric":"id","as_of_date":"YYYY-MM-DD"|null,"confidence":0.0-1.0}}"#,
            query,
            systems.join(","),
            metrics.join(",")
        );
        
        let response = self.call_llm(&prompt).await?;
        
        // Parse JSON response
        let interpretation: QueryInterpretation = serde_json::from_str(&response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}", e)))?;
        
        Ok(interpretation)
    }
    
    pub async fn resolve_ambiguity(
        &self,
        ambiguity_type: &str,
        options: Vec<AmbiguityOption>,
    ) -> Result<AmbiguityResolution> {
        // Token-optimized: Compact JSON serialization
        let options_json = serde_json::to_string(&options)
            .map_err(|e| RcaError::Llm(format!("Failed to serialize options: {}", e)))?;
        
        // Token-optimized prompt: concise, minimal example
        let prompt = format!(
            r#"Generate ≤3 questions for: "{}"
Options: {}
Return: {{"questions":[{{"question":"text","options":[{{"id":"id","label":"label","description":"desc"}}]}}]}}"#,
            ambiguity_type,
            options_json
        );
        
        let response = self.call_llm(&prompt).await?;
        let resolution: AmbiguityResolution = serde_json::from_str(&response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse ambiguity resolution: {}", e)))?;
        
        Ok(resolution)
    }
    
    pub async fn explain_rca(
        &self,
        rca_result: &crate::rca::RcaResult,
    ) -> Result<Explanation> {
        // For now, skip LLM explanation since we're not testing LLM yet
        // Return a simple explanation based on the result structure
        let summary = format!(
            "RCA completed for {} vs {} - {} metric",
            rca_result.system_a, rca_result.system_b, rca_result.metric
        );
        let details: Vec<String> = rca_result.classifications.iter()
            .map(|c| format!("{}: {}", c.root_cause, c.description))
            .collect();
        
        Ok(Explanation { summary, details })
    }
    
    pub async fn analyze_csv_query(
        &self,
        query: &str,
        columns_a: &[String],
        columns_b: &[String],
        sample_data_a: Option<&str>,
        sample_data_b: Option<&str>,
    ) -> Result<CsvAnalysis> {
        // Build column information
        let common_cols: Vec<String> = columns_a.iter()
            .filter(|c| columns_b.contains(c))
            .cloned()
            .collect();
        
        let all_cols: Vec<String> = columns_a.iter()
            .chain(columns_b.iter())
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        
        // Create prompt for LLM to analyze the query and columns
        let prompt = format!(
            r#"Analyze this reconciliation query and CSV structure. Return JSON only.

Query: "{}"

Available Columns (both CSVs): {}
Common Columns: {}

{}Return JSON with:
{{
  "grain_column": "column_name_for_entity_key",
  "metric_column": "column_name_for_metric_value" | null,
  "aggregation_type": "count" | "sum" | "avg" | "max" | "min",
  "filters": [
    {{"column": "col_name", "operator": "=" | "!=" | ">" | "<" | ">=" | "<=" | "in" | "contains", "value": "value_or_array"}}
  ],
  "metric_name": "descriptive_name"
}}

Rules:
- grain_column: The column that uniquely identifies entities (e.g., loan_id, customer_id)
- metric_column: The numeric column to aggregate (null if counting rows)
- aggregation_type: 
  * "count" if query mentions "numbers", "count", "how many"
  * "sum" if query mentions "total", "sum", "amount"
  * "avg" if query mentions "average", "mean"
- filters: Extract any conditions from query. Match query terms to actual column names and values:
  * If query mentions "MSME", look for columns like msme_flag, psl_type, msme_category, etc.
  * Match the actual value format: could be "yes"/"no", "MSME"/"N/A", true/false, 1/0, etc.
  * Use the exact value format found in the data (check sample data if provided)
- metric_name: Short descriptive name for the metric

Examples:
- Query "MSME numbers not matching" with column psl_type having values ["MSME", "N/A"] 
  -> filter: [{{"column":"psl_type","operator":"=","value":"MSME"}}]
- Query "MSME numbers not matching" with column msme_flag having values ["yes", "no"]
  -> filter: [{{"column":"msme_flag","operator":"=","value":"yes"}}]
- Query "Total disbursement amount differences" -> grain: loan_id, metric: disbursement_amount, agg: sum, filters: []
- Query "Average loan amount for MSME" with column psl_type -> filter: [{{"column":"psl_type","operator":"=","value":"MSME"}}]"#,
            query,
            all_cols.join(", "),
            common_cols.join(", "),
            if let (Some(sa), Some(sb)) = (sample_data_a, sample_data_b) {
                format!("Sample Data A (first 3 rows): {}\nSample Data B (first 3 rows): {}\n\n", sa, sb)
            } else {
                String::new()
            }
        );
        
        let response = self.call_llm(&prompt).await?;
        
        // Clean response - remove markdown code blocks if present
        let cleaned_response = response
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        // Parse JSON response
        let analysis: CsvAnalysis = serde_json::from_str(&cleaned_response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse CSV analysis: {}. Response: {}", e, cleaned_response)))?;
        
        Ok(analysis)
    }
    
    pub async fn call_llm(&self, prompt: &str) -> Result<String> {
        // For now, return dummy response if API key is dummy
        if self.api_key == "dummy-api-key" {
            // Smart dummy response: extract system names from prompt
            // Check for System A first
            let system_a = if prompt.contains("system_a") || prompt.contains("System A") {
                "system_a"
            } else if prompt.contains("khatabook") || prompt.contains("kb") {
                "khatabook"
            } else {
                "system_a" // default fallback for tests
            };
            
            // Check for System B, C, D, E, F by looking for "vs System X" pattern
            let system_b = if prompt.contains("vs System F") || prompt.contains("vs system_f") {
                "system_f"
            } else if prompt.contains("vs System E") || prompt.contains("vs system_e") {
                "system_e"
            } else if prompt.contains("vs System D") || prompt.contains("vs system_d") {
                "system_d"
            } else if prompt.contains("vs System C") || prompt.contains("vs system_c") {
                "system_c"
            } else if prompt.contains("system_b") || prompt.contains("System B") {
                "system_b"
            } else if prompt.contains("tb") || prompt.contains("tally") {
                "tb"
            } else {
                "system_b" // default fallback
            };
            
            // Extract date if present
            let date_match = regex::Regex::new(r"\d{4}-\d{2}-\d{2}").ok();
            let as_of_date = date_match
                .and_then(|re| re.find(prompt))
                .map(|m| format!("\"{}\"", m.as_str()))
                .unwrap_or_else(|| "null".to_string());
            
            return Ok(format!(
                r#"{{"system_a": "{}", "system_b": "{}", "metric": "tos", "as_of_date": {}, "confidence": 0.95}}"#,
                system_a, system_b, as_of_date
            ));
        }
        
        let client = reqwest::Client::new();
        // Token-optimized: concise system message, lower max_completion_tokens for JSON responses
        // Use max_completion_tokens for newer models (like gpt-5.2), fallback to max_tokens for older models
        let mut body = serde_json::json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": "Return JSON only, no text."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
        });
        
        // Use max_completion_tokens for newer models, max_tokens for older ones
        // For reasoning models (gpt-5.2, o1), need more tokens as they use reasoning tokens
        if self.model.starts_with("gpt-5") || self.model.contains("o1") {
            // Reasoning models need more tokens - reasoning tokens + completion tokens
            body["max_completion_tokens"] = serde_json::json!(2000);
        } else if self.model.starts_with("gpt-4") {
            body["max_completion_tokens"] = serde_json::json!(500);
        } else {
            body["max_tokens"] = serde_json::json!(500);
        }
        
        let response = client
            .post(&format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| RcaError::Llm(format!("LLM API call failed: {}", e)))?;
        
        // Check HTTP status
        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(RcaError::Llm(format!("LLM API error ({}): {}", status, error_text)));
        }
        
        let response_json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}", e)))?;
        
        // Check for error in response
        if let Some(error) = response_json.get("error") {
            return Err(RcaError::Llm(format!("LLM API error: {}", serde_json::to_string(error).unwrap_or_else(|_| "Unknown error".to_string()))));
        }
        
        // Extract content with better error message
        let choices = response_json.get("choices")
            .and_then(|c| c.as_array())
            .ok_or_else(|| RcaError::Llm(format!("No choices array in LLM response. Response: {}", serde_json::to_string(&response_json).unwrap_or_else(|_| "Could not serialize".to_string()))))?;
        
        if choices.is_empty() {
            return Err(RcaError::Llm(format!("Empty choices array in LLM response. Response: {}", serde_json::to_string(&response_json).unwrap_or_else(|_| "Could not serialize".to_string()))));
        }
        
        // Check for finish_reason - if it's "length" or "content_filter", content might be truncated
        if let Some(finish_reason) = choices[0].get("finish_reason").and_then(|r| r.as_str()) {
            if finish_reason == "length" {
                eprintln!("⚠️  Warning: LLM response was truncated due to length limit");
            } else if finish_reason == "content_filter" {
                return Err(RcaError::Llm("LLM response was filtered by content policy".to_string()));
            }
        }
        
        let content = choices[0]["message"]["content"]
            .as_str()
            .ok_or_else(|| {
                let response_str = serde_json::to_string(&response_json).unwrap_or_else(|_| "Could not serialize".to_string());
                eprintln!("Debug: Full response structure: {}", response_str);
                RcaError::Llm(format!("No content in LLM response. Response structure: {}", response_str))
            })?;
        
        if content.is_empty() {
            return Err(RcaError::Llm(format!("Empty content in LLM response. Full response: {}", serde_json::to_string(&response_json).unwrap_or_else(|_| "Could not serialize".to_string()))));
        }
        
        Ok(content.to_string())
    }
}

