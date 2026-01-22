//! Data Assistant - Cursor-like AI assistant for data queries
//! 
//! This module provides a comprehensive AI assistant that can:
//! 1. Answer questions about data/metadata using knowledge base
//! 2. Execute queries when appropriate
//! 3. Ask for clarification when needed
//! 4. Provide natural language answers based on node registry, knowledge pages, and metadata

use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use crate::node_registry::NodeRegistry;
use crate::metadata::Metadata;
use crate::intent_compiler::{IntentCompiler, IntentCompilationResult, TaskType};
use crate::query_engine::QueryEngine;
use crate::sql_engine::SqlEngine;
use crate::sql_compiler::{SqlCompiler, SqlIntent};
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Response from the data assistant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssistantResponse {
    /// Type of response
    pub response_type: ResponseType,
    
    /// Natural language answer
    pub answer: String,
    
    /// If clarification is needed
    pub clarification: Option<ClarificationRequest>,
    
    /// If a query was executed, include the result
    pub query_result: Option<serde_json::Value>,
    
    /// Relevant nodes/knowledge found
    pub relevant_knowledge: Vec<KnowledgeReference>,
    
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
    
    /// Reasoning steps taken
    pub reasoning_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseType {
    /// Direct answer to question
    Answer,
    /// Query execution result
    QueryResult,
    /// Needs clarification
    NeedsClarification,
    /// Error occurred
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClarificationRequest {
    pub question: String,
    pub missing_pieces: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeReference {
    pub ref_id: String,
    pub node_type: String,
    pub name: String,
    pub relevance_score: f64,
    pub excerpt: Option<String>,
}

/// Data Assistant - Cursor-like AI for data queries
pub struct DataAssistant {
    llm: LlmClient,
    node_registry: NodeRegistry,
    metadata: Metadata,
    data_dir: PathBuf,
}

impl DataAssistant {
    /// Create a new Data Assistant
    pub fn new(
        llm: LlmClient,
        node_registry: NodeRegistry,
        metadata: Metadata,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            llm,
            node_registry,
            metadata,
            data_dir,
        }
    }
    
    /// Answer a question using all available knowledge
    pub async fn answer(&self, question: &str) -> Result<AssistantResponse> {
        info!("🤖 Data Assistant: Processing question: {}", question);
        
        let mut reasoning_steps = Vec::new();
        reasoning_steps.push(format!("Analyzing question: {}", question));
        
        // Step 1: Search knowledge base for relevant information
        info!("📚 Step 1: Searching knowledge base...");
        let (nodes, knowledge_pages, metadata_pages) = self.node_registry.search_all(question);
        reasoning_steps.push(format!("Found {} relevant nodes in knowledge base", nodes.len()));
        
        // Step 2: Build context from knowledge base
        let knowledge_context = self.build_knowledge_context(nodes, knowledge_pages, metadata_pages);
        
        // Step 3: Determine if this is a query execution request or a knowledge question
        info!("🔍 Step 2: Determining query type...");
        let query_type = self.classify_query(question, &knowledge_context).await?;
        reasoning_steps.push(format!("Query type: {:?}", query_type));
        
        // Re-search to get references for knowledge references
        let (nodes_ref, knowledge_pages_ref, _) = self.node_registry.search_all(question);
        
        match query_type {
            QueryType::KnowledgeQuestion => {
                // Answer using knowledge base
                let mut response = self.answer_knowledge_question(question, &knowledge_context, reasoning_steps).await?;
                response.relevant_knowledge = self.build_knowledge_references(nodes_ref, knowledge_pages_ref);
                Ok(response)
            }
            QueryType::DataQuery => {
                // Execute as a data query
                let mut response = self.execute_data_query(question, &knowledge_context, reasoning_steps).await?;
                response.relevant_knowledge = self.build_knowledge_references(nodes_ref, knowledge_pages_ref);
                Ok(response)
            }
            QueryType::NeedsClarification(clarification) => {
                Ok(AssistantResponse {
                    response_type: ResponseType::NeedsClarification,
                    answer: format!("I need more information to answer your question."),
                    clarification: Some(clarification),
                    query_result: None,
                    relevant_knowledge: self.build_knowledge_references(nodes_ref, knowledge_pages_ref),
                    confidence: 0.5,
                    reasoning_steps,
                })
            }
        }
    }
    
    /// Build context string from knowledge base search results
    fn build_knowledge_context(
        &self,
        nodes: Vec<&crate::node_registry::Node>,
        knowledge_pages: Vec<&crate::node_registry::KnowledgePage>,
        metadata_pages: Vec<&crate::node_registry::MetadataPage>,
    ) -> String {
        let mut context_parts = Vec::new();
        
        for (idx, node) in nodes.iter().enumerate() {
            context_parts.push(format!("=== Node: {} ({}) ===", node.name, node.node_type));
            
            if let Some(kp) = knowledge_pages.get(idx) {
                context_parts.push(format!("Knowledge: {}", kp.full_text));
            }
            
            if let Some(mp) = metadata_pages.get(idx) {
                if !mp.technical_data.is_empty() {
                    context_parts.push(format!("Metadata: {:?}", mp.technical_data));
                }
            }
        }
        
        // Add metadata context
        context_parts.push("\n=== Available Systems ===".to_string());
        let systems: Vec<String> = self.metadata.tables.iter()
            .map(|t| t.system.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        context_parts.push(format!("Systems: {}", systems.join(", ")));
        
        context_parts.push("\n=== Available Tables ===".to_string());
        for table in &self.metadata.tables {
            context_parts.push(format!("- {} (system: {}, entity: {})", 
                table.name, table.system, table.entity));
        }
        
        context_parts.push("\n=== Available Metrics ===".to_string());
        for metric in &self.metadata.metrics {
            context_parts.push(format!("- {}: {}", metric.id, metric.description));
        }
        
        context_parts.join("\n\n")
    }
    
    /// Build knowledge references for response
    fn build_knowledge_references(
        &self,
        nodes: Vec<&crate::node_registry::Node>,
        knowledge_pages: Vec<&crate::node_registry::KnowledgePage>,
    ) -> Vec<KnowledgeReference> {
        nodes.iter().enumerate().map(|(idx, node)| {
            let excerpt = knowledge_pages.get(idx).map(|kp| {
                // Get first 200 chars of full_text
                let text = &kp.full_text;
                if text.len() > 200 {
                    format!("{}...", &text[..200])
                } else {
                    text.clone()
                }
            });
            
            KnowledgeReference {
                ref_id: node.ref_id.clone(),
                node_type: node.node_type.clone(),
                name: node.name.clone(),
                relevance_score: 0.8, // Could be calculated based on search match quality
                excerpt,
            }
        }).collect()
    }
    
    /// Classify the query type using LLM
    async fn classify_query(
        &self,
        question: &str,
        knowledge_context: &str,
    ) -> Result<QueryType> {
        let prompt = format!(
            r#"You are a data assistant analyzing user questions. Classify the question and determine the best response.

USER QUESTION: "{}"

KNOWLEDGE BASE CONTEXT:
{}

CLASSIFICATION RULES:
1. KNOWLEDGE_QUESTION: Questions about what data exists, what tables/columns mean, relationships, business rules, etc.
   Examples:
   - "What tables are in khatabook?"
   - "What does the outstanding column mean?"
   - "How are loans and customers related?"
   - "What is the TOS metric?"

2. DATA_QUERY: Questions asking for actual data values, calculations, aggregations, or asking "what is" with specific values.
   Examples:
   - "What is the total outstanding of khatabook as of 2024-01-15?"
   - "What is the final outstanding at the end of the year?"
   - "Show me all customers in system A"
   - "What is the average loan amount?"
   - "What is the total outstanding balance for SCF products?"
   - "How many loans are there?"

3. NEEDS_CLARIFICATION: Questions that are unclear or missing critical information.
   Examples:
   - "What is outstanding?" (missing: which system? which date?)
   - "Show me data" (too vague)

OUTPUT FORMAT (JSON only, no markdown):
{{
  "type": "knowledge_question|data_query|needs_clarification",
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation",
  "clarification": {{"question": "...", "missing_pieces": ["..."]}} // Only if type is needs_clarification
}}

Be decisive - if the question clearly asks for data values, classify as data_query. If it asks about metadata/knowledge, classify as knowledge_question.

IMPORTANT: For data queries, be confident and infer reasonable defaults:
- If question mentions "outstanding" or "balance", assume total_outstanding_balance column
- If question mentions "SCF" or "scf", assume product_type = 'scf' 
- If question mentions "disbursed", assume status = 'DISBURSED'
- If question mentions "not deleted" or "active", assume __is_deleted = false
- If question mentions "end of year" or "year-end", use date_constraint with "end_of_year"
- Use the most relevant table from the schema (e.g., assetsdb_gold_lmsdata_loan for loan data)
- Only ask for clarification if the question is truly ambiguous or missing critical information"#,
            question, knowledge_context
        );
        
        let response = self.llm.call_llm(&prompt).await?;
        let cleaned = response
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        let json: serde_json::Value = serde_json::from_str(&cleaned)
            .map_err(|e| RcaError::Llm(format!("Failed to parse classification: {}. Response: {}", e, cleaned)))?;
        
        let query_type_str = json["type"].as_str()
            .ok_or_else(|| RcaError::Llm("Missing 'type' in classification response".to_string()))?;
        
        match query_type_str {
            "knowledge_question" => Ok(QueryType::KnowledgeQuestion),
            "data_query" => Ok(QueryType::DataQuery),
            "needs_clarification" => {
                let clarification = json.get("clarification")
                    .and_then(|c| {
                        Some(ClarificationRequest {
                            question: c["question"].as_str()?.to_string(),
                            missing_pieces: c["missing_pieces"]
                                .as_array()?
                                .iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect(),
                        })
                    })
                    .unwrap_or_else(|| ClarificationRequest {
                        question: "Could you provide more details?".to_string(),
                        missing_pieces: vec![],
                    });
                Ok(QueryType::NeedsClarification(clarification))
            }
            _ => Err(RcaError::Llm(format!("Unknown query type: {}", query_type_str))),
        }
    }
    
    /// Answer a knowledge question using LLM and knowledge base
    async fn answer_knowledge_question(
        &self,
        question: &str,
        knowledge_context: &str,
        mut reasoning_steps: Vec<String>,
    ) -> Result<AssistantResponse> {
        info!("📖 Answering knowledge question...");
        reasoning_steps.push("Using knowledge base to answer question".to_string());
        
        let prompt = format!(
            r#"You are a helpful data assistant. Answer the user's question using the knowledge base context provided.

USER QUESTION: "{}"

KNOWLEDGE BASE CONTEXT:
{}

INSTRUCTIONS:
1. Answer the question directly and concisely
2. Use information from the knowledge base context
3. If information is not available, say so clearly
4. Provide specific examples when relevant (table names, column names, etc.)
5. Be helpful and conversational

ANSWER:"#,
            question, knowledge_context
        );
        
        let answer = self.llm.call_llm(&prompt).await?;
        let cleaned_answer = answer.trim().trim_start_matches("ANSWER:").trim().to_string();
        
        reasoning_steps.push("Generated answer from knowledge base".to_string());
        
        Ok(AssistantResponse {
            response_type: ResponseType::Answer,
            answer: cleaned_answer,
            clarification: None,
            query_result: None,
            relevant_knowledge: vec![], // Will be populated by caller
            confidence: 0.85,
            reasoning_steps,
        })
    }
    
    /// Execute a data query by generating SQL and executing it
    async fn execute_data_query(
        &self,
        question: &str,
        knowledge_context: &str,
        mut reasoning_steps: Vec<String>,
    ) -> Result<AssistantResponse> {
        info!("🔍 Executing data query...");
        reasoning_steps.push("Classified as data query - generating SQL".to_string());
        
        // Step 1: Generate SQL intent JSON from natural language question
        let intent = self.generate_sql_intent(question, knowledge_context).await?;
        reasoning_steps.push(format!("Generated SQL intent: {:?}", intent));
        
        // Step 2: Compile intent to SQL using deterministic compiler
        let sql = self.compile_sql_from_intent(&intent)?;
        reasoning_steps.push(format!("Compiled SQL: {}", sql));
        
        // Step 2: Execute SQL using DuckDB
        let sql_engine = SqlEngine::new(self.metadata.clone(), self.data_dir.clone());
        match sql_engine.execute_sql(&sql).await {
            Ok(result) => {
                reasoning_steps.push(format!("Query executed successfully, returned {} rows", result.rows.len()));
                
                // Step 3: Generate natural language answer from results
                let answer = self.generate_answer_from_results(question, &sql, &result).await?;
                
                let result_json = serde_json::json!({
                    "sql": sql,
                    "columns": result.columns,
                    "rows": result.rows,
                    "row_count": result.rows.len()
                });
                
                Ok(AssistantResponse {
                    response_type: ResponseType::QueryResult,
                    answer,
                    clarification: None,
                    query_result: Some(result_json),
                    relevant_knowledge: vec![],
                    confidence: 0.9,
                    reasoning_steps,
                })
            }
            Err(e) => {
                reasoning_steps.push(format!("SQL execution failed: {}", e));
                
                // Try to provide helpful error message
                let error_msg = if e.to_string().contains("not found") {
                    format!("I couldn't find the required tables or columns. Error: {}. Please check if the table names and column names are correct.", e)
                } else {
                    format!("Failed to execute query: {}. The generated SQL was: {}", e, sql)
                };
                
                Ok(AssistantResponse {
                    response_type: ResponseType::Error,
                    answer: error_msg,
                    clarification: None,
                    query_result: Some(serde_json::json!({
                        "sql": sql,
                        "error": e.to_string()
                    })),
                    relevant_knowledge: vec![],
                    confidence: 0.0,
                    reasoning_steps,
                })
            }
        }
    }
    
    /// Generate SQL intent JSON from natural language question using LLM
    async fn generate_sql_intent(
        &self,
        question: &str,
        knowledge_context: &str,
    ) -> Result<SqlIntent> {
        // Build table schema information
        let mut schema_info = String::new();
        schema_info.push_str("AVAILABLE TABLES AND COLUMNS:\n");
        for table in &self.metadata.tables {
            schema_info.push_str(&format!("\nTable: {} (system: {}, entity: {})\n", 
                table.name, table.system, table.entity));
            schema_info.push_str("Columns:\n");
            if let Some(ref columns) = table.columns {
                for col in columns {
                    schema_info.push_str(&format!("  - {} ({})\n", 
                        col.name, 
                        col.data_type.as_ref().unwrap_or(&"unknown".to_string())));
                }
            }
            if let Some(ref time_col) = table.time_column {
                schema_info.push_str(&format!("  Time column: {}\n", time_col));
            }
        }
        
        let prompt = format!(
            r#"You are a SQL intent generator. Convert the user's natural language question into a JSON specification that will be used to generate SQL.

USER QUESTION: "{}"

SCHEMA INFORMATION:
{}

KNOWLEDGE CONTEXT:
{}

INSTRUCTIONS:
1. Generate a JSON object with the following structure:
{{
  "tables": ["table_name_or_pattern"],
  "columns": [{{"name": "column_pattern", "table": "optional_table", "alias": "optional_alias"}}],
  "aggregations": [{{"function": "sum|avg|count|min|max", "column": "column_pattern", "table": "optional", "alias": "optional"}}],
  "filters": [{{"column": "column_pattern", "table": "optional", "operator": "=|!=|>|<|>=|<=|IN|LIKE|IS NULL", "value": "value_or_array"}}],
  "group_by": ["column_pattern"],
  "order_by": [{{"column": "column_pattern", "table": "optional", "direction": "ASC|DESC"}}],
  "limit": number,
  "joins": [{{"left_table": "table1", "right_table": "table2", "join_type": "INNER|LEFT|RIGHT|FULL", "condition": [{{"left_column": "col1", "right_column": "col2"}}]}}],
  "date_constraint": {{"column": "optional_date_column", "value": "2024-12-31" | {{"start": "...", "end": "..."}} | "end_of_year|start_of_year|today"}}
}}

2. Use partial/pattern matching for table and column names (e.g., "outstanding" will match "total_outstanding_balance")
3. For "end of year", use {{"value": "end_of_year"}}
4. For aggregations like "total", use {{"function": "sum"}}
5. Return ONLY valid JSON, no markdown, no explanations

JSON:"#,
            question, schema_info, knowledge_context
        );
        
        let json_str = self.llm.call_llm(&prompt).await?;
        
        // Clean up JSON - remove markdown code blocks
        let cleaned_json = json_str
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .trim_start_matches("JSON:")
            .trim();
        
        // Parse JSON intent
        let intent: SqlIntent = serde_json::from_str(cleaned_json)
            .map_err(|e| RcaError::Llm(format!("Failed to parse SQL intent JSON: {}. Response: {}", e, cleaned_json)))?;
        
        Ok(intent)
    }
    
    /// Generate SQL from intent using deterministic compiler
    fn compile_sql_from_intent(&self, intent: &SqlIntent) -> Result<String> {
        let compiler = SqlCompiler::new(self.metadata.clone());
        compiler.compile(intent)
    }
    
    /// Generate natural language answer from SQL results
    async fn generate_answer_from_results(
        &self,
        question: &str,
        sql: &str,
        result: &crate::sql_engine::SqlQueryResult,
    ) -> Result<String> {
        // Format results for LLM
        let results_summary = if result.rows.is_empty() {
            "No rows returned.".to_string()
        } else if result.rows.len() == 1 {
            // Single row result - format nicely
            let row = &result.rows[0];
            let mut parts = Vec::new();
            for (col, val) in row {
                let val_str = match val {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "NULL".to_string(),
                    _ => format!("{}", val),
                };
                parts.push(format!("{}: {}", col, val_str));
            }
            parts.join(", ")
        } else {
            // Multiple rows - summarize
            format!("Returned {} rows with columns: {}", result.rows.len(), result.columns.join(", "))
        };
        
        let prompt = format!(
            r#"You are a helpful data assistant. Answer the user's question based on the SQL query results.

USER QUESTION: "{}"

SQL QUERY EXECUTED:
{}

QUERY RESULTS:
{}

INSTRUCTIONS:
1. Answer the question directly and naturally
2. Include the actual values from the results
3. If results show numbers, format them nicely (e.g., "8.4 million" instead of "8400000")
4. Be conversational and helpful
5. If no results, explain why (e.g., "No data found matching your criteria")

ANSWER:"#,
            question, sql, results_summary
        );
        
        let answer = self.llm.call_llm(&prompt).await?;
        let cleaned_answer = answer.trim().trim_start_matches("ANSWER:").trim().to_string();
        
        Ok(cleaned_answer)
    }
    
    /// Generate a natural language summary of query results
    async fn generate_query_summary(
        &self,
        question: &str,
        result: &crate::query_engine::QueryResult,
    ) -> Result<String> {
        let prompt = format!(
            r#"Generate a natural language summary of query results.

ORIGINAL QUESTION: "{}"

QUERY RESULTS:
- System: {}
- Metric: {}
- As-of Date: {:?}
- Row Count: {}
- Summary Statistics:
  - Total: {:?}
  - Average: {:?}
  - Min: {:?}
  - Max: {:?}
  - Distinct Count: {}

Generate a concise, natural language answer to the original question using these results."#,
            question,
            result.system,
            result.metric,
            result.as_of_date,
            result.data.row_count,
            result.summary.total,
            result.summary.average,
            result.summary.min,
            result.summary.max,
            result.summary.distinct_count,
        );
        
        let summary = self.llm.call_llm(&prompt).await?;
        Ok(summary.trim().to_string())
    }
}

#[derive(Debug)]
enum QueryType {
    KnowledgeQuestion,
    DataQuery,
    NeedsClarification(ClarificationRequest),
}

