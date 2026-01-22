//! SQL Compiler - Deterministic SQL generation from JSON intent
//! 
//! Takes a JSON intent specification and generates valid SQL using actual table/column metadata.
//! This ensures SQL is always correct and uses real schema information.

use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

/// SQL Intent - JSON specification from LLM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlIntent {
    /// Tables to query (can be partial names, will be matched)
    pub tables: Vec<String>,
    
    /// Columns to select (can be partial names, will be matched)
    pub columns: Option<Vec<ColumnSpec>>,
    
    /// Aggregations to perform
    pub aggregations: Option<Vec<AggregationSpec>>,
    
    /// Filter conditions
    pub filters: Option<Vec<FilterSpec>>,
    
    /// Group by columns
    pub group_by: Option<Vec<String>>,
    
    /// Order by columns
    pub order_by: Option<Vec<OrderBySpec>>,
    
    /// Limit number of rows
    pub limit: Option<usize>,
    
    /// Joins between tables
    pub joins: Option<Vec<JoinSpec>>,
    
    /// Date/time constraints
    #[serde(default)]
    pub date_constraint: Option<DateConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSpec {
    /// Column name or pattern (e.g., "outstanding", "balance")
    pub name: String,
    
    /// Table name (optional, for disambiguation)
    pub table: Option<String>,
    
    /// Alias for the column
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationSpec {
    /// Aggregation function: "sum", "avg", "count", "min", "max"
    pub function: String,
    
    /// Column to aggregate
    pub column: String,
    
    /// Table name (optional)
    pub table: Option<String>,
    
    /// Alias for the result
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSpec {
    /// Column name
    pub column: String,
    
    /// Table name (optional)
    pub table: Option<String>,
    
    /// Operator: "=", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "IS NULL", "IS NOT NULL"
    pub operator: String,
    
    /// Value (can be string, number, boolean, or array for IN)
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBySpec {
    /// Column name
    pub column: String,
    
    /// Table name (optional)
    pub table: Option<String>,
    
    /// Direction: "ASC" or "DESC"
    pub direction: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinSpec {
    /// Left table name
    pub left_table: String,
    
    /// Right table name
    pub right_table: String,
    
    /// Join type: "INNER", "LEFT", "RIGHT", "FULL"
    pub join_type: Option<String>,
    
    /// Join condition (column pairs)
    pub condition: Vec<JoinCondition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    /// Left column name
    pub left_column: String,
    
    /// Right column name
    pub right_column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateConstraint {
    /// Column name for date filtering
    pub column: Option<String>,
    
    /// Date value or range
    pub value: DateValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DateValue {
    /// Single date: "2024-12-31"
    Single(String),
    /// Date range: {"start": "2024-01-01", "end": "2024-12-31"}
    Range { start: String, end: String },
    /// Relative date: "end_of_year", "start_of_year", "today", "yesterday"
    Relative(String),
}

/// SQL Compiler - Deterministic SQL generation
pub struct SqlCompiler {
    metadata: Metadata,
}

impl SqlCompiler {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
    
    /// Compile SQL intent to actual SQL query
    pub fn compile(&self, intent: &SqlIntent) -> Result<String> {
        info!("🔧 Compiling SQL intent to query...");
        
        // Step 1: Resolve table names
        let tables = self.resolve_tables(&intent.tables)?;
        if tables.is_empty() {
            return Err(RcaError::Execution("No matching tables found".to_string()));
        }
        
        let main_table = &tables[0];
        
        // Step 2: Build SELECT clause
        let select_clause = self.build_select_clause(intent, main_table)?;
        
        // Step 3: Build FROM clause
        let from_clause = format!("FROM {}", main_table.name);
        
        // Step 4: Build JOIN clauses
        let join_clauses = self.build_join_clauses(intent, &tables)?;
        
        // Step 5: Build WHERE clause
        let where_clause = self.build_where_clause(intent, &tables)?;
        
        // Step 6: Build GROUP BY clause
        let group_by_clause = self.build_group_by_clause(intent, &tables)?;
        
        // Step 7: Build ORDER BY clause
        let order_by_clause = self.build_order_by_clause(intent, &tables)?;
        
        // Step 8: Build LIMIT clause
        let limit_clause = if let Some(limit) = intent.limit {
            format!("LIMIT {}", limit)
        } else {
            String::new()
        };
        
        // Combine all clauses
        let mut sql_parts = vec![select_clause, from_clause];
        sql_parts.extend(join_clauses);
        if !where_clause.is_empty() {
            sql_parts.push(where_clause);
        }
        if !group_by_clause.is_empty() {
            sql_parts.push(group_by_clause);
        }
        if !order_by_clause.is_empty() {
            sql_parts.push(order_by_clause);
        }
        if !limit_clause.is_empty() {
            sql_parts.push(limit_clause);
        }
        
        let sql = sql_parts.join(" ");
        info!("✅ Generated SQL: {}", sql);
        
        Ok(sql)
    }
    
    /// Resolve table names from intent (match partial names)
    fn resolve_tables(&self, table_names: &[String]) -> Result<Vec<&crate::metadata::Table>> {
        let mut resolved = Vec::new();
        let mut resolved_names = std::collections::HashSet::new();
        
        for table_name in table_names {
            // Try exact match first
            if let Some(table) = self.metadata.tables.iter().find(|t| t.name == *table_name) {
                if !resolved_names.contains(&table.name) {
                    resolved_names.insert(table.name.clone());
                    resolved.push(table);
                }
                continue;
            }
            
            // Try partial match (contains)
            if let Some(table) = self.metadata.tables.iter()
                .find(|t| (t.name.contains(table_name) || table_name.contains(&t.name)) && !resolved_names.contains(&t.name)) {
                resolved_names.insert(table.name.clone());
                resolved.push(table);
                continue;
            }
            
            // Try matching by entity or system
            if let Some(table) = self.metadata.tables.iter()
                .find(|t| (t.entity == *table_name || t.system == *table_name) && !resolved_names.contains(&t.name)) {
                resolved_names.insert(table.name.clone());
                resolved.push(table);
            }
        }
        
        Ok(resolved)
    }
    
    /// Build SELECT clause
    fn build_select_clause(&self, intent: &SqlIntent, main_table: &crate::metadata::Table) -> Result<String> {
        let mut select_parts = Vec::new();
        
        // Handle aggregations
        if let Some(ref aggregations) = intent.aggregations {
            for agg in aggregations {
                let column = self.resolve_column(&agg.column, &agg.table, main_table)?;
                let func = agg.function.to_uppercase();
                let alias = agg.alias.as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                select_parts.push(format!("{}({}){}", func, column, alias));
            }
        }
        
        // Handle regular columns
        if let Some(ref columns) = intent.columns {
            for col_spec in columns {
                let column = self.resolve_column(&col_spec.name, &col_spec.table, main_table)?;
                let alias = col_spec.alias.as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                select_parts.push(format!("{}{}", column, alias));
            }
        }
        
        // Default: SELECT * if nothing specified
        if select_parts.is_empty() {
            select_parts.push("*".to_string());
        }
        
        Ok(format!("SELECT {}", select_parts.join(", ")))
    }
    
    /// Resolve column name (match partial names)
    fn resolve_column(&self, column_name: &str, table_name: &Option<String>, default_table: &crate::metadata::Table) -> Result<String> {
        let table = if let Some(ref tname) = table_name {
            self.resolve_tables(&[tname.clone()])?
                .first()
                .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", tname)))?
        } else {
            default_table
        };
        
        // Try exact match
        if let Some(ref columns) = table.columns {
            if let Some(col) = columns.iter().find(|c| c.name == *column_name) {
                return Ok(format!("{}.{}", table.name, col.name));
            }
            
            // Try partial match (contains, case-insensitive)
            if let Some(col) = columns.iter().find(|c| 
                c.name.to_lowercase().contains(&column_name.to_lowercase()) ||
                column_name.to_lowercase().contains(&c.name.to_lowercase())
            ) {
                return Ok(format!("{}.{}", table.name, col.name));
            }
        }
        
        // If not found, return as-is (might be an expression)
        Ok(format!("{}.{}", table.name, column_name))
    }
    
    /// Build WHERE clause
    fn build_where_clause(&self, intent: &SqlIntent, tables: &[&crate::metadata::Table]) -> Result<String> {
        let mut conditions = Vec::new();
        
        // Handle filters
        if let Some(ref filters) = intent.filters {
            for filter in filters {
                let column = self.resolve_column(&filter.column, &filter.table, tables[0])?;
                let condition = self.build_filter_condition(&column, &filter.operator, &filter.value)?;
                conditions.push(condition);
            }
        }
        
        // Handle date constraint
        if let Some(ref date_constraint) = intent.date_constraint {
            let date_col = if let Some(ref col) = date_constraint.column {
                self.resolve_column(col, &None, tables[0])?
            } else if let Some(ref time_col) = tables[0].time_column {
                format!("{}.{}", tables[0].name, time_col)
            } else {
                return Err(RcaError::Execution("No date column specified and table has no time_column".to_string()));
            };
            
            let date_condition = match &date_constraint.value {
                DateValue::Single(date) => format!("{} = '{}'", date_col, date),
                DateValue::Range { start, end } => format!("{} >= '{}' AND {} <= '{}'", date_col, start, date_col, end),
                DateValue::Relative(rel) => {
                    match rel.as_str() {
                        "end_of_year" => format!("{} = (SELECT MAX({}) FROM {})", date_col, date_col, tables[0].name),
                        "start_of_year" => format!("{} = (SELECT MIN({}) FROM {})", date_col, date_col, tables[0].name),
                        _ => format!("{} = CURRENT_DATE", date_col), // Default to today
                    }
                }
            };
            conditions.push(date_condition);
        }
        
        if conditions.is_empty() {
            return Ok(String::new());
        }
        
        Ok(format!("WHERE {}", conditions.join(" AND ")))
    }
    
    /// Build filter condition
    fn build_filter_condition(&self, column: &str, operator: &str, value: &serde_json::Value) -> Result<String> {
        let op = operator.to_uppercase();
        match op.as_str() {
            "=" | "!=" | ">" | "<" | ">=" | "<=" => {
                let val_str = self.format_value(value)?;
                // For string equality, make case-insensitive
                if op == "=" && value.is_string() {
                    Ok(format!("UPPER({}) = UPPER({})", column, val_str))
                } else {
                    Ok(format!("{} {} {}", column, op, val_str))
                }
            }
            "IN" => {
                if let Some(arr) = value.as_array() {
                    let vals: Vec<String> = arr.iter()
                        .map(|v| self.format_value(v))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(format!("{} IN ({})", column, vals.join(", ")))
                } else {
                    Err(RcaError::Execution("IN operator requires an array value".to_string()))
                }
            }
            "LIKE" => {
                let val_str = self.format_value(value)?;
                // Make LIKE case-insensitive by default
                Ok(format!("UPPER({}) LIKE UPPER({})", column, val_str))
            }
            "IS NULL" => Ok(format!("{} IS NULL", column)),
            "IS NOT NULL" => Ok(format!("{} IS NOT NULL", column)),
            _ => Err(RcaError::Execution(format!("Unknown operator: {}", operator))),
        }
    }
    
    /// Format value for SQL
    fn format_value(&self, value: &serde_json::Value) -> Result<String> {
        match value {
            serde_json::Value::String(s) => Ok(format!("'{}'", s.replace("'", "''"))),
            serde_json::Value::Number(n) => Ok(n.to_string()),
            serde_json::Value::Bool(b) => Ok(b.to_string().to_uppercase()),
            serde_json::Value::Null => Ok("NULL".to_string()),
            _ => Err(RcaError::Execution(format!("Unsupported value type: {:?}", value))),
        }
    }
    
    /// Build JOIN clauses
    fn build_join_clauses(&self, intent: &SqlIntent, tables: &[&crate::metadata::Table]) -> Result<Vec<String>> {
        let mut join_clauses = Vec::new();
        
        if let Some(ref joins) = intent.joins {
            for join in joins {
                let join_type = join.join_type.as_deref().unwrap_or("LEFT").to_uppercase();
                let conditions: Vec<String> = join.condition.iter()
                    .map(|c| format!("{}.{} = {}.{}", 
                        join.left_table, c.left_column,
                        join.right_table, c.right_column))
                    .collect();
                join_clauses.push(format!("{} JOIN {} ON {}", 
                    join_type, join.right_table, conditions.join(" AND ")));
            }
        }
        
        Ok(join_clauses)
    }
    
    /// Build GROUP BY clause
    fn build_group_by_clause(&self, intent: &SqlIntent, tables: &[&crate::metadata::Table]) -> Result<String> {
        if let Some(ref group_by) = intent.group_by {
            if group_by.is_empty() {
                return Ok(String::new());
            }
            let columns: Vec<String> = group_by.iter()
                .map(|col| self.resolve_column(col, &None, tables[0]))
                .collect::<Result<Vec<_>>>()?;
            if columns.is_empty() {
                Ok(String::new())
            } else {
                Ok(format!("GROUP BY {}", columns.join(", ")))
            }
        } else {
            Ok(String::new())
        }
    }
    
    /// Build ORDER BY clause
    fn build_order_by_clause(&self, intent: &SqlIntent, tables: &[&crate::metadata::Table]) -> Result<String> {
        if let Some(ref order_by) = intent.order_by {
            if order_by.is_empty() {
                return Ok(String::new());
            }
            let parts: Vec<String> = order_by.iter()
                .map(|spec| {
                    let col = self.resolve_column(&spec.column, &spec.table, tables[0])?;
                    let dir = spec.direction.as_deref().unwrap_or("ASC").to_uppercase();
                    Ok(format!("{} {}", col, dir))
                })
                .collect::<Result<Vec<_>>>()?;
            if parts.is_empty() {
                Ok(String::new())
            } else {
                // Check if we have GROUP BY - if so, ORDER BY must use aggregated columns or GROUP BY columns
                let has_group_by = intent.group_by.is_some() && 
                    intent.group_by.as_ref().map(|g| !g.is_empty()).unwrap_or(false);
                
                if has_group_by {
                    // For GROUP BY queries, ORDER BY should use aggregated columns or GROUP BY columns
                    // This is a simplified check - in production, you'd want more sophisticated validation
                    let order_by_cols: Vec<String> = order_by.iter()
                        .map(|s| s.column.clone())
                        .collect();
                    let group_by_cols: Vec<String> = intent.group_by.as_ref()
                        .map(|g| g.clone())
                        .unwrap_or_default();
                    
                    // Check if ORDER BY columns are in GROUP BY or are aggregations
                    let valid_order_by: Vec<String> = parts.iter()
                        .filter(|part| {
                            // Check if it's a GROUP BY column or an aggregation alias
                            let col_name = part.split_whitespace().next().unwrap_or("");
                            group_by_cols.iter().any(|g| col_name.contains(g)) ||
                            col_name.contains("SUM") || col_name.contains("AVG") || 
                            col_name.contains("COUNT") || col_name.contains("MAX") || 
                            col_name.contains("MIN")
                        })
                        .cloned()
                        .collect();
                    
                    if valid_order_by.is_empty() {
                        // Skip ORDER BY if it's invalid for GROUP BY
                        return Ok(String::new());
                    }
                    Ok(format!("ORDER BY {}", valid_order_by.join(", ")))
                } else {
                    Ok(format!("ORDER BY {}", parts.join(", ")))
                }
            }
        } else {
            Ok(String::new())
        }
    }
}

