use crate::error::{RcaError, Result};
use crate::metadata::{Metadata, Rule, PipelineOp, Table};
use crate::operators::RelationalEngine;
use crate::time::TimeResolver;
use polars::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;

pub struct RuleCompiler {
    metadata: Metadata,
    engine: RelationalEngine,
    time_resolver: TimeResolver,
}

impl RuleCompiler {
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self {
            metadata: metadata.clone(),
            engine: RelationalEngine::new(data_dir),
            time_resolver: TimeResolver::new(metadata),
        }
    }
    
    /// Compile a rule into an execution plan by automatically constructing pipeline
    /// from rule specification + metadata
    pub fn compile(&self, rule_id: &str) -> Result<ExecutionPlan> {
        let rule = self.metadata
            .get_rule(rule_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", rule_id)))?;
        
        // Automatically construct pipeline from rule specification
        let steps = self.construct_pipeline(rule)?;
        
        Ok(ExecutionPlan {
            rule_id: rule_id.to_string(),
            rule: rule.clone(),
            steps,
        })
    }
    
    /// Automatically construct pipeline from rule's computation definition
    fn construct_pipeline(&self, rule: &Rule) -> Result<Vec<PipelineOp>> {
        let mut steps = Vec::new();
        
        // Step 1: Map source entities to tables for this system
        let entity_to_tables: HashMap<String, Vec<&Table>> = rule.computation.source_entities
            .iter()
            .map(|entity| {
                let tables: Vec<&Table> = self.metadata.tables
                    .iter()
                    .filter(|t| t.entity == *entity && t.system == rule.system)
                    .collect();
                (entity.clone(), tables)
            })
            .collect();
        
        // Check that all entities have at least one table
        for entity in &rule.computation.source_entities {
            if entity_to_tables.get(entity).map_or(true, |t| t.is_empty()) {
                return Err(RcaError::Execution(format!(
                    "No table found for entity '{}' in system '{}'",
                    entity, rule.system
                )));
            }
        }
        
        // Step 2: Determine root table (usually the target entity's table)
        // Prefer table that has the required columns from the formula
        let root_entity = &rule.target_entity;
        let root_tables = entity_to_tables.get(root_entity)
            .ok_or_else(|| RcaError::Execution(format!("No tables for root entity: {}", root_entity)))?;
        
        // If formula is a direct column reference, prefer table that has that column
        let root_table = if !rule.computation.formula.contains("SUM(") && 
                             !rule.computation.formula.contains("AVG(") &&
                             !rule.computation.formula.contains("COUNT(") {
            // Direct column reference - find table that likely has this column
            let formula_col = rule.computation.formula.split_whitespace().next().unwrap_or("");
            root_tables.iter()
                .find(|t| {
                    // Prefer tables with names that suggest they contain summary/precomputed data
                    // if the column name suggests it's a summary metric
                    (formula_col.contains("total") || formula_col.contains("summary") || formula_col.contains("outstanding")) &&
                    (t.name.contains("summary") || t.name.contains("total") || t.name.contains("outstanding") || 
                     t.name.contains("metrics") || t.name.contains("details"))
                })
                .or_else(|| root_tables.first())
                .ok_or_else(|| RcaError::Execution(format!("No root table found for entity: {}", root_entity)))?
        } else {
            root_tables.first()
                .ok_or_else(|| RcaError::Execution(format!("No root table found for entity: {}", root_entity)))?
        };
        
        // Step 3: Build join plan - find shortest paths from root to all other entity tables
        let mut visited_tables = HashSet::new();
        visited_tables.insert(root_table.name.clone());
        
        // Start with root table scan
        steps.push(PipelineOp::Scan { table: root_table.name.clone() });
        
        // For each other entity, find join path and add joins
        for entity in &rule.computation.source_entities {
            if *entity == *root_entity {
                continue;
            }
            
            let entity_tables = entity_to_tables.get(entity)
                .ok_or_else(|| RcaError::Execution(format!("No tables for entity: {}", entity)))?;
            
            for entity_table in entity_tables {
                if visited_tables.contains(&entity_table.name) {
                    continue;
                }
                
                // Find join path from root to this entity table
                // BFS will find path through any intermediate nodes
                let join_path = self.find_join_path_to_table(&root_table.name, &entity_table.name, &visited_tables)?;
                
                for join_step in join_path {
                    if !visited_tables.contains(&join_step.to) {
                        // Determine join type from lineage relationship
                        let join_type = self.determine_join_type(&join_step.from, &join_step.to)?;
                        let join_keys: Vec<String> = join_step.keys.keys().cloned().collect();
                        
                        // Note: Aggregation will be handled inline during join execution
                        // if the table grain is higher than target grain
                        steps.push(PipelineOp::Join {
                            table: join_step.to.clone(),
                            on: join_keys,
                            join_type,
                        });
                        
                        visited_tables.insert(join_step.to.clone());
                    }
                }
            }
        }
        
        // Step 4: Parse formula to determine if we need derive + aggregate or just select
        // If formula contains SUM/AVG/etc, it means: derive intermediate, then aggregate
        // If formula is just a column name, just select that column (with optional group by)
        
        let formula_upper = rule.computation.formula.to_uppercase();
        let has_aggregation = formula_upper.contains("SUM(") || formula_upper.contains("AVG(") || 
                             formula_upper.contains("COUNT(") || formula_upper.contains("MAX(") || 
                             formula_upper.contains("MIN(");
        
        if has_aggregation {
            // Formula like "SUM(emi_amount - COALESCE(transaction_amount, 0))"
            // Step 4a: Derive intermediate column first
            // Extract inner expression by finding the first '(' and removing the last ')'
            let agg_func_start = formula_upper.find('(').unwrap_or(0);
            let mut inner_expr = rule.computation.formula[agg_func_start+1..].to_string();
            // Remove trailing ')' if present
            if inner_expr.ends_with(')') {
                inner_expr.pop();
            }
            
            let intermediate_col = "computed_value".to_string(); // Temporary column
            steps.push(PipelineOp::Derive {
                expr: inner_expr.clone(),
                r#as: intermediate_col.clone(),
            });
            
            // Step 4b: Group and aggregate
            let mut agg_map = HashMap::new();
            if formula_upper.starts_with("SUM") {
                agg_map.insert(rule.metric.clone(), format!("SUM({})", intermediate_col));
            } else if formula_upper.starts_with("AVG") {
                agg_map.insert(rule.metric.clone(), format!("AVG({})", intermediate_col));
            } else if formula_upper.starts_with("COUNT") {
                agg_map.insert(rule.metric.clone(), format!("COUNT({})", intermediate_col));
            } else {
                // Default to SUM
                agg_map.insert(rule.metric.clone(), format!("SUM({})", intermediate_col));
            }
            
            steps.push(PipelineOp::Group {
                by: rule.computation.aggregation_grain.clone(),
                agg: agg_map,
            });
        } else {
            // Formula is a direct column reference like "total_outstanding"
            // If we need aggregation grain, group by it, otherwise just rename in select
            if !rule.computation.aggregation_grain.is_empty() && 
               rule.computation.aggregation_grain != rule.target_grain {
                // Need to group by aggregation grain
                let mut agg_map = HashMap::new();
                agg_map.insert(rule.metric.clone(), rule.computation.formula.clone());
                steps.push(PipelineOp::Group {
                    by: rule.computation.aggregation_grain.clone(),
                    agg: agg_map,
                });
            }
            // If no special aggregation needed, we'll rename the column in the select step
        }
        
        // Step 6: Select final columns (grain + metric)
        let mut final_columns = rule.target_grain.clone();
        // For direct column formulas, alias the column to the metric name
        if !has_aggregation {
            final_columns.push(format!("{} as {}", rule.computation.formula, rule.metric));
        } else {
            final_columns.push(rule.metric.clone());
        }
        steps.push(PipelineOp::Select { columns: final_columns });
        
        Ok(steps)
    }
    
    /// Find join path from a source table to a target table using lineage
    /// Returns the shortest path through lineage edges (can include intermediate nodes)
    fn find_join_path_to_table(
        &self,
        from: &str,
        to: &str,
        visited: &HashSet<String>,
    ) -> Result<Vec<JoinPathStep>> {
        // BFS to find shortest path - intermediate nodes are allowed
        let mut queue = VecDeque::new();
        queue.push_back((from.to_string(), vec![]));
        let mut seen = HashSet::new();
        seen.insert(from.to_string());
        // Don't add visited to seen - allow traversal through already-visited nodes
        // We just need to find a path, not avoid visited nodes
        
        while let Some((current, path)) = queue.pop_front() {
            if current == to {
                return Ok(path);
            }
            
            // Check all lineage edges from current node
            for edge in &self.metadata.lineage.edges {
                // Forward direction
                if edge.from == current && !seen.contains(&edge.to) {
                    let mut new_path = path.clone();
                    new_path.push(JoinPathStep {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                        keys: edge.keys.clone(),
                    });
                    
                    if edge.to == to {
                        return Ok(new_path);
                    }
                    
                    seen.insert(edge.to.clone());
                    queue.push_back((edge.to.clone(), new_path));
                }
                
                // Reverse direction (if bidirectional joins are supported)
                if edge.to == current && !seen.contains(&edge.from) {
                    // Create reverse edge keys
                    let mut reverse_keys = HashMap::new();
                    for (k, v) in &edge.keys {
                        reverse_keys.insert(v.clone(), k.clone());
                    }
                    
                    let mut new_path = path.clone();
                    new_path.push(JoinPathStep {
                        from: edge.to.clone(),
                        to: edge.from.clone(),
                        keys: reverse_keys,
                    });
                    
                    if edge.from == to {
                        return Ok(new_path);
                    }
                    
                    seen.insert(edge.from.clone());
                    queue.push_back((edge.from.clone(), new_path));
                }
            }
        }
        
        Err(RcaError::Execution(format!(
            "No join path found from {} to {} (checked {} edges)",
            from, to, self.metadata.lineage.edges.len()
        )))
    }
    
    /// Determine join type from lineage relationship
    fn determine_join_type(&self, from: &str, to: &str) -> Result<String> {
        // Check lineage edges for relationship type
        for edge in &self.metadata.lineage.edges {
            if edge.from == from && edge.to == to {
                match edge.relationship.as_str() {
                    "one_to_many" | "one_to_one" => return Ok("left".to_string()),
                    "many_to_one" => return Ok("inner".to_string()),
                    "many_to_many" => return Ok("inner".to_string()),
                    _ => return Ok("left".to_string()), // Default to left join
                }
            }
            // Check reverse
            if edge.from == to && edge.to == from {
                match edge.relationship.as_str() {
                    "one_to_many" | "many_to_one" => return Ok("inner".to_string()),
                    "one_to_one" => return Ok("left".to_string()),
                    "many_to_many" => return Ok("inner".to_string()),
                    _ => return Ok("left".to_string()),
                }
            }
        }
        
        // Default to left join if relationship not specified
        Ok("left".to_string())
    }
    
    /// Check if a table needs to be aggregated before joining
    /// Returns true if the table's grain (primary_key) is significantly higher (more granular) than the target grain
    /// We only aggregate tables that are at a much higher grain (like date-level) to avoid join explosions
    /// Tables that are close to target grain (like loan_id + emi_number) should be joined first, then aggregated
    fn table_needs_aggregation(&self, table: &Table, target_grain: &[String]) -> bool {
        // Use primary_key as proxy for grain (grain is often same as primary_key)
        let table_grain = &table.primary_key;
        
        // If table grain has significantly more elements than target grain (3+ more), it needs aggregation
        // This catches date-level tables like loan_id + date + type
        if table_grain.len() >= target_grain.len() + 2 {
            return true;
        }
        
        // If table grain has 1-2 more elements, check if the extra columns are date-related
        // Date-related tables should be aggregated before joining to avoid explosion
        if table_grain.len() > target_grain.len() {
            let extra_cols: Vec<_> = table_grain.iter()
                .filter(|col| !target_grain.contains(col))
                .collect();
            
            // If extra columns include date-related columns, aggregate
            for col in &extra_cols {
                if col.contains("date") || col.contains("Date") || col.contains("_date") {
                    return true;
                }
            }
        }
        
        // For tables close to target grain (like loan_id + emi_number), don't aggregate before joining
        // They'll be joined first, then aggregated together in the final step
        false
    }
    
    /// Get aggregation columns for a table when aggregating to target grain
    /// Sums all numeric columns, skips non-numeric columns that aren't in target grain
    fn get_aggregation_columns(&self, table: &Table, target_grain: &[String]) -> HashMap<String, String> {
        let mut agg_map = HashMap::new();
        
        // For each column in the table, determine aggregation
        if let Some(columns) = &table.columns {
            for col in columns {
                // Skip grain columns (they're in the GROUP BY)
                if target_grain.contains(&col.name) {
                    continue;
                }
                
                // Determine aggregation based on column type
                // Use data_type if available, otherwise default to string
                let col_type = col.data_type.as_deref().unwrap_or("string");
                match col_type {
                    "float" | "integer" | "numeric" | "double" => {
                        // Sum numeric columns
                        agg_map.insert(col.name.clone(), format!("SUM({})", col.name));
                    }
                    _ => {
                        // Skip non-numeric columns that aren't in target grain
                        // They won't be needed for the final aggregation
                    }
                }
            }
        }
        
        agg_map
    }
}

#[derive(Debug, Clone)]
struct JoinPathStep {
    from: String,
    to: String,
    keys: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub rule_id: String,
    pub rule: Rule,
    pub steps: Vec<crate::metadata::PipelineOp>,
}

pub struct RuleExecutor {
    compiler: RuleCompiler,
}

impl RuleExecutor {
    pub fn new(compiler: RuleCompiler) -> Self {
        Self { compiler }
    }
    
    /// Execute a rule and return the result dataframe
    pub async fn execute(
        &self,
        rule_id: &str,
        as_of_date: Option<chrono::NaiveDate>,
    ) -> Result<DataFrame> {
        let plan = self.compiler.compile(rule_id)?;
        
        let mut result: Option<DataFrame> = None;
        let mut current_table: Option<String> = None;
        
        for (step_idx, step) in plan.steps.iter().enumerate() {
            // Apply time filtering for scan operations
            if let crate::metadata::PipelineOp::Scan { table } = step {
                // Use metadata to get correct table path
                let mut df = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
                
                // Apply as-of filtering
                if let Some(date) = as_of_date {
                    df = self.compiler.time_resolver.apply_as_of(df, table, Some(date))?;
                }
                
                result = Some(df);
                current_table = Some(table.clone());
                continue;
            }
            
            // Execute operation - for joins, we also need to use metadata for table paths
            if let crate::metadata::PipelineOp::Join { table, on, join_type } = step {
                // Check if this table needs aggregation before joining
                let target_table = self.compiler.metadata.tables.iter()
                    .find(|t| t.name == *table)
                    .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table)))?;
                
                let rule = &plan.rule;
                let needs_aggregation = self.compiler.table_needs_aggregation(target_table, &rule.computation.aggregation_grain);
                
                let mut right = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
                
                // Apply as-of filtering if needed
                if let Some(date) = as_of_date {
                    right = self.compiler.time_resolver.apply_as_of(right, table, Some(date))?;
                }
                
                // Aggregate if needed before joining
                // Include join keys in GROUP BY to preserve them for the join
                if needs_aggregation {
                    // Combine target grain and join keys for GROUP BY
                    let mut group_by_cols = rule.computation.aggregation_grain.clone();
                    for join_key in on {
                        if !group_by_cols.contains(join_key) {
                            group_by_cols.push(join_key.clone());
                        }
                    }
                    
                    // Check if GROUP BY matches the table's original grain (primary_key)
                    // If so, no aggregation is needed - the table is already at this grain
                    let table_grain = &target_table.primary_key;
                    let group_by_matches_grain = group_by_cols.len() == table_grain.len() &&
                        group_by_cols.iter().all(|col| table_grain.contains(col)) &&
                        table_grain.iter().all(|col| group_by_cols.contains(col));
                    
                    if !group_by_matches_grain {
                        let agg_columns = self.compiler.get_aggregation_columns(target_table, &group_by_cols);
                        // Only aggregate if we have columns to aggregate
                        if !agg_columns.is_empty() {
                            right = self.compiler.engine.execute_op(
                                &crate::metadata::PipelineOp::Group {
                                    by: group_by_cols,
                                    agg: agg_columns,
                                },
                                Some(right),
                                None,
                            ).await?;
                        }
                    }
                    // If GROUP BY matches original grain, no aggregation needed - use table as-is
                }
                
                let left = result.unwrap();
                result = Some(
                    self.compiler.engine.join(left, right, on, join_type).await?
                );
                continue;
            }
            
            // For other operations
            result = Some(
                self.compiler.engine.execute_op(step, result, None).await?
            );
        }
        
        result.ok_or_else(|| RcaError::Execution("No result from rule execution".to_string()))
    }
    
    /// Execute with step-by-step tracking for drilldown
    pub async fn execute_with_steps(
        &self,
        rule_id: &str,
        as_of_date: Option<chrono::NaiveDate>,
    ) -> Result<Vec<ExecutionStep>> {
        let plan = self.compiler.compile(rule_id)?;
        
        let mut steps = Vec::new();
        let mut result: Option<DataFrame> = None;
        let mut current_table: Option<String> = None;
        
        for (step_idx, step) in plan.steps.iter().enumerate() {
            let step_name = format!("step_{}", step_idx);
            
            if let crate::metadata::PipelineOp::Scan { table } = step {
                let mut df = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
                
                if let Some(date) = as_of_date {
                    df = self.compiler.time_resolver.apply_as_of(df, table, Some(date))?;
                }
                
                steps.push(ExecutionStep {
                    step_name: step_name.clone(),
                    operation: format!("{:?}", step),
                    row_count: df.height(),
                    columns: df.get_column_names().iter().map(|s| s.to_string()).collect(),
                    data: Some(df.clone()),
                });
                
                result = Some(df);
                current_table = Some(table.clone());
                continue;
            }
            
            // Handle join separately to use metadata
            if let crate::metadata::PipelineOp::Join { table, on, join_type } = step {
                // Check if this table needs aggregation before joining
                let target_table = self.compiler.metadata.tables.iter()
                    .find(|t| t.name == *table)
                    .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table)))?;
                
                let rule = &plan.rule;
                let needs_aggregation = self.compiler.table_needs_aggregation(target_table, &rule.computation.aggregation_grain);
                
                let mut right = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
                
                // Apply as-of filtering if needed
                if let Some(date) = as_of_date {
                    right = self.compiler.time_resolver.apply_as_of(right, table, Some(date))?;
                }
                
                // Aggregate if needed before joining
                // Include join keys in GROUP BY to preserve them for the join
                if needs_aggregation {
                    // Combine target grain and join keys for GROUP BY
                    let mut group_by_cols = rule.computation.aggregation_grain.clone();
                    for join_key in on {
                        if !group_by_cols.contains(join_key) {
                            group_by_cols.push(join_key.clone());
                        }
                    }
                    
                    // Check if GROUP BY matches the table's original grain (primary_key)
                    // If so, no aggregation is needed - the table is already at this grain
                    let table_grain = &target_table.primary_key;
                    let group_by_matches_grain = group_by_cols.len() == table_grain.len() &&
                        group_by_cols.iter().all(|col| table_grain.contains(col)) &&
                        table_grain.iter().all(|col| group_by_cols.contains(col));
                    
                    if !group_by_matches_grain {
                        let agg_columns = self.compiler.get_aggregation_columns(target_table, &group_by_cols);
                        // Only aggregate if we have columns to aggregate
                        if !agg_columns.is_empty() {
                            right = self.compiler.engine.execute_op(
                                &crate::metadata::PipelineOp::Group {
                                    by: group_by_cols,
                                    agg: agg_columns,
                                },
                                Some(right),
                                None,
                            ).await?;
                        }
                    }
                    // If GROUP BY matches original grain, no aggregation needed - use table as-is
                }
                
                let left = result.unwrap();
                let df = self.compiler.engine.join(left, right, on, join_type).await?;
                
                steps.push(ExecutionStep {
                    step_name: step_name.clone(),
                    operation: format!("{:?}", step),
                    row_count: df.height(),
                    columns: df.get_column_names().iter().map(|s| s.to_string()).collect(),
                    data: Some(df.clone()),
                });
                
                result = Some(df);
                continue;
            }
            
            // For other operations
            let df = self.compiler.engine.execute_op(step, result.clone(), None).await?;
            
            steps.push(ExecutionStep {
                step_name: step_name.clone(),
                operation: format!("{:?}", step),
                row_count: df.height(),
                columns: df.get_column_names().iter().map(|s| s.to_string()).collect(),
                data: Some(df.clone()),
            });
            
            result = Some(df);
        }
        
        Ok(steps)
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionStep {
    pub step_name: String,
    pub operation: String,
    pub row_count: usize,
    pub columns: Vec<String>,
    pub data: Option<DataFrame>,
}

