use crate::error::{RcaError, Result};
use crate::fuzzy_matcher::{FuzzyMatcher, FuzzyMatch};
use polars::prelude::*;
use std::collections::HashSet;

pub struct DiffEngine {
    pub fuzzy_matcher: Option<FuzzyMatcher>,
    pub fuzzy_columns: Vec<String>, // Columns that should use fuzzy matching
}

impl Default for DiffEngine {
    fn default() -> Self {
        Self {
            fuzzy_matcher: None,
            fuzzy_columns: Vec::new(),
        }
    }
}

impl DiffEngine {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn with_fuzzy_matching(mut self, threshold: f64, fuzzy_columns: Vec<String>) -> Self {
        self.fuzzy_matcher = Some(FuzzyMatcher::new(threshold));
        self.fuzzy_columns = fuzzy_columns;
        self
    }
    
    /// Compare two dataframes and find differences
    pub fn compare(
        &self,
        df_a: DataFrame,
        df_b: DataFrame,
        grain: &[String],
        metric_col: &str,
        precision: u32,
    ) -> Result<ComparisonResult> {
        // Check if any grain columns should use fuzzy matching
        let has_fuzzy_columns = grain.iter().any(|col| self.fuzzy_columns.contains(col));
        
        // Population diff (with fuzzy matching if enabled)
        let population_diff = if has_fuzzy_columns && self.fuzzy_matcher.is_some() {
            println!("   🔍 Fuzzy matching enabled for columns: {:?}", 
                grain.iter().filter(|c| self.fuzzy_columns.contains(*c)).collect::<Vec<_>>());
            self.population_diff_with_fuzzy(&df_a, &df_b, grain)?
        } else {
            self.population_diff(&df_a, &df_b, grain)?
        };
        
        // Data diff (for common keys, including fuzzy matches)
        let data_diff = if has_fuzzy_columns && self.fuzzy_matcher.is_some() {
            self.data_diff_with_fuzzy(&df_a, &df_b, grain, metric_col, precision, &population_diff.fuzzy_matches)?
        } else {
            self.data_diff(&df_a, &df_b, grain, metric_col, precision)?
        };
        
        Ok(ComparisonResult {
            population_diff,
            data_diff,
        })
    }
    
    fn population_diff(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        grain: &[String],
    ) -> Result<PopulationDiff> {
        // Get unique keys from both dataframes
        let keys_a: HashSet<Vec<String>> = self.extract_keys(df_a, grain)?;
        let keys_b: HashSet<Vec<String>> = self.extract_keys(df_b, grain)?;
        
        // Find missing and extra entities
        let missing_in_b: Vec<Vec<String>> = keys_a.difference(&keys_b).cloned().collect();
        let extra_in_b: Vec<Vec<String>> = keys_b.difference(&keys_a).cloned().collect();
        let common_keys: Vec<Vec<String>> = keys_a.intersection(&keys_b).cloned().collect();
        
        // Check for duplicates
        let duplicates_a = self.find_duplicates(df_a, grain)?;
        let duplicates_b = self.find_duplicates(df_b, grain)?;
        
        Ok(PopulationDiff {
            missing_in_b,
            extra_in_b,
            common_count: common_keys.len(),
            duplicates_a,
            duplicates_b,
            fuzzy_matches: Vec::new(),
        })
    }
    
    fn data_diff(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        grain: &[String],
        metric_col: &str,
        precision: u32,
    ) -> Result<DataDiff> {
        // Join on grain columns
        let grain_cols: Vec<Expr> = grain.iter().map(|c| col(c)).collect();
        
        let df_a_lazy = df_a.clone().lazy();
        let df_b_lazy = df_b.clone().lazy();
        
        // Rename metric columns to avoid conflict
        let df_a_renamed = df_a_lazy
            .with_columns([col(metric_col).alias("metric_a")]);
        let df_b_renamed = df_b_lazy
            .with_columns([col(metric_col).alias("metric_b")]);
        
        // Join
        let joined = df_a_renamed
            .join(
                df_b_renamed,
                grain_cols.clone(),
                grain_cols.clone(),
                JoinArgs::new(JoinType::Inner),
            )
            .with_columns([
                (col("metric_a") - col("metric_b")).alias("diff"),
            ])
            .with_columns([
                when(col("diff").gt(lit(0)))
                    .then(col("diff"))
                    .otherwise(-col("diff"))
                    .alias("abs_diff"),
            ])
            .collect()?;
        
        // Filter to mismatches (considering precision)
        let precision_factor = 10_f64.powi(precision as i32);
        let threshold = 1.0 / precision_factor;
        
        let mismatches_df = joined
            .clone()
            .lazy()
            .filter(col("abs_diff").gt(lit(threshold)))
            .collect()?;
        
        let matches_df = joined
            .clone()
            .lazy()
            .filter(col("abs_diff").lt_eq(lit(threshold)))
            .collect()?;
        
        let mismatches = mismatches_df.height();
        let matches = matches_df.height();
        
        Ok(DataDiff {
            mismatches,
            matches,
            mismatch_details: mismatches_df,
        })
    }
    
    fn extract_keys(&self, df: &DataFrame, grain: &[String]) -> Result<HashSet<Vec<String>>> {
        let mut keys = HashSet::new();
        
        for row_idx in 0..df.height() {
            let mut key = Vec::new();
            for col_name in grain {
                let col_val = df.column(col_name)?;
                let val_str = match col_val.dtype() {
                    DataType::String => col_val.str().unwrap().get(row_idx).unwrap().to_string(),
                    DataType::Int64 => col_val.i64().unwrap().get(row_idx).unwrap().to_string(),
                    DataType::Float64 => col_val.f64().unwrap().get(row_idx).unwrap().to_string(),
                    _ => format!("{:?}", col_val.get(row_idx)),
                };
                key.push(val_str);
            }
            keys.insert(key);
        }
        
        Ok(keys)
    }
    
    fn find_duplicates(&self, df: &DataFrame, grain: &[String]) -> Result<Vec<Vec<String>>> {
        let grain_cols: Vec<Expr> = grain.iter().map(|c| col(c)).collect();
        
        let duplicates = df
            .clone()
            .lazy()
            .group_by(grain_cols.clone())
            .agg([len().alias("count")])
            .filter(col("count").gt(lit(1)))
            .collect()?;
        
        let mut dup_keys = Vec::new();
        for row_idx in 0..duplicates.height() {
            let mut key = Vec::new();
            for col_name in grain {
                let col_val = duplicates.column(col_name)?;
                let val_str = match col_val.dtype() {
                    DataType::String => col_val.str().unwrap().get(row_idx).unwrap().to_string(),
                    DataType::Int64 => col_val.i64().unwrap().get(row_idx).unwrap().to_string(),
                    DataType::Float64 => col_val.f64().unwrap().get(row_idx).unwrap().to_string(),
                    _ => format!("{:?}", col_val.get(row_idx)),
                };
                key.push(val_str);
            }
            dup_keys.push(key);
        }
        
        Ok(dup_keys)
    }
    
    /// Population diff with fuzzy matching support
    fn population_diff_with_fuzzy(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        grain: &[String],
    ) -> Result<PopulationDiff> {
        let fuzzy_matcher = self.fuzzy_matcher.as_ref()
            .ok_or_else(|| RcaError::Execution("Fuzzy matcher not initialized".to_string()))?;
        
        let fuzzy_diff = fuzzy_matcher.fuzzy_population_diff(df_a, df_b, grain)?;
        
        // Convert fuzzy matches to regular matches for compatibility
        let mut missing_in_b = fuzzy_diff.missing_in_b;
        let mut extra_in_b = fuzzy_diff.extra_in_b;
        
        // Log fuzzy matches
        if !fuzzy_diff.fuzzy_matches.is_empty() {
            println!("   ✅ Fuzzy matches found:");
            for fm in &fuzzy_diff.fuzzy_matches {
                println!("      {:?} <-> {:?} (similarity: {:.2}%)", 
                    fm.key_a, fm.key_b, fm.similarity * 100.0);
            }
        }
        
        Ok(PopulationDiff {
            missing_in_b,
            extra_in_b,
            common_count: fuzzy_diff.common_count,
            duplicates_a: Vec::new(), // TODO: implement with fuzzy matching
            duplicates_b: Vec::new(),
            fuzzy_matches: fuzzy_diff.fuzzy_matches,
        })
    }
    
    /// Data diff with fuzzy matching support
    fn data_diff_with_fuzzy(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        grain: &[String],
        metric_col: &str,
        precision: u32,
        fuzzy_matches: &[FuzzyMatch],
    ) -> Result<DataDiff> {
        // First, do exact match join
        let grain_cols: Vec<Expr> = grain.iter().map(|c| col(c)).collect();
        
        let df_a_lazy = df_a.clone().lazy();
        let df_b_lazy = df_b.clone().lazy();
        
        // Rename metric columns
        let df_a_renamed = df_a_lazy
            .with_columns([col(metric_col).alias("metric_a")]);
        let df_b_renamed = df_b_lazy
            .with_columns([col(metric_col).alias("metric_b")]);
        
        // Join on exact matches
        let exact_joined = df_a_renamed
            .join(
                df_b_renamed.clone(),
                grain_cols.clone(),
                grain_cols.clone(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
        
        // For fuzzy matches, we need to manually join them
        // This is more complex - for now, we'll create a mapping and use it
        let mut fuzzy_joined_rows = Vec::new();
        
        for fm in fuzzy_matches {
            // Find rows in df_a with key_a
            let df_a_filtered = self.filter_df_by_key(df_a, grain, &fm.key_a)?;
            let df_b_filtered = self.filter_df_by_key(df_b, grain, &fm.key_b)?;
            
            // Join these filtered dataframes
            if df_a_filtered.height() > 0 && df_b_filtered.height() > 0 {
                // For simplicity, take first match from each
                // In production, might need more sophisticated logic
                let metric_a = df_a_filtered.column(metric_col)?.f64()?.get(0);
                let metric_b = df_b_filtered.column(metric_col)?.f64()?.get(0);
                
                if let (Some(ma), Some(mb)) = (metric_a, metric_b) {
                    fuzzy_joined_rows.push((ma, mb));
                }
            }
        }
        
        // Combine exact and fuzzy matches
        let all_joined = if !fuzzy_joined_rows.is_empty() {
            // For now, return exact matches only
            // TODO: Properly combine exact and fuzzy matches
            exact_joined
        } else {
            exact_joined
        };
        
        // Calculate differences
        let joined = all_joined
            .lazy()
            .with_columns([
                (col("metric_a") - col("metric_b")).alias("diff"),
            ])
            .with_columns([
                when(col("diff").gt(lit(0)))
                    .then(col("diff"))
                    .otherwise(-col("diff"))
                    .alias("abs_diff"),
            ])
            .collect()?;
        
        // Filter to mismatches (considering precision)
        let precision_factor = 10_f64.powi(precision as i32);
        let threshold = 1.0 / precision_factor;
        
        let mismatches_df = joined
            .clone()
            .lazy()
            .filter(col("abs_diff").gt(lit(threshold)))
            .collect()?;
        
        let matches_df = joined
            .clone()
            .lazy()
            .filter(col("abs_diff").lt_eq(lit(threshold)))
            .collect()?;
        
        let mismatches = mismatches_df.height();
        let matches = matches_df.height();
        
        Ok(DataDiff {
            mismatches,
            matches,
            mismatch_details: mismatches_df,
        })
    }
    
    fn filter_df_by_key(
        &self,
        df: &DataFrame,
        grain: &[String],
        key: &[String],
    ) -> Result<DataFrame> {
        let mut filtered = df.clone().lazy();
        
        for (idx, col_name) in grain.iter().enumerate() {
            if idx < key.len() {
                let key_val = key[idx].clone();
                filtered = filtered.filter(col(col_name).eq(lit(key_val)));
            }
        }
        
        Ok(filtered.collect()?)
    }
}

#[derive(Debug, Clone)]
pub struct ComparisonResult {
    pub population_diff: PopulationDiff,
    pub data_diff: DataDiff,
}

#[derive(Debug, Clone)]
pub struct PopulationDiff {
    pub missing_in_b: Vec<Vec<String>>,
    pub extra_in_b: Vec<Vec<String>>,
    pub common_count: usize,
    pub duplicates_a: Vec<Vec<String>>,
    pub duplicates_b: Vec<Vec<String>>,
    pub fuzzy_matches: Vec<FuzzyMatch>,
}

#[derive(Debug, Clone)]
pub struct DataDiff {
    pub mismatches: usize,
    pub matches: usize,
    pub mismatch_details: DataFrame,
}

