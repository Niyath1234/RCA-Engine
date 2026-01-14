//! Simple tests for Phase 2 and Phase 3 components
//! 
//! Tests the core functionality without requiring full metadata setup.

use rca_engine::core::agent::rca_cursor::{
    executor::{ExecutionResult, ExecutionMetadata},
    diff::GrainDiffEngine,
    attribution::GrainAttributionEngine,
    confidence::{ConfidenceModel, ConfidenceFactors},
};
use polars::prelude::*;
use std::time::Duration;

/// Create a test execution result with simple data
fn create_test_execution_result(
    grain_key: &str,
    metric_column: &str,
    data: Vec<(String, f64)>,
) -> ExecutionResult {
    let mut grain_values = Vec::new();
    let mut metric_values = Vec::new();

    for (grain_val, metric_val) in data {
        grain_values.push(grain_val);
        metric_values.push(metric_val);
    }

    let df = DataFrame::new(vec![
        Series::new(grain_key, grain_values),
        Series::new(metric_column, metric_values),
    ]).unwrap();

    ExecutionResult {
        schema: df.schema().clone(),
        row_count: df.height(),
        dataframe: df,
        grain_key: grain_key.to_string(),
        metadata: ExecutionMetadata {
            execution_time: Duration::from_secs(1),
            rows_scanned: 100,
            memory_mb: 10.0,
            nodes_executed: 3,
            filter_selectivity: Some(0.8),
            join_selectivity: Some(0.9),
        },
    }
}

#[test]
fn test_grain_diff_basic() {
    let result_a = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 1000.0),
            ("loan_2".to_string(), 2000.0),
        ],
    );

    let result_b = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 1100.0),
            ("loan_2".to_string(), 2000.0),
        ],
    );

    let diff_engine = GrainDiffEngine::new(10);
    let diff_result = diff_engine.compute_diff(&result_a, &result_b, "total_amount").unwrap();

    assert_eq!(diff_result.grain_key, "loan_id");
    assert_eq!(diff_result.total_grain_units_a, 2);
    assert_eq!(diff_result.total_grain_units_b, 2);
    assert_eq!(diff_result.mismatch_count, 1);
    
    // Check that differences are sorted by impact
    assert!(!diff_result.differences.is_empty());
    let first_diff = &diff_result.differences[0];
    assert_eq!(first_diff.grain_value[0], "loan_1");
    assert_eq!(first_diff.impact, 100.0);
}

#[test]
fn test_confidence_model() {
    let model = ConfidenceModel::new();

    let factors = ConfidenceFactors {
        join_completeness: 1.0,
        null_rate: 0.0,
        filter_coverage: 1.0,
        data_freshness: 1.0,
        sampling_ratio: 1.0,
    };

    let confidence = model.compute_confidence(&factors);
    assert!(confidence > 0.9);
}

#[test]
fn test_attribution_basic() {
    let result_a = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![("loan_1".to_string(), 1000.0)],
    );

    let result_b = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![("loan_1".to_string(), 1100.0)],
    );

    let diff_engine = GrainDiffEngine::new(10);
    let diff_result = diff_engine.compute_diff(&result_a, &result_b, "total_amount").unwrap();

    let attribution_engine = GrainAttributionEngine::new(10);
    let attributions = attribution_engine
        .compute_attributions(&diff_result, &result_a, &result_b, "total_amount")
        .unwrap();

    assert!(!attributions.is_empty());
    assert_eq!(attributions[0].grain_value[0], "loan_1");
}

