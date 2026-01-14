//! Tests for Phase 2 (RcaCursor Core) and Phase 3 (Grain-Level Diff & Attribution)
//! 
//! Tests the execution engine, grain diff engine, attribution engine, and confidence model.

use rca_engine::core::agent::rca_cursor::{
    executor::{ExecutionResult, ExecutionMetadata},
    diff::GrainDiffEngine,
    attribution::GrainAttributionEngine,
    confidence::{ConfidenceModel, ConfidenceFactors},
};
use polars::prelude::*;
use std::time::Duration;

// Note: We'll test the components directly without needing full metadata setup
// The execution engine tests would require actual metadata, so we'll focus on
// testing the diff, attribution, and confidence components which work with
// ExecutionResult structures.

/// Create a test execution result
fn create_test_execution_result(
    grain_key: &str,
    metric_column: &str,
    data: Vec<(String, f64)>,
) -> ExecutionResult {
    // Create DataFrame with grain_key and metric_column
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
fn test_grain_diff_engine_basic() {
    // Create test execution results
    let result_a = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 1000.0),
            ("loan_2".to_string(), 2000.0),
            ("loan_3".to_string(), 3000.0),
        ],
    );

    let result_b = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 1100.0), // +100 difference
            ("loan_2".to_string(), 2000.0), // No difference
            ("loan_4".to_string(), 4000.0), // Missing in A
        ],
    );

    // Create diff engine
    let diff_engine = GrainDiffEngine::new(10);

    // Compute diff
    let diff_result = diff_engine.compute_diff(&result_a, &result_b, "total_amount").unwrap();

    // Verify results
    assert_eq!(diff_result.grain_key, "loan_id");
    assert_eq!(diff_result.total_grain_units_a, 3);
    assert_eq!(diff_result.total_grain_units_b, 3);
    assert_eq!(diff_result.missing_left_count, 1); // loan_4 only in B
    assert_eq!(diff_result.missing_right_count, 1); // loan_3 only in A
    assert_eq!(diff_result.mismatch_count, 1); // loan_1 has different values

    // Check that differences are sorted by impact
    assert!(!diff_result.differences.is_empty());
    let first_diff = &diff_result.differences[0];
    assert_eq!(first_diff.grain_value[0], "loan_4"); // Highest impact (4000 - 0 = 4000)
    assert_eq!(first_diff.impact, 4000.0);
}

#[test]
fn test_grain_diff_engine_top_k() {
    // Create test execution results with many differences
    let mut data_a = Vec::new();
    let mut data_b = Vec::new();

    for i in 1..=20 {
        data_a.push((format!("loan_{}", i), i as f64 * 100.0));
        data_b.push((format!("loan_{}", i), (i as f64 + 10.0) * 100.0));
    }

    let result_a = create_test_execution_result("loan_id", "total_amount", data_a);
    let result_b = create_test_execution_result("loan_id", "total_amount", data_b);

    // Create diff engine with top_k = 5
    let diff_engine = GrainDiffEngine::new(5);

    // Compute diff
    let diff_result = diff_engine.compute_diff(&result_a, &result_b, "total_amount").unwrap();

    // Verify top K
    assert_eq!(diff_result.differences.len(), 5);
    assert_eq!(diff_result.top_k, 5);
}

#[test]
fn test_grain_attribution_engine() {
    // Create test execution results
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

    // Create diff result
    let diff_engine = GrainDiffEngine::new(10);
    let diff_result = diff_engine.compute_diff(&result_a, &result_b, "total_amount").unwrap();

    // Create attribution engine
    let attribution_engine = GrainAttributionEngine::new(10);

    // Compute attributions
    let attributions = attribution_engine
        .compute_attributions(&diff_result, &result_a, &result_b, "total_amount")
        .unwrap();

    // Verify attributions
    assert!(!attributions.is_empty());
    let attribution = &attributions[0];
    assert_eq!(attribution.grain_value[0], "loan_1");
    assert_eq!(attribution.impact, 100.0); // |1100 - 1000|
    assert!(!attribution.contributors.is_empty());
}

#[test]
fn test_confidence_model_high_confidence() {
    let model = ConfidenceModel::new();

    let factors = ConfidenceFactors {
        join_completeness: 1.0,
        null_rate: 0.0,
        filter_coverage: 1.0,
        data_freshness: 1.0,
        sampling_ratio: 1.0,
    };

    let confidence = model.compute_confidence(&factors);
    assert!(confidence > 0.9, "Expected high confidence, got {}", confidence);
}

#[test]
fn test_confidence_model_low_confidence() {
    let model = ConfidenceModel::new();

    let factors = ConfidenceFactors {
        join_completeness: 0.5,
        null_rate: 0.5,
        filter_coverage: 0.5,
        data_freshness: 0.0,
        sampling_ratio: 0.1,
    };

    let confidence = model.compute_confidence(&factors);
    assert!(confidence < 0.5, "Expected low confidence, got {}", confidence);
}

#[test]
fn test_confidence_model_from_metadata() {
    let model = ConfidenceModel::new();

    let metadata_a = ExecutionMetadata {
        execution_time: Duration::from_secs(1),
        rows_scanned: 1000,
        memory_mb: 50.0,
        nodes_executed: 5,
        filter_selectivity: Some(0.8),
        join_selectivity: Some(0.9),
    };

    let metadata_b = ExecutionMetadata {
        execution_time: Duration::from_secs(1),
        rows_scanned: 1000,
        memory_mb: 50.0,
        nodes_executed: 5,
        filter_selectivity: Some(0.85),
        join_selectivity: Some(0.95),
    };

    let confidence = model
        .compute_from_metadata(&metadata_a, &metadata_b, 2, None)
        .unwrap();

    // Should have reasonable confidence
    assert!(confidence > 0.0 && confidence <= 1.0);
}

#[test]
fn test_grain_diff_with_missing_values() {
    // Test case where system A has values but system B doesn't
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
            ("loan_1".to_string(), 1000.0),
            // loan_2 missing in B
        ],
    );

    let diff_engine = GrainDiffEngine::new(10);
    let diff_result = diff_engine.compute_diff(&result_a, &result_b, "total_amount").unwrap();

    // Should detect missing_right_count
    assert_eq!(diff_result.missing_right_count, 1);
}

#[test]
fn test_grain_diff_impact_sorting() {
    // Create differences with varying impacts
    let result_a = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 100.0),  // Small difference
            ("loan_2".to_string(), 200.0),  // Medium difference
            ("loan_3".to_string(), 300.0),  // Large difference
        ],
    );

    let result_b = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 110.0),  // +10 impact
            ("loan_2".to_string(), 250.0),  // +50 impact
            ("loan_3".to_string(), 400.0),  // +100 impact
        ],
    );

    let diff_engine = GrainDiffEngine::new(10);
    let diff_result = diff_engine.compute_diff(&result_a, &result_b, "total_amount").unwrap();

    // Verify sorting by impact (descending)
    assert_eq!(diff_result.differences[0].impact, 100.0); // loan_3
    assert_eq!(diff_result.differences[1].impact, 50.0);  // loan_2
    assert_eq!(diff_result.differences[2].impact, 10.0);   // loan_1
}

#[tokio::test]
async fn test_execution_result_grain_normalization() {
    // Test that execution result maintains grain normalization
    let result = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 1000.0),
            ("loan_2".to_string(), 2000.0),
            ("loan_3".to_string(), 3000.0),
        ],
    );

    // Verify grain normalization: one row per grain_key
    let df = &result.dataframe;
    let grain_key_col = df.column("loan_id").unwrap();
    
    // Check uniqueness
    let unique_count = grain_key_col.unique().unwrap().len();
    assert_eq!(unique_count, df.height(), "Grain normalization failed: duplicate grain keys");
}

#[test]
fn test_attribution_contribution_percentage() {
    // Create test data with known differences
    let result_a = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 100.0),
            ("loan_2".to_string(), 200.0),
        ],
    );

    let result_b = create_test_execution_result(
        "loan_id",
        "total_amount",
        vec![
            ("loan_1".to_string(), 150.0), // +50 impact
            ("loan_2".to_string(), 250.0), // +50 impact
        ],
    );

    let diff_engine = GrainDiffEngine::new(10);
    let diff_result = diff_engine.compute_diff(&result_a, &result_b, "total_amount").unwrap();

    let attribution_engine = GrainAttributionEngine::new(10);
    let attributions = attribution_engine
        .compute_attributions(&diff_result, &result_a, &result_b, "total_amount")
        .unwrap();

    // Both should have equal contribution percentage (50% each)
    assert_eq!(attributions.len(), 2);
    for attr in &attributions {
        // Each should contribute 50% (50 / 100 total impact)
        assert!((attr.contribution_percentage - 50.0).abs() < 1.0);
    }
}

