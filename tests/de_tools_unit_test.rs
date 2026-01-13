/// Unit tests for Data Engineering tools
/// These tests verify DE tools work correctly in isolation

use rca_engine::data_engineering::*;
use polars::prelude::*;

#[test]
fn test_inspect_columns() {
    // Create test dataframe
    let df = df! [
        "customer_id" => ["C001", "C002", "C003", "C004", "C005"],
        "amount" => [1000.50, 2000.00, 3000.75, 4000.00, 5000.25],
        "status" => ["active", "active", "inactive", "active", "active"],
    ].unwrap();
    
    let result = inspect_columns(&df, &["customer_id".to_string(), "amount".to_string()], 5).unwrap();
    
    assert_eq!(result.columns.len(), 2);
    assert!(result.columns.contains_key("customer_id"));
    assert!(result.columns.contains_key("amount"));
    
    let customer_inspection = result.columns.get("customer_id").unwrap();
    assert_eq!(customer_inspection.total_count, 5);
    assert_eq!(customer_inspection.null_count, 0);
}

#[test]
fn test_validate_schema_compatibility() {
    // Create two dataframes with compatible schemas
    let df_a = df! [
        "loan_id" => ["L001", "L002"],
        "amount" => [1000.0, 2000.0],
    ].unwrap();
    
    let df_b = df! [
        "loan_id" => ["L001", "L002"],
        "amount" => [1000.0, 2000.0],
    ].unwrap();
    
    let mut join_columns = std::collections::HashMap::new();
    join_columns.insert("loan_id".to_string(), "loan_id".to_string());
    
    let result = validate_schema_compatibility(&df_a, &df_b, &join_columns).unwrap();
    
    assert!(result.compatible);
    assert!(result.issues.is_empty());
}

#[test]
fn test_validate_schema_type_mismatch() {
    // Create two dataframes with type mismatch
    let df_a = df! [
        "loan_id" => ["L001", "L002"], // String
        "amount" => [1000.0, 2000.0],
    ].unwrap();
    
    let df_b = df! [
        "loan_id" => [1, 2], // Integer - type mismatch!
        "amount" => [1000.0, 2000.0],
    ].unwrap();
    
    let mut join_columns = std::collections::HashMap::new();
    join_columns.insert("loan_id".to_string(), "loan_id".to_string());
    
    let result = validate_schema_compatibility(&df_a, &df_b, &join_columns).unwrap();
    
    // Should detect type mismatch but still be compatible (can cast)
    assert!(result.compatible); // Types can be cast
    assert!(!result.issues.is_empty()); // But issues should be reported
}

#[test]
fn test_detect_anomalies_nulls() {
    // Create dataframe with nulls
    let df = df! [
        "customer_id" => [Some("C001"), Some("C002"), None, Some("C004"), Some("C005")],
        "amount" => [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],
    ].unwrap();
    
    let result = detect_anomalies(&df, Some(&["customer_id".to_string()]), &["nulls".to_string()]).unwrap();
    
    assert!(!result.anomalies.is_empty());
    assert!(result.anomalies.iter().any(|a| a.check_type == "nulls" && a.column == "customer_id"));
}

#[test]
fn test_detect_anomalies_duplicates() {
    // Create dataframe with duplicates
    let df = df! [
        "transaction_id" => ["T001", "T002", "T003", "T003", "T004"], // T003 is duplicate
        "amount" => [100.0, 200.0, 300.0, 300.0, 400.0],
    ].unwrap();
    
    let result = detect_anomalies(&df, Some(&["transaction_id".to_string()]), &["duplicates".to_string()]).unwrap();
    
    assert!(!result.anomalies.is_empty());
    assert!(result.anomalies.iter().any(|a| a.check_type == "duplicates" && a.column == "transaction_id"));
}

#[test]
fn test_validate_join_keys() {
    // Create two dataframes for join validation
    let df_a = df! [
        "loan_id" => ["L001", "L002", "L003"],
        "amount" => [1000.0, 2000.0, 3000.0],
    ].unwrap();
    
    let df_b = df! [
        "loan_id" => ["L001", "L002", "L004"], // L003 missing, L004 extra
        "tos" => [1000.0, 2000.0, 4000.0],
    ].unwrap();
    
    let mut join_keys = std::collections::HashMap::new();
    join_keys.insert("loan_id".to_string(), "loan_id".to_string());
    
    let result = validate_join_keys(&df_a, &df_b, &join_keys, "inner").unwrap();
    
    // Should detect overlap issues
    assert!(result.can_join); // Can still join, but with warnings
    assert!(!result.issues.is_empty()); // Should have info about overlap
}

#[test]
fn test_validate_join_keys_no_overlap() {
    // Create two dataframes with no overlapping keys (inner join will fail)
    let df_a = df! [
        "loan_id" => ["L001", "L002"],
        "amount" => [1000.0, 2000.0],
    ].unwrap();
    
    let df_b = df! [
        "loan_id" => ["L003", "L004"], // No overlap!
        "tos" => [3000.0, 4000.0],
    ].unwrap();
    
    let mut join_keys = std::collections::HashMap::new();
    join_keys.insert("loan_id".to_string(), "loan_id".to_string());
    
    let result = validate_join_keys(&df_a, &df_b, &join_keys, "inner").unwrap();
    
    // Inner join with no overlap should fail
    assert!(!result.can_join);
    assert!(result.issues.iter().any(|i| i.message.contains("No overlapping values")));
}

#[test]
fn test_cast_types() {
    // Create dataframe with string numbers
    let df = df! [
        "loan_id" => ["1", "2", "3"], // String
        "amount" => ["1000.50", "2000.00", "3000.75"], // String
    ].unwrap();
    
    let mut type_mapping = std::collections::HashMap::new();
    type_mapping.insert("loan_id".to_string(), "int64".to_string());
    type_mapping.insert("amount".to_string(), "float64".to_string());
    
    let result = cast_dataframe_types(df, &type_mapping).unwrap();
    
    // Verify types were cast
    assert_eq!(result.column("loan_id").unwrap().dtype(), &DataType::Int64);
    assert_eq!(result.column("amount").unwrap().dtype(), &DataType::Float64);
}


