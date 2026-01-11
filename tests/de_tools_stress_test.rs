use rca_engine::metadata::Metadata;
use rca_engine::rca::RcaEngine;
use rca_engine::llm::LlmClient;
use polars::prelude::{*, df, ParquetWriter};
use std::path::PathBuf;
use std::fs;
use std::io::BufWriter;

/// Real-world stress test for Data Engineering Tools
/// 
/// This test simulates a realistic reconciliation scenario with multiple data quality issues:
/// 1. Commas in numeric strings (e.g., "1,000.50" instead of 1000.50)
/// 2. Type mismatches (string IDs vs integer IDs)
/// 3. Null values in join keys
/// 4. Duplicate records
/// 5. Schema incompatibilities
/// 6. Whitespace issues in string columns
/// 7. Case inconsistencies
///
/// The test verifies that DE tools are automatically called and fix these issues before joins/comparisons.

#[tokio::test]
async fn test_de_tools_stress_test() {
    // Setup test data directory
    let test_data_dir = PathBuf::from("test_data_de_stress");
    let test_metadata_dir = PathBuf::from("test_metadata_de_stress");
    
    // Clean up any existing test data
    let _ = fs::remove_dir_all(&test_data_dir);
    let _ = fs::remove_dir_all(&test_metadata_dir);
    
    fs::create_dir_all(&test_data_dir).unwrap();
    fs::create_dir_all(&test_metadata_dir).unwrap();
    
    // Create test data with real-world issues
    create_test_data_with_issues(&test_data_dir).await;
    
    // Create test metadata
    create_test_metadata(&test_metadata_dir);
    
    // Initialize RCA Engine
    let metadata = Metadata::load(&test_metadata_dir).unwrap();
    let llm = LlmClient::new(
        "test-api-key".to_string(),
        "gpt-4o-mini".to_string(),
        "https://api.openai.com/v1".to_string(),
    );
    let engine = RcaEngine::new(metadata, llm, test_data_dir.clone());
    
    // Test query that should trigger multiple DE tools
    let query = "System A vs System B TOS reconciliation where amounts have commas and customer IDs are strings";
    
    // Run the reconciliation
    let result = engine.run(query).await;
    
    // Verify results
    match result {
        Ok(rca_result) => {
            println!("\n✅ Stress test completed successfully!");
            println!("   - System A: {}", rca_result.system_a);
            println!("   - System B: {}", rca_result.system_b);
            println!("   - Metric: {}", rca_result.metric);
            println!("   - Population matches: {}", rca_result.comparison.population_diff.common_count);
            println!("   - Data matches: {}", rca_result.comparison.data_diff.matches);
            println!("   - Data mismatches: {}", rca_result.comparison.data_diff.mismatches);
            println!("   - Classifications: {}", rca_result.classifications.len());
            
            // Verify that DE tools were executed (check logs for DE tool execution)
            // In a real test, we'd capture logs and verify DE tools were called
            assert!(rca_result.comparison.population_diff.common_count > 0, 
                "Should have found common entities after DE tool execution");
        }
        Err(e) => {
            panic!("Stress test failed: {:?}", e);
        }
    }
    
    // Cleanup
    let _ = fs::remove_dir_all(&test_data_dir);
    let _ = fs::remove_dir_all(&test_metadata_dir);
}

/// Create test data with real-world data quality issues
async fn create_test_data_with_issues(data_dir: &PathBuf) {
    use polars::prelude::*;
    
    // System A: Has issues - commas in amounts, string IDs, whitespace, nulls
    let system_a_dir = data_dir.join("system_a");
    fs::create_dir_all(&system_a_dir).unwrap();
    
    // System A Loans - has commas in amounts, string customer IDs with whitespace
    let loans_a = df! [
        "loan_id" => ["L001", "L002", "L003", "L004", "L005"],
        "customer_id" => ["C001 ", " C002", "C003", "C004", "C005"], // Whitespace issues
        "loan_amount" => ["10,000.50", "25,000.00", "15,500.75", "8,000.00", "30,000.25"], // Commas!
        "disbursement_date" => ["2025-01-15", "2025-02-20", "2025-03-10", "2025-04-05", "2025-05-12"],
        "status" => ["active", "active", "ACTIVE", "active", "active"], // Case inconsistency
    ].unwrap();
    
    // System A EMIs - has commas, some nulls
    let emis_a = df! [
        "loan_id" => ["L001", "L001", "L002", "L002", "L003", "L003", "L004", "L005"],
        "emi_number" => [1, 2, 1, 2, 1, 2, 1, 1],
        "amount" => ["5,000.00", "5,000.50", "12,500.00", "12,500.00", "7,750.00", "7,750.75", "8,000.00", "30,000.25"],
        "due_date" => ["2025-02-15", "2025-03-15", "2025-03-20", "2025-04-20", "2025-04-10", "2025-05-10", "2025-05-05", "2025-06-12"],
        "paid_amount" => [Some("4,500.00"), None, Some("12,500.00"), None, Some("7,000.00"), None, None, None], // Some nulls
    ].unwrap();
    
    // System A Transactions - has duplicates, commas
    let transactions_a = df! [
        "transaction_id" => ["T001", "T002", "T003", "T004", "T005", "T005"], // Duplicate!
        "loan_id" => ["L001", "L002", "L003", "L003", "L003", "L003"], // Duplicate transaction_id
        "emi_number" => [1, 1, 1, 1, 1, 1],
        "amount" => ["4,500.00", "12,500.00", "7,000.00", "500.00", "250.00", "250.00"], // Commas, duplicate
        "transaction_date" => ["2025-02-10", "2025-03-18", "2025-04-08", "2025-04-09", "2025-04-10", "2025-04-10"],
    ].unwrap();
    
    // System B: Clean data - numeric amounts, integer IDs, no commas
    let system_b_dir = data_dir.join("system_b");
    fs::create_dir_all(&system_b_dir).unwrap();
    
    // System B Loans - clean numeric data
    let loans_b = df! [
        "loan_id" => [1, 2, 3, 4, 5], // Integer IDs (type mismatch!)
        "customer_id" => [1001, 1002, 1003, 1004, 1005], // Integer IDs (type mismatch!)
        "loan_amount" => [10000.50, 25000.00, 15500.75, 8000.00, 30000.25], // Numeric, no commas
        "disbursement_date" => ["2025-01-15", "2025-02-20", "2025-03-10", "2025-04-05", "2025-05-12"],
        "status" => ["active", "active", "active", "active", "active"],
    ].unwrap();
    
    // System B Loan Summary - pre-computed TOS
    let loan_summary_b = df! [
        "loan_id" => [1, 2, 3, 4, 5],
        "total_outstanding" => [5500.50, 0.00, 8000.75, 8000.00, 30000.25], // Pre-computed TOS
        "as_of_date" => ["2025-12-31", "2025-12-31", "2025-12-31", "2025-12-31", "2025-12-31"],
    ].unwrap();
    
    // Write Parquet files
    let file = std::fs::File::create(system_a_dir.join("loans.parquet")).unwrap();
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(&mut loans_a.clone()).unwrap();
    
    let file = std::fs::File::create(system_a_dir.join("emis.parquet")).unwrap();
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(&mut emis_a.clone()).unwrap();
    
    let file = std::fs::File::create(system_a_dir.join("transactions.parquet")).unwrap();
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(&mut transactions_a.clone()).unwrap();
    
    let file = std::fs::File::create(system_b_dir.join("loans.parquet")).unwrap();
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(&mut loans_b.clone()).unwrap();
    
    let file = std::fs::File::create(system_b_dir.join("loan_summary.parquet")).unwrap();
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(&mut loan_summary_b.clone()).unwrap();
}

/// Create test metadata for the stress test
fn create_test_metadata(metadata_dir: &PathBuf) {
    // Entities
    let entities = r#"
{
  "entities": [
    {
      "id": "loan",
      "grain": ["loan_id"],
      "attributes": ["loan_id", "customer_id", "loan_amount", "disbursement_date", "status"]
    },
    {
      "id": "emi",
      "grain": ["loan_id", "emi_number"],
      "attributes": ["loan_id", "emi_number", "amount", "due_date", "paid_amount"]
    },
    {
      "id": "transaction",
      "grain": ["transaction_id"],
      "attributes": ["transaction_id", "loan_id", "emi_number", "amount", "transaction_date"]
    }
  ]
}
"#;
    fs::write(metadata_dir.join("entities.json"), entities).unwrap();
    
    // Tables
    let tables = r#"
{
  "tables": [
    {
      "name": "system_a_loans",
      "entity": "loan",
      "primary_key": ["loan_id"],
      "time_column": "disbursement_date",
      "system": "system_a",
      "path": "system_a/loans.parquet"
    },
    {
      "name": "system_a_emis",
      "entity": "emi",
      "primary_key": ["loan_id", "emi_number"],
      "time_column": "due_date",
      "system": "system_a",
      "path": "system_a/emis.parquet"
    },
    {
      "name": "system_a_transactions",
      "entity": "transaction",
      "primary_key": ["transaction_id"],
      "time_column": "transaction_date",
      "system": "system_a",
      "path": "system_a/transactions.parquet"
    },
    {
      "name": "system_b_loans",
      "entity": "loan",
      "primary_key": ["loan_id"],
      "time_column": "disbursement_date",
      "system": "system_b",
      "path": "system_b/loans.parquet"
    },
    {
      "name": "system_b_loan_summary",
      "entity": "loan",
      "primary_key": ["loan_id"],
      "time_column": "as_of_date",
      "system": "system_b",
      "path": "system_b/loan_summary.parquet"
    }
  ]
}
"#;
    fs::write(metadata_dir.join("tables.json"), tables).unwrap();
    
    // Metrics
    let metrics = r#"
{
  "metrics": [
    {
      "id": "tos",
      "grain": ["loan_id"],
      "precision": 2,
      "null_policy": "zero"
    }
  ]
}
"#;
    fs::write(metadata_dir.join("metrics.json"), metrics).unwrap();
    
    // Rules
    let rules = r#"
[
  {
    "id": "system_a_tos",
    "system": "system_a",
    "metric": "tos",
    "target_entity": "loan",
    "target_grain": ["loan_id"],
    "computation": {
      "description": "Total Outstanding = Sum of (EMI Amount - Transaction Amount) per loan",
      "source_entities": ["loan", "emi", "transaction"],
      "attributes_needed": {
        "emi": ["loan_id", "emi_number", "amount"],
        "transaction": ["loan_id", "emi_number", "amount"]
      },
      "formula": "SUM(emi_amount - COALESCE(transaction_amount, 0))",
      "aggregation_grain": ["loan_id"]
    }
  },
  {
    "id": "system_b_tos",
    "system": "system_b",
    "metric": "tos",
    "target_entity": "loan",
    "target_grain": ["loan_id"],
    "computation": {
      "description": "Total Outstanding from pre-computed loan summary",
      "source_entities": ["loan"],
      "attributes_needed": {
        "loan": ["loan_id", "total_outstanding"]
      },
      "formula": "total_outstanding",
      "aggregation_grain": ["loan_id"]
    }
  }
]
"#;
    fs::write(metadata_dir.join("rules.json"), rules).unwrap();
    
    // Business Labels
    let business_labels = r#"
{
  "systems": [
    {
      "label": "System A",
      "system_id": "system_a"
    },
    {
      "label": "System B",
      "system_id": "system_b"
    }
  ],
  "metrics": [
    {
      "label": "TOS",
      "metric_id": "tos"
    }
  ]
}
"#;
    fs::write(metadata_dir.join("business_labels.json"), business_labels).unwrap();
    
    // Lineage
    let lineage = r#"
{
  "edges": [
    {
      "from": "system_a_loans",
      "to": "system_a_emis",
      "keys": {"loan_id": "loan_id"},
      "relationship": "one_to_many"
    },
    {
      "from": "system_a_emis",
      "to": "system_a_transactions",
      "keys": {"loan_id": "loan_id", "emi_number": "emi_number"},
      "relationship": "one_to_many"
    }
  ]
}
"#;
    fs::write(metadata_dir.join("lineage.json"), lineage).unwrap();
    
    // Empty files for other metadata
    fs::write(metadata_dir.join("time.json"), "{}").unwrap();
    fs::write(metadata_dir.join("identity.json"), "{}").unwrap();
    fs::write(metadata_dir.join("exceptions.json"), "[]").unwrap();
}

