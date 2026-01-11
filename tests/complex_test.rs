use polars::prelude::*;
use rca_engine::metadata::Metadata;
use rca_engine::rca::RcaEngine;
use rca_engine::llm::LlmClient;
use std::path::PathBuf;
use std::fs;
use dotenv;

/// Create complex test data files with 10 tables (only 6 will participate)
fn create_complex_test_data(data_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Create directories
    fs::create_dir_all(data_dir.join("system_a"))?;
    fs::create_dir_all(data_dir.join("system_b"))?;

    println!("📊 Creating test data for 10 tables (6 will participate in computation)...\n");

    // 1. system_a_customers (NOT PARTICIPATING - will be excluded)
    let customers_df = df! [
        "customer_id" => ["C001", "C002", "C003"],
        "name" => ["Alice", "Bob", "Charlie"],
        "created_date" => ["2025-01-01", "2025-01-02", "2025-01-03"]
    ]?;
    let customers_path = data_dir.join("system_a/customers.parquet");
    let mut file = std::fs::File::create(&customers_path)?;
    ParquetWriter::new(&mut file).finish(&mut customers_df.clone())?;
    println!("   ✓ Created system_a_customers.parquet (NOT PARTICIPATING)");

    // 2. system_a_loans (PARTICIPATING - root table)
    let loans_df = df! [
        "loan_id" => ["L001", "L002", "L003"],
        "customer_id" => ["C001", "C002", "C003"],
        "disbursement_date" => ["2025-01-15", "2025-02-20", "2025-03-10"],
        "principal_amount" => [100000.0, 50000.0, 75000.0]
    ]?;
    let loans_path = data_dir.join("system_a/loans.parquet");
    let mut file = std::fs::File::create(&loans_path)?;
    ParquetWriter::new(&mut file).finish(&mut loans_df.clone())?;
    println!("   ✓ Created system_a_loans.parquet (PARTICIPATING - ROOT)");

    // 3. system_a_loan_details (NOT PARTICIPATING - will be excluded)
    let loan_details_df = df! [
        "loan_id" => ["L001", "L001", "L002"],
        "detail_id" => ["D001", "D002", "D003"],
        "update_date" => ["2025-01-16", "2025-01-17", "2025-02-21"],
        "detail_value" => [100.0, 200.0, 150.0]
    ]?;
    let loan_details_path = data_dir.join("system_a/loan_details.parquet");
    let mut file = std::fs::File::create(&loan_details_path)?;
    ParquetWriter::new(&mut file).finish(&mut loan_details_df.clone())?;
    println!("   ✓ Created system_a_loan_details.parquet (NOT PARTICIPATING)");

    // 4. system_a_emis (PARTICIPATING)
    let emis_df = df! [
        "loan_id" => ["L001", "L001", "L002", "L003"],
        "emi_number" => [1, 2, 1, 1],
        "due_date" => ["2025-02-15", "2025-03-15", "2025-03-20", "2025-04-10"],
        "emi_amount" => [5000.0, 5000.0, 3000.0, 4000.0]
    ]?;
    let emis_path = data_dir.join("system_a/emis.parquet");
    let mut file = std::fs::File::create(&emis_path)?;
    ParquetWriter::new(&mut file).finish(&mut emis_df.clone())?;
    println!("   ✓ Created system_a_emis.parquet (PARTICIPATING)");

    // 5. system_a_transactions (PARTICIPATING)
    let transactions_df = df! [
        "transaction_id" => ["T001", "T002", "T003", "T004"],
        "loan_id" => ["L001", "L001", "L002", "L003"],
        "emi_number" => [1, 2, 1, 1],
        "transaction_date" => ["2025-02-10", "2025-03-12", "2025-03-18", "2025-04-08"],
        "transaction_amount" => [4500.0, 4800.0, 3000.0, 3500.0]
    ]?;
    let transactions_path = data_dir.join("system_a/transactions.parquet");
    let mut file = std::fs::File::create(&transactions_path)?;
    ParquetWriter::new(&mut file).finish(&mut transactions_df.clone())?;
    println!("   ✓ Created system_a_transactions.parquet (PARTICIPATING)");

    // 6. system_a_payments (NOT PARTICIPATING - will be excluded)
    let payments_df = df! [
        "payment_id" => ["P001", "P002"],
        "transaction_id" => ["T001", "T002"],
        "payment_amount" => [4500.0, 4800.0],
        "payment_date" => ["2025-02-10", "2025-03-12"]
    ]?;
    let payments_path = data_dir.join("system_a/payments.parquet");
    let mut file = std::fs::File::create(&payments_path)?;
    ParquetWriter::new(&mut file).finish(&mut payments_df.clone())?;
    println!("   ✓ Created system_a_payments.parquet (NOT PARTICIPATING)");

    // 7. system_a_penalties (PARTICIPATING)
    let penalties_df = df! [
        "penalty_id" => ["PEN001", "PEN002"],
        "loan_id" => ["L001", "L002"],
        "penalty_amount" => [100.0, 50.0],
        "penalty_date" => ["2025-02-20", "2025-03-25"]
    ]?;
    let penalties_path = data_dir.join("system_a/penalties.parquet");
    let mut file = std::fs::File::create(&penalties_path)?;
    ParquetWriter::new(&mut file).finish(&mut penalties_df.clone())?;
    println!("   ✓ Created system_a_penalties.parquet (PARTICIPATING)");

    // 8. system_a_charges (PARTICIPATING)
    let charges_df = df! [
        "charge_id" => ["CHG001", "CHG002"],
        "loan_id" => ["L001", "L003"],
        "charge_amount" => [25.0, 30.0],
        "charge_date" => ["2025-02-18", "2025-04-05"]
    ]?;
    let charges_path = data_dir.join("system_a/charges.parquet");
    let mut file = std::fs::File::create(&charges_path)?;
    ParquetWriter::new(&mut file).finish(&mut charges_df.clone())?;
    println!("   ✓ Created system_a_charges.parquet (PARTICIPATING)");

    // 9. system_a_refunds (PARTICIPATING)
    let refunds_df = df! [
        "refund_id" => ["REF001"],
        "payment_id" => ["P001"],
        "refund_amount" => [50.0],
        "refund_date" => ["2025-02-15"]
    ]?;
    let refunds_path = data_dir.join("system_a/refunds.parquet");
    let mut file = std::fs::File::create(&refunds_path)?;
    ParquetWriter::new(&mut file).finish(&mut refunds_df.clone())?;
    println!("   ✓ Created system_a_refunds.parquet (PARTICIPATING)");

    // 10. system_a_adjustments (PARTICIPATING)
    let adjustments_df = df! [
        "adjustment_id" => ["ADJ001", "ADJ002"],
        "loan_id" => ["L001", "L002"],
        "adjustment_amount" => [75.0, 40.0],
        "adjustment_date" => ["2025-02-25", "2025-03-30"]
    ]?;
    let adjustments_path = data_dir.join("system_a/adjustments.parquet");
    let mut file = std::fs::File::create(&adjustments_path)?;
    ParquetWriter::new(&mut file).finish(&mut adjustments_df.clone())?;
    println!("   ✓ Created system_a_adjustments.parquet (PARTICIPATING)");

    // System B tables
    // 11. system_b_loans (PARTICIPATING)
    let system_b_loans_df = df! [
        "loan_id" => ["L001", "L002", "L003"],
        "customer_id" => ["C001", "C002", "C003"],
        "disbursement_date" => ["2025-01-15", "2025-02-20", "2025-03-10"],
        "principal_amount" => [100000.0, 50000.0, 75000.0]
    ]?;
    let system_b_loans_path = data_dir.join("system_b/loans.parquet");
    let mut file = std::fs::File::create(&system_b_loans_path)?;
    ParquetWriter::new(&mut file).finish(&mut system_b_loans_df.clone())?;
    println!("   ✓ Created system_b_loans.parquet (PARTICIPATING)");

    // 12. system_b_loan_summary (PARTICIPATING)
    // Expected TOS calculations:
    // L001: 5000 + 5000 - 4500 - 4800 + 100 - 25 - 50 + 75 = 1700
    // L002: 3000 - 3000 + 50 - 0 - 0 + 40 = 90
    // L003: 4000 - 3500 + 0 - 30 - 0 + 0 = 470
    // But System B has different values to create mismatches
    let system_b_summary_df = df! [
        "loan_id" => ["L001", "L002", "L003"],
        "as_of_date" => ["2025-12-31", "2025-12-31", "2025-12-31"],
        "total_outstanding" => [1800.0, 90.0, 500.0]  // Different from calculated values
    ]?;
    let system_b_summary_path = data_dir.join("system_b/loan_summary.parquet");
    let mut file = std::fs::File::create(&system_b_summary_path)?;
    ParquetWriter::new(&mut file).finish(&mut system_b_summary_df.clone())?;
    println!("   ✓ Created system_b_loan_summary.parquet (PARTICIPATING)");

    println!("\n✅ All test data files created successfully!");
    println!("   Total tables: 12");
    println!("   Participating in computation: 6 (system_a_loans, system_a_emis, system_a_transactions, system_a_penalties, system_a_charges, system_a_adjustments, system_b_loans, system_b_loan_summary)");
    println!("   Non-participating: 4 (system_a_customers, system_a_loan_details, system_a_payments, system_a_refunds)");

    Ok(())
}

#[tokio::test]
async fn test_complex_reconciliation_with_chain_of_thought() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(80));
    println!("🧪 COMPLEX TEST CASE: 10 Nodes, 6 Participating, Multi-Table Aggregation");
    println!("{}\n", "=".repeat(80));

    // Setup: Create temporary directories
    let test_dir = std::env::temp_dir().join("rca_engine_complex_test");
    let metadata_dir = test_dir.join("metadata");
    let data_dir = test_dir.join("data");
    
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir)?;

    // Copy complex test metadata files
    println!("📋 Loading complex test metadata...\n");
    let source_metadata = PathBuf::from("metadata");
    
    // Copy standard metadata files
    for entry in fs::read_dir(&source_metadata)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        
        // Skip complex test files - we'll copy them separately
        if file_name_str.starts_with("complex_test_") {
            continue;
        }
        
        fs::copy(entry.path(), metadata_dir.join(&file_name))?;
    }
    
    // Copy complex test metadata files and rename them
    fs::copy(
        source_metadata.join("complex_test_tables.json"),
        metadata_dir.join("tables.json")
    )?;
    fs::copy(
        source_metadata.join("complex_test_lineage.json"),
        metadata_dir.join("lineage.json")
    )?;
    fs::copy(
        source_metadata.join("complex_test_rules.json"),
        metadata_dir.join("rules.json")
    )?;
    fs::copy(
        source_metadata.join("complex_test_entities.json"),
        metadata_dir.join("entities.json")
    )?;
    fs::copy(
        source_metadata.join("complex_test_business_labels.json"),
        metadata_dir.join("business_labels.json")
    )?;
    
    println!("   ✓ Metadata files loaded\n");

    // Create complex test data files
    println!("📊 Creating complex test data...\n");
    create_complex_test_data(&data_dir)?;

    // Load metadata
    println!("\n🔍 Loading metadata...");
    let metadata = Metadata::load(&metadata_dir)?;
    println!("   ✓ Loaded {} entities", metadata.entities.len());
    println!("   ✓ Loaded {} tables", metadata.tables.len());
    println!("   ✓ Loaded {} rules", metadata.rules.len());
    println!("   ✓ Loaded {} metrics", metadata.metrics.len());
    println!("   ✓ Loaded {} lineage edges", metadata.lineage.edges.len());

    // Initialize LLM client - check environment variables and .env file
    println!("\n🤖 Initializing LLM client...");
    
    // First, try to load from .env file (if it exists)
    // dotenv::dotenv() searches from current directory up to root
    let dotenv_loaded = dotenv::dotenv().is_ok();
    if dotenv_loaded {
        println!("   ✓ Loaded .env file");
    } else {
        println!("   ℹ️  No .env file found (checking environment variables directly)");
    }
    
    // Check for API key in environment (either from .env or already set)
    let api_key = std::env::var("OPENAI_API_KEY")
        .unwrap_or_else(|_| {
            println!("   ⚠️  Warning: OPENAI_API_KEY not found in environment or .env file");
            println!("   ℹ️  Using dummy mode - test will use mock LLM responses");
            println!("   ℹ️  To use real OpenAI API:");
            println!("      - Set OPENAI_API_KEY environment variable, OR");
            println!("      - Create a .env file in the project root with: OPENAI_API_KEY=your-key");
            "dummy-api-key".to_string()
        });
    
    let model = std::env::var("OPENAI_MODEL")
        .unwrap_or_else(|_| "gpt-4o-mini".to_string());
    
    let base_url = std::env::var("OPENAI_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    
    let llm = LlmClient::new(api_key.clone(), model.clone(), base_url.clone());
    
    if api_key == "dummy-api-key" {
        println!("   ✓ LLM client initialized (DUMMY MODE - using mock responses)");
        println!("   ⚠️  Note: This test will NOT make real API calls to OpenAI");
    } else {
        println!("   ✓ LLM client initialized (REAL MODE - using OpenAI API)");
        println!("   ✓ Model: {}", model);
        println!("   ✓ Base URL: {}", base_url);
        println!("   ✓ API Key: {}...{}", &api_key[..8.min(api_key.len())], &api_key[api_key.len().saturating_sub(4)..]);
    }

    // Create RCA engine
    println!("\n🚀 Creating RCA Engine...");
    let engine = RcaEngine::new(metadata, llm, data_dir.clone());
    println!("   ✓ RCA Engine created");

    // Run complex reconciliation query
    println!("\n⚙️  Running complex reconciliation query...");
    let query = "System A vs System B TOS reconciliation as of 2025-12-31";
    println!("   Query: \"{}\"", query);
    
    let result = engine.run(query).await?;

    // Verify results
    println!("\n{}", "=".repeat(80));
    println!("✅ VERIFICATION RESULTS");
    println!("{}\n", "=".repeat(80));
    println!("   System A: {}", result.system_a);
    println!("   System B: {}", result.system_b);
    println!("   Metric: {}", result.metric);
    assert_eq!(result.system_a, "system_a");
    assert_eq!(result.system_b, "system_b");
    assert_eq!(result.metric, "tos");

    println!("\n   Classifications: {}", result.classifications.len());
    assert!(!result.classifications.is_empty(), "Should have at least one classification");

    println!("\n   Population Diff:");
    println!("     Common: {}", result.comparison.population_diff.common_count);
    println!("     Missing in B: {}", result.comparison.population_diff.missing_in_b.len());
    println!("     Extra in B: {}", result.comparison.population_diff.extra_in_b.len());

    println!("\n   Data Diff:");
    println!("     Matches: {}", result.comparison.data_diff.matches);
    println!("     Mismatches: {}", result.comparison.data_diff.mismatches);

    // Expected: Should have mismatches due to different TOS calculations
    assert!(
        result.comparison.data_diff.mismatches > 0 || 
        result.comparison.population_diff.missing_in_b.len() > 0 ||
        result.comparison.population_diff.extra_in_b.len() > 0,
        "Should have at least one mismatch to demonstrate RCA"
    );

    println!("\n{}", "=".repeat(80));
    println!("📄 FULL RESULT SUMMARY");
    println!("{}\n", "=".repeat(80));
    println!("{}", result);

    println!("\n{}", "=".repeat(80));
    println!("✅ COMPLEX TEST PASSED: Chain-of-thought execution completed successfully!");
    println!("{}\n", "=".repeat(80));
    
    // Cleanup (optional - comment out to keep test files for inspection)
    // fs::remove_dir_all(&test_dir)?;

    Ok(())
}

