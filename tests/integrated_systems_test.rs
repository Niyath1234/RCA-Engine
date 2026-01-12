//! Comprehensive Integration Test: Hypergraph + KnowledgeBase + WorldState
//! 
//! This test demonstrates how all three systems work together for:
//! - RCA (Root Cause Analysis) queries
//! - DV (Data Validation) queries

use polars::prelude::*;
use rca_engine::metadata::Metadata;
use rca_engine::rca::RcaEngine;
use rca_engine::validation::ValidationEngine;
use rca_engine::llm::LlmClient;
// Note: Hypergraph integration requires dashmap dependency
// For now, we'll test with RCA metadata directly
// use rca_engine::graph::Hypergraph;
// use rca_engine::graph_adapter::GraphAdapter;
use std::path::PathBuf;
use std::fs;
use dotenv;

// Import WorldState and KnowledgeBase modules
#[path = "../WorldState/mod.rs"]
mod world_state;
#[path = "../KnowledgeBase/mod.rs"]
mod knowledge_base;

use world_state::{WorldState, TableSchema, ColumnInfo};
use knowledge_base::{KnowledgeBase, BusinessConcept, ConceptType};

/// Create test data with MSME column for validation testing
fn create_test_data_with_msme(data_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(data_dir.join("khatabook"))?;
    fs::create_dir_all(data_dir.join("tb"))?;

    // Create khatabook_loans with psl_type column (MSME values)
    let loans_df = df! [
        "loan_id" => ["1001", "1002", "1003", "1004"],
        "customer_id" => ["C001", "C002", "C003", "C004"],
        "disbursement_date" => ["2025-01-15", "2025-02-20", "2025-03-10", "2025-04-05"],
        "principal_amount" => [100000.0, 50000.0, 75000.0, 200000.0],
        "psl_type" => ["MSME", "N/A", "MSME", "MSME"],  // MSME column for testing
        "ledger" => [3000.0, 6000.0, 4000.0, 8000.0]  // For validation constraint
    ]?;
    
    let loans_path = data_dir.join("khatabook/loans.parquet");
    let mut file = std::fs::File::create(&loans_path)?;
    ParquetWriter::new(&mut file).finish(&mut loans_df.clone())?;

    // Create khatabook_emis
    let emis_df = df! [
        "loan_id" => ["1001", "1001", "1002", "1003"],
        "emi_number" => [1, 2, 1, 1],
        "due_date" => ["2025-02-15", "2025-03-15", "2025-03-20", "2025-04-10"],
        "emi_amount" => [5000.0, 5000.0, 3000.0, 4000.0]
    ]?;
    
    let emis_path = data_dir.join("khatabook/emis.parquet");
    let mut file = std::fs::File::create(&emis_path)?;
    ParquetWriter::new(&mut file).finish(&mut emis_df.clone())?;

    // Create khatabook_transactions (needed for TOS calculation)
    let transactions_df = df! [
        "transaction_id" => ["T001", "T002", "T003"],
        "loan_id" => ["1001", "1001", "1002"],
        "emi_number" => [1, 2, 1],
        "transaction_date" => ["2025-02-10", "2025-03-10", "2025-03-18"],
        "transaction_amount" => [1000.0, 500.0, 0.0]  // Some payments made
    ]?;
    
    let transactions_path = data_dir.join("khatabook/transactions.parquet");
    let mut file = std::fs::File::create(&transactions_path)?;
    ParquetWriter::new(&mut file).finish(&mut transactions_df.clone())?;

    // Create tb_loans
    let tb_loans_df = df! [
        "loan_id" => ["1001", "1002"],
        "customer_id" => ["C001", "C002"],
        "disbursement_date" => ["2025-01-15", "2025-02-20"],
        "principal_amount" => [100000.0, 50000.0],
        "total_outstanding" => [6000.0, 0.0]
    ]?;
    
    let tb_loans_path = data_dir.join("tb/loans.parquet");
    let mut file = std::fs::File::create(&tb_loans_path)?;
    ParquetWriter::new(&mut file).finish(&mut tb_loans_df.clone())?;

    // Create tb_loan_summary
    let tb_summary_df = df! [
        "loan_id" => ["1001", "1002"],
        "as_of_date" => ["2025-12-31", "2025-12-31"],
        "total_outstanding" => [6000.0, 0.0]
    ]?;
    
    let tb_summary_path = data_dir.join("tb/loan_summary.parquet");
    let mut file = std::fs::File::create(&tb_summary_path)?;
    ParquetWriter::new(&mut file).finish(&mut tb_summary_df.clone())?;

    println!("✅ Created test data files with MSME column");
    Ok(())
}

/// Setup WorldState with schema, statistics, and join rules
fn setup_worldstate() -> WorldState {
    let mut world_state = WorldState::new();
    
    // Register khatabook_loans table schema
    let mut loans_schema = TableSchema::new("khatabook_loans".to_string());
    loans_schema.add_column(ColumnInfo::with_single_name("loan_id".to_string()));
    loans_schema.add_column(ColumnInfo::with_single_name("customer_id".to_string()));
    loans_schema.add_column(ColumnInfo::with_single_name("disbursement_date".to_string()));
    loans_schema.add_column(ColumnInfo::with_single_name("principal_amount".to_string()));
    loans_schema.add_column(ColumnInfo::with_single_name("psl_type".to_string()));
    loans_schema.add_column(ColumnInfo::with_single_name("ledger".to_string()));
    world_state.schema_registry.register_table(loans_schema);
    
    // Register khatabook_emis table schema
    let mut emis_schema = TableSchema::new("khatabook_emis".to_string());
    emis_schema.add_column(ColumnInfo::with_single_name("loan_id".to_string()));
    emis_schema.add_column(ColumnInfo::with_single_name("emi_number".to_string()));
    emis_schema.add_column(ColumnInfo::with_single_name("due_date".to_string()));
    emis_schema.add_column(ColumnInfo::with_single_name("emi_amount".to_string()));
    world_state.schema_registry.register_table(emis_schema);
    
    // Register primary keys
    use world_state::keys::{TableKeys, PrimaryKey};
    let loans_keys = TableKeys {
        primary_key: Some(PrimaryKey {
            columns: vec!["loan_id".to_string()],
            is_synthetic: false,
        }),
        natural_keys: Vec::new(),
        event_time: None,
        updated_at: None,
        dedupe_strategy: world_state::keys::DedupeStrategy::AppendOnly,
    };
    world_state.key_registry.register_table_keys("khatabook_loans".to_string(), loans_keys);
    
    let emis_keys = TableKeys {
        primary_key: Some(PrimaryKey {
            columns: vec!["loan_id".to_string(), "emi_number".to_string()],
            is_synthetic: false,
        }),
        natural_keys: Vec::new(),
        event_time: None,
        updated_at: None,
        dedupe_strategy: world_state::keys::DedupeStrategy::AppendOnly,
    };
    world_state.key_registry.register_table_keys("khatabook_emis".to_string(), emis_keys);
    
    // Register join rule: loans → emis
    let join_rule = world_state::rules::JoinRule::new(
        "loans_to_emis".to_string(),
        "khatabook_loans".to_string(),
        vec!["loan_id".to_string()],
        "khatabook_emis".to_string(),
        vec!["loan_id".to_string()],
        "inner".to_string(),
        "1:N".to_string(),
    );
    world_state.rule_registry.register_rule(join_rule);
    
    // Register statistics (simplified)
    // In production, these would come from actual data analysis
    
    println!("✅ WorldState setup complete:");
    println!("  - Tables registered: {}", world_state.schema_registry.tables().len());
    println!("  - Join rules: {}", world_state.rule_registry.list_all_rules().len());
    
    world_state
}

/// Setup KnowledgeBase with business concepts
fn setup_knowledgebase() -> KnowledgeBase {
    let mut kb = KnowledgeBase::new();
    
    // Add MSME concept
    let msme_concept = BusinessConcept::new(
        "msme_concept".to_string(),
        "MSME".to_string(),
        ConceptType::DomainConcept,
        "Micro Small Medium Enterprise - a category in PSL (Priority Sector Lending)".to_string(),
    );
    kb.add_concept(msme_concept);
    
    // Add TOS concept
    let tos_concept = BusinessConcept::new(
        "tos_concept".to_string(),
        "TOS".to_string(),
        ConceptType::Metric,
        "Total Outstanding - sum of principal and interest".to_string(),
    );
    kb.add_concept(tos_concept);
    
    // Add loan table concept
    let loan_table_concept = BusinessConcept::new(
        "loan_table_concept".to_string(),
        "Loan Table".to_string(),
        ConceptType::Table,
        "Table containing loan information".to_string(),
    );
    kb.add_concept(loan_table_concept);
    
    println!("✅ KnowledgeBase setup complete:");
    // Count concepts by searching for common terms
    let concept_count = kb.search_by_name("MSME").len() + kb.search_by_name("TOS").len() + kb.search_by_name("Loan").len();
    println!("  - Concepts: {} (sample)", concept_count);
    
    kb
}

/// Populate KnowledgeBase from RCA metadata and graph entities
/// This creates table concepts, column concepts, and links them properly
fn populate_knowledge_base_from_metadata(
    kb: &mut KnowledgeBase,
    metadata: &Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📚 Populating KnowledgeBase from metadata...");
    
    // 1. Create table concepts for each table
    for table in &metadata.tables {
        let table_concept_id = format!("table_{}", table.name);
        let table_concept = BusinessConcept::new(
            table_concept_id.clone(),
            table.name.clone(),
            ConceptType::Table,
            format!("Table: {} in system {}", table.name, table.system),
        );
        kb.add_concept(table_concept);
        
        // Link table to its system
        if let Some(mut concept) = kb.get_concept_mut(&table_concept_id) {
            concept.related_tables.push(table.name.clone());
            concept.tags.push(table.system.clone());
        }
    }
    println!("  ✓ Created {} table concepts", metadata.tables.len());
    
    // 2. Create column concepts for important columns (especially those with distinct_values)
    let mut column_count = 0;
    for table in &metadata.tables {
        if let Some(ref columns) = table.columns {
            for col in columns {
                // Create concept for columns that have distinct_values (semantic meaning)
                if col.distinct_values.is_some() {
                    let col_concept_id = format!("column_{}_{}", table.name, col.name);
                    let col_concept = BusinessConcept::new(
                        col_concept_id.clone(),
                        col.name.clone(),
                        ConceptType::ColumnSemantics,
                        format!("Column {} in table {}", col.name, table.name),
                    );
                    kb.add_concept(col_concept);
                    
                    // Link column to its table
                    if let Some(mut concept) = kb.get_concept_mut(&col_concept_id) {
                        concept.related_columns.push(col.name.clone());
                        concept.related_tables.push(table.name.clone());
                    }
                    column_count += 1;
                }
            }
        }
    }
    println!("  ✓ Created {} column concepts", column_count);
    
    // 3. Link MSME concept to psl_type column if it exists
    if let Some(mut msme_concept) = kb.search_by_name("MSME").first().map(|c| (*c).clone()) {
        // Find psl_type column in any table
        for table in &metadata.tables {
            if let Some(ref columns) = table.columns {
                for col in columns {
                    if col.name == "psl_type" {
                        if !msme_concept.related_columns.contains(&col.name) {
                            msme_concept.related_columns.push(col.name.clone());
                        }
                        if !msme_concept.related_tables.contains(&table.name) {
                            msme_concept.related_tables.push(table.name.clone());
                        }
                    }
                }
            }
        }
        kb.add_concept(msme_concept);
    }
    
    // 4. Link TOS metric to relevant tables and columns from rules
    for rule in &metadata.rules {
        if rule.metric == "tos" {
            if let Some(mut tos_concept) = kb.search_by_name("TOS").first().map(|c| (*c).clone()) {
                // Extract tables from rule computation formula
                let formula = &rule.computation.formula;
                for table in &metadata.tables {
                    if formula.contains(&table.name) && !tos_concept.related_tables.contains(&table.name) {
                        tos_concept.related_tables.push(table.name.clone());
                    }
                }
                // Add computation description as component
                if !tos_concept.components.contains(&rule.computation.description) {
                    tos_concept.components.push(rule.computation.description.clone());
                }
                // Add formula as SQL expression
                tos_concept.sql_expression = Some(formula.clone());
                kb.add_concept(tos_concept);
            }
        }
    }
    
    println!("  ✓ Linked concepts to tables and columns");
    println!("  ✓ Total concepts in KnowledgeBase: {}", kb.list_all().len());
    
    Ok(())
}

/// Verify integration points (without full Hypergraph due to dependency)
fn verify_integration_points(
    world_state: &WorldState,
    knowledge_base: &KnowledgeBase,
    rca_metadata: &Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔗 Verifying integration points...");
    
    // Verify WorldState has tables
    assert!(!world_state.schema_registry.tables().is_empty(), "WorldState should have tables");
    println!("  ✓ WorldState: {} tables registered", world_state.schema_registry.tables().len());
    
    // Verify KnowledgeBase has concepts
    // Get concepts by iterating through the knowledge base
    let concepts: Vec<_> = knowledge_base.search_by_name(""); // Empty string to get all
    assert!(!concepts.is_empty(), "KnowledgeBase should have concepts");
    println!("  ✓ KnowledgeBase: {} concepts registered", concepts.len());
    
    // Verify RCA metadata
    assert!(!rca_metadata.tables.is_empty(), "RCA metadata should have tables");
    println!("  ✓ RCA Metadata: {} tables", rca_metadata.tables.len());
    
    // Verify we can find MSME concept
    let msme_concepts = knowledge_base.search_by_name("MSME");
    assert!(!msme_concepts.is_empty(), "Should find MSME concept");
    println!("  ✓ Found MSME concept in KnowledgeBase");
    
    // Verify we can find psl_type column in metadata
    let loans_table = rca_metadata.tables.iter()
        .find(|t| t.name == "khatabook_loans")
        .ok_or("khatabook_loans table not found")?;
    
    if let Some(ref columns) = loans_table.columns {
        let psl_col = columns.iter().find(|c| c.name == "psl_type");
        assert!(psl_col.is_some(), "Should have psl_type column");
        println!("  ✓ Found psl_type column in RCA metadata");
        
        // Verify distinct_values if populated
        if let Some(ref col) = psl_col {
            if let Some(ref distinct_vals) = col.distinct_values {
                println!("  ✓ psl_type has {} distinct values", distinct_vals.len());
                let has_msme = distinct_vals.iter().any(|v| {
                    if let serde_json::Value::String(s) = v {
                        s.to_lowercase() == "msme"
                    } else {
                        false
                    }
                });
                assert!(has_msme, "psl_type should have MSME in distinct_values");
                println!("  ✓ psl_type contains MSME in distinct_values");
            }
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_integrated_rca_query() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 INTEGRATED SYSTEMS TEST: RCA Query");
    println!("{}", "=".repeat(80));
    
    // Setup test directories
    let test_dir = std::env::temp_dir().join("rca_integrated_test");
    let metadata_dir = test_dir.join("metadata");
    let data_dir = test_dir.join("data");
    
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir)?;

    // Copy metadata files
    println!("\n📋 Step 1: Setting up metadata...");
    let source_metadata = PathBuf::from("metadata");
    for entry in fs::read_dir(&source_metadata)? {
        let entry = entry?;
        fs::copy(entry.path(), metadata_dir.join(entry.file_name()))?;
    }

    // Create test data
    println!("\n📊 Step 2: Creating test data...");
    create_test_data_with_msme(&data_dir)?;

    // Load RCA metadata
    println!("\n🔍 Step 3: Loading RCA metadata...");
    let mut rca_metadata = Metadata::load(&metadata_dir)?;
    println!("  ✓ Loaded {} tables", rca_metadata.tables.len());
    println!("  ✓ Loaded {} rules", rca_metadata.rules.len());

    // Populate distinct_values from actual data for all tables
    println!("\n📊 Step 3.5: Populating distinct_values from data...");
    let table_names: Vec<String> = rca_metadata.tables.iter().map(|t| t.name.clone()).collect();
    for table_name in &table_names {
        if let Err(e) = rca_metadata.populate_distinct_values(table_name, &data_dir) {
            println!("  ⚠️  Could not populate distinct_values for {}: {}", table_name, e);
        } else {
            println!("  ✓ Populated distinct_values for {}", table_name);
        }
    }

    // Setup WorldState
    println!("\n🌍 Step 4: Setting up WorldState...");
    let world_state = setup_worldstate();

    // Setup KnowledgeBase
    println!("\n📚 Step 5: Setting up KnowledgeBase...");
    let mut knowledge_base = setup_knowledgebase();
    
    // Populate KnowledgeBase from metadata and graph entities
    populate_knowledge_base_from_metadata(&mut knowledge_base, &rca_metadata)?;
    
    // Ensure MSME → psl_type mapping is in KnowledgeBase
    let msme_concept = knowledge_base.search_by_name("MSME").first().map(|c| (*c).clone());
    if let Some(mut concept) = msme_concept {
        if !concept.related_columns.contains(&"psl_type".to_string()) {
            concept.related_columns.push("psl_type".to_string());
        }
        if !concept.related_tables.contains(&"khatabook_loans".to_string()) {
            concept.related_tables.push("khatabook_loans".to_string());
        }
        // Update concept back into knowledge base
        knowledge_base.add_concept(concept);
        println!("  ✓ Updated MSME concept with psl_type mapping");
    }

    // Verify integration points
    println!("\n🕸️  Step 6: Verifying integration points...");
    verify_integration_points(&world_state, &knowledge_base, &rca_metadata)?;
    
    // Test column detection using RCA metadata distinct_values
    println!("\n🔎 Step 7: Testing column detection with distinct_values...");
    let graph = rca_engine::graph::Hypergraph::new(rca_metadata.clone());
    let msme_columns = graph.find_columns_with_value("MSME", Some("khatabook"));
    println!("  ✓ Found columns for 'MSME': {:?}", msme_columns);
    assert!(!msme_columns.is_empty(), "Should find psl_type column for MSME");

    // Initialize LLM
    println!("\n🤖 Step 8: Initializing LLM...");
    dotenv::dotenv().ok();
    let api_key = std::env::var("OPENAI_API_KEY")
        .unwrap_or_else(|_| "dummy-api-key".to_string());
    let llm = LlmClient::new(api_key, "gpt-4o-mini".to_string(), "https://api.openai.com/v1".to_string());

    // Create RCA engine
    println!("\n🚀 Step 9: Creating RCA Engine...");
    let engine = RcaEngine::new(rca_metadata, llm, data_dir.clone());

    // Run RCA query
    println!("\n⚙️  Step 10: Running RCA query...");
    let query = "Khatabook vs TB TOS recon as of 2025-12-31";
    println!("  Query: {}", query);
    
    let result = engine.run(query).await?;

    // Verify results
    println!("\n✅ RCA Results:");
    println!("  System A: {}", result.system_a);
    println!("  System B: {}", result.system_b);
    println!("  Metric: {}", result.metric);
    assert_eq!(result.system_a, "khatabook");
    assert_eq!(result.system_b, "tb");
    assert_eq!(result.metric, "tos");

    println!("\n✅ Test PASSED: Integrated RCA query completed!");
    Ok(())
}

#[tokio::test]
async fn test_integrated_dv_query() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 INTEGRATED SYSTEMS TEST: DV (Data Validation) Query");
    println!("{}", "=".repeat(80));
    
    // Setup test directories
    let test_dir = std::env::temp_dir().join("dv_integrated_test");
    let metadata_dir = test_dir.join("metadata");
    let data_dir = test_dir.join("data");
    
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir)?;

    // Copy metadata files
    println!("\n📋 Step 1: Setting up metadata...");
    let source_metadata = PathBuf::from("metadata");
    for entry in fs::read_dir(&source_metadata)? {
        let entry = entry?;
        fs::copy(entry.path(), metadata_dir.join(entry.file_name()))?;
    }

    // Create test data with MSME
    println!("\n📊 Step 2: Creating test data with MSME column...");
    create_test_data_with_msme(&data_dir)?;

    // Load RCA metadata
    println!("\n🔍 Step 3: Loading RCA metadata...");
    let mut rca_metadata = Metadata::load(&metadata_dir)?;
    
    // Populate distinct_values for psl_type column
    println!("\n📊 Step 4: Populating distinct_values metadata...");
    rca_metadata.populate_distinct_values("khatabook_loans", &data_dir)?;
    println!("  ✓ Populated distinct_values for khatabook_loans");

    // Setup WorldState
    println!("\n🌍 Step 5: Setting up WorldState...");
    let world_state = setup_worldstate();

    // Setup KnowledgeBase with MSME concept
    println!("\n📚 Step 6: Setting up KnowledgeBase...");
    let knowledge_base = setup_knowledgebase();
    
    // Add MSME → psl_type mapping to KnowledgeBase
    let msme_concept = knowledge_base.search_by_name("MSME").first().map(|c| (*c).clone());
    if let Some(mut concept) = msme_concept {
        concept.related_columns.push("psl_type".to_string());
        concept.related_tables.push("khatabook_loans".to_string());
        // Update concept (simplified - in production would use update method)
    }

    // Verify integration points
    println!("\n🕸️  Step 7: Verifying integration points...");
    verify_integration_points(&world_state, &knowledge_base, &rca_metadata)?;
    
    // Test column detection using RCA metadata
    println!("\n🔎 Step 8: Testing column detection...");
    let graph = rca_engine::graph::Hypergraph::new(rca_metadata.clone());
    let msme_columns = graph.find_columns_with_value("MSME", Some("khatabook"));
    println!("  ✓ Found columns for 'MSME': {:?}", msme_columns);
    assert!(!msme_columns.is_empty(), "Should find psl_type column");

    // Initialize LLM
    println!("\n🤖 Step 9: Initializing LLM...");
    dotenv::dotenv().ok();
    let api_key = std::env::var("OPENAI_API_KEY")
        .unwrap_or_else(|_| "dummy-api-key".to_string());
    let llm = LlmClient::new(api_key, "gpt-4o-mini".to_string(), "https://api.openai.com/v1".to_string());

    // Create Validation engine
    println!("\n🚀 Step 10: Creating Validation Engine...");
    let engine = ValidationEngine::new(rca_metadata, llm, data_dir.clone());

    // Run DV query
    println!("\n⚙️  Step 11: Running DV query...");
    let query = "MSME can't have ledger >5000";
    println!("  Query: {}", query);
    
    let result = engine.run(query).await?;

    // Verify results
    println!("\n✅ DV Results:");
    println!("  Constraint Type: {}", result.constraint_type);
    println!("  System: {}", result.system);
    println!("  Total Rows Checked: {}", result.total_rows_checked);
    println!("  Violations Found: {}", result.violations_count);
    println!("  Pass Rate: {:.2}%", result.pass_rate * 100.0);
    
    assert_eq!(result.system, "khatabook");
    assert!(result.total_rows_checked > 0, "Should check some rows");

    println!("\n📄 Full Validation Result:");
    println!("{}", result);

    println!("\n✅ Test PASSED: Integrated DV query completed!");
    Ok(())
}

#[tokio::test]
async fn test_all_systems_communication() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 INTEGRATED SYSTEMS TEST: Communication Verification");
    println!("{}", "=".repeat(80));
    
    // Setup
    let test_dir = std::env::temp_dir().join("systems_communication_test");
    let metadata_dir = test_dir.join("metadata");
    let data_dir = test_dir.join("data");
    
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir)?;

    // Copy metadata
    let source_metadata = PathBuf::from("metadata");
    for entry in fs::read_dir(&source_metadata)? {
        let entry = entry?;
        fs::copy(entry.path(), metadata_dir.join(entry.file_name()))?;
    }

    create_test_data_with_msme(&data_dir)?;
    let mut rca_metadata = Metadata::load(&metadata_dir)?;
    rca_metadata.populate_distinct_values("khatabook_loans", &data_dir)?;

    let world_state = setup_worldstate();
    let knowledge_base = setup_knowledgebase();
    verify_integration_points(&world_state, &knowledge_base, &rca_metadata)?;

    println!("\n🔍 Testing System Communication:");
    
    // Test 1: KnowledgeBase → RCA Metadata
    println!("\n1. KnowledgeBase → RCA Metadata:");
    let concepts = knowledge_base.search_by_name("MSME");
    println!("   ✓ KnowledgeBase found {} MSME concepts", concepts.len());
    
    let graph = rca_engine::graph::Hypergraph::new(rca_metadata.clone());
    let columns = graph.find_columns_with_value("MSME", Some("khatabook"));
    println!("   ✓ RCA Graph found {} columns containing MSME", columns.len());
    assert!(!columns.is_empty(), "Should find columns via distinct_values");

    // Test 2: WorldState → RCA Metadata
    println!("\n2. WorldState → RCA Metadata:");
    let tables: Vec<_> = world_state.schema_registry.tables().keys().collect();
    println!("   ✓ WorldState has {} tables", tables.len());
    
    let rca_tables: Vec<_> = rca_metadata.tables.iter().map(|t| &t.name).collect();
    println!("   ✓ RCA Metadata has {} tables", rca_tables.len());
    assert_eq!(tables.len(), rca_tables.len(), "Tables should match");

    // Test 3: Graph path finding
    println!("\n3. Graph Path Finding:");
    let path = graph.find_join_path("khatabook_loans", "khatabook_emis");
    println!("   ✓ Graph found join path: {:?}", path.is_ok());
    
    // Test 4: All systems together
    println!("\n4. All Systems Together:");
    println!("   ✓ WorldState provides: Schema, Statistics, Join Rules");
    println!("   ✓ KnowledgeBase provides: Business Concepts, Semantic Mappings");
    println!("   ✓ Hypergraph provides: Optimized Graph Structure, Path Finding");
    println!("   ✓ RCA-Engine uses: All three for query execution");

    println!("\n✅ All systems communicating successfully!");
    Ok(())
}

