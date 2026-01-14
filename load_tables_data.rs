// Script to load all CSV files from tables directory into the RCA engine
use rca_engine::ingestion::{CsvConnector, IngestionOrchestrator};
use rca_engine::world_state::WorldState;
use rca_engine::metadata::Metadata;
use std::path::PathBuf;
use std::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let data_dir = PathBuf::from("data");
    let tables_dir = PathBuf::from("tables");
    
    // Ensure data directory exists
    if !data_dir.exists() {
        fs::create_dir_all(&data_dir)?;
        println!("📁 Created data directory: {}", data_dir.display());
    }
    
    // Load metadata
    println!("📚 Loading metadata...");
    let _metadata = Metadata::load(&metadata_dir)?;
    println!("✅ Metadata loaded successfully");
    
    // Create WorldState
    let mut world_state = WorldState::new();
    
    // Create orchestrator
    let orchestrator = IngestionOrchestrator::new();
    
    // List of tables to load (from tables.json)
    let table_files = vec![
        ("repayments", "repayments.csv"),
        ("lmsdata_emi_payment_view", "lmsdata_emi_payment_view.csv"),
        ("current_month_collection_report", "current_month_collection_report.csv"),
        ("outstanding_daily", "outstanding_daily.csv"),
        ("da_orders", "da_orders.csv"),
        ("provisional_writeoff", "provisional_writeoff.csv"),
        ("writeoff_users", "writeoff_users.csv"),
    ];
    
    println!("\n📥 Starting data ingestion for {} tables...\n", table_files.len());
    
    let mut results = Vec::new();
    
    for (table_name, file_name) in &table_files {
        let csv_path = tables_dir.join(file_name);
        
        if !csv_path.exists() {
            println!("⚠️  Skipping {} - file not found: {}", table_name, csv_path.display());
            continue;
        }
        
        println!("📥 Loading {}...", table_name);
        
        // Check file size for progress indication
        let file_size = fs::metadata(&csv_path)
            .map(|m| m.len())
            .unwrap_or(0);
        let file_size_mb = file_size as f64 / (1024.0 * 1024.0);
        if file_size_mb > 1.0 {
            println!("   File size: {:.2} MB", file_size_mb);
        }
        
        // Read CSV content
        println!("   Reading CSV file...");
        let csv_content = match fs::read_to_string(&csv_path) {
            Ok(content) => {
                println!("   ✅ File read successfully");
                content
            }
            Err(e) => {
                eprintln!("❌ Failed to read {}: {}", csv_path.display(), e);
                continue;
            }
        };
        
        // Create CSV connector
        let connector = Box::new(CsvConnector::new(
            format!("{}_csv", table_name),
            csv_content,
        ));
        
        // Ingest data
        println!("   Processing and converting to parquet...");
        match orchestrator.ingest(
            &mut world_state,
            &data_dir,
            connector,
            Some(table_name.to_string()),
        ) {
            Ok(result) => {
                println!("✅ {} loaded: {} records", table_name, result.records_ingested);
                println!("   Parquet file: {}/{}.parquet", data_dir.display(), table_name);
                results.push((table_name.clone(), result.records_ingested));
            }
            Err(e) => {
                eprintln!("❌ Failed to load {}: {}", table_name, e);
                eprintln!("   Error details: {:?}", e);
            }
        }
        
        println!();
    }
    
    // Print summary
    println!("\n🎉 Data ingestion complete!");
    println!("\nSummary:");
    println!("{:-<60}", "");
    for (table_name, record_count) in &results {
        println!("  {:<40} {:>15} records", table_name, record_count);
    }
    println!("{:-<60}", "");
    println!("  {:<40} {:>15} tables loaded", "Total", results.len());
    
    Ok(())
}

