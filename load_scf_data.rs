// Script to load SCF CSV files into the RCA engine
use rca_engine::ingestion::{CsvConnector, IngestionOrchestrator};
use rca_engine::world_state::WorldState;
use rca_engine::metadata::Metadata;
use std::path::PathBuf;
use std::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let data_dir = PathBuf::from("data");
    
    // Load metadata
    let metadata = Metadata::load(&metadata_dir)?;
    
    // Create WorldState
    let mut world_state = WorldState::new();
    
    // Create orchestrator
    let orchestrator = IngestionOrchestrator::new();
    
    // Load scf_v1
    println!("📥 Loading scf_v1 data...");
    let csv_v1_path = data_dir.join("scf_v1/scf_loans.csv");
    let csv_v1_content = fs::read_to_string(&csv_v1_path)?;
    let connector_v1 = Box::new(CsvConnector::new("scf_v1_csv".to_string(), csv_v1_content));
    let result_v1 = orchestrator.ingest(
        &mut world_state,
        &data_dir,
        connector_v1,
        Some("scf_v1_loans".to_string()),
    )?;
    println!("✅ scf_v1 loaded: {} records", result_v1.records_ingested);
    
    // Load scf_v2
    println!("📥 Loading scf_v2 data...");
    let csv_v2_path = data_dir.join("scf_v2/scf_loans.csv");
    let csv_v2_content = fs::read_to_string(&csv_v2_path)?;
    let connector_v2 = Box::new(CsvConnector::new("scf_v2_csv".to_string(), csv_v2_content));
    let result_v2 = orchestrator.ingest(
        &mut world_state,
        &data_dir,
        connector_v2,
        Some("scf_v2_loans".to_string()),
    )?;
    println!("✅ scf_v2 loaded: {} records", result_v2.records_ingested);
    
    println!("\n🎉 Both SCF datasets loaded successfully!");
    println!("   - scf_v1: {} records", result_v1.records_ingested);
    println!("   - scf_v2: {} records", result_v2.records_ingested);
    
    Ok(())
}

