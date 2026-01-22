use polars::prelude::*;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📦 Creating dummy test tables...\n");
    
    // Ensure data directory exists
    fs::create_dir_all("data")?;
    
    // 1. Create table_complete_profile (metadata about all tables)
    let table_complete_profile = df! [
        "table_name" => [
            "table_complete_profile",
            "repayments",
            "lmsdata_emi_payment_view",
            "current_month_collection_report",
            "outstanding_daily",
            "da_orders",
            "provisional_writeoff",
            "writeoff_users",
            "disbursements",
            "collections"
        ],
        "system" => [
            "rca_engine",
            "lms",
            "lms",
            "collections",
            "lms",
            "da",
            "lms",
            "lms",
            "lms",
            "collections"
        ],
        "entity_id" => [
            "table_001",
            "table_002",
            "table_003",
            "table_004",
            "table_005",
            "table_006",
            "table_007",
            "table_008",
            "table_009",
            "table_010"
        ],
        "row_count" => [10, 10000, 10000, 10000, 10000, 724674, 97907, 1525892, 50000, 75000],
        "table_size_bytes" => [1024, 1024000, 1024000, 1024000, 1024000, 13133045, 2835495, 52661600, 5120000, 7680000],
        "avg_row_length" => [102, 102, 102, 102, 102, 18, 29, 34, 102, 102],
        "avg_completeness" => [0.95, 0.98, 0.97, 0.96, 0.99, 0.92, 0.94, 0.93, 0.97, 0.96],
        "avg_validity" => [0.98, 0.99, 0.98, 0.97, 0.99, 0.95, 0.96, 0.94, 0.98, 0.97],
        "freshest_data" => [
            "2025-01-21",
            "2025-01-20",
            "2025-01-20",
            "2025-01-21",
            "2025-01-21",
            "2025-01-19",
            "2025-01-18",
            "2025-01-17",
            "2025-01-21",
            "2025-01-20"
        ]
    ]?;
    
    let mut file = std::fs::File::create("data/table_complete_profile.parquet")?;
    ParquetWriter::new(&mut file).finish(&mut table_complete_profile.clone())?;
    println!("✅ Created data/table_complete_profile.parquet (10 rows)");
    
    // 2. Create a sample loan table for testing SQL queries
    let lmsdata_loan = df! [
        "loan_id" => ["L001", "L002", "L003", "L004", "L005", "L006", "L007", "L008", "L009", "L010"],
        "product_type" => ["scf", "scf", "term_loan", "scf", "scf", "term_loan", "scf", "scf", "term_loan", "scf"],
        "status" => ["DISBURSED", "DISBURSED", "DISBURSED", "CLOSED", "DISBURSED", "DISBURSED", "DISBURSED", "CLOSED", "DISBURSED", "DISBURSED"],
        "total_outstanding_balance" => [10000000.0, 15000000.0, 20000000.0, 0.0, 12000000.0, 18000000.0, 25000000.0, 0.0, 30000000.0, 22000000.0],
        "__is_deleted" => [false, false, false, false, false, false, false, false, false, false],
        "disbursement_date" => ["2025-01-01", "2025-01-15", "2025-02-01", "2024-12-01", "2025-01-20", "2025-02-10", "2025-01-25", "2024-11-15", "2025-02-15", "2025-01-30"],
        "customer_id" => ["C001", "C002", "C003", "C004", "C005", "C006", "C007", "C008", "C009", "C010"]
    ]?;
    
    let mut file = std::fs::File::create("data/assetsdb_gold_lmsdata_loan.parquet")?;
    ParquetWriter::new(&mut file).finish(&mut lmsdata_loan.clone())?;
    println!("✅ Created data/assetsdb_gold_lmsdata_loan.parquet (10 rows)");
    
    // 3. Create a simpler test table
    let test_table = df! [
        "id" => [1, 2, 3, 4, 5],
        "name" => ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age" => [25, 30, 35, 28, 32],
        "city" => ["New York", "London", "Tokyo", "Paris", "Berlin"],
        "salary" => [50000.0, 60000.0, 70000.0, 55000.0, 65000.0]
    ]?;
    
    let mut file = std::fs::File::create("data/test_table.parquet")?;
    ParquetWriter::new(&mut file).finish(&mut test_table.clone())?;
    println!("✅ Created data/test_table.parquet (5 rows)");
    
    println!("\n🎉 All dummy test tables created successfully!");
    println!("\nYou can now test queries like:");
    println!("  SELECT * FROM table_complete_profile WHERE table_name = 'disbursements';");
    println!("  SELECT sum(cast(total_outstanding_balance/1e6 as double))/1e7 as AUM_in_Cr");
    println!("    FROM assetsdb_gold_lmsdata_loan");
    println!("    WHERE product_type = 'scf' AND status = 'DISBURSED' AND __is_deleted = false;");
    
    Ok(())
}

