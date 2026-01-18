#!/bin/bash
# Script to load all CSV files from tables directory into the RCA engine pipeline

cd "$(dirname "$0")"

echo "📥 Loading CSV files from tables directory into RCA Engine..."
echo ""

# Run the Rust script to load all tables
cargo run --bin load_tables_data

echo ""
echo "✅ Table data loading complete!"

