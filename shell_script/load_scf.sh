#!/bin/bash
# Script to load SCF CSV files into the RCA engine pipeline

cd "$(dirname "$0")"

echo "📥 Loading SCF CSV files into RCA Engine..."
echo ""

# Load scf_v1
echo "Loading scf_v1..."
cargo run --bin rca-engine upload data/scf_v1/scf_loans.csv --metadata-dir metadata --data-dir data <<EOF
SCF loan data from system v1
scf_v1
loan
loan_account_id
loan_account_id
EOF

echo ""
echo "Loading scf_v2..."
cargo run --bin rca-engine upload data/scf_v2/scf_loans.csv --metadata-dir metadata --data-dir data <<EOF
SCF loan data from system v2
scf_v2
loan
loan_account_id
loan_account_id
EOF

echo ""
echo "✅ SCF data loaded successfully!"

