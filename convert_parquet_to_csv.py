"""
Convert multi_grain_test parquet files to CSV for simplified RCA testing
"""
import pandas as pd
import os
from pathlib import Path

# Base paths
parquet_base = Path("data/multi_grain_test")
csv_base = Path("test_data/multi_grain_csv")

# Create CSV directory
csv_base.mkdir(parents=True, exist_ok=True)

# System A tables to convert
system_a_tables = [
    "loan_summary",
    "customer_loan_mapping",
    "daily_interest_accruals",
    "daily_fees",
    "daily_penalties",
    "emi_schedule",
    "emi_transactions",
    "detailed_transactions",
    "fee_details",
    "customer_summary"
]

# System B tables
system_b_tables = [
    "loan_summary"
]

print("Converting System A tables...")
for table in system_a_tables:
    parquet_path = parquet_base / "system_a" / f"{table}.parquet"
    csv_path = csv_base / f"system_a_{table}.csv"
    
    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
        df.to_csv(csv_path, index=False)
        print(f"✅ {table}: {len(df)} rows → {csv_path}")
    else:
        print(f"❌ {parquet_path} not found")

print("\nConverting System B tables...")
for table in system_b_tables:
    parquet_path = parquet_base / "system_b" / f"{table}.parquet"
    csv_path = csv_base / f"system_b_{table}.csv"
    
    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
        df.to_csv(csv_path, index=False)
        print(f"✅ {table}: {len(df)} rows → {csv_path}")
    else:
        print(f"❌ {parquet_path} not found")

print("\n✅ Conversion complete!")
print(f"CSV files saved to: {csv_base}")

