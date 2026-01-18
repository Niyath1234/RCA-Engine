# Simplified RCA Engine - User Guide

## Vision: Simple 5-Step Process

Users upload tables with basic metadata and ask questions in natural language. The system automatically figures out everything else.

## How It Works

### Step 1: Upload Your Tables

Upload CSV files with just 2 things:
1. **Primary Keys** (Required) - The column(s) that uniquely identify each row
2. **Column Descriptions** (Optional) - If not provided, LLM will infer from column names

```json
POST /api/tables/upload

{
  "table_name": "khatabook_customers",
  "csv_path": "data/khatabook_customers.csv",
  "primary_keys": ["customer_id"],
  "column_descriptions": {
    "customer_id": "Unique customer identifier",
    "total_outstanding": "Total amount customer owes"
  }
}
```

### Step 2: Upload More Tables

```json
POST /api/tables/upload

{
  "table_name": "tb_loan_details",
  "csv_path": "data/tb_loan_details.csv",
  "primary_keys": ["loan_id"],
  "column_descriptions": {
    "loan_id": "Unique loan identifier",
    "customer_id": "Customer who owns this loan",
    "principal_amount": "Original loan amount",
    "interest_amount": "Interest charged"
  }
}
```

### Step 3: Ask Your Question

```json
POST /api/reconcile/ask

{
  "question": "TOS recon between khatabook and TB"
}
```

### What Happens Automatically

The system:

1. ✅ **Detects Systems** from table names
   - `khatabook_customers` → System: "khatabook"
   - `tb_loan_details` → System: "tb"

2. ✅ **Groups Tables by System**
   - System "khatabook": [khatabook_customers, khatabook_accounts, ...]
   - System "tb": [tb_loan_details, tb_payments, ...]

3. ✅ **Infers Relationships** from common columns
   - `customer_id` links customers to loans
   - System handles grain mismatch automatically (customer-level vs loan-level)

4. ✅ **Generates Business Rules** from column patterns
   - Finds "total_outstanding" in khatabook
   - Finds "principal_amount + interest_amount" in tb
   - Creates reconciliation rules automatically

5. ✅ **Executes Reconciliation**
   - Compares data at row level
   - Identifies exact differences
   - Explains root causes

## Example Response

```json
{
  "success": true,
  "message": "Detected reconciliation: total_outstanding between khatabook and tb",
  "intent": {
    "metric": "total_outstanding",
    "systems": ["khatabook", "tb"],
    "tables": {
      "khatabook": ["khatabook_customers", "khatabook_accounts"],
      "tb": ["tb_loan_details", "tb_payments"]
    },
    "suggested_rules": [
      "System khatabook: Sum of total_outstanding from khatabook_customers",
      "System tb: Sum of principal_amount from tb_loan_details"
    ]
  },
  "results": {
    "system_a_total": 1000000.00,
    "system_b_total": 950000.00,
    "difference": 50000.00,
    "matching_rows": 950,
    "mismatched_rows": 10,
    "missing_in_a": 0,
    "missing_in_b": 50,
    "summary": "System tb is missing 50 rows. Root cause: Join failure on customer_id."
  }
}
```

## Real-World Example

### Scenario: Reconcile TOS between Khatabook and Trial Balance

**Tables Uploaded:**

1. `khatabook_customers.csv`
   - Primary key: `customer_id`
   - Columns: `customer_id`, `total_outstanding`, `as_of_date`

2. `tb_loan_details.csv`
   - Primary key: `loan_id`
   - Columns: `loan_id`, `customer_id`, `principal`, `interest`

3. `tb_payments.csv`
   - Primary key: `payment_id`
   - Columns: `payment_id`, `loan_id`, `payment_amount`

**Question:**
```
"TOS recon between khatabook and TB"
```

**System Auto-Detects:**
- System A: "khatabook" (khatabook_customers)
- System B: "tb" (tb_loan_details, tb_payments)
- Metric: "total_outstanding" (from TOS)
- Grain mismatch: customer-level vs loan-level → auto-aggregates

**Result:**
```
Reconciliation Complete:
- Khatabook TOS: 1,000,000
- TB TOS: 950,000
- Difference: 50,000

Root Cause:
- 50 customers missing in TB
- Reason: Join failure between tb_loan_details and customers
- Affected IDs: CUST001, CUST002, ..., CUST050
```

## Key Advantages

### ❌ Old Way (Complex)
```json
{
  "metadata": {
    "tables": [...], // 100 lines
    "rules": [...],  // 200 lines
    "lineage": [...], // 150 lines
    "entities": [...] // 100 lines
  },
  "systems": {
    "system_a": {...}, // 50 lines
    "system_b": {...}  // 50 lines
  },
  // Total: ~650 lines of manual configuration
}
```

### ✅ New Way (Simple)
```json
POST /api/tables/upload
{
  "table_name": "khatabook_customers",
  "primary_keys": ["customer_id"]
}

POST /api/reconcile/ask
{
  "question": "TOS recon between khatabook and TB"
}

// Total: 8 lines
```

## Implementation Status

### ✅ Implemented

1. **Table Upload Module** (`src/table_upload.rs`)
   - Register tables with primary keys
   - Auto-detect table prefix (system membership)
   - Infer schema from CSV
   - Generate full metadata on-the-fly

2. **Simplified Intent Compiler** (`src/simplified_intent.rs`)
   - Auto-detect systems from question
   - Extract metric name
   - Find relevant tables
   - Generate business rules

3. **Simplified API** (`src/simplified_api.rs`)
   - `/api/tables/upload` - Upload tables
   - `/api/reconcile/ask` - Ask questions
   - Automatic system detection
   - Intent compilation

### 🔧 Integration Needed

1. **Connect to RCA Engine**
   - Wire simplified API to existing RCA cursor/graph traversal
   - Use generated metadata
   - Execute reconciliation

2. **Enhanced Business Rule Generation**
   - Better pattern matching for metrics
   - Handle complex formulas
   - Support custom rules

3. **UI Updates**
   - Simple upload form (table name + CSV + primary keys)
   - Question input box
   - Results display

## Technical Architecture

```
User Uploads Tables
    ↓
TableRegistry (stores minimal metadata)
    ↓
User Asks Question: "TOS recon between khatabook and TB"
    ↓
SimplifiedIntentCompiler
    ├─ Detects systems: ["khatabook", "tb"]
    ├─ Finds tables for each system
    ├─ Generates metadata on-the-fly
    └─ Creates IntentSpec
    ↓
Existing RCA Engine
    ├─ Task Grounding (uses generated metadata)
    ├─ Graph Traversal / RCA Cursor
    └─ Returns Results
    ↓
User Gets Answer
```

## Next Steps

1. ✅ Create table upload module
2. ✅ Create simplified intent compiler
3. ✅ Create simplified API
4. 🔧 Wire to existing RCA engine
5. 🔧 Test end-to-end with sample data
6. 🔧 Update UI for simplified workflow

## Summary

**Before**: Manual metadata configuration, complex JSON files, 650+ lines

**After**: Upload CSVs with primary keys, ask questions, get answers automatically

**Example**:
- Upload: khatabook_customers.csv (primary_key: customer_id)
- Upload: tb_loan_details.csv (primary_key: loan_id)
- Ask: "TOS recon between khatabook and TB"
- Result: Complete reconciliation with root causes

**The system handles everything else automatically!**

