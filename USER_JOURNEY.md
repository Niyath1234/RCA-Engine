# RCA Engine - User Journey Guide
## For Business Users (Finance Teams, Analysts, etc.)

## Overview

This guide is for **business users** who want to find out why numbers don't match between different data sources. You don't need to be a programmer or understand technical details. You just need to:

1. **Upload your CSV files** with primary keys
2. **Describe your business rules** (in plain English)
3. **Ask questions** about the differences

That's it! The system automatically figures out which tables belong to which system based on your question.

---

## Table of Contents

1. [Step 1: Prepare Your CSV Files](#step-1-prepare-your-csv-files)
2. [Step 2: Tell Us About Your Tables](#step-2-tell-us-about-your-tables)
3. [Step 3: Describe Your Business Rules](#step-3-describe-your-business-rules)
4. [Step 4: Ask Your Question](#step-4-ask-your-question)
5. [Step 5: Understand the Results](#step-5-understand-the-results)

---

## Step 1: Prepare Your CSV Files

### What Are CSV Files?

CSV files are Excel-like files that you can save from Excel, Google Sheets, or export from your database. They look like this:

```csv
customer_id,customer_name,total_outstanding,date
CUST001,John Smith,25000.00,2024-01-15
CUST002,Jane Doe,8000.00,2024-01-15
CUST003,Bob Wilson,45000.00,2024-01-15
```

### File Requirements

✅ **First row must be column names** (headers)
✅ **Use commas to separate columns**
✅ **Save as .csv format**
✅ **One file per table**

### Example Files

**File: `khatabook_customers.csv`**
```csv
customer_id,customer_name,total_outstanding,as_of_date
CUST001,John Smith,25000.00,2024-01-15
CUST002,Jane Doe,8000.00,2024-01-15
CUST003,Bob Wilson,45000.00,2024-01-15
```

**File: `tb_loan_details.csv`**
```csv
loan_id,customer_id,principal_amount,interest_amount,payment_amount,loan_status
L001,CUST001,10000.00,2000.00,2000.00,active
L002,CUST001,14000.00,3000.00,2000.00,active
L003,CUST002,7500.00,1500.00,1000.00,active
```

---

## Step 2: Tell Us About Your Tables

### What We Need When You Upload

For each CSV file you upload, provide:

1. **Primary Key(s)** (Required)
   - The column(s) that uniquely identify each row
   - Examples: `customer_id`, `loan_id`, `customer_id + date`

2. **Column Descriptions** (Optional, but recommended)
   - Brief description of what each column means
   - If not provided, the LLM will reason from column names
   - Better descriptions = Better results!

### Example: Uploading Tables

**Table: `khatabook_customers.csv`**
- **Primary Key**: `customer_id`
- **Column Descriptions** (optional):
  - `customer_id`: Unique customer identifier
  - `customer_name`: Customer's full name
  - `total_outstanding`: Total amount customer owes
  - `as_of_date`: Date of the record

**Table: `tb_loan_details.csv`**
- **Primary Key**: `loan_id`
- **Column Descriptions** (optional):
  - `loan_id`: Unique loan identifier
  - `customer_id`: Which customer this loan belongs to
  - `principal_amount`: Original loan amount
  - `interest_amount`: Interest charged on the loan
  - `payment_amount`: Amount paid by customer
  - `loan_status`: Current status (active, closed, etc.)

**Table: `tb_payments.csv`**
- **Primary Key**: `payment_id`
- **Column Descriptions**: (can skip if column names are clear)

### Important Notes

- **No need to specify "System A" or "System B"** - The system figures this out from your question!
- **Column descriptions are optional** - But they help the LLM understand your data better
- **The LLM will infer relationships** - Between tables based on common column names and your question

---

## Step 3: Describe Your Business Rules

### What Are Business Rules?

Business rules are **how you calculate your metric** (like Total Outstanding, Recovery, etc.) in plain English.

### Write Rules in Plain English

Instead of complex formulas, just tell us in simple language:

✅ **Good Examples**:
- "Sum of account balances plus transaction amounts minus writeoff amounts for active accounts only"
- "Total principal amount plus interest minus payments for each loan"
- "Sum of all payment amounts received for each customer"

### Example Business Rules

**For Khatabook**:
> "Total outstanding amount for each customer"

**For TB**:
> "Sum of principal amount plus interest amount minus payment amounts for each loan, grouped by customer"

### How to Write Good Business Rules

1. **Start with what you're calculating**: "Total outstanding", "Recovery amount", etc.
2. **List what you're adding**: "account balances", "transaction amounts"
3. **List what you're subtracting**: "writeoff amounts", "payments made"
4. **Add any conditions**: "for active accounts only", "for loans that are overdue"
5. **Specify grouping**: "for each customer", "grouped by loan"

---

## Step 4: Ask Your Question

### The Magic Happens Here!

Just ask your question in plain English, mentioning the data sources you want to compare:

✅ **Good Questions**:

```
TOS recon between khatabook and TB
```

```
Why is Total Outstanding different between khatabook and TB?
```

```
Recovery recon between payment_system and ledger
```

```
Why is disbursement different between core_banking and loan_management?
```

### What Happens Automatically

When you ask "TOS recon between khatabook and TB", the system automatically:

1. ✅ **Identifies System A**: All tables with "khatabook" in the name (khatabook_customers, khatabook_accounts, etc.) and their connections
2. ✅ **Identifies System B**: All tables with "TB" in the name (tb_loan_details, tb_payments, etc.) and their connections
3. ✅ **Figures out relationships**: Between tables based on common columns (e.g., customer_id links customers to loans)
4. ✅ **Handles grain differences**: Automatically aggregates if one system is at customer level and another at loan level
5. ✅ **Applies business rules**: Uses the rules you provided for each system
6. ✅ **Performs reconciliation**: Compares the results and identifies differences

### More Examples

**Example 1: Simple**
```
Disbursement recon between system_a and system_b
```
→ System automatically groups system_a_* tables vs system_b_* tables

**Example 2: Named Systems**
```
Why is recovery different between core_banking and payment_gateway?
```
→ System groups core_banking_* tables vs payment_gateway_* tables

**Example 3: With Filters**
```
TOS recon between khatabook and TB for Digital loans only
```
→ System applies filter "loan_type = 'Digital'" during reconciliation

---

## Step 5: Understand the Results

### What You'll Get Back

The system will tell you:

1. **How many entities match** (same values in both systems)
2. **How many entities differ** (different values)
3. **What's missing** (entities in one system but not the other)
4. **Why they're different** (root cause)

### Understanding the Results

#### Scenario 1: All Data Exists, But Values Are Different

**Result**:
```
Population Match: 5 common customers
Missing in TB: 0
Extra in TB: 0
Data Matches: 4
Data Mismatches: 1

Mismatch Details:
- CUST001: khatabook=13000, TB=12500 (Difference: 500)
```

**What This Means**:
- ✅ All 5 customers exist in both systems (no missing data)
- ✅ 4 customers have the same values (they match)
- ⚠️ 1 customer has different values (CUST001: 13000 vs 12500)

**Root Cause**: "Data Mismatch - Value Difference"

#### Scenario 2: Some Data Is Missing

**Result**:
```
Population Match: 4 common customers
Missing in TB: 1 (CUST003)
Extra in TB: 0
Data Matches: 3
Data Mismatches: 1
```

**What This Means**:
- ⚠️ 1 customer exists in khatabook but not in TB (missing data)
- ✅ 3 customers match perfectly
- ⚠️ 1 customer has different values

**Root Cause**: "Logic Mismatch - Missing Data"

#### Scenario 3: Different Levels of Detail (Grain Mismatch)

**Result**:
```
Khatabook Grain: customer_id (customer level)
TB Grain: loan_id (loan level)

System automatically aggregated TB loan data to customer level for comparison.

Population Match: 5 common customers
Data Matches: 4
Data Mismatches: 1
```

**What This Means**:
- Khatabook has data at customer level (one row per customer)
- TB has data at loan level (one row per loan)
- The system automatically aggregated loan data to customer level
- Then compared the results

**Root Cause**: "Logic Mismatch - Grain Difference (Auto-resolved)"

### Root Cause Types Explained

| Root Cause | What It Means | Example |
|------------|---------------|---------|
| **Value Difference** | Same entity exists in both systems, but values differ | CUST001: 13000 (khatabook) vs 12500 (TB) |
| **Missing Data** | Entity exists in one system but not the other | CUST003 exists in khatabook but not in TB |
| **Grain Difference** | Systems track at different levels (customer vs loan) - Auto-resolved | khatabook has customer totals, TB has individual loans |
| **Calculation Difference** | Different formulas used to calculate the metric | khatabook uses simple sum, TB uses weighted average |

---

## Real-World Examples

### Example 1: TOS Reconciliation

**Upload**:
1. `khatabook_customers.csv` (primary key: `customer_id`)
2. `tb_loan_details.csv` (primary key: `loan_id`)
3. `tb_payments.csv` (primary key: `payment_id`)

**Business Rules**:
- **Khatabook**: "Total outstanding amount for each customer"
- **TB**: "Sum of principal amount plus interest amount minus payment amounts for each loan, grouped by customer"

**Question**: 
```
TOS recon between khatabook and TB
```

**Result**:
- System automatically identifies khatabook tables as one system
- System automatically identifies TB tables as another system
- System detects grain mismatch (customer vs loan level)
- System aggregates TB data to customer level
- System compares and shows differences

### Example 2: Recovery Reconciliation

**Upload**:
1. `payment_system_transactions.csv` (primary key: `transaction_id`)
2. `ledger_entries.csv` (primary key: `entry_id`)

**Business Rules**:
- **Payment System**: "Sum of all payment amounts received for each customer"
- **Ledger**: "Total of principal recovered plus interest recovered for each customer"

**Question**: 
```
Recovery recon between payment_system and ledger
```

**Result**:
- System groups payment_system_* tables vs ledger_* tables
- Compares recovery amounts
- Identifies mismatches

---

## Summary

### The 5-Step Process

1. ✅ **Prepare CSV Files** - Export your data
2. ✅ **Upload with Metadata** - Specify primary keys, optionally describe columns
3. ✅ **Write Business Rules** - In plain English
4. ✅ **Ask Your Question** - Mention the systems to compare (e.g., "TOS recon between khatabook and TB")
5. ✅ **Get Results** - Understand matches, mismatches, and root causes

### What Makes This Simple

- ❌ **No need to label "System A" or "System B"** - The system figures it out from your question
- ❌ **No need to specify table relationships** - The LLM infers them from column names
- ❌ **No need to handle grain mismatches** - The system auto-aggregates as needed
- ❌ **No JSON, no programming, no SQL** - Just plain English

### What You Provide

1. CSV files
2. Primary keys (required)
3. Column descriptions (optional, but helpful)
4. Business rules (in plain English)
5. Your question (e.g., "TOS recon between khatabook and TB")

### What The System Does Automatically

1. ✅ Groups tables into systems based on your question
2. ✅ Infers table relationships from common columns
3. ✅ Handles grain mismatches (customer vs loan level)
4. ✅ Applies business rules
5. ✅ Compares data and identifies root causes

---

**Ready to get started? Upload your CSV files with primary keys and ask your question!** 🚀
