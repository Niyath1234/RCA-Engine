# RCA Engine - User Knowledge Guide

## Table of Contents
1. [Overview](#overview)
2. [Phase 4: LLM Formatter Contracts](#phase-4-llm-formatter-contracts)
3. [Complete User Journey](#complete-user-journey)
4. [Rule System](#rule-system)
5. [Task Grounding & Ambiguity Resolution](#task-grounding--ambiguity-resolution)
6. [Examples & Use Cases](#examples--use-cases)

---

## Overview

The RCA (Root Cause Analysis) Engine is an automated system that identifies and explains discrepancies between data systems. It transforms complex manual RCA processes into automated, intelligent investigations.

### Key Capabilities
- **Natural Language Queries**: Ask questions in plain English
- **Automated Analysis**: System handles SQL generation, execution, and comparison
- **Row-Level Precision**: Identifies exact rows causing discrepancies
- **Root Cause Attribution**: Explains why differences occur
- **Strict Validation**: Ensures data quality at every step

---

## Phase 4: LLM Formatter Contracts

### What Are Formatter Contracts?

Formatter contracts ensure that data sent to and received from the LLM is valid, consistent, and usable. They act as **guardrails** to prevent garbage data from entering or leaving the system.

### 4.1 Strict Input Contract

**Purpose**: Validates data before sending to LLM

**What It Checks**:
- ✅ All required fields are present
- ✅ All types match exactly (strings, numbers, arrays, objects)
- ✅ All constraints are satisfied (ranges, non-empty, etc.)
- ✅ Data consistency (delta = value_b - value_a, impact = abs(delta))
- ✅ No null or empty values where not allowed

**Example - Valid Input**:
```rust
FormatterInput {
    question: "Why are values different?",
    summary: {
        total_grain_units: 100,
        missing_left_count: 5,
        aggregate_difference: 1500.0
    },
    top_differences: [{
        grain_values: ["L001"],
        value_a: 1000.0,
        value_b: 1200.0,
        delta: 200.0,        // ✅ Must equal value_b - value_a
        impact: 200.0        // ✅ Must equal abs(delta)
    }],
    confidence: 0.85        // ✅ Must be 0.0-1.0
}
```

**Example - Invalid Input (Rejected)**:
```rust
{
    delta: 300.0,           // ❌ Should be 200.0 (1200 - 1000)
    impact: 150.0,          // ❌ Should be 200.0 (abs(200))
    confidence: 1.5         // ❌ Should be ≤ 1.0
}
// Validation fails with specific error messages
```

### 4.2 Strict Output Contract

**Purpose**: Validates LLM response before using it

**What It Checks**:
- ✅ All required fields are present
- ✅ All types match exactly
- ✅ Display format is valid enum
- ✅ Content length meets minimum requirements
- ✅ Format consistency (grain_focused requires non-empty grain units)
- ✅ No whitespace-only content

**Example - Valid Output**:
```rust
FormatterOutput {
    display_format: DisplayFormat::Summary,
    display_content: "Found 10 mismatches with aggregate difference of 1500.0...",  // ✅ ≥ 10 chars
    key_grain_units: [
        ["L001"],           // ✅ Non-empty arrays
        ["L002"]
    ],
    reasoning: Some("Summary format chosen...")
}
```

**Example - Invalid Output (Rejected)**:
```rust
{
    display_format: DisplayFormat::Narrative,
    display_content: "Short",  // ❌ Too short (< 50 chars for narrative)
    key_grain_units: [
        []                    // ❌ Empty grain unit
    ]
}
// Validation fails, falls back to template formatting
```

### Why Contracts Matter

**Without Contracts**:
- ❌ LLM might receive bad data → produces garbage
- ❌ LLM might return invalid format → system crashes
- ❌ No way to catch errors early

**With Contracts**:
- ✅ Bad input → caught before LLM call
- ✅ Bad output → caught after LLM call, fallback used
- ✅ Clear error messages → easier debugging
- ✅ System reliability → always produces valid results

---

## Complete User Journey

### Step-by-Step Flow

```
1. User Query
   "Why is recovery mismatching between system A and B for Digital loans on 2026-01-08?"
   ↓
2. Intent Compilation
   - Extracts: systems (A, B), metric (recovery), filters (Digital loans, date)
   - Generates: IntentSpec
   ↓
3. Task Grounding
   - Maps intent to concrete tables/columns
   - Finds rules in knowledge graph
   - Resolves grain and constraints
   ↓
4. RCA Analysis
   - Validates task
   - Builds execution plan
   - Executes queries on both systems
   - Computes grain-level differences
   - Calculates attributions
   ↓
5. Phase 4.1: Input Contract Validation
   - Validates all data before sending to LLM
   - Checks consistency (delta = value_b - value_a)
   - Ensures all constraints satisfied
   ↓
6. LLM Formatting
   - LLM formats results intelligently
   - Chooses display format (summary/table/narrative/grain_focused)
   - Generates human-readable content
   ↓
7. Phase 4.2: Output Contract Validation
   - Validates LLM response
   - Checks format consistency
   - Ensures content quality
   ↓
8. User Sees Results
   - Formatted, validated results
   - Or fallback template if LLM fails
```

### What User Experiences

**Success Case**:
1. User asks question
2. System analyzes data (~5-10 seconds)
3. System formats results intelligently
4. User gets clear, formatted answer

**Failure Case (Contracts Protect User)**:
1. User asks question
2. System analyzes data
3. Input validation fails → User sees error (not garbage)
4. OR Output validation fails → User gets template results (still usable)

**Key Benefits**:
- ✅ **Reliability**: Always gets valid results (never crashes)
- ✅ **Quality**: LLM formatting when possible, template fallback otherwise
- ✅ **Consistency**: Results follow predictable structure
- ✅ **Trust**: Validation ensures correctness

---

## Rule System

### Rule Definition - Simplified

Rules can be defined **minimally** - the system infers most details automatically.

#### Minimal Rule Definition

```json
{
  "id": "system_a_tos",
  "system": "system_a",
  "metric": "TOS",
  "target_entity": "loan",
  "target_grain": ["loan_id"],
  "computation": {
    "description": "Total Outstanding = principal + interest - payments * factor",
    "source_entities": ["loan", "interest", "payment", "fee"],  // ← Just entity names!
    "formula": "principal_amount + accrued_interest - paid_amount * adjustment_factor",
    "aggregation_grain": ["loan_id"]
  }
}
```

**That's it!** The system automatically:
- Finds tables for each entity
- Discovers join paths
- Maps column names
- Generates SQL

#### Natural Language Rules

You can even define rules in natural language:

**Input**:
```
"TOS is principal plus interest minus payments times adjustment factor for loans"
```

**System Auto-Generates**:
```json
{
  "id": "system_a_tos",
  "computation": {
    "formula": "principal_amount + accrued_interest - paid_amount * adjustment_factor",
    "source_entities": ["loan", "interest", "payment", "fee"]
  }
}
```

### What Gets Auto-Inferred

#### 1. Table Selection (Automatic)
```rust
source_entities: ["loan", "interest", "payment", "fee"]
    ↓
Metadata lookup:
- "loan" → finds: ["loan_summary", "loan_details"]
- "interest" → finds: ["interest_accrued", "interest_history"]
- "payment" → finds: ["payments", "payment_history"]
- "fee" → finds: ["fee_adjustments", "fee_details"]

// System picks best match based on:
// - Column names in formula
// - Table labels
// - Entity relationships
```

#### 2. Join Path Discovery (Automatic)
```rust
// System uses metadata to find join paths
Root: loan_summary (has loan_id)
    ↓
Finds relationships:
- loan_summary.loan_id → interest_accrued.loan_id ✓
- loan_summary.loan_id → payments.loan_id ✓
- loan_summary.loan_id → fee_adjustments.loan_id ✓

// Automatically builds joins:
LEFT JOIN interest_accrued ON loan_summary.loan_id = interest_accrued.loan_id
LEFT JOIN payments ON loan_summary.loan_id = payments.loan_id
LEFT JOIN fee_adjustments ON loan_summary.loan_id = fee_adjustments.loan_id
```

#### 3. Column Mapping (Automatic)
```rust
// Formula: "principal_amount + accrued_interest - paid_amount * adjustment_factor"
    ↓
System searches tables for matching columns:
- principal_amount → found in loan_summary ✓
- accrued_interest → found in interest_accrued ✓
- paid_amount → found in payments ✓
- adjustment_factor → found in fee_adjustments ✓

// Automatically maps to: loan_summary.principal_amount + ...
```

### When You Need More Detail

Only specify details when:
1. **Multiple tables match** → specify `source_table`
2. **Custom join conditions** → specify `joins` explicitly
3. **Complex filters** → specify `filter_conditions`
4. **Non-standard column names** → specify `attributes_needed` explicitly

**Example (when needed)**:
```json
{
  "computation": {
    "source_table": "loan_summary",  // ← Specify if ambiguous
    "filter_conditions": {            // ← Specify custom filters
      "status": "active",
      "loan_type": "digital"
    },
    "attributes_needed": {            // ← Specify if column names don't match
      "loan": ["principal_amount", "loan_id"],
      "interest": ["accrued_interest"]
    }
  }
}
```

---

## Task Grounding & Ambiguity Resolution

### Scenario: User Says "Exclude Writeoff Loans"

**Challenge**: System has 50+ tables, including `writeoff_users`, `loan_summary`, etc. Which table/column represents "writeoff loans"?

### How System Solves This

#### Step 1: Intent Compiler Extracts Constraint
```rust
IntentSpec {
    constraints: [
        FilterConstraint {
            intent: "exclude writeoff loans",  // ← User's intent
            column: None,                      // ← Not yet mapped!
        }
    ]
}
```

#### Step 2: Task Grounder Uses Fuzzy Matching + LLM Reasoning

**A. Fuzzy Matching Finds Candidates**:
```rust
Candidates = [
    {
        table: "writeoff_users",
        score: 0.95,  // High match - contains "writeoff"
        columns: ["user_id", "writeoff_date", "writeoff_amount"],
        labels: ["writeoff", "users"]
    },
    {
        table: "loan_summary", 
        score: 0.60,  // Medium match - has "status" column
        columns: ["loan_id", "status", "writeoff_flag"],
        labels: ["loans", "status"]
    }
]
```

**B. LLM Reasoning Selects Best Match**:
```
LLM Prompt: "User wants to 'exclude writeoff loans'

Available tables:
1. writeoff_users - Contains writeoff information for users
2. loan_summary - Contains loan information with writeoff_flag

Which table best represents 'writeoff loans'?"

LLM Reasoning:
"User wants to exclude writeoff loans. The loan_summary table has 
a writeoff_flag column which directly indicates if a loan is written off. 
This is more relevant for filtering loans.

Best match: loan_summary.writeoff_flag
Filter: WHERE writeoff_flag = false"
```

**C. System Selects Best Match**:
```rust
GroundedTask {
    filters: [
        Filter {
            table: "loan_summary",
            column: "writeoff_flag",
            operator: "=",
            value: "false"  // Exclude writeoffs
        }
    ],
    confidence: 0.85
}
```

#### Step 3: Validation Ensures Correctness
```rust
// Task Validator checks:
✓ Does "loan_summary" table exist? → Yes
✓ Does "writeoff_flag" column exist? → Yes  
✓ Is "writeoff_flag" boolean type? → Yes
✓ Can we filter on this column? → Yes
✓ Is filter reachable from grain? → Yes

// ✅ Validation passes
```

#### Step 4: Execution Applies Filter
```sql
-- System generates SQL with the filter
SELECT 
    loan_id,
    SUM(paid_amount) as recovery
FROM loan_summary ls
JOIN payments p ON ls.loan_id = p.loan_id
WHERE ls.writeoff_flag = false  -- ← User's constraint applied
  AND p.paid_date = '2026-01-08'
GROUP BY loan_id
```

### Edge Cases & Handling

#### Case 1: Multiple Writeoff-Related Tables
**If both exist**:
- `writeoff_users` (user-level writeoffs)
- `loan_summary.writeoff_flag` (loan-level writeoffs)

**LLM reasoning**: "User said 'writeoff loans' - this suggests loan-level filtering. `loan_summary.writeoff_flag` is more appropriate."

**System selects**: `loan_summary.writeoff_flag`

#### Case 2: Ambiguous Match
**If multiple tables score similarly**:
```rust
Candidates = [
    { table: "writeoff_users", score: 0.75 },
    { table: "loan_summary", score: 0.73 },
    { table: "writeoff_history", score: 0.70 }
]
```

**System asks LLM for clarification**:
```
LLM Prompt: "Multiple tables match 'writeoff loans'. 
Which is most relevant for filtering loans in recovery analysis?"

LLM Response: "loan_summary.writeoff_flag - it's directly on the loan entity"
```

#### Case 3: No Direct Match Found
**If no table contains "writeoff"**:
```rust
Candidates = []  // No matches
```

**System uses LLM to infer**:
```
LLM Prompt: "User wants to exclude writeoff loans, but no 'writeoff' 
table found. Available tables: loan_summary, loan_status, ..."

LLM Response: "Check loan_summary.status column - writeoff loans 
might have status = 'WRITTEN_OFF' or similar"
```

**System tries**:
```rust
// Check loan_summary.status for writeoff-related values
SELECT DISTINCT status FROM loan_summary
// Finds: ["ACTIVE", "CLOSED", "WRITTEN_OFF", "DEFAULTED"]

// LLM confirms: "WRITTEN_OFF" matches user's intent
Filter: { column: "status", operator: "!=", value: "WRITTEN_OFF" }
```

---

## Examples & Use Cases

### Example 1: Simple Recovery Reconciliation

**User Query**:
```
"Why is recovery mismatching between system A and B for Digital loans on 2026-01-08?"
```

**System Processing**:
1. Intent Compiler: Extracts systems, metric, filters, date
2. Task Grounder: Finds `payments` table, `recovery` rule
3. Execution: Runs queries on both systems
4. Comparison: Finds 50 loans with differences
5. Attribution: Traces to source rows
6. Formatting: LLM formats results

**User Sees**:
```
RCA Analysis Results

Analysis found 50 loans missing in System B and 20 value mismatches, 
causing an aggregate difference of $50,000.

Top discrepancies:
• Loan L001: Difference of $200
• Loan L002: Difference of $100

Confidence: 85%
```

### Example 2: Complex TOS Calculation

**Rule Definition**:
```json
{
  "metric": "TOS",
  "formula": "principal_amount + accrued_interest - paid_amount * adjustment_factor",
  "source_entities": ["loan", "interest", "payment", "fee"]
}
```

**System Auto-Generates**:
```sql
SELECT 
    ls.loan_id,
    SUM(
        ls.principal_amount + 
        ia.accrued_interest - 
        p.paid_amount * 
        fa.adjustment_factor
    ) as TOS
FROM loan_summary ls
LEFT JOIN interest_accrued ia ON ls.loan_id = ia.loan_id
LEFT JOIN payments p ON ls.loan_id = p.loan_id
LEFT JOIN fee_adjustments fa ON ls.loan_id = fa.loan_id
GROUP BY ls.loan_id
```

**Attribution Shows**:
```
TOS difference of 200.0 caused by:
- Interest accrued: +100.0
- Fee adjustment: +80.0
- Principal: +50.0
- Payments: -30.0
```

### Example 3: Filter with Ambiguity

**User Query**:
```
"Why is recovery different? Exclude writeoff loans."
```

**System Processing**:
1. Finds multiple writeoff-related tables
2. Uses LLM reasoning to select `loan_summary.writeoff_flag`
3. Applies filter: `WHERE writeoff_flag = false`
4. Continues with filtered analysis

**Result**: Analysis excludes writeoff loans automatically

---

## Key Takeaways

### 1. Phase 4 Contracts
- **Input Contract**: Validates data before LLM (catches bad data early)
- **Output Contract**: Validates LLM response (ensures quality output)
- **Fallback**: Template formatting if LLM fails
- **Result**: Always reliable, never crashes

### 2. Rule System
- **Minimal Definition**: Just formula + entities
- **Auto-Inference**: System handles tables, joins, columns
- **Natural Language**: Can define rules in plain English
- **Flexibility**: Add details only when needed

### 3. Task Grounding
- **Fuzzy Matching**: Finds candidate tables/columns
- **LLM Reasoning**: Selects best match intelligently
- **Validation**: Ensures correctness before execution
- **Graceful Degradation**: Tries alternatives if first match fails

### 4. User Experience
- **Simple Queries**: Ask in natural language
- **Fast Results**: 5-10 seconds typically
- **Reliable**: Contracts ensure quality
- **Informative**: Clear explanations of differences

---

## Technical Details

### Validation Flow

```
RCAResult → build_input_contract() → validate_input() ✅ → LLM
                                                              ↓
FormattedDisplayResult ← build_formatted_display() ← validate_output() ✅
```

### Rule Compilation Flow

```
Rule Definition → Rule Compiler → Table Selection → Join Discovery → SQL Generation
```

### Task Grounding Flow

```
User Intent → Fuzzy Matching → LLM Reasoning → Validation → Grounded Task
```

---

## Best Practices

### Rule Definition
1. **Start Simple**: Define minimal rule first
2. **Add Details Only When Needed**: System infers most things
3. **Use Natural Language**: Easier to understand and maintain
4. **Test Incrementally**: Verify rule works before adding complexity

### Query Formulation
1. **Be Specific**: Include filters, dates, grain when known
2. **Use Business Terms**: System understands domain language
3. **Trust the System**: It handles ambiguity resolution
4. **Review Results**: Check attribution for insights

### Contract Validation
1. **Trust the Contracts**: They catch errors automatically
2. **Check Error Messages**: They're specific and helpful
3. **Use Fallback**: Template formatting is always available
4. **Monitor Confidence**: Low confidence suggests data quality issues

---

## Troubleshooting

### Common Issues

#### Issue: Rule Not Found
**Solution**: Check rule exists in knowledge graph, verify system/metric names match

#### Issue: Validation Fails
**Solution**: Check error message - it's specific about what's wrong. Fix data consistency or add missing fields.

#### Issue: Ambiguous Table Selection
**Solution**: System will ask for clarification or use LLM reasoning. You can also specify `source_table` explicitly.

#### Issue: Join Path Not Found
**Solution**: Check metadata has relationship definitions. System needs primary/foreign key relationships.

#### Issue: Column Not Found
**Solution**: Check column names match. System uses fuzzy matching but exact match is preferred.

---

## Root Cause Identification & Precision

### UUID-Level Precision

The RCA Engine provides **row-level precision** by identifying exact rows (UUIDs) causing discrepancies. This enables pinpoint accuracy in root cause analysis.

#### How It Works

**Example Scenario**: Recovery reconciliation for Digital loans
- System A: 80 Digital loan payments
- System B: 60 Digital loan payments
- Expected differences: 30 missing in B, 10 missing in A, 10 value mismatches

**System Output**:
```
📊 Top Differences (UUID-level):
   1. UUID: uuid_a_001, System A: 1110.00, System B: 1210.00, Delta: 100.00
   2. UUID: uuid_a_002, System A: 1120.00, System B: 1220.00, Delta: 100.00
   3. UUID: uuid_a_003, System A: 1130.00, System B: 1230.00, Delta: 100.00
   ...
```

**Key Capabilities**:
- ✅ **Exact UUID Identification**: Each difference shows the specific UUID causing the mismatch
- ✅ **Value Comparison**: Shows exact values from both systems side-by-side
- ✅ **Delta Calculation**: Computes precise difference for each row
- ✅ **Impact Sorting**: Differences sorted by impact (largest first)

### Root Cause Attribution

The system not only identifies **which** rows differ, but also **why** they differ through attribution analysis.

#### Attribution Components

**1. Missing Row Detection**:
```
✅ Identified missing rows in both systems
✅ Pinpointed exact UUIDs causing differences
```

**2. Value Mismatch Detection**:
```
✅ Identified value mismatches
✅ Aggregate difference calculated: 5000.00
```

**3. Source Row Attribution**:
```
🔍 Root Cause Analysis:
   - Attributions computed: 10
   - Top attributions:
     1. Grain: ["uuid_a_001"], Impact: 100.00, Contributors: 2
        - Table: payments_a, Row: ["uuid_a_001"], Contribution: 50.00
        - Table: payments_b, Row: ["uuid_b_001"], Contribution: 50.00
```

### Test Results: Real-World Workflow

**Test Scenario**: `test_real_world_with_actual_execution`

**Query**: "Why is recovery mismatching between system A and B for Digital loans on 2026-01-08?"

**System Capabilities Verified**:
- ✅ **Task Validation**: Validates task structure and metadata
- ✅ **Data Execution**: Executes queries on both systems
- ✅ **Grain-Level Diff**: Computes differences at UUID level
- ✅ **Attribution**: Traces differences to source rows
- ✅ **Root Cause Identification**: Identifies exact UUIDs causing differences
- ✅ **Missing Row Detection**: Finds rows missing in either system
- ✅ **Value Mismatch Detection**: Identifies rows with different values
- ✅ **Aggregate Calculation**: Computes total difference across all mismatches
- ✅ **Formatter Integration**: Formats results with Phase 4 contracts

**Output Example**:
```
✅ Complete workflow executed successfully!
   - Grain: payment
   - Total grain units: 90
   - Missing in A: 10
   - Missing in B: 30
   - Mismatches: 10
   - Aggregate difference: 5000.00
   - Confidence: 0.85

🎯 Root Cause Identification Status:
   ✅ Identified missing rows in both systems
   ✅ Pinpointed exact UUIDs causing differences
   ✅ Generated attributions for differences
   ✅ Identified value mismatches
   ✅ Aggregate difference calculated: 5000.00
```

### Technical Implementation

#### UUID Handling

The system properly handles UUID extraction from grain values:

```rust
// Correct UUID extraction (handles Option<String>)
let uuid = diff.grain_value.first().map(|s| s.as_str()).unwrap_or("unknown");
```

**Key Points**:
- ✅ Handles `Option<String>` safely
- ✅ Converts to `&str` for display
- ✅ Provides fallback for missing values
- ✅ Works with any grain type (not just UUIDs)

#### Difference Computation

```rust
GrainDifference {
    grain_value: vec!["uuid_a_001".to_string()],  // Exact UUID
    value_a: 1110.0,                               // System A value
    value_b: 1210.0,                               // System B value
    delta: 100.0,                                  // Difference (B - A)
    impact: 100.0,                                 // Absolute impact
}
```

### Use Cases

#### Use Case 1: Payment Reconciliation
**Query**: "Why are payments different between systems?"
**Result**: Exact UUIDs of payments with differences, with values and deltas

#### Use Case 2: Loan Balance Reconciliation
**Query**: "Why is TOS mismatching for Digital loans?"
**Result**: Specific loan IDs with balance differences, attributed to source transactions

#### Use Case 3: Missing Data Detection
**Query**: "What payments are missing in System B?"
**Result**: Complete list of UUIDs missing in System B, with their values from System A

### Benefits

**For Analysts**:
- ✅ **No Manual Investigation**: System identifies exact rows automatically
- ✅ **Time Savings**: No need to write SQL or compare datasets manually
- ✅ **Confidence**: Know exactly which rows cause differences

**For Debugging**:
- ✅ **Precise Targeting**: Fix specific rows, not entire datasets
- ✅ **Attribution**: Understand why differences occur
- ✅ **Traceability**: Track differences back to source data

**For Reporting**:
- ✅ **Clear Results**: UUID-level precision in reports
- ✅ **Actionable**: Know exactly what to investigate
- ✅ **Comprehensive**: Covers missing rows, value mismatches, and aggregates

---

## Summary

The RCA Engine provides:
- ✅ **Automated RCA**: No manual SQL writing
- ✅ **Row-Level Precision**: Exact rows causing differences
- ✅ **Intelligent Formatting**: LLM-enhanced results
- ✅ **Strict Validation**: Contracts ensure quality
- ✅ **Simple Rules**: Minimal definition, maximum automation
- ✅ **Ambiguity Resolution**: Handles unclear user queries
- ✅ **Reliable Results**: Always produces valid output

**The system transforms complex manual RCA into simple, automated, reliable analysis.**

