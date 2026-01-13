# RCA Engine - Complete System Summary

## Executive Summary

The RCA (Root Cause Analysis) Engine is a comprehensive, production-ready system designed to automatically identify and explain discrepancies between data systems. It solves the critical problem of **reconciling metrics across multiple systems** by providing row-level analysis, lineage tracing, and AI-powered root cause attribution.

### Natural Language to Root Cause: The Complete Transformation

**From:** Writing complex 300-line SQL queries  
**To:** Asking simple 1-2 line natural language questions

**Example Transformation:**

**User Query (Simple):**
```
"Why is recovery mismatching between system A and B for Digital loans on 2026-01-08?"
```

**System Internally:**
1. **Infers:** Metric = recovery (paid_amount), Systems = A & B, Date = 2026-01-08, Grain = payment-level, Grouping = bucket
2. **Translates:** Complex SQL logic (FTD, CM, LMTD, LM date constraints, bucket grouping)
3. **Executes:** Small SQL probes (LIMIT 100) to explore the knowledge graph
4. **Stores:** All findings incrementally (like an explorer)
5. **Diffs:** Row-level data to find exact UUIDs causing mismatch
6. **Explains:** Why each UUID differs (join failures, filter issues, rule differences)

**Result:** Exact UUIDs causing the mismatch with complete explanations, without writing any SQL.

### Key Problem Solved

**Traditional RCA Challenges:**
- Manual investigation of discrepancies is time-consuming and error-prone
- Aggregate-level comparisons don't reveal root causes
- Lack of visibility into data transformation pipelines
- Difficulty tracing where discrepancies originate
- No systematic way to verify reconciliation correctness

**RCA Engine Solution:**
- **Automated row-level analysis** - Identifies exact rows causing discrepancies
- **Complete lineage tracing** - Tracks joins, filters, and rule transformations
- **AI-powered attribution** - Uses LLM reasoning to explain root causes
- **Deterministic verification** - Proves aggregate mismatches match row-level differences
- **Performance optimized** - Handles large-scale datasets efficiently
- **Trust layer** - Provides auditability and reproducibility

---

## System Architecture

### High-Level Overview

The RCA Engine supports **two complementary approaches**:

1. **Fixed Pipeline Approach (RCA Cursor)** - Comprehensive, deterministic analysis
2. **Dynamic Graph Traversal Approach (Graph Traversal Agent)** - Adaptive, intelligent exploration

```
┌─────────────────────────────────────────────────────────────┐
│                    User Query/Problem                        │
│         "Why are TOS values different between systems?"      │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Intent Compiler (LLM-Powered)                   │
│  - Parses natural language query                            │
│  - Extracts systems, metrics, constraints                   │
│  - Generates IntentSpec                                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Task Grounder                                  │
│  - Maps intent to concrete tables/columns                   │
│  - Selects optimal rules using LLM reasoning                │
│  - Resolves grain and constraints                           │
│  - Builds initial knowledge graph                           │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────────────┐   ┌──────────────────────────────┐
│  APPROACH 1:          │   │  APPROACH 2:                  │
│  Fixed Pipeline       │   │  Dynamic Graph Traversal      │
│  (RCA Cursor)         │   │  (Graph Traversal Agent)      │
│                       │   │                               │
│  ┌─────────────────┐ │   │  ┌──────────────────────────┐  │
│  │ Phase 1:        │ │   │  │ Build Knowledge Graph   │  │
│  │ Normalize       │ │   │  │ - Tables, Rules, Joins  │  │
│  │ Metrics         │ │   │  │ - Filters, Metrics      │  │
│  └────────┬────────┘ │   │  │ - Rich Metadata         │  │
│           │          │   │  └───────────┬──────────────┘  │
│           ▼          │   │              │                 │
│  ┌─────────────────┐ │   │              ▼                 │
│  │ Phase 2:        │ │   │  ┌──────────────────────────┐  │
│  │ Materialize     │ │   │  │ Traverse → Test →        │  │
│  │ Rows            │ │   │  │ Observe → Decide        │  │
│  └────────┬────────┘ │   │  │                          │  │
│           │          │   │  │ 1. Choose Next Node     │  │
│           ▼          │   │  │    (LLM-guided)         │  │
│  ┌─────────────────┐ │   │  │ 2. Run SQL Probe        │  │
│  │ Phase 3:        │ │   │  │    (Small, focused)    │  │
│  │ Canonicalize    │ │   │  │ 3. Observe Result       │  │
│  └────────┬────────┘ │   │  │    (Interpret findings) │  │
│           │          │   │  │ 4. Decide Next Step     │  │
│           ▼          │   │  │    (Adaptive)           │  │
│  ┌─────────────────┐ │   │  │ 5. Repeat Until Found  │  │
│  │ Phase 4:        │ │   │  └───────────┬──────────────┘  │
│  │ Row Diff        │ │   │              │                 │
│  └────────┬────────┘ │   │              ▼                 │
│           │          │   │  ┌──────────────────────────┐  │
│           ▼          │   │  │ Root Cause Found         │  │
│  ┌─────────────────┐ │   │  │ - Findings Recorded     │  │
│  │ Phase 5:        │ │   │  │ - Hypothesis Formed     │  │
│  │ Lineage Trace   │ │   │  │ - Early Termination     │  │
│  └────────┬────────┘ │   │  └───────────┬──────────────┘  │
│           │          │   │              │                 │
│           ▼          │   │              │                 │
│  ┌─────────────────┐ │   │              │                 │
│  │ Phase 6:        │ │   │              │                 │
│  │ Attribution     │ │   │              │                 │
│  └────────┬────────┘ │   │              │                 │
│           │          │   │              │                 │
│           ▼          │   │              │                 │
│  ┌─────────────────┐ │   │              │                 │
│  │ Phase 7:        │ │   │              │                 │
│  │ Narrative       │ │   │              │                 │
│  └────────┬────────┘ │   │              │                 │
│           │          │   │              │                 │
│           ▼          │   │              │                 │
│  ┌─────────────────┐ │   │              │                 │
│  │ Phase 8:        │ │   │              │                 │
│  │ Reconciliation  │ │   │              │                 │
│  └────────┬────────┘ │   │              │                 │
│           │          │   │              │                 │
└───────────┼──────────┘   └──────────────┼─────────────────┘
            │                              │
            └──────────────┬───────────────┘
                           ▼
        ┌──────────────────────────────────────┐
        │         Final RCA Result              │
        │  - Row-level differences              │
        │  - Root cause explanations            │
        │  - Human-readable narratives          │
        │  - Verification proof                │
        └──────────────────────────────────────┘
```

### Architecture Modes

**Mode 1: Fixed Pipeline (RCA Cursor)**
- **Use Case:** Comprehensive, deterministic analysis
- **When:** Deep/Forensic mode, full reconciliation
- **Characteristics:** Always runs all phases, complete analysis

**Mode 2: Dynamic Graph Traversal (Graph Traversal Agent)**
- **Use Case:** Fast, adaptive investigation
- **When:** Quick root cause finding, targeted exploration
- **Characteristics:** Dynamic node selection, early termination, adaptive paths

**Mode 3: Hybrid Approach**
- **Use Case:** Progressive escalation
- **When:** Start fast, escalate if needed
- **Characteristics:** Start with traversal, escalate to pipeline

---

## Core Components

### 0. Knowledge Graph & Hypergraph

**Purpose:** Represents the complete data transformation graph with rich metadata.

**Components:**

**a) Hypergraph:**
- Advanced graph structure with node/edge statistics
- Fragment metadata (column fragments, value distributions)
- Path finding and adjacency queries
- Data quality scores and selectivities

**b) Node Types:**
- **Table Nodes:** Base data tables with column metadata
- **Rule Nodes:** Business rule calculations with formulas
- **Join Nodes:** Join relationships with keys and types
- **Filter Nodes:** Filter conditions and expressions
- **Metric Nodes:** Final metric calculations

**c) Rich Metadata:**
- Table: columns, types, descriptions, labels, grain, attributes
- Rule: formulas, source entities, filter conditions, descriptions
- Join: join keys, join types, table relationships
- Metric: descriptions, grain, precision, units
- Hypergraph Stats: row counts, distinct counts, data quality scores, selectivities

**Key Innovation:**
- **LLM receives full context** at each node
- **Metadata guides decisions** (which columns exist, join feasibility, data quality)
- **Hypergraph stats inform** probe expectations and result interpretation

### 1. Intent Compiler

**Purpose:** Converts natural language queries into structured intent specifications.

**How it works:**
- Uses LLM to parse user queries
- Extracts systems, metrics, entities, constraints
- Handles ambiguity resolution
- Generates `IntentSpec` with task type (RCA or Data Validation)

**Example:**
```
Query: "Why is System A TOS different from System B TOS for loan L001?"
↓
IntentSpec {
    task_type: RCA,
    systems: ["system_a", "system_b"],
    target_metrics: ["tos"],
    entities: ["loan"],
    grain: ["loan_id"],
    constraints: [loan_id = "L001"]
}
```

### 2. Task Grounder

**Purpose:** Maps abstract intent to concrete database tables, columns, and rules.

**How it works:**
- Uses fuzzy matching to find relevant tables
- Employs LLM reasoning for optimal table/rule selection
- Considers labels, grain, and entity relationships
- Scores and ranks candidate tables

**Key Features:**
- **LLM-guided selection:** Uses comprehensive reasoning about table relevance
- **Label-based guidance:** Matches task labels with table/rule labels
- **Rule reasoning:** Selects best rules using chain-of-thought reasoning
- **Column inference:** Identifies relevant columns for metrics

### 3. Metric Normalization

**Purpose:** Converts business rules into standardized metric definitions.

**How it works:**
- Parses rule formulas and aggregations
- Extracts base tables, joins, filters
- Normalizes aggregation types (sum, count, avg, etc.)
- Creates `MetricDefinition` for consistent processing

**Benefits:**
- Standardizes different rule formats
- Enables comparison across systems
- Simplifies downstream processing

### 4. Row Materialization Engine

**Purpose:** Extracts row-level data from both systems at canonical grain.

**How it works:**
- Builds queries from `MetricDefinition`
- Applies joins, filters, transformations
- Converts aggregation queries to pre-aggregation form
- Outputs individual rows instead of aggregates

**Key Innovation:**
- **Pre-aggregation extraction:** Gets row-level data before aggregation
- **Canonical grain:** Ensures both systems output at same grain level
- **Transformation tracking:** Records all transformations for lineage

### 5. Canonical Mapping Layer

**Purpose:** Maps dataframes from different systems to common format.

**How it works:**
- Infers column mappings automatically
- Renames columns to canonical names
- Normalizes data types
- Handles missing columns gracefully

**Canonical Entity Schema:**
```rust
CanonicalEntity {
    keys: Vec<String>,           // e.g., ["loan_id"]
    value_columns: Vec<String>,   // e.g., ["paid_amount"]
    attributes: Vec<String>,      // e.g., ["customer_id", "product_type"]
}
```

### 6. Row Diff Engine

**Purpose:** Identifies exact differences between two canonical dataframes.

**How it works:**
- Compares rows by key (e.g., loan_id)
- Categorizes differences:
  - `missing_left`: Rows only in left system
  - `missing_right`: Rows only in right system
  - `value_mismatch`: Rows with different values
  - `matches`: Rows that match exactly
- Calculates precision-aware numeric differences

**Output:**
```rust
RowDiffResult {
    missing_left: DataFrame,      // Rows only in System A
    missing_right: DataFrame,      // Rows only in System B
    value_mismatch: DataFrame,    // Rows with different values
    matches: DataFrame,            // Matching rows
    summary: DiffSummary { ... }
}
```

### 7. Lineage Tracing

**Purpose:** Tracks how each row was processed through the pipeline.

**Components:**

**a) Join Tracing:**
- Records which joins succeeded/failed per row
- Tracks join conditions and types
- Identifies missing join matches

**b) Filter Tracing:**
- Records filter decisions (pass/fail) per row
- Captures filter expressions and values
- Explains why rows were filtered out

**c) Rule Tracing:**
- Tracks rule execution per row
- Records input/output values
- Identifies which rules fired

**Why it matters:**
- Provides complete audit trail
- Enables precise root cause identification
- Explains data transformations

### 8. Root Cause Attribution Engine

**Purpose:** Combines lineage traces to explain why rows differ.

**How it works:**
- Analyzes join traces for missing matches
- Examines filter traces for dropped rows
- Reviews rule traces for transformations
- Combines evidence into structured explanations

**Output:**
```rust
RowExplanation {
    row_id: Vec<String>,
    difference_type: MissingInRight | MissingInLeft | ValueMismatch,
    explanations: Vec<ExplanationItem> {
        source: Join | Filter | Rule | DataQuality,
        explanation: "Row dropped due to failed join: ...",
        evidence: { ... }
    },
    confidence: 0.0-1.0
}
```

### 9. Narrative Builder

**Purpose:** Converts structured explanations into human-readable narratives.

**How it works:**
- Template-based generation (always available)
- LLM-enhanced narratives (optional)
- Provides summary, details, and recommendations

**Example Output:**
```
Row [L001]: Missing in System B

Summary: Row L001 exists in System A but not in System B.

Details:
1. Row dropped due to failed join: loan_summary -> customer_loan_mapping 
   on customer_id. Customer C123 not found in System B.
2. Filter condition not met: status = 'active'. Row has status = 'closed'.

Recommendations:
- Check if customer C123 exists in System B
- Verify status field mapping between systems
- Review join logic for customer_loan_mapping table
```

### 10. Aggregate Reconciliation Engine

**Purpose:** Proves that row-level differences explain aggregate mismatch.

**How it works:**
- Calculates: `sum(missing_left) - sum(missing_right) + sum(mismatch_diff)`
- Compares with reported aggregate mismatch
- Verifies within precision tolerance
- Provides breakdown by category

**Mathematical Proof:**
```
reported_mismatch = sum(System A) - sum(System B)

calculated_mismatch = 
    sum(missing_left values) -           // Rows only in A
    sum(missing_right values) +          // Rows only in B (subtract)
    sum(value_mismatch differences)      // Value differences

Verification: |reported_mismatch - calculated_mismatch| < tolerance
```

**Why it matters:**
- **Trust:** Proves the analysis is correct
- **Completeness:** Ensures no discrepancies were missed
- **Auditability:** Provides mathematical proof

### 11. SQL Engine Tool Module

**Purpose:** Executes small SQL probe queries for dynamic graph traversal.

**How it works:**
- Executes SQL queries against tables (CSV/Parquet)
- Returns probe results with sample rows, statistics, warnings
- Supports table probes, join probes, filter probes, rule probes
- Fast iteration with LIMIT 100 for quick exploration

**Key Features:**
- **Small probes:** Fast, focused queries (LIMIT 100)
- **Result analysis:** Row counts, null counts, value ranges
- **Join testing:** Tests join feasibility and finds failures
- **Filter testing:** Tests filter conditions and impact

**Example:**
```rust
let sql_engine = SqlEngine::new(metadata, data_dir);
let result = sql_engine.execute_probe(
    "SELECT * FROM payments WHERE paid_date = '2026-01-08' LIMIT 100",
    Some(100)
).await?;
```

### 12. Graph Traversal Agent

**Purpose:** Dynamically navigates knowledge graph to find root causes.

**Core Pattern: Traverse → Test → Observe → Decide → Repeat**

**How it works:**
1. **Traverse:** Choose next best node to visit (LLM-guided)
2. **Test:** Run small SQL probe at that node
3. **Observe:** Analyze probe result (row count, nulls, join failures, etc.)
4. **Decide:** Determine next step based on observations
5. **Repeat:** Continue until root cause found

**Node Selection:**
- **Relevance to findings:** Missing rows → probe joins
- **Information gain:** Which node eliminates most possibilities?
- **Proximity:** Nodes connected to visited nodes
- **LLM reasoning:** Uses system prompts for intelligent selection

**Key Features:**
- **Dynamic paths:** Not fixed pipeline, adapts to findings
- **Early termination:** Stops when root cause found
- **Rich metadata:** LLM gets full context (columns, rules, stats)
- **Small probes:** Fast iteration, focused queries

**Example Flow:**
```
1. Probe base rows: System A has 1000 rows, System B has 950 rows
   → Observation: Missing 50 rows
   → Decision: Probe joins

2. Probe join: payments_a LEFT JOIN orders ON order_id
   → Observation: 50 rows where join failed
   → Decision: ROOT CAUSE FOUND - Join failure
   → Stop
```

### 13. Agent System Prompts

**Purpose:** Guides LLM to make intelligent decisions during graph traversal.

**Components:**

**a) System Prompt (`SYSTEM_PROMPT.md`):**
- Core philosophy: Traverse → Test → Observe → Decide → Repeat
- Node types and their purposes
- Decision-making framework
- Result interpretation guidelines
- Common patterns and best practices

**b) Prompt Builders (`agent_prompts.rs`):**
- `build_node_selection_prompt()` - Choose next node
- `build_result_interpretation_prompt()` - Understand probe results
- `build_sql_generation_prompt()` - Generate optimal SQL
- `build_hypothesis_prompt()` - Form hypotheses

**c) Rich Context:**
- Full node metadata (tables, rules, joins, metrics)
- Hypergraph statistics (row counts, selectivities, data quality)
- Current findings and visited path
- Problem description and constraints

**Key Innovation:**
- **LLM receives full context** at each decision point
- **Structured responses** with reasoning and confidence
- **Pattern-based guidance** (missing rows → probe joins)
- **Adaptive decision making** based on observations

**Purpose:** Proves that row-level differences explain aggregate mismatch.

**How it works:**
- Calculates: `sum(missing_left) - sum(missing_right) + sum(mismatch_diff)`
- Compares with reported aggregate mismatch
- Verifies within precision tolerance
- Provides breakdown by category

**Mathematical Proof:**
```
reported_mismatch = sum(System A) - sum(System B)

calculated_mismatch = 
    sum(missing_left values) -           // Rows only in A
    sum(missing_right values) +          // Rows only in B (subtract)
    sum(value_mismatch differences)      // Value differences

Verification: |reported_mismatch - calculated_mismatch| < tolerance
```

**Why it matters:**
- **Trust:** Proves the analysis is correct
- **Completeness:** Ensures no discrepancies were missed
- **Auditability:** Provides mathematical proof

---

## Advanced Features

### Dynamic Graph Traversal

**Purpose:** Adaptive, intelligent root cause investigation.

**Key Innovation:**
- **Not fixed pipeline:** Chooses next step based on observations
- **Small SQL probes:** Fast iteration with focused queries
- **Early termination:** Stops when root cause found
- **Rich metadata:** LLM gets full context at each node

**Traversal Patterns:**

**Pattern 1: Missing Rows Investigation**
```
Finding: System A has 1000 rows, System B has 950 rows
→ Next: Probe joins (most likely cause)
→ Then: Probe filters (if joins pass)
→ Then: Probe base tables (if filters pass)
```

**Pattern 2: Value Mismatch Investigation**
```
Finding: Same rows exist but values differ
→ Next: Probe rules (calculation differences)
→ Then: Probe formulas/transformations
→ Then: Probe source data quality
```

**Pattern 3: Join Failure Investigation**
```
Finding: Join returns 0 rows or many NULLs
→ Next: Probe left table (check if source data exists)
→ Then: Probe right table (check if target data exists)
→ Then: Probe join keys (check key values match)
```

**Benefits:**
- **Faster:** Only explores relevant paths
- **Smarter:** Adapts to findings dynamically
- **Efficient:** Small probes, early termination
- **Explainable:** Records reasoning at each step

### Hypergraph Integration

**Purpose:** Leverage advanced graph features for intelligent traversal.

**Features:**

**1. Node Statistics:**
- Row counts, distinct counts
- Data quality scores
- Null percentages
- Value distributions

**2. Edge Statistics:**
- Join selectivity (how many rows match)
- Filter selectivity (how many rows pass filter)
- Query performance metrics

**3. Fragment Metadata:**
- Column fragments (value ranges)
- Value distributions
- Top-N values

**4. Path Finding:**
- Optimized join path discovery
- Shortest path algorithms
- Related table discovery

**Benefits:**
- **Informed decisions:** Know data quality before probing
- **Better SQL:** Know join feasibility, expected row counts
- **Faster exploration:** Use statistics to prioritize nodes
- **Quality insights:** Understand data quality issues early

### LLM Strategy Layer (Phase 7)

**Purpose:** Uses AI to guide metric selection and drilldown strategies.

**Capabilities:**

1. **Metric Strategy Selection:**
   - Analyzes problem description
   - Selects optimal metric and rules
   - Considers rule complexity and data availability
   - Provides confidence scores

2. **Drilldown Strategy Generation:**
   - Suggests dimensions for investigation
   - Prioritizes drilldown paths
   - Explains expected insights

3. **Investigation Path Suggestions:**
   - Proposes multiple investigation paths
   - Prioritizes by likelihood of finding root cause
   - Provides step-by-step guidance

### Performance Optimizations (Phase 8)

**1. Chunked Extraction:**
- Processes large datasets in chunks
- Reduces memory footprint
- Enables streaming processing

**2. Sampling Strategies:**
- Random sampling for quick analysis
- Stratified sampling for representative subsets
- Top-N sampling for high-value rows
- Systematic sampling for even distribution

**3. Hash-Based Diff:**
- Fast comparison using row hashes
- Reduces computation for large datasets
- Returns key-level differences efficiently

**4. Pushdown Predicates:**
- Pushes filters to data source
- Reduces data transfer
- Optimizes query execution

### Trust Layer (Phase 8)

**1. Evidence Storage:**
- Stores complete execution records
- Tracks inputs, outputs, intermediates
- Enables auditability

**2. Deterministic Replay:**
- Replays executions from evidence
- Verifies outputs match original
- Enables reproducibility

**3. Verification Engine:**
- Aggregate reconciliation proof
- Consistency checks
- Integrity validation

---

## Model Reasoning and Conclusion Process

This section explains the complete step-by-step process of how the RCA Engine's reasoning system works, from initial problem understanding to final root cause conclusion.

### Overview: The Explorer Pattern

The RCA Engine uses an **explorer-like reasoning process** where the LLM:

1. **Writes Real Short Queries** ✅
   - Generates focused SQL probes (LIMIT 100)
   - Fast iteration with small, targeted queries
   - Each query tests a specific hypothesis

2. **Stores Gained Information Like an Explorer** ✅
   - **`visited_path`**: Tracks all nodes explored (like explorer's route map)
   - **`findings`**: Stores all discoveries with full evidence (like explorer's field notes)
   - **`last_probe_result`**: Each node remembers its probe result (like observations at each location)
   - **`current_hypothesis`**: Maintains current understanding (like explorer's working theory)
   - **`nodes`**: Builds knowledge graph incrementally (like explorer's map)

3. **Reaches Conclusion** ✅
   - Accumulates evidence from all probes
   - Forms and updates hypotheses based on findings
   - Stops when root cause found with high confidence
   - Provides complete explanation with evidence trail

### The Reasoning Cycle

The core cycle is:

```
Traverse → Test → Observe → Decide → Repeat
```

**Like an Explorer:**
- **Traverse**: Choose next location to explore (node selection)
- **Test**: Run small probe query (like taking a sample)
- **Observe**: Analyze result and store findings (like recording observations)
- **Decide**: Determine next step based on what you learned (like planning next move)
- **Repeat**: Continue until root cause found (like exploring until you find the answer)

This cycle continues until sufficient evidence is gathered to reach a conclusion about the root cause.

---

### Phase 1: Problem Understanding and Intent Compilation

**Step 1.1: Natural Language Parsing**
- **Input:** User query (e.g., "Why is System A TOS different from System B TOS?")
- **Process:** LLM parses the query to extract:
  - Systems involved (System A, System B)
  - Metrics in question (TOS - Total Outstanding)
  - Entities (loans, customers, etc.)
  - Constraints (date ranges, specific IDs, filters)
  - Problem type (RCA vs Data Validation)
- **Output:** `IntentSpec` with structured problem definition

**Step 1.2: Ambiguity Resolution**
- **Process:** LLM resolves ambiguities:
  - Which "TOS" metric? (if multiple exist)
  - Which systems? (if names are ambiguous)
  - What grain level? (loan-level, customer-level, etc.)
- **Reasoning:** Uses context from metadata, knowledge base, and previous queries
- **Output:** Resolved intent with no ambiguities

**Example Reasoning:**
```
Query: "Why is TOS different?"
LLM Reasoning:
- "TOS" could mean "Total Outstanding" or "Terms of Service"
- Context: Financial domain → likely "Total Outstanding"
- Need to identify which systems and which grain
- Ask clarifying questions or infer from metadata
```

---

### Phase 2: Knowledge Graph Construction and Task Grounding

**Step 2.1: Graph Building**
- **Process:** System builds knowledge graph from metadata:
  - **Table Nodes:** All relevant tables with columns, types, descriptions
  - **Rule Nodes:** Business rules that calculate metrics
  - **Join Nodes:** Relationships between tables
  - **Filter Nodes:** Filter conditions applied to data
  - **Metric Nodes:** Final metric definitions
- **Enrichment:** Adds hypergraph statistics (row counts, selectivities, data quality scores)

**Step 2.2: Table and Rule Selection**
- **Process:** LLM reasons about which tables/rules are relevant:
  - **Fuzzy Matching:** Finds tables with similar names/labels
  - **Label Matching:** Matches task labels with table/rule labels
  - **Grain Analysis:** Ensures tables match required grain level
  - **Chain-of-Thought Reasoning:** LLM explains why each candidate is relevant
- **Output:** Ranked list of candidate tables/rules with confidence scores

**Example Reasoning:**
```
Problem: "TOS discrepancy for loans"
LLM Reasoning:
1. TOS likely calculated from loan transactions
2. Need tables: loan_summary, transactions, payments
3. Rules: system_a_tos_rule, system_b_tos_rule
4. Grain: loan_id (one row per loan)
5. Confidence: High for loan_summary (0.95), Medium for transactions (0.75)
```

**Step 2.3: Column and Constraint Resolution**
- **Process:** Maps abstract entities to concrete columns:
  - Identifies primary keys (loan_id, customer_id, etc.)
  - Maps metric names to column names
  - Resolves filter conditions to SQL predicates
- **Output:** `GroundedTask` with concrete table/column mappings

---

### Phase 3: Investigation Strategy Selection

**Step 3.1: Approach Selection**
- **Decision Point:** Choose between Fixed Pipeline or Dynamic Traversal
- **Reasoning:**
  - **Fixed Pipeline:** When comprehensive analysis needed, all phases required
  - **Dynamic Traversal:** When quick root cause finding needed, adaptive exploration
- **Factors:** Problem complexity, time constraints, completeness requirements

**Step 3.2: Initial Hypothesis Formation**
- **Process:** LLM forms initial hypotheses about root cause:
  - **Hypothesis 1:** Missing rows (data not present in one system)
  - **Hypothesis 2:** Join failures (data exists but joins fail)
  - **Hypothesis 3:** Filter issues (data filtered out incorrectly)
  - **Hypothesis 4:** Rule differences (calculation logic differs)
  - **Hypothesis 5:** Data quality issues (nulls, incorrect values)
- **Reasoning:** Based on problem description, metadata, and common patterns
- **Output:** Ranked hypotheses with confidence scores

**Example Reasoning:**
```
Problem: "System A has 1000 rows, System B has 950 rows"
LLM Reasoning:
- 50 rows missing → likely not data quality issue (too many)
- Most likely: Join failure (60% confidence)
- Second: Filter issue (30% confidence)
- Third: Missing source data (10% confidence)
- Strategy: Probe joins first, then filters
```

---

### Phase 4: Dynamic Graph Traversal (If Using Traversal Approach)

**Explorer Pattern: Building Knowledge Incrementally**

The system works like an **explorer** that:
- ✅ **Writes short queries** (LIMIT 100) - Fast, focused probes
- ✅ **Stores all gained information** - Every probe result is saved
- ✅ **Builds knowledge incrementally** - Each step adds to understanding
- ✅ **Reaches conclusion** - Stops when root cause found with high confidence

**Information Storage (Explorer Memory):**
- **`visited_path`**: All nodes explored (like a map of where you've been)
- **`findings`**: All discoveries with full evidence (like explorer's notes)
- **`last_probe_result`**: Each node stores its probe result (like observations at each location)
- **`current_hypothesis`**: Current understanding (like explorer's theory)
- **`nodes`**: All discovered nodes with metadata (like explorer's map)

**Example Explorer Journey:**
```
Step 1: Probe table:payments_a
  → Query: SELECT * FROM payments_a WHERE paid_date = '2026-01-08' LIMIT 100
  → Result: 1000 rows
  → Stored: visited_path = [table:payments_a], findings = []
  → Knowledge: "System A has 1000 rows"

Step 2: Probe table:payments_b  
  → Query: SELECT * FROM payments_b WHERE paid_date = '2026-01-08' LIMIT 100
  → Result: 950 rows
  → Stored: visited_path = [table:payments_a, table:payments_b], findings = []
  → Knowledge: "System B missing 50 rows compared to System A"

Step 3: Probe join:payments:orders
  → Query: SELECT a.* FROM payments_a a LEFT JOIN orders o ON a.order_id = o.order_id 
           WHERE a.paid_date = '2026-01-08' AND o.order_id IS NULL LIMIT 100
  → Result: 50 rows with failed joins
  → Stored: visited_path = [...], findings = [JoinFailure(50 rows)]
  → Knowledge: "50 rows missing due to join failure - ROOT CAUSE FOUND"
  → Conclusion: Stop exploration, report root cause
```

**Step 4.1: Node Selection**
- **Process:** LLM chooses next node to probe:
  - **Input:** All candidate nodes with rich metadata
  - **Context:** Current findings, visited path, problem description
  - **Reasoning Factors:**
    1. **Relevance to Findings:** Missing rows → probe joins
    2. **Information Gain:** Which node eliminates most possibilities?
    3. **Proximity:** Nodes connected to visited nodes
    4. **Metadata:** Column existence, join feasibility, data quality
  - **Output:** Selected node with reasoning and expected insight

**Example Reasoning:**
```
Current Finding: System B missing 50 rows
Candidate Nodes:
- join:payments:orders (score: 0.9, most likely cause)
- filter:status_active (score: 0.6, possible cause)
- table:payments_b (score: 0.3, already probed)

LLM Reasoning:
"Missing rows are most commonly caused by join failures. 
The join:payments:orders node has high relevance (0.9) and 
is directly connected to the payments table we just probed.
Expected insight: Will reveal if 50 rows failed to join."
Decision: Probe join:payments:orders
```

**Step 4.2: SQL Probe Generation**
- **Process:** LLM generates optimal SQL probe:
  - **Input:** Node metadata (columns, types, join keys, filters)
  - **Reasoning:** 
    - Uses column names from metadata
    - Applies constraints from problem
    - Tests specific hypothesis (join failure, filter issue, etc.)
    - Keeps query small (LIMIT 100) for fast iteration
  - **Output:** SQL query optimized for the specific probe type

**Example Reasoning:**
```
Node: join:payments:orders
Hypothesis: Join failures causing missing rows
LLM Reasoning:
"To test join failures, I need to:
1. LEFT JOIN payments to orders
2. Filter for rows where order_id IS NULL
3. Apply date constraint from problem
4. LIMIT 100 for fast execution"
SQL: SELECT a.* FROM payments_a a 
     LEFT JOIN orders o ON a.order_id = o.order_id 
     WHERE a.paid_date = '2026-01-08' AND o.order_id IS NULL 
     LIMIT 100
```

**Step 4.3: Probe Execution**
- **Process:** SQL Engine executes probe query
- **Output:** `SqlProbeResult` with:
  - Row count
  - Sample rows
  - Column statistics (null counts, value ranges)
  - Execution time
  - Warnings (if any)

**Step 4.4: Result Observation and Interpretation**
- **Process:** LLM analyzes probe result:
  - **Row Count Analysis:**
    - `row_count == 0`: No data → eliminate this path or probe upstream
    - `row_count < expected`: Some data missing → likely root cause
    - `row_count == expected`: Data exists → probe downstream
  - **Null Analysis:**
    - High nulls in join keys → join failure confirmed
    - High nulls in value columns → data quality issue
  - **Join Analysis:**
    - `LEFT JOIN ... WHERE right.key IS NULL` returns rows → join failures found
    - Join returns 0 rows → all joins succeeded
  - **Filter Analysis:**
    - Filter returns 0 rows → all rows filtered out
    - Filter returns fewer rows → some rows filtered
  - **Value Analysis:**
    - Compare value ranges between systems
    - Identify outliers or unexpected values

**Example Reasoning:**
```
Probe Result:
- Row count: 50 rows
- All rows have NULL order_id after join
- Sample: payment_id=123, order_id=NULL

LLM Interpretation:
"Probe found exactly 50 rows where join failed (order_id IS NULL).
This matches the 50 missing rows in System B.
Conclusion: Root cause is join failure between payments and orders.
Confidence: High (0.95) - exact match between missing rows and join failures."
```

**Step 4.5: Finding Recording**
- **Process:** System records finding:
  - **Finding Type:** JoinFailure, FilterIssue, MissingData, ValueMismatch, etc.
  - **Description:** Human-readable explanation
  - **Evidence:** Probe result, sample rows, statistics
  - **Confidence:** 0.0-1.0 based on evidence strength
- **Output:** `Finding` added to traversal state

**Step 4.6: Hypothesis Update**
- **Process:** LLM updates current hypothesis:
  - **If Finding Confirms Hypothesis:**
    - Increase confidence
    - Consider root cause found if confidence high enough
  - **If Finding Rejects Hypothesis:**
    - Decrease confidence
    - Form new hypothesis based on finding
  - **If Finding Partially Confirms:**
    - Refine hypothesis
    - Plan additional probes to confirm

**Example Reasoning:**
```
Initial Hypothesis: "Join failure causing missing rows" (60% confidence)
Finding: 50 rows with failed joins, exactly matching missing rows
Updated Hypothesis: "Join failure between payments and orders is root cause" (95% confidence)
Decision: Root cause found with high confidence → STOP
```

**Step 4.7: Decision Making**
- **Process:** LLM decides next action:
  - **Root Cause Found:** 
    - Confidence > threshold (e.g., 0.9)
    - Evidence sufficient
    - Action: STOP, record root cause
  - **More Evidence Needed:**
    - Confidence medium (e.g., 0.5-0.9)
    - Action: Probe related nodes to confirm
  - **Hypothesis Rejected:**
    - Finding contradicts hypothesis
    - Action: Form new hypothesis, probe different path
  - **No Progress:**
    - Probe didn't reveal useful information
    - Action: Try different node or approach

**Example Reasoning:**
```
Finding: 50 join failures found
Confidence: 0.95 (high)
Evidence: Exact match between missing rows and join failures
Decision: ROOT CAUSE FOUND
Action: STOP, record finding, generate explanation
```

**Step 4.8: Loop Continuation or Termination**
- **Process:** System checks termination conditions:
  - **Root Cause Found:** Confidence high, evidence sufficient → STOP
  - **Max Depth Reached:** Too many probes → STOP with current findings
  - **Max Iterations:** Time limit reached → STOP with partial findings
  - **All Paths Explored:** No more relevant nodes → STOP
  - **Otherwise:** Continue to Step 4.1 (select next node)

---

### Phase 5: Evidence Accumulation and Pattern Recognition

**Step 5.1: Finding Aggregation**
- **Process:** System aggregates all findings:
  - Groups findings by type (JoinFailure, FilterIssue, etc.)
  - Counts affected rows per finding
  - Calculates aggregate impact (sum of differences)
  - Identifies patterns across findings

**Step 5.2: Cross-Finding Analysis**
- **Process:** LLM analyzes relationships between findings:
  - **Causal Chains:** Finding A causes Finding B
  - **Independent Issues:** Multiple unrelated root causes
  - **Cascading Effects:** One issue leads to multiple symptoms
  - **Common Patterns:** Similar issues across multiple entities

**Example Reasoning:**
```
Findings:
1. Join failure: 30 rows (payments → orders)
2. Filter issue: 15 rows (status filter too restrictive)
3. Missing data: 5 rows (source data not present)

LLM Analysis:
"Three independent root causes identified:
1. Join failure is primary cause (30 rows, 60% of discrepancy)
2. Filter issue is secondary (15 rows, 30% of discrepancy)
3. Missing data is minor (5 rows, 10% of discrepancy)

Total: 50 rows, matches aggregate mismatch exactly."
```

**Step 5.3: Confidence Calculation**
- **Process:** System calculates overall confidence:
  - **Evidence Strength:** How strong is each piece of evidence?
  - **Consistency:** Do findings support each other?
  - **Completeness:** Is aggregate mismatch explained?
  - **Verification:** Does row-level sum match aggregate?
- **Output:** Overall confidence score (0.0-1.0)

---

### Phase 6: Root Cause Conclusion

**Step 6.1: Root Cause Identification**
- **Process:** LLM identifies primary root cause(s):
  - **Primary Root Cause:** Highest impact finding (most rows affected)
  - **Secondary Root Causes:** Additional findings with significant impact
  - **Contributing Factors:** Minor issues that contribute to discrepancy
- **Reasoning:** Based on:
  - Row count impact
  - Confidence scores
  - Causal relationships
  - Business significance

**Step 6.2: Explanation Generation**
- **Process:** LLM generates structured explanation:
  - **Summary:** One-sentence root cause description
  - **Details:** Step-by-step explanation of how root cause occurred
  - **Evidence:** Specific findings that support conclusion
  - **Impact:** Number of rows affected, aggregate difference
  - **Recommendations:** Actions to fix the issue

**Example Reasoning:**
```
Root Cause: Join failure between payments and orders tables

Explanation:
"System B is missing 50 payment rows because the join between 
payments_a and orders tables failed. Specifically, 50 payment 
records have order_id values that do not exist in the orders 
table. This causes these payments to be excluded from System B's 
aggregate calculation, resulting in a 50-row discrepancy.

Evidence:
- Probe of join:payments:orders found 50 rows with NULL order_id
- Exact match with 50 missing rows in System B
- Sample rows: payment_id=123, order_id=NULL

Impact:
- 50 rows missing in System B
- Aggregate difference: 50,000 (sum of missing payment amounts)

Recommendations:
1. Verify order_id values in payments table
2. Check if orders table is missing records
3. Review join logic for payments → orders
4. Consider using LEFT JOIN instead of INNER JOIN if appropriate"
```

**Step 6.3: Verification**
- **Process:** System verifies conclusion:
  - **Mathematical Verification:** 
    - Calculates: `sum(missing_left) - sum(missing_right) + sum(mismatch_diff)`
    - Compares with reported aggregate mismatch
    - Verifies within precision tolerance
  - **Completeness Check:**
    - Ensures all discrepancies are explained
    - Checks for missing categories
  - **Consistency Check:**
    - Verifies findings are consistent with each other
    - Checks for contradictions

**Example Reasoning:**
```
Verification:
Reported mismatch: 50,000
Calculated mismatch: 
  - Missing in System B: 50,000 (50 rows × 1,000 avg)
  - Missing in System A: 0
  - Value mismatches: 0
  - Total: 50,000

Match: ✅ Exact match (within tolerance)
Conclusion: Root cause explanation is mathematically verified.
```

---

### Phase 7: Narrative Generation

**Step 7.1: Structured Narrative Building**
- **Process:** System builds human-readable narrative:
  - **Template-Based:** Uses structured templates for consistency
  - **LLM-Enhanced:** Optional LLM enhancement for natural language
  - **Sections:**
    - Executive Summary
    - Root Cause Details
    - Evidence and Findings
    - Impact Analysis
    - Recommendations

**Step 7.2: Narrative Refinement**
- **Process:** LLM refines narrative:
  - Ensures clarity and readability
  - Adds context and business significance
  - Provides actionable recommendations
  - Includes relevant examples

---

### Complete Reasoning Flow Example

**Problem:** "Why is paid_amount different on 2026-01-08 between system A and B?"

**Step-by-Step Reasoning:**

```
1. INTENT COMPILATION
   Input: "Why is paid_amount different on 2026-01-08 between system A and B?"
   LLM Reasoning: Extract systems (A, B), metric (paid_amount), constraint (date)
   Output: IntentSpec { systems: [A, B], metric: paid_amount, date: 2026-01-08 }

2. TASK GROUNDING
   LLM Reasoning: Find tables with payments, identify paid_amount column
   Output: GroundedTask { tables: [payments_a, payments_b], columns: [paid_amount] }

3. INITIAL HYPOTHESIS
   LLM Reasoning: "Different paid_amount could be due to missing rows or value differences"
   Output: Hypotheses: [MissingRows(0.6), ValueMismatch(0.4)]

4. NODE SELECTION (Iteration 1)
   LLM Reasoning: "Start with base tables to check if data exists"
   Decision: Probe table:payments_a
   SQL: SELECT * FROM payments_a WHERE paid_date = '2026-01-08' LIMIT 100

5. PROBE EXECUTION
   Result: 1000 rows found
   Observation: System A has data

6. NODE SELECTION (Iteration 2)
   LLM Reasoning: "Now probe System B to compare row counts"
   Decision: Probe table:payments_b
   SQL: SELECT * FROM payments_b WHERE paid_date = '2026-01-08' LIMIT 100

7. PROBE EXECUTION
   Result: 950 rows found
   Observation: System B missing 50 rows compared to System A

8. HYPOTHESIS UPDATE
   LLM Reasoning: "Missing rows confirmed, update hypothesis"
   Updated Hypothesis: MissingRows(0.9), likely due to join failure (0.7)

9. NODE SELECTION (Iteration 3)
   LLM Reasoning: "Missing rows most commonly caused by join failures. Probe joins."
   Decision: Probe join:payments:orders
   SQL: SELECT a.* FROM payments_a a LEFT JOIN orders o 
        ON a.order_id = o.order_id 
        WHERE a.paid_date = '2026-01-08' AND o.order_id IS NULL 
        LIMIT 100

10. PROBE EXECUTION
    Result: 50 rows where join failed (order_id IS NULL)
    Observation: Exact match - 50 missing rows = 50 join failures

11. FINDING RECORDING
    Finding: JoinFailure {
        description: "50 rows missing due to failed join on order_id",
        evidence: 50 rows with NULL order_id,
        confidence: 0.95
    }

12. DECISION MAKING
    LLM Reasoning: "Root cause found with high confidence. Evidence sufficient."
    Decision: ROOT CAUSE FOUND → STOP

13. ROOT CAUSE CONCLUSION
    Root Cause: "Join failure between payments and orders tables"
    Explanation: "50 payment rows have order_id values not found in orders table"
    Impact: 50 rows missing, aggregate difference: 50,000

14. VERIFICATION
    Calculated mismatch: 50,000
    Reported mismatch: 50,000
    Verification: ✅ Match confirmed

15. NARRATIVE GENERATION
    Output: Human-readable report with summary, details, recommendations
```

---

### Key Reasoning Principles

**1. Evidence-Based Reasoning**
- Every conclusion must be supported by evidence
- Evidence comes from probe results, not assumptions
- Confidence scores reflect evidence strength

**2. Adaptive Exploration**
- Next step depends on current findings
- Not fixed pipeline - adapts to observations
- Eliminates possibilities systematically

**3. Hypothesis-Driven Investigation**
- Form hypotheses early
- Test hypotheses with probes
- Update hypotheses based on findings
- Stop when hypothesis confirmed with high confidence

**4. Information Gain Maximization**
- Choose nodes that eliminate most possibilities
- Prioritize high-impact probes
- Avoid redundant exploration

**5. Early Termination**
- Stop when root cause found
- Don't continue exploring after conclusion
- Balance thoroughness with efficiency

**6. Mathematical Verification**
- Verify conclusions mathematically
- Ensure aggregate matches row-level differences
- Provide proof of correctness

---

### Reasoning Quality Factors

**High-Quality Reasoning:**
- ✅ Clear hypothesis formation
- ✅ Systematic evidence gathering
- ✅ Logical conclusion from evidence
- ✅ Mathematical verification
- ✅ Comprehensive explanation

**Low-Quality Reasoning:**
- ❌ Jumping to conclusions without evidence
- ❌ Ignoring contradictory findings
- ❌ Not verifying mathematically
- ❌ Vague or incomplete explanations

---

### Conclusion

The RCA Engine's reasoning process is a **systematic, evidence-based investigation** that:

1. **Understands** the problem through intent compilation
2. **Explores** the knowledge graph adaptively
3. **Tests** hypotheses with SQL probes
4. **Observes** results and interprets findings
5. **Decides** next steps based on evidence
6. **Accumulates** evidence to build confidence
7. **Concludes** root cause when evidence sufficient
8. **Verifies** conclusion mathematically
9. **Explains** root cause in human-readable format

This process transforms **manual, time-consuming RCA** into an **automated, intelligent investigation** that finds root causes efficiently and explainably.

---

## How It Solves RCA Problems

### Problem 1: Aggregate-Level Analysis Doesn't Reveal Root Causes

**Traditional Approach:**
```
System A TOS: 1,000,000
System B TOS: 950,000
Difference: 50,000
→ "There's a 50k difference, but we don't know why"
```

**RCA Engine Solution:**
```
Row-level analysis reveals:
- Missing in System B: 30 rows totaling 30,000
- Missing in System A: 5 rows totaling 5,000
- Value mismatches: 20 rows with 25,000 difference

Root causes identified:
- 15 rows: Failed join on customer_id
- 10 rows: Filtered out by status filter
- 5 rows: Rule calculation difference
```

### Problem 2: No Visibility into Data Pipeline

**Traditional Approach:**
- Black box: Data goes in, aggregate comes out
- No way to trace where discrepancies originate
- Manual investigation required

**RCA Engine Solution:**
- **Complete lineage tracing:** Every row's journey is tracked
- **Join tracing:** See which joins failed and why
- **Filter tracing:** Understand which filters dropped rows
- **Rule tracing:** See how rules transformed data

### Problem 3: Manual Investigation is Time-Consuming

**Traditional Approach:**
- Analyst manually queries systems
- Compares results
- Investigates discrepancies one by one
- Takes hours or days

**RCA Engine Solution:**
- **Automated pipeline:** Runs end-to-end automatically
- **Parallel processing:** Analyzes both systems simultaneously
- **Structured output:** Provides ready-to-use explanations
- **Takes minutes instead of hours**

### Problem 4: Cannot Verify Reconciliation Correctness

**Traditional Approach:**
- Trust that row-level analysis is correct
- No way to verify aggregate matches row differences
- Potential for missing discrepancies

**RCA Engine Solution:**
- **Mathematical proof:** Aggregate reconciliation engine
- **Verification:** Proves sum(row_diff) == aggregate_mismatch
- **Breakdown:** Shows contribution from each category
- **Confidence:** Provides verification status

### Problem 5: Difficult to Reproduce Analysis

**Traditional Approach:**
- Analysis steps not recorded
- Cannot reproduce results
- Difficult to debug issues

**RCA Engine Solution:**
- **Evidence storage:** Complete execution records
- **Deterministic replay:** Reproduce exact analysis
- **Verification:** Compare replay results with original
- **Auditability:** Full audit trail

### Problem 6: Large-Scale Data Performance

**Traditional Approach:**
- Load entire datasets into memory
- Slow processing for large datasets
- Memory constraints

**RCA Engine Solution:**
- **Chunked extraction:** Process in chunks
- **Hash-based diff:** Fast comparison
- **Pushdown predicates:** Optimize data loading
- **Sampling:** Quick analysis of subsets

---

## Natural Language to Root Cause: Complete Example

### Example: Recovery Reconciliation for Digital Loans

This example demonstrates how the system transforms a simple natural language query into a complete root cause analysis with UUID-level precision.

#### User Query (Simple Intent)

```
"Why is recovery mismatching between system A and B for Digital loans on 2026-01-08?"
```

#### System Processing

**Step 1: Intent Compilation**
- **LLM Infers:**
  - Metric: `recovery` (maps to `paid_amount`)
  - Systems: `system_a`, `system_b`
  - Date: `2026-01-08`
  - Filter: `loan_type = 'Digital'`
  - Likely grain: `payment-level` (uuid, paid_date, mis_date, current_bucket)
  - Likely grouping: `current_bucket`

**Step 2: Task Grounding**
- **LLM Reasons:**
  - Finds tables: `payments_a`, `payments_b`, `loan_summary`
  - Identifies columns: `paid_amount`, `paid_date`, `mis_date`, `current_bucket`, `uuid`
  - Selects rules: `system_a_recovery_rule`, `system_b_recovery_rule`
  - Resolves grain: `["uuid", "paid_date", "mis_date", "current_bucket"]`

**Step 3: Complex Date Logic Inference**
- **System Understands:**
  - FTD (First Time Default): `paid_date = 2026-01-08`
  - CM (Current Month): `mis_date = 2026-01-08`
  - LMTD (Last Month to Date): `mis_date = 2025-12-08`
  - LM (Last Month snapshot): `mis_date = 2025-12-04`
- **LLM Generates:** Multiple constraint sets for each date logic type

**Step 4: Dynamic Graph Traversal**
- **Explorer Pattern:**
  ```
  Probe 1: table:payments_a WHERE loan_type='Digital' AND paid_date='2026-01-08' LIMIT 100
    → Result: 1000 rows
    → Stored: visited_path = [table:payments_a]
  
  Probe 2: table:payments_b WHERE loan_type='Digital' AND paid_date='2026-01-08' LIMIT 100
    → Result: 950 rows
    → Stored: visited_path = [...], findings = []
    → Knowledge: "50 rows missing in System B"
  
  Probe 3: join:payments:orders WHERE loan_type='Digital' AND paid_date='2026-01-08' LIMIT 100
    → Result: 50 rows with failed joins
    → Stored: findings = [JoinFailure(50 rows, evidence: SqlProbeResult)]
    → Knowledge: "50 rows missing due to join failure - ROOT CAUSE FOUND"
  ```

**Step 5: Row-Level Diff**
- **System Executes:**
  - Materializes rows from both systems at grain: `(uuid, paid_date, mis_date, current_bucket)`
  - Groups by `current_bucket`
  - Compares row-by-row
  - Identifies exact UUIDs causing mismatch

**Step 6: UUID-Level Reporting**
- **Output:**
  ```
  Root Cause: Join failure between payments and orders tables
  
  Affected UUIDs:
  - uuid_001: Missing in System B (join failed on order_id)
  - uuid_002: Missing in System B (join failed on order_id)
  - ... (48 more UUIDs)
  
  Breakdown by Bucket:
  - Bucket 0-30: 20 UUIDs affected
  - Bucket 31-60: 15 UUIDs affected
  - Bucket 61-90: 10 UUIDs affected
  - Bucket 91+: 5 UUIDs affected
  
  Aggregate Impact:
  - System A total: 1,000,000
  - System B total: 950,000
  - Difference: 50,000 (matches sum of missing UUIDs)
  ```

**Step 7: Explanation**
- **System Explains:**
  - Why each UUID is missing (join failure details)
  - Which join condition failed
  - What data exists in System A but not System B
  - Recommendations to fix

#### More Precise Intent (If User Provides)

```
"Reconcile paid_amount between system A and B for Digital loans, grouped by current_bucket, for:
- FTD: paid_date = 2026-01-08
- CM: mis_date = 2026-01-08
- LMTD: mis_date = 2025-12-08
- LM: mis_date = 2025-12-04
And show which uuids cause the difference."
```

**System Processing:**
- **LLM Parses:** Explicit date constraints for each recovery type
- **System Executes:** Separate probes for each date logic
- **System Aggregates:** Combines results across all date types
- **System Reports:** UUID-level differences for each bucket and date type

#### Fully Explicit Intent (Maximum Detail)

```
"For Digital loans, reconcile recovery amounts between System A and System B.
Compare paid_amount at grain (uuid, paid_date, mis_date, current_bucket).
Focus on:
- paid_date = 2026-01-08 (FTD)
- mis_date = 2026-01-08 (Current Month)
- mis_date = 2025-12-08 (Last Month to Date)
- mis_date = 2025-12-04 (Last Month snapshot)
Group results by current_bucket.
If totals don't match, show the exact uuid(s) causing the mismatch and explain why."
```

**System Processing:**
- **LLM Parses:** All explicit requirements
- **System Executes:** Comprehensive analysis with all constraints
- **System Reports:** Complete UUID-level breakdown with explanations

### Key Capabilities Demonstrated

✅ **Natural Language Understanding:** Parses simple queries and infers complex requirements  
✅ **Intent Inference:** Extracts metric, systems, dates, grain, grouping from minimal input  
✅ **Complex Logic Translation:** Converts business logic (FTD, CM, LMTD, LM) to SQL constraints  
✅ **Small Query Execution:** Uses LIMIT 100 probes for fast iteration  
✅ **Information Storage:** Stores all findings incrementally (explorer pattern)  
✅ **Row-Level Analysis:** Identifies exact UUIDs causing mismatch  
✅ **Bucket Grouping:** Groups results by current_bucket  
✅ **Root Cause Explanation:** Explains why each UUID differs  
✅ **Mathematical Verification:** Proves aggregate matches row-level differences  

### The Transformation

**Before (Traditional Approach):**
- User writes 300-line SQL query
- Manually handles FTD, CM, LMTD, LM logic
- Manually joins tables, applies filters
- Manually compares results
- Manually identifies UUIDs causing mismatch
- Manually explains differences

**After (RCA Engine):**
- User asks: "Why is recovery mismatching between system A and B for Digital loans on 2026-01-08?"
- System handles everything automatically
- Returns: Exact UUIDs with complete explanations

### Implementation Status

**✅ Fully Implemented:**
- Natural language query parsing (Intent Compiler)
- Intent inference (systems, metrics, dates, grain, grouping)
- Small SQL probe generation (LIMIT 100)
- Explorer pattern (stores all findings incrementally)
- Row-level diff (identifies exact rows causing mismatch)
- Root cause explanation (why each row differs)
- Mathematical verification (proves aggregate matches row-level)

**✅ Partially Implemented (May Need Enhancement):**
- Complex date logic (FTD, CM, LMTD, LM): Currently supports date constraints, but may need explicit business logic mapping
- Multiple date constraints in one query: Supported via multiple constraints, but may need explicit grouping by date type
- UUID-level reporting: Supported via row-level diff, but may need explicit UUID formatting in output

**🔧 May Need Enhancement:**
- Explicit bucket grouping in output format
- Business term mapping (FTD → paid_date, CM → mis_date, etc.)
- Multiple date logic types in single query (may need to split into multiple sub-queries)

**Note:** The core architecture supports all these capabilities. The enhancements needed are primarily in:
1. **Intent Compiler:** Better understanding of business terms (FTD, CM, LMTD, LM)
2. **Task Grounder:** Explicit grouping by bucket in output
3. **Output Format:** UUID-level reporting with bucket breakdown

The system **can work like this** - it's designed for this exact use case. The enhancements are refinements to make it work even better for complex business logic scenarios.

---

## Complete RCA Flow Examples

### Example 1: Fixed Pipeline Approach (RCA Cursor)

**Scenario: TOS Discrepancy Investigation**

**1. User Query:**
```
"Why is System A TOS different from System B TOS for loan L001?"
```

**2. Intent Compilation:**
```json
{
  "task_type": "RCA",
  "systems": ["system_a", "system_b"],
  "target_metrics": ["tos"],
  "entities": ["loan"],
  "grain": ["loan_id"],
  "constraints": [{"column": "loan_id", "operator": "=", "value": "L001"}]
}
```

**3. Task Grounding:**
- Finds tables: `loan_summary`, `emi_transactions`, `fee_details`
- Selects rules: `system_a_tos_rule`, `system_b_tos_rule`
- Identifies columns: `loan_id`, `total_outstanding`

**4-12. Fixed Pipeline Execution:**
- Metric Normalization → Row Materialization → Canonicalization
- Row Diff → Lineage Tracing → Attribution → Narrative → Reconciliation

**Result:** Complete, deterministic analysis with full audit trail.

---

### Example 2: Dynamic Graph Traversal Approach (Graph Traversal Agent)

**Scenario: Paid Amount Discrepancy Investigation**

**1. User Query:**
```
"Why is paid_amount different on 2026-01-08 between system A and B?"
```

**2. Intent Compilation & Graph Building:**
- Builds knowledge graph with nodes:
  - Metric nodes: `metric:system_a:paid_amount`, `metric:system_b:paid_amount`
  - Rule nodes: `rule:system_a_paid_rule`, `rule:system_b_paid_rule`
  - Table nodes: `table:payments_a`, `table:payments_b`
  - Join nodes: `join:payments_a:orders`, `join:payments_b:orders`

**3. Traversal Step 1: Probe Base Rows**
```
Node: table:payments_a
Probe SQL: SELECT * FROM payments_a WHERE paid_date = '2026-01-08' LIMIT 100
Result: 1000 rows
Observation: System A has data
LLM Decision: Continue, probe System B
```

**4. Traversal Step 2: Probe System B**
```
Node: table:payments_b
Probe SQL: SELECT * FROM payments_b WHERE paid_date = '2026-01-08' LIMIT 100
Result: 950 rows
Observation: System B missing 50 rows compared to System A
LLM Decision: Missing rows → probe joins (most likely cause)
```

**5. Traversal Step 3: Probe Join**
```
Node: join:payments_a:orders
Probe SQL: SELECT a.* FROM payments_a a LEFT JOIN orders o 
           ON a.order_id = o.order_id 
           WHERE a.paid_date = '2026-01-08' AND o.order_id IS NULL
Result: 50 rows where join failed
Observation: 50 rows missing due to failed join
LLM Decision: ROOT CAUSE FOUND - Join failure explains discrepancy
```

**6. Early Termination:**
```
Root Cause: Join failure between payments_a and orders
Hypothesis: 50 rows missing in system B due to failed join on order_id
Findings Recorded: JoinFailure finding with 50 rows evidence
Status: ✅ STOP - Root cause found
```

**Result:** Fast, targeted investigation with early termination.

---

### Comparison: Fixed Pipeline vs Dynamic Traversal

| Aspect | Fixed Pipeline | Dynamic Traversal |
|--------|---------------|-------------------|
| **Execution** | Always runs all phases | Only explores relevant nodes |
| **Speed** | Slower (comprehensive) | Faster (targeted) |
| **Completeness** | Always complete | May miss edge cases |
| **Adaptability** | None | High (adapts to findings) |
| **Use Case** | Deep/Forensic analysis | Quick investigation |
| **Root Cause** | Always finds if exists | Finds if on explored path |
| **Best For** | Production reconciliation | Rapid debugging |

---

## Key Innovations

### 1. Dynamic Graph Traversal (NEW)

**Why it matters:**
- **Adaptive:** Chooses next step based on observations, not fixed sequence
- **Efficient:** Only explores relevant paths, stops early when root cause found
- **Intelligent:** LLM-guided node selection with full context
- **Fast:** Small SQL probes enable rapid iteration

**Key Property:**
- Does NOT follow fixed pipeline (A → B → C)
- Dynamically chooses next step based on:
  - Current findings (missing rows → probe joins)
  - Information gain (which node eliminates most possibilities?)
  - Proximity (nodes connected to visited nodes)
  - LLM reasoning (intelligent selection with full metadata)

### 2. Rich Metadata at Each Node (NEW)

**Why it matters:**
- **LLM gets full context:** Columns, types, descriptions, rules, formulas
- **Better decisions:** Know what columns exist, join feasibility, data quality
- **Informed SQL generation:** Know column names, types, primary keys
- **Quality insights:** Understand data quality before probing

**What LLM Receives:**
- Table metadata: columns, types, descriptions, labels, grain
- Rule metadata: formulas, source entities, filter conditions
- Join metadata: join keys, join types, relationships
- Hypergraph stats: row counts, selectivities, data quality scores

### 3. Row-Level Analysis Instead of Aggregate

**Why it matters:**
- Aggregates hide individual discrepancies
- Row-level reveals exact problems
- Enables precise root cause identification

### 4. Complete Lineage Tracing

**Why it matters:**
- Provides audit trail
- Explains data transformations
- Enables debugging

### 5. Canonical Entity Model

**Why it matters:**
- Standardizes across systems
- Enables comparison
- Simplifies processing

### 6. Mathematical Verification

**Why it matters:**
- Proves correctness
- Builds trust
- Ensures completeness

### 7. LLM-Powered Reasoning with System Prompts (ENHANCED)

**Why it matters:**
- Handles ambiguity
- Selects optimal strategies
- Generates natural language explanations
- **Guides graph traversal** with system prompts
- **Makes intelligent node selections** with full metadata context
- **Interprets probe results** with pattern recognition
- **Forms hypotheses** based on findings

### 8. Performance Optimizations

**Why it matters:**
- Handles large-scale data
- Reduces processing time
- Optimizes resource usage

### 9. Trust and Auditability

**Why it matters:**
- Enables reproducibility
- Provides audit trail
- Builds confidence

---

## Use Cases

### 1. Financial Reconciliation

**Scenario:** Reconcile loan balances between core banking system and accounting system.

**RCA Engine:**
- Identifies missing transactions
- Explains calculation differences
- Verifies aggregate reconciliation
- Provides audit trail

### 2. Data Quality Validation

**Scenario:** Validate data consistency across multiple data warehouses.

**RCA Engine:**
- Compares row-level data
- Identifies data quality issues
- Traces to source systems
- Generates validation reports

### 3. ETL Pipeline Debugging

**Scenario:** Debug why ETL pipeline produces different results than expected.

**RCA Engine:**
- Traces data transformations
- Identifies failed joins/filters
- Explains rule transformations
- Provides debugging insights

### 4. Regulatory Reporting

**Scenario:** Ensure regulatory reports match source system data.

**RCA Engine:**
- Verifies report accuracy
- Provides reconciliation proof
- Generates audit evidence
- Enables reproducibility

### 5. System Migration Validation

**Scenario:** Validate data migration from legacy to new system.

**RCA Engine:**
- Compares migrated data with source
- Identifies migration issues
- Provides detailed discrepancy reports
- Verifies completeness

---

## Technical Architecture

### Data Flow

**Fixed Pipeline Flow:**
```
Metadata (JSON) + Hypergraph
    ↓
Intent Compiler → IntentSpec
    ↓
Task Grounder → GroundedTask
    ↓
RCA Cursor Pipeline:
    1. Metric Normalization
    2. Row Materialization (System A & B)
    3. Canonicalization
    4. Row Diff
    5. Lineage Tracing
    6. Root Cause Attribution
    7. Narrative Building
    8. Aggregate Reconciliation
    ↓
RcaCursorResult
```

**Dynamic Graph Traversal Flow:**
```
Metadata (JSON) + Hypergraph
    ↓
Intent Compiler → IntentSpec
    ↓
Task Grounder → GroundedTask + Knowledge Graph
    ↓
Graph Traversal Agent:
    Loop: Traverse → Test → Observe → Decide
    1. Choose Next Node (LLM-guided with metadata)
    2. Run SQL Probe (small, focused query)
    3. Observe Result (interpret findings)
    4. Decide Next Step (adapt based on observations)
    5. Repeat Until Root Cause Found
    ↓
TraversalState (findings, hypothesis, root cause)
```

### Knowledge Graph Structure

```
Nodes:
├── Table Nodes
│   ├── Metadata: columns, types, descriptions, labels, grain
│   └── Hypergraph Stats: row_count, distinct_count, data_quality_score
├── Rule Nodes
│   ├── Metadata: formula, source_entities, filter_conditions
│   └── Business Logic: computation definition
├── Join Nodes
│   ├── Metadata: join_keys, join_type, table relationships
│   └── Hypergraph Stats: join_selectivity
├── Filter Nodes
│   ├── Metadata: condition, table, description
│   └── Hypergraph Stats: filter_selectivity
└── Metric Nodes
    ├── Metadata: description, grain, precision, unit
    └── Business Context: metric definition

Edges:
├── Table → Join (via lineage)
├── Join → Table (relationships)
├── Table → Rule (used in computation)
├── Rule → Metric (calculates metric)
└── Filter → Table (applied to table)
```

### Key Data Structures

**IntentSpec:**
- Task type (RCA/DV)
- Systems, metrics, entities
- Constraints, grain, time scope

**GroundedTask:**
- Candidate tables with confidence scores
- Resolved columns and constraints
- Required grain level

**TraversalNode (NEW):**
- Node ID and type (Table, Rule, Join, Filter, Metric)
- Visit status and count
- Last probe result
- Score and reasons
- **Rich metadata** (table_info, rule_info, join_info, hypergraph_stats)

**NodeMetadata (NEW):**
- Table metadata: columns, types, descriptions, labels, grain
- Rule metadata: formulas, source entities, filter conditions
- Join metadata: join keys, join types, relationships
- Metric metadata: descriptions, grain, precision, units
- Hypergraph stats: row counts, selectivities, data quality scores

**SqlProbeResult (NEW):**
- Row count and sample rows
- Column names
- Summary statistics (null counts, value ranges)
- Execution time and warnings

**TraversalState (NEW):**
- All nodes in graph
- Visited path
- Findings (with evidence)
- Current hypothesis
- Root cause status

**MetricDefinition:**
- Base tables
- Joins and filters
- Aggregation formulas
- Group by columns

**RowDiffResult:**
- Missing left/right dataframes
- Value mismatch dataframe
- Summary statistics

**RowExplanation:**
- Row identifier
- Difference type
- Explanations from lineage
- Confidence score

**RcaCursorResult:**
- Complete row diff
- Reconciliation proof
- Root cause explanations
- Human-readable narratives

---

## Performance Characteristics

### Scalability

- **Chunked processing:** Handles datasets larger than memory
- **Hash-based diff:** O(n) comparison instead of O(n²)
- **Pushdown predicates:** Reduces data transfer
- **Parallel processing:** Analyzes systems concurrently

### Accuracy

- **Precision-aware comparison:** Handles floating-point precision
- **Complete lineage:** Tracks all transformations
- **Mathematical verification:** Proves correctness
- **Confidence scoring:** Indicates explanation quality

### Reliability

- **Deterministic replay:** Reproducible results
- **Evidence storage:** Complete audit trail
- **Error handling:** Graceful degradation
- **Verification:** Multiple consistency checks

---

## Integration Points

### Inputs

1. **Metadata:** Table definitions, rules, lineage, labels
2. **Data:** Parquet/CSV files from source systems
3. **Queries:** Natural language or structured queries
4. **Configuration:** LLM settings, performance parameters

### Outputs

1. **RCA Results:** Row-level differences and explanations
2. **Narratives:** Human-readable root cause descriptions
3. **Evidence:** Execution records for auditability
4. **Verification:** Reconciliation proofs and checks

### APIs

- **Rust API:** Direct library usage
- **CLI:** Command-line interface
- **HTTP Server:** REST API (via server binary)
- **LLM Integration:** OpenAI-compatible API

---

## Future Enhancements

### Planned Features

1. **Real-time RCA:** Stream processing for live data
2. **Anomaly Detection:** Automatic discrepancy detection
3. **Predictive RCA:** ML models for root cause prediction
4. **Visualization:** Interactive dashboards for RCA results
5. **Collaboration:** Multi-user investigation workflows
6. **DuckDB Integration:** Full SQL support for SQL Engine (currently uses Polars)
7. **Parallel Probes:** Execute multiple probes simultaneously
8. **Probe Caching:** Cache probe results to avoid re-execution
9. **Learning:** Learn from past traversals to improve node selection
10. **Hypergraph Stats Updates:** Real-time stats from actual data queries

### Extensibility

- **Custom Rules:** Plugin system for custom rule types
- **Data Sources:** Connectors for various data sources
- **Export Formats:** Multiple output formats (JSON, CSV, PDF)
- **Integration:** APIs for external tool integration
- **SQL Engines:** Support for DuckDB, PostgreSQL, MySQL, etc.
- **Graph Algorithms:** Additional traversal strategies (BFS, DFS, A*)
- **Prompt Templates:** Parameterized, customizable prompts

---

## Conclusion

The RCA Engine provides a **complete, automated solution** for root cause analysis of data discrepancies. By combining:

- **Row-level analysis** for precision
- **Lineage tracing** for visibility
- **AI-powered reasoning** for intelligence
- **Mathematical verification** for trust
- **Performance optimizations** for scale
- **Dynamic graph traversal** for adaptive investigation (NEW)
- **Rich metadata integration** for intelligent decisions (NEW)
- **Hypergraph statistics** for informed exploration (NEW)
- **System prompts** for consistent LLM behavior (NEW)

It transforms the **manual, time-consuming process** of RCA into an **automated, reliable, and auditable** system that provides actionable insights in minutes instead of hours or days.

### Dual-Mode Architecture

The system now supports **two complementary approaches**:

1. **Fixed Pipeline (RCA Cursor):** Comprehensive, deterministic analysis
   - Always runs all phases
   - Complete audit trail
   - Best for production reconciliation

2. **Dynamic Graph Traversal (Graph Traversal Agent):** Adaptive, intelligent exploration
   - Chooses next step based on observations
   - Early termination when root cause found
   - Best for rapid debugging

### Key Innovations

- **Knowledge Graph Navigation:** Agent navigates graph of Tables, Rules, Joins, Filters, Metrics
- **Small SQL Probes:** Fast iteration with focused queries (LIMIT 100)
- **LLM-Guided Decisions:** System prompts ensure consistent, intelligent behavior
- **Rich Metadata:** LLM receives full context (columns, rules, stats) at each node
- **Hypergraph Integration:** Leverages advanced graph features (statistics, path finding)

The system is **production-ready**, with comprehensive error handling, performance optimizations, and trust mechanisms that ensure reliability and reproducibility for critical business use cases.

### Traverse → Test → Observe → Decide → Repeat

The RCA Engine is now a **knowledge-graph-aware explorer agent** that:

**✅ Writes Real Short Queries:**
- LLM generates focused SQL probes (LIMIT 100)
- Fast iteration with small, targeted queries
- Each query tests a specific hypothesis

**✅ Stores Gained Information Like an Explorer:**
- **`visited_path`**: Tracks all nodes explored (explorer's route map)
- **`findings`**: Stores all discoveries with full evidence (explorer's field notes)
- **`last_probe_result`**: Each node remembers its probe result (observations at each location)
- **`current_hypothesis`**: Maintains current understanding (explorer's working theory)
- **`nodes`**: Builds knowledge graph incrementally (explorer's map)

**✅ Reaches Conclusion:**
- Accumulates evidence from all probes
- Forms and updates hypotheses based on findings
- Stops when root cause found with high confidence
- Provides complete explanation with evidence trail

**Additional Capabilities:**
- ✅ Dynamically chooses next step based on observations
- ✅ Stops early when root cause found
- ✅ Only explores relevant paths
- ✅ Adapts to findings dynamically
- ✅ Leverages rich metadata and hypergraph statistics
- ✅ Makes intelligent decisions with LLM guidance

This transforms RCA from a **fixed pipeline** into an **intelligent, adaptive explorer** that:
- Writes short queries to probe the knowledge graph
- Stores all gained information incrementally
- Builds understanding step-by-step
- Reaches conclusions based on accumulated evidence

