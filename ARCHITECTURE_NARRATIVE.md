# RCA Engine Architecture: How Everything Works Together

## Overview

The RCA (Root Cause Analysis) Engine is a sophisticated system that combines **Node Registry**, **FAISS-powered fuzzy search**, **Knowledge and Metadata Registers**, and **Intent Compilation** to enable intelligent data reconciliation and root cause analysis. This document explains how all components work together in harmony.

---

## 🏗️ The Three-Layer Architecture

### Layer 1: Node Registry (The Foundation)

**What it is:**
The Node Registry is the central catalog that tracks every entity in your data ecosystem - tables, columns, metrics, rules, and relationships. Think of it as the "phone book" of your data universe.

**How it works:**

1. **Registration Process:**
   ```
   User uploads a table → System creates a Node
   ├── Generates unique Reference ID (UUID)
   ├── Stores basic metadata (name, type, created_at)
   └── Links to Knowledge and Metadata Registers
   ```

2. **Node Structure:**
   - **Reference ID**: Unique identifier (also serves as page ID in registers)
   - **Node Type**: table, metric, entity, etc.
   - **Name**: Human-readable name
   - **Metadata**: Additional properties (CSV path, primary keys, etc.)

3. **Example:**
   ```rust
   Node {
       ref_id: "550e8400-e29b-41d4-a716-446655440000",
       node_type: "table",
       name: "customer_accounts",
       created_at: "2024-01-15T10:30:00Z",
       metadata: {
           "csv_path": "data/customer_accounts.csv",
           "primary_keys": ["customer_id"]
       }
   }
   ```

---

### Layer 2: Knowledge & Metadata Registers (The Twin Memory System)

**The Concept:**
Every Node has **two pages** - one in the Knowledge Register (human-readable) and one in the Metadata Register (machine-readable). They share the same Reference ID, creating a bidirectional bridge.

#### Knowledge Register (Human-Readable Memory)

**Purpose:** Stores information that humans and LLMs can understand and search through.

**Structure:**
- **Full Text**: Complete description of the entity
- **Keywords**: Searchable terms extracted from descriptions
- **Segments**: Organized chunks of information with child references
  - Column descriptions (segment IDs: 1000-1999)
  - Table descriptions (segment IDs: 2000-2999)
  - Business rules (segment IDs: 3000-3999)
  - Relationships (segment IDs: 4000-4999)

**Example Knowledge Page:**
```json
{
  "page_id": "550e8400-e29b-41d4-a716-446655440000",
  "node_ref_id": "550e8400-e29b-41d4-a716-446655440000",
  "full_text": "Customer accounts table contains all customer account information including account status, branch codes, and activation flags.",
  "keywords": ["customer", "account", "branch", "status", "activation"],
  "segments": {
    "1001": {
      "segment_type": "ColumnDescription",
      "text_content": "customer_id: Unique identifier for each customer",
      "start_child_ref_id": 1001,
      "end_child_ref_id": 1002
    }
  }
}
```

#### Metadata Register (Machine-Readable Memory)

**Purpose:** Stores technical metadata that systems can programmatically access.

**Structure:**
- **Technical Data**: Structured information (column types, constraints, etc.)
- **Segments**: Technical metadata organized by type
- **Relationships**: Foreign keys, joins, lineage

**Example Metadata Page:**
```json
{
  "page_id": "550e8400-e29b-41d4-a716-446655440000",
  "node_ref_id": "550e8400-e29b-41d4-a716-446655440000",
  "technical_data": {
    "columns": [
      {"name": "customer_id", "type": "string", "primary_key": true},
      {"name": "branch_code", "type": "string"},
      {"name": "is_active", "type": "integer"}
    ],
    "primary_keys": ["customer_id"],
    "row_count": 125000
  },
  "segments": {
    "1001": {
      "segment_type": "ColumnMetadata",
      "start_child_ref_id": 1001,
      "end_child_ref_id": 1002
    }
  }
}
```

---

### Layer 3: FAISS-Powered Fuzzy Search (The Intelligent Finder)

**What is FAISS?**
FAISS (Facebook AI Similarity Search) is a library for efficient similarity search. Our implementation uses **character-level embeddings** to find similar table/column names even with typos or variations.

**How FAISS Search Works:**

1. **Index Building Phase:**
   ```
   System Startup → Build FAISS Index
   ├── Extract all table names from metadata
   ├── Extract all column names (grouped by table)
   ├── Convert names to 128-dim character embeddings
   └── Build fast lookup structure
   ```

2. **Search Process:**
   ```
   User searches "custmer_acounts" (typo)
   ├── Convert to embedding vector
   ├── FAISS finds top 10 similar candidates
   │   ├── "customer_accounts" (similarity: 0.92)
   │   ├── "customer_account" (similarity: 0.85)
   │   └── "account_customer" (similarity: 0.78)
   ├── Refine with string similarity (Levenshtein)
   └── Return best match: "customer_accounts"
   ```

3. **Hybrid Search Strategy:**
   - **FAISS similarity** (60% weight): Fast vector similarity
   - **String similarity** (40% weight): Exact character matching
   - **Combined score**: More accurate than either alone

**When FAISS is Used:**
- Only activated when you have **100+ tables** (performance threshold)
- Falls back to linear search for smaller datasets
- Used in Intent Validator to prevent hallucination

---

## 🔄 The Complete Flow: From Query to Answer

### Scenario: User asks "Find mismatch in minority_category between system_a and system_b"

#### Step 1: Query Reception
```
User Query → /api/reasoning/assess
└── Intent Compiler receives query
```

#### Step 2: Intent Compilation (Fail-Fast Mode)
```
Intent Compiler
├── Extracts entities: "minority_category", "system_a", "system_b"
├── Determines task type: "mismatch_detection"
├── Checks confidence: 0.85 (above 0.7 threshold)
└── Proceeds to validation
```

#### Step 3: Validation Against Metadata (Hallucination Prevention)
```
Intent Validator
├── For each entity in intent:
│   ├── Check if exists in metadata
│   ├── If not found:
│   │   ├── Use FAISS to find similar names
│   │   ├── "minority_category" → finds "minority_code" (similarity: 0.88)
│   │   └── Suggest correction to user
│   └── If found: proceed
└── Validate relationships exist
```

#### Step 4: Knowledge Base Search (If Needed)
```
Data Assistant (if user asks questions)
├── Search Knowledge Register for "minority_category"
│   ├── Grep search in full_text and keywords
│   ├── Find matching Knowledge Pages
│   └── Extract Reference IDs
├── Look up corresponding Nodes
├── Retrieve Metadata Pages for technical details
└── Build comprehensive answer
```

#### Step 5: Graph Construction
```
Hypergraph Builder
├── Load metadata (tables, rules, lineage)
├── Create nodes from tables
├── Create edges from lineage relationships
└── Enable graph traversal for RCA
```

#### Step 6: Query Execution
```
RCA Engine
├── Load data from CSV files
├── Apply rules for metric computation
├── Compare system_a vs system_b
├── Detect mismatches
└── Classify root causes
```

---

## 🎯 Key Features Explained

### 1. Node Registration Flow

**When a table is registered:**

```rust
POST /api/register/table
{
  "table_name": "customer_accounts",
  "csv_path": "data/customer_accounts.csv",
  "primary_keys": ["customer_id"],
  "column_descriptions": {
    "customer_id": "Unique customer identifier",
    "branch_code": "Branch where account was opened"
  }
}
```

**What happens internally:**

1. **Node Creation:**
   ```rust
   Node {
       ref_id: "uuid-generated",
       node_type: "table",
       name: "customer_accounts",
       ...
   }
   ```

2. **Knowledge Page Creation:**
   ```rust
   KnowledgePage {
       page_id: "same-uuid-as-node",
       full_text: "Customer accounts table...",
       keywords: ["customer", "account", "branch"],
       segments: {
           "1001": ColumnDescription("customer_id: Unique customer identifier"),
           "1002": ColumnDescription("branch_code: Branch where account was opened")
       }
   }
   ```

3. **Metadata Page Creation:**
   ```rust
   MetadataPage {
       page_id: "same-uuid-as-node",
       technical_data: {
           columns: [...],
           primary_keys: ["customer_id"],
           csv_path: "data/customer_accounts.csv"
       }
   }
   ```

4. **Registry Update:**
   - Node added to `nodes` HashMap
   - Knowledge Page added to `knowledge_register.pages`
   - Metadata Page added to `metadata_register.pages`
   - All share the same Reference ID

### 2. Search Flow

**When searching for "customer account":**

```rust
registry.search_all("customer account")
```

**Process:**

1. **Knowledge Register Search:**
   ```rust
   // Grep search in full_text and keywords
   for page in knowledge_register.pages {
       if page.full_text.contains("customer account") ||
          page.keywords.contains("customer") {
           matching_pages.push(page.page_id)
       }
   }
   ```

2. **Node Lookup:**
   ```rust
   // Find nodes by reference ID
   for page_id in matching_pages {
       if let Some(node) = nodes.get(page_id) {
           matching_nodes.push(node)
       }
   }
   ```

3. **Metadata Retrieval:**
   ```rust
   // Get technical details
   for page_id in matching_pages {
       if let Some(metadata) = metadata_register.pages.get(page_id) {
           matching_metadata.push(metadata)
       }
   }
   ```

4. **Return Results:**
   ```rust
   (matching_nodes, matching_knowledge_pages, matching_metadata_pages)
   ```

### 3. FAISS Integration in Intent Validation

**When validating "minority_category" (typo):**

```rust
// In IntentValidator
let best_match = if let Some(ref faiss) = self.faiss_matcher {
    // Step 1: FAISS finds candidates
    let candidates = faiss.find_similar_tables("minority_category", 10);
    // Returns: [
    //   ("minority_code", 0.88),
    //   ("minority_category", 0.92),  // Actually exists!
    //   ("social_category", 0.75)
    // ]
    
    // Step 2: Refine with string similarity
    candidates.iter()
        .map(|(name, faiss_sim)| {
            let string_sim = string_similarity(name, "minority_category");
            let combined_sim = faiss_sim * 0.6 + string_sim * 0.4;
            (name, combined_sim)
        })
        .max_by_key(|(_, sim)| (*sim * 1000.0) as u64)
}
```

**Result:** System finds "minority_category" exists, validates successfully.

### 4. Learning Store Integration

**When user approves a correction:**

```rust
POST /api/learning/approve
{
  "incorrect_name": "minority_cat",
  "correct_name": "minority_category",
  "correction_type": "column",
  "table_name": "customer_ind_info"
}
```

**What happens:**

1. **Store Correction:**
   ```rust
   LearningStore.learn_correction(
       "minority_cat",
       "minority_category",
       "column",
       Some("customer_ind_info")
   )
   ```

2. **Future Use:**
   ```rust
   // Next time user types "minority_cat"
   if let Some(correction) = learning_store.get_correction("minority_cat") {
       // Automatically use "minority_category"
       return correction.correct_name;
   }
   ```

3. **Validation Enhancement:**
   ```rust
   // In IntentValidator
   if let Some(learning_store) = &self.learning_store {
       if let Some(correction) = learning_store.get_correction(table_name) {
           // Use learned correction
           return Ok(correction.correct_name);
       }
   }
   ```

---

## 🔗 Component Interactions

### Data Assistant (Cursor-like AI)

**Flow:**
```
User Question → Data Assistant
├── Search Knowledge Register
├── Find relevant Nodes
├── Retrieve Metadata Pages
├── Query actual data (if needed)
└── Generate comprehensive answer
```

**Example:**
```
Q: "What is minority_category?"
A: Data Assistant searches Knowledge Register
   → Finds Knowledge Page for "customer_ind_info"
   → Retrieves segment describing "minority_category"
   → Answers: "Minority category is a classification field..."
```

### Intent Compiler + Validator

**Flow:**
```
Natural Language Query → Intent Compiler
├── Extract entities (tables, columns, metrics)
├── Determine task type
├── Check confidence
└── If confident → Intent Validator
    ├── Validate against metadata
    ├── Use FAISS for fuzzy matching
    ├── Check Learning Store for corrections
    └── Return validated intent or clarification request
```

### Graph Traversal Agent

**Flow:**
```
RCA Query → Graph Traversal Agent
├── Load Hypergraph from metadata
├── Start from root nodes
├── Traverse edges (relationships)
├── Query data at each step
└── Identify root causes
```

---

## 📊 Performance Optimizations

### 1. FAISS Index Building
- **When:** Only for 100+ tables
- **Why:** Overhead not worth it for small datasets
- **Benefit:** O(log n) search instead of O(n)

### 2. Caching
- **Node Registry:** Loaded once at startup
- **FAISS Index:** Built once, reused for all searches
- **Metadata:** Cached in memory

### 3. Lazy Loading
- **Knowledge Pages:** Loaded on-demand during search
- **Metadata Pages:** Loaded when technical details needed
- **Data Files:** Loaded only when querying

---

## 🎬 Real-World Example: Complete Flow

**User Query:** "Find mismatch in minority_category between core_system and reporting_system"

### Step-by-Step Execution:

1. **Query Reception** (server.rs)
   ```
   POST /api/reasoning/query
   → handle_request() receives query
   ```

2. **Intent Compilation** (intent_compiler.rs)
   ```
   IntentCompiler.compile_with_clarification()
   → Extracts: task_type="mismatch", metric="minority_category", 
      systems=["core_system", "reporting_system"]
   → Confidence: 0.88 (above threshold)
   ```

3. **Validation** (intent_validator.rs)
   ```
   IntentValidator.validate_against_metadata()
   → Checks "minority_category" exists
   → FAISS finds it in "customer_ind_info" table
   → Validates both systems exist
   → Returns: Valid intent
   ```

4. **Knowledge Search** (node_registry.rs)
   ```
   NodeRegistry.search_all("minority_category")
   → Searches Knowledge Register
   → Finds Knowledge Page for "customer_ind_info"
   → Retrieves column description segment
   → Returns: Node + Knowledge Page + Metadata Page
   ```

5. **RCA Execution** (rca.rs)
   ```
   RcaEngine.run()
   → Loads data from both systems
   → Applies rules for "minority_category"
   → Compares values
   → Detects mismatches
   → Classifies root causes
   ```

6. **Result Formatting** (server.rs)
   ```
   Format results with:
   → Mismatch details table
   → Root cause classifications
   → Recommendations
   → Returns JSON response
   ```

---

## 🎯 Key Design Principles

### 1. **Separation of Concerns**
- **Knowledge Register**: Human-readable, searchable
- **Metadata Register**: Machine-readable, structured
- **Node Registry**: Central catalog linking everything

### 2. **Fail-Fast with Clarification**
- Low confidence queries → Ask for clarification
- Prevents hallucination
- Improves accuracy

### 3. **Learning from User Corrections**
- User-approved corrections stored
- Future queries use learned mappings
- System gets smarter over time

### 4. **Performance at Scale**
- FAISS for large datasets (100+ tables)
- Linear search for small datasets
- Caching for frequently accessed data

### 5. **Bidirectional Linking**
- Reference ID connects Node → Knowledge Page → Metadata Page
- Search in Knowledge → Find Node → Get Metadata
- Search in Metadata → Find Node → Get Knowledge

---

## 🚀 Summary

The RCA Engine architecture is a **three-layer system**:

1. **Node Registry**: Central catalog of all entities
2. **Knowledge & Metadata Registers**: Twin memory systems (human + machine)
3. **FAISS Search**: Fast fuzzy matching for typos and variations

**Key Flow:**
```
User Query → Intent Compilation → Validation (FAISS) → 
Knowledge Search → Graph Traversal → RCA Execution → Results
```

**Everything is connected through Reference IDs**, creating a seamless bridge between human understanding (Knowledge Register) and machine execution (Metadata Register), with FAISS ensuring robust fuzzy matching even with typos or variations.

The system learns from user corrections, prevents hallucination through validation, and scales efficiently using FAISS for large datasets. It's a complete, production-ready architecture for intelligent data reconciliation and root cause analysis.

