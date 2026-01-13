//! Real-World Examples Test for Deep Reasoning System
//! 
//! Tests with actual problem statements users would provide to verify
//! that the deep reasoning detection and prompts work correctly.

use rca_engine::agentic_prompts;

#[test]
fn test_real_world_customer_level_aggregation() {
    // Real scenario: User wants customer-level metrics but tables have loan_id
    let problem = "I need to compare customer-level total outstanding between System A and System B. The tables have loan_id but I need customer-level aggregation.";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep, 
        "Real-world customer-level aggregation should require deep reasoning");
    
    // Verify prompt contains relevant guidance
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    assert!(prompt.contains("customer") || prompt.contains("loan_id") || 
            prompt.contains("aggregate") || prompt.contains("grain"),
        "Prompt should guide on customer-level aggregation");
}

#[test]
fn test_real_world_vague_metric_query() {
    // Real scenario: User asks vaguely about a metric
    let problem = "Why is the balance different between the two systems?";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Vague metric query should require deep reasoning");
    
    // Should be able to infer: balance = metric, systems = System A vs B
    // But needs to find: which tables, which columns, how to calculate
}

#[test]
fn test_real_world_multi_grain_scenario() {
    // Real scenario: Daily data needs to be aggregated to loan level
    let problem = "Calculate the total outstanding at loan level from daily interest accruals and EMI schedule";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Multi-grain aggregation scenario should require deep reasoning");
    
    // Should infer:
    // - Target grain: loan_id
    // - Source grains: loan_id+date (daily_interest_accruals), loan_id+emi_number (emi_schedule)
    // - Aggregation needed: SUM for amounts, GROUP BY loan_id
}

#[test]
fn test_real_world_semantic_column_search() {
    // Real scenario: User doesn't know exact column names
    let problem = "Find the loan identifier column and the outstanding amount column to calculate TOS";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Semantic column search should require deep reasoning");
    
    // Should infer:
    // - "loan identifier" → loan_id, loan_key, loan_number (semantic match)
    // - "outstanding amount" → total_outstanding, outstanding, balance (semantic match)
}

#[test]
fn test_real_world_reconciliation_without_steps() {
    // Real scenario: User wants reconciliation but doesn't specify how
    let problem = "Reconcile the discrepancy in TOS calculation between System A and System B for loan L12345";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Reconciliation without explicit steps should require deep reasoning");
    
    // Should infer:
    // - Need to find calculation rules for both systems
    // - Need to understand table structures and grains
    // - Need to identify where discrepancy might occur
}

#[test]
fn test_real_world_explicit_query_should_not_require_deep() {
    // Real scenario: User provides explicit SQL/instructions
    let problem = "SELECT loan_id, customer_id, total_outstanding FROM loan_summary WHERE loan_id = 'L12345'";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(!requires_deep,
        "Explicit SQL query should NOT require deep reasoning");
}

#[test]
fn test_real_world_table_specific_query() {
    // Real scenario: User specifies exact table and column
    let problem = "Query the loan_summary table and get the total_outstanding column for loan_id L12345";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(!requires_deep,
        "Explicit table/column query should NOT require deep reasoning");
}

#[test]
fn test_real_world_grain_mismatch_detection() {
    // Real scenario: Implicit grain mismatch
    let problem = "I have daily transaction data with loan_id and date. I need customer-level totals. How do I aggregate?";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Grain mismatch scenario should require deep reasoning");
    
    // Should infer:
    // - Source grain: loan_id + date
    // - Target grain: customer_id (but need to find customer_id column)
    // - Need to: GROUP BY customer_id, aggregate loan_id first, then aggregate dates
}

#[test]
fn test_real_world_ambiguous_metric_definition() {
    // Real scenario: Multiple ways to calculate a metric
    let problem = "What is the total outstanding? Should I use the snapshot table or calculate from transactions?";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Ambiguous metric definition should require deep reasoning");
    
    // Should infer:
    // - Need to explore both options
    // - Compare snapshot vs calculated approach
    // - Understand business rules for TOS calculation
}

#[test]
fn test_real_world_join_strategy_inference() {
    // Real scenario: User doesn't specify join strategy
    let problem = "I need to combine loan_summary with daily_interest_accruals and emi_schedule to calculate TOS";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Join strategy inference should require deep reasoning");
    
    // Should infer:
    // - Need to check grains of each table
    // - Determine if pre-aggregation needed
    // - Find join keys
    // - Determine join order to avoid explosion
}

#[test]
fn test_real_world_business_rule_inference() {
    // Real scenario: User wants to understand calculation logic
    let problem = "How is TOS calculated in System A? I see multiple tables with amounts";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Business rule inference should require deep reasoning");
    
    // Should infer:
    // - Need to find calculation rules
    // - Understand table relationships
    // - Infer formula from table structures
}

#[test]
fn test_prompt_handles_real_world_scenarios() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    // Verify prompt addresses real-world scenarios
    let scenarios = vec![
        "customer",
        "loan",
        "aggregat",
        "grain",
        "column",
        "join",
        "semantic",
    ];
    
    for scenario in scenarios {
        assert!(prompt_lower.contains(scenario),
            "Prompt should address scenario: {}", scenario);
    }
}

#[test]
fn test_detection_accuracy_real_world_examples() {
    // Test a batch of real-world examples
    let examples = vec![
        // Should require deep reasoning
        ("Compare customer-level metrics", true),
        ("Why is the balance different?", true),
        ("How do I aggregate daily data to loan level?", true),
        ("Find the loan identifier column", true),
        ("Reconcile the difference", true),
        ("Calculate TOS from multiple tables", true),
        ("What columns should I use for this calculation?", true),
        
        // Should NOT require deep reasoning
        ("SELECT * FROM loan_summary", false),
        ("Query loan_summary table", false),
        ("Get loan_id column from loan_summary", false),
        ("SELECT loan_id, total_outstanding FROM loan_summary WHERE loan_id = 'L12345'", false),
    ];
    
    for (problem, expected) in examples {
        let result = agentic_prompts::requires_deep_reasoning(problem);
        assert_eq!(result, expected,
            "Problem: '{}' - Expected: {}, Got: {}", problem, expected, result);
    }
}

#[test]
fn test_prompt_guides_autonomous_reasoning() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    
    // Verify prompt explicitly guides autonomous reasoning
    assert!(prompt.contains("autonomous") || prompt.contains("infer") || 
            prompt.contains("reason") || prompt.contains("without explicit"),
        "Prompt should guide autonomous reasoning");
    
    // Verify prompt mentions specific examples
    let prompt_lower = prompt.to_lowercase();
    assert!(prompt_lower.contains("loan_id") || prompt_lower.contains("customer_id") ||
            prompt_lower.contains("aggregate") || prompt_lower.contains("grain"),
        "Prompt should include concrete examples");
}

#[test]
fn test_prompt_question_guidelines_are_clear() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    // Verify clear guidelines on when to ask questions
    assert!(prompt_lower.contains("ask") || prompt_lower.contains("question"),
        "Prompt should mention question asking");
    
    assert!(prompt_lower.contains("only when") || prompt_lower.contains("truly") ||
            prompt_lower.contains("ambiguous") || prompt_lower.contains("cannot infer"),
        "Prompt should specify when to ask questions");
    
    assert!(prompt_lower.contains("infer") || prompt_lower.contains("autonomous") ||
            prompt_lower.contains("reason"),
        "Prompt should emphasize inference over asking");
}

#[test]
fn test_complex_real_world_scenario() {
    // Complex real-world scenario combining multiple aspects
    let problem = "I need to compare customer-level total outstanding between System A and System B. \
                   System A calculates it from daily interest accruals and EMI schedule, \
                   while System B has a pre-aggregated snapshot. \
                   The tables have different grains - some have loan_id, some have loan_id+date, \
                   and I need customer_id level. How do I reconcile this?";
    
    let requires_deep = agentic_prompts::requires_deep_reasoning(problem);
    assert!(requires_deep,
        "Complex multi-aspect scenario should definitely require deep reasoning");
    
    // This scenario requires:
    // 1. Grain analysis (loan_id → customer_id)
    // 2. Aggregation reasoning (daily → loan → customer)
    // 3. Join strategy (multiple tables with different grains)
    // 4. Business logic inference (how each system calculates)
    // 5. Semantic understanding (TOS, outstanding, etc.)
}

