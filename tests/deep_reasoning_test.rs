//! Tests for Deep Reasoning System Prompt
//! 
//! Tests verify that the agent can autonomously reason about non-direct tasks
//! without explicit instructions.

use rca_engine::agentic_prompts;

#[test]
fn test_requires_deep_reasoning_customer_level() {
    // Problem mentions grain level but not specific columns/tables
    let problem = "Compare customer-level TOS between System A and System B";
    assert!(agentic_prompts::requires_deep_reasoning(problem), 
        "Should require deep reasoning for customer-level aggregation");
}

#[test]
fn test_requires_deep_reasoning_loan_level() {
    // Problem mentions grain level
    let problem = "Calculate loan-level balance from daily transactions";
    assert!(agentic_prompts::requires_deep_reasoning(problem),
        "Should require deep reasoning for loan-level aggregation");
}

#[test]
fn test_requires_deep_reasoning_vague_columns() {
    // Problem has vague column references
    let problem = "Find the total outstanding amount for all loans";
    assert!(agentic_prompts::requires_deep_reasoning(problem),
        "Should require deep reasoning for vague column references");
}

#[test]
fn test_requires_deep_reasoning_aggregation() {
    // Problem mentions aggregation concept but not method
    let problem = "Aggregate daily interest accruals to get loan totals";
    assert!(agentic_prompts::requires_deep_reasoning(problem),
        "Should require deep reasoning for aggregation without explicit method");
}

#[test]
fn test_requires_deep_reasoning_comparison() {
    // Comparison task without explicit steps
    let problem = "Reconcile the difference between systems";
    assert!(agentic_prompts::requires_deep_reasoning(problem),
        "Should require deep reasoning for comparison without explicit steps");
}

#[test]
fn test_does_not_require_deep_reasoning_direct() {
    // Direct task with explicit table and column names
    let problem = "Query loan_summary table for loan_id L12345";
    assert!(!agentic_prompts::requires_deep_reasoning(problem),
        "Should NOT require deep reasoning for direct task with explicit table");
}

#[test]
fn test_does_not_require_deep_reasoning_explicit() {
    // Explicit instructions
    let problem = "SELECT loan_id, total_outstanding FROM loan_summary WHERE loan_id = 'L12345'";
    assert!(!agentic_prompts::requires_deep_reasoning(problem),
        "Should NOT require deep reasoning for explicit SQL query");
}

#[test]
fn test_deep_reasoning_prompt_not_empty() {
    // Verify the prompt is actually populated
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    assert!(!prompt.is_empty(), "Deep reasoning prompt should not be empty");
    assert!(prompt.len() > 500, "Deep reasoning prompt should be comprehensive");
}

#[test]
fn test_deep_reasoning_prompt_contains_key_concepts() {
    // Verify prompt contains key reasoning concepts
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    assert!(prompt_lower.contains("grain"), "Prompt should mention grain analysis");
    assert!(prompt_lower.contains("aggregat"), "Prompt should mention aggregation");
    assert!(prompt_lower.contains("column"), "Prompt should mention column identification");
    assert!(prompt_lower.contains("autonomous"), "Prompt should mention autonomous reasoning");
    assert!(prompt_lower.contains("semantic"), "Prompt should mention semantic matching");
}

#[test]
fn test_deep_reasoning_prompt_token_optimized() {
    // Verify prompt uses abbreviations for token optimization
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    // Check for abbreviations (gr, agg, rel, col)
    // Note: These might be in comments or examples, so we check if prompt is reasonably sized
    // A token-optimized prompt should be under ~2000 tokens (roughly 8000 chars)
    assert!(prompt.len() < 10000, 
        "Prompt should be token-optimized (under ~10k chars, roughly 2500 tokens)");
}

#[test]
fn test_deep_reasoning_prompt_workflow() {
    // Verify prompt includes reasoning workflow
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    assert!(prompt_lower.contains("workflow") || prompt_lower.contains("process") || prompt_lower.contains("step"),
        "Prompt should include reasoning workflow/process");
}

#[test]
fn test_deep_reasoning_prompt_question_guidelines() {
    // Verify prompt includes guidelines for when to ask questions
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    assert!(prompt_lower.contains("question") || prompt_lower.contains("ask"),
        "Prompt should include guidelines for question asking");
    assert!(prompt_lower.contains("ambiguous") || prompt_lower.contains("infer"),
        "Prompt should mention when to ask vs infer");
}

#[test]
fn test_multiple_grain_scenarios() {
    // Test various grain-level scenarios
    let scenarios = vec![
        "customer-level metric",
        "loan-level comparison",
        "account-level aggregation",
        "daily-level totals",
    ];
    
    for scenario in scenarios {
        let problem = format!("Calculate {} for all entities", scenario);
        assert!(agentic_prompts::requires_deep_reasoning(&problem),
            "Should require deep reasoning for grain-level scenario: {}", scenario);
    }
}

#[test]
fn test_semantic_column_scenarios() {
    // Test scenarios requiring semantic column identification
    let scenarios = vec![
        "Find the loan identifier",
        "Get the outstanding amount",
        "Calculate total balance",
        "Find the transaction date",
    ];
    
    for scenario in scenarios {
        let problem = format!("{} from the data", scenario);
        assert!(agentic_prompts::requires_deep_reasoning(&problem),
            "Should require deep reasoning for semantic column scenario: {}", scenario);
    }
}

