//! Integration tests for Deep Reasoning System
//! 
//! Tests verify that the agentic reasoner actually uses deep reasoning
//! when processing non-direct tasks.

use rca_engine::agentic_prompts;
use rca_engine::agentic_reasoner::AgenticReasoner;
use rca_engine::llm::LlmClient;
use rca_engine::graph::Hypergraph;
use rca_engine::metadata::Metadata;
use std::collections::HashMap;

/// Test that requires_deep_reasoning correctly identifies non-direct tasks
#[tokio::test]
async fn test_deep_reasoning_detection_integration() {
    // Test various scenarios
    let test_cases = vec![
        ("Compare customer-level TOS between systems", true),
        ("Calculate loan-level balance from daily transactions", true),
        ("Find the total outstanding amount", true),
        ("Aggregate daily interest accruals", true),
        ("Query loan_summary table for loan_id L12345", false),
        ("SELECT loan_id FROM loan_summary WHERE loan_id = 'L12345'", false),
    ];
    
    for (problem, expected) in test_cases {
        let result = agentic_prompts::requires_deep_reasoning(problem);
        assert_eq!(result, expected, 
            "Problem: '{}' - Expected deep reasoning: {}, Got: {}", 
            problem, expected, result);
    }
}

/// Test that deep reasoning prompt is comprehensive
#[test]
fn test_deep_reasoning_prompt_comprehensive() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    
    // Check prompt length (should be comprehensive but token-optimized)
    assert!(prompt.len() > 2000, "Prompt should be comprehensive");
    assert!(prompt.len() < 10000, "Prompt should be token-optimized");
    
    // Check for key sections
    let prompt_lower = prompt.to_lowercase();
    assert!(prompt_lower.contains("autonomous"), "Should mention autonomous reasoning");
    assert!(prompt_lower.contains("grain"), "Should cover grain analysis");
    assert!(prompt_lower.contains("aggregat"), "Should cover aggregation");
    assert!(prompt_lower.contains("semantic"), "Should cover semantic matching");
    assert!(prompt_lower.contains("column"), "Should cover column identification");
    assert!(prompt_lower.contains("workflow") || prompt_lower.contains("process"), 
        "Should include reasoning workflow");
}

/// Test that the prompt includes examples of autonomous reasoning
#[test]
fn test_prompt_includes_autonomous_examples() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    // Check for examples of what the agent should infer
    assert!(prompt_lower.contains("customer-level") || prompt_lower.contains("loan-level") ||
            prompt_lower.contains("aggregate") || prompt_lower.contains("infer"),
        "Prompt should include examples of autonomous reasoning");
}

/// Test token optimization strategies are present
#[test]
fn test_token_optimization_present() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    // Check that prompt mentions token optimization
    assert!(prompt_lower.contains("token") || prompt_lower.contains("optimiz") ||
            prompt_lower.contains("abbrev") || prompt_lower.contains("concise"),
        "Prompt should mention token optimization strategies");
}

/// Test that prompt distinguishes when to ask vs infer
#[test]
fn test_question_vs_infer_guidelines() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    // Should have clear guidelines on when to ask questions vs infer
    assert!(prompt_lower.contains("ask") || prompt_lower.contains("question"),
        "Prompt should mention question asking");
    assert!(prompt_lower.contains("infer") || prompt_lower.contains("autonomous"),
        "Prompt should mention inference");
    assert!(prompt_lower.contains("ambiguous") || prompt_lower.contains("only when"),
        "Prompt should specify when to ask questions");
}

/// Test edge cases for deep reasoning detection
#[test]
fn test_deep_reasoning_edge_cases() {
    // Empty string should not require deep reasoning (edge case)
    assert!(!agentic_prompts::requires_deep_reasoning(""), 
        "Empty string should not require deep reasoning");
    
    // Very explicit SQL should not require deep reasoning
    assert!(!agentic_prompts::requires_deep_reasoning(
        "SELECT loan_id, customer_id, total_outstanding FROM loan_summary WHERE loan_id = 'L12345'"),
        "Explicit SQL should not require deep reasoning");
    
    // Vague business question should require deep reasoning
    assert!(agentic_prompts::requires_deep_reasoning(
        "Why is the customer-level metric different between systems?"),
        "Vague business question should require deep reasoning");
}

/// Test that prompt covers all key reasoning capabilities
#[test]
fn test_prompt_covers_all_capabilities() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    let prompt_lower = prompt.to_lowercase();
    
    let required_capabilities = vec![
        "grain",
        "aggregat",
        "column",
        "join",
        "relationship",
        "business",
        "semantic",
    ];
    
    for capability in required_capabilities {
        assert!(prompt_lower.contains(capability),
            "Prompt should cover capability: {}", capability);
    }
}

