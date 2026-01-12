//! Verify Deep Reasoning Prompt Content
//! 
//! This test verifies the actual prompt content is correct

use rca_engine::agentic_prompts;

#[test]
fn test_verify_prompt_content() {
    let prompt = agentic_prompts::get_deep_reasoning_prompt();
    
    println!("\n{}", "=".repeat(80));
    println!("DEEP REASONING PROMPT VERIFICATION");
    println!("{}", "=".repeat(80));
    println!();
    
    println!("📏 Prompt Length: {} characters", prompt.len());
    println!("📊 Estimated Tokens: ~{} tokens", prompt.len() / 4);
    println!();
    
    println!("🔍 Key Sections Check:");
    println!("{}", "-".repeat(80));
    
    let checks = vec![
        ("Autonomous reasoning", prompt.contains("autonomous") || prompt.contains("Autonomous")),
        ("Grain analysis", prompt.to_lowercase().contains("grain")),
        ("Aggregation reasoning", prompt.to_lowercase().contains("aggregat")),
        ("Semantic column identification", prompt.to_lowercase().contains("semantic")),
        ("Column identification", prompt.to_lowercase().contains("column")),
        ("Join strategy", prompt.to_lowercase().contains("join")),
        ("Business logic inference", prompt.to_lowercase().contains("business")),
        ("Question asking guidelines", prompt.to_lowercase().contains("question") || prompt.to_lowercase().contains("ask")),
        ("Workflow/process", prompt.to_lowercase().contains("workflow") || prompt.to_lowercase().contains("process") || prompt.to_lowercase().contains("step")),
        ("Token optimization", prompt.to_lowercase().contains("token") || prompt.to_lowercase().contains("optimiz") || prompt.to_lowercase().contains("abbrev")),
    ];
    
    let mut all_present = true;
    for (name, present) in &checks {
        let status = if *present { "✅" } else { "❌" };
        println!("{} {}", status, name);
        if !present {
            all_present = false;
        }
    }
    
    println!();
    println!("{}", "=".repeat(80));
    if all_present {
        println!("✅ PROMPT CONTAINS ALL REQUIRED SECTIONS");
    } else {
        println!("❌ PROMPT MISSING SOME SECTIONS");
    }
    println!("{}", "=".repeat(80));
    
    // Verify prompt is token-optimized
    assert!(prompt.len() < 10000, "Prompt should be under 10k chars for token optimization");
    assert!(prompt.len() > 2000, "Prompt should be comprehensive (over 2k chars)");
    
    // Verify all key sections present
    assert!(all_present, "Prompt should contain all required sections");
    
    println!();
    println!("📝 Sample from prompt (first 500 chars):");
    println!("{}", "-".repeat(80));
    println!("{}", &prompt.chars().take(500).collect::<String>());
    println!("...");
}

