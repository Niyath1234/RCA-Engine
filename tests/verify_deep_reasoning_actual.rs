//! Verify Deep Reasoning Detection Actually Works
//! 
//! This test actually calls the function and prints results to verify correctness

use rca_engine::agentic_prompts;

fn main() {
    println!("{}", "=".repeat(80));
    println!("VERIFYING DEEP REASONING DETECTION - ACTUAL RESULTS");
    println!("{}", "=".repeat(80));
    println!();
    
    // Test cases that SHOULD require deep reasoning
    println!("🔍 TESTS THAT SHOULD REQUIRE DEEP REASONING:");
    println!("{}", "-".repeat(80));
    
    let should_require = vec![
        ("Compare customer-level TOS between System A and System B", true),
        ("Calculate loan-level balance from daily transactions", true),
        ("Find the total outstanding amount", true),
        ("Aggregate daily interest accruals to get loan totals", true),
        ("Reconcile the difference between systems", true),
        ("Why is the balance different?", true),
        ("How do I aggregate daily data to loan level?", true),
        ("Find the loan identifier column", true),
        ("What columns should I use for this calculation?", true),
        ("Calculate TOS from multiple tables", true),
        ("I need to combine loan_summary with daily_interest_accruals to calculate TOS", true),
    ];
    
    let mut correct = 0;
    let mut incorrect = 0;
    
    for (problem, expected) in &should_require {
        let actual = agentic_prompts::requires_deep_reasoning(problem);
        let status = if actual == *expected { "✅" } else { "❌" };
        if actual == *expected {
            correct += 1;
        } else {
            incorrect += 1;
        }
        println!("{} Problem: \"{}\"", status, problem);
        println!("   Expected: {}, Got: {}", expected, actual);
        if actual != *expected {
            println!("   ⚠️  MISMATCH!");
        }
        println!();
    }
    
    println!("{}", "=".repeat(80));
    println!("🔍 TESTS THAT SHOULD NOT REQUIRE DEEP REASONING:");
    println!("{}", "-".repeat(80));
    
    let should_not_require = vec![
        ("SELECT loan_id, total_outstanding FROM loan_summary WHERE loan_id = 'L12345'", false),
        ("Query loan_summary table", false),
        ("Get loan_id column from loan_summary", false),
        ("Query the loan_summary table and get the total_outstanding column for loan_id L12345", false),
    ];
    
    for (problem, expected) in &should_not_require {
        let actual = agentic_prompts::requires_deep_reasoning(problem);
        let status = if actual == *expected { "✅" } else { "❌" };
        if actual == *expected {
            correct += 1;
        } else {
            incorrect += 1;
        }
        println!("{} Problem: \"{}\"", status, problem);
        println!("   Expected: {}, Got: {}", expected, actual);
        if actual != *expected {
            println!("   ⚠️  MISMATCH!");
        }
        println!();
    }
    
    println!("{}", "=".repeat(80));
    println!("📊 SUMMARY:");
    println!("{}", "-".repeat(80));
    println!("✅ Correct: {}", correct);
    println!("❌ Incorrect: {}", incorrect);
    println!("📈 Accuracy: {:.1}%", (correct as f64 / (correct + incorrect) as f64) * 100.0);
    println!("{}", "=".repeat(80));
    
    if incorrect > 0 {
        println!();
        println!("⚠️  SOME TESTS FAILED - DETECTION LOGIC NEEDS FIXING");
        std::process::exit(1);
    } else {
        println!();
        println!("✅ ALL TESTS PASSED - DETECTION LOGIC IS CORRECT");
    }
}

#[test]
fn test_verify_actual_results() {
    main();
}

