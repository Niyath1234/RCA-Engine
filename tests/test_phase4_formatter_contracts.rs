//! Phase 4: LLM Formatter Contract Tests
//! 
//! Tests for strict input and output contracts in the LLM formatter.

use rca_engine::core::rca::formatter_v2::{
    FormatterV2, FormatterInput, FormatterOutput, FormatterSummary,
    FormatterGrainDifference, FormatterAttribution, GrainInfo, DisplayFormat
};
use rca_engine::core::rca::result_v2::{RCAResult, RCASummary, GrainDifference, Attribution};
use rca_engine::error::Result;

/// Helper function to create a valid RCAResult for testing
fn create_valid_rca_result() -> RCAResult {
    RCAResult::new(
        "loan".to_string(),
        "loan_id".to_string(),
        RCASummary {
            total_grain_units: 100,
            missing_left_count: 5,
            missing_right_count: 3,
            mismatch_count: 10,
            aggregate_difference: 1500.0,
            top_k: 10,
        },
    )
    .with_differences(vec![
        GrainDifference {
            grain_value: vec!["L001".to_string()],
            value_a: 1000.0,
            value_b: 1200.0,
            delta: 200.0,
            impact: 200.0,
        },
        GrainDifference {
            grain_value: vec!["L002".to_string()],
            value_a: 500.0,
            value_b: 600.0,
            delta: 100.0,
            impact: 100.0,
        },
    ])
    .with_attributions(vec![
        Attribution {
            grain_value: vec!["L001".to_string()],
            impact: 200.0,
            contribution_percentage: 50.0,
            contributors: vec![],
            explanation_graph: std::collections::HashMap::new(),
        },
    ])
    .with_confidence(0.85)
}

#[test]
fn test_phase4_1_valid_input_contract() {
    // Test that valid input passes validation
    let formatter = FormatterV2::new();
    let result = create_valid_rca_result();
    
    let input = formatter.build_input_contract(&result, Some("Test question")).unwrap();
    let validation_result = formatter.validate_input(&input);
    
    assert!(validation_result.is_ok(), 
        "Valid input should pass validation. Error: {:?}", validation_result.err());
}

#[test]
fn test_phase4_1_input_missing_question() {
    // Test that missing question field fails validation
    let formatter = FormatterV2::new();
    let result = create_valid_rca_result();
    
    let mut input = formatter.build_input_contract(&result, Some("Test")).unwrap();
    
    // Serialize and deserialize to create invalid JSON
    let mut json = serde_json::to_value(&input).unwrap();
    json.as_object_mut().unwrap().remove("question");
    
    // Try to validate the invalid JSON
    let validation_result = formatter.validate_input_comprehensive(&json, &input);
    assert!(validation_result.is_err(), 
        "Missing question field should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("question"));
}

#[test]
fn test_phase4_1_input_invalid_confidence() {
    // Test that confidence outside 0-1 range fails validation
    let formatter = FormatterV2::new();
    let result = create_valid_rca_result();
    
    let mut input = formatter.build_input_contract(&result, Some("Test")).unwrap();
    input.confidence = 1.5; // Invalid: > 1.0
    
    let validation_result = formatter.validate_input(&input);
    assert!(validation_result.is_err(), 
        "Confidence > 1.0 should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("confidence"));
}

#[test]
fn test_phase4_1_input_invalid_contribution_percentage() {
    // Test that contribution_percentage outside 0-100 range fails validation
    let formatter = FormatterV2::new();
    let mut result = create_valid_rca_result();
    
    // Create invalid attribution with contribution > 100
    result.attributions = vec![
        Attribution {
            grain_value: vec!["L001".to_string()],
            impact: 200.0,
            contribution_percentage: 150.0, // Invalid: > 100
            contributors: vec![],
            explanation_graph: std::collections::HashMap::new(),
        },
    ];
    
    let input = formatter.build_input_contract(&result, Some("Test")).unwrap();
    let validation_result = formatter.validate_input(&input);
    assert!(validation_result.is_err(), 
        "Contribution percentage > 100 should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("contribution_percentage"));
}

#[test]
fn test_phase4_1_input_invalid_delta_consistency() {
    // Test that delta inconsistency fails validation
    let formatter = FormatterV2::new();
    let mut result = create_valid_rca_result();
    
    // Create difference with inconsistent delta
    result.top_differences = vec![
        GrainDifference {
            grain_value: vec!["L001".to_string()],
            value_a: 1000.0,
            value_b: 1200.0,
            delta: 300.0, // Invalid: should be 200.0 (1200 - 1000)
            impact: 200.0,
        },
    ];
    
    let input = formatter.build_input_contract(&result, Some("Test")).unwrap();
    let validation_result = formatter.validate_input(&input);
    assert!(validation_result.is_err(), 
        "Inconsistent delta should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("delta"));
}

#[test]
fn test_phase4_1_input_invalid_impact_consistency() {
    // Test that impact inconsistency fails validation
    let formatter = FormatterV2::new();
    let mut result = create_valid_rca_result();
    
    // Create difference with inconsistent impact
    result.top_differences = vec![
        GrainDifference {
            grain_value: vec!["L001".to_string()],
            value_a: 1000.0,
            value_b: 1200.0,
            delta: 200.0,
            impact: 150.0, // Invalid: should be 200.0 (abs(200))
        },
    ];
    
    let input = formatter.build_input_contract(&result, Some("Test")).unwrap();
    let validation_result = formatter.validate_input(&input);
    assert!(validation_result.is_err(), 
        "Inconsistent impact should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("impact"));
}

#[test]
fn test_phase4_1_input_empty_grain_values() {
    // Test that empty grain_values fails validation
    let formatter = FormatterV2::new();
    let mut result = create_valid_rca_result();
    
    // Create difference with empty grain_values
    result.top_differences = vec![
        GrainDifference {
            grain_value: vec![], // Invalid: empty
            value_a: 1000.0,
            value_b: 1200.0,
            delta: 200.0,
            impact: 200.0,
        },
    ];
    
    let input = formatter.build_input_contract(&result, Some("Test")).unwrap();
    let validation_result = formatter.validate_input(&input);
    assert!(validation_result.is_err(), 
        "Empty grain_values should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("grain_values"));
}

#[test]
fn test_phase4_1_input_empty_grain_info() {
    // Test that empty grain/grain_key fails validation
    let formatter = FormatterV2::new();
    let mut result = create_valid_rca_result();
    result.grain = String::new(); // Invalid: empty
    
    let input = formatter.build_input_contract(&result, Some("Test")).unwrap();
    let validation_result = formatter.validate_input(&input);
    assert!(validation_result.is_err(), 
        "Empty grain should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("grain"));
}

#[test]
fn test_phase4_2_valid_output_contract() {
    // Test that valid output passes validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::Summary,
        display_content: "This is a valid display content with sufficient length for testing.".to_string(),
        key_grain_units: vec![
            vec!["L001".to_string()],
            vec!["L002".to_string()],
        ],
        reasoning: Some("This is a valid reasoning.".to_string()),
    };
    
    let validation_result = formatter.validate_output(&output);
    assert!(validation_result.is_ok(), 
        "Valid output should pass validation. Error: {:?}", validation_result.err());
}

#[test]
fn test_phase4_2_output_missing_display_content() {
    // Test that empty display_content fails validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::Summary,
        display_content: String::new(), // Invalid: empty
        key_grain_units: vec![vec!["L001".to_string()]],
        reasoning: None,
    };
    
    let validation_result = formatter.validate_output(&output);
    assert!(validation_result.is_err(), 
        "Empty display_content should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("display_content"));
}

#[test]
fn test_phase4_2_output_short_display_content() {
    // Test that display_content shorter than 10 characters fails validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::Summary,
        display_content: "Short".to_string(), // Invalid: < 10 chars
        key_grain_units: vec![vec!["L001".to_string()]],
        reasoning: None,
    };
    
    let validation_result = formatter.validate_output(&output);
    assert!(validation_result.is_err(), 
        "Display content < 10 characters should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("too short"));
}

#[test]
fn test_phase4_2_output_narrative_too_short() {
    // Test that narrative format with content < 50 characters fails validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::Narrative,
        display_content: "This is a short narrative.".to_string(), // Invalid: < 50 chars for narrative
        key_grain_units: vec![vec!["L001".to_string()]],
        reasoning: None,
    };
    
    let validation_result = formatter.validate_output(&output);
    assert!(validation_result.is_err(), 
        "Narrative format with content < 50 characters should fail validation");
    let error_msg = validation_result.unwrap_err().to_string();
    assert!(error_msg.contains("narrative") || error_msg.contains("50"), 
        "Error message should mention narrative or 50 characters. Got: {}", error_msg);
}

#[test]
fn test_phase4_2_output_grain_focused_empty_key_units() {
    // Test that grain_focused format with empty key_grain_units fails validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::GrainFocused,
        display_content: "This is a valid display content with sufficient length.".to_string(),
        key_grain_units: vec![], // Invalid: empty for grain_focused
        reasoning: None,
    };
    
    let validation_result = formatter.validate_output(&output);
    assert!(validation_result.is_err(), 
        "Grain-focused format with empty key_grain_units should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("grain_focused"));
}

#[test]
fn test_phase4_2_output_empty_grain_unit() {
    // Test that empty grain unit arrays fail validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::Summary,
        display_content: "This is a valid display content with sufficient length.".to_string(),
        key_grain_units: vec![
            vec![], // Invalid: empty grain unit
        ],
        reasoning: None,
    };
    
    let validation_result = formatter.validate_output(&output);
    assert!(validation_result.is_err(), 
        "Empty grain unit should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("cannot be empty"));
}

#[test]
fn test_phase4_2_output_empty_grain_value() {
    // Test that empty grain values fail validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::Summary,
        display_content: "This is a valid display content with sufficient length.".to_string(),
        key_grain_units: vec![
            vec!["".to_string()], // Invalid: empty string
        ],
        reasoning: None,
    };
    
    let validation_result = formatter.validate_output(&output);
    assert!(validation_result.is_err(), 
        "Empty grain value should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("cannot be empty"));
}

#[test]
fn test_phase4_2_output_invalid_display_format() {
    // Test that invalid display_format enum fails validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::Summary,
        display_content: "This is a valid display content with sufficient length.".to_string(),
        key_grain_units: vec![vec!["L001".to_string()]],
        reasoning: None,
    };
    
    // Serialize and modify to invalid format
    let mut json = serde_json::to_value(&output).unwrap();
    json["display_format"] = serde_json::json!("invalid_format");
    
    let validation_result = formatter.validate_output_comprehensive(&json, &output);
    assert!(validation_result.is_err(), 
        "Invalid display_format should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("display_format"));
}

#[test]
fn test_phase4_2_output_whitespace_only_content() {
    // Test that whitespace-only display_content fails validation
    let formatter = FormatterV2::new();
    
    let output = FormatterOutput {
        display_format: DisplayFormat::Summary,
        display_content: "   \n\t  ".to_string(), // Invalid: whitespace only
        key_grain_units: vec![vec!["L001".to_string()]],
        reasoning: None,
    };
    
    let validation_result = formatter.validate_output(&output);
    assert!(validation_result.is_err(), 
        "Whitespace-only display_content should fail validation");
    assert!(validation_result.unwrap_err().to_string().contains("whitespace"));
}

#[test]
fn test_phase4_integration_valid_flow() {
    // Test the complete flow with valid data
    let formatter = FormatterV2::new();
    let result = create_valid_rca_result();
    
    // Build input contract
    let input = formatter.build_input_contract(&result, Some("Why are values different?")).unwrap();
    
    // Validate input
    assert!(formatter.validate_input(&input).is_ok(), 
        "Input validation should pass for valid data");
    
    // Create valid output
    let output = FormatterOutput {
        display_format: DisplayFormat::Summary,
        display_content: "The analysis found 10 mismatches with an aggregate difference of 1500.0. Top differences include L001 (delta: 200.0) and L002 (delta: 100.0).".to_string(),
        key_grain_units: vec![
            vec!["L001".to_string()],
            vec!["L002".to_string()],
        ],
        reasoning: Some("Summary format chosen because user asked about differences.".to_string()),
    };
    
    // Validate output
    assert!(formatter.validate_output(&output).is_ok(), 
        "Output validation should pass for valid data");
}

#[test]
fn test_phase4_integration_fallback_on_invalid_output() {
    // Test that fallback is used when output validation fails
    let formatter = FormatterV2::new();
    let result = create_valid_rca_result();
    
    // This should use fallback formatting since LLM is not available
    // and the fallback should work with valid RCAResult
    let formatted = formatter.format_fallback(&result, Some("Test question"));
    assert!(formatted.is_ok(), 
        "Fallback formatting should work for valid RCAResult");
    
    let formatted_result = formatted.unwrap();
    assert!(!formatted_result.display_content.is_empty(), 
        "Fallback should produce non-empty content");
    assert!(!formatted_result.key_identifiers.is_empty(), 
        "Fallback should extract key identifiers");
}

