//! Test for RcaCursor Logical Plan Builder and Execution Planner

use rca_engine::core::agent::rca_cursor::{
    TaskValidator, RcaTask, ExecutionMode,
    LogicalPlanBuilder, ExecutionPlanner,
};
use rca_engine::metadata::{Metadata, Entity, Table, Rule, ComputationDefinition, Metric};
use std::collections::HashMap;

/// Create a minimal test metadata
fn create_test_metadata() -> Metadata {
    use std::collections::HashMap;
    
    let entities = vec![
        Entity {
            id: "repayment".to_string(),
            name: "repayment".to_string(),
            description: "Repayment entity".to_string(),
            grain: vec!["repayment_id".to_string()],
            attributes: vec!["amount".to_string(), "loan_id".to_string()],
        },
        Entity {
            id: "loan".to_string(),
            name: "loan".to_string(),
            description: "Loan entity".to_string(),
            grain: vec!["loan_id".to_string()],
            attributes: vec!["loan_id".to_string(), "customer_id".to_string()],
        },
    ];
    
    let tables = vec![
        Table {
            name: "repayments_a".to_string(),
            entity: "repayment".to_string(),
            primary_key: vec!["repayment_id".to_string()],
            time_column: "created_at".to_string(),
            system: "system_a".to_string(),
            path: "tables/repayments.csv".to_string(),
            columns: Some(vec![]),
            labels: None,
        },
        Table {
            name: "repayments_b".to_string(),
            entity: "repayment".to_string(),
            primary_key: vec!["repayment_id".to_string()],
            time_column: "created_at".to_string(),
            system: "system_b".to_string(),
            path: "tables/repayments.csv".to_string(),
            columns: Some(vec![]),
            labels: None,
        },
        Table {
            name: "loans_a".to_string(),
            entity: "loan".to_string(),
            primary_key: vec!["loan_id".to_string()],
            time_column: "created_at".to_string(),
            system: "system_a".to_string(),
            path: "tables/loans.csv".to_string(),
            columns: Some(vec![]),
            labels: None,
        },
        Table {
            name: "loans_b".to_string(),
            entity: "loan".to_string(),
            primary_key: vec!["loan_id".to_string()],
            time_column: "created_at".to_string(),
            system: "system_b".to_string(),
            path: "tables/loans.csv".to_string(),
            columns: Some(vec![]),
            labels: None,
        },
    ];
    
    let metrics = vec![
        Metric {
            id: "total_amount".to_string(),
            name: "Total Amount".to_string(),
            description: "Total repayment amount".to_string(),
            grain: vec!["loan_id".to_string()],
            precision: 2,
            null_policy: "zero".to_string(),
            unit: "currency".to_string(),
            versions: vec![],
        },
    ];
    
    let rules = vec![
        Rule {
            id: "rule_a".to_string(),
            system: "system_a".to_string(),
            metric: "total_amount".to_string(),
            target_entity: "repayment".to_string(),
            target_grain: vec!["loan_id".to_string()],
            computation: ComputationDefinition {
                description: "Sum repayment amounts".to_string(),
                source_entities: vec!["repayment".to_string()],
                attributes_needed: HashMap::new(),
                formula: "SUM(amount)".to_string(),
                aggregation_grain: vec!["loan_id".to_string()],
                filter_conditions: None,
                source_table: Some("repayments_a".to_string()),
                note: None,
            },
            labels: None,
        },
        Rule {
            id: "rule_b".to_string(),
            system: "system_b".to_string(),
            metric: "total_amount".to_string(),
            target_entity: "repayment".to_string(),
            target_grain: vec!["loan_id".to_string()],
            computation: ComputationDefinition {
                description: "Sum repayment amounts".to_string(),
                source_entities: vec!["repayment".to_string()],
                attributes_needed: HashMap::new(),
                formula: "SUM(amount)".to_string(),
                aggregation_grain: vec!["loan_id".to_string()],
                filter_conditions: None,
                source_table: Some("repayments_b".to_string()),
                note: None,
            },
            labels: None,
        },
    ];
    
    let lineage = rca_engine::metadata::LineageObject {
        edges: vec![],
        possible_joins: vec![],
    };
    
    let business_labels = rca_engine::metadata::BusinessLabelObject {
        systems: vec![],
        metrics: vec![],
        reconciliation_types: vec![],
    };
    
    let time_rules = rca_engine::metadata::TimeRules {
        as_of_rules: vec![],
        lateness_rules: vec![],
    };
    
    let identity = rca_engine::metadata::IdentityObject {
        canonical_keys: vec![],
        key_mappings: vec![],
    };
    
    let exceptions = rca_engine::metadata::ExceptionsObject {
        exceptions: vec![],
    };
    
    // Build indexes
    let tables_by_name: HashMap<_, _> = tables.iter()
        .map(|t| (t.name.clone(), t.clone()))
        .collect();
    
    let mut tables_by_entity: HashMap<_, _> = HashMap::new();
    for table in &tables {
        tables_by_entity
            .entry(table.entity.clone())
            .or_insert_with(Vec::new)
            .push(table.clone());
    }
    
    let mut tables_by_system: HashMap<_, _> = HashMap::new();
    for table in &tables {
        tables_by_system
            .entry(table.system.clone())
            .or_insert_with(Vec::new)
            .push(table.clone());
    }
    
    let rules_by_id: HashMap<_, _> = rules.iter()
        .map(|r| (r.id.clone(), r.clone()))
        .collect();
    
    let mut rules_by_system_metric = HashMap::new();
    for rule in &rules {
        rules_by_system_metric
            .entry((rule.system.clone(), rule.metric.clone()))
            .or_insert_with(Vec::new)
            .push(rule.clone());
    }
    
    let metrics_by_id: HashMap<_, _> = metrics.iter()
        .map(|m| (m.id.clone(), m.clone()))
        .collect();
    
    let entities_by_id: HashMap<_, _> = entities.iter()
        .map(|e| (e.id.clone(), e.clone()))
        .collect();
    
    Metadata {
        entities,
        tables,
        metrics,
        business_labels,
        rules,
        lineage,
        time_rules,
        identity,
        exceptions,
        tables_by_name,
        tables_by_entity,
        tables_by_system,
        rules_by_id,
        rules_by_system_metric,
        metrics_by_id,
        entities_by_id,
    }
}

#[test]
fn test_logical_plan_builder() {
    let metadata = create_test_metadata();
    
    // Create a task (use repayment as grain since that's the base entity)
    let task = RcaTask {
        metric: "total_amount".to_string(),
        system_a: "system_a".to_string(),
        system_b: "system_b".to_string(),
        grain: "repayment".to_string(), // Use repayment as grain (same as base entity)
        filters: vec![],
        time_window: None,
        mode: ExecutionMode::Fast,
    };

    // Validate the task
    let validator = TaskValidator::new(metadata.clone()).expect("Failed to create validator");
    let validated_task = validator.validate(task).expect("Task validation failed");

    // Build logical plans
    let plan_builder = LogicalPlanBuilder::new(metadata);
    let (plan_a, plan_b) = plan_builder.build_plans(&validated_task)
        .expect("Failed to build logical plans");

    // Verify plans were created
    println!("✅ Logical plans created successfully");
    println!("Plan A: {:?}", plan_a);
    println!("Plan B: {:?}", plan_b);
    
    // Basic checks
    assert!(matches!(plan_a, rca_engine::core::engine::logical_plan::LogicalPlan::Aggregate { .. }));
    assert!(matches!(plan_b, rca_engine::core::engine::logical_plan::LogicalPlan::Aggregate { .. }));
}

#[test]
fn test_execution_planner() {
    let metadata = create_test_metadata();
    
    // Create a task (use repayment as grain since that's the base entity)
    let task = RcaTask {
        metric: "total_amount".to_string(),
        system_a: "system_a".to_string(),
        system_b: "system_b".to_string(),
        grain: "repayment".to_string(), // Use repayment as grain (same as base entity)
        filters: vec![],
        time_window: None,
        mode: ExecutionMode::Fast,
    };

    // Validate the task
    let validator = TaskValidator::new(metadata.clone()).expect("Failed to create validator");
    let validated_task = validator.validate(task).expect("Task validation failed");

    // Build logical plans
    let plan_builder = LogicalPlanBuilder::new(metadata.clone());
    let (logical_plan_a, logical_plan_b) = plan_builder.build_plans(&validated_task)
        .expect("Failed to build logical plans");

    // Create execution plans
    let execution_planner = ExecutionPlanner::new(metadata);
    let (exec_plan_a, exec_plan_b) = execution_planner.plan_execution(
        &validated_task,
        &logical_plan_a,
        &logical_plan_b,
    ).expect("Failed to create execution plans");

    // Verify execution plans were created
    println!("✅ Execution plans created successfully");
    println!("Execution Plan A nodes: {}", exec_plan_a.nodes.len());
    println!("Execution Plan B nodes: {}", exec_plan_b.nodes.len());
    println!("Stop conditions: {:?}", exec_plan_a.stop_conditions);
    println!("Cost budget: {}", exec_plan_a.cost_budget);
    
    // Basic checks
    assert!(!exec_plan_a.nodes.is_empty());
    assert!(!exec_plan_b.nodes.is_empty());
    assert_eq!(exec_plan_a.cost_budget, 100.0); // Fast mode cost budget
    assert!(exec_plan_a.stop_conditions.max_rows.is_some());
}

#[test]
fn test_different_execution_modes() {
    let metadata = create_test_metadata();
    
    for mode in [ExecutionMode::Fast, ExecutionMode::Deep, ExecutionMode::Forensic] {
        let task = RcaTask {
            metric: "total_amount".to_string(),
            system_a: "system_a".to_string(),
            system_b: "system_b".to_string(),
            grain: "repayment".to_string(), // Use repayment as grain (same as base entity)
            filters: vec![],
            time_window: None,
            mode: mode.clone(),
        };

        let validator = TaskValidator::new(metadata.clone()).expect("Failed to create validator");
        let validated_task = validator.validate(task).expect("Task validation failed");

        let plan_builder = LogicalPlanBuilder::new(metadata.clone());
        let (logical_plan_a, logical_plan_b) = plan_builder.build_plans(&validated_task)
            .expect("Failed to build logical plans");

        let execution_planner = ExecutionPlanner::new(metadata.clone());
        let (exec_plan_a, _exec_plan_b) = execution_planner.plan_execution(
            &validated_task,
            &logical_plan_a,
            &logical_plan_b,
        ).expect("Failed to create execution plans");

        println!("Mode: {:?}, Cost budget: {}, Max rows: {:?}", 
                 mode, exec_plan_a.cost_budget, exec_plan_a.stop_conditions.max_rows);
        
        // Verify mode-specific settings
        match mode {
            ExecutionMode::Fast => {
                assert_eq!(exec_plan_a.cost_budget, 100.0);
                assert_eq!(exec_plan_a.stop_conditions.max_rows, Some(1_000_000));
            }
            ExecutionMode::Deep => {
                assert_eq!(exec_plan_a.cost_budget, 1000.0);
                assert_eq!(exec_plan_a.stop_conditions.max_rows, Some(10_000_000));
            }
            ExecutionMode::Forensic => {
                assert_eq!(exec_plan_a.cost_budget, 10000.0);
                assert!(exec_plan_a.stop_conditions.max_rows.is_none());
            }
        }
    }
    
    println!("✅ All execution modes tested successfully");
}

