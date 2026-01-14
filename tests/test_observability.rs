//! Tests for Phase 5: Observability & Debuggability
//! 
//! Tests execution trace collection, trace store, and debug endpoint functionality.

use rca_engine::core::observability::{
    ExecutionTrace, NodeExecution, TraceCollector, TraceStore, GLOBAL_TRACE_STORE,
};
use std::time::Duration;

#[test]
fn test_trace_collector_creation() {
    let collector = TraceCollector::new("test-request-123".to_string());
    let trace = collector.build();
    
    assert_eq!(trace.request_id, "test-request-123");
    assert_eq!(trace.nodes_executed.len(), 0);
    assert_eq!(trace.timings.len(), 0);
    assert_eq!(trace.row_counts.len(), 0);
}

#[test]
fn test_trace_collector_phase_tracking() {
    let mut collector = TraceCollector::new("test-request-456".to_string());
    
    // Start and end a phase
    collector.start_phase("validation");
    std::thread::sleep(Duration::from_millis(10)); // Small delay to ensure timing
    collector.end_phase("validation");
    
    let trace = collector.build();
    assert!(trace.timings.contains_key("validation"));
    assert!(trace.timings["validation"] >= Duration::from_millis(10));
}

#[test]
fn test_trace_collector_node_execution() {
    let mut collector = TraceCollector::new("test-request-789".to_string());
    
    collector.record_node_execution(
        "node_1".to_string(),
        "Scan".to_string(),
        Some(1000),
        true,
        None,
    );
    
    collector.record_node_execution(
        "node_2".to_string(),
        "Join".to_string(),
        Some(500),
        false,
        Some("Join failed".to_string()),
    );
    
    let trace = collector.build();
    assert_eq!(trace.nodes_executed.len(), 2);
    
    let node1 = &trace.nodes_executed[0];
    assert_eq!(node1.node_id, "node_1");
    assert_eq!(node1.node_type, "Scan");
    assert_eq!(node1.rows_processed, Some(1000));
    assert!(node1.success);
    
    let node2 = &trace.nodes_executed[1];
    assert_eq!(node2.node_id, "node_2");
    assert_eq!(node2.node_type, "Join");
    assert_eq!(node2.rows_processed, Some(500));
    assert!(!node2.success);
    assert_eq!(node2.error, Some("Join failed".to_string()));
}

#[test]
fn test_trace_collector_row_counts() {
    let mut collector = TraceCollector::new("test-request-row-counts".to_string());
    
    collector.record_row_count("system_a", 1000);
    collector.record_row_count("system_b", 2000);
    collector.record_row_count("diff_result", 50);
    
    let trace = collector.build();
    assert_eq!(trace.row_counts.get("system_a"), Some(&1000));
    assert_eq!(trace.row_counts.get("system_b"), Some(&2000));
    assert_eq!(trace.row_counts.get("diff_result"), Some(&50));
}

#[test]
fn test_trace_collector_filter_selectivity() {
    let mut collector = TraceCollector::new("test-request-selectivity".to_string());
    
    collector.record_filter_selectivity("system_a", 0.8);
    collector.record_filter_selectivity("system_b", 0.9);
    
    let trace = collector.build();
    assert_eq!(trace.filter_selectivity.get("system_a"), Some(&0.8));
    assert_eq!(trace.filter_selectivity.get("system_b"), Some(&0.9));
}

#[test]
fn test_trace_collector_confidence_progression() {
    let mut collector = TraceCollector::new("test-request-confidence".to_string());
    
    collector.record_confidence(0.5);
    collector.record_confidence(0.7);
    collector.record_confidence(0.9);
    
    let trace = collector.build();
    assert_eq!(trace.confidence_progression.len(), 3);
    assert_eq!(trace.confidence_progression[0], 0.5);
    assert_eq!(trace.confidence_progression[1], 0.7);
    assert_eq!(trace.confidence_progression[2], 0.9);
}

#[test]
fn test_trace_collector_grain_resolution_path() {
    let mut collector = TraceCollector::new("test-request-grain".to_string());
    
    let path = vec!["repayment".to_string(), "loan".to_string()];
    collector.set_grain_resolution_path(path.clone());
    
    let trace = collector.build();
    assert_eq!(trace.grain_resolution_path, Some(path));
}

#[test]
fn test_trace_store_basic_operations() {
    let store = TraceStore::new();
    
    let trace = ExecutionTrace::new("test-store-1".to_string());
    store.store(trace);
    
    let retrieved = store.get("test-store-1");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().request_id, "test-store-1");
    
    let not_found = store.get("non-existent");
    assert!(not_found.is_none());
}

#[test]
fn test_trace_store_list_request_ids() {
    let store = TraceStore::new();
    
    store.store(ExecutionTrace::new("id-1".to_string()));
    store.store(ExecutionTrace::new("id-2".to_string()));
    store.store(ExecutionTrace::new("id-3".to_string()));
    
    let ids = store.list_request_ids();
    assert!(ids.contains(&"id-1".to_string()));
    assert!(ids.contains(&"id-2".to_string()));
    assert!(ids.contains(&"id-3".to_string()));
    assert_eq!(ids.len(), 3);
}

#[test]
fn test_trace_store_count() {
    let store = TraceStore::new();
    
    assert_eq!(store.count(), 0);
    
    store.store(ExecutionTrace::new("count-1".to_string()));
    assert_eq!(store.count(), 1);
    
    store.store(ExecutionTrace::new("count-2".to_string()));
    assert_eq!(store.count(), 2);
}

#[test]
fn test_trace_store_clear() {
    let store = TraceStore::new();
    
    store.store(ExecutionTrace::new("clear-1".to_string()));
    store.store(ExecutionTrace::new("clear-2".to_string()));
    assert_eq!(store.count(), 2);
    
    store.clear();
    assert_eq!(store.count(), 0);
}

#[test]
fn test_global_trace_store() {
    // Clear any existing traces
    GLOBAL_TRACE_STORE.clear();
    
    let trace = ExecutionTrace::new("global-test-1".to_string());
    GLOBAL_TRACE_STORE.store(trace);
    
    let retrieved = GLOBAL_TRACE_STORE.get("global-test-1");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().request_id, "global-test-1");
}

#[test]
fn test_execution_trace_serialization() {
    let mut trace = ExecutionTrace::new("serialization-test".to_string());
    
    trace.nodes_executed.push(NodeExecution {
        node_id: "node_1".to_string(),
        node_type: "Scan".to_string(),
        start_time: Some(Duration::from_secs(0)),
        end_time: Some(Duration::from_secs(1)),
        duration: Some(Duration::from_secs(1)),
        rows_processed: Some(1000),
        success: true,
        error: None,
    });
    
    trace.timings.insert("validation".to_string(), Duration::from_secs(2));
    trace.row_counts.insert("system_a".to_string(), 1000);
    trace.filter_selectivity.insert("filter_1".to_string(), 0.8);
    trace.confidence_progression.push(0.9);
    
    // Test serialization to JSON
    let json = serde_json::to_string(&trace).expect("Failed to serialize trace");
    assert!(json.contains("serialization-test"));
    assert!(json.contains("node_1"));
    assert!(json.contains("Scan"));
    
    // Test deserialization from JSON
    let deserialized: ExecutionTrace = serde_json::from_str(&json)
        .expect("Failed to deserialize trace");
    assert_eq!(deserialized.request_id, "serialization-test");
    assert_eq!(deserialized.nodes_executed.len(), 1);
}

#[test]
fn test_trace_collector_full_workflow() {
    let mut collector = TraceCollector::new("workflow-test".to_string());
    
    // Simulate a full RCA execution workflow
    collector.start_phase("validation");
    collector.record_node_execution("validation".to_string(), "Validation".to_string(), None, true, None);
    collector.end_phase("validation");
    
    collector.start_phase("logical_plan");
    collector.record_node_execution("plan_a".to_string(), "LogicalPlan".to_string(), None, true, None);
    collector.record_node_execution("plan_b".to_string(), "LogicalPlan".to_string(), None, true, None);
    collector.end_phase("logical_plan");
    
    collector.start_phase("execution");
    collector.record_node_execution("exec_a".to_string(), "Execution".to_string(), Some(1000), true, None);
    collector.record_row_count("system_a", 1000);
    collector.record_node_execution("exec_b".to_string(), "Execution".to_string(), Some(2000), true, None);
    collector.record_row_count("system_b", 2000);
    collector.record_filter_selectivity("system_a", 0.8);
    collector.end_phase("execution");
    
    collector.start_phase("diff");
    collector.record_node_execution("diff".to_string(), "GrainDiff".to_string(), Some(50), true, None);
    collector.record_row_count("diff_result", 50);
    collector.end_phase("diff");
    
    collector.record_confidence(0.9);
    collector.set_grain_resolution_path(vec!["repayment".to_string(), "loan".to_string()]);
    
    let trace = collector.build();
    
    // Verify all phases were tracked
    assert!(trace.timings.contains_key("validation"));
    assert!(trace.timings.contains_key("logical_plan"));
    assert!(trace.timings.contains_key("execution"));
    assert!(trace.timings.contains_key("diff"));
    
    // Verify nodes were recorded (validation + plan_a + plan_b + exec_a + exec_b + diff = 6)
    assert_eq!(trace.nodes_executed.len(), 6);
    
    // Verify row counts
    assert_eq!(trace.row_counts.get("system_a"), Some(&1000));
    assert_eq!(trace.row_counts.get("system_b"), Some(&2000));
    assert_eq!(trace.row_counts.get("diff_result"), Some(&50));
    
    // Verify filter selectivity
    assert_eq!(trace.filter_selectivity.get("system_a"), Some(&0.8));
    
    // Verify confidence
    assert_eq!(trace.confidence_progression.len(), 1);
    assert_eq!(trace.confidence_progression[0], 0.9);
    
    // Verify grain resolution path
    assert_eq!(
        trace.grain_resolution_path,
        Some(vec!["repayment".to_string(), "loan".to_string()])
    );
}

#[test]
fn test_trace_store_concurrent_access() {
    use std::sync::Arc;
    use std::thread;
    
    let store = Arc::new(TraceStore::new());
    store.clear();
    
    let mut handles = Vec::new();
    
    // Spawn multiple threads to store traces concurrently
    for i in 0..10 {
        let store_clone = store.clone();
        let handle = thread::spawn(move || {
            let trace = ExecutionTrace::new(format!("concurrent-{}", i));
            store_clone.store(trace);
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all traces were stored
    assert_eq!(store.count(), 10);
    
    // Verify we can retrieve all traces
    for i in 0..10 {
        let trace = store.get(&format!("concurrent-{}", i));
        assert!(trace.is_some());
        assert_eq!(trace.unwrap().request_id, format!("concurrent-{}", i));
    }
}

