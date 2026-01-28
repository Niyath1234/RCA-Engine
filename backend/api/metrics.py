"""
Metrics Endpoints

Metrics and monitoring endpoints.
"""

from flask import Blueprint, jsonify
from backend.orchestrator import RCAEngineOrchestrator

metrics_router = Blueprint('metrics', __name__)

# Global orchestrator instance (would be injected in production)
_orchestrator: RCAEngineOrchestrator = None


def init_metrics(orchestrator: RCAEngineOrchestrator):
    """Initialize metrics endpoints with orchestrator."""
    global _orchestrator
    _orchestrator = orchestrator


@metrics_router.route('/metrics', methods=['GET'])
def get_metrics():
    """Get current metrics."""
    if not _orchestrator:
        return jsonify({
            'error': 'Service not initialized'
        }), 503
    
    metrics = _orchestrator.get_metrics()
    return jsonify(metrics)


@metrics_router.route('/metrics/prometheus', methods=['GET'])
def prometheus_metrics():
    """Get metrics in Prometheus format."""
    if not _orchestrator:
        return '', 503
    
    metrics = _orchestrator.get_metrics()
    
    # Convert to Prometheus format
    prometheus_lines = []
    
    # Planning latency
    planning = metrics.get('planning_latency', {})
    if planning:
        prometheus_lines.append(f"# TYPE planning_latency_ms histogram")
        prometheus_lines.append(f"planning_latency_ms_count {planning.get('count', 0)}")
        prometheus_lines.append(f"planning_latency_ms_sum {planning.get('avg', 0) * planning.get('count', 0)}")
        prometheus_lines.append(f"planning_latency_ms_bucket{{le=\"+Inf\"}} {planning.get('count', 0)}")
    
    # Execution latency
    execution = metrics.get('execution_latency', {})
    if execution:
        prometheus_lines.append(f"# TYPE execution_latency_ms histogram")
        prometheus_lines.append(f"execution_latency_ms_count {execution.get('count', 0)}")
        prometheus_lines.append(f"execution_latency_ms_sum {execution.get('avg', 0) * execution.get('count', 0)}")
        prometheus_lines.append(f"execution_latency_ms_bucket{{le=\"+Inf\"}} {execution.get('count', 0)}")
    
    # Failure reasons
    failures = metrics.get('failure_reason', {})
    for reason, count in failures.items():
        prometheus_lines.append(f"failure_reason_total{{reason=\"{reason}\"}} {count}")
    
    return '\n'.join(prometheus_lines), 200, {'Content-Type': 'text/plain'}

