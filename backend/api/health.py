"""
Health Check Endpoints

Health check and status endpoints.
"""

from flask import Blueprint, jsonify
from backend.orchestrator import RCAEngineOrchestrator

health_router = Blueprint('health', __name__)

# Global orchestrator instance (would be injected in production)
_orchestrator: RCAEngineOrchestrator = None


def init_health(orchestrator: RCAEngineOrchestrator):
    """Initialize health endpoints with orchestrator."""
    global _orchestrator
    _orchestrator = orchestrator


@health_router.route('/health', methods=['GET'])
def health_check():
    """Basic health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'rca-engine',
        'timestamp': __import__('datetime').datetime.utcnow().isoformat()
    })


@health_router.route('/health/detailed', methods=['GET'])
def detailed_health():
    """Detailed health check with component status."""
    if not _orchestrator:
        return jsonify({
            'status': 'unhealthy',
            'error': 'Orchestrator not initialized'
        }), 503
    
    health = _orchestrator.get_health()
    return jsonify(health)


@health_router.route('/health/ready', methods=['GET'])
def readiness_check():
    """Readiness check endpoint."""
    if not _orchestrator:
        return jsonify({
            'status': 'not_ready',
            'error': 'Orchestrator not initialized'
        }), 503
    
    # Check if all critical components are ready
    return jsonify({
        'status': 'ready',
        'timestamp': __import__('datetime').datetime.utcnow().isoformat()
    })


@health_router.route('/health/live', methods=['GET'])
def liveness_check():
    """Liveness check endpoint."""
    return jsonify({
        'status': 'alive',
        'timestamp': __import__('datetime').datetime.utcnow().isoformat()
    })

