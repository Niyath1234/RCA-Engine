"""
Query Endpoints

Query processing endpoints.
"""

from flask import Blueprint, request, jsonify
from backend.orchestrator import RCAEngineOrchestrator

query_router = Blueprint('query', __name__)

# Global orchestrator instance (would be injected in production)
_orchestrator: RCAEngineOrchestrator = None


def init_query(orchestrator: RCAEngineOrchestrator):
    """Initialize query endpoints with orchestrator."""
    global _orchestrator
    _orchestrator = orchestrator


@query_router.route('/query', methods=['POST'])
def process_query():
    """Process natural language query."""
    if not _orchestrator:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    data = request.get_json()
    if not data or 'query' not in data:
        return jsonify({
            'success': False,
            'error': 'Missing required field: query'
        }), 400
    
    user_query = data['query']
    user_id = data.get('user_id')
    context = data.get('context', {})
    
    result = _orchestrator.process_query(user_query, user_id, context)
    
    status_code = 200 if result.get('success') else 500
    return jsonify(result), status_code


@query_router.route('/query/batch', methods=['POST'])
def process_batch_query():
    """Process multiple queries in batch."""
    if not _orchestrator:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    data = request.get_json()
    if not data or 'queries' not in data:
        return jsonify({
            'success': False,
            'error': 'Missing required field: queries'
        }), 400
    
    queries = data['queries']
    user_id = data.get('user_id')
    context = data.get('context', {})
    
    results = []
    for query in queries:
        result = _orchestrator.process_query(query, user_id, context)
        results.append(result)
    
    return jsonify({
        'success': True,
        'results': results,
        'count': len(results)
    })

