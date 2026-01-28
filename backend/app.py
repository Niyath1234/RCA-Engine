"""
RCA Engine Flask Application

Main Flask application that integrates all components.
"""

from flask import Flask
from flask_cors import CORS
from backend.orchestrator import RCAEngineOrchestrator
from backend.api import health_router, query_router, metrics_router

app = Flask(__name__)
CORS(app)

# Initialize orchestrator
orchestrator = RCAEngineOrchestrator()

# Initialize API routers
health_router.init_health(orchestrator)
query_router.init_query(orchestrator)
metrics_router.init_metrics(orchestrator)

# Register blueprints
app.register_blueprint(health_router, url_prefix='/api/v1')
app.register_blueprint(query_router, url_prefix='/api/v1')
app.register_blueprint(metrics_router, url_prefix='/api/v1')


@app.route('/', methods=['GET'])
def root():
    """Root endpoint."""
    return {
        'service': 'RCA Engine',
        'version': '1.0.0',
        'status': 'operational',
        'endpoints': {
            'health': '/api/v1/health',
            'query': '/api/v1/query',
            'metrics': '/api/v1/metrics',
        }
    }


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)

