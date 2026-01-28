"""
Planning Plane

Handle intent extraction and SQL generation.
SLA: < 5s latency
Failure Mode: Return error, cache fallback
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from datetime import datetime


@dataclass
class PlanningResult:
    """Result from planning plane."""
    success: bool
    planning_id: str
    sql: Optional[str] = None
    intent: Optional[Dict[str, Any]] = None
    schema: Optional[Dict[str, Any]] = None
    metrics: Optional[List[str]] = None
    steps: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    timestamp: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'success': self.success,
            'planning_id': self.planning_id,
            'timestamp': self.timestamp or datetime.utcnow().isoformat(),
        }
        if self.sql:
            result['sql'] = self.sql
        if self.intent:
            result['intent'] = self.intent
        if self.schema:
            result['schema'] = self.schema
        if self.metrics:
            result['metrics'] = self.metrics
        if self.steps:
            result['steps'] = self.steps
        if self.error:
            result['error'] = self.error
        if self.error_code:
            result['error_code'] = self.error_code
        return result


class PlanningPlane:
    """Handle intent extraction and SQL generation."""
    
    def __init__(self, multi_step_planner=None, guardrails=None, cache=None):
        """
        Initialize planning plane.
        
        Args:
            multi_step_planner: Multi-step planner instance
            guardrails: Planning guardrails instance
            cache: Cache for intent/planning results
        """
        self.multi_step_planner = multi_step_planner
        self.guardrails = guardrails
        self.cache = cache
    
    def plan_query(self, user_query: str, context: Dict[str, Any]) -> PlanningResult:
        """
        Generate SQL from user query.
        
        Args:
            user_query: User's natural language query
            context: Context dictionary (user_id, request_id, etc.)
        
        Returns:
            PlanningResult
        """
        planning_id = self._generate_planning_id()
        
        # Check cache first
        if self.cache:
            cache_key = self._generate_cache_key(user_query, context)
            cached_result = self.cache.get(cache_key)
            if cached_result:
                return PlanningResult(
                    success=True,
                    planning_id=planning_id,
                    sql=cached_result.get('sql'),
                    intent=cached_result.get('intent'),
                    schema=cached_result.get('schema'),
                    metrics=cached_result.get('metrics'),
                    steps=[{'step': 'cache_hit', 'data': cached_result}],
                    timestamp=datetime.utcnow().isoformat()
                )
        
        # Use multi-step planner
        if not self.multi_step_planner:
            return PlanningResult(
                success=False,
                planning_id=planning_id,
                error='Planning service not available',
                error_code='PLANNING_UNAVAILABLE',
                timestamp=datetime.utcnow().isoformat()
            )
        
        try:
            result = self.multi_step_planner.plan(user_query, context)
            
            # Cache result
            if self.cache and result.success:
                cache_key = self._generate_cache_key(user_query, context)
                self.cache.set(cache_key, {
                    'sql': result.sql,
                    'intent': result.intent,
                    'schema': result.schema,
                    'metrics': result.metrics,
                }, ttl=3600)
            
            return result
            
        except Exception as e:
            return PlanningResult(
                success=False,
                planning_id=planning_id,
                error=str(e),
                error_code='PLANNING_ERROR',
                timestamp=datetime.utcnow().isoformat()
            )
    
    def _generate_planning_id(self) -> str:
        """Generate unique planning ID."""
        import uuid
        return str(uuid.uuid4())
    
    def _generate_cache_key(self, query: str, context: Dict[str, Any]) -> str:
        """Generate cache key from query and context."""
        import hashlib
        import json
        
        key_data = {
            'query': query,
            'context': context.get('metadata_version', ''),
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()

