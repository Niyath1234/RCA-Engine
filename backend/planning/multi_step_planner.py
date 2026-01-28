"""
Multi-Step Planner

Never do: User query → SQL
Instead: Intent → Schema → Metrics → Skeleton → SQL
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class PlanningStep:
    """Represents a step in the planning pipeline."""
    step_name: str
    input_data: Dict[str, Any]
    output_data: Dict[str, Any]
    timestamp: str
    duration_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'step': self.step_name,
            'input': self.input_data,
            'output': self.output_data,
            'timestamp': self.timestamp,
            'duration_ms': self.duration_ms,
        }


class MultiStepPlanner:
    """Multi-step planning with explicit stages."""
    
    def __init__(self, intent_extractor=None, schema_selector=None,
                 metric_resolver=None, query_builder=None, guardrails=None):
        """
        Initialize multi-step planner.
        
        Args:
            intent_extractor: Intent extraction service
            schema_selector: Schema selection service
            metric_resolver: Metric resolution service
            query_builder: Query building service
            guardrails: Planning guardrails instance
        """
        self.intent_extractor = intent_extractor
        self.schema_selector = schema_selector
        self.metric_resolver = metric_resolver
        self.query_builder = query_builder
        self.guardrails = guardrails
        self.step_logs = {}
    
    def plan(self, user_query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute multi-step planning pipeline.
        
        Args:
            user_query: User's natural language query
            context: Context dictionary
        
        Returns:
            Planning result dictionary
        """
        planning_id = self._generate_planning_id()
        steps = []
        
        try:
            # Step 1: Intent Extraction
            intent = self._extract_intent(user_query, context)
            step = self._log_step(planning_id, 'intent_extraction', {'query': user_query}, intent)
            steps.append(step)
            
            # Validate intent
            if self.guardrails:
                validation = self.guardrails.validate_intent(intent)
                if not validation['valid']:
                    raise ValueError(f"Intent validation failed: {validation.get('error')}")
            
            # Step 2: Schema Selection
            schema = self._select_schema(intent, context)
            step = self._log_step(planning_id, 'schema_selection', {'intent': intent}, schema)
            steps.append(step)
            
            # Validate schema
            if self.guardrails:
                validation = self.guardrails.validate_schema(schema)
                if not validation['valid']:
                    raise ValueError(f"Schema validation failed: {validation.get('error')}")
            
            # Step 3: Metric Resolution
            metrics = self._resolve_metrics(intent, schema)
            step = self._log_step(planning_id, 'metric_resolution', 
                                 {'intent': intent, 'schema': schema}, metrics)
            steps.append(step)
            
            # Step 4: Query Skeleton
            skeleton = self._build_query_skeleton(intent, schema, metrics)
            step = self._log_step(planning_id, 'query_skeleton',
                                 {'intent': intent, 'schema': schema, 'metrics': metrics},
                                 skeleton)
            steps.append(step)
            
            # Step 5: Final SQL
            sql = self._finalize_sql(skeleton, schema)
            step = self._log_step(planning_id, 'final_sql', {'skeleton': skeleton}, {'sql': sql})
            steps.append(step)
            
            return {
                'success': True,
                'planning_id': planning_id,
                'intent': intent,
                'schema': schema,
                'metrics': metrics,
                'sql': sql,
                'steps': [s.to_dict() for s in steps],
            }
            
        except Exception as e:
            logger.error(f"Planning failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'planning_id': planning_id,
                'error': str(e),
                'steps': [s.to_dict() for s in steps],
            }
    
    def _extract_intent(self, user_query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract intent from user query."""
        if self.intent_extractor:
            return self.intent_extractor.extract(user_query, context)
        
        # Fallback: simple extraction
        return {
            'query': user_query,
            'metric': None,
            'time_range': None,
            'aggregation': None,
        }
    
    def _select_schema(self, intent: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Select schema based on intent."""
        if self.schema_selector:
            return self.schema_selector.select(intent, context)
        
        # Fallback: use context schema
        return context.get('schema', {})
    
    def _resolve_metrics(self, intent: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
        """Resolve metrics from intent and schema."""
        if self.metric_resolver:
            return self.metric_resolver.resolve(intent, schema)
        
        # Fallback: extract from intent
        metric = intent.get('metric')
        return [metric] if metric else []
    
    def _build_query_skeleton(self, intent: Dict[str, Any], schema: Dict[str, Any],
                            metrics: List[str]) -> Dict[str, Any]:
        """Build query skeleton."""
        if self.query_builder:
            return self.query_builder.build_skeleton(intent, schema, metrics)
        
        # Fallback: simple skeleton
        return {
            'tables': schema.get('tables', []),
            'columns': metrics,
            'filters': intent.get('filters', []),
            'aggregations': intent.get('aggregation'),
        }
    
    def _finalize_sql(self, skeleton: Dict[str, Any], schema: Dict[str, Any]) -> str:
        """Finalize SQL from skeleton."""
        if self.query_builder:
            return self.query_builder.build_sql(skeleton, schema)
        
        # Fallback: generate simple SQL
        tables = skeleton.get('tables', [])
        columns = skeleton.get('columns', ['*'])
        
        if not tables:
            raise ValueError("No tables specified in skeleton")
        
        sql = f"SELECT {', '.join(columns)} FROM {tables[0]}"
        
        # Add WHERE clause if filters exist
        filters = skeleton.get('filters', [])
        if filters:
            sql += f" WHERE {' AND '.join(filters)}"
        
        # Add LIMIT
        sql += " LIMIT 1000"
        
        return sql
    
    def _log_step(self, planning_id: str, step_name: str,
                  input_data: Dict[str, Any], output_data: Dict[str, Any]) -> PlanningStep:
        """Log planning step for inspection."""
        start_time = datetime.utcnow()
        
        # Store step log
        if planning_id not in self.step_logs:
            self.step_logs[planning_id] = []
        
        step = PlanningStep(
            step_name=step_name,
            input_data=input_data,
            output_data=output_data,
            timestamp=start_time.isoformat(),
            duration_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
        )
        
        self.step_logs[planning_id].append(step)
        
        # Log to logger
        logger.info({
            'planning_id': planning_id,
            'step': step_name,
            'input': input_data,
            'output': output_data,
            'timestamp': step.timestamp,
        })
        
        return step
    
    def get_step_log(self, planning_id: str) -> List[Dict[str, Any]]:
        """Get step log for planning ID."""
        return [step.to_dict() for step in self.step_logs.get(planning_id, [])]
    
    def _generate_planning_id(self) -> str:
        """Generate unique planning ID."""
        import uuid
        return str(uuid.uuid4())

