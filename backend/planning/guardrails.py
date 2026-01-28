"""
Planning Guardrails

Guardrails for each planning step.
"""

from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class PlanningGuardrails:
    """Guardrails for each planning step."""
    
    def __init__(self, known_metrics: Optional[List[str]] = None,
                 table_validator=None):
        """
        Initialize planning guardrails.
        
        Args:
            known_metrics: List of known/valid metrics
            table_validator: Function to validate table existence
        """
        self.known_metrics = known_metrics or []
        self.table_validator = table_validator
    
    def validate_intent(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate intent maps to known metrics.
        
        Args:
            intent: Intent dictionary
        
        Returns:
            Validation result dictionary
        """
        metric = intent.get('metric')
        
        if not metric:
            return {
                'valid': False,
                'error': 'Metric not specified in intent',
                'available_metrics': self.known_metrics,
            }
        
        if self.known_metrics and metric not in self.known_metrics:
            return {
                'valid': False,
                'error': f'Unknown metric: {metric}',
                'available_metrics': self.known_metrics,
            }
        
        return {'valid': True}
    
    def validate_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate tables exist.
        
        Args:
            schema: Schema dictionary
        
        Returns:
            Validation result dictionary
        """
        tables = schema.get('tables', [])
        
        if not tables:
            return {
                'valid': False,
                'error': 'No tables specified in schema',
            }
        
        # Validate each table exists
        if self.table_validator:
            for table in tables:
                if not self.table_validator(table):
                    return {
                        'valid': False,
                        'error': f'Table does not exist: {table}',
                    }
        
        return {'valid': True}
    
    def validate_joins(self, joins: List[Dict[str, Any]], schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate joins are legal.
        
        Args:
            joins: List of join dictionaries
            schema: Schema dictionary
        
        Returns:
            Validation result dictionary
        """
        # Extract table names from schema
        schema_tables = schema.get('tables', [])
        
        for join in joins:
            left_table = join.get('left_table')
            right_table = join.get('right_table')
            
            # Check tables exist
            if left_table not in schema_tables:
                return {
                    'valid': False,
                    'error': f'Left table in join does not exist: {left_table}',
                }
            
            if right_table not in schema_tables:
                return {
                    'valid': False,
                    'error': f'Right table in join does not exist: {right_table}',
                }
            
            # Check join condition exists
            if not join.get('condition'):
                return {
                    'valid': False,
                    'error': f'Join missing condition: {left_table} JOIN {right_table}',
                }
        
        return {'valid': True}
    
    def validate_metrics(self, metrics: List[str], schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate metrics are available in schema.
        
        Args:
            metrics: List of metric names
            schema: Schema dictionary
        
        Returns:
            Validation result dictionary
        """
        available_columns = schema.get('columns', [])
        
        for metric in metrics:
            # Check if metric exists in schema columns
            if available_columns and metric not in available_columns:
                # Check if it's an aggregation (e.g., SUM(column))
                if '(' in metric and ')' in metric:
                    # Extract column name from aggregation
                    import re
                    match = re.search(r'\(([^)]+)\)', metric)
                    if match:
                        column = match.group(1)
                        if column not in available_columns:
                            return {
                                'valid': False,
                                'error': f'Metric column not found: {column}',
                            }
                else:
                    return {
                        'valid': False,
                        'error': f'Metric not found in schema: {metric}',
                    }
        
        return {'valid': True}
    
    def validate_all(self, intent: Dict[str, Any], schema: Dict[str, Any],
                    metrics: List[str], joins: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Run all validations.
        
        Args:
            intent: Intent dictionary
            schema: Schema dictionary
            metrics: List of metrics
            joins: Optional list of joins
        
        Returns:
            Validation result dictionary
        """
        # Validate intent
        result = self.validate_intent(intent)
        if not result['valid']:
            return result
        
        # Validate schema
        result = self.validate_schema(schema)
        if not result['valid']:
            return result
        
        # Validate metrics
        result = self.validate_metrics(metrics, schema)
        if not result['valid']:
            return result
        
        # Validate joins if present
        if joins:
            result = self.validate_joins(joins, schema)
            if not result['valid']:
                return result
        
        return {'valid': True}

