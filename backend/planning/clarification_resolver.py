"""
Clarification Resolver

Handles user responses to clarification questions and merges them into query intent.
"""

from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ClarificationResolver:
    """
    Resolves clarification answers and merges them into query intent.
    """
    
    def __init__(self, metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize clarification resolver.
        
        Args:
            metadata: Optional metadata for context
        """
        self.metadata = metadata or {}
    
    def merge_answers_into_intent(self, original_intent: Dict[str, Any],
                                  answers: Dict[str, Any],
                                  original_query: str) -> Dict[str, Any]:
        """
        Merge user answers into query intent.
        
        Args:
            original_intent: Original intent (may be partial/ambiguous)
            answers: User answers to clarification questions
                     Format: {"field": "value", "metric": "revenue", "time_range": "last 30 days"}
            original_query: Original user query
        
        Returns:
            Updated intent with answers merged in
        """
        intent = original_intent.copy() if original_intent else {}
        
        # Merge metric answer
        if 'metric' in answers:
            metric_name = answers['metric']
            # Find metric in semantic registry
            metric_def = self._find_metric(metric_name)
            if metric_def:
                intent['metric'] = {
                    'name': metric_def.get('name'),
                    'sql_expression': metric_def.get('sql_expression'),
                    'base_table': metric_def.get('base_table')
                }
                # Update query type if metric provided
                intent['query_type'] = 'metric'
                # Update base table from metric
                if metric_def.get('base_table'):
                    intent['base_table'] = metric_def.get('base_table')
            else:
                # Use as-is if not found in registry
                intent['metric'] = {'name': metric_name}
                intent['query_type'] = 'metric'
        
        # Merge time range answer
        if 'time_range' in answers:
            time_range = answers['time_range']
            intent['time_range'] = time_range
            intent['time_context'] = self._parse_time_range(time_range)
        
        # Merge base_table answer
        if 'base_table' in answers:
            intent['base_table'] = answers['base_table']
            intent['anchor_entity'] = answers['base_table']
        
        # Merge dimensions answer
        if 'dimensions' in answers:
            dimensions = answers['dimensions']
            if isinstance(dimensions, str):
                dimensions = [d.strip() for d in dimensions.split(',')]
            elif not isinstance(dimensions, list):
                dimensions = [dimensions]
            
            intent['dimensions'] = []
            intent['group_by'] = []
            
            for dim_name in dimensions:
                dim_def = self._find_dimension(dim_name)
                if dim_def:
                    intent['dimensions'].append({
                        'name': dim_def.get('name'),
                        'base_table': dim_def.get('base_table'),
                        'column': dim_def.get('column')
                    })
                    intent['group_by'].append(dim_def.get('name'))
                else:
                    # Use as-is if not found
                    intent['dimensions'].append({'name': dim_name})
                    intent['group_by'].append(dim_name)
        
        # Merge filters answer
        if 'filters' in answers:
            filters = answers['filters']
            if isinstance(filters, list):
                intent['filters'] = filters
            elif isinstance(filters, dict):
                intent['filters'] = [filters]
        
        # Merge columns answer (for relational queries)
        if 'columns' in answers:
            columns = answers['columns']
            if isinstance(columns, str):
                columns = [c.strip() for c in columns.split(',')]
            elif not isinstance(columns, list):
                columns = [columns]
            intent['columns'] = columns
        
        # Ensure query type is set
        if 'query_type' not in intent:
            # Infer from context
            if intent.get('metric'):
                intent['query_type'] = 'metric'
            else:
                intent['query_type'] = 'relational'
        
        logger.info(f"Merged answers into intent: {list(answers.keys())}")
        
        return intent
    
    def _find_metric(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Find metric definition in semantic registry."""
        semantic_registry = self.metadata.get('semantic_registry', {})
        metrics = semantic_registry.get('metrics', [])
        
        metric_name_lower = metric_name.lower()
        
        for metric in metrics:
            if metric.get('name', '').lower() == metric_name_lower:
                return metric
            # Check aliases
            aliases = metric.get('aliases', [])
            if any(alias.lower() == metric_name_lower for alias in aliases):
                return metric
        
        return None
    
    def _find_dimension(self, dimension_name: str) -> Optional[Dict[str, Any]]:
        """Find dimension definition in semantic registry."""
        semantic_registry = self.metadata.get('semantic_registry', {})
        dimensions = semantic_registry.get('dimensions', [])
        
        dimension_name_lower = dimension_name.lower()
        
        for dimension in dimensions:
            if dimension.get('name', '').lower() == dimension_name_lower:
                return dimension
            # Check aliases
            aliases = dimension.get('aliases', [])
            if any(alias.lower() == dimension_name_lower for alias in aliases):
                return dimension
        
        return None
    
    def _parse_time_range(self, time_range: str) -> Dict[str, Any]:
        """
        Parse time range string into structured format.
        
        Examples:
        - "last 7 days" -> {"type": "relative", "value": 7, "unit": "days"}
        - "last 30 days" -> {"type": "relative", "value": 30, "unit": "days"}
        - "2024-01-01 to 2024-01-31" -> {"type": "absolute", "start": "...", "end": "..."}
        """
        import re
        
        time_range_lower = time_range.lower().strip()
        
        # Relative time ranges
        relative_match = re.match(r'last\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)', time_range_lower)
        if relative_match:
            value = int(relative_match.group(1))
            unit = relative_match.group(2).rstrip('s')  # Remove plural
            return {
                'type': 'relative',
                'value': value,
                'unit': unit
            }
        
        # Absolute date ranges
        date_range_match = re.match(r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})', time_range_lower)
        if date_range_match:
            return {
                'type': 'absolute',
                'start': date_range_match.group(1),
                'end': date_range_match.group(2)
            }
        
        # Default: all time
        if 'all time' in time_range_lower or 'all' in time_range_lower:
            return {
                'type': 'all'
            }
        
        # Fallback: return as-is
        return {
            'type': 'custom',
            'value': time_range
        }
    
    def resolve_clarified_query(self, original_query: str,
                                original_intent: Optional[Dict[str, Any]],
                                answers: Dict[str, Any],
                                metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Resolve a clarified query by merging answers and regenerating intent.
        
        Args:
            original_query: Original user query
            original_intent: Original intent (may be partial)
            answers: User answers to clarification questions
            metadata: Optional metadata (uses self.metadata if not provided)
        
        Returns:
            Dictionary with resolved intent and updated query
        """
        metadata = metadata or self.metadata
        
        # Merge answers into intent
        resolved_intent = self.merge_answers_into_intent(
            original_intent or {},
            answers,
            original_query
        )
        
        # Build clarified query text (optional, for display)
        clarified_query = self._build_clarified_query_text(original_query, answers)
        
        return {
            'original_query': original_query,
            'clarified_query': clarified_query,
            'resolved_intent': resolved_intent,
            'answers': answers
        }
    
    def _build_clarified_query_text(self, original_query: str, answers: Dict[str, Any]) -> str:
        """
        Build a clarified query text from original query and answers.
        
        This is mainly for display/logging purposes.
        """
        parts = [original_query]
        
        if 'metric' in answers:
            parts.append(f"metric: {answers['metric']}")
        
        if 'time_range' in answers:
            parts.append(f"time range: {answers['time_range']}")
        
        if 'dimensions' in answers:
            dims = answers['dimensions']
            if isinstance(dims, list):
                parts.append(f"grouped by: {', '.join(dims)}")
            else:
                parts.append(f"grouped by: {dims}")
        
        return " | ".join(parts)

