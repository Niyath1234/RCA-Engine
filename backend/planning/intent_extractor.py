"""
Intent Extractor

Extract intent from natural language queries.
"""

from typing import Dict, Any, Optional
from backend.interfaces import LLMProvider


class IntentExtractor:
    """Extract intent from user queries."""
    
    def __init__(self, llm_provider: Optional[LLMProvider] = None):
        """
        Initialize intent extractor.
        
        Args:
            llm_provider: Optional LLM provider for extraction
        """
        self.llm_provider = llm_provider
    
    def extract(self, user_query: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Extract intent from user query.
        
        Args:
            user_query: User's natural language query
            context: Additional context
        
        Returns:
            Intent dictionary
        """
        context = context or {}
        
        # Use LLM provider if available
        if self.llm_provider:
            try:
                return self.llm_provider.extract_intent(user_query, context)
            except Exception as e:
                # Fallback to rule-based extraction
                return self._rule_based_extraction(user_query)
        
        # Fallback to rule-based extraction
        return self._rule_based_extraction(user_query)
    
    def _rule_based_extraction(self, user_query: str) -> Dict[str, Any]:
        """
        Rule-based intent extraction (fallback).
        
        Args:
            user_query: User query
        
        Returns:
            Intent dictionary
        """
        import re
        
        query_lower = user_query.lower()
        
        # Extract metric keywords
        metrics = []
        metric_keywords = ['revenue', 'sales', 'users', 'orders', 'profit', 'cost', 'count']
        for keyword in metric_keywords:
            if keyword in query_lower:
                metrics.append(keyword)
        
        # Extract time range
        time_range = None
        time_patterns = [
            (r'last\s+(\d+)\s+days?', 'last {n} days'),
            (r'last\s+(\d+)\s+weeks?', 'last {n} weeks'),
            (r'last\s+(\d+)\s+months?', 'last {n} months'),
            (r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})', 'date range'),
        ]
        
        for pattern, description in time_patterns:
            match = re.search(pattern, query_lower)
            if match:
                if description == 'date range':
                    time_range = f"{match.group(1)} to {match.group(2)}"
                else:
                    time_range = description.format(n=match.group(1))
                break
        
        # Extract aggregation
        aggregation = None
        aggregation_keywords = {
            'sum': 'sum',
            'total': 'sum',
            'average': 'avg',
            'avg': 'avg',
            'mean': 'avg',
            'count': 'count',
            'maximum': 'max',
            'max': 'max',
            'minimum': 'min',
            'min': 'min',
        }
        
        for keyword, agg_type in aggregation_keywords.items():
            if keyword in query_lower:
                aggregation = agg_type
                break
        
        # Extract filters
        filters = []
        filter_patterns = [
            (r'where\s+(.+?)(?:\s+group|\s+order|\s+limit|$)', 'where'),
            (r'filtered\s+by\s+(.+?)(?:\s+group|\s+order|\s+limit|$)', 'filter'),
        ]
        
        for pattern, _ in filter_patterns:
            match = re.search(pattern, query_lower)
            if match:
                filters.append(match.group(1))
        
        return {
            'query': user_query,
            'metric': metrics[0] if metrics else None,
            'metrics': metrics,
            'time_range': time_range,
            'aggregation': aggregation,
            'filters': filters,
        }

