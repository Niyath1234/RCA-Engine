"""
Result Explainer

Generate natural language explanations of query results.
"""

from typing import Dict, Any, Optional
from backend.interfaces import LLMProvider


class ResultExplainer:
    """Explain query results in natural language."""
    
    def __init__(self, llm_provider: Optional[LLMProvider] = None):
        """
        Initialize result explainer.
        
        Args:
            llm_provider: Optional LLM provider for explanations
        """
        self.llm_provider = llm_provider
    
    def explain(self, execution_result: Dict[str, Any], sql: Optional[str] = None) -> str:
        """
        Explain query result.
        
        Args:
            execution_result: Execution result dictionary
            sql: Optional SQL query that was executed
        
        Returns:
            Natural language explanation
        """
        # Use LLM provider if available
        if self.llm_provider and sql:
            try:
                return self.llm_provider.explain_result(sql, execution_result)
            except Exception as e:
                # Fallback to rule-based explanation
                return self._rule_based_explanation(execution_result)
        
        # Fallback to rule-based explanation
        return self._rule_based_explanation(execution_result)
    
    def _rule_based_explanation(self, execution_result: Dict[str, Any]) -> str:
        """
        Rule-based explanation (fallback).
        
        Args:
            execution_result: Execution result dictionary
        
        Returns:
            Explanation string
        """
        rows_returned = execution_result.get('rows_returned', 0)
        columns = execution_result.get('columns', [])
        duration_ms = execution_result.get('duration_ms', 0)
        partial = execution_result.get('partial', False)
        
        explanation_parts = []
        
        # Row count
        if rows_returned == 0:
            explanation_parts.append("The query returned no results.")
        elif rows_returned == 1:
            explanation_parts.append("The query returned 1 row.")
        else:
            explanation_parts.append(f"The query returned {rows_returned} rows.")
        
        # Columns
        if columns:
            if len(columns) == 1:
                explanation_parts.append(f"The result contains the column: {columns[0]}.")
            else:
                explanation_parts.append(f"The result contains {len(columns)} columns: {', '.join(columns[:5])}")
                if len(columns) > 5:
                    explanation_parts.append(f"and {len(columns) - 5} more.")
        
        # Duration
        if duration_ms > 0:
            if duration_ms < 1000:
                explanation_parts.append(f"Query executed in {duration_ms:.0f} milliseconds.")
            else:
                explanation_parts.append(f"Query executed in {duration_ms/1000:.2f} seconds.")
        
        # Partial results
        if partial:
            explanation_parts.append("Note: Results may be incomplete due to timeout.")
        
        return ' '.join(explanation_parts)

