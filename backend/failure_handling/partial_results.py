"""
Partial Result Handler

Handle partial results from timeouts.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class FailureResponse:
    """Response from failure handler."""
    success: bool
    partial: bool = False
    data: Optional[Dict[str, Any]] = None
    warning: Optional[str] = None
    error: Optional[str] = None
    suggestion: Optional[str] = None


class PartialResultHandler:
    """Handle partial results from timeouts."""
    
    def handle_timeout(self, partial_result: Dict[str, Any], 
                      allow_partial: bool = False) -> FailureResponse:
        """
        Handle query timeout.
        
        Args:
            partial_result: Partial result dictionary
            allow_partial: Whether partial results are allowed
        
        Returns:
            FailureResponse
        """
        if not allow_partial:
            return FailureResponse(
                success=False,
                error='Query execution timeout',
                suggestion='Try narrowing your query or adding filters'
            )
        
        # Return partial result with clear marking
        return FailureResponse(
            success=True,
            partial=True,
            data=partial_result,
            warning='Results are partial due to timeout'
        )
    
    def mark_partial(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mark result as partial.
        
        Args:
            result: Result dictionary
        
        Returns:
            Result dictionary with partial marking
        """
        result['partial'] = True
        result['warning'] = 'Results may be incomplete'
        return result

