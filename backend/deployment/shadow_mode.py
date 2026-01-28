"""
Shadow Mode

Run RCA Engine in shadow mode for comparison.
"""

from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass


@dataclass
class ShadowResult:
    """Result from shadow mode execution."""
    new_result: Dict[str, Any]
    old_result: Optional[Dict[str, Any]] = None
    comparison: Optional[Dict[str, Any]] = None


class ShadowMode:
    """Run RCA Engine in shadow mode for comparison."""
    
    def __init__(self, new_system_executor: Optional[Callable] = None,
                 old_system_executor: Optional[Callable] = None):
        """
        Initialize shadow mode.
        
        Args:
            new_system_executor: Function to execute new system
            old_system_executor: Function to execute old system
        """
        self.new_system_executor = new_system_executor
        self.old_system_executor = old_system_executor
    
    def execute_shadow(self, query: str, context: Dict[str, Any] = None) -> ShadowResult:
        """
        Execute query in shadow mode.
        
        Args:
            query: User query
            context: Context dictionary
        
        Returns:
            ShadowResult
        """
        context = context or {}
        
        # Execute with new system
        new_result = None
        if self.new_system_executor:
            try:
                new_result = self.new_system_executor(query, context)
            except Exception as e:
                new_result = {'error': str(e)}
        
        # Execute with old system (if available)
        old_result = None
        if self.old_system_executor:
            try:
                old_result = self.old_system_executor(query, context)
            except Exception as e:
                old_result = {'error': str(e)}
        
        # Compare results
        comparison = None
        if new_result and old_result:
            comparison = self._compare_results(new_result, old_result)
            
            # Log divergence if significant
            divergence = comparison.get('divergence', 0)
            if divergence > 0.1:  # 10% divergence threshold
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Shadow mode divergence detected: {divergence:.2%}",
                    extra={
                        'query': query,
                        'divergence': divergence,
                        'new_result': new_result,
                        'old_result': old_result,
                    }
                )
        
        return ShadowResult(
            new_result=new_result or {},
            old_result=old_result,
            comparison=comparison
        )
    
    def _compare_results(self, new_result: Dict[str, Any], 
                        old_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare results from new and old systems.
        
        Args:
            new_result: Result from new system
            old_result: Result from old system
        
        Returns:
            Comparison dictionary
        """
        # Simple comparison - in production, use more sophisticated comparison
        new_data = new_result.get('data', [])
        old_data = old_result.get('data', [])
        
        # Calculate divergence
        divergence = 0.0
        if len(new_data) != len(old_data):
            divergence = abs(len(new_data) - len(old_data)) / max(len(new_data), len(old_data), 1)
        
        return {
            'divergence': divergence,
            'new_count': len(new_data),
            'old_count': len(old_data),
            'match': divergence < 0.01,  # 1% threshold
        }

