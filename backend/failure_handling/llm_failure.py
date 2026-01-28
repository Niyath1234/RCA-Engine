"""
LLM Failure Handler

Handle LLM failures with retry and fallback.
"""

import time
from dataclasses import dataclass
from typing import Dict, Any, Optional, Callable


@dataclass
class FailureResponse:
    """Response from failure handler."""
    success: bool
    fallback: bool = False
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    suggestion: Optional[str] = None
    retries: int = 0


class LLMFailureHandler:
    """Handle LLM failures."""
    
    def __init__(self, max_retries: int = 1, backoff_factor: float = 2.0):
        """
        Initialize LLM failure handler.
        
        Args:
            max_retries: Maximum number of retries
            backoff_factor: Exponential backoff factor
        """
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.intent_cache = {}
    
    def handle_llm_failure(self, error: Exception, context: Dict[str, Any]) -> FailureResponse:
        """
        Handle LLM failure with fallback.
        
        Args:
            error: Exception that occurred
            context: Context dictionary
        
        Returns:
            FailureResponse
        """
        # Check if we should retry
        if self._should_retry(error):
            retry_result = self._retry_with_backoff(context)
            if retry_result['success']:
                return FailureResponse(
                    success=True,
                    data=retry_result.get('data'),
                    retries=retry_result.get('retries', 0)
                )
        
        # Fallback to cached intent if available
        user_query = context.get('user_query', '')
        cached_intent = self._get_cached_intent(user_query)
        if cached_intent:
            return FailureResponse(
                success=True,
                fallback=True,
                data=cached_intent,
                suggestion='Using cached intent due to LLM failure'
            )
        
        # Ask for clarification
        return FailureResponse(
            success=False,
            error='LLM service unavailable',
            suggestion='Please rephrase your query or try again later'
        )
    
    def _should_retry(self, error: Exception) -> bool:
        """Check if error should be retried."""
        retryable_errors = [
            'timeout',
            'rate limit',
            'connection',
            'temporary',
        ]
        
        error_str = str(error).lower()
        return any(retryable in error_str for retryable in retryable_errors)
    
    def _retry_with_backoff(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Retry with exponential backoff."""
        # This would call the LLM function again
        # For now, return failure
        return {
            'success': False,
            'retries': self.max_retries
        }
    
    def _get_cached_intent(self, user_query: str) -> Optional[Dict[str, Any]]:
        """Get cached intent for user query."""
        # Simple hash-based cache lookup
        import hashlib
        cache_key = hashlib.sha256(user_query.encode()).hexdigest()
        return self.intent_cache.get(cache_key)
    
    def cache_intent(self, user_query: str, intent: Dict[str, Any]):
        """Cache intent for user query."""
        import hashlib
        cache_key = hashlib.sha256(user_query.encode()).hexdigest()
        self.intent_cache[cache_key] = intent

