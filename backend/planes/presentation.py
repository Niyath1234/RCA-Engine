"""
Presentation Plane

Format and present results.
SLA: < 500ms latency
Failure Mode: Degrade gracefully
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from datetime import datetime


@dataclass
class PresentationResult:
    """Result from presentation plane."""
    success: bool
    format: str
    content: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    explanation: Optional[str] = None
    error: Optional[str] = None
    timestamp: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'success': self.success,
            'format': self.format,
            'timestamp': self.timestamp or datetime.utcnow().isoformat(),
        }
        if self.content:
            result['content'] = self.content
        if self.data:
            result['data'] = self.data
        if self.explanation:
            result['explanation'] = self.explanation
        if self.error:
            result['error'] = self.error
        return result


class PresentationPlane:
    """Format and present results."""
    
    def __init__(self, formatter=None, explainer=None, cache=None):
        """
        Initialize presentation plane.
        
        Args:
            formatter: Result formatter instance
            explainer: Result explainer instance
            cache: Cache for formatted results
        """
        self.formatter = formatter
        self.explainer = explainer
        self.cache = cache
    
    def format_results(self, execution_result: Dict[str, Any],
                      format_type: str = 'json',
                      generate_explanation: bool = True) -> PresentationResult:
        """
        Format results for presentation.
        
        Args:
            execution_result: Execution result dictionary
            format_type: Output format ('json', 'csv', 'table', 'markdown')
            generate_explanation: Whether to generate explanation
        
        Returns:
            PresentationResult
        """
        # Check cache
        if self.cache:
            cache_key = self._generate_cache_key(execution_result, format_type)
            cached = self.cache.get(cache_key)
            if cached:
                return PresentationResult(
                    success=True,
                    format=format_type,
                    content=cached.get('content'),
                    data=cached.get('data'),
                    explanation=cached.get('explanation'),
                    timestamp=datetime.utcnow().isoformat()
                )
        
        # Format data
        if not self.formatter:
            # Fallback: return raw data
            return PresentationResult(
                success=True,
                format=format_type,
                data=execution_result,
                timestamp=datetime.utcnow().isoformat()
            )
        
        try:
            formatted_content = self.formatter.format(execution_result, format_type)
            
            # Generate explanation if requested
            explanation = None
            if generate_explanation and self.explainer:
                try:
                    explanation = self.explainer.explain(execution_result)
                except Exception as e:
                    # Degrade gracefully - explanation failure doesn't fail the whole request
                    explanation = f"Explanation unavailable: {str(e)}"
            
            result = PresentationResult(
                success=True,
                format=format_type,
                content=formatted_content if isinstance(formatted_content, str) else None,
                data=formatted_content if isinstance(formatted_content, dict) else execution_result,
                explanation=explanation,
                timestamp=datetime.utcnow().isoformat()
            )
            
            # Cache result
            if self.cache:
                cache_key = self._generate_cache_key(execution_result, format_type)
                self.cache.set(cache_key, {
                    'content': formatted_content,
                    'data': result.data,
                    'explanation': explanation,
                }, ttl=1800)
            
            return result
            
        except Exception as e:
            # Degrade gracefully - return raw data
            return PresentationResult(
                success=True,
                format=format_type,
                data=execution_result,
                error=f"Formatting failed: {str(e)}",
                timestamp=datetime.utcnow().isoformat()
            )
    
    def _generate_cache_key(self, execution_result: Dict[str, Any], format_type: str) -> str:
        """Generate cache key from execution result."""
        import hashlib
        import json
        
        key_data = {
            'data_hash': hashlib.sha256(
                json.dumps(execution_result.get('data', []), sort_keys=True).encode()
            ).hexdigest()[:16],
            'format': format_type,
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()

