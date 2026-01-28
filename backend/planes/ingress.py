"""
Ingress Plane

Handle API ingress, authentication, rate limiting, and input validation.
SLA: < 50ms latency
Failure Mode: Reject request
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from datetime import datetime


@dataclass
class IngressResult:
    """Result from ingress plane."""
    success: bool
    request_id: str
    user_id: Optional[str] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    validated_input: Optional[Dict[str, Any]] = None
    timestamp: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'success': self.success,
            'request_id': self.request_id,
            'timestamp': self.timestamp or datetime.utcnow().isoformat(),
        }
        if self.user_id:
            result['user_id'] = self.user_id
        if self.error:
            result['error'] = self.error
        if self.error_code:
            result['error_code'] = self.error_code
        if self.validated_input:
            result['validated_input'] = self.validated_input
        return result


class IngressPlane:
    """Handle API ingress, auth, rate limiting."""
    
    def __init__(self, authenticator=None, rate_limiter=None, validator=None):
        """
        Initialize ingress plane.
        
        Args:
            authenticator: Authentication service
            rate_limiter: Rate limiting service
            validator: Input validation service
        """
        self.authenticator = authenticator
        self.rate_limiter = rate_limiter
        self.validator = validator
    
    def process_request(self, request: Dict[str, Any]) -> IngressResult:
        """
        Process incoming request.
        
        Args:
            request: Request dictionary with 'query', 'token', etc.
        
        Returns:
            IngressResult
        """
        request_id = self._generate_request_id()
        
        # Step 1: Authenticate
        auth_result = self._authenticate(request.get('token'))
        if not auth_result['success']:
            return IngressResult(
                success=False,
                request_id=request_id,
                error=auth_result.get('error', 'Authentication failed'),
                error_code='AUTH_FAILED'
            )
        
        user_id = auth_result.get('user_id')
        
        # Step 2: Rate limit check
        if self.rate_limiter:
            rate_limit_result = self.rate_limiter.check_rate_limit(user_id, 'query')
            if not rate_limit_result['allowed']:
                return IngressResult(
                    success=False,
                    request_id=request_id,
                    user_id=user_id,
                    error='Rate limit exceeded',
                    error_code='RATE_LIMIT_EXCEEDED'
                )
        
        # Step 3: Input validation
        if self.validator:
            validation_result = self.validator.validate(request.get('query', ''))
            if not validation_result['valid']:
                return IngressResult(
                    success=False,
                    request_id=request_id,
                    user_id=user_id,
                    error=validation_result.get('error', 'Invalid input'),
                    error_code='VALIDATION_FAILED'
                )
        
        # Step 4: Prepare validated input
        validated_input = {
            'query': request.get('query', ''),
            'format': request.get('format', 'json'),
            'options': request.get('options', {}),
        }
        
        return IngressResult(
            success=True,
            request_id=request_id,
            user_id=user_id,
            validated_input=validated_input,
            timestamp=datetime.utcnow().isoformat()
        )
    
    def _authenticate(self, token: Optional[str]) -> Dict[str, Any]:
        """Authenticate user token."""
        if not self.authenticator:
            # No authentication required
            return {'success': True, 'user_id': 'anonymous'}
        
        if not token:
            return {'success': False, 'error': 'Token required'}
        
        return self.authenticator.authenticate(token)
    
    def _generate_request_id(self) -> str:
        """Generate unique request ID."""
        import uuid
        return str(uuid.uuid4())

