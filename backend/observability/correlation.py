"""
Correlation IDs

Correlation IDs for end-to-end tracing.
"""

import uuid
from typing import Optional
from dataclasses import dataclass


@dataclass
class CorrelationID:
    """Correlation IDs for end-to-end tracing."""
    request_id: str
    planning_id: Optional[str] = None
    execution_id: Optional[str] = None
    
    def __post_init__(self):
        """Initialize correlation ID."""
        if not self.request_id:
            self.request_id = self._generate_id()
    
    @classmethod
    def create(cls, request_id: Optional[str] = None) -> 'CorrelationID':
        """
        Create new correlation ID.
        
        Args:
            request_id: Optional request ID (generated if not provided)
        
        Returns:
            CorrelationID instance
        """
        return cls(request_id=request_id or cls._generate_id())
    
    def set_planning_id(self, planning_id: str):
        """
        Set planning ID.
        
        Args:
            planning_id: Planning ID
        """
        self.planning_id = planning_id
    
    def set_execution_id(self, execution_id: str):
        """
        Set execution ID.
        
        Args:
            execution_id: Execution ID
        """
        self.execution_id = execution_id
    
    def to_dict(self) -> dict:
        """
        Convert to dictionary for logging.
        
        Returns:
            Dictionary representation
        """
        result = {'request_id': self.request_id}
        if self.planning_id:
            result['planning_id'] = self.planning_id
        if self.execution_id:
            result['execution_id'] = self.execution_id
        return result
    
    def to_string(self) -> str:
        """
        Convert to string representation.
        
        Returns:
            String representation
        """
        parts = [f"req:{self.request_id}"]
        if self.planning_id:
            parts.append(f"plan:{self.planning_id}")
        if self.execution_id:
            parts.append(f"exec:{self.execution_id}")
        return "|".join(parts)
    
    @staticmethod
    def _generate_id() -> str:
        """
        Generate unique ID.
        
        Returns:
            UUID string
        """
        return str(uuid.uuid4())
    
    def inject_into_context(self):
        """
        Inject into logging context.
        Note: This is a placeholder - actual implementation depends on logging framework.
        """
        # In production, use structlog or similar
        # structlog.contextvars.bind_contextvars(**self.to_dict())
        pass

