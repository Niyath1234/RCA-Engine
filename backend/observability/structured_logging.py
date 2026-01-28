"""
Structured Logging

Structured logging with correlation IDs.
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from .correlation import CorrelationID


class StructuredLogger:
    """Structured logging with correlation IDs."""
    
    def __init__(self, name: str = 'rca_engine', level: int = logging.INFO):
        """
        Initialize structured logger.
        
        Args:
            name: Logger name
            level: Logging level
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Add console handler if not already present
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(handler)
    
    def log_request(self, correlation_id: CorrelationID, request: Dict[str, Any]):
        """
        Log request with correlation ID.
        
        Args:
            correlation_id: Correlation ID
            request: Request dictionary
        """
        self._log('info', 'request_received', correlation_id, **request)
    
    def log_planning_step(self, correlation_id: CorrelationID, step: str, data: Dict[str, Any]):
        """
        Log planning step.
        
        Args:
            correlation_id: Correlation ID
            step: Step name
            data: Step data
        """
        self._log('info', 'planning_step', correlation_id, step=step, **data)
    
    def log_execution(self, correlation_id: CorrelationID, sql: str, duration_ms: float):
        """
        Log query execution.
        
        Args:
            correlation_id: Correlation ID
            sql: SQL query
            duration_ms: Execution duration in milliseconds
        """
        self._log('info', 'query_executed', correlation_id, 
                 sql=sql, duration_ms=duration_ms)
    
    def log_error(self, correlation_id: CorrelationID, error: Exception, context: Dict[str, Any]):
        """
        Log error with full context.
        
        Args:
            correlation_id: Correlation ID
            error: Exception object
            context: Additional context
        """
        self._log('error', 'error_occurred', correlation_id,
                 error_type=type(error).__name__,
                 error_message=str(error),
                 **context)
    
    def log_metric(self, correlation_id: CorrelationID, metric_name: str, value: float):
        """
        Log metric.
        
        Args:
            correlation_id: Correlation ID
            metric_name: Metric name
            value: Metric value
        """
        self._log('info', 'metric', correlation_id, metric=metric_name, value=value)
    
    def _log(self, level: str, event: str, correlation_id: CorrelationID, **kwargs):
        """
        Internal logging method.
        
        Args:
            level: Log level
            event: Event name
            correlation_id: Correlation ID
            **kwargs: Additional fields
        """
        log_entry = {
            'event': event,
            'timestamp': datetime.utcnow().isoformat(),
            **correlation_id.to_dict(),
            **kwargs
        }
        
        log_message = json.dumps(log_entry)
        
        if level == 'info':
            self.logger.info(log_message)
        elif level == 'warning':
            self.logger.warning(log_message)
        elif level == 'error':
            self.logger.error(log_message)
        elif level == 'critical':
            self.logger.critical(log_message)
        else:
            self.logger.debug(log_message)

