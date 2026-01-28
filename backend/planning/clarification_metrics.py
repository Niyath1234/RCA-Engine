"""
Clarification Metrics and Monitoring

Tracks clarification usage, success rates, and performance metrics.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import logging
from dataclasses import dataclass, asdict
import json

logger = logging.getLogger(__name__)


@dataclass
class ClarificationMetrics:
    """Metrics for clarification usage."""
    total_queries: int = 0
    clarification_needed: int = 0
    clarification_resolved: int = 0
    average_questions_per_query: float = 0.0
    average_confidence: float = 0.0
    total_clarification_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class ClarificationMetricsCollector:
    """
    Collects and aggregates clarification metrics.
    """
    
    def __init__(self):
        """Initialize metrics collector."""
        self.metrics = ClarificationMetrics()
        self._question_counts = []  # Track questions per query
        self._confidence_scores = []  # Track confidence scores
        self._clarification_times = []  # Track clarification check times
    
    def record_query(self, needs_clarification: bool = False):
        """Record a query."""
        self.metrics.total_queries += 1
        if needs_clarification:
            self.metrics.clarification_needed += 1
    
    def record_clarification(self, questions_count: int, confidence: float, 
                           time_ms: float):
        """Record clarification details."""
        self._question_counts.append(questions_count)
        self._confidence_scores.append(confidence)
        self._clarification_times.append(time_ms)
        
        # Update averages
        if self._question_counts:
            self.metrics.average_questions_per_query = sum(self._question_counts) / len(self._question_counts)
        
        if self._confidence_scores:
            self.metrics.average_confidence = sum(self._confidence_scores) / len(self._confidence_scores)
        
        if self._clarification_times:
            self.metrics.total_clarification_time_ms = sum(self._clarification_times)
    
    def record_resolution(self):
        """Record a successful clarification resolution."""
        self.metrics.clarification_resolved += 1
    
    def get_metrics(self) -> ClarificationMetrics:
        """Get current metrics."""
        return self.metrics
    
    def get_stats(self) -> Dict[str, Any]:
        """Get detailed statistics."""
        stats = self.metrics.to_dict()
        
        # Add additional stats
        if self.metrics.total_queries > 0:
            stats['clarification_rate'] = (
                self.metrics.clarification_needed / self.metrics.total_queries
            )
        else:
            stats['clarification_rate'] = 0.0
        
        if self.metrics.clarification_needed > 0:
            stats['resolution_rate'] = (
                self.metrics.clarification_resolved / self.metrics.clarification_needed
            )
        else:
            stats['resolution_rate'] = 0.0
        
        if self._clarification_times:
            stats['average_clarification_time_ms'] = (
                sum(self._clarification_times) / len(self._clarification_times)
            )
        else:
            stats['average_clarification_time_ms'] = 0.0
        
        return stats
    
    def reset(self):
        """Reset metrics."""
        self.metrics = ClarificationMetrics()
        self._question_counts = []
        self._confidence_scores = []
        self._clarification_times = []


# Global metrics collector instance
_metrics_collector: Optional[ClarificationMetricsCollector] = None


def get_clarification_metrics() -> ClarificationMetricsCollector:
    """Get or create global metrics collector."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = ClarificationMetricsCollector()
    return _metrics_collector


def log_clarification_event(event_type: str, **kwargs):
    """
    Log a clarification event with structured logging.
    
    Args:
        event_type: Type of event ('analyze', 'resolve', 'question_generated', etc.)
        **kwargs: Additional event data
    """
    log_data = {
        'event_type': event_type,
        'timestamp': datetime.utcnow().isoformat(),
        **kwargs
    }
    
    logger.info(f"Clarification event: {json.dumps(log_data)}")
    
    # Also record in metrics
    metrics = get_clarification_metrics()
    
    if event_type == 'query_analyzed':
        needs_clarification = kwargs.get('needs_clarification', False)
        metrics.record_query(needs_clarification=needs_clarification)
        
        if needs_clarification:
            questions_count = kwargs.get('questions_count', 0)
            confidence = kwargs.get('confidence', 0.0)
            time_ms = kwargs.get('time_ms', 0.0)
            metrics.record_clarification(questions_count, confidence, time_ms)
    
    elif event_type == 'clarification_resolved':
        metrics.record_resolution()

