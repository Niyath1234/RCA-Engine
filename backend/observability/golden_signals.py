"""
Golden Signals

Track the minimum set of signals that matter.
"""

from typing import Dict, Any
from collections import defaultdict
from datetime import datetime
import time


class Histogram:
    """Simple histogram implementation."""
    
    def __init__(self):
        """Initialize histogram."""
        self.values = []
    
    def observe(self, value: float):
        """Record a value."""
        self.values.append(value)
    
    def get(self) -> Dict[str, float]:
        """Get histogram statistics."""
        if not self.values:
            return {'count': 0, 'min': 0, 'max': 0, 'avg': 0, 'p50': 0, 'p95': 0, 'p99': 0}
        
        sorted_values = sorted(self.values)
        n = len(sorted_values)
        
        return {
            'count': n,
            'min': min(self.values),
            'max': max(self.values),
            'avg': sum(self.values) / n,
            'p50': sorted_values[int(n * 0.50)] if n > 0 else 0,
            'p95': sorted_values[int(n * 0.95)] if n > 0 else 0,
            'p99': sorted_values[int(n * 0.99)] if n > 0 else 0,
        }
    
    def reset(self):
        """Reset histogram."""
        self.values = []


class Counter:
    """Simple counter implementation."""
    
    def __init__(self):
        """Initialize counter."""
        self.count = 0
    
    def inc(self, value: float = 1.0):
        """Increment counter."""
        self.count += value
    
    def get(self) -> float:
        """Get counter value."""
        return self.count
    
    def reset(self):
        """Reset counter."""
        self.count = 0


class GoldenSignals:
    """Track golden signals for RCA Engine."""
    
    def __init__(self):
        """Initialize golden signals."""
        self.signals = {
            'planning_latency': Histogram(),
            'execution_latency': Histogram(),
            'failure_reason': defaultdict(Counter),
            'rows_scanned': Histogram(),
            'rows_returned': Histogram(),
            'cost_per_query': Histogram(),
        }
    
    def record_planning(self, duration_ms: float):
        """
        Record planning latency.
        
        Args:
            duration_ms: Planning duration in milliseconds
        """
        self.signals['planning_latency'].observe(duration_ms)
    
    def record_execution(self, duration_ms: float, rows_scanned: int, rows_returned: int):
        """
        Record execution metrics.
        
        Args:
            duration_ms: Execution duration in milliseconds
            rows_scanned: Number of rows scanned
            rows_returned: Number of rows returned
        """
        self.signals['execution_latency'].observe(duration_ms)
        self.signals['rows_scanned'].observe(float(rows_scanned))
        self.signals['rows_returned'].observe(float(rows_returned))
    
    def record_failure(self, reason: str):
        """
        Record failure reason.
        
        Args:
            reason: Failure reason
        """
        self.signals['failure_reason'][reason].inc()
    
    def record_cost(self, cost_usd: float):
        """
        Record cost per query.
        
        Args:
            cost_usd: Cost in USD
        """
        self.signals['cost_per_query'].observe(cost_usd)
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current metrics.
        
        Returns:
            Metrics dictionary
        """
        metrics = {}
        
        for name, signal in self.signals.items():
            if isinstance(signal, Histogram):
                metrics[name] = signal.get()
            elif isinstance(signal, defaultdict):
                metrics[name] = {k: v.get() for k, v in signal.items()}
            else:
                metrics[name] = signal.get()
        
        return metrics
    
    def reset(self):
        """Reset all metrics."""
        for signal in self.signals.values():
            if isinstance(signal, Histogram):
                signal.reset()
            elif isinstance(signal, defaultdict):
                for counter in signal.values():
                    counter.reset()
            else:
                signal.reset()

