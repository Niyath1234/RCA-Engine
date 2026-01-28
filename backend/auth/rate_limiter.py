"""
Rate Limiter

Rate limiting implementations.
"""

import time
from typing import Dict, Any, Optional
from threading import Lock
from collections import defaultdict


class RateLimiter:
    """Base rate limiter interface."""
    
    def check_rate_limit(self, user_id: str, endpoint: str) -> Dict[str, Any]:
        """
        Check if request is within rate limit.
        
        Args:
            user_id: User ID
            endpoint: Endpoint name
        
        Returns:
            Rate limit check result
        """
        raise NotImplementedError
    
    def get_remaining_requests(self, user_id: str) -> int:
        """
        Get remaining requests for user.
        
        Args:
            user_id: User ID
        
        Returns:
            Number of remaining requests
        """
        raise NotImplementedError


class TokenBucketRateLimiter(RateLimiter):
    """Token bucket rate limiter implementation."""
    
    def __init__(self, requests_per_minute: int = 60, requests_per_hour: int = 1000):
        """
        Initialize token bucket rate limiter.
        
        Args:
            requests_per_minute: Requests allowed per minute
            requests_per_hour: Requests allowed per hour
        """
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        
        # Token buckets: {user_id: {'minute': tokens, 'hour': tokens, 'last_refill': timestamp}}
        self.buckets: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'minute': requests_per_minute,
            'hour': requests_per_hour,
            'last_minute_refill': time.time(),
            'last_hour_refill': time.time(),
        })
        
        self.lock = Lock()
    
    def check_rate_limit(self, user_id: str, endpoint: str) -> Dict[str, Any]:
        """
        Check if request is within rate limit.
        
        Args:
            user_id: User ID
            endpoint: Endpoint name
        
        Returns:
            Rate limit check result
        """
        with self.lock:
            bucket = self.buckets[user_id]
            now = time.time()
            
            # Refill tokens
            self._refill_tokens(bucket, now)
            
            # Check minute limit
            if bucket['minute'] <= 0:
                return {
                    'allowed': False,
                    'reason': 'Rate limit exceeded (per minute)',
                    'retry_after': 60 - (now - bucket['last_minute_refill']),
                }
            
            # Check hour limit
            if bucket['hour'] <= 0:
                return {
                    'allowed': False,
                    'reason': 'Rate limit exceeded (per hour)',
                    'retry_after': 3600 - (now - bucket['last_hour_refill']),
                }
            
            # Consume tokens
            bucket['minute'] -= 1
            bucket['hour'] -= 1
            
            return {
                'allowed': True,
                'remaining_minute': bucket['minute'],
                'remaining_hour': bucket['hour'],
            }
    
    def get_remaining_requests(self, user_id: str) -> int:
        """Get remaining requests for user."""
        with self.lock:
            bucket = self.buckets.get(user_id)
            if not bucket:
                return min(self.requests_per_minute, self.requests_per_hour)
            
            now = time.time()
            self._refill_tokens(bucket, now)
            
            return min(bucket['minute'], bucket['hour'])
    
    def _refill_tokens(self, bucket: Dict[str, Any], now: float):
        """Refill tokens based on elapsed time."""
        # Refill minute bucket
        elapsed_minutes = (now - bucket['last_minute_refill']) / 60
        if elapsed_minutes >= 1:
            bucket['minute'] = min(
                self.requests_per_minute,
                bucket['minute'] + int(elapsed_minutes * self.requests_per_minute)
            )
            bucket['last_minute_refill'] = now
        
        # Refill hour bucket
        elapsed_hours = (now - bucket['last_hour_refill']) / 3600
        if elapsed_hours >= 1:
            bucket['hour'] = min(
                self.requests_per_hour,
                bucket['hour'] + int(elapsed_hours * self.requests_per_hour)
            )
            bucket['last_hour_refill'] = now


class SlidingWindowRateLimiter(RateLimiter):
    """Sliding window rate limiter implementation."""
    
    def __init__(self, requests_per_minute: int = 60):
        """
        Initialize sliding window rate limiter.
        
        Args:
            requests_per_minute: Requests allowed per minute
        """
        self.requests_per_minute = requests_per_minute
        # Store request timestamps: {user_id: [timestamps]}
        self.windows: Dict[str, list] = defaultdict(list)
        self.lock = Lock()
    
    def check_rate_limit(self, user_id: str, endpoint: str) -> Dict[str, Any]:
        """Check if request is within rate limit."""
        with self.lock:
            now = time.time()
            window = self.windows[user_id]
            
            # Remove timestamps older than 1 minute
            window[:] = [ts for ts in window if now - ts < 60]
            
            # Check limit
            if len(window) >= self.requests_per_minute:
                oldest_request = min(window)
                retry_after = 60 - (now - oldest_request)
                return {
                    'allowed': False,
                    'reason': 'Rate limit exceeded',
                    'retry_after': retry_after,
                }
            
            # Add current request
            window.append(now)
            
            return {
                'allowed': True,
                'remaining': self.requests_per_minute - len(window),
            }
    
    def get_remaining_requests(self, user_id: str) -> int:
        """Get remaining requests."""
        with self.lock:
            now = time.time()
            window = self.windows.get(user_id, [])
            
            # Remove old timestamps
            window[:] = [ts for ts in window if now - ts < 60]
            
            return max(0, self.requests_per_minute - len(window))

