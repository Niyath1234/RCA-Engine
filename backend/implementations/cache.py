"""
Cache Implementations

Memory and Redis cache implementations.
"""

import json
import hashlib
import time
from typing import Optional, Dict, Any
from threading import Lock
from backend.interfaces import Cache


class MemoryCache(Cache):
    """In-memory cache implementation."""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        """
        Initialize memory cache.
        
        Args:
            max_size: Maximum number of entries
            default_ttl: Default time-to-live in seconds
        """
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.lock = Lock()
    
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache."""
        with self.lock:
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            expires_at = entry.get('expires_at')
            
            # Check if expired
            if expires_at and time.time() > expires_at:
                del self.cache[key]
                return None
            
            return entry.get('value')
    
    def set(self, key: str, value: Dict[str, Any], ttl: int = 3600):
        """Set value in cache."""
        with self.lock:
            # Evict if at capacity
            if len(self.cache) >= self.max_size and key not in self.cache:
                # Remove oldest entry (simple FIFO)
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]
            
            expires_at = time.time() + ttl if ttl > 0 else None
            
            self.cache[key] = {
                'value': value,
                'expires_at': expires_at,
                'created_at': time.time(),
            }
    
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False
    
    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        with self.lock:
            count = 0
            keys_to_delete = []
            
            for key in self.cache.keys():
                if pattern in key:  # Simple pattern matching
                    keys_to_delete.append(key)
            
            for key in keys_to_delete:
                del self.cache[key]
                count += 1
            
            return count
    
    def clear(self):
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        with self.lock:
            if key not in self.cache:
                return False
            
            entry = self.cache[key]
            expires_at = entry.get('expires_at')
            
            # Check if expired
            if expires_at and time.time() > expires_at:
                del self.cache[key]
                return False
            
            return True


class RedisCache(Cache):
    """Redis cache implementation."""
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0,
                 default_ttl: int = 3600):
        """
        Initialize Redis cache.
        
        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            default_ttl: Default time-to-live in seconds
        """
        try:
            import redis
        except ImportError:
            raise ImportError("redis package is required. Install with: pip install redis")
        
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.default_ttl = default_ttl
        
        # Test connection
        try:
            self.client.ping()
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Redis: {str(e)}") from e
    
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache."""
        try:
            value = self.client.get(key)
            if value is None:
                return None
            
            return json.loads(value)
        except Exception:
            return None
    
    def set(self, key: str, value: Dict[str, Any], ttl: int = 3600):
        """Set value in cache."""
        try:
            value_str = json.dumps(value)
            self.client.setex(key, ttl, value_str)
        except Exception as e:
            raise RuntimeError(f"Failed to set cache value: {str(e)}") from e
    
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        try:
            return bool(self.client.delete(key))
        except Exception:
            return False
    
    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        try:
            count = 0
            for key in self.client.scan_iter(match=pattern):
                self.client.delete(key)
                count += 1
            return count
        except Exception:
            return 0
    
    def clear(self):
        """Clear all cache entries."""
        try:
            self.client.flushdb()
        except Exception:
            pass
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        try:
            return bool(self.client.exists(key))
        except Exception:
            return False

