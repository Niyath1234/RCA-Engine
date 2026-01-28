"""
Cache Interface

Abstract interface for cache implementations to enable replaceability.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class Cache(ABC):
    """Interface for cache."""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
        
        Returns:
            Cached value or None
        """
        pass
    
    @abstractmethod
    def set(self, key: str, value: Dict[str, Any], ttl: int = 3600):
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
        """
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """
        Delete key from cache.
        
        Args:
            key: Cache key
        
        Returns:
            True if deleted, False otherwise
        """
        pass
    
    @abstractmethod
    def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate cache entries matching pattern.
        
        Args:
            pattern: Pattern to match (e.g., "user:*")
        
        Returns:
            Number of keys invalidated
        """
        pass
    
    @abstractmethod
    def clear(self):
        """Clear all cache entries."""
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if key exists in cache.
        
        Args:
            key: Cache key
        
        Returns:
            True if exists, False otherwise
        """
        pass

