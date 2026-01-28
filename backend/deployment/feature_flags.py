"""
Feature Flags

Feature flag management for gradual rollout.
"""

from typing import Dict, Any, Optional
import hashlib


class FeatureFlags:
    """Feature flag management."""
    
    def __init__(self, flags: Optional[Dict[str, bool]] = None):
        """
        Initialize feature flags.
        
        Args:
            flags: Initial feature flags dictionary
        """
        self.flags = flags or {
            'multi_step_planning': False,
            'query_optimization': False,
            'advanced_caching': False,
            'rag_versioning': False,
        }
        self.user_rollouts = {}  # user_id -> set of enabled flags
    
    def is_enabled(self, flag: str, user_id: Optional[str] = None) -> bool:
        """
        Check if feature flag is enabled.
        
        Args:
            flag: Feature flag name
            user_id: Optional user ID for user-specific rollout
        
        Returns:
            True if enabled, False otherwise
        """
        # Check global flag
        if not self.flags.get(flag, False):
            return False
        
        # Check user-specific rollout
        if user_id:
            return self._is_user_enabled(flag, user_id)
        
        return True
    
    def _is_user_enabled(self, flag: str, user_id: str) -> bool:
        """
        Check if user is in rollout.
        
        Args:
            flag: Feature flag name
            user_id: User ID
        
        Returns:
            True if user is enabled, False otherwise
        """
        # Check explicit user rollouts
        if user_id in self.user_rollouts:
            return flag in self.user_rollouts[user_id]
        
        # Gradual rollout logic (e.g., 10% of users)
        # Use hash-based consistent assignment
        user_hash = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        rollout_percentage = self._get_rollout_percentage(flag)
        
        return (user_hash % 100) < rollout_percentage
    
    def _get_rollout_percentage(self, flag: str) -> int:
        """
        Get rollout percentage for flag.
        
        Args:
            flag: Feature flag name
        
        Returns:
            Rollout percentage (0-100)
        """
        # In production, this would be configurable
        # For now, return 0 (no rollout)
        return 0
    
    def enable_flag(self, flag: str):
        """
        Enable feature flag globally.
        
        Args:
            flag: Feature flag name
        """
        self.flags[flag] = True
    
    def disable_flag(self, flag: str):
        """
        Disable feature flag globally.
        
        Args:
            flag: Feature flag name
        """
        self.flags[flag] = False
    
    def enable_for_user(self, flag: str, user_id: str):
        """
        Enable feature flag for specific user.
        
        Args:
            flag: Feature flag name
            user_id: User ID
        """
        if user_id not in self.user_rollouts:
            self.user_rollouts[user_id] = set()
        self.user_rollouts[user_id].add(flag)
    
    def disable_for_user(self, flag: str, user_id: str):
        """
        Disable feature flag for specific user.
        
        Args:
            flag: Feature flag name
            user_id: User ID
        """
        if user_id in self.user_rollouts:
            self.user_rollouts[user_id].discard(flag)

