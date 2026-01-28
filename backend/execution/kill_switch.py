"""
Kill Switch

Kill switches for query execution.
Per-query, per-user, and global kill switches.
"""

from typing import Set, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class KillSwitch:
    """Kill switches for query execution."""
    
    def __init__(self):
        """Initialize kill switch."""
        self.global_kill = False
        self.user_kills: Set[str] = set()
        self.query_kills: Set[str] = set()
        self.global_kill_reason: Optional[str] = None
        self.user_kill_reasons: dict = {}
    
    def check_kill_switch(self, user_id: Optional[str] = None, 
                         query_id: Optional[str] = None) -> bool:
        """
        Check if kill switch is active.
        
        Args:
            user_id: User ID to check
            query_id: Query ID to check
        
        Returns:
            True if kill switch is active, False otherwise
        """
        if self.global_kill:
            return True
        
        if user_id and user_id in self.user_kills:
            return True
        
        if query_id and query_id in self.query_kills:
            return True
        
        return False
    
    def get_kill_reason(self, user_id: Optional[str] = None,
                       query_id: Optional[str] = None) -> Optional[str]:
        """
        Get reason for kill switch activation.
        
        Args:
            user_id: User ID
            query_id: Query ID
        
        Returns:
            Kill reason or None
        """
        if self.global_kill:
            return self.global_kill_reason or "Global kill switch activated"
        
        if user_id and user_id in self.user_kills:
            return self.user_kill_reasons.get(user_id, "User kill switch activated")
        
        if query_id and query_id in self.query_kills:
            return "Query kill switch activated"
        
        return None
    
    def activate_global_kill(self, reason: Optional[str] = None):
        """
        Activate global kill switch.
        
        Args:
            reason: Optional reason for activation
        """
        self.global_kill = True
        self.global_kill_reason = reason
        logger.critical(f"Global kill switch activated: {reason or 'No reason provided'}")
    
    def deactivate_global_kill(self):
        """Deactivate global kill switch."""
        self.global_kill = False
        self.global_kill_reason = None
        logger.info("Global kill switch deactivated")
    
    def activate_user_kill(self, user_id: str, reason: Optional[str] = None):
        """
        Activate kill switch for user.
        
        Args:
            user_id: User ID
            reason: Optional reason for activation
        """
        self.user_kills.add(user_id)
        if reason:
            self.user_kill_reasons[user_id] = reason
        logger.warning(f"Kill switch activated for user {user_id}: {reason or 'No reason provided'}")
    
    def deactivate_user_kill(self, user_id: str):
        """
        Deactivate kill switch for user.
        
        Args:
            user_id: User ID
        """
        self.user_kills.discard(user_id)
        self.user_kill_reasons.pop(user_id, None)
        logger.info(f"Kill switch deactivated for user {user_id}")
    
    def activate_query_kill(self, query_id: str):
        """
        Activate kill switch for query.
        
        Args:
            query_id: Query ID
        """
        self.query_kills.add(query_id)
        logger.warning(f"Kill switch activated for query {query_id}")
    
    def deactivate_query_kill(self, query_id: str):
        """
        Deactivate kill switch for query.
        
        Args:
            query_id: Query ID
        """
        self.query_kills.discard(query_id)
        logger.info(f"Kill switch deactivated for query {query_id}")
    
    def get_status(self) -> dict:
        """
        Get kill switch status.
        
        Returns:
            Status dictionary
        """
        return {
            'global_kill': self.global_kill,
            'global_kill_reason': self.global_kill_reason,
            'user_kills': list(self.user_kills),
            'query_kills': list(self.query_kills),
            'user_kill_reasons': self.user_kill_reasons.copy(),
        }

