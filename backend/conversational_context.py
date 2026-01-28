#!/usr/bin/env python3
"""
Conversational Context Manager

Manages conversational state for SQL query building, allowing users to
incrementally modify queries through natural language.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
from collections import defaultdict


class ConversationalContext:
    """Manages conversational context for SQL query building."""
    
    def __init__(self, session_id: str, ttl_minutes: int = 30):
        """
        Initialize conversational context.
        
        Args:
            session_id: Unique session identifier
            ttl_minutes: Time-to-live in minutes for the context
        """
        self.session_id = session_id
        self.created_at = datetime.utcnow()
        self.ttl_minutes = ttl_minutes
        self.last_updated = datetime.utcnow()
        
        # Current query intent
        self.current_intent: Optional[Dict[str, Any]] = None
        self.current_sql: Optional[str] = None
        
        # Query history
        self.query_history: List[Dict[str, Any]] = []
        
        # Modification tracking
        self.modifications: List[Dict[str, Any]] = []
    
    def is_expired(self) -> bool:
        """Check if context has expired."""
        return datetime.utcnow() - self.last_updated > timedelta(minutes=self.ttl_minutes)
    
    def update(self, intent: Dict[str, Any], sql: Optional[str] = None):
        """Update context with new intent and SQL."""
        self.current_intent = intent
        self.current_sql = sql
        self.last_updated = datetime.utcnow()
        
        # Add to history
        self.query_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'intent': intent.copy(),
            'sql': sql
        })
    
    def add_modification(self, modification_type: str, details: Dict[str, Any]):
        """Add a modification to the context."""
        self.modifications.append({
            'timestamp': datetime.utcnow().isoformat(),
            'type': modification_type,
            'details': details
        })
        self.last_updated = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary."""
        return {
            'session_id': self.session_id,
            'created_at': self.created_at.isoformat(),
            'last_updated': self.last_updated.isoformat(),
            'current_intent': self.current_intent,
            'current_sql': self.current_sql,
            'query_history': self.query_history,
            'modifications': self.modifications
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConversationalContext':
        """Create context from dictionary."""
        context = cls(data['session_id'])
        context.created_at = datetime.fromisoformat(data['created_at'])
        context.last_updated = datetime.fromisoformat(data['last_updated'])
        context.current_intent = data.get('current_intent')
        context.current_sql = data.get('current_sql')
        context.query_history = data.get('query_history', [])
        context.modifications = data.get('modifications', [])
        return context


class ConversationalContextManager:
    """Manages multiple conversational contexts."""
    
    def __init__(self, cleanup_interval_minutes: int = 5):
        """
        Initialize context manager.
        
        Args:
            cleanup_interval_minutes: How often to clean up expired contexts
        """
        self.contexts: Dict[str, ConversationalContext] = {}
        self.cleanup_interval_minutes = cleanup_interval_minutes
        self.last_cleanup = datetime.utcnow()
    
    def get_or_create_context(self, session_id: str) -> ConversationalContext:
        """Get or create a conversational context."""
        # Cleanup expired contexts periodically
        if (datetime.utcnow() - self.last_cleanup).total_seconds() > self.cleanup_interval_minutes * 60:
            self._cleanup_expired()
        
        if session_id not in self.contexts:
            self.contexts[session_id] = ConversationalContext(session_id)
        
        context = self.contexts[session_id]
        
        # Check if expired
        if context.is_expired():
            # Create new context
            self.contexts[session_id] = ConversationalContext(session_id)
            return self.contexts[session_id]
        
        return context
    
    def get_context(self, session_id: str) -> Optional[ConversationalContext]:
        """Get context if it exists and is not expired."""
        if session_id not in self.contexts:
            return None
        
        context = self.contexts[session_id]
        if context.is_expired():
            del self.contexts[session_id]
            return None
        
        return context
    
    def delete_context(self, session_id: str):
        """Delete a context."""
        if session_id in self.contexts:
            del self.contexts[session_id]
    
    def _cleanup_expired(self):
        """Remove expired contexts."""
        expired_sessions = [
            session_id for session_id, context in self.contexts.items()
            if context.is_expired()
        ]
        for session_id in expired_sessions:
            del self.contexts[session_id]
        self.last_cleanup = datetime.utcnow()
    
    def get_all_contexts(self) -> Dict[str, ConversationalContext]:
        """Get all active contexts."""
        self._cleanup_expired()
        return self.contexts.copy()


# Global context manager instance
_context_manager: Optional[ConversationalContextManager] = None


def get_context_manager() -> ConversationalContextManager:
    """Get or create global context manager."""
    global _context_manager
    if _context_manager is None:
        _context_manager = ConversationalContextManager()
    return _context_manager

