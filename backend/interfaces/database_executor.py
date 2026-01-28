"""
Database Executor Interface

Abstract interface for database execution to enable replaceability.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List


class DatabaseExecutor(ABC):
    """Interface for database execution."""
    
    @abstractmethod
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute SQL query.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
        
        Returns:
            Query result dictionary
        """
        pass
    
    @abstractmethod
    def execute_readonly(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute read-only query (enforced).
        
        Args:
            sql: SQL query string
            params: Optional query parameters
        
        Returns:
            Query result dictionary
        """
        pass
    
    @abstractmethod
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get table schema.
        
        Args:
            table_name: Table name
        
        Returns:
            Schema dictionary
        """
        pass
    
    @abstractmethod
    def list_tables(self) -> List[str]:
        """
        List all tables.
        
        Returns:
            List of table names
        """
        pass
    
    @abstractmethod
    def validate_query(self, sql: str) -> bool:
        """
        Validate query syntax.
        
        Args:
            sql: SQL query string
        
        Returns:
            True if valid, False otherwise
        """
        pass
    
    @abstractmethod
    def estimate_cost(self, sql: str) -> Dict[str, Any]:
        """
        Estimate query execution cost.
        
        Args:
            sql: SQL query string
        
        Returns:
            Cost estimate dictionary
        """
        pass

