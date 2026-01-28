"""
LLM Provider Interface

Abstract interface for LLM providers to enable replaceability.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class LLMProvider(ABC):
    """Interface for LLM providers."""
    
    @abstractmethod
    def generate_sql(self, prompt: str, context: Dict[str, Any]) -> str:
        """
        Generate SQL from prompt.
        
        Args:
            prompt: User query or prompt
            context: Context dictionary (schema, metadata, etc.)
        
        Returns:
            Generated SQL query string
        """
        pass
    
    @abstractmethod
    def explain_result(self, sql: str, result: Dict[str, Any]) -> str:
        """
        Explain query result in natural language.
        
        Args:
            sql: SQL query that was executed
            result: Query result dictionary
        
        Returns:
            Natural language explanation
        """
        pass
    
    @abstractmethod
    def extract_intent(self, user_query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract intent from user query.
        
        Args:
            user_query: User's natural language query
            context: Additional context
        
        Returns:
            Intent dictionary
        """
        pass
    
    @abstractmethod
    def get_model_name(self) -> str:
        """
        Get model name/identifier.
        
        Returns:
            Model name string
        """
        pass
    
    @abstractmethod
    def get_temperature(self) -> float:
        """
        Get current temperature setting.
        
        Returns:
            Temperature value
        """
        pass
    
    @abstractmethod
    def set_temperature(self, temperature: float):
        """
        Set temperature for deterministic generation.
        
        Args:
            temperature: Temperature value (0.0 for deterministic)
        """
        pass

