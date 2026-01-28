"""
Vector Database Interface

Abstract interface for vector databases to enable replaceability.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class VectorDB(ABC):
    """Interface for vector database."""
    
    @abstractmethod
    def store_embedding(self, entity_id: str, embedding: List[float], 
                       metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Store embedding in vector database.
        
        Args:
            entity_id: Entity identifier
            embedding: Vector embedding
            metadata: Optional metadata
        
        Returns:
            Storage identifier
        """
        pass
    
    @abstractmethod
    def search_similar(self, embedding: List[float], limit: int = 10,
                      filter_metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Search for similar embeddings.
        
        Args:
            embedding: Query embedding
            limit: Maximum number of results
            filter_metadata: Optional metadata filters
        
        Returns:
            List of similar entities with scores
        """
        pass
    
    @abstractmethod
    def get_embedding(self, entity_id: str) -> Optional[List[float]]:
        """
        Get embedding for entity.
        
        Args:
            entity_id: Entity identifier
        
        Returns:
            Embedding vector or None
        """
        pass
    
    @abstractmethod
    def delete_embedding(self, entity_id: str) -> bool:
        """
        Delete embedding.
        
        Args:
            entity_id: Entity identifier
        
        Returns:
            True if deleted, False otherwise
        """
        pass
    
    @abstractmethod
    def update_metadata(self, entity_id: str, metadata: Dict[str, Any]) -> bool:
        """
        Update metadata for entity.
        
        Args:
            entity_id: Entity identifier
            metadata: Metadata to update
        
        Returns:
            True if updated, False otherwise
        """
        pass

