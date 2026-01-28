#!/usr/bin/env python3
"""
KnowledgeBase REST API Client

Python client for interacting with the KnowledgeBase REST API server.
Provides RAG retrieval capabilities for LLM query generation.
"""

import requests
import json
from typing import Dict, List, Optional, Any
from pathlib import Path


class KnowledgeBaseClient:
    """Client for KnowledgeBase REST API."""
    
    def __init__(self, base_url: str = "http://127.0.0.1:8080"):
        """
        Initialize KnowledgeBase client.
        
        Args:
            base_url: Base URL of the KnowledgeBase API server
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
        })
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check if the API server is healthy.
        
        Returns:
            Health status dictionary
        """
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "concepts_count": 0,
                "vector_store_size": 0
            }
    
    def rag_retrieve(self, query: str, top_k: int = 10) -> Dict[str, Any]:
        """
        Retrieve relevant concepts using RAG.
        
        Args:
            query: Search query text
            top_k: Number of top results to return
            
        Returns:
            Dictionary with 'results' and 'context' keys
        """
        try:
            payload = {
                "query": query,
                "top_k": top_k
            }
            response = self.session.post(
                f"{self.base_url}/rag",
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                "results": [],
                "context": "",
                "error": str(e)
            }
    
    def get_rag_context(self, query: str, top_k: int = 10) -> str:
        """
        Get formatted RAG context string for LLM prompts.
        
        Args:
            query: Search query text
            top_k: Number of top results to return
            
        Returns:
            Formatted context string
        """
        result = self.rag_retrieve(query, top_k)
        return result.get("context", "")
    
    def search_concepts(self, query: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search for concepts by text.
        
        Args:
            query: Search query text
            top_k: Number of top results to return
            
        Returns:
            List of concept search results
        """
        try:
            params = {
                "q": query,
                "top_k": top_k
            }
            response = self.session.get(
                f"{self.base_url}/search",
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return []
    
    def list_concepts(self) -> List[Dict[str, Any]]:
        """
        List all concepts (for debugging).
        
        Returns:
            List of all concepts
        """
        try:
            response = self.session.get(f"{self.base_url}/concepts", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return []


# Global client instance (lazy initialization)
_client_instance: Optional[KnowledgeBaseClient] = None


def get_knowledge_base_client(base_url: Optional[str] = None) -> KnowledgeBaseClient:
    """
    Get or create the global KnowledgeBase client instance.
    
    Args:
        base_url: Optional base URL (uses default if not provided)
        
    Returns:
        KnowledgeBaseClient instance
    """
    global _client_instance
    
    if _client_instance is None:
        url = base_url or os.getenv("KB_API_URL", "http://127.0.0.1:8080")
        _client_instance = KnowledgeBaseClient(url)
    
    return _client_instance


# Import os for environment variable access
import os

