"""
Reproducibility Engine

Invariant 3: Every answer must be reproducible.
Given user input, metadata snapshot, and generated SQL,
you must be able to replay the answer.
"""

import hashlib
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict


@dataclass
class ExecutionContext:
    """Captured execution context for reproducibility."""
    query: str
    sql: str
    metadata_version: str
    metadata_snapshot: Dict[str, Any]
    timestamp: str
    llm_model: str
    llm_temperature: float
    config_hash: str
    planning_id: Optional[str] = None
    execution_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionContext':
        """Create from dictionary."""
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'ExecutionContext':
        """Deserialize from JSON."""
        return cls.from_dict(json.loads(json_str))


class ReproducibilityEngine:
    """Ensure all queries are reproducible."""
    
    def __init__(self, metadata_store=None, config_store=None):
        """
        Initialize reproducibility engine.
        
        Args:
            metadata_store: Storage for metadata snapshots
            config_store: Storage for configuration snapshots
        """
        self.metadata_store = metadata_store or {}
        self.config_store = config_store or {}
    
    def capture_execution_context(
        self,
        query: str,
        sql: str,
        metadata_version: str,
        metadata_snapshot: Dict[str, Any],
        llm_model: str,
        llm_temperature: float,
        config: Dict[str, Any],
        planning_id: Optional[str] = None,
        execution_id: Optional[str] = None
    ) -> ExecutionContext:
        """
        Capture full execution context.
        
        Args:
            query: User query
            sql: Generated SQL
            metadata_version: Version identifier for metadata
            metadata_snapshot: Full metadata snapshot
            llm_model: LLM model used
            llm_temperature: LLM temperature setting
            config: Configuration dictionary
            planning_id: Optional planning ID
            execution_id: Optional execution ID
        
        Returns:
            ExecutionContext object
        """
        # Store metadata snapshot if not already stored
        if metadata_version not in self.metadata_store:
            self.metadata_store[metadata_version] = metadata_snapshot
        
        # Compute config hash
        config_hash = self._compute_config_hash(config)
        
        # Store config snapshot if not already stored
        if config_hash not in self.config_store:
            self.config_store[config_hash] = config
        
        context = ExecutionContext(
            query=query,
            sql=sql,
            metadata_version=metadata_version,
            metadata_snapshot=metadata_snapshot,
            timestamp=datetime.utcnow().isoformat(),
            llm_model=llm_model,
            llm_temperature=llm_temperature,
            config_hash=config_hash,
            planning_id=planning_id,
            execution_id=execution_id,
        )
        
        return context
    
    def replay_query(
        self,
        execution_context: ExecutionContext,
        llm_provider=None,
        db_executor=None
    ) -> Dict[str, Any]:
        """
        Replay query from captured context.
        
        Args:
            execution_context: Captured execution context
            llm_provider: LLM provider for regeneration (optional)
            db_executor: Database executor (optional)
        
        Returns:
            Replay result dictionary
        """
        # Restore metadata snapshot
        metadata = self.metadata_store.get(
            execution_context.metadata_version,
            execution_context.metadata_snapshot
        )
        
        # Restore config
        config = self.config_store.get(
            execution_context.config_hash,
            {}
        )
        
        # If LLM provider provided, regenerate SQL
        if llm_provider:
            # Regenerate SQL with same parameters
            regenerated_sql = llm_provider.generate_sql(
                prompt=execution_context.query,
                context={
                    'metadata': metadata,
                    'temperature': execution_context.llm_temperature,
                    'model': execution_context.llm_model,
                }
            )
        else:
            # Use original SQL
            regenerated_sql = execution_context.sql
        
        # Execute query if executor provided
        if db_executor:
            result = db_executor.execute(regenerated_sql)
        else:
            result = {'sql': regenerated_sql, 'executed': False}
        
        return {
            'original_context': execution_context.to_dict(),
            'regenerated_sql': regenerated_sql,
            'result': result,
            'replay_timestamp': datetime.utcnow().isoformat(),
        }
    
    def get_metadata_snapshot(self, version: str) -> Optional[Dict[str, Any]]:
        """
        Get versioned metadata snapshot.
        
        Args:
            version: Version identifier
        
        Returns:
            Metadata snapshot or None
        """
        return self.metadata_store.get(version)
    
    def store_metadata_snapshot(self, version: str, snapshot: Dict[str, Any]):
        """
        Store metadata snapshot.
        
        Args:
            version: Version identifier
            snapshot: Metadata snapshot
        """
        self.metadata_store[version] = snapshot
    
    def _compute_config_hash(self, config: Dict[str, Any]) -> str:
        """
        Compute hash of configuration.
        
        Args:
            config: Configuration dictionary
        
        Returns:
            Hash string
        """
        # Sort keys for consistent hashing
        config_str = json.dumps(config, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()[:16]
    
    def generate_metadata_version(self, metadata: Dict[str, Any]) -> str:
        """
        Generate version identifier for metadata.
        
        Args:
            metadata: Metadata dictionary
        
        Returns:
            Version identifier
        """
        # Create hash from metadata
        metadata_str = json.dumps(metadata, sort_keys=True)
        version = hashlib.sha256(metadata_str.encode()).hexdigest()[:16]
        
        # Store snapshot
        self.store_metadata_snapshot(version, metadata)
        
        return version

