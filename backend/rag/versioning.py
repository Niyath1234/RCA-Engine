"""
RAG Versioning

Append-only fact storage with versioning.
Facts are immutable. Corrections are new facts. Deletes are logical.
"""

import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional


class RAGVersioning:
    """Append-only fact storage with versioning."""
    
    def __init__(self, fact_store=None, derived_index=None):
        """
        Initialize RAG versioning.
        
        Args:
            fact_store: Fact storage (dict or database)
            derived_index: Derived index for latest facts
        """
        self.fact_store = fact_store or {}
        self.derived_index = derived_index or {}
    
    def store_fact(self, fact: Dict[str, Any]) -> str:
        """
        Store fact (always append, never update).
        
        Args:
            fact: Fact dictionary
        
        Returns:
            Fact ID
        """
        fact_id = self._generate_fact_id()
        
        fact_record = {
            'fact_id': fact_id,
            'fact': fact,
            'timestamp': datetime.utcnow().isoformat(),
            'version': 1,
        }
        
        # Append to fact store
        self._append_fact(fact_record)
        
        # Update derived index (non-authoritative)
        self._update_derived_index(fact_record)
        
        return fact_id
    
    def correct_fact(self, fact_id: str, correction: Dict[str, Any]) -> str:
        """
        Correct fact by creating new fact.
        
        Args:
            fact_id: Original fact ID
            correction: Correction data
        
        Returns:
            Correction fact ID
        """
        original = self._get_fact(fact_id)
        if not original:
            raise ValueError(f"Fact not found: {fact_id}")
        
        # Create correction fact
        correction_fact_id = self._generate_fact_id()
        correction_record = {
            'fact_id': correction_fact_id,
            'corrects': fact_id,
            'original': original,
            'correction': correction,
            'timestamp': datetime.utcnow().isoformat(),
            'version': original.get('version', 1) + 1,
        }
        
        # Append correction
        self._append_fact(correction_record)
        
        # Update derived index
        self._update_derived_index(correction_record)
        
        return correction_fact_id
    
    def get_latest_facts(self, entity_id: str) -> List[Dict[str, Any]]:
        """
        Get latest facts for entity (default behavior).
        
        Args:
            entity_id: Entity identifier
        
        Returns:
            List of latest facts
        """
        # Query derived index for latest_ref_id
        latest_ref_id = self._get_latest_ref_id(entity_id)
        
        if not latest_ref_id:
            return []
        
        # Build fact chain
        facts = self._build_fact_chain(latest_ref_id)
        
        return facts
    
    def get_full_history(self, entity_id: str) -> List[Dict[str, Any]]:
        """
        Get full fact history (explicit request).
        
        Args:
            entity_id: Entity identifier
        
        Returns:
            List of all facts chronologically ordered
        """
        # Query all facts for entity
        all_facts = self._get_all_facts(entity_id)
        
        # Return chronologically ordered
        return sorted(all_facts, key=lambda f: f.get('timestamp', ''))
    
    def _append_fact(self, fact_record: Dict[str, Any]):
        """Append fact to store."""
        fact_id = fact_record['fact_id']
        self.fact_store[fact_id] = fact_record
    
    def _update_derived_index(self, fact_record: Dict[str, Any]):
        """Update derived index (can be rebuilt)."""
        # Extract entity ID from fact
        fact = fact_record.get('fact', {})
        entity_id = fact.get('entity_id')
        
        if entity_id:
            # Update latest_ref_id index
            self.derived_index[entity_id] = fact_record['fact_id']
    
    def _get_fact(self, fact_id: str) -> Optional[Dict[str, Any]]:
        """Get fact by ID."""
        return self.fact_store.get(fact_id)
    
    def _get_latest_ref_id(self, entity_id: str) -> Optional[str]:
        """Get latest reference ID for entity."""
        return self.derived_index.get(entity_id)
    
    def _build_fact_chain(self, latest_ref_id: str) -> List[Dict[str, Any]]:
        """Build fact chain from latest reference."""
        facts = []
        current_id = latest_ref_id
        
        while current_id:
            fact = self._get_fact(current_id)
            if not fact:
                break
            
            facts.append(fact)
            
            # Check if this corrects another fact
            corrects = fact.get('corrects')
            if corrects:
                current_id = corrects
            else:
                break
        
        return facts
    
    def _get_all_facts(self, entity_id: str) -> List[Dict[str, Any]]:
        """Get all facts for entity."""
        facts = []
        for fact_id, fact_record in self.fact_store.items():
            fact = fact_record.get('fact', {})
            if fact.get('entity_id') == entity_id:
                facts.append(fact_record)
        return facts
    
    def rebuild_index(self):
        """Rebuild derived index from facts."""
        # Clear index
        self.derived_index = {}
        
        # Recompute latest_ref_id from fact chain
        entity_facts = {}
        for fact_id, fact_record in self.fact_store.items():
            fact = fact_record.get('fact', {})
            entity_id = fact.get('entity_id')
            
            if entity_id:
                if entity_id not in entity_facts:
                    entity_facts[entity_id] = []
                entity_facts[entity_id].append(fact_record)
        
        # For each entity, find latest fact
        for entity_id, facts in entity_facts.items():
            # Sort by timestamp, get latest
            sorted_facts = sorted(facts, key=lambda f: f.get('timestamp', ''), reverse=True)
            if sorted_facts:
                self.derived_index[entity_id] = sorted_facts[0]['fact_id']
    
    def _generate_fact_id(self) -> str:
        """Generate unique fact ID."""
        return str(uuid.uuid4())

