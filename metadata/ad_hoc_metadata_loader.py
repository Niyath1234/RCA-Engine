#!/usr/bin/env python3
"""
Load Ad-Hoc Metadata for Enterprise Pipeline

This script loads all ad-hoc metadata files and merges them into a single
metadata dictionary for use with the enterprise pipeline.
"""

import json
from pathlib import Path
from typing import Dict, Any, List

def load_ad_hoc_metadata(metadata_dir: str = None) -> Dict[str, Any]:
    """
    Load all ad-hoc metadata files and merge into single dictionary.
    
    Args:
        metadata_dir: Directory containing metadata files (defaults to metadata/)
    
    Returns:
        Complete metadata dictionary
    """
    if metadata_dir is None:
        metadata_dir = Path(__file__).parent
    
    metadata = {
        "tables": {},
        "knowledge_base": {},
        "rules": [],
        "semantic_registry": {},
        "lineage": {},
        "entities": [],
        "business_labels": {}
    }
    
    # Load tables
    tables_file = Path(metadata_dir) / "ad_hoc_tables.json"
    if tables_file.exists():
        with open(tables_file, 'r') as f:
            tables_data = json.load(f)
            metadata["tables"] = tables_data.get("tables", [])
    
    # Load knowledge base
    kb_file = Path(metadata_dir) / "ad_hoc_knowledge_base.json"
    if kb_file.exists():
        with open(kb_file, 'r') as f:
            kb_data = json.load(f)
            metadata["knowledge_base"] = kb_data.get("terms", {})
    
    # Load rules
    rules_file = Path(metadata_dir) / "ad_hoc_rules.json"
    if rules_file.exists():
        with open(rules_file, 'r') as f:
            metadata["rules"] = json.load(f)
    
    # Load semantic registry
    semantic_file = Path(metadata_dir) / "ad_hoc_semantic_registry.json"
    if semantic_file.exists():
        with open(semantic_file, 'r') as f:
            semantic_data = json.load(f)
            metadata["semantic_registry"] = {
                "metrics": semantic_data.get("metrics", []),
                "dimensions": semantic_data.get("dimensions", []),
                "computed_dimensions": semantic_data.get("computed_dimensions", [])
            }
    
    # Load lineage
    lineage_file = Path(metadata_dir) / "ad_hoc_lineage.json"
    if lineage_file.exists():
        with open(lineage_file, 'r') as f:
            metadata["lineage"] = json.load(f)
    
    # Load entities
    entities_file = Path(metadata_dir) / "ad_hoc_entities.json"
    if entities_file.exists():
        with open(entities_file, 'r') as f:
            entities_data = json.load(f)
            metadata["entities"] = entities_data.get("entities", [])
    
    # Load business labels
    labels_file = Path(metadata_dir) / "ad_hoc_business_labels.json"
    if labels_file.exists():
        with open(labels_file, 'r') as f:
            labels_data = json.load(f)
            metadata["business_labels"] = labels_data.get("labels", {})
    
    # Convert tables list to dict for easier lookup
    tables_dict = {}
    for table in metadata["tables"]:
        tables_dict[table["name"]] = table
    
    metadata["tables_dict"] = tables_dict
    
    return metadata


def get_metadata_for_pipeline(metadata_dir: str = None) -> Dict[str, Any]:
    """
    Get metadata formatted for enterprise pipeline.
    
    Returns:
        Metadata dictionary compatible with enterprise pipeline
    """
    metadata = load_ad_hoc_metadata(metadata_dir)
    
    # Format for pipeline
    return {
        "tables": {
            "tables": metadata["tables"]
        },
        "knowledge_base": metadata["knowledge_base"],
        "rules": metadata["rules"],
        "semantic_registry": metadata["semantic_registry"],
        "lineage": metadata["lineage"],
        "entities": metadata["entities"],
        "business_labels": metadata["business_labels"]
    }


if __name__ == "__main__":
    # Test loading
    metadata = get_metadata_for_pipeline()
    print(f"Loaded {len(metadata['tables']['tables'])} tables")
    print(f"Loaded {len(metadata['knowledge_base'])} knowledge base terms")
    print(f"Loaded {len(metadata['rules'])} rules")
    print(f"Loaded {len(metadata['semantic_registry']['metrics'])} metrics")
    print(f"Loaded {len(metadata['semantic_registry']['dimensions'])} dimensions")

