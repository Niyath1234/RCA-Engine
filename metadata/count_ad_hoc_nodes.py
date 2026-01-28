#!/usr/bin/env python3
"""
Count Nodes Created from Ad-Hoc Metadata

Analyzes all metadata files and counts how many nodes would be created
in the knowledge graph.
"""

import json
from pathlib import Path
from collections import defaultdict

def count_nodes_from_metadata(metadata_dir: str = None):
    """Count all nodes that would be created from metadata."""
    if metadata_dir is None:
        metadata_dir = Path(__file__).parent
    
    node_counts = defaultdict(int)
    node_details = defaultdict(list)
    
    # Load tables
    tables_file = Path(metadata_dir) / "ad_hoc_tables.json"
    if tables_file.exists():
        with open(tables_file, 'r') as f:
            tables_data = json.load(f)
            tables = tables_data.get("tables", [])
            
            for table in tables:
                # Table node
                node_counts["table"] += 1
                node_details["table"].append(f"table:{table['name']}")
                
                # Column nodes
                columns = table.get("columns", [])
                for col in columns:
                    node_counts["column"] += 1
                    node_details["column"].append(f"column:{table['name']}.{col['name']}")
                
                # Primary key nodes (if multiple)
                pk = table.get("primary_key", [])
                if len(pk) > 1:
                    node_counts["primary_key"] += len(pk)
                    for key in pk:
                        node_details["primary_key"].append(f"pk:{table['name']}.{key}")
    
    # Load knowledge base (business terms)
    kb_file = Path(metadata_dir) / "ad_hoc_knowledge_base.json"
    if kb_file.exists():
        with open(kb_file, 'r') as f:
            kb_data = json.load(f)
            terms = kb_data.get("terms", {})
            
            for term_name, term_data in terms.items():
                node_counts["business_term"] += 1
                node_details["business_term"].append(f"term:{term_name}")
    
    # Load rules
    rules_file = Path(metadata_dir) / "ad_hoc_rules.json"
    if rules_file.exists():
        with open(rules_file, 'r') as f:
            rules = json.load(f)
            
            for rule in rules:
                node_counts["rule"] += 1
                node_details["rule"].append(f"rule:{rule.get('id', 'unknown')}")
    
    # Load semantic registry (metrics and dimensions)
    semantic_file = Path(metadata_dir) / "ad_hoc_semantic_registry.json"
    if semantic_file.exists():
        with open(semantic_file, 'r') as f:
            semantic_data = json.load(f)
            
            # Metrics
            metrics = semantic_data.get("metrics", [])
            for metric in metrics:
                node_counts["metric"] += 1
                node_details["metric"].append(f"metric:{metric.get('name', 'unknown')}")
            
            # Dimensions
            dimensions = semantic_data.get("dimensions", [])
            for dim in dimensions:
                node_counts["dimension"] += 1
                node_details["dimension"].append(f"dimension:{dim.get('name', 'unknown')}")
            
            # Computed dimensions
            computed_dims = semantic_data.get("computed_dimensions", [])
            for comp_dim in computed_dims:
                node_counts["computed_dimension"] += 1
                node_details["computed_dimension"].append(f"computed_dim:{comp_dim.get('name', 'unknown')}")
    
    # Load entities
    entities_file = Path(metadata_dir) / "ad_hoc_entities.json"
    if entities_file.exists():
        with open(entities_file, 'r') as f:
            entities_data = json.load(f)
            entities = entities_data.get("entities", [])
            
            for entity in entities:
                node_counts["entity"] += 1
                node_details["entity"].append(f"entity:{entity.get('name', 'unknown')}")
    
    # Load lineage (relationships create relationship nodes)
    lineage_file = Path(metadata_dir) / "ad_hoc_lineage.json"
    if lineage_file.exists():
        with open(lineage_file, 'r') as f:
            lineage_data = json.load(f)
            
            # Relationships
            relationships = lineage_data.get("relationships", [])
            for rel in relationships:
                node_counts["relationship"] += 1
                node_details["relationship"].append(f"rel:{rel.get('from_table')}->{rel.get('to_table')}")
            
            # Table groups
            table_groups = lineage_data.get("table_groups", {})
            for group_name, group_data in table_groups.items():
                node_counts["table_group"] += 1
                node_details["table_group"].append(f"group:{group_name}")
    
    # Load business labels
    labels_file = Path(metadata_dir) / "ad_hoc_business_labels.json"
    if labels_file.exists():
        with open(labels_file, 'r') as f:
            labels_data = json.load(f)
            labels = labels_data.get("labels", {})
            
            for label_name, label_data in labels.items():
                node_counts["business_label"] += 1
                node_details["business_label"].append(f"label:{label_name}")
    
    return node_counts, node_details


def print_node_summary(node_counts, node_details):
    """Print summary of nodes."""
    print("\n" + "="*80)
    print("KNOWLEDGE GRAPH NODE COUNT SUMMARY")
    print("="*80)
    
    total_nodes = sum(node_counts.values())
    
    print(f"\nTotal Nodes: {total_nodes}\n")
    print("-"*80)
    print(f"{'Node Type':<30} {'Count':<10} {'Percentage':<10}")
    print("-"*80)
    
    for node_type in sorted(node_counts.keys()):
        count = node_counts[node_type]
        percentage = (count / total_nodes * 100) if total_nodes > 0 else 0
        print(f"{node_type:<30} {count:<10} {percentage:>6.2f}%")
    
    print("-"*80)
    print(f"{'TOTAL':<30} {total_nodes:<10} {'100.00%':<10}")
    print("-"*80)
    
    # Show details
    print("\n" + "="*80)
    print("NODE DETAILS BY TYPE")
    print("="*80)
    
    for node_type in sorted(node_details.keys()):
        nodes = node_details[node_type]
        print(f"\n{node_type.upper()} ({len(nodes)} nodes):")
        for node in nodes[:20]:  # Show first 20
            print(f"  • {node}")
        if len(nodes) > 20:
            print(f"  ... and {len(nodes) - 20} more")
    
    # Table breakdown
    print("\n" + "="*80)
    print("TABLE BREAKDOWN")
    print("="*80)
    
    tables_file = Path(__file__).parent / "ad_hoc_tables.json"
    if tables_file.exists():
        with open(tables_file, 'r') as f:
            tables_data = json.load(f)
            tables = tables_data.get("tables", [])
            
            print(f"\nTotal Tables: {len(tables)}\n")
            for table in tables:
                col_count = len(table.get("columns", []))
                print(f"  • {table['name']}")
                print(f"    - Columns: {col_count}")
                print(f"    - Primary Key: {', '.join(table.get('primary_key', []))}")
                print(f"    - Time Column: {table.get('time_column', 'None')}")
                print()


if __name__ == "__main__":
    node_counts, node_details = count_nodes_from_metadata()
    print_node_summary(node_counts, node_details)
    
    # Also show relationship count
    print("\n" + "="*80)
    print("RELATIONSHIPS")
    print("="*80)
    
    lineage_file = Path(__file__).parent / "ad_hoc_lineage.json"
    if lineage_file.exists():
        with open(lineage_file, 'r') as f:
            lineage_data = json.load(f)
            relationships = lineage_data.get("relationships", [])
            table_groups = lineage_data.get("table_groups", {})
            
            print(f"\nTable Relationships: {len(relationships)}")
            for rel in relationships:
                print(f"  • {rel['from_table']} -> {rel['to_table']} ({rel.get('join_type', 'INNER')})")
            
            print(f"\nTable Groups: {len(table_groups)}")
            for group_name, group_data in table_groups.items():
                tables = group_data.get("tables", [])
                print(f"  • {group_name}: {len(tables)} tables")

