#!/usr/bin/env python3
"""
Metadata Ingestion API

This module provides API endpoints for ingesting natural language metadata
and converting it to structured JSON files that the RCA Engine can use.

It orchestrates the natural language parsers and saves the results to metadata files.
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from natural_language_metadata_parser import NaturalLanguageMetadataParser


class MetadataIngestionService:
    """Service for ingesting natural language metadata."""
    
    def __init__(self, metadata_dir: Optional[str] = None):
        self.metadata_dir = Path(metadata_dir) if metadata_dir else Path(__file__).parent.parent / "metadata"
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.parser = NaturalLanguageMetadataParser()
    
    def ingest_table(self, table_description: str, system: Optional[str] = None, 
                    output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Ingest a single table description and add it to tables.json.
        
        Args:
            table_description: Natural language table description
            system: Optional system name
            output_file: Optional output file path (default: metadata/tables.json)
        
        Returns:
            Dictionary with success status and parsed table data
        """
        try:
            # Parse table description
            table_data = self.parser.parse_table_description(table_description, system)
            
            # Load existing tables
            tables_file = self.metadata_dir / (output_file or "tables.json")
            if tables_file.exists():
                with open(tables_file, 'r') as f:
                    existing_data = json.load(f)
                tables = existing_data.get("tables", [])
            else:
                tables = []
            
            # Check if table already exists
            table_name = table_data.get("name")
            existing_table_names = [t.get("name") for t in tables]
            
            if table_name in existing_table_names:
                # Update existing table
                for i, table in enumerate(tables):
                    if table.get("name") == table_name:
                        tables[i] = table_data
                        break
            else:
                # Add new table
                tables.append(table_data)
            
            # Save updated tables
            output_data = {"tables": tables}
            with open(tables_file, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            return {
                "success": True,
                "table": table_data,
                "message": f"Table '{table_name}' {'updated' if table_name in existing_table_names else 'added'} successfully"
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def ingest_join_condition(self, join_text: str, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Ingest a join condition and add it to lineage.json.
        
        Args:
            join_text: Natural language join condition
            output_file: Optional output file path (default: metadata/lineage.json)
        
        Returns:
            Dictionary with success status and parsed edge data
        """
        try:
            # Load existing tables to help with table name resolution
            tables_file = self.metadata_dir / "tables.json"
            existing_tables = []
            if tables_file.exists():
                with open(tables_file, 'r') as f:
                    tables_data = json.load(f)
                    existing_tables = [t.get("name") for t in tables_data.get("tables", [])]
            
            # Parse join condition
            edge_data = self.parser.parse_join_condition(join_text, existing_tables)
            
            # Load existing lineage
            lineage_file = self.metadata_dir / (output_file or "lineage.json")
            if lineage_file.exists():
                with open(lineage_file, 'r') as f:
                    existing_data = json.load(f)
                edges = existing_data.get("edges", [])
            else:
                edges = []
            
            # Check if edge already exists (same from/to tables)
            from_table = edge_data.get("from")
            to_table = edge_data.get("to")
            
            edge_exists = False
            for i, edge in enumerate(edges):
                if edge.get("from") == from_table and edge.get("to") == to_table:
                    # Update existing edge
                    edges[i] = edge_data
                    edge_exists = True
                    break
            
            if not edge_exists:
                # Add new edge
                edges.append(edge_data)
            
            # Save updated lineage
            output_data = {"edges": edges, "possible_joins": existing_data.get("possible_joins", [])}
            with open(lineage_file, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            return {
                "success": True,
                "edge": edge_data,
                "message": f"Join condition from '{from_table}' to '{to_table}' {'updated' if edge_exists else 'added'} successfully"
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def ingest_business_rules(self, rules_text: str, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Ingest business rules and add them to rules.json.
        
        Args:
            rules_text: Natural language business rules
            output_file: Optional output file path (default: metadata/rules.json)
        
        Returns:
            Dictionary with success status and parsed rules data
        """
        try:
            # Parse business rules
            rules_data = self.parser.parse_business_rules(rules_text)
            
            # Load existing rules
            rules_file = self.metadata_dir / (output_file or "rules.json")
            if rules_file.exists():
                with open(rules_file, 'r') as f:
                    existing_rules = json.load(f)
                if not isinstance(existing_rules, list):
                    existing_rules = []
            else:
                existing_rules = []
            
            # Merge rules (avoid duplicates by ID)
            existing_rule_ids = {rule.get("id") for rule in existing_rules if rule.get("id")}
            
            for rule in rules_data:
                rule_id = rule.get("id")
                if rule_id and rule_id in existing_rule_ids:
                    # Update existing rule
                    for i, existing_rule in enumerate(existing_rules):
                        if existing_rule.get("id") == rule_id:
                            existing_rules[i] = rule
                            break
                else:
                    # Add new rule
                    existing_rules.append(rule)
            
            # Save updated rules
            with open(rules_file, 'w') as f:
                json.dump(existing_rules, f, indent=2)
            
            return {
                "success": True,
                "rules": rules_data,
                "message": f"Added/updated {len(rules_data)} business rules successfully"
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def ingest_complete_metadata(self, metadata_text: str, system: Optional[str] = None) -> Dict[str, Any]:
        """
        Ingest complete metadata from a single text block containing tables, joins, and rules.
        
        Args:
            metadata_text: Natural language metadata containing tables, joins, and rules
            system: Optional system name
        
        Returns:
            Dictionary with success status and all parsed data
        """
        results = {
            "success": True,
            "tables": [],
            "edges": [],
            "rules": [],
            "errors": []
        }
        
        # Try to split into sections
        sections = {
            "tables": [],
            "joins": [],
            "rules": []
        }
        
        current_section = None
        current_content = []
        
        lines = metadata_text.split('\n')
        for line in lines:
            line_lower = line.lower().strip()
            
            # Detect section headers
            if any(keyword in line_lower for keyword in ['table:', 'table name:', '## table']):
                if current_section and current_content:
                    sections[current_section].append('\n'.join(current_content))
                current_section = "tables"
                current_content = [line]
            elif any(keyword in line_lower for keyword in ['join', 'connects', 'links']):
                if current_section and current_content:
                    sections[current_section].append('\n'.join(current_content))
                current_section = "joins"
                current_content = [line]
            elif any(keyword in line_lower for keyword in ['business rule', 'rule:', 'validation']):
                if current_section and current_content:
                    sections[current_section].append('\n'.join(current_content))
                current_section = "rules"
                current_content = [line]
            elif current_section:
                current_content.append(line)
        
        # Process last section
        if current_section and current_content:
            sections[current_section].append('\n'.join(current_content))
        
        # Process tables
        for table_text in sections["tables"]:
            try:
                result = self.ingest_table(table_text, system)
                if result.get("success"):
                    results["tables"].append(result.get("table"))
                else:
                    results["errors"].append(f"Table parsing error: {result.get('error')}")
            except Exception as e:
                results["errors"].append(f"Table parsing exception: {str(e)}")
        
        # Process joins
        for join_text in sections["joins"]:
            try:
                result = self.ingest_join_condition(join_text)
                if result.get("success"):
                    results["edges"].append(result.get("edge"))
                else:
                    results["errors"].append(f"Join parsing error: {result.get('error')}")
            except Exception as e:
                results["errors"].append(f"Join parsing exception: {str(e)}")
        
        # Process rules
        if sections["rules"]:
            rules_text = '\n\n'.join(sections["rules"])
            try:
                result = self.ingest_business_rules(rules_text)
                if result.get("success"):
                    results["rules"] = result.get("rules", [])
                else:
                    results["errors"].append(f"Rules parsing error: {result.get('error')}")
            except Exception as e:
                results["errors"].append(f"Rules parsing exception: {str(e)}")
        
        if results["errors"]:
            results["success"] = False
        
        return results


def ingest_metadata_from_text(metadata_text: str, system: Optional[str] = None, 
                               metadata_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function to ingest complete metadata from text.
    
    Args:
        metadata_text: Natural language metadata text
        system: Optional system name
        metadata_dir: Optional metadata directory path
    
    Returns:
        Dictionary with ingestion results
    """
    service = MetadataIngestionService(metadata_dir)
    return service.ingest_complete_metadata(metadata_text, system)


if __name__ == "__main__":
    # CLI mode for testing
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python metadata_ingestion_api.py <command> [args...]")
        print("\nCommands:")
        print("  table <description> [system] - Ingest a table description")
        print("  join <join_condition> - Ingest a join condition")
        print("  rules <rules_text> - Ingest business rules")
        print("  complete <metadata_text> [system] - Ingest complete metadata")
        sys.exit(1)
    
    command = sys.argv[1]
    service = MetadataIngestionService()
    
    if command == "table":
        if len(sys.argv) < 3:
            print("Error: Table description required")
            sys.exit(1)
        description = sys.argv[2]
        system = sys.argv[3] if len(sys.argv) > 3 else None
        result = service.ingest_table(description, system)
        print(json.dumps(result, indent=2))
    
    elif command == "join":
        if len(sys.argv) < 3:
            print("Error: Join condition required")
            sys.exit(1)
        join_text = sys.argv[2]
        result = service.ingest_join_condition(join_text)
        print(json.dumps(result, indent=2))
    
    elif command == "rules":
        if len(sys.argv) < 3:
            print("Error: Business rules text required")
            sys.exit(1)
        rules_text = sys.argv[2]
        result = service.ingest_business_rules(rules_text)
        print(json.dumps(result, indent=2))
    
    elif command == "complete":
        if len(sys.argv) < 3:
            print("Error: Metadata text required")
            sys.exit(1)
        metadata_text = sys.argv[2]
        system = sys.argv[3] if len(sys.argv) > 3 else None
        result = service.ingest_complete_metadata(metadata_text, system)
        print(json.dumps(result, indent=2))
    
    else:
        print(f"Error: Unknown command: {command}")
        sys.exit(1)

