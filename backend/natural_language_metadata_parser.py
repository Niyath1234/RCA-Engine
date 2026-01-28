#!/usr/bin/env python3
"""
Natural Language Metadata Parser

This module parses natural language table descriptions, join conditions, and business rules
into structured JSON metadata that the RCA Engine can use.

It uses LLM to extract structured information from natural language input.
"""

import json
import os
import re
from typing import Dict, List, Any, Optional, Tuple
import requests


class NaturalLanguageMetadataParser:
    """Parser for natural language metadata input."""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4"):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")
        self.model = model
        self.base_url = "https://api.openai.com/v1/chat/completions"
    
    def call_llm(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """Call OpenAI API."""
        if not self.api_key:
            raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,  # Low temperature for deterministic output
            "max_tokens": 4000
        }
        
        try:
            response = requests.post(self.base_url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            result = response.json()
            return result["choices"][0]["message"]["content"]
        except Exception as e:
            raise Exception(f"LLM API call failed: {str(e)}")
    
    def parse_table_description(self, text: str, system: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse natural language table description into structured table metadata.
        
        Input examples:
        - "Table: customers\nDescription: Customer master data\nColumns:\n  - customer_id (string): Unique identifier"
        - "I have a customers table with customer_id, name, email columns. It contains customer master data."
        
        Returns:
            Dictionary matching tables.json format
        """
        system_prompt = """You are a metadata parser. Extract structured table information from natural language descriptions.

Your task is to parse natural language table descriptions and convert them to structured JSON format.

CRITICAL INSTRUCTIONS:
- Extract table name, description, columns, primary keys, time columns
- Infer data types from context (string, int64, float64, date, timestamp, boolean)
- Identify primary keys (usually columns with "id", "key", "identifier" in name)
- Identify time columns (usually columns with "date", "time", "timestamp" in name)
- Extract column descriptions from natural language
- If system name is not provided, infer from table name (e.g., "system_a_customers" -> system: "system_a")
- If entity type is not clear, infer from table name (e.g., "customers" -> entity: "customer")

Return ONLY valid JSON matching this structure:
{
  "name": "table_name",
  "entity": "entity_type",
  "primary_key": ["column1"],
  "time_column": "column_name" or null,
  "system": "system_name" or null,
  "description": "table description",
  "columns": [
    {
      "name": "column_name",
      "description": "column description",
      "data_type": "string|int64|float64|date|timestamp|boolean"
    }
  ]
}"""
        
        user_prompt = f"""Parse this natural language table description into structured JSON:

TABLE DESCRIPTION:
{text}

{f"SYSTEM: {system}" if system else ""}

Extract:
1. Table name
2. Table description
3. All columns with their data types and descriptions
4. Primary key columns (usually ID columns)
5. Time column (if any - usually date/timestamp columns)
6. System name (if not provided, infer from table name)
7. Entity type (infer from table name/description)

Return ONLY the JSON object (no markdown, no code blocks, no explanations):"""
        
        try:
            response = self.call_llm(user_prompt, system_prompt)
            
            # Clean JSON response
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            table_data = json.loads(response)
            
            # Ensure required fields
            if "name" not in table_data:
                raise ValueError("Table name not found in parsed output")
            if "columns" not in table_data:
                table_data["columns"] = []
            
            return table_data
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse LLM response as JSON: {e}\nResponse: {response[:500]}")
        except Exception as e:
            raise Exception(f"Table description parsing failed: {e}")
    
    def parse_join_condition(self, text: str, existing_tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Parse natural language join condition into structured lineage edge.
        
        Input examples:
        - "Table A joins Table C based on customer_id"
        - "Table A joins Table C based on customer_id and cif"
        - "customers table joins orders table where customers.customer_id equals orders.cust_id"
        - "customers left joins orders on customer_id"
        
        Returns:
            Dictionary matching lineage.json edge format
        """
        system_prompt = """You are a join condition parser. Extract structured join information from natural language.

Your task is to parse natural language join descriptions and convert them to structured JSON format.

CRITICAL INSTRUCTIONS:
- Extract source table (from_table) and target table (to_table)
- Extract join keys (column pairs)
- Infer relationship type:
  * "one to many" or "one-to-many" -> one_to_many
  * "many to one" or "many-to-one" -> many_to_one
  * "one to one" or "one-to-one" -> one_to_one
  * "many to many" or "many-to-many" -> many_to_many
  * Default: infer from context (usually one_to_many)
- If column names are the same in both tables, use same name for both keys
- If column names differ, map them correctly (e.g., customer_id -> cust_id)

Return ONLY valid JSON matching this structure:
{
  "type": "edge",
  "from": "source_table_name",
  "to": "target_table_name",
  "keys": {
    "left_column": "right_column"
  },
  "relationship": "one_to_many|many_to_one|one_to_one|many_to_many"
}"""
        
        tables_context = ""
        if existing_tables:
            tables_context = f"\nEXISTING TABLES: {', '.join(existing_tables)}"
        
        user_prompt = f"""Parse this natural language join condition into structured JSON:

JOIN CONDITION:
{text}
{tables_context}

Extract:
1. Source table (from_table)
2. Target table (to_table)
3. Join keys (column pairs)
4. Relationship type (one_to_many, many_to_one, one_to_one, many_to_many)

If table names are ambiguous, use the exact names from the input or match against existing tables.

Return ONLY the JSON object (no markdown, no code blocks, no explanations):"""
        
        try:
            response = self.call_llm(user_prompt, system_prompt)
            
            # Clean JSON response
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            edge_data = json.loads(response)
            
            # Ensure required fields
            if "from" not in edge_data or "to" not in edge_data:
                raise ValueError("Table names not found in parsed output")
            if "keys" not in edge_data:
                raise ValueError("Join keys not found in parsed output")
            if "relationship" not in edge_data:
                edge_data["relationship"] = "one_to_many"  # Default
            if "type" not in edge_data:
                edge_data["type"] = "edge"
            
            return edge_data
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse LLM response as JSON: {e}\nResponse: {response[:500]}")
        except Exception as e:
            raise Exception(f"Join condition parsing failed: {e}")
    
    def parse_business_rules(self, text: str) -> List[Dict[str, Any]]:
        """
        Parse natural language business rules into structured rules.
        
        Input examples:
        - "Business Rules:\n- Each customer can have multiple orders\n- Only active customers can place orders"
        - "Rule: Order total is sum of line items"
        - "Validation: Customer status must be active before placing order"
        
        Returns:
            List of dictionaries matching rules.json format
        """
        system_prompt = """You are a business rules parser. Extract structured business rules from natural language.

Your task is to parse natural language business rules and convert them to structured JSON format.

CRITICAL INSTRUCTIONS:
- Extract rule descriptions
- Identify rule types:
  * Calculation rules (e.g., "sum of X", "total is Y")
  * Validation rules (e.g., "must be", "only if", "required")
  * Relationship rules (e.g., "one to many", "belongs to")
- Convert validation rules to SQL-like conditions when possible
- Keep calculation rules as natural language descriptions
- Extract entities/tables the rule applies to

Return ONLY valid JSON array matching this structure:
[
  {
    "id": "rule_id",
    "description": "Natural language rule description",
    "rule_type": "calculation|validation|relationship",
    "applies_to": ["table1", "table2"] or null,
    "condition": "SQL-like condition" or null,
    "formula": "calculation formula" or null
  }
]"""
        
        user_prompt = f"""Parse this natural language business rules text into structured JSON:

BUSINESS RULES:
{text}

Extract:
1. All individual rules
2. Rule types (calculation, validation, relationship)
3. Entities/tables each rule applies to
4. SQL-like conditions for validation rules
5. Formulas for calculation rules

Return ONLY the JSON array (no markdown, no code blocks, no explanations):"""
        
        try:
            response = self.call_llm(user_prompt, system_prompt)
            
            # Clean JSON response
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            rules_data = json.loads(response)
            
            # Ensure it's a list
            if not isinstance(rules_data, list):
                rules_data = [rules_data]
            
            # Add IDs if missing
            for i, rule in enumerate(rules_data):
                if "id" not in rule:
                    rule["id"] = f"rule_{i+1}"
            
            return rules_data
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse LLM response as JSON: {e}\nResponse: {response[:500]}")
        except Exception as e:
            raise Exception(f"Business rules parsing failed: {e}")
    
    def parse_multiple_tables(self, text: str) -> List[Dict[str, Any]]:
        """
        Parse multiple table descriptions from a single text block.
        
        Input can contain multiple table descriptions separated by blank lines or markers.
        """
        # Split by common separators
        parts = re.split(r'\n\n+|\n---+\n|Table\s*\d+:|##\s*Table', text, flags=re.IGNORECASE)
        
        tables = []
        for part in parts:
            part = part.strip()
            if not part or len(part) < 10:  # Skip very short parts
                continue
            
            # Check if it looks like a table description
            if any(keyword in part.lower() for keyword in ['table', 'column', 'description', 'primary key']):
                try:
                    table = self.parse_table_description(part)
                    tables.append(table)
                except Exception as e:
                    # Skip invalid parts but continue processing
                    print(f"Warning: Failed to parse table description: {e}")
                    continue
        
        return tables
    
    def parse_multiple_joins(self, text: str, existing_tables: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Parse multiple join conditions from a single text block.
        
        Input can contain multiple join descriptions separated by newlines or markers.
        """
        # Split by newlines and filter for join-like statements
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        joins = []
        current_join = []
        
        for line in lines:
            # Check if line contains join keywords
            if any(keyword in line.lower() for keyword in ['join', 'connects', 'links', 'based on', 'on ']):
                if current_join:
                    # Process accumulated join text
                    join_text = ' '.join(current_join)
                    try:
                        edge = self.parse_join_condition(join_text, existing_tables)
                        joins.append(edge)
                    except Exception as e:
                        print(f"Warning: Failed to parse join condition: {e}")
                    current_join = []
                current_join.append(line)
            elif current_join:
                current_join.append(line)
        
        # Process last join if any
        if current_join:
            join_text = ' '.join(current_join)
            try:
                edge = self.parse_join_condition(join_text, existing_tables)
                joins.append(edge)
            except Exception as e:
                print(f"Warning: Failed to parse join condition: {e}")
        
        return joins


def parse_table_description(text: str, system: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to parse a single table description."""
    parser = NaturalLanguageMetadataParser()
    return parser.parse_table_description(text, system)


def parse_join_condition(text: str, existing_tables: Optional[List[str]] = None) -> Dict[str, Any]:
    """Convenience function to parse a single join condition."""
    parser = NaturalLanguageMetadataParser()
    return parser.parse_join_condition(text, existing_tables)


def parse_business_rules(text: str) -> List[Dict[str, Any]]:
    """Convenience function to parse business rules."""
    parser = NaturalLanguageMetadataParser()
    return parser.parse_business_rules(text)

