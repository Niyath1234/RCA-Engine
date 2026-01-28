#!/usr/bin/env python3
"""
Table Agent - LLM constrained by schema

Confirms or overrides the base table based on column availability.
"""

from typing import Dict, Any, List, Optional
import json


class TableAgent:
    """
    Table Resolution Agent.
    
    Purpose: Confirm or override base table based on column availability.
    Authority: MEDIUM - constrained by schema metadata.
    """
    
    def __init__(self, llm_client, metadata: Dict[str, Any]):
        """
        Initialize Table Agent.
        
        Args:
            llm_client: LLM client
            metadata: Metadata dictionary with table schemas
        """
        self.llm = llm_client
        self.metadata = metadata
        self.tables = metadata.get('tables', {}).get('tables', [])
    
    def resolve(self, metric_output: Dict[str, Any], query: str) -> Dict[str, Any]:
        """
        Resolve base table from metric requirements.
        
        Args:
            metric_output: Output from Metric Agent
            query: Original user query
        
        Returns:
            {
                "base_table": "kb_adh433",
                "justification": "Contains ledger_balance and loan-level grain"
            }
        
        Raises:
            ValueError: If table does not contain all required columns
        """
        resolved_metrics = metric_output.get('resolved_metrics', [])
        
        if not resolved_metrics:
            # No metrics - might be record query
            return {
                "base_table": self._find_table_from_query(query),
                "justification": "Record query - no metric requirements"
            }
        
        # Collect all required columns from metrics
        required_columns = set()
        default_tables = []
        
        for metric in resolved_metrics:
            required_columns.update(metric.get('required_columns', []))
            default_table = metric.get('default_table')
            if default_table:
                default_tables.append(default_table)
        
        # Try default table first
        if default_tables:
            default_table = default_tables[0]
            table_schema = self._get_table_schema(default_table)
            
            if table_schema and self._has_all_columns(table_schema, required_columns):
                return {
                    "base_table": default_table,
                    "justification": f"Default table from metric contains all required columns: {', '.join(required_columns)}"
                }
        
        # Search for table with all required columns
        candidate_table = self._find_table_with_columns(required_columns, query)
        
        if not candidate_table:
            raise ValueError(
                f"No table found containing all required columns: {', '.join(required_columns)}. "
                "Cannot proceed without valid table."
            )
        
        return {
            "base_table": candidate_table,
            "justification": f"Table contains all required columns: {', '.join(required_columns)}"
        }
    
    def _get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get table schema by name."""
        for table in self.tables:
            if table.get('name') == table_name:
                return table
        return None
    
    def _has_all_columns(self, table_schema: Dict[str, Any], required_columns: set) -> bool:
        """Check if table has all required columns."""
        table_columns = {col.get('name') or col.get('column', '') 
                        for col in table_schema.get('columns', [])}
        
        # Check if all required columns exist (case-insensitive)
        table_columns_lower = {col.lower() for col in table_columns}
        required_lower = {col.lower() for col in required_columns}
        
        return required_lower.issubset(table_columns_lower)
    
    def _find_table_with_columns(self, required_columns: set, query: str) -> Optional[str]:
        """Find table that contains all required columns."""
        for table in self.tables:
            if self._has_all_columns(table, required_columns):
                return table.get('name')
        return None
    
    def _find_table_from_query(self, query: str) -> str:
        """Find table from query context (for record queries)."""
        # Use LLM to identify table from query
        system_prompt = """You are a Table Resolution Agent.
Choose a table from the available tables based on the query.
Return JSON only: {"table_name": "name"}"""
        
        available_tables = [t.get('name') for t in self.tables]
        
        user_prompt = f"""Query: "{query}"

Available tables:
{json.dumps(available_tables, indent=2)}

Return JSON: {{"table_name": "chosen_table"}}"""
        
        try:
            response = self.llm.call_llm(user_prompt, system_prompt)
            
            # Clean JSON response
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            result = json.loads(response)
            return result.get('table_name', available_tables[0] if available_tables else '')
            
        except Exception:
            return self.tables[0].get('name', '') if self.tables else ''

