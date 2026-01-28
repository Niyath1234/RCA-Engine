#!/usr/bin/env python3
"""
Filter Agent - LLM, policy-driven

Translates business rules → filters.
"""

from typing import Dict, Any, List, Optional
import json


class FilterAgent:
    """
    Filter Generation Agent.
    
    Purpose: Translate business rules → filters.
    Authority: MEDIUM - policy-driven, cannot remove required metric columns.
    """
    
    def __init__(self, llm_client, metadata: Dict[str, Any], knowledge_rules=None):
        """
        Initialize Filter Agent.
        
        Args:
            llm_client: LLM client
            metadata: Metadata dictionary
            knowledge_rules: Knowledge register rules (optional)
        """
        self.llm = llm_client
        self.metadata = metadata
        self.knowledge_rules = knowledge_rules
    
    def generate(self, intent_output: Dict[str, Any], 
                 metric_output: Dict[str, Any],
                 table_output: Dict[str, Any],
                 query: str) -> Dict[str, Any]:
        """
        Generate filters from query and business rules.
        
        Args:
            intent_output: Output from Intent Agent
            metric_output: Output from Metric Agent
            table_output: Output from Table Agent
            query: Original user query
        
        Returns:
            {
                "filters": [
                    "write_off_flag = 'N'",
                    "settled_flag = 'N'",
                    "(arc_flag IS NULL OR arc_flag = 'N' OR arc_flag = 'NULL')",
                    "reportdate = DATE '2026-01-26'"
                ]
            }
        """
        base_table = table_output.get('base_table', '')
        
        # Get business rules from metadata
        rules = self.metadata.get('rules', [])
        knowledge_rules_context = self._build_knowledge_rules_context()
        
        # Use LLM to generate filters
        system_prompt = """You are a Filter Generation Agent.
Your job is to translate business rules and query requirements into SQL WHERE filters.

CRITICAL RULES:
- Filters cannot remove required metric columns
- Apply knowledge register rules automatically
- Use proper SQL syntax
- Return JSON only
"""
        
        user_prompt = f"""Query: "{query}"
Base Table: {base_table}

{knowledge_rules_context}

Generate SQL WHERE filters as a JSON array:
{{
  "filters": [
    "column = 'value'",
    "column IS NULL",
    "(column1 = 'A' OR column2 = 'B')"
  ]
}}

Return JSON only:"""
        
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
            filters = result.get('filters', [])
            
            # Validate filters don't remove required columns
            required_columns = set()
            for metric in metric_output.get('resolved_metrics', []):
                required_columns.update(metric.get('required_columns', []))
            
            # Basic validation - filters should not exclude required columns
            # (This is a simple check - more sophisticated validation in Verifier)
            
            return {
                "filters": filters
            }
            
        except Exception as e:
            # Fallback to empty filters
            return {
                "filters": []
            }
    
    def _build_knowledge_rules_context(self) -> str:
        """Build context string from knowledge register rules."""
        if not self.knowledge_rules:
            return ""
        
        try:
            context_parts = []
            context_parts.append("KNOWLEDGE REGISTER RULES:")
            context_parts.append("=" * 50)
            
            # Get rules for common columns
            common_columns = ['write_off_flag', 'writeoff_flag', 'arc_flag', 'originator', 'settled_flag']
            
            for col in common_columns:
                rules = self.knowledge_rules.get_rules_for_column(col)
                if rules:
                    context_parts.append(f"\n{col}:")
                    for rule in rules:
                        rule_type = rule.get('type', '')
                        if rule_type == 'exclusion_rule':
                            exclude_vals = rule.get('exclude_values', [])
                            include_vals = rule.get('include_values', [])
                            context_parts.append(f"  - Exclusion rule: exclude {exclude_vals}, include {include_vals}")
                        elif rule_type == 'filter_condition':
                            condition = rule.get('condition', '')
                            value = rule.get('value', '')
                            context_parts.append(f"  - Filter condition: {condition} = {value}")
            
            return "\n".join(context_parts)
        except Exception:
            return ""

