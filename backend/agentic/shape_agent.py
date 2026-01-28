#!/usr/bin/env python3
"""
Shape Agent - LLM, lowest authority

Decides result presentation (dimensions, constants).
"""

from typing import Dict, Any, List, Optional
import json


class ShapeAgent:
    """
    Result Shape Agent.
    
    Purpose: Decide result presentation (dimensions, constants).
    Authority: LOWEST - cannot remove or alter metrics.
    """
    
    def __init__(self, llm_client):
        """
        Initialize Shape Agent.
        
        Args:
            llm_client: LLM client
        """
        self.llm = llm_client
    
    def generate(self, intent_output: Dict[str, Any],
                 metric_output: Dict[str, Any],
                 query: str) -> Dict[str, Any]:
        """
        Generate result shape (dimensions, constants).
        
        Args:
            intent_output: Output from Intent Agent
            metric_output: Output from Metric Agent
            query: Original user query
        
        Returns:
            {
                "dimensions": [
                    {"name": "region", "sql": "'OS'"},
                    {"name": "product_group", "sql": "'Credit Card'"}
                ]
            }
        """
        requested_dimensions = intent_output.get('requested_dimensions', [])
        
        # Use LLM to generate dimensions
        system_prompt = """You are a Shape Agent.
Your job is to decide result presentation (dimensions, constants).

CRITICAL RULES:
- You CANNOT remove or alter metrics
- You can only add dimensions/constants for presentation
- Return JSON only
"""
        
        user_prompt = f"""Query: "{query}"
Requested dimensions: {json.dumps(requested_dimensions, indent=2)}

Generate dimensions as JSON:
{{
  "dimensions": [
    {{"name": "region", "sql": "'OS'"}},
    {{"name": "product_group", "sql": "'Credit Card'"}}
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
            
            return {
                "dimensions": result.get('dimensions', [])
            }
            
        except Exception:
            # Fallback to empty dimensions
            return {
                "dimensions": []
            }

