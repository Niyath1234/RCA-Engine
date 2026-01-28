#!/usr/bin/env python3
"""
Intent Agent - LLM, Low Authority

Classifies what kind of question this is.
Cannot invent tables or metrics.
"""

from typing import Dict, Any, Optional
import json


class IntentAgent:
    """
    Intent Classification Agent.
    
    Purpose: Classify what kind of question this is.
    Authority: LOW - cannot invent tables or metrics.
    """
    
    def __init__(self, llm_client):
        """
        Initialize Intent Agent.
        
        Args:
            llm_client: LLM client (from LLMQueryGenerator)
        """
        self.llm = llm_client
    
    def classify(self, query: str) -> Dict[str, Any]:
        """
        Classify query intent.
        
        Returns:
            {
                "query_type": "metric | record | mixed",
                "requested_metrics": ["TOS"],
                "requested_dimensions": [],
                "confidence": 0.0
            }
        
        Raises:
            ValueError: If confidence < 0.7
        """
        system_prompt = """You are an Intent Classification Agent.
Your job is to classify the query, not to generate SQL.
Return JSON only.

You must identify:
1. Query type: "metric" (aggregation), "record" (individual rows), or "mixed"
2. Requested metrics: List of metric names mentioned (e.g., ["TOS", "POS"])
3. Requested dimensions: List of dimension names mentioned (e.g., ["region", "product"])
4. Confidence: Your confidence level (0.0 to 1.0)

CRITICAL RULES:
- You CANNOT invent table names or metric definitions
- You can only identify what the user is asking for
- If unsure, set confidence < 0.7
- Return JSON only, no explanations
"""
        
        user_prompt = f"""Classify this query:

"{query}"

Return JSON:
{{
  "query_type": "metric | record | mixed",
  "requested_metrics": ["list", "of", "metric", "names"],
  "requested_dimensions": ["list", "of", "dimension", "names"],
  "confidence": 0.0
}}"""
        
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
            
            # Validate confidence
            confidence = result.get('confidence', 0.0)
            if confidence < 0.7:
                raise ValueError(
                    f"Intent classification confidence too low ({confidence}). "
                    "Please clarify your query or retry."
                )
            
            return result
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse intent classification: {e}")
        except Exception as e:
            raise ValueError(f"Intent classification failed: {e}")

