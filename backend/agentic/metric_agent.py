#!/usr/bin/env python3
"""
Metric Agent - LLM + Registry, HIGH Authority

This is the most important agent.
If metric resolution fails, the pipeline MUST STOP.
"""

from typing import Dict, Any, List, Optional
import json


class MetricAgent:
    """
    Metric Resolution Agent.
    
    Purpose: Resolve metrics from user query using Metric Registry.
    Authority: HIGH - pipeline stops if unresolved.
    
    Golden Invariant: If Metric Agent fails, system must refuse to answer.
    """
    
    def __init__(self, llm_client, metric_registry):
        """
        Initialize Metric Agent.
        
        Args:
            llm_client: LLM client
            metric_registry: MetricRegistry instance
        """
        self.llm = llm_client
        self.registry = metric_registry
    
    def resolve(self, intent_output: Dict[str, Any], query: str) -> Dict[str, Any]:
        """
        Resolve metrics from intent and query.
        
        Args:
            intent_output: Output from Intent Agent
            query: Original user query
        
        Returns:
            {
                "resolved_metrics": [
                    {
                        "name": "TOS",
                        "canonical_name": "current_pos",
                        "sql_template": "...",
                        "required_columns": ["ledger_balance"],
                        "default_table": "kb_adh433"
                    }
                ],
                "status": "RESOLVED"
            }
        
        Raises:
            ValueError: If no metric matches (UNRESOLVED status)
        """
        requested_metrics = intent_output.get('requested_metrics', [])
        
        if not requested_metrics:
            # No metrics requested - this might be a record query
            if intent_output.get('query_type') == 'metric':
                raise ValueError(
                    "Metric query type detected but no metrics requested. "
                    "Cannot proceed without metric resolution."
                )
            return {
                "resolved_metrics": [],
                "status": "NOT_APPLICABLE"  # Record queries don't need metrics
            }
        
        # Use LLM to disambiguate metric names
        resolved_metrics = []
        
        # Extract product hint from query (e.g., "khatabook")
        product_hint = None
        query_lower = query.lower()
        # Check for khatabook first (most specific)
        if 'khatabook' in query_lower or 'kb_' in query_lower:
            product_hint = 'khatabook'
        elif 'credit card' in query_lower or 'credit_card' in query_lower or 'cc' in query_lower:
            product_hint = 'credit_card'
        elif 'digital' in query_lower:
            product_hint = 'digital'
        elif 'bank' in query_lower:
            product_hint = 'bank'
        elif 'da' in query_lower:
            product_hint = 'da'
        
        for metric_name in requested_metrics:
            # First, try direct registry lookup with product hint
            metric_def = self.registry.resolve(metric_name, product_hint)
            
            if metric_def:
                resolved_metrics.append({
                    "name": metric_name,
                    "canonical_name": metric_def['canonical_name'],
                    "sql_template": metric_def['sql'],
                    "required_columns": metric_def['required_columns'],
                    "default_table": metric_def['default_table']
                })
                continue
            
            # If not found, use LLM to search registry
            metric_def = self._llm_disambiguate(metric_name, query, product_hint)
            
            if metric_def:
                resolved_metrics.append({
                    "name": metric_name,
                    "canonical_name": metric_def['canonical_name'],
                    "sql_template": metric_def['sql'],
                    "required_columns": metric_def['required_columns'],
                    "default_table": metric_def['default_table']
                })
            else:
                # CRITICAL: Metric unresolved - STOP THE PIPELINE
                raise ValueError(
                    f"Metric '{metric_name}' could not be resolved. "
                    f"Available metrics: {', '.join(self.registry.list_all())}. "
                    "Cannot generate SQL without a valid metric."
                )
        
        if not resolved_metrics:
            raise ValueError(
                "No metrics could be resolved. "
                f"Available metrics: {', '.join(self.registry.list_all())}. "
                "Cannot proceed without metric resolution."
            )
        
        return {
            "resolved_metrics": resolved_metrics,
            "status": "RESOLVED"
        }
    
    def _llm_disambiguate(self, metric_name: str, query: str, product_hint: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Use LLM to disambiguate metric name against registry.
        
        Returns:
            Metric definition if found, None otherwise
        """
        available_metrics = self.registry.list_all()
        
        if not available_metrics:
            return None
        
        system_prompt = """You are a Metric Resolution Agent.
You must ONLY choose from registered metrics.
If no metric matches, return "UNRESOLVED".

Return JSON only:
{
  "matched_metric": "canonical_name" | "UNRESOLVED",
  "confidence": 0.0
}"""
        
        user_prompt = f"""User requested metric: "{metric_name}"
Query context: "{query}"

Available metrics in registry:
{json.dumps(available_metrics, indent=2)}

Match the requested metric to a registered metric.
Consider aliases and descriptions.
If no match, return "UNRESOLVED".

Return JSON:"""
        
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
            
            matched = result.get('matched_metric')
            confidence = result.get('confidence', 0.0)
            
            if matched == "UNRESOLVED" or confidence < 0.7:
                return None
            
            # Get metric from registry with product hint
            return self.registry.resolve(matched, product_hint)
            
        except Exception:
            return None

