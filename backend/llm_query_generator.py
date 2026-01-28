#!/usr/bin/env python3
"""
LLM-based Query Generator with Comprehensive Context

This module uses LLM to generate SQL queries by analyzing:
- All tables from metadata
- All metrics and dimensions from semantic registry
- Relationship information from hypergraph/lineage
- Business rules
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import requests

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from test_outstanding_daily_regeneration import load_metadata
from sql_builder import TableRelationshipResolver, IntentValidator, SQLBuilder, FixConfidence
from knowledge_base_client import get_knowledge_base_client

class LLMQueryGenerator:
    """LLM-based query generator with comprehensive context."""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None, kb_api_url: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")
        # Use model from environment variable if not provided, fallback to gpt-4
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4")
        # Use base URL from environment variable if set, otherwise default
        base_url_env = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        # Ensure base_url ends with /chat/completions if it's just the base
        if base_url_env.endswith("/v1"):
            self.base_url = f"{base_url_env}/chat/completions"
        elif base_url_env.endswith("/chat/completions"):
            self.base_url = base_url_env
        else:
            self.base_url = f"{base_url_env}/chat/completions"
        
        # Initialize KnowledgeBase client (optional, fails gracefully if server not running)
        try:
            self.kb_client = get_knowledge_base_client(kb_api_url)
            # Test connection
            health = self.kb_client.health_check()
            if health.get("status") == "healthy":
                print(f"✅ KnowledgeBase RAG enabled ({health.get('concepts_count', 0)} concepts)")
            else:
                print("⚠️  KnowledgeBase server not available, RAG disabled")
                self.kb_client = None
        except Exception as e:
            print(f"⚠️  KnowledgeBase client initialization failed: {e}, RAG disabled")
            self.kb_client = None
    
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
        
        # GPT-5.2 uses max_completion_tokens instead of max_tokens
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,  # Low temperature for deterministic output
        }
        
        # Use max_completion_tokens for GPT-5.2, max_tokens for other models
        if "gpt-5" in self.model.lower() or "gpt-4o" in self.model.lower():
            payload["max_completion_tokens"] = 3000
        else:
            payload["max_tokens"] = 3000
        
        try:
            response = requests.post(self.base_url, headers=headers, json=payload, timeout=120)
            response.raise_for_status()
            result = response.json()
            return result["choices"][0]["message"]["content"]
        except requests.exceptions.HTTPError as e:
            error_detail = ""
            try:
                error_detail = f" - {response.json()}"
            except:
                error_detail = f" - {response.text[:200]}"
            raise Exception(f"LLM API call failed: {str(e)}{error_detail}")
        except Exception as e:
            raise Exception(f"LLM API call failed: {str(e)}")
    
    def build_comprehensive_context(self, metadata: Dict[str, Any], query_text: Optional[str] = None) -> str:
        """
        Build comprehensive context from metadata.
        Uses node-level isolation - only includes relevant nodes for the query.
        
        Args:
            metadata: Metadata dictionary (can be isolated or full)
            query_text: Optional query text for node-level isolation
        """
        # Use node-level accessor if query_text provided
        if query_text:
            try:
                from backend.node_level_metadata_accessor import get_node_level_accessor
                accessor = get_node_level_accessor()
                # Build isolated metadata - only what's needed
                isolated_metadata = accessor.build_isolated_context(query_text)
                # Merge with provided metadata (isolated takes precedence)
                metadata = {**metadata, **isolated_metadata}
            except Exception as e:
                # Fallback to full metadata if isolation fails
                print(f"Node-level isolation failed, using full metadata: {e}")
        
        context_parts = []
        
        # 1. Tables metadata (now isolated to relevant tables only)
        context_parts.append("=" * 80)
        context_parts.append("RELEVANT TABLES (Node-Level Access)")
        context_parts.append("=" * 80)
        
        tables = metadata.get("tables", {}).get("tables", [])
        for table in tables:
            context_parts.append(f"\nTable: {table.get('name')}")
            context_parts.append(f"  System: {table.get('system')}")
            context_parts.append(f"  Entity: {table.get('entity')}")
            context_parts.append(f"  Primary Key: {', '.join(table.get('primary_key', []))}")
            if table.get('time_column'):
                context_parts.append(f"  Time Column: {table.get('time_column')}")
            context_parts.append(f"  Description: {table.get('description', 'N/A')}")
            context_parts.append("  Columns:")
            for col in table.get('columns', []):
                col_name = col.get('name') or col.get('column', '')
                col_type = col.get('data_type') or col.get('type', 'unknown')
                col_desc = col.get('description', '')
                context_parts.append(f"    - {col_name} ({col_type}): {col_desc}")
        
        # 2. Semantic Registry - Metrics (isolated to relevant metrics)
        registry = metadata.get("semantic_registry", {})
        context_parts.append("\n" + "=" * 80)
        context_parts.append(f"RELEVANT METRICS (Node-Level Access - {len(registry.get('metrics', []))} metrics)")
        context_parts.append("=" * 80)
        
        for metric in registry.get("metrics", []):
            context_parts.append(f"\nMetric: {metric.get('name')}")
            context_parts.append(f"  Description: {metric.get('description')}")
            context_parts.append(f"  Base Table: {metric.get('base_table')}")
            context_parts.append(f"  SQL Expression: {metric.get('sql_expression', 'N/A')}")
            context_parts.append(f"  Allowed Dimensions: {', '.join(metric.get('allowed_dimensions', []))}")
            if metric.get('required_filters'):
                context_parts.append(f"  Required Filters: {', '.join(metric.get('required_filters', []))}")
        
        # 3. Semantic Registry - Dimensions (isolated to relevant dimensions)
        context_parts.append("\n" + "=" * 80)
        context_parts.append(f"RELEVANT DIMENSIONS (Node-Level Access - {len(registry.get('dimensions', []))} dimensions)")
        context_parts.append("=" * 80)
        
        for dim in registry.get("dimensions", []):
            context_parts.append(f"\nDimension: {dim.get('name')}")
            context_parts.append(f"  Description: {dim.get('description')}")
            context_parts.append(f"  Base Table: {dim.get('base_table')}")
            context_parts.append(f"  Column: {dim.get('column')}")
            if dim.get('sql_expression'):
                context_parts.append(f"  SQL Expression: {dim.get('sql_expression')}")
            if dim.get('join_path'):
                context_parts.append("  Join Path:")
                for join in dim.get('join_path', []):
                    context_parts.append(f"    {join.get('from_table')} -> {join.get('to_table')} ON {join.get('on')}")
        
        # 4. Relationship information (isolated to relevant joins only)
        if "lineage" in metadata:
            lineage = metadata.get("lineage", {})
            edges = lineage.get("edges", [])
            context_parts.append("\n" + "=" * 80)
            context_parts.append(f"RELEVANT TABLE RELATIONSHIPS (Node-Level Access - {len(edges)} joins)")
            context_parts.append("=" * 80)
            if edges:
                for edge in edges:
                    context_parts.append(f"  {edge.get('from')} -> {edge.get('to')} ON {edge.get('on', 'N/A')}")
            else:
                context_parts.append("  No relevant joins found")
        
        # 5. Knowledge Base - Business Terms & Definitions (isolated to relevant terms)
        kb_terms = metadata.get("knowledge_base", {}).get("terms", {})
        if kb_terms:
            context_parts.append("\n" + "=" * 80)
            context_parts.append(f"RELEVANT BUSINESS TERMS (Node-Level Access - {len(kb_terms)} terms)")
            context_parts.append("=" * 80)
            
            for term, definition in kb_terms.items():
                        context_parts.append(f"\nTerm: {term}")
                        context_parts.append(f"  Definition: {definition.get('definition', 'N/A')}")
                        if definition.get('aliases'):
                            context_parts.append(f"  Aliases: {', '.join(definition.get('aliases', []))}")
                        if definition.get('related_tables'):
                            context_parts.append(f"  Related Tables: {', '.join(definition.get('related_tables', []))}")
                        if definition.get('business_meaning'):
                            context_parts.append(f"  Business Meaning: {definition.get('business_meaning')}")
        
        # 6. Business Rules (isolated to relevant rules)
        rules = metadata.get("rules", [])
        if rules:
            context_parts.append("\n" + "=" * 80)
            context_parts.append(f"RELEVANT BUSINESS RULES (Node-Level Access - {len(rules)} rules)")
            context_parts.append("=" * 80)
            
            for rule in rules:
                context_parts.append(f"\nRule: {rule.get('name', 'Unnamed')}")
                if rule.get('description'):
                    context_parts.append(f"  Description: {rule.get('description')}")
                if rule.get('sql_expression'):
                    context_parts.append(f"  SQL Expression: {rule.get('sql_expression')}")
                if rule.get('condition'):
                    context_parts.append(f"  Condition: {rule.get('condition')}")
        
        return "\n".join(context_parts)
    
    def _get_rag_context(self, query: str, top_k: int = 5) -> str:
        """
        Get RAG context from KnowledgeBase vector store.
        
        Args:
            query: User query
            top_k: Number of relevant concepts to retrieve
            
        Returns:
            Formatted RAG context string
        """
        if not self.kb_client:
            return ""
        
        try:
            return self.kb_client.get_rag_context(query, top_k)
        except Exception as e:
            # Fail silently if RAG is unavailable
            return ""
    
    def _build_knowledge_register_rules_context(self, metadata: Dict[str, Any]) -> str:
        """
        Build context string from knowledge register rules.
        
        Args:
            metadata: Metadata dictionary
        
        Returns:
            Formatted knowledge register rules context
        """
        try:
            from backend.knowledge_register_rules import get_knowledge_register_rules
            knowledge_rules = get_knowledge_register_rules()
        except Exception:
            return ""
        
        context_parts = []
        context_parts.append("KNOWLEDGE REGISTER RULES:")
        context_parts.append("=" * 50)
        
        # Get rules for common columns
        common_columns = ['write_off_flag', 'writeoff_flag', 'arc_flag', 'originator', 'settled_flag']
        
        for col in common_columns:
            rules = knowledge_rules.get_rules_for_column(col)
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
        
        # Add general rules
        general_rules = knowledge_rules.rules_cache.get('general', [])
        if general_rules:
            context_parts.append(f"\nGeneral Rules ({len(general_rules)} rules):")
            for rule in general_rules[:5]:  # Limit to first 5
                rule_id = rule.get('id', '')
                computation = rule.get('computation', {})
                filter_conditions = computation.get('filter_conditions', {})
                if filter_conditions:
                    context_parts.append(f"  - {rule_id}: {filter_conditions}")
        
        return "\n".join(context_parts)
    
    def generate_sql_intent(self, query: str, metadata: Dict[str, Any], 
                          conversational_context: Optional[Dict[str, Any]] = None) -> tuple[Dict[str, Any], List[str]]:
        """
        Use LLM to generate SQL intent with comprehensive context. 
        Supports conversational queries that build on previous queries.
        
        Args:
            query: User query (can be a modification like "add X" or "remove Y")
            metadata: Metadata dictionary
            conversational_context: Optional previous query context
        
        Returns:
            Tuple of (intent, reasoning_steps)
        """
        
        # Use hybrid knowledge retrieval (RAG + Graph + Rules)
        try:
            from backend.hybrid_knowledge_retriever import HybridKnowledgeRetriever
            hybrid_retriever = HybridKnowledgeRetriever()
            hybrid_context = hybrid_retriever.build_optimized_context(query, metadata, max_knowledge_items=30)
            
            # Build comprehensive context with node-level isolation
            structured_context = self.build_comprehensive_context(metadata, query)
            
            # Combine hybrid (semantic + structured) with comprehensive (isolated)
            context = f"{hybrid_context}\n\n{structured_context}"
        except Exception as e:
            # Fallback to original approach
            print(f"Hybrid retrieval failed, using fallback: {e}")
            
            # Get RAG context from KnowledgeBase vector store
            rag_context = self._get_rag_context(query, top_k=5)
            
            # Build comprehensive context with node-level isolation
            context = self.build_comprehensive_context(metadata, query)
            
            # Add knowledge register rules to context
            knowledge_rules_context = self._build_knowledge_register_rules_context(metadata)
            if knowledge_rules_context:
                context = f"{context}\n\n{knowledge_rules_context}"
            
            # Prepend RAG context if available
            if rag_context:
                context = f"{rag_context}\n\n{context}"
        
        reasoning_steps = []
        
        reasoning_steps.append("🔍 Analyzing query: " + query)
        
        # Count knowledge base terms
        kb_path = Path(__file__).parent.parent / "metadata" / "knowledge_base.json"
        kb_terms_count = 0
        if kb_path.exists():
            try:
                with open(kb_path, 'r', encoding='utf-8') as f:
                    kb = json.load(f)
                    kb_terms_count = len(kb.get("terms", {}))
            except:
                pass
        
        # Count RAG concepts if available
        rag_info = ""
        if self.kb_client:
            try:
                health = self.kb_client.health_check()
                if health.get("status") == "healthy":
                    rag_info = f", RAG: {health.get('concepts_count', 0)} concepts"
            except:
                pass
        
        # Count retrieved knowledge from hybrid retriever
        try:
            from backend.hybrid_knowledge_retriever import HybridKnowledgeRetriever
            hybrid_retriever = HybridKnowledgeRetriever()
            retrieved = hybrid_retriever.retrieve_for_query(query, metadata, max_results=30)
            hybrid_info = f", Hybrid Retrieval: {len(retrieved)} knowledge items"
        except Exception:
            hybrid_info = ""
        
        reasoning_steps.append(f"📊 Loaded context: {len(metadata.get('tables', {}).get('tables', []))} tables, {len(metadata.get('semantic_registry', {}).get('metrics', []))} metrics, {len(metadata.get('semantic_registry', {}).get('dimensions', []))} dimensions, {kb_terms_count} business terms, {len(metadata.get('rules', []))} business rules{rag_info}{hybrid_info}")
        
        # Build conversational context if available
        conversational_prompt = ""
        if conversational_context and conversational_context.get('current_intent'):
            prev_intent = conversational_context['current_intent']
            prev_sql = conversational_context.get('current_sql', '')
            conversational_prompt = f"""

PREVIOUS QUERY CONTEXT:
The user has a previous query that you should build upon:
- Previous Intent: {json.dumps(prev_intent, indent=2)}
- Previous SQL: {prev_sql}

CONVERSATIONAL MODIFICATIONS:
If the current query is a modification (e.g., "add X", "remove Y", "also show Z"), you should:
1. Start with the previous intent as a base
2. Apply the modification requested
3. Maintain all previous filters, joins, and columns unless explicitly removed
4. Add new columns/filters as requested
5. Preserve the query structure and logic from the previous query

Examples:
- "add writeoff flag as column" → Add write_off_flag to columns, apply knowledge rule filter
- "remove arc cases" → Add arc_flag filter to exclude arc cases
- "also show originator" → Add originator column with proper handling
"""
        
        system_prompt = """You are an expert SQL query generator with conversational capabilities. Your task is to analyze natural language queries and generate SQL queries using ALL available information from:
1. Table metadata (columns, types, primary keys, time columns)
2. Semantic registry (metrics, dimensions, their SQL expressions, join paths)
3. Relationship information (how tables connect)
4. Business terms & definitions (aliases, related tables, business meanings)
5. Business rules (constraints and validation rules)

CRITICAL INSTRUCTIONS:
- ALWAYS check ALL available tables, metrics, dimensions, business terms, and rules before generating SQL
- Use the EXACT table names and column names from metadata
- For metrics, use the provided SQL expressions from semantic registry
- For dimensions, follow the join paths specified in metadata
- Check business terms for aliases - if user mentions an alias, use the actual term/column name
- Apply business rules - ensure generated SQL complies with all business rules
- Distinguish between relational queries (individual records) and metric queries (aggregations)

QUERY TYPE DETECTION:
- If query asks for "total", "sum", "count", "average", "aggregate", or mentions a metric name → METRIC query
- If query asks for individual records, rows, or "show me all" without aggregation → RELATIONAL query
- Examples:
  * "Show me all loans" → RELATIONAL
  * "Show me the total principal outstanding" → METRIC
  * "Total principal outstanding grouped by order type" → METRIC

FOR METRIC QUERIES:
- MUST include the metric in the intent (find matching metric from semantic registry)
- MUST include all GROUP BY dimensions in the intent
- Use SUM() aggregation for "total" queries
- Metric SQL expression should be wrapped in aggregation if not already aggregated
- Dimensions come FIRST in SELECT, metric comes AFTER (for proper GROUP BY)

COMPUTED DIMENSIONS (CRITICAL):
- When user describes business logic in natural language, generate CASE statements automatically
- Examples:
  * "order type as Bank" → sql_expression: "'Bank'"
  * "region: OS if branch_code is 333, else NE" → sql_expression: "CASE WHEN branch_code = 333 THEN 'OS' ELSE 'NE' END"
  * "region: OS if branch_code is 333 and product is EDL, else NE" → sql_expression: "CASE WHEN branch_code = 333 AND LOWER(product_name) LIKE '%edl%' THEN 'OS' ELSE 'NE' END"
  * "product group: EDL if product contains 'edl', CC if 'Cash Credit', else Other" → sql_expression: "CASE WHEN LOWER(product_name) LIKE '%edl%' THEN 'EDL' WHEN product_name = 'Cash Credit' THEN 'CC' ELSE 'Other' END"
- For computed dimensions, include them in intent with:
  * "name": dimension name
  * "sql_expression": the CASE statement or expression
  * "is_computed": true
- Support nested CASE statements when user describes nested logic
- Use LOWER() for case-insensitive matching when user says "contains", "like", etc.
- Use IN clause when user lists multiple values
- Use LIKE with % when user says "contains" or "like"

FOR RELATIONAL QUERIES:
- Select individual columns mentioned in query
- Use appropriate JOINs to get required data
- Apply filters as WHERE conditions

JOIN INSTRUCTIONS:
- Use INNER JOIN when filtering by related table (e.g., written off loans need INNER JOIN writeoff_users)
- Use LEFT JOIN for optional relationships
- JOIN ON clauses must use table aliases (t1, t2, etc.), NOT full table names
- Example: "t1.order_id = t2.order_id" NOT "s3_tool_propagator.outstanding_daily.order_id = ..."

FILTER PARSING:
- "written off" → JOIN to writeoff_users table AND filter WHERE writeoff_users.order_id IS NOT NULL
- "DPD > 90" → WHERE outstanding_daily.dpd > 90
- Parse ALL filters mentioned in the query

IMPORTANT: You must provide a "reasoning" field in your JSON response that shows your chain of thought:
- Which tables you considered and why
- Which metrics/dimensions you evaluated
- Why you chose specific joins
- Why you applied certain filters
- Your decision-making process

KNOWLEDGE REGISTER RULES (CRITICAL):
- ALWAYS check knowledge register rules for each column/node mentioned
- Apply filter rules automatically (e.g., write_off_flag should be = 'N', not != 'Y')
- Apply exclusion rules (e.g., arc_flag for khatabook: IS NULL OR = 'N' OR = 'NULL')
- Use LOWER(TRIM()) for originator columns
- These rules are part of the business knowledge and MUST be applied

Return JSON with both "intent" and "reasoning" fields."""
        
        user_prompt = f"""{conversational_prompt}

Analyze this query step-by-step and generate SQL intent JSON with reasoning:

QUERY: "{query}"

COMPREHENSIVE CONTEXT:
{context}

Generate a JSON object with this structure:
{{
  "reasoning": {{
    "step1_table_analysis": "Which tables did I consider? Why did I choose the base table?",
    "step2_metric_analysis": "Did I check metrics? Which ones? Why did I choose/not choose a metric?",
    "step3_dimension_analysis": "Which dimensions did I evaluate? Why are they needed?",
    "step4_join_analysis": "Which joins are needed? Why? What relationships did I identify?",
    "step5_filter_analysis": "Which filters did I parse from the query? Why are they needed?",
    "step6_query_type": "Is this relational or metric? Why?",
    "step7_final_decisions": "Summary of all decisions made"
  }},
  "intent": {{
    "query_type": "relational" | "metric",
    "base_table": "exact_table_name_from_metadata",
    "metric": {{"name": "metric_name", "sql_expression": "..."}} | null,
    "columns": ["column1", "column2", ...],
    "joins": [
      {{
        "table": "table_name",
        "type": "INNER" | "LEFT",
        "on": "left_table.column = right_table.column",
        "reason": "why this join is needed"
      }}
    ],
    "filters": [
      {{
        "column": "column_name",
        "table": "table_name",
        "operator": "=" | ">" | "<" | ">=" | "<=" | "IS NULL" | "IS NOT NULL",
        "value": "value_or_null",
        "reason": "why this filter is needed"
      }}
    ],
    "group_by": ["dimension1", "dimension2"] | null,
    "order_by": [{{"column": "col", "direction": "ASC" | "DESC"}}] | null
  }}
}}

COMPUTED DIMENSIONS (when user describes business logic):
- If user describes how to compute a dimension (e.g., "region: OS if branch_code is 333, else NE"), 
  generate a computed_dimension with sql_expression containing the CASE statement
- Examples of user descriptions to detect:
  * "order type as Bank" → computed_dimension: {{"name": "order_type", "sql_expression": "'Bank'", "is_computed": true}}
  * "region: OS if branch_code is 333, else NE" → computed_dimension: {{"name": "region", "sql_expression": "CASE WHEN branch_code = 333 THEN 'OS' ELSE 'NE' END", "is_computed": true}}
  * "region: OS if branch_code is 333 and product is EDL, else NE" → computed_dimension: {{"name": "region", "sql_expression": "CASE WHEN branch_code = 333 AND LOWER(product_name) LIKE '%edl%' THEN 'OS' ELSE 'NE' END", "is_computed": true}}
  * "product group: EDL if product contains 'edl', CC if 'Cash Credit', else Other" → computed_dimension: {{"name": "product_group", "sql_expression": "CASE WHEN LOWER(product_name) LIKE '%edl%' THEN 'EDL' WHEN product_name = 'Cash Credit' THEN 'CC' ELSE 'Other' END", "is_computed": true}}
- Include computed_dimensions in intent when user describes business logic
- Use LOWER() for case-insensitive matching
- Use LIKE with % for "contains" patterns
- Use IN for multiple values
- Support nested CASE for complex logic

IMPORTANT:
- Show your chain of thought in the "reasoning" field
- Check ALL tables to find the best match for the query
- Check ALL metrics to see if query matches a metric definition
- Check ALL dimensions for grouping/filtering needs
- Include ALL joins needed based on relationships
- Parse ALL filters from the query text
- Generate computed_dimensions when user describes business logic
- Use exact names from metadata
- Explain your reasoning for each decision

Return ONLY the JSON object:"""
        
        try:
            reasoning_steps.append("🤖 Calling LLM to analyze query with comprehensive context...")
            response = self.call_llm(user_prompt, system_prompt)
            reasoning_steps.append("✅ LLM response received, parsing...")
            
            # Clean JSON response
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            full_response = json.loads(response)
            
            # Extract reasoning and intent
            reasoning_data = full_response.get("reasoning", {})
            intent = full_response.get("intent", full_response)  # Fallback if structure is different
            
            # Fix: Extract computed dimensions from columns if LLM put them there
            # The LLM sometimes puts computed dimensions in columns instead of computed_dimensions
            computed_dims = intent.get('computed_dimensions', [])
            computed_dim_map = {dim.get('name'): dim for dim in computed_dims}
            
            # Check if columns contain computed dimension dicts
            columns = intent.get('columns', [])
            if columns:
                for col in columns:
                    if isinstance(col, dict) and col.get('is_computed'):
                        # Found a computed dimension in columns - extract it
                        dim_name = col.get('name', '')
                        if dim_name and dim_name not in computed_dim_map:
                            # Add to computed_dimensions if not already there
                            computed_dims.append({
                                'name': dim_name,
                                'sql_expression': col.get('sql_expression', ''),
                                'is_computed': True
                            })
                            computed_dim_map[dim_name] = computed_dims[-1]
                            reasoning_steps.append(f"   🔧 Extracted computed dimension '{dim_name}' from columns field")
                
                # Update intent with extracted computed dimensions
                if computed_dims:
                    intent['computed_dimensions'] = computed_dims
            
            # Convert reasoning to list of steps
            if reasoning_data:
                reasoning_steps.append("\n📝 LLM Reasoning Chain:")
                for step_key, step_value in reasoning_data.items():
                    step_name = step_key.replace("step", "").replace("_", " ").title()
                    reasoning_steps.append(f"   {step_name}: {step_value}")
            
            reasoning_steps.append(f"\n✅ Intent resolved: {intent.get('query_type', 'unknown')} query on {intent.get('base_table', 'unknown')}")
            
            return intent, reasoning_steps
        except json.JSONDecodeError as e:
            reasoning_steps.append(f"❌ Failed to parse LLM response: {e}")
            raise Exception(f"Failed to parse LLM response as JSON: {e}\nResponse: {response[:500]}")
        except Exception as e:
            reasoning_steps.append(f"❌ LLM generation failed: {e}")
            raise Exception(f"LLM query generation failed: {e}")
    
    def intent_to_sql(self, intent: Dict[str, Any], metadata: Dict[str, Any], query_text: Optional[str] = None) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Convert SQL intent to actual SQL query using robust SQL builder.
        Uses node-level isolation - only loads relevant tables/joins.
        
        Args:
            intent: SQL intent dictionary
            metadata: Metadata dictionary
            query_text: Optional query text for node-level isolation
        
        Returns:
            (sql_query, explain_plan, warnings)
        """
        # Get query_text from intent if not provided
        if not query_text:
            query_text = intent.get('_query_text') or intent.get('query_text', '')
        
        # Initialize resolver and validator with node-level isolation
        # Enable learning by default - will ask user when join paths not found
        resolver = TableRelationshipResolver(metadata, enable_learning=True, query_text=query_text)
        validator = IntentValidator(resolver)
        
        # Initialize warnings list
        warnings = []
        
        # Try to fix common issues first (before validation)
        try:
            fixed_intent, fix_confidence, fix_reasons = validator.fix_intent(intent)
        except Exception as e:
            # If fix_intent fails, log and continue with original intent
            import traceback
            traceback.print_exc()
            warnings.append(f"fix_intent failed: {str(e)}")
            fixed_intent = intent
            fix_confidence = FixConfidence.UNSAFE
            fix_reasons = []
        
        # Apply fixes based on confidence
        if fix_confidence == FixConfidence.SAFE:
            intent = fixed_intent
            warnings.extend([f"Auto-fixed: {r}" for r in fix_reasons])
        elif fix_confidence == FixConfidence.AMBIGUOUS:
            # For ambiguous fixes, still try but warn
            intent = fixed_intent
            warnings.append(f"AMBIGUOUS FIX APPLIED: {', '.join(fix_reasons)}")
            warnings.append("Please review the generated SQL carefully")
        else:
            # UNSAFE - don't apply, but still try to validate
            warnings.append(f"Cannot auto-fix: {', '.join(fix_reasons)}")
        
        # Now validate the (possibly fixed) intent
        try:
            is_valid, errors, validation_warnings = validator.validate(intent)
            warnings.extend(validation_warnings)
        except Exception as e:
            # If validation fails, log and try to continue
            import traceback
            traceback.print_exc()
            errors = [f"Validation error: {str(e)}"]
            is_valid = False
            warnings.append(f"Validation exception: {str(e)}")
        
        if not is_valid:
            # If still invalid after fixes, raise error with details
            error_msg = f"Invalid intent: {', '.join(errors)}"
            if warnings:
                error_msg += f"\nWarnings: {', '.join(warnings)}"
            raise ValueError(error_msg)
        
        # Build SQL using robust builder
        builder = SQLBuilder(resolver)
        sql, explain_plan = builder.build(intent, include_explain=True)
        
        warnings_str = "\n".join([f"⚠️  {w}" for w in warnings]) if warnings else None
        
        return sql, explain_plan, warnings_str

def generate_sql_with_llm(query: str, use_llm: bool = True) -> dict:
    """Generate SQL using LLM with comprehensive context."""
    try:
        metadata = load_metadata()
        
        if use_llm:
            generator = LLMQueryGenerator()
            intent, reasoning_steps = generator.generate_sql_intent(query, metadata)
            # Pass query text for node-level isolation
            sql, explain_plan, warnings = generator.intent_to_sql(intent, metadata, query_text=query)
            
            reasoning_steps.append(f"\n🔧 Generated SQL:\n{sql}")
            
            if explain_plan:
                reasoning_steps.append(f"\n📋 Query Explain Plan:\n{explain_plan}")
            
            if warnings:
                reasoning_steps.append(f"\n⚠️  Warnings:\n{warnings}")
            
            result = {
                "success": True,
                "sql": sql,
                "intent": intent,
                "reasoning_steps": reasoning_steps,
                "method": "llm_with_full_context"
            }
            
            if explain_plan:
                result["explain_plan"] = explain_plan
            
            if warnings:
                result["warnings"] = warnings
            
            return result
        else:
            # Fallback to rule-based
            from test_outstanding_daily_regeneration import (
                classify_query_intent, find_metric_by_query, find_dimensions_by_query,
                identify_required_joins, identify_required_filters, generate_sql_from_metadata
            )
            
            registry = metadata["semantic_registry"]
            tables = metadata["tables"]
            intent = classify_query_intent(query)
            metric = find_metric_by_query(registry, query)
            dimensions = find_dimensions_by_query(registry, query, metric, tables)
            filters = identify_required_filters(query, metric, dimensions, registry)
            joins = identify_required_joins(query, metric, dimensions, filters, registry, tables)
            sql = generate_sql_from_metadata(query, metric, dimensions, joins, filters, registry, tables)
            
            return {
                "success": True,
                "sql": sql,
                "method": "rule_based"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "method": "llm" if use_llm else "rule_based"
        }

