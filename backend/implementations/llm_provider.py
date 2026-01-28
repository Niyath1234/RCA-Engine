"""
LLM Provider Implementation

OpenAI provider implementation.
"""

import time
from typing import Dict, Any, Optional
from backend.interfaces import LLMProvider


class OpenAIProvider(LLMProvider):
    """OpenAI LLM provider implementation."""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4",
                 temperature: float = 0.0, max_tokens: int = 3000, timeout: int = 120):
        """
        Initialize OpenAI provider.
        
        Args:
            api_key: OpenAI API key
            model: Model name (default: gpt-4)
            temperature: Temperature for generation (default: 0.0 for deterministic)
            max_tokens: Maximum tokens to generate
            timeout: Request timeout in seconds
        """
        try:
            import openai
        except ImportError:
            raise ImportError("openai package is required. Install with: pip install openai")
        
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.timeout = timeout
        
        # Initialize OpenAI client
        self.client = openai.OpenAI(api_key=api_key) if api_key else None
    
    def generate_sql(self, prompt: str, context: Dict[str, Any]) -> str:
        """
        Generate SQL from prompt.
        
        Args:
            prompt: User query or prompt
            context: Context dictionary (schema, metadata, etc.)
        
        Returns:
            Generated SQL query string
        """
        if not self.client:
            raise ValueError("OpenAI API key not provided")
        
        # Build system prompt
        schema = context.get('schema', {})
        metadata = context.get('metadata', {})
        
        system_prompt = self._build_system_prompt(schema, metadata)
        
        # Build user prompt
        user_prompt = self._build_user_prompt(prompt, context)
        
        # Call OpenAI API
        start_time = time.time()
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                timeout=self.timeout,
            )
            
            duration_ms = (time.time() - start_time) * 1000
            
            sql = response.choices[0].message.content.strip()
            
            # Extract SQL from markdown code blocks if present
            if '```' in sql:
                lines = sql.split('\n')
                sql_lines = []
                in_code_block = False
                for line in lines:
                    if line.strip().startswith('```'):
                        in_code_block = not in_code_block
                        continue
                    if in_code_block:
                        sql_lines.append(line)
                sql = '\n'.join(sql_lines).strip()
            
            return sql
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            raise RuntimeError(f"OpenAI API call failed: {str(e)}") from e
    
    def explain_result(self, sql: str, result: Dict[str, Any]) -> str:
        """
        Explain query result in natural language.
        
        Args:
            sql: SQL query that was executed
            result: Query result dictionary
        
        Returns:
            Natural language explanation
        """
        if not self.client:
            raise ValueError("OpenAI API key not provided")
        
        # Build explanation prompt
        rows_returned = result.get('rows_returned', 0)
        columns = result.get('columns', [])
        sample_data = result.get('data', [])[:5]  # First 5 rows
        
        prompt = f"""Explain the following SQL query result in natural language.

SQL Query:
{sql}

Result Summary:
- Rows returned: {rows_returned}
- Columns: {', '.join(columns)}
- Sample data: {sample_data}

Provide a clear, concise explanation of what this query returned."""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a data analyst explaining query results."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,  # Slightly higher for more natural explanations
                max_tokens=500,
                timeout=self.timeout,
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            return f"Explanation unavailable: {str(e)}"
    
    def extract_intent(self, user_query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract intent from user query.
        
        Args:
            user_query: User's natural language query
            context: Additional context
        
        Returns:
            Intent dictionary
        """
        if not self.client:
            raise ValueError("OpenAI API key not provided")
        
        prompt = f"""Extract the intent from the following user query.

User Query: {user_query}

Return a JSON object with:
- metric: The metric being queried (e.g., "revenue", "users", "orders")
- time_range: Time range if specified (e.g., "last 7 days", "2024-01-01 to 2024-01-31")
- aggregation: Aggregation type if specified (e.g., "sum", "average", "count")
- filters: List of filters if specified
- dimensions: List of dimensions to group by if specified

Return only valid JSON, no additional text."""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an intent extraction system. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=500,
                timeout=self.timeout,
                response_format={"type": "json_object"},
            )
            
            import json
            intent = json.loads(response.choices[0].message.content)
            return intent
            
        except Exception as e:
            # Fallback to simple extraction
            return {
                'query': user_query,
                'metric': None,
                'time_range': None,
                'aggregation': None,
            }
    
    def get_model_name(self) -> str:
        """Get model name."""
        return self.model
    
    def get_temperature(self) -> float:
        """Get current temperature setting."""
        return self.temperature
    
    def set_temperature(self, temperature: float):
        """Set temperature for deterministic generation."""
        if temperature < 0 or temperature > 2:
            raise ValueError("Temperature must be between 0 and 2")
        self.temperature = temperature
    
    def _build_system_prompt(self, schema: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        """Build system prompt from schema and metadata."""
        prompt = """You are a SQL query generator. Generate SQL queries based on user requests.

Rules:
1. Generate only valid SQL queries
2. Always include LIMIT clause
3. Use explicit column names, never SELECT *
4. Use proper JOIN syntax
5. Return only the SQL query, no explanations

"""
        
        if schema:
            prompt += f"Schema:\n{self._format_schema(schema)}\n\n"
        
        if metadata:
            prompt += f"Metadata:\n{self._format_metadata(metadata)}\n\n"
        
        return prompt
    
    def _build_user_prompt(self, prompt: str, context: Dict[str, Any]) -> str:
        """Build user prompt."""
        user_prompt = f"User Query: {prompt}\n\n"
        
        if context.get('examples'):
            user_prompt += "Examples:\n"
            for example in context['examples']:
                user_prompt += f"- {example}\n"
            user_prompt += "\n"
        
        user_prompt += "Generate the SQL query:"
        
        return user_prompt
    
    def _format_schema(self, schema: Dict[str, Any]) -> str:
        """Format schema for prompt."""
        if isinstance(schema, dict):
            if 'tables' in schema:
                formatted = []
                for table in schema['tables']:
                    if isinstance(table, dict):
                        table_name = table.get('name', 'unknown')
                        columns = table.get('columns', [])
                        formatted.append(f"Table: {table_name}")
                        for col in columns:
                            col_name = col.get('name', 'unknown')
                            col_type = col.get('type', 'unknown')
                            formatted.append(f"  - {col_name} ({col_type})")
                    else:
                        formatted.append(f"Table: {table}")
                return '\n'.join(formatted)
        return str(schema)
    
    def _format_metadata(self, metadata: Dict[str, Any]) -> str:
        """Format metadata for prompt."""
        return str(metadata)

