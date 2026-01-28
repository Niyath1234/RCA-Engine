#!/usr/bin/env python3
"""
SQL Renderer - Deterministic, Non-LLM

LLMs never write final SQL directly.
This is a deterministic renderer that converts agent outputs to SQL.
"""

from typing import Dict, Any, List, Optional


class SQLRenderer:
    """
    Deterministic SQL Renderer.
    
    Purpose: Convert agent outputs to SQL query.
    Authority: NONE - pure deterministic rendering.
    
    This is NOT an LLM - it's a template renderer.
    """
    
    def render(self, intent_output: Dict[str, Any],
               metric_output: Dict[str, Any],
               table_output: Dict[str, Any],
               filter_output: Dict[str, Any],
               shape_output: Dict[str, Any]) -> str:
        """
        Render SQL query from agent outputs.
        
        Returns:
            SQL query string
        """
        base_table = table_output.get('base_table', '')
        query_type = intent_output.get('query_type', 'metric')
        
        # Build SELECT clause
        select_parts = []
        
        # Add dimensions first (for GROUP BY)
        dimensions = shape_output.get('dimensions', [])
        for dim in dimensions:
            dim_name = dim.get('name', '')
            dim_sql = dim.get('sql', '')
            select_parts.append(f"{dim_sql} AS {dim_name}")
        
        # Add metrics
        resolved_metrics = metric_output.get('resolved_metrics', [])
        for metric in resolved_metrics:
            metric_name = metric.get('name', '')
            canonical_name = metric.get('canonical_name', metric_name)
            sql_template = metric.get('sql_template', '')
            
            # Replace column references with table alias if needed
            sql_template = self._add_table_alias(sql_template, base_table, 't1')
            
            select_parts.append(f"{sql_template} AS {canonical_name}")
        
        # Build FROM clause
        from_clause = f"FROM {base_table} t1"
        
        # Build WHERE clause
        filters = filter_output.get('filters', [])
        where_clause = ""
        if filters:
            where_parts = []
            for filter_expr in filters:
                # Add table alias if not present
                filter_expr = self._add_table_alias_to_filter(filter_expr, base_table, 't1')
                where_parts.append(filter_expr)
            where_clause = f"WHERE {' AND '.join(where_parts)}"
        
        # Build GROUP BY clause
        group_by_clause = ""
        if query_type == 'metric' and dimensions:
            dim_names = [dim.get('name', '') for dim in dimensions]
            group_by_clause = f"GROUP BY {', '.join(dim_names)}"
        
        # Assemble SQL
        sql = f"SELECT {', '.join(select_parts)}\n"
        sql += f"    {from_clause}\n"
        if where_clause:
            sql += f"    {where_clause}\n"
        if group_by_clause:
            sql += f"    {group_by_clause}"
        
        return sql.strip()
    
    def _add_table_alias(self, sql_expression: str, table_name: str, alias: str) -> str:
        """
        Add table alias to column references in SQL expression.
        
        Example: "SUM(ledger_balance)" -> "SUM(t1.ledger_balance)"
        """
        import re
        
        # Pattern: column name not already prefixed with table
        # Match word boundaries, but not if already has table prefix
        pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        
        def replace_column(match):
            col_name = match.group(1)
            
            # Skip SQL keywords
            sql_keywords = {'SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'CAST', 'ABS', 'ROUND',
                           'DOUBLE', 'AS', 'WHEN', 'THEN', 'ELSE', 'END', 'CASE', 'AND', 'OR',
                           'IS', 'NULL', 'IN', 'LIKE', 'DATE'}
            if col_name.upper() in sql_keywords:
                return col_name
            
            # Check if already has table prefix
            if '.' in sql_expression[:match.start()] or '.' in sql_expression[match.end():]:
                # Might already have prefix - be conservative
                return col_name
            
            # Add alias prefix
            return f"{alias}.{col_name}"
        
        result = re.sub(pattern, replace_column, sql_expression)
        return result
    
    def _add_table_alias_to_filter(self, filter_expr: str, table_name: str, alias: str) -> str:
        """
        Add table alias to filter expression.
        
        Example: "ledger_balance > 0" -> "t1.ledger_balance > 0"
        But preserve function calls: "LOWER(product_name)" -> "LOWER(t1.product_name)"
        """
        import re
        
        # Skip if already has table alias
        if f"{alias}." in filter_expr:
            return filter_expr
        
        # Pattern: Match function calls like LOWER(column) or UPPER(column)
        # FUNCTION(column) -> FUNCTION(alias.column)
        function_pattern = r'\b(LOWER|UPPER|TRIM|CAST|ABS|ROUND)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)'
        
        def replace_function(match):
            func_name = match.group(1)
            col_name = match.group(2)
            # Skip if already has alias
            if '.' in match.group(0):
                return match.group(0)
            return f"{func_name}({alias}.{col_name})"
        
        result = re.sub(function_pattern, replace_function, filter_expr, flags=re.IGNORECASE)
        
        # Now handle standalone column references (not in function calls, not in quotes)
        # Split by operators and keywords to find column references
        # Simple approach: find column names that aren't in quotes or function calls
        
        # Find all column-like identifiers
        col_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        
        def replace_column(match):
            col_name = match.group(1)
            start_pos = match.start()
            end_pos = match.end()
            
            # Skip SQL keywords
            sql_keywords = {'AND', 'OR', 'IS', 'NULL', 'IN', 'LIKE', 'DATE', 'SELECT', 'FROM',
                           'WHERE', 'GROUP', 'BY', 'ORDER', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
                           'LOWER', 'UPPER', 'TRIM', 'CAST', 'ABS', 'ROUND', 'SUM', 'COUNT', 'AVG'}
            if col_name.upper() in sql_keywords:
                return col_name
            
            # Check if we're in a quoted string
            before = result[:start_pos]
            after = result[end_pos:]
            quote_count_before = before.count("'") + before.count('"')
            quote_count_after = after.count("'") + after.count('"')
            if quote_count_before % 2 == 1:  # Inside quoted string
                return col_name
            
            # Check if already has alias
            if start_pos > 0 and result[start_pos-1] == '.':
                return col_name
            
            # Check if it's part of a function call (already handled)
            func_before = before.rstrip()
            if func_before.endswith('('):
                return col_name
            
            # Check if next char suggests this is a column (operator, comma, space)
            if end_pos < len(result):
                next_char = result[end_pos]
                if next_char in [' ', '=', '>', '<', '!', ',', ')']:
                    return f"{alias}.{col_name}"
            
            return col_name
        
        result = re.sub(col_pattern, replace_column, result)
        return result

