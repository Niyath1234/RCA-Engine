"""
Query Sandbox

Hard sandboxing for query execution.
Multiple layers of protection: DB role, connection, query rewrite.
"""

import re
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class SandboxConfig:
    """Configuration for query sandbox."""
    max_execution_time: int = 30  # seconds
    max_rows: int = 10000
    default_limit: int = 1000
    read_only_role: str = 'rca_readonly'
    enforce_ordering: bool = True
    enforce_limit: bool = True


class QuerySandbox:
    """Hard sandboxing for query execution."""
    
    def __init__(self, db_config: Dict[str, Any], sandbox_config: Optional[SandboxConfig] = None):
        """
        Initialize query sandbox.
        
        Args:
            db_config: Database configuration
            sandbox_config: Sandbox configuration
        """
        self.db_config = db_config
        self.config = sandbox_config or SandboxConfig()
    
    def execute_sandboxed(self, sql: str) -> str:
        """
        Execute query in sandboxed environment (rewrite for safety).
        
        Args:
            sql: SQL query string
        
        Returns:
            Safe SQL query string
        
        Raises:
            SecurityError: If query contains dangerous patterns
        """
        # Step 1: Query rewriting (safety checks)
        safe_sql = self._rewrite_query(sql)
        
        return safe_sql
    
    def _rewrite_query(self, sql: str) -> str:
        """
        Rewrite query for safety.
        
        Args:
            sql: Original SQL query
        
        Returns:
            Safe SQL query
        """
        # Block dangerous patterns first
        self._block_dangerous_patterns(sql)
        
        # Parse SQL (simplified - in production, use proper SQL parser)
        sql_upper = sql.upper().strip()
        
        # Inject LIMIT if missing or exceeds max
        if self.config.enforce_limit:
            sql = self._inject_limit(sql)
        
        # Inject ORDER BY if missing (for determinism)
        if self.config.enforce_ordering:
            sql = self._inject_ordering(sql)
        
        return sql
    
    def _inject_limit(self, sql: str) -> str:
        """Inject LIMIT if missing or exceeds max."""
        sql_upper = sql.upper()
        
        # Check if LIMIT exists
        limit_match = re.search(r'LIMIT\s+(\d+)', sql_upper, re.IGNORECASE)
        
        if limit_match:
            current_limit = int(limit_match.group(1))
            if current_limit > self.config.max_rows:
                # Replace with max_rows
                sql = re.sub(
                    r'LIMIT\s+\d+',
                    f'LIMIT {self.config.max_rows}',
                    sql,
                    flags=re.IGNORECASE
                )
        else:
            # Add LIMIT clause
            sql = sql.rstrip().rstrip(';') + f' LIMIT {self.config.default_limit}'
        
        return sql
    
    def _inject_ordering(self, sql: str) -> str:
        """Inject ORDER BY if missing."""
        sql_upper = sql.upper()
        
        # Check if ORDER BY exists
        if 'ORDER BY' in sql_upper:
            return sql
        
        # Only add ORDER BY for SELECT queries
        if not sql_upper.startswith('SELECT'):
            return sql
        
        # Check if it's a single-row query (aggregation without GROUP BY)
        has_group_by = 'GROUP BY' in sql_upper
        
        # Add deterministic ordering
        # Extract first column from SELECT as ordering column
        match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_upper, re.IGNORECASE | re.DOTALL)
        if match:
            columns = match.group(1).strip()
            if columns != '*':
                first_col = columns.split(',')[0].strip()
                # Remove aliases
                first_col = re.sub(r'\s+AS\s+\w+', '', first_col, flags=re.IGNORECASE)
                order_by_col = first_col
            else:
                # Use first column from table (simplified)
                order_by_col = '1'  # Fallback to column index
        else:
            order_by_col = '1'
        
        # Insert ORDER BY before LIMIT if present, otherwise append
        if 'LIMIT' in sql_upper:
            limit_pos = sql_upper.rfind('LIMIT')
            sql = sql[:limit_pos].rstrip() + f' ORDER BY {order_by_col} ' + sql[limit_pos:]
        else:
            sql = sql.rstrip().rstrip(';') + f' ORDER BY {order_by_col}'
        
        return sql
    
    def _block_dangerous_patterns(self, sql: str):
        """
        Block dangerous SQL patterns.
        
        Args:
            sql: SQL query string
        
        Raises:
            SecurityError: If dangerous pattern detected
        """
        dangerous_patterns = [
            (r'CROSS\s+JOIN(?!.*ON)', 'CROSS JOIN without condition'),
            (r'DELETE\s+FROM', 'DELETE statement'),
            (r'UPDATE\s+.*SET', 'UPDATE statement'),
            (r'DROP\s+TABLE', 'DROP TABLE statement'),
            (r'TRUNCATE', 'TRUNCATE statement'),
            (r'ALTER\s+TABLE', 'ALTER TABLE statement'),
            (r'CREATE\s+TABLE', 'CREATE TABLE statement'),
            (r'INSERT\s+INTO', 'INSERT statement'),
        ]
        
        sql_upper = sql.upper()
        for pattern, description in dangerous_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                raise SecurityError(f"Dangerous pattern blocked: {description}")
    
    def get_readonly_connection_config(self) -> Dict[str, Any]:
        """
        Get configuration for read-only connection.
        
        Returns:
            Connection configuration dictionary
        """
        config = self.db_config.copy()
        config['role'] = self.config.read_only_role
        config['readonly'] = True
        config['statement_timeout'] = self.config.max_execution_time * 1000  # milliseconds
        return config


class SecurityError(Exception):
    """Security violation error."""
    pass

