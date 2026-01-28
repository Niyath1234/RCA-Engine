"""
Determinism Enforcer

Invariant 1: Query execution must be deterministic.
Same input → same SQL → same execution plan → same output.
"""

import re
from typing import Optional
from dataclasses import dataclass


@dataclass
class DeterminismViolation:
    """Represents a determinism violation."""
    violation_type: str
    message: str
    suggestion: str


class DeterminismEnforcer:
    """Enforce deterministic query execution."""
    
    def __init__(self, max_rows: int = 10000, default_limit: int = 1000):
        self.max_rows = max_rows
        self.default_limit = default_limit
    
    def enforce_ordering(self, sql: str, primary_keys: Optional[list] = None) -> str:
        """
        Inject ORDER BY if missing for deterministic results.
        
        Args:
            sql: SQL query string
            primary_keys: List of primary key columns to use for ordering
        
        Returns:
            SQL with ORDER BY clause added if needed
        """
        sql_upper = sql.upper().strip()
        
        # Check if query has ORDER BY
        if 'ORDER BY' in sql_upper:
            return sql
        
        # Check if it's a SELECT query that might return multiple rows
        if not sql_upper.startswith('SELECT'):
            return sql
        
        # Check if it's an aggregation (GROUP BY) - these need ordering too
        has_group_by = 'GROUP BY' in sql_upper
        has_limit = 'LIMIT' in sql_upper
        
        # If it's a single-row query (aggregation without GROUP BY), might not need ordering
        # But for safety, add ordering anyway
        if has_group_by or not has_limit:
            # Add deterministic ordering
            if primary_keys:
                order_by_cols = ', '.join(primary_keys)
            else:
                # Extract column names from SELECT clause as fallback
                order_by_cols = self._extract_select_columns(sql)
                if not order_by_cols:
                    # Last resort: use all columns
                    order_by_cols = '*'
            
            # Insert ORDER BY before LIMIT if present, otherwise append
            if 'LIMIT' in sql_upper:
                # Insert before LIMIT
                limit_pos = sql_upper.rfind('LIMIT')
                sql = sql[:limit_pos].rstrip() + f' ORDER BY {order_by_cols} ' + sql[limit_pos:]
            else:
                sql = sql.rstrip().rstrip(';') + f' ORDER BY {order_by_cols}'
        
        return sql
    
    def enforce_limit(self, sql: str) -> str:
        """
        Inject LIMIT if missing or exceeds max_rows.
        
        Args:
            sql: SQL query string
        
        Returns:
            SQL with LIMIT clause enforced
        """
        sql_upper = sql.upper().strip()
        
        # Check if LIMIT exists
        limit_match = re.search(r'LIMIT\s+(\d+)', sql_upper, re.IGNORECASE)
        
        if limit_match:
            current_limit = int(limit_match.group(1))
            if current_limit > self.max_rows:
                # Replace with max_rows
                sql = re.sub(
                    r'LIMIT\s+\d+',
                    f'LIMIT {self.max_rows}',
                    sql,
                    flags=re.IGNORECASE
                )
        else:
            # Add LIMIT clause
            sql = sql.rstrip().rstrip(';') + f' LIMIT {self.default_limit}'
        
        return sql
    
    def validate_determinism(self, sql: str) -> tuple[bool, Optional[DeterminismViolation]]:
        """
        Validate query is deterministic.
        
        Args:
            sql: SQL query string
        
        Returns:
            Tuple of (is_valid, violation)
        """
        sql_upper = sql.upper()
        
        # Check for non-deterministic functions without seeds
        non_deterministic_patterns = [
            (r'RAND\(\)(?!\s*\(\s*\d)', 'RAND() without seed'),
            (r'RANDOM\(\)(?!\s*\(\s*\d)', 'RANDOM() without seed'),
            (r'NEWID\(\)', 'NEWID() generates random values'),
            (r'UUID\(\)', 'UUID() generates random values'),
        ]
        
        for pattern, violation_type in non_deterministic_patterns:
            if re.search(pattern, sql_upper):
                return False, DeterminismViolation(
                    violation_type=violation_type,
                    message=f"Non-deterministic function detected: {violation_type}",
                    suggestion="Use seeded random functions or remove non-deterministic operations"
                )
        
        # Check for NOW() without explicit time (though this might be acceptable)
        # We'll warn but not fail
        if re.search(r'\bNOW\(\)\b', sql_upper) and 'WHERE' not in sql_upper:
            # This is a warning, not a violation
            pass
        
        return True, None
    
    def enforce_all(self, sql: str, primary_keys: Optional[list] = None) -> str:
        """
        Enforce all determinism rules.
        
        Args:
            sql: SQL query string
            primary_keys: List of primary key columns
        
        Returns:
            SQL with all determinism rules enforced
        """
        # Validate first
        is_valid, violation = self.validate_determinism(sql)
        if not is_valid and violation:
            raise ValueError(f"Determinism violation: {violation.message}. {violation.suggestion}")
        
        # Enforce ordering
        sql = self.enforce_ordering(sql, primary_keys)
        
        # Enforce limit
        sql = self.enforce_limit(sql)
        
        return sql
    
    def _extract_select_columns(self, sql: str) -> str:
        """Extract column names from SELECT clause."""
        # Simple extraction - get text between SELECT and FROM
        match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if match:
            columns = match.group(1).strip()
            # If it's SELECT *, return empty (will use * as fallback)
            if columns == '*':
                return ''
            # Return first column as ordering column
            first_col = columns.split(',')[0].strip()
            # Remove aliases
            first_col = re.sub(r'\s+AS\s+\w+', '', first_col, flags=re.IGNORECASE)
            return first_col
        return ''

