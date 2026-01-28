"""
Fail-Closed Enforcer

Invariant 4: Fail closed, not open.
If something is ambiguous or unsafe, reject it.
Ask for clarification. Apply stricter limits.
Never "try anyway."
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class ValidationResult:
    """Result of validation check."""
    valid: bool
    error: Optional[str] = None
    suggestion: Optional[str] = None
    available_options: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {'valid': self.valid}
        if self.error:
            result['error'] = self.error
        if self.suggestion:
            result['suggestion'] = self.suggestion
        if self.available_options:
            result['available_options'] = self.available_options
        return result


class FailClosedEnforcer:
    """Enforce fail-closed behavior."""
    
    def __init__(self, known_metrics: Optional[List[str]] = None):
        """
        Initialize fail-closed enforcer.
        
        Args:
            known_metrics: List of known/valid metrics
        """
        self.known_metrics = known_metrics or []
    
    def validate_query_intent(self, intent: Dict[str, Any]) -> ValidationResult:
        """
        Validate intent before proceeding.
        
        Args:
            intent: Intent dictionary
        
        Returns:
            ValidationResult
        """
        # Must have explicit metric
        metric = intent.get('metric')
        if not metric:
            return ValidationResult(
                valid=False,
                error="Ambiguous intent: metric not specified",
                suggestion="Please specify the metric you want to query",
                available_options=self.known_metrics if self.known_metrics else None
            )
        
        # If we have known metrics, validate against them
        if self.known_metrics and metric not in self.known_metrics:
            return ValidationResult(
                valid=False,
                error=f"Unknown metric: {metric}",
                suggestion="Please use one of the available metrics",
                available_options=self.known_metrics
            )
        
        # Must have explicit time range or aggregation
        has_time_range = bool(intent.get('time_range'))
        has_aggregation = bool(intent.get('aggregation'))
        
        if not has_time_range and not has_aggregation:
            return ValidationResult(
                valid=False,
                error="Ambiguous intent: time range or aggregation required",
                suggestion="Please specify time range (e.g., 'last 7 days') or aggregation type (e.g., 'sum', 'average')"
            )
        
        return ValidationResult(valid=True)
    
    def validate_sql_safety(self, sql: str) -> ValidationResult:
        """
        Validate SQL is safe to execute.
        
        Args:
            sql: SQL query string
        
        Returns:
            ValidationResult
        """
        import re
        
        # Check for dangerous patterns
        dangerous_patterns = [
            (r'DROP\s+TABLE', 'DROP TABLE statement detected'),
            (r'TRUNCATE', 'TRUNCATE statement detected'),
            (r'DELETE\s+FROM.*WHERE\s+1\s*=\s*1', 'Unbounded DELETE detected'),
            (r'UPDATE.*SET.*WHERE\s+1\s*=\s*1', 'Unbounded UPDATE detected'),
            (r'ALTER\s+TABLE', 'ALTER TABLE statement detected'),
            (r'CREATE\s+TABLE', 'CREATE TABLE statement detected'),
            (r'INSERT\s+INTO', 'INSERT statement detected'),
        ]
        
        sql_upper = sql.upper()
        for pattern, description in dangerous_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                return ValidationResult(
                    valid=False,
                    error=f"Dangerous SQL pattern detected: {description}",
                    suggestion="This query contains potentially destructive operations and cannot be executed"
                )
        
        return ValidationResult(valid=True)
    
    def validate_table_access(self, table_names: List[str], 
                            allowed_tables: Optional[List[str]] = None) -> ValidationResult:
        """
        Validate table access permissions.
        
        Args:
            table_names: List of table names in query
            allowed_tables: List of allowed tables (if None, all allowed)
        
        Returns:
            ValidationResult
        """
        if allowed_tables is None:
            return ValidationResult(valid=True)
        
        disallowed = [t for t in table_names if t not in allowed_tables]
        if disallowed:
            return ValidationResult(
                valid=False,
                error=f"Access denied to tables: {', '.join(disallowed)}",
                suggestion="Contact administrator for access to these tables",
                available_options=allowed_tables
            )
        
        return ValidationResult(valid=True)
    
    def validate_column_access(self, column_names: List[str],
                             allowed_columns: Optional[List[str]] = None) -> ValidationResult:
        """
        Validate column access permissions.
        
        Args:
            column_names: List of column names in query
            allowed_columns: List of allowed columns (if None, all allowed)
        
        Returns:
            ValidationResult
        """
        if allowed_columns is None:
            return ValidationResult(valid=True)
        
        disallowed = [c for c in column_names if c not in allowed_columns]
        if disallowed:
            return ValidationResult(
                valid=False,
                error=f"Access denied to columns: {', '.join(disallowed)}",
                suggestion="Contact administrator for access to these columns",
                available_options=allowed_columns
            )
        
        return ValidationResult(valid=True)
    
    def validate_query_complexity(self, sql: str, max_joins: int = 4,
                                 max_subqueries: int = 2) -> ValidationResult:
        """
        Validate query complexity limits.
        
        Args:
            sql: SQL query string
            max_joins: Maximum number of joins allowed
            max_subqueries: Maximum number of subqueries allowed
        
        Returns:
            ValidationResult
        """
        import re
        
        # Count JOINs
        join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
        if join_count > max_joins:
            return ValidationResult(
                valid=False,
                error=f"Query too complex: {join_count} joins (max {max_joins})",
                suggestion=f"Simplify query to use at most {max_joins} joins"
            )
        
        # Count subqueries (simple heuristic)
        subquery_count = len(re.findall(r'\(\s*SELECT', sql, re.IGNORECASE))
        if subquery_count > max_subqueries:
            return ValidationResult(
                valid=False,
                error=f"Query too complex: {subquery_count} subqueries (max {max_subqueries})",
                suggestion=f"Simplify query to use at most {max_subqueries} subqueries"
            )
        
        return ValidationResult(valid=True)
    
    def validate_all(self, intent: Optional[Dict[str, Any]] = None,
                     sql: Optional[str] = None,
                     table_names: Optional[List[str]] = None,
                     column_names: Optional[List[str]] = None,
                     allowed_tables: Optional[List[str]] = None,
                     allowed_columns: Optional[List[str]] = None) -> ValidationResult:
        """
        Run all validations.
        
        Args:
            intent: Intent dictionary
            sql: SQL query string
            table_names: List of table names
            column_names: List of column names
            allowed_tables: Allowed tables
            allowed_columns: Allowed columns
        
        Returns:
            ValidationResult
        """
        # Validate intent
        if intent:
            result = self.validate_query_intent(intent)
            if not result.valid:
                return result
        
        # Validate SQL safety
        if sql:
            result = self.validate_sql_safety(sql)
            if not result.valid:
                return result
            
            result = self.validate_query_complexity(sql)
            if not result.valid:
                return result
        
        # Validate table access
        if table_names:
            result = self.validate_table_access(table_names, allowed_tables)
            if not result.valid:
                return result
        
        # Validate column access
        if column_names:
            result = self.validate_column_access(column_names, allowed_columns)
            if not result.valid:
                return result
        
        return ValidationResult(valid=True)

