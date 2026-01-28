"""
Query Firewall

Firewall for blocking dangerous queries.
"""

import re
from dataclasses import dataclass
from typing import List, Tuple, Optional


@dataclass
class FirewallResult:
    """Result from query firewall check."""
    allowed: bool
    reason: Optional[str] = None
    suggestion: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        result = {'allowed': self.allowed}
        if self.reason:
            result['reason'] = self.reason
        if self.suggestion:
            result['suggestion'] = self.suggestion
        return result


class QueryFirewall:
    """Firewall for blocking dangerous queries."""
    
    BLOCKED_PATTERNS: List[Tuple[str, str, Optional[str]]] = [
        # Pattern, reason, suggestion
        (r'CROSS\s+JOIN(?!.*ON)', 'CROSS JOIN without condition', 
         'Add explicit join condition or use INNER JOIN'),
        (r'SELECT\s+\*', 'SELECT * (use explicit columns)', 
         'Specify explicit column names instead of SELECT *'),
        (r'(JOIN.*){5,}', 'Too many joins (>4)', 
         'Simplify query to use at most 4 joins'),
        (r'GROUP\s+BY\s*$', 'GROUP BY without HAVING or LIMIT', 
         'Add HAVING clause or LIMIT to GROUP BY query'),
        (r'UNION\s+ALL.*UNION\s+ALL.*UNION\s+ALL', 'Too many UNION ALL operations', 
         'Limit UNION ALL operations'),
        (r'WHERE\s+1\s*=\s*1', 'Suspicious WHERE clause (1=1)', 
         'Remove unnecessary WHERE 1=1 condition'),
    ]
    
    def __init__(self, custom_patterns: Optional[List[Tuple[str, str, Optional[str]]]] = None):
        """
        Initialize query firewall.
        
        Args:
            custom_patterns: Custom patterns to add to blocked patterns
        """
        self.patterns = self.BLOCKED_PATTERNS.copy()
        if custom_patterns:
            self.patterns.extend(custom_patterns)
    
    def check_query(self, sql: str) -> FirewallResult:
        """
        Check if query passes firewall.
        
        Args:
            sql: SQL query string
        
        Returns:
            FirewallResult
        """
        sql_upper = sql.upper()
        
        for pattern, reason, suggestion in self.patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE | re.DOTALL):
                return FirewallResult(
                    allowed=False,
                    reason=reason,
                    suggestion=suggestion
                )
        
        # Additional checks
        # Check for too many subqueries
        subquery_count = len(re.findall(r'\(\s*SELECT', sql_upper, re.IGNORECASE))
        if subquery_count > 3:
            return FirewallResult(
                allowed=False,
                reason=f'Too many subqueries ({subquery_count}, max 3)',
                suggestion='Simplify query to use fewer subqueries'
            )
        
        # Check for unbounded result sets (no LIMIT in certain cases)
        if 'GROUP BY' in sql_upper and 'LIMIT' not in sql_upper:
            return FirewallResult(
                allowed=False,
                reason='GROUP BY query without LIMIT',
                suggestion='Add LIMIT clause to GROUP BY query'
            )
        
        return FirewallResult(allowed=True)
    
    def add_pattern(self, pattern: str, reason: str, suggestion: Optional[str] = None):
        """
        Add custom pattern to firewall.
        
        Args:
            pattern: Regex pattern to match
            reason: Reason for blocking
            suggestion: Optional suggestion for user
        """
        self.patterns.append((pattern, reason, suggestion))
    
    def remove_pattern(self, pattern: str):
        """
        Remove pattern from firewall.
        
        Args:
            pattern: Pattern to remove
        """
        self.patterns = [p for p in self.patterns if p[0] != pattern]

