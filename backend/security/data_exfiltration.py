"""
Data Exfiltration Protection

Protect against data exfiltration attacks.
"""

import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Validation result."""
    valid: bool
    error: Optional[str] = None
    suggestion: Optional[str] = None


class DataExfiltrationProtection:
    """Protect against data exfiltration."""
    
    def __init__(self, max_rows: int = 10000, max_export_rows: int = 1000):
        """
        Initialize data exfiltration protection.
        
        Args:
            max_rows: Maximum rows allowed in query
            max_export_rows: Maximum rows allowed for export
        """
        self.max_rows = max_rows
        self.max_export_rows = max_export_rows
    
    def validate_query(self, sql: str, user_permissions: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """
        Validate query doesn't exfiltrate data.
        
        Args:
            sql: SQL query string
            user_permissions: User permissions dictionary
        
        Returns:
            ValidationResult
        """
        user_permissions = user_permissions or {}
        sql_upper = sql.upper()
        
        # Block SELECT *
        if re.search(r'SELECT\s+\*', sql_upper, re.IGNORECASE):
            return ValidationResult(
                valid=False,
                error='SELECT * is not allowed',
                suggestion='Specify explicit columns'
            )
        
        # Check row limits
        if not self._has_row_limit(sql):
            return ValidationResult(
                valid=False,
                error='Query must have row limit',
                suggestion='Add LIMIT clause'
            )
        
        # Check limit value
        limit_match = re.search(r'LIMIT\s+(\d+)', sql_upper, re.IGNORECASE)
        if limit_match:
            limit_value = int(limit_match.group(1))
            max_allowed = user_permissions.get('max_rows', self.max_rows)
            if limit_value > max_allowed:
                return ValidationResult(
                    valid=False,
                    error=f'Row limit ({limit_value}) exceeds maximum ({max_allowed})',
                    suggestion=f'Reduce LIMIT to {max_allowed} or less'
                )
        
        # Check column allowlist
        columns = self._extract_columns(sql)
        allowed_columns = user_permissions.get('allowed_columns')
        if allowed_columns:
            disallowed = [c for c in columns if c not in allowed_columns]
            if disallowed:
                return ValidationResult(
                    valid=False,
                    error=f'Access denied to columns: {", ".join(disallowed)}',
                    suggestion='Contact administrator for access to these columns'
                )
        
        # Check for suspicious patterns
        suspicious_patterns = [
            (r'INTO\s+OUTFILE', 'INTO OUTFILE statement'),
            (r'INTO\s+DUMPFILE', 'INTO DUMPFILE statement'),
            (r'COPY\s+.*TO', 'COPY TO statement'),
        ]
        
        for pattern, description in suspicious_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                return ValidationResult(
                    valid=False,
                    error=f'Suspicious pattern detected: {description}',
                    suggestion='File export operations are not allowed'
                )
        
        return ValidationResult(valid=True)
    
    def _has_row_limit(self, sql: str) -> bool:
        """Check if query has row limit."""
        return bool(re.search(r'LIMIT\s+\d+', sql.upper(), re.IGNORECASE))
    
    def _extract_columns(self, sql: str) -> List[str]:
        """Extract column names from SELECT clause."""
        match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        
        columns_str = match.group(1).strip()
        if columns_str == '*':
            return []
        
        # Split by comma and clean
        columns = []
        for col in columns_str.split(','):
            col = col.strip()
            # Remove aliases
            col = re.sub(r'\s+AS\s+\w+', '', col, flags=re.IGNORECASE)
            # Remove table prefixes
            col = col.split('.')[-1]
            columns.append(col)
        
        return columns

