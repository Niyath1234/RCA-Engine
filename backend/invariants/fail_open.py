"""
Fail-Open Enforcer (Cursor-like behavior)

Permissive mode: Try to interpret vague queries rather than rejecting them.
Make reasonable assumptions and warn the user.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from backend.invariants.fail_closed import ValidationResult


@dataclass
class PermissiveValidationResult:
    """Result of permissive validation - allows ambiguous queries with warnings."""
    valid: bool
    warnings: List[str] = None
    assumptions: List[str] = None
    error: Optional[str] = None
    suggestion: Optional[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.assumptions is None:
            self.assumptions = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {'valid': self.valid}
        if self.warnings:
            result['warnings'] = self.warnings
        if self.assumptions:
            result['assumptions'] = self.assumptions
        if self.error:
            result['error'] = self.error
        if self.suggestion:
            result['suggestion'] = self.suggestion
        return result


class FailOpenEnforcer:
    """
    Permissive enforcer - interprets vague queries like Cursor.
    
    Key differences from FailClosedEnforcer:
    - Makes reasonable assumptions instead of rejecting
    - Returns warnings instead of errors for ambiguous queries
    - Only blocks truly unsafe operations (DROP, DELETE, etc.)
    
    Can operate in two modes:
    1. Assumption mode (default): Makes assumptions and warns
    2. Clarification mode: Asks questions before proceeding
    """
    
    def __init__(self, known_metrics: Optional[List[str]] = None, 
                 default_time_range: str = "last 30 days",
                 clarification_mode: bool = False,
                 clarification_agent=None):
        """
        Initialize fail-open enforcer.
        
        Args:
            known_metrics: List of known/valid metrics (for suggestions)
            default_time_range: Default time range to use if not specified
            clarification_mode: If True, asks questions instead of making assumptions
            clarification_agent: Optional ClarificationAgent instance
        """
        self.known_metrics = known_metrics or []
        self.default_time_range = default_time_range
        self.clarification_mode = clarification_mode
        self.clarification_agent = clarification_agent
    
    def validate_query_intent(self, intent: Dict[str, Any], 
                            query_text: Optional[str] = None,
                            metadata: Optional[Dict[str, Any]] = None) -> PermissiveValidationResult:
        """
        Permissive validation - makes assumptions OR asks questions.
        
        Args:
            intent: Intent dictionary
            query_text: Original query text (for context)
            metadata: Optional metadata for clarification agent
        
        Returns:
            PermissiveValidationResult with warnings/assumptions OR clarification questions
        """
        # If clarification mode is enabled, check if we should ask questions
        if self.clarification_mode and self.clarification_agent and query_text:
            clarification_result = self.clarification_agent.analyze_query(
                query_text, intent, metadata
            )
            
            if clarification_result.needs_clarification:
                # Return clarification questions instead of assumptions
                return PermissiveValidationResult(
                    valid=False,  # Not valid until clarified
                    error="Query needs clarification",
                    warnings=[f"Please answer {len(clarification_result.questions)} clarification question(s)"],
                    assumptions=[q.question for q in clarification_result.questions]
                )
        
        warnings = []
        assumptions = []
        
        # Check for metric - if missing, try to infer
        metric = intent.get('metric')
        if not metric:
            # Try to infer from query text or intent structure
            if query_text:
                # Look for metric-like keywords
                query_lower = query_text.lower()
                metric_keywords = {
                    'revenue': 'revenue',
                    'sales': 'sales',
                    'total': 'total',
                    'sum': 'sum',
                    'count': 'count',
                    'outstanding': 'outstanding',
                    'principal': 'principal',
                }
                
                for keyword, metric_name in metric_keywords.items():
                    if keyword in query_lower:
                        assumptions.append(f"Inferred metric: '{metric_name}' from query keyword '{keyword}'")
                        warnings.append(f"Metric not explicitly specified - inferred from query context")
                        break
            
            if not assumptions:
                warnings.append("No metric specified - assuming relational query (individual records)")
                assumptions.append("Query type inferred as 'relational' (no aggregation)")
        
        # Check for time range or aggregation - make reasonable defaults
        has_time_range = bool(intent.get('time_range'))
        has_aggregation = bool(intent.get('aggregation'))
        query_type = intent.get('query_type', 'relational')
        
        if not has_time_range and not has_aggregation:
            if query_type == 'metric':
                # For metric queries, suggest a default time range
                warnings.append(f"No time range specified - using default: '{self.default_time_range}'")
                assumptions.append(f"Applied default time range: {self.default_time_range}")
            else:
                # For relational queries, no time range needed
                assumptions.append("Relational query - no time range required")
        
        # Unknown metric - warn but don't block
        if metric and self.known_metrics and metric not in self.known_metrics:
            warnings.append(f"Metric '{metric}' not found in known metrics - proceeding anyway")
            assumptions.append(f"Using metric '{metric}' as specified (may need verification)")
        
        return PermissiveValidationResult(
            valid=True,  # Always valid - we make assumptions
            warnings=warnings,
            assumptions=assumptions
        )
    
    def validate_sql_safety(self, sql: str) -> PermissiveValidationResult:
        """
        Validate SQL safety - only block truly dangerous operations.
        
        Args:
            sql: SQL query string
        
        Returns:
            PermissiveValidationResult
        """
        import re
        
        # Only block destructive operations
        dangerous_patterns = [
            (r'DROP\s+TABLE', 'DROP TABLE statement detected'),
            (r'TRUNCATE\s+TABLE', 'TRUNCATE statement detected'),
            (r'DELETE\s+FROM.*WHERE\s+1\s*=\s*1', 'Unbounded DELETE detected'),
            (r'UPDATE.*SET.*WHERE\s+1\s*=\s*1', 'Unbounded UPDATE detected'),
            (r'ALTER\s+TABLE', 'ALTER TABLE statement detected'),
        ]
        
        sql_upper = sql.upper()
        warnings = []
        
        # Warn about potentially risky operations but don't block
        risky_patterns = [
            (r'CREATE\s+TABLE', 'CREATE TABLE statement'),
            (r'INSERT\s+INTO', 'INSERT statement'),
        ]
        
        for pattern, description in risky_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                warnings.append(f"Potentially risky operation: {description}")
        
        # Block only truly dangerous operations
        for pattern, description in dangerous_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                return PermissiveValidationResult(
                    valid=False,
                    error=f"Dangerous SQL pattern detected: {description}",
                    suggestion="This query contains destructive operations and cannot be executed"
                )
        
        return PermissiveValidationResult(
            valid=True,
            warnings=warnings
        )
    
    def validate_query_complexity(self, sql: str, max_joins: int = 10,
                                 max_subqueries: int = 5) -> PermissiveValidationResult:
        """
        Permissive complexity validation - warns but allows complex queries.
        
        Args:
            sql: SQL query string
            max_joins: Maximum recommended joins (warns if exceeded)
            max_subqueries: Maximum recommended subqueries (warns if exceeded)
        
        Returns:
            PermissiveValidationResult
        """
        import re
        
        warnings = []
        
        # Count JOINs
        join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
        if join_count > max_joins:
            warnings.append(f"Complex query: {join_count} joins (recommended max: {max_joins}) - may be slow")
        
        # Count subqueries
        subquery_count = len(re.findall(r'\(\s*SELECT', sql, re.IGNORECASE))
        if subquery_count > max_subqueries:
            warnings.append(f"Complex query: {subquery_count} subqueries (recommended max: {max_subqueries}) - may be slow")
        
        return PermissiveValidationResult(
            valid=True,  # Always allow, just warn
            warnings=warnings
        )
    
    def validate_all(self, intent: Optional[Dict[str, Any]] = None,
                     sql: Optional[str] = None,
                     query_text: Optional[str] = None,
                     table_names: Optional[List[str]] = None,
                     column_names: Optional[List[str]] = None,
                     allowed_tables: Optional[List[str]] = None,
                     allowed_columns: Optional[List[str]] = None) -> PermissiveValidationResult:
        """
        Run all validations in permissive mode.
        
        Args:
            intent: Intent dictionary
            sql: SQL query string
            query_text: Original query text
            table_names: List of table names
            column_names: List of column names
            allowed_tables: Allowed tables (if None, all allowed)
            allowed_columns: Allowed columns (if None, all allowed)
        
        Returns:
            PermissiveValidationResult
        """
        all_warnings = []
        all_assumptions = []
        
        # Validate intent (permissive)
        if intent:
            result = self.validate_query_intent(intent, query_text)
            all_warnings.extend(result.warnings)
            all_assumptions.extend(result.assumptions)
            if result.error:
                return result  # Only fail on errors, not warnings
        
        # Validate SQL safety (only blocks dangerous operations)
        if sql:
            result = self.validate_sql_safety(sql)
            if not result.valid:
                return result  # Block dangerous operations
            all_warnings.extend(result.warnings)
            
            result = self.validate_query_complexity(sql)
            all_warnings.extend(result.warnings)
        
        # Validate table access (permissive - warn but allow)
        if table_names and allowed_tables:
            disallowed = [t for t in table_names if t not in allowed_tables]
            if disallowed:
                all_warnings.append(f"Tables may not be accessible: {', '.join(disallowed)}")
        
        # Validate column access (permissive - warn but allow)
        if column_names and allowed_columns:
            disallowed = [c for c in column_names if c not in allowed_columns]
            if disallowed:
                all_warnings.append(f"Columns may not be accessible: {', '.join(disallowed)}")
        
        return PermissiveValidationResult(
            valid=True,
            warnings=all_warnings,
            assumptions=all_assumptions
        )
    
    def get_clarification_questions(self, query: str, intent: Optional[Dict[str, Any]] = None,
                                    metadata: Optional[Dict[str, Any]] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Get clarification questions for ambiguous query.
        
        Args:
            query: User query text
            intent: Optional extracted intent
            metadata: Optional metadata
        
        Returns:
            List of clarification questions or None if not needed
        """
        if not self.clarification_agent:
            return None
        
        result = self.clarification_agent.analyze_query(query, intent, metadata)
        
        if result.needs_clarification:
            return [q.to_dict() for q in result.questions]
        
        return None

