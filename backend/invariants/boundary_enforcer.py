"""
LLM-Database Boundary Enforcer

Invariant 2: LLMs do NOT touch databases.
LLMs generate SQL and explain results. They never execute queries,
open connections, or see credentials.
"""

import sys
import re
from typing import Dict, Any, List


class LLMDatabaseBoundary:
    """Enforce hard boundary between LLM and database."""
    
    FORBIDDEN_MODULES = [
        'psycopg2',
        'pymysql',
        'sqlalchemy.engine',
        'sqlalchemy.pool',
        'sqlalchemy.orm',
        'cx_Oracle',
        'pymssql',
    ]
    
    SENSITIVE_SCHEMA_KEYS = [
        'connection_string',
        'credentials',
        'password',
        'api_key',
        'secret',
        'row_count',
        'indexes',
        'statistics',
        'size_bytes',
    ]
    
    def __init__(self, strict_mode: bool = True):
        """
        Initialize boundary enforcer.
        
        Args:
            strict_mode: If True, validate imports at initialization
        """
        self.strict_mode = strict_mode
        if strict_mode:
            self._validate_imports()
    
    def _validate_imports(self):
        """Ensure LLM modules don't import database modules."""
        # Get all currently loaded modules
        loaded_modules = list(sys.modules.keys())
        
        # Check for forbidden imports in LLM/planning modules
        violations = []
        for module_name in loaded_modules:
            # Check if this is an LLM or planning module
            is_llm_module = any(keyword in module_name.lower() for keyword in 
                               ['llm', 'planning', 'query_generator', 'intent'])
            
            if is_llm_module:
                # Check if it imports forbidden modules
                for forbidden in self.FORBIDDEN_MODULES:
                    if forbidden in module_name:
                        violations.append(f"{module_name} imports {forbidden}")
        
        if violations:
            raise RuntimeError(
                f"LLM-Database boundary violation detected:\n" + 
                "\n".join(f"  - {v}" for v in violations)
            )
    
    def sanitize_schema_for_llm(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove sensitive information before sending to LLM.
        
        Args:
            schema: Schema dictionary
        
        Returns:
            Sanitized schema dictionary
        """
        if not isinstance(schema, dict):
            return schema
        
        sanitized = {}
        
        for key, value in schema.items():
            # Skip sensitive keys
            if any(sensitive in key.lower() for sensitive in self.SENSITIVE_SCHEMA_KEYS):
                continue
            
            # Recursively sanitize nested dictionaries
            if isinstance(value, dict):
                sanitized[key] = self.sanitize_schema_for_llm(value)
            elif isinstance(value, list):
                sanitized[key] = [
                    self.sanitize_schema_for_llm(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                sanitized[key] = value
        
        return sanitized
    
    def sanitize_table_names(self, table_names: List[str]) -> List[str]:
        """
        Sanitize table names to prevent injection.
        
        Args:
            table_names: List of table names
        
        Returns:
            Sanitized table names
        """
        sanitized = []
        for name in table_names:
            # Only allow alphanumeric, underscore, and dot (for schema.table)
            if re.match(r'^[a-zA-Z0-9_.]+$', name):
                sanitized.append(name)
        return sanitized
    
    def validate_no_db_access(self, code_string: str) -> tuple[bool, str]:
        """
        Validate that code string doesn't contain database access patterns.
        
        Args:
            code_string: Code string to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        db_access_patterns = [
            r'\.connect\(',
            r'\.execute\(',
            r'\.cursor\(',
            r'\.query\(',
            r'psycopg2',
            r'pymysql',
            r'sqlalchemy\.create_engine',
        ]
        
        for pattern in db_access_patterns:
            if re.search(pattern, code_string, re.IGNORECASE):
                return False, f"Database access pattern detected: {pattern}"
        
        return True, ""
    
    def create_safe_context(self, schema: Dict[str, Any], 
                          additional_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create safe context for LLM that excludes database access.
        
        Args:
            schema: Schema dictionary
            additional_context: Additional context to include
        
        Returns:
            Safe context dictionary
        """
        safe_context = {
            'schema': self.sanitize_schema_for_llm(schema),
            'timestamp': None,  # Don't expose current time
        }
        
        if additional_context:
            # Sanitize additional context too
            safe_context.update(self.sanitize_schema_for_llm(additional_context))
        
        return safe_context

