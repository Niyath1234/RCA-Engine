"""
Prompt Injection Protection

Protect against prompt injection attacks.
"""

import re
from typing import List, Dict, Any


class PromptInjectionProtection:
    """Protect against prompt injection."""
    
    DANGEROUS_PATTERNS = [
        r'```',  # Code blocks
        r'---',  # Markdown separators
        r'<script',  # Script tags
        r'javascript:',  # JavaScript protocol
        r'eval\(',  # Eval function
        r'exec\(',  # Exec function
    ]
    
    MAX_INPUT_LENGTH = 1000
    
    def __init__(self, max_length: int = MAX_INPUT_LENGTH):
        """
        Initialize prompt injection protection.
        
        Args:
            max_length: Maximum input length
        """
        self.max_length = max_length
    
    def sanitize_user_input(self, user_input: str) -> str:
        """
        Sanitize user input before sending to LLM.
        
        Args:
            user_input: User input string
        
        Returns:
            Sanitized input string
        """
        # Remove control characters
        sanitized = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', user_input)
        
        # Limit length
        if len(sanitized) > self.max_length:
            sanitized = sanitized[:self.max_length]
        
        # Remove dangerous patterns
        for pattern in self.DANGEROUS_PATTERNS:
            sanitized = re.sub(pattern, '', sanitized, flags=re.IGNORECASE)
        
        # Remove multiple spaces
        sanitized = re.sub(r'\s+', ' ', sanitized)
        
        return sanitized.strip()
    
    def sanitize_table_names(self, table_names: List[str]) -> List[str]:
        """
        Sanitize table names.
        
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
    
    def sanitize_error_messages(self, error: Exception) -> str:
        """
        Sanitize error messages before showing to user.
        
        Args:
            error: Exception object
        
        Returns:
            Safe error message
        """
        # Don't expose internal details
        safe_message = "An error occurred. Please try again."
        
        # Log full error internally (this would go to internal logs)
        # logger.error("Error occurred", error=str(error), exc_info=True)
        
        return safe_message
    
    def detect_injection(self, user_input: str) -> bool:
        """
        Detect potential prompt injection.
        
        Args:
            user_input: User input string
        
        Returns:
            True if injection detected, False otherwise
        """
        # Check for suspicious patterns
        injection_patterns = [
            r'ignore\s+previous',
            r'forget\s+previous',
            r'new\s+instructions',
            r'system\s*:',
            r'admin\s*:',
            r'override',
        ]
        
        user_input_lower = user_input.lower()
        for pattern in injection_patterns:
            if re.search(pattern, user_input_lower):
                return True
        
        return False
    
    def sanitize_for_llm(self, user_input: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create safe context for LLM.
        
        Args:
            user_input: User input
            context: Additional context
        
        Returns:
            Safe context dictionary
        """
        # Sanitize user input
        sanitized_input = self.sanitize_user_input(user_input)
        
        # Check for injection
        if self.detect_injection(sanitized_input):
            raise ValueError("Potential prompt injection detected")
        
        safe_context = {
            'query': sanitized_input,
        }
        
        if context:
            # Sanitize context
            safe_context.update(self._sanitize_context(context))
        
        return safe_context
    
    def _sanitize_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize context dictionary."""
        sanitized = {}
        for key, value in context.items():
            if isinstance(value, str):
                sanitized[key] = self.sanitize_user_input(value)
            elif isinstance(value, list):
                sanitized[key] = [self.sanitize_user_input(v) if isinstance(v, str) else v 
                                 for v in value]
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_context(value)
            else:
                sanitized[key] = value
        return sanitized

