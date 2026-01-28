#!/usr/bin/env python3
"""
Query Modifier

Handles incremental modifications to SQL queries based on natural language instructions.
Supports operations like:
- Add column
- Add filter
- Remove filter
- Modify existing filters
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from backend.conversational_context import ConversationalContext


class QueryModifier:
    """Handles query modifications based on natural language."""
    
    def __init__(self, resolver):
        """
        Initialize query modifier.
        
        Args:
            resolver: Table resolver for metadata access
        """
        self.resolver = resolver
    
    def detect_modification_type(self, query_text: str) -> Optional[str]:
        """
        Detect what type of modification is being requested.
        
        Returns:
            Modification type: 'add_column', 'add_filter', 'remove_filter', 'modify_filter', None
        """
        query_lower = query_text.lower().strip()
        
        # Patterns for different modification types
        patterns = {
            'add_column': [
                r'add\s+(?:a\s+)?(.+?)\s+(?:as\s+)?(?:column|field)',
                r'include\s+(?:a\s+)?(.+?)\s+(?:as\s+)?(?:column|field)',
                r'show\s+(?:me\s+)?(.+?)\s+(?:as\s+)?(?:column|field)',
                r'add\s+(?:the\s+)?(.+?)\s+to\s+(?:it|the\s+query|the\s+result)',
            ],
            'add_filter': [
                r'add\s+(?:a\s+)?(.+?)\s+filter',
                r'filter\s+by\s+(.+?)',
                r'where\s+(.+?)',
                r'only\s+(.+?)',
                r'excluding\s+(.+?)',
                r'exclude\s+(.+?)',
                r'remove\s+(.+?)',
            ],
            'remove_filter': [
                r'remove\s+(?:the\s+)?(.+?)\s+filter',
                r'remove\s+(?:the\s+)?(.+?)\s+condition',
                r'don\'?t\s+filter\s+by\s+(.+?)',
                r'without\s+(?:the\s+)?(.+?)\s+filter',
            ],
            'modify_filter': [
                r'change\s+(?:the\s+)?(.+?)\s+filter',
                r'modify\s+(?:the\s+)?(.+?)\s+filter',
                r'update\s+(?:the\s+)?(.+?)\s+filter',
            ]
        }
        
        for mod_type, pattern_list in patterns.items():
            for pattern in pattern_list:
                if re.search(pattern, query_lower):
                    return mod_type
        
        return None
    
    def extract_column_name(self, query_text: str) -> Optional[str]:
        """Extract column name from query text."""
        query_lower = query_text.lower()
        
        # Common patterns for column names
        patterns = [
            r'add\s+(?:a\s+)?(.+?)\s+(?:as\s+)?(?:column|field)',
            r'include\s+(?:a\s+)?(.+?)\s+(?:as\s+)?(?:column|field)',
            r'show\s+(?:me\s+)?(.+?)\s+(?:as\s+)?(?:column|field)',
            r'add\s+(?:the\s+)?(.+?)\s+to\s+(?:it|the\s+query)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, query_lower)
            if match:
                column_name = match.group(1).strip()
                # Clean up common words
                column_name = re.sub(r'\s+(?:flag|column|field|value)', '', column_name)
                return column_name
        
        return None
    
    def extract_filter_condition(self, query_text: str) -> Optional[Dict[str, Any]]:
        """Extract filter condition from query text."""
        query_lower = query_text.lower()
        
        # Try to extract filter details
        # This is a simplified version - in production, use LLM or more sophisticated NLP
        
        # Pattern: "remove X" or "excluding X" or "filter by X"
        exclusion_patterns = [
            r'remove\s+(.+?)(?:\s+filter|\s+condition|$)',
            r'excluding\s+(.+?)(?:\s+filter|\s+condition|$)',
            r'exclude\s+(.+?)(?:\s+filter|\s+condition|$)',
            r'filter\s+by\s+(.+?)(?:\s+filter|\s+condition|$)',
        ]
        
        for pattern in exclusion_patterns:
            match = re.search(pattern, query_lower)
            if match:
                condition_text = match.group(1).strip()
                # Try to identify column and value
                # This is simplified - real implementation would use NLP
                return {'text': condition_text}
        
        return None
    
    def add_column_to_intent(self, intent: Dict[str, Any], column_name: str, 
                            table_name: Optional[str] = None) -> Tuple[Dict[str, Any], List[str]]:
        """
        Add a column to the query intent.
        
        Args:
            intent: Current query intent
            column_name: Name of column to add
            table_name: Optional table name
        
        Returns:
            Tuple of (updated_intent, reasons)
        """
        reasons = []
        
        # Determine table if not provided
        if not table_name:
            base_table = intent.get('base_table', '')
            table_name = base_table
        
        # Check if column exists in metadata
        table_info = self.resolver.tables.get(table_name, {})
        columns = table_info.get('columns', [])
        
        # Find matching column
        matching_column = None
        for col in columns:
            col_name = col.get('name', '').lower()
            if column_name.lower() in col_name or col_name in column_name.lower():
                matching_column = col
                break
        
        if not matching_column:
            # Try to find in other tables
            for table_name_check, table_info_check in self.resolver.tables.items():
                columns_check = table_info_check.get('columns', [])
                for col in columns_check:
                    col_name = col.get('name', '').lower()
                    if column_name.lower() in col_name or col_name in column_name.lower():
                        matching_column = col
                        table_name = table_name_check
                        break
                if matching_column:
                    break
        
        if matching_column:
            # Add to columns list
            columns_list = intent.get('columns', [])
            if columns_list == ['*']:
                columns_list = []
            
            column_expr = f"{table_name}.{matching_column.get('name')}"
            if column_expr not in columns_list:
                columns_list.append(column_expr)
                intent['columns'] = columns_list
                reasons.append(f"Added column: {matching_column.get('name')} from table {table_name}")
            else:
                reasons.append(f"Column {matching_column.get('name')} already in query")
        else:
            reasons.append(f"Could not find column matching '{column_name}' in metadata")
        
        return intent, reasons
    
    def add_filter_to_intent(self, intent: Dict[str, Any], filter_text: str) -> Tuple[Dict[str, Any], List[str]]:
        """
        Add a filter to the query intent.
        
        Args:
            intent: Current query intent
            filter_text: Natural language filter description
        
        Returns:
            Tuple of (updated_intent, reasons)
        """
        reasons = []
        
        # This is simplified - in production, use LLM to parse filter_text into filter structure
        # For now, delegate to existing exclusion request handler
        from backend.sql_builder import SQLBuilder
        from backend.dimension_resolver import DimensionResolver
        
        # Create a minimal dimension resolver for SQL builder
        dimension_resolver = DimensionResolver({})
        sql_builder = SQLBuilder(self.resolver, dimension_resolver)
        
        # Use the exclusion request handler
        updated_intent, exclusion_reasons, clarification = sql_builder._apply_user_exclusion_request(
            intent, filter_text
        )
        
        if clarification:
            reasons.append(clarification)
        else:
            reasons.extend(exclusion_reasons)
            intent = updated_intent
        
        return intent, reasons
    
    def remove_filter_from_intent(self, intent: Dict[str, Any], filter_description: str) -> Tuple[Dict[str, Any], List[str]]:
        """
        Remove a filter from the query intent.
        
        Args:
            intent: Current query intent
            filter_description: Description of filter to remove
        
        Returns:
            Tuple of (updated_intent, reasons)
        """
        reasons = []
        filters = intent.get('filters', [])
        
        # Try to match filter description to existing filters
        filter_description_lower = filter_description.lower()
        
        removed_filters = []
        remaining_filters = []
        
        for filt in filters:
            col = filt.get('column', '').lower()
            reason = filt.get('reason', '').lower()
            
            # Check if this filter matches the description
            if (filter_description_lower in col or 
                filter_description_lower in reason or
                col in filter_description_lower):
                removed_filters.append(filt)
                reasons.append(f"Removed filter: {filt.get('column')}")
            else:
                remaining_filters.append(filt)
        
        if removed_filters:
            intent['filters'] = remaining_filters
        else:
            reasons.append(f"Could not find filter matching '{filter_description}'")
        
        return intent, reasons
    
    def modify_query(self, context: ConversationalContext, query_text: str) -> Tuple[Dict[str, Any], List[str], Optional[str]]:
        """
        Modify query based on natural language instruction.
        
        Args:
            context: Conversational context with current query
            query_text: Natural language modification instruction
        
        Returns:
            Tuple of (updated_intent, reasons, clarification_question)
        """
        if not context.current_intent:
            return None, ["No previous query to modify"], None
        
        intent = context.current_intent.copy()
        reasons = []
        clarification = None
        
        # Detect modification type
        mod_type = self.detect_modification_type(query_text)
        
        if mod_type == 'add_column':
            column_name = self.extract_column_name(query_text)
            if column_name:
                intent, add_reasons = self.add_column_to_intent(intent, column_name)
                reasons.extend(add_reasons)
            else:
                clarification = "Could not identify which column to add. Please specify the column name."
        
        elif mod_type == 'add_filter':
            filter_condition = self.extract_filter_condition(query_text)
            if filter_condition:
                intent, filter_reasons = self.add_filter_to_intent(intent, filter_condition.get('text', query_text))
                reasons.extend(filter_reasons)
            else:
                clarification = "Could not identify filter condition. Please specify what to filter."
        
        elif mod_type == 'remove_filter':
            filter_description = self.extract_filter_condition(query_text)
            if filter_description:
                intent, remove_reasons = self.remove_filter_from_intent(intent, filter_description.get('text', query_text))
                reasons.extend(remove_reasons)
            else:
                clarification = "Could not identify which filter to remove. Please specify."
        
        else:
            # Try to infer modification type
            # Check if it's an exclusion request
            if any(word in query_text.lower() for word in ['remove', 'exclude', 'excluding', 'without']):
                intent, filter_reasons, clarification = self.add_filter_to_intent(intent, query_text)
                reasons.extend(filter_reasons)
            else:
                clarification = f"Could not understand modification type. Please specify: add column, add filter, or remove filter."
        
        return intent, reasons, clarification

