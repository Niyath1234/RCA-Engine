"""
Join Type Resolver

Determines join type (LEFT, RIGHT, INNER) based on query intent, not metadata.
Join type is dynamic and depends on what data the query needs.
"""

from typing import Dict, Any, Optional, List
from enum import Enum


class JoinType(Enum):
    """SQL join types."""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class JoinTypeResolver:
    """Resolves join type based on query intent."""
    
    def __init__(self):
        """Initialize join type resolver."""
        pass
    
    def determine_join_type(
        self,
        from_table: str,
        to_table: str,
        relationship_type: str,
        query_intent: Dict[str, Any],
        is_required: bool = False,
        is_optional: bool = False
    ) -> JoinType:
        """
        Determine join type based on query intent and relationship.
        
        Args:
            from_table: Source table
            to_table: Target table
            relationship_type: Relationship type (one_to_one, one_to_many, etc.)
            query_intent: Query intent dictionary
            is_required: Whether the joined table is required for the query
            is_optional: Whether the joined table is optional
        
        Returns:
            JoinType enum
        """
        # Explicit user preference (highest priority)
        explicit_type = query_intent.get('join_type')
        if explicit_type:
            return JoinType[explicit_type.upper()]
        
        # Check if joined table columns are in SELECT
        select_columns = query_intent.get('select', [])
        to_table_columns = self._extract_table_columns(select_columns, to_table)
        
        # Check if joined table has filters
        filters = query_intent.get('filters', [])
        to_table_filters = self._extract_table_filters(filters, to_table)
        
        # Check if joined table is used in GROUP BY
        group_by = query_intent.get('group_by') or []
        to_table_in_group_by = any(
            col.startswith(f"{to_table}.") or col == to_table
            for col in group_by
        ) if group_by else False
        
        # Decision logic:
        
        # 1. If joined table has filters, use INNER (we need matching rows)
        if to_table_filters:
            return JoinType.INNER
        
        # 2. If joined table is in GROUP BY, use INNER (we need matching rows)
        if to_table_in_group_by:
            return JoinType.INNER
        
        # 3. If is_required flag is set, use INNER
        if is_required:
            return JoinType.INNER
        
        # 4. If is_optional flag is set, use LEFT
        if is_optional:
            return JoinType.LEFT
        
        # 5. If joined table columns are selected but no filters:
        #    - For one_to_one: LEFT (optional relationship)
        #    - For one_to_many: LEFT (may not have matches)
        #    - For many_to_one: INNER (fact table usually requires dimension)
        if to_table_columns:
            if relationship_type == 'many_to_one':
                # Fact → Dimension: Usually INNER (fact needs dimension)
                return JoinType.INNER
            elif relationship_type == 'one_to_many':
                # Dimension → Fact: LEFT (dimension may not have facts)
                return JoinType.LEFT
            elif relationship_type == 'one_to_one':
                # One-to-one: LEFT (optional relationship)
                return JoinType.LEFT
            else:
                # Default: LEFT for safety (preserve all rows from left table)
                return JoinType.LEFT
        
        # 6. If joined table is only used for filtering (not selected):
        #    Use INNER (we only want rows that match)
        if to_table_filters and not to_table_columns:
            return JoinType.INNER
        
        # 7. Default based on relationship type
        if relationship_type == 'many_to_one':
            # Fact → Dimension: Usually INNER
            return JoinType.INNER
        elif relationship_type == 'one_to_many':
            # Dimension → Fact: LEFT (preserve all dimensions)
            return JoinType.LEFT
        elif relationship_type == 'one_to_one':
            # One-to-one: LEFT (optional)
            return JoinType.LEFT
        else:
            # Default: LEFT (preserve all rows from left table)
            return JoinType.LEFT
    
    def _extract_table_columns(self, select_columns: List[str], table_name: str) -> List[str]:
        """Extract columns that belong to a specific table."""
        table_columns = []
        for col in select_columns:
            # Check if column references the table
            if '.' in col:
                table, column = col.split('.', 1)
                if table == table_name:
                    table_columns.append(column)
            # If no table prefix, check if it's a simple column name
            # (This is less reliable, but handles cases like "customer_name")
            elif table_name.lower() in col.lower():
                table_columns.append(col)
        return table_columns
    
    def _extract_table_filters(self, filters: List[str], table_name: str) -> List[str]:
        """Extract filters that reference a specific table."""
        table_filters = []
        for filter_expr in filters:
            # Check if filter references the table
            if f"{table_name}." in filter_expr:
                table_filters.append(filter_expr)
        return table_filters
    
    def determine_join_type_from_intent(
        self,
        base_table: str,
        join_table: str,
        relationship: Dict[str, Any],
        intent: Dict[str, Any]
    ) -> JoinType:
        """
        Determine join type from full query intent.
        
        Args:
            base_table: Base table (FROM clause)
            join_table: Table being joined
            relationship: Relationship dictionary with 'on', 'relationship_type', etc.
            intent: Full query intent
        
        Returns:
            JoinType enum
        """
        relationship_type = relationship.get('relationship_type', 'one_to_many')
        
        # Check if this join is required based on intent
        is_required = self._is_join_required(base_table, join_table, intent)
        is_optional = self._is_join_optional(base_table, join_table, intent)
        
        return self.determine_join_type(
            from_table=base_table,
            to_table=join_table,
            relationship_type=relationship_type,
            query_intent=intent,
            is_required=is_required,
            is_optional=is_optional
        )
    
    def _is_join_required(self, base_table: str, join_table: str, intent: Dict[str, Any]) -> bool:
        """Check if join is required (must have matching rows)."""
        # Check filters on joined table
        filters = intent.get('filters', [])
        for filter_expr in filters:
            if f"{join_table}." in filter_expr:
                # If filter uses IS NOT NULL or comparison operators, it's required
                if 'IS NOT NULL' in filter_expr.upper() or any(op in filter_expr for op in ['=', '>', '<', '!=']):
                    return True
        
        # Check if joined table is in GROUP BY
        group_by = intent.get('group_by') or []
        if group_by and any(f"{join_table}." in col or col == join_table for col in group_by):
            return True
        
        return False
    
    def _is_join_optional(self, base_table: str, join_table: str, intent: Dict[str, Any]) -> bool:
        """Check if join is optional (can have NULLs)."""
        # If columns from joined table are selected but no filters, it's optional
        select_columns = intent.get('select', [])
        join_table_columns = self._extract_table_columns(select_columns, join_table)
        
        if join_table_columns:
            # Check if there are filters
            filters = intent.get('filters', [])
            join_table_filters = self._extract_table_filters(filters, join_table)
            
            # If columns selected but no filters, it's optional
            if not join_table_filters:
                return True
        
        return False

