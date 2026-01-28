#!/usr/bin/env python3
"""
Semantic Dimension Resolver

Resolves dimension names to their SQL expressions (physical columns or computed expressions).
This is the critical layer that enables computed dimensions with CASE statements.
"""

from typing import Dict, List, Any, Optional, Tuple
from enum import Enum


class DimensionType(Enum):
    """Type of dimension."""
    PHYSICAL = "physical"  # Actual column in table
    COMPUTED = "computed"  # SQL expression (CASE, function, etc.)


class ResolvedDimension:
    """A resolved dimension with its SQL expression."""
    
    def __init__(self, name: str, expression: str, dimension_type: DimensionType, 
                 base_table: str, alias: Optional[str] = None, groupable: bool = True,
                 filterable: bool = True):
        self.name = name
        self.expression = expression
        self.dimension_type = dimension_type
        self.base_table = base_table
        self.alias = alias or name
        self.groupable = groupable
        self.filterable = filterable
    
    def __repr__(self):
        return f"ResolvedDimension(name={self.name}, type={self.dimension_type.value}, expression={self.expression[:50]}...)"


class DimensionResolver:
    """
    Resolves dimension names to their SQL expressions.
    
    Supports both physical columns and computed dimensions (with CASE statements, functions, etc.).
    """
    
    def __init__(self, semantic_registry: Dict[str, Any]):
        """
        Initialize dimension resolver.
        
        Args:
            semantic_registry: Semantic registry containing dimension definitions
        """
        self.registry = semantic_registry
        self.dimensions = {dim.get('name'): dim for dim in semantic_registry.get('dimensions', [])}
    
    def resolve_dimension(self, dimension_name: str, join_aliases: Optional[Dict[str, str]] = None, 
                         base_alias: str = 't1') -> Optional[ResolvedDimension]:
        """
        Resolve a dimension name to its SQL expression.
        
        Args:
            dimension_name: Name of the dimension to resolve
            join_aliases: Mapping of table names to aliases
            base_alias: Default alias for base table
        
        Returns:
            ResolvedDimension or None if not found
        """
        dim_def = self.dimensions.get(dimension_name)
        if not dim_def:
            return None
        
        base_table = dim_def.get('base_table', '')
        
        # Check if dimension has SQL expression (computed)
        sql_expression = dim_def.get('sql_expression', '')
        if sql_expression:
            # Computed dimension
            expression = sql_expression
            
            # Replace table references with aliases if provided
            if join_aliases:
                expression = self._replace_table_names(expression, join_aliases, base_alias)
            
            return ResolvedDimension(
                name=dimension_name,
                expression=expression,
                dimension_type=DimensionType.COMPUTED,
                base_table=base_table,
                alias=dimension_name,
                groupable=True,  # CASE expressions are groupable
                filterable=True   # CASE expressions can be filtered (via CTE)
            )
        else:
            # Physical column dimension
            column = dim_def.get('column', dimension_name)
            table = dim_def.get('base_table', '')
            alias = join_aliases.get(table, base_alias) if join_aliases else base_alias
            
            expression = f"{alias}.{column}"
            
            return ResolvedDimension(
                name=dimension_name,
                expression=expression,
                dimension_type=DimensionType.PHYSICAL,
                base_table=base_table,
                alias=dimension_name,
                groupable=True,
                filterable=True
            )
    
    def resolve_dimensions(self, dimension_names: List[str], join_aliases: Optional[Dict[str, str]] = None,
                          base_alias: str = 't1') -> List[ResolvedDimension]:
        """
        Resolve multiple dimensions.
        
        Args:
            dimension_names: List of dimension names to resolve
            join_aliases: Mapping of table names to aliases
            base_alias: Default alias for base table
        
        Returns:
            List of ResolvedDimension objects
        """
        if dimension_names is None:
            return []
        resolved = []
        for dim_name in dimension_names:
            resolved_dim = self.resolve_dimension(dim_name, join_aliases, base_alias)
            if resolved_dim:
                resolved.append(resolved_dim)
        return resolved
    
    def _replace_table_names(self, expression: str, join_aliases: Dict[str, str], base_alias: str) -> str:
        """
        Replace table names in SQL expression with aliases.
        
        Args:
            expression: SQL expression
            join_aliases: Mapping of table names to aliases
            base_alias: Default alias for base table
        
        Returns:
            Expression with table names replaced by aliases
        """
        result = expression
        
        # Replace table.column with alias.column
        for table_name, alias in join_aliases.items():
            # Pattern: table_name.column or schema.table_name.column
            patterns = [
                f"{table_name}.",
                f"{table_name.lower()}.",
                f"{table_name.upper()}."
            ]
            for pattern in patterns:
                result = result.replace(pattern, f"{alias}.")
        
        return result
    
    def needs_cte(self, dimensions: List[ResolvedDimension]) -> bool:
        """
        Check if dimensions require a CTE (Common Table Expression).
        
        CTEs are needed when:
        - Multiple computed dimensions are used
        - Same expression appears in SELECT and GROUP BY
        - Complex expressions that shouldn't be repeated
        
        Args:
            dimensions: List of resolved dimensions
        
        Returns:
            True if CTE is needed
        """
        computed_count = sum(1 for d in dimensions if d.dimension_type == DimensionType.COMPUTED)
        
        # Use CTE if we have computed dimensions (to avoid repeating CASE expressions)
        return computed_count > 0
    
    def get_all_dimensions(self) -> Dict[str, Dict[str, Any]]:
        """Get all dimension definitions."""
        return self.dimensions.copy()

