#!/usr/bin/env python3
"""
Typed Query Intent Contract

This module defines the formal schema for SQL query intents,
ensuring type safety and preventing malformed intents.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum


class QueryType(Enum):
    """Type of query."""
    RELATIONAL = "relational"  # Returns individual records
    METRIC = "metric"  # Returns aggregated metrics


class JoinType(Enum):
    """Type of SQL JOIN."""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class FilterOperator(Enum):
    """Filter operators."""
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    LIKE = "LIKE"
    IN = "IN"


class OrderDirection(Enum):
    """Sort direction."""
    ASC = "ASC"
    DESC = "DESC"


@dataclass
class Attribute:
    """An attribute/column to select."""
    name: str
    table: str
    column: Optional[str] = None
    sql_expression: Optional[str] = None
    alias: Optional[str] = None


@dataclass
class Join:
    """A table join specification."""
    table: str
    join_type: JoinType = JoinType.LEFT
    on_clause: str = ""
    reason: str = ""
    cardinality_safe: bool = True  # Whether this join preserves cardinality
    relationship_type: Optional[str] = None  # "one_to_many", "many_to_one", "many_to_many"


@dataclass
class Filter:
    """A WHERE clause filter."""
    column: str
    table: str
    operator: FilterOperator
    value: Optional[Any] = None
    reason: str = ""


@dataclass
class Aggregation:
    """A metric aggregation."""
    name: str
    sql_expression: str
    function: str = "SUM"  # SUM, COUNT, AVG, MAX, MIN
    alias: Optional[str] = None


@dataclass
class OrderBy:
    """An ORDER BY specification."""
    column: str
    direction: OrderDirection = OrderDirection.ASC


@dataclass
class QueryIntent:
    """
    Formal query intent contract.
    
    This ensures all intents have:
    - An anchor entity (the main table)
    - Explicit attributes/columns
    - Valid joins
    - Proper filters
    """
    # Core intent
    query_type: QueryType
    anchor_entity: str  # The main table/entity
    base_table: str  # The actual table name
    
    # What to select
    attributes: List[Attribute] = field(default_factory=list)
    aggregations: Optional[List[Aggregation]] = None
    
    # How to join
    joins: List[Join] = field(default_factory=list)
    
    # How to filter
    filters: List[Filter] = field(default_factory=list)
    
    # How to group (for metric queries)
    group_by: List[str] = field(default_factory=list)
    
    # How to order
    order_by: Optional[List[OrderBy]] = None
    
    # Metadata
    reasoning: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryIntent':
        """Create QueryIntent from dictionary (for LLM output)."""
        # Convert query_type
        query_type_str = data.get('query_type', 'relational')
        query_type = QueryType.RELATIONAL if query_type_str == 'relational' else QueryType.METRIC
        
        # Parse attributes
        attributes = []
        columns = data.get('columns', [])
        for col in columns:
            if isinstance(col, str):
                # Simple column name
                attributes.append(Attribute(name=col, table=data.get('base_table', '')))
            elif isinstance(col, dict):
                attributes.append(Attribute(**col))
        
        # Parse joins
        joins = []
        for join_data in data.get('joins', []):
            join_type_str = join_data.get('type', 'LEFT').upper()
            join_type = JoinType.LEFT
            if join_type_str == 'INNER':
                join_type = JoinType.INNER
            elif join_type_str == 'RIGHT':
                join_type = JoinType.RIGHT
            elif join_type_str == 'FULL':
                join_type = JoinType.FULL
            
            joins.append(Join(
                table=join_data.get('table', ''),
                join_type=join_type,
                on_clause=join_data.get('on', ''),
                reason=join_data.get('reason', ''),
                cardinality_safe=join_data.get('cardinality_safe', True),
                relationship_type=join_data.get('relationship_type')
            ))
        
        # Parse filters
        filters = []
        for filter_data in data.get('filters', []):
            op_str = filter_data.get('operator', '=')
            operator = FilterOperator.EQUALS
            for op in FilterOperator:
                if op.value == op_str:
                    operator = op
                    break
            
            filters.append(Filter(
                column=filter_data.get('column', ''),
                table=filter_data.get('table', data.get('base_table', '')),
                operator=operator,
                value=filter_data.get('value'),
                reason=filter_data.get('reason', '')
            ))
        
        # Parse aggregations
        aggregations = None
        metric = data.get('metric')
        if metric:
            aggregations = [Aggregation(
                name=metric.get('name', 'value'),
                sql_expression=metric.get('sql_expression', ''),
                function=metric.get('function', 'SUM'),
                alias=f"total_{metric.get('name', 'value')}"
            )]
        
        # Parse order_by
        order_by = None
        if data.get('order_by'):
            order_by = []
            for order_data in data.get('order_by', []):
                dir_str = order_data.get('direction', 'ASC').upper()
                direction = OrderDirection.ASC if dir_str == 'ASC' else OrderDirection.DESC
                order_by.append(OrderBy(
                    column=order_data.get('column', ''),
                    direction=direction
                ))
        
        return cls(
            query_type=query_type,
            anchor_entity=data.get('anchor_entity', data.get('base_table', '')),
            base_table=data.get('base_table', ''),
            attributes=attributes,
            aggregations=aggregations,
            joins=joins,
            filters=filters,
            group_by=data.get('group_by', []),
            order_by=order_by,
            reasoning=data.get('reasoning', {})
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert QueryIntent back to dictionary."""
        return {
            'query_type': self.query_type.value,
            'anchor_entity': self.anchor_entity,
            'base_table': self.base_table,
            'columns': [attr.name for attr in self.attributes],
            'joins': [{
                'table': j.table,
                'type': j.join_type.value,
                'on': j.on_clause,
                'reason': j.reason,
                'cardinality_safe': j.cardinality_safe,
                'relationship_type': j.relationship_type
            } for j in self.joins],
            'filters': [{
                'column': f.column,
                'table': f.table,
                'operator': f.operator.value,
                'value': f.value,
                'reason': f.reason
            } for f in self.filters],
            'group_by': self.group_by,
            'order_by': [{
                'column': o.column,
                'direction': o.direction.value
            } for o in (self.order_by or [])],
            'metric': {
                'name': self.aggregations[0].name,
                'sql_expression': self.aggregations[0].sql_expression
            } if self.aggregations else None,
            'reasoning': self.reasoning
        }





