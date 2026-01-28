"""
Query Builder

Build SQL queries from intent and schema.
"""

from typing import Dict, Any, List, Optional


class QueryBuilder:
    """Build SQL queries from intent and schema."""
    
    def build_skeleton(self, intent: Dict[str, Any], schema: Dict[str, Any],
                      metrics: List[str]) -> Dict[str, Any]:
        """
        Build query skeleton.
        
        Args:
            intent: Intent dictionary
            schema: Schema dictionary
            metrics: List of metrics
        
        Returns:
            Query skeleton dictionary
        """
        # Determine tables from schema
        tables = schema.get('tables', [])
        if isinstance(tables, list) and tables:
            if isinstance(tables[0], dict):
                table_names = [t.get('name') for t in tables]
            else:
                table_names = tables
        else:
            table_names = []
        
        # Determine columns
        columns = metrics or schema.get('columns', [])
        
        # Extract filters from intent
        filters = intent.get('filters', [])
        
        # Extract aggregation
        aggregation = intent.get('aggregation')
        
        # Extract dimensions (GROUP BY)
        dimensions = intent.get('dimensions', [])
        
        return {
            'tables': table_names,
            'columns': columns,
            'filters': filters,
            'aggregations': aggregation,
            'dimensions': dimensions,
            'time_range': intent.get('time_range'),
        }
    
    def build_sql(self, skeleton: Dict[str, Any], schema: Dict[str, Any]) -> str:
        """
        Build SQL from skeleton.
        
        Args:
            skeleton: Query skeleton dictionary
            schema: Schema dictionary
        
        Returns:
            SQL query string
        """
        tables = skeleton.get('tables', [])
        columns = skeleton.get('columns', [])
        filters = skeleton.get('filters', [])
        aggregation = skeleton.get('aggregations')
        dimensions = skeleton.get('dimensions', [])
        time_range = skeleton.get('time_range')
        
        if not tables:
            raise ValueError("No tables specified in skeleton")
        
        # Build SELECT clause
        if aggregation and columns:
            if aggregation == 'sum':
                select_clause = f"SELECT SUM({columns[0]}) as total_{columns[0]}"
            elif aggregation == 'avg':
                select_clause = f"SELECT AVG({columns[0]}) as avg_{columns[0]}"
            elif aggregation == 'count':
                select_clause = f"SELECT COUNT(*) as count"
            elif aggregation == 'max':
                select_clause = f"SELECT MAX({columns[0]}) as max_{columns[0]}"
            elif aggregation == 'min':
                select_clause = f"SELECT MIN({columns[0]}) as min_{columns[0]}"
            else:
                select_clause = f"SELECT {', '.join(columns)}"
        else:
            if columns:
                select_clause = f"SELECT {', '.join(columns)}"
            else:
                select_clause = "SELECT *"
        
        # Build FROM clause
        from_clause = f"FROM {tables[0]}"
        
        # Build WHERE clause
        where_parts = []
        
        # Add filters
        if filters:
            where_parts.extend(filters)
        
        # Add time range filter
        if time_range:
            time_filter = self._build_time_filter(time_range, schema)
            if time_filter:
                where_parts.append(time_filter)
        
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        
        # Build GROUP BY clause
        group_by_clause = ""
        if dimensions:
            group_by_clause = f"GROUP BY {', '.join(dimensions)}"
        elif aggregation and not dimensions:
            # If aggregation but no dimensions, group by all non-aggregated columns
            pass
        
        # Build ORDER BY clause
        order_by_clause = "ORDER BY 1"  # Default ordering
        
        # Build LIMIT clause
        limit_clause = "LIMIT 1000"  # Default limit
        
        # Combine SQL
        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)
        
        sql = ' '.join(sql_parts)
        
        return sql
    
    def _build_time_filter(self, time_range: str, schema: Dict[str, Any]) -> Optional[str]:
        """
        Build time filter from time range.
        
        Args:
            time_range: Time range string
            schema: Schema dictionary
        
        Returns:
            SQL WHERE clause for time filter
        """
        # Find date/time column in schema
        date_columns = []
        for table in schema.get('tables', []):
            if isinstance(table, dict):
                for col in table.get('columns', []):
                    col_type = col.get('type', '').lower()
                    if 'date' in col_type or 'time' in col_type or 'timestamp' in col_type:
                        date_columns.append(f"{table.get('name')}.{col.get('name')}")
        
        if not date_columns:
            return None
        
        date_column = date_columns[0]
        
        # Parse time range
        import re
        from datetime import datetime, timedelta
        
        if 'last' in time_range.lower():
            # Extract number and unit
            match = re.search(r'last\s+(\d+)\s+(\w+)', time_range.lower())
            if match:
                number = int(match.group(1))
                unit = match.group(2)
                
                if 'day' in unit:
                    delta = timedelta(days=number)
                elif 'week' in unit:
                    delta = timedelta(weeks=number)
                elif 'month' in unit:
                    delta = timedelta(days=number * 30)
                else:
                    return None
                
                cutoff_date = datetime.now() - delta
                return f"{date_column} >= '{cutoff_date.strftime('%Y-%m-%d')}'"
        
        elif 'to' in time_range:
            # Date range
            dates = time_range.split(' to ')
            if len(dates) == 2:
                return f"{date_column} BETWEEN '{dates[0].strip()}' AND '{dates[1].strip()}'"
        
        return None

