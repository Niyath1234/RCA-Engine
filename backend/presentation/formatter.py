"""
Result Formatter

Format query results for different output types.
"""

import csv
import io
from typing import Dict, Any, List, Optional


class ResultFormatter:
    """Format query results."""
    
    def format(self, execution_result: Dict[str, Any], format_type: str = 'json') -> Any:
        """
        Format execution result.
        
        Args:
            execution_result: Execution result dictionary
            format_type: Output format ('json', 'csv', 'table', 'markdown')
        
        Returns:
            Formatted result
        """
        if format_type == 'json':
            return self.format_json(execution_result)
        elif format_type == 'csv':
            return self.format_csv(execution_result)
        elif format_type == 'table':
            return self.format_table(execution_result)
        elif format_type == 'markdown':
            return self.format_markdown(execution_result)
        else:
            return execution_result
    
    def format_json(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Format as JSON."""
        return {
            'success': execution_result.get('success', True),
            'data': execution_result.get('data', []),
            'columns': execution_result.get('columns', []),
            'rows_returned': execution_result.get('rows_returned', 0),
            'rows_scanned': execution_result.get('rows_scanned', 0),
            'duration_ms': execution_result.get('duration_ms', 0),
            'partial': execution_result.get('partial', False),
        }
    
    def format_csv(self, execution_result: Dict[str, Any]) -> str:
        """Format as CSV."""
        data = execution_result.get('data', [])
        columns = execution_result.get('columns', [])
        
        if not data:
            return ''
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
        
        return output.getvalue()
    
    def format_table(self, execution_result: Dict[str, Any]) -> str:
        """Format as human-readable table."""
        data = execution_result.get('data', [])
        columns = execution_result.get('columns', [])
        
        if not data:
            return "No data returned."
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            col_widths[col] = max(
                len(str(col)),
                max((len(str(row.get(col, ''))) for row in data), default=0)
            )
        
        # Build table
        lines = []
        
        # Header
        header = ' | '.join(str(col).ljust(col_widths[col]) for col in columns)
        lines.append(header)
        lines.append('-' * len(header))
        
        # Rows
        for row in data:
            row_str = ' | '.join(str(row.get(col, '')).ljust(col_widths[col]) for col in columns)
            lines.append(row_str)
        
        return '\n'.join(lines)
    
    def format_markdown(self, execution_result: Dict[str, Any]) -> str:
        """Format as Markdown table."""
        data = execution_result.get('data', [])
        columns = execution_result.get('columns', [])
        
        if not data:
            return "No data returned."
        
        lines = []
        
        # Header
        header = '| ' + ' | '.join(columns) + ' |'
        lines.append(header)
        
        # Separator
        separator = '| ' + ' | '.join(['---'] * len(columns)) + ' |'
        lines.append(separator)
        
        # Rows
        for row in data:
            row_str = '| ' + ' | '.join(str(row.get(col, '')) for col in columns) + ' |'
            lines.append(row_str)
        
        return '\n'.join(lines)

