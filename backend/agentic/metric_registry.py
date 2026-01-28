#!/usr/bin/env python3
"""
Metric Registry - Ground Truth (Non-LLM)

This is the single source of truth for all metrics.
The LLM may reason, but it may never decide correctness alone.
"""

from typing import Dict, List, Optional, Any
import json
from pathlib import Path


class MetricRegistry:
    """
    Metric Registry - Hard truth, no LLM involvement.
    
    This registry contains canonical metric definitions that cannot be
    invented or hallucinated by LLMs.
    """
    
    def __init__(self, registry_data: Optional[Dict[str, Any]] = None, 
                 registry_file: Optional[Path] = None):
        """
        Initialize metric registry.
        
        Args:
            registry_data: Dictionary with metrics (from semantic_registry.json)
            registry_file: Path to semantic_registry.json file
        """
        self.metrics: Dict[str, Dict[str, Any]] = {}
        
        if registry_file:
            with open(registry_file, 'r') as f:
                registry_data = json.load(f)
        
        if registry_data:
            self._load_from_data(registry_data)
    
    def _load_from_data(self, data: Dict[str, Any]):
        """Load metrics from semantic registry data."""
        metrics_list = data.get('metrics', [])
        
        for metric in metrics_list:
            name = metric.get('name', '').lower()
            if not name:
                continue
            
            # Handle product-specific metrics (e.g., current_pos for khatabook)
            product_specific = metric.get('product_specific', {})
            
            if product_specific:
                # Create separate entries for each product variant
                # Use consistent naming: always use name_product format
                for product, product_def in product_specific.items():
                    product_name = f"{name}_{product}"
                    sql_expr = product_def.get('sql_expression', '')
                    
                    self.metrics[product_name] = {
                        'canonical_name': name,  # Keep original canonical name
                        'sql': sql_expr,
                        'required_columns': self._extract_required_columns(sql_expr),
                        'default_table': product_def.get('base_table', ''),
                        'description': metric.get('description', ''),
                        'aggregation': metric.get('aggregation', 'sum'),
                        'allowed_dimensions': metric.get('allowed_dimensions', []),
                        'required_filters': metric.get('required_filters', []),
                        'aliases': self._get_aliases(name, metric),
                        'product': product  # Track which product this is for
                    }
                
                # Also create main entry that points to product-specific
                self.metrics[name] = {
                    'canonical_name': name,
                    'sql': metric.get('sql_expression', 'varies_by_product'),
                    'required_columns': [],
                    'default_table': metric.get('base_table', 'varies_by_product'),
                    'description': metric.get('description', ''),
                    'aggregation': metric.get('aggregation', 'sum'),
                    'allowed_dimensions': metric.get('allowed_dimensions', []),
                    'required_filters': metric.get('required_filters', []),
                    'aliases': self._get_aliases(name, metric),
                    'product_specific': True  # Flag that this has product variants
                }
            else:
                # Standard metric
                sql_expr = metric.get('sql_expression', '')
                self.metrics[name] = {
                    'canonical_name': name,
                    'sql': sql_expr,
                    'required_columns': self._extract_required_columns(sql_expr),
                    'default_table': metric.get('base_table', ''),
                    'description': metric.get('description', ''),
                    'aggregation': metric.get('aggregation', 'sum'),
                    'allowed_dimensions': metric.get('allowed_dimensions', []),
                    'required_filters': metric.get('required_filters', []),
                    'aliases': self._get_aliases(name, metric)
                }
    
    def _extract_required_columns(self, sql_expression: str) -> List[str]:
        """
        Extract required columns from SQL expression.
        Simple pattern matching - assumes table.column or column format.
        """
        import re
        columns = []
        
        # Pattern: table.column or column
        # More specific: look for column names in CAST() or function calls
        patterns = [
            r'CAST\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS',  # CAST(column AS
            r'CAST\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s+AS',  # CAST(table.column AS
            r'([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)',  # table.column
            r'\b([a-zA-Z_][a-zA-Z0-9_]+)\b'  # standalone column (simple heuristic)
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql_expression, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    # For CAST(table.column), take column
                    if len(match) >= 2 and match[1]:
                        col = match[1]
                    else:
                        col = match[0] if match[0] else ''
                else:
                    col = match
                
                if not col:
                    continue
                
                # Filter out SQL keywords (but keep column names that contain keywords)
                sql_keywords = {'sum', 'count', 'avg', 'max', 'min', 'cast', 'abs', 'round', 
                               'double', 'as', 'when', 'then', 'else', 'end', 'case', 'date',
                               'trunc', 'month', 'date_trunc'}
                
                # Special handling: extract balance columns even if they contain "balance"
                if 'balance' in sql_expression.lower():
                    balance_match = re.search(r'([a-zA-Z_][a-zA-Z0-9_]*balance)', sql_expression, re.IGNORECASE)
                    if balance_match:
                        balance_col = balance_match.group(1)
                        if balance_col.lower() not in sql_keywords:
                            col = balance_col
                
                if col.lower() not in sql_keywords and col not in columns:
                    columns.append(col)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in columns:
            if col.lower() not in seen:
                seen.add(col.lower())
                unique_columns.append(col)
        
        return unique_columns
    
    def _get_aliases(self, name: str, metric: Dict[str, Any]) -> List[str]:
        """Get aliases for a metric (e.g., TOS -> current_pos)."""
        aliases = []
        
        # Common aliases
        alias_map = {
            'current_pos': ['tos', 'total_outstanding', 'outstanding'],
            'tos': ['current_pos', 'total_outstanding', 'outstanding'],
            'pos': ['principal_outstanding', 'principal'],
        }
        
        if name in alias_map:
            aliases.extend(alias_map[name])
        
        # Add description-based aliases
        description = metric.get('description', '').lower()
        if 'total outstanding' in description:
            aliases.append('tos')
        if 'principal outstanding' in description:
            aliases.append('pos')
        
        return list(set(aliases))  # Remove duplicates
    
    def resolve(self, metric_name: str, product_hint: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Resolve metric by name or alias.
        
        Args:
            metric_name: Metric name to resolve
            product_hint: Optional product hint (e.g., 'khatabook') for product-specific metrics
        
        Returns:
            Metric definition if found, None otherwise
        """
        metric_name_lower = metric_name.lower().strip()
        
        # If product hint provided, try product-specific first
        if product_hint:
            product_hint_lower = product_hint.lower()
            
            # First, look for product-specific variant directly
            for key, val in self.metrics.items():
                if val.get('canonical_name') == metric_name_lower and val.get('product') == product_hint_lower:
                    return val.copy()
            
            # Also try just the canonical name with product context
            if metric_name_lower in self.metrics:
                metric_def = self.metrics[metric_name_lower]
                if metric_def.get('product_specific'):
                    # Look for product-specific variant
                    for key, val in self.metrics.items():
                        if val.get('canonical_name') == metric_name_lower and val.get('product') == product_hint_lower:
                            return val.copy()
        
        # Direct match
        if metric_name_lower in self.metrics:
            metric_def = self.metrics[metric_name_lower].copy()
            # If it's product-specific and we have a hint, prefer product variant
            if metric_def.get('product_specific') and product_hint:
                product_hint_lower = product_hint.lower()
                for key, val in self.metrics.items():
                    if val.get('canonical_name') == metric_name_lower and val.get('product') == product_hint_lower:
                        return val.copy()
            return metric_def
        
        # Alias match - check if metric_name is an alias for any canonical name
        for key, metric_def in self.metrics.items():
            canonical_name = metric_def.get('canonical_name', key)
            aliases = metric_def.get('aliases', [])
            
            # Check if metric_name matches this canonical name or its aliases
            if metric_name_lower == canonical_name.lower() or metric_name_lower in [a.lower() for a in aliases]:
                # If we have a product hint, look for product-specific variant
                if product_hint:
                    product_hint_lower = product_hint.lower()
                    # Look for product-specific variant
                    product_key = f"{canonical_name}_{product_hint_lower}"
                    if product_key in self.metrics:
                        return self.metrics[product_key].copy()
                
                # Return the matched metric (or product-specific if found)
                return metric_def.copy()
        
        return None
    
    def list_all(self) -> List[str]:
        """List all registered metric names."""
        return list(self.metrics.keys())
    
    def get_metric(self, name: str) -> Optional[Dict[str, Any]]:
        """Get metric by canonical name."""
        return self.metrics.get(name.lower())
    
    def search(self, query: str) -> List[Dict[str, Any]]:
        """
        Search metrics by name, alias, or description.
        
        Returns:
            List of matching metric definitions
        """
        query_lower = query.lower()
        results = []
        
        for name, metric in self.metrics.items():
            # Check name
            if query_lower in name:
                results.append(metric.copy())
                continue
            
            # Check aliases
            if any(query_lower in alias.lower() for alias in metric.get('aliases', [])):
                results.append(metric.copy())
                continue
            
            # Check description
            if query_lower in metric.get('description', '').lower():
                results.append(metric.copy())
                continue
        
        return results

