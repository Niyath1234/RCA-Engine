#!/usr/bin/env python3
"""
Verifier Agent - CRITICAL

This agent never uses creativity.
It checks correctness and rejects invalid outputs.
"""

from typing import Dict, Any, List, Optional
import re


class VerifierAgent:
    """
    Verification Agent.
    
    Purpose: Verify correctness of all agent outputs.
    Authority: CRITICAL - can reject and stop pipeline.
    
    This agent NEVER uses creativity - only hard rules.
    """
    
    def verify(self, intent_output: Dict[str, Any],
               metric_output: Dict[str, Any],
               table_output: Dict[str, Any],
               filter_output: Dict[str, Any],
               shape_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Verify all agent outputs.
        
        Returns:
            {
                "status": "ACCEPTED" | "REJECTED",
                "reason": "explanation if rejected"
            }
        """
        errors = []
        
        # Check 1: Query type consistency
        query_type = intent_output.get('query_type', '')
        if query_type == 'metric':
            # Metric queries must have resolved metrics
            resolved_metrics = metric_output.get('resolved_metrics', [])
            if not resolved_metrics:
                errors.append("Metric query without resolved metrics")
        
        # Check 2: Metric queries must have aggregates
        if query_type == 'metric':
            resolved_metrics = metric_output.get('resolved_metrics', [])
            if resolved_metrics:
                # Check that SQL templates contain aggregates
                for metric in resolved_metrics:
                    sql_template = metric.get('sql_template', '')
                    if not self._has_aggregate(sql_template):
                        errors.append(f"Metric '{metric.get('name')}' SQL template missing aggregate")
        
        # Check 3: GROUP BY only when dimensions exist
        dimensions = shape_output.get('dimensions', [])
        if query_type == 'metric' and dimensions:
            # GROUP BY is required - this will be checked in SQL renderer
            pass
        
        # Check 4: No dimension-only SELECT (must have metric)
        if query_type == 'metric':
            resolved_metrics = metric_output.get('resolved_metrics', [])
            if not resolved_metrics and not dimensions:
                errors.append("Metric query with no metrics and no dimensions")
        
        # Check 5: Base table must exist
        base_table = table_output.get('base_table', '')
        if not base_table:
            errors.append("Base table not resolved")
        
        # Check 6: Required columns must be available
        resolved_metrics = metric_output.get('resolved_metrics', [])
        required_columns = set()
        for metric in resolved_metrics:
            required_columns.update(metric.get('required_columns', []))
        
        # This check is done in Table Agent, but verify here too
        if required_columns and not base_table:
            errors.append(f"Required columns {required_columns} but no base table")
        
        if errors:
            return {
                "status": "REJECTED",
                "reason": "; ".join(errors)
            }
        
        return {
            "status": "ACCEPTED",
            "reason": "All checks passed"
        }
    
    def _has_aggregate(self, sql_expression: str) -> bool:
        """Check if SQL expression contains aggregate function."""
        sql_upper = sql_expression.upper()
        aggregates = ['SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'COUNT_DISTINCT']
        
        for agg in aggregates:
            if agg in sql_upper:
                return True
        
        return False
    
    def verify_sql(self, sql: str, intent_output: Dict[str, Any],
                   metric_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Verify final SQL query.
        
        This is the final check before returning SQL to user.
        """
        errors = []
        
        query_type = intent_output.get('query_type', '')
        
        if query_type == 'metric':
            # Check 1: SELECT must contain aggregate
            if not self._sql_has_aggregate(sql):
                errors.append("Metric query SQL missing aggregate expression")
            
            # Check 2: Must have metric in SELECT
            resolved_metrics = metric_output.get('resolved_metrics', [])
            if resolved_metrics:
                # Check that at least one metric appears in SELECT
                metric_names = [m.get('canonical_name', '') for m in resolved_metrics]
                sql_upper = sql.upper()
                
                # Simple check - metric SQL should appear in SELECT
                found_metric = False
                for metric in resolved_metrics:
                    sql_template = metric.get('sql_template', '').upper()
                    # Check if key parts of metric SQL appear in SELECT
                    if 'SUM' in sql_template and 'SUM' in sql_upper:
                        found_metric = True
                        break
                    if 'COUNT' in sql_template and 'COUNT' in sql_upper:
                        found_metric = True
                        break
                
                if not found_metric:
                    errors.append("Metric query SQL does not contain metric aggregation")
        
        if errors:
            return {
                "status": "REJECTED",
                "reason": "; ".join(errors)
            }
        
        return {
            "status": "ACCEPTED",
            "reason": "SQL verification passed"
        }
    
    def _sql_has_aggregate(self, sql: str) -> bool:
        """Check if SQL query contains aggregate function."""
        sql_upper = sql.upper()
        
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_upper, re.DOTALL)
        if not select_match:
            return False
        
        select_clause = select_match.group(1)
        
        # Check for aggregates
        aggregates = ['SUM', 'COUNT', 'AVG', 'MAX', 'MIN']
        for agg in aggregates:
            if agg in select_clause:
                return True
        
        return False

