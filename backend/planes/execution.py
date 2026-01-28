"""
Execution Plane

Execute queries in sandboxed environment.
SLA: < 30s latency
Failure Mode: Timeout, return partial
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from datetime import datetime


@dataclass
class ExecutionResult:
    """Result from execution plane."""
    success: bool
    execution_id: str
    rows_returned: int = 0
    rows_scanned: int = 0
    duration_ms: float = 0.0
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    partial: bool = False
    warning: Optional[str] = None
    timestamp: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'success': self.success,
            'execution_id': self.execution_id,
            'rows_returned': self.rows_returned,
            'rows_scanned': self.rows_scanned,
            'duration_ms': self.duration_ms,
            'partial': self.partial,
            'timestamp': self.timestamp or datetime.utcnow().isoformat(),
        }
        if self.data:
            result['data'] = self.data
        if self.columns:
            result['columns'] = self.columns
        if self.error:
            result['error'] = self.error
        if self.error_code:
            result['error_code'] = self.error_code
        if self.warning:
            result['warning'] = self.warning
        return result


class ExecutionPlane:
    """Execute queries in sandboxed environment."""
    
    def __init__(self, sandbox=None, query_firewall=None, kill_switch=None, db_executor=None):
        """
        Initialize execution plane.
        
        Args:
            sandbox: Query sandbox instance
            query_firewall: Query firewall instance
            kill_switch: Kill switch instance
            db_executor: Database executor instance
        """
        self.sandbox = sandbox
        self.query_firewall = query_firewall
        self.kill_switch = kill_switch
        self.db_executor = db_executor
    
    def execute_query(self, sql: str, db_config: Dict[str, Any],
                     user_id: Optional[str] = None) -> ExecutionResult:
        """
        Execute query with full sandboxing.
        
        Args:
            sql: SQL query string
            db_config: Database configuration
            user_id: User ID for kill switch checks
        
        Returns:
            ExecutionResult
        """
        execution_id = self._generate_execution_id()
        start_time = datetime.utcnow()
        
        # Step 1: Check kill switch
        if self.kill_switch and user_id:
            if self.kill_switch.check_kill_switch(user_id, execution_id):
                return ExecutionResult(
                    success=False,
                    execution_id=execution_id,
                    error='Query execution blocked by kill switch',
                    error_code='KILL_SWITCH_ACTIVE',
                    timestamp=datetime.utcnow().isoformat()
                )
        
        # Step 2: Query firewall check
        if self.query_firewall:
            firewall_result = self.query_firewall.check_query(sql)
            if not firewall_result['allowed']:
                return ExecutionResult(
                    success=False,
                    execution_id=execution_id,
                    error=firewall_result.get('reason', 'Query blocked by firewall'),
                    error_code='FIREWALL_BLOCKED',
                    timestamp=datetime.utcnow().isoformat()
                )
        
        # Step 3: Sandbox query (rewrite for safety)
        if self.sandbox:
            try:
                safe_sql = self.sandbox.execute_sandboxed(sql)
            except Exception as e:
                return ExecutionResult(
                    success=False,
                    execution_id=execution_id,
                    error=f'Sandbox error: {str(e)}',
                    error_code='SANDBOX_ERROR',
                    timestamp=datetime.utcnow().isoformat()
                )
        else:
            safe_sql = sql
        
        # Step 4: Execute query
        if not self.db_executor:
            return ExecutionResult(
                success=False,
                execution_id=execution_id,
                error='Database executor not available',
                error_code='EXECUTOR_UNAVAILABLE',
                timestamp=datetime.utcnow().isoformat()
            )
        
        try:
            result = self.db_executor.execute_readonly(safe_sql)
            
            end_time = datetime.utcnow()
            duration_ms = (end_time - start_time).total_seconds() * 1000
            
            return ExecutionResult(
                success=True,
                execution_id=execution_id,
                rows_returned=result.get('rows_returned', 0),
                rows_scanned=result.get('rows_scanned', 0),
                duration_ms=duration_ms,
                data=result.get('data', []),
                columns=result.get('columns', []),
                partial=result.get('partial', False),
                warning=result.get('warning'),
                timestamp=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration_ms = (end_time - start_time).total_seconds() * 1000
            
            return ExecutionResult(
                success=False,
                execution_id=execution_id,
                duration_ms=duration_ms,
                error=str(e),
                error_code='EXECUTION_ERROR',
                timestamp=datetime.utcnow().isoformat()
            )
    
    def _generate_execution_id(self) -> str:
        """Generate unique execution ID."""
        import uuid
        return str(uuid.uuid4())

