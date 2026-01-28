"""
Database Executor Implementations

Concrete implementations of DatabaseExecutor interface.
"""

import time
from typing import Dict, Any, Optional, List
from backend.interfaces import DatabaseExecutor


class PostgreSQLExecutor(DatabaseExecutor):
    """PostgreSQL database executor."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL executor.
        
        Args:
            config: Database configuration dictionary
        """
        try:
            import psycopg2
            from psycopg2 import pool
        except ImportError:
            raise ImportError("psycopg2-binary is required for PostgreSQL support")
        
        self.config = config
        self.pool = None
        self._create_pool()
    
    def _create_pool(self):
        """Create connection pool."""
        import psycopg2
        from psycopg2 import pool
        
        pool_size = self.config.get('pool_size', 10)
        
        self.pool = psycopg2.pool.SimpleConnectionPool(
            1,
            pool_size,
            host=self.config['host'],
            port=self.config.get('port', 5432),
            database=self.config['database'],
            user=self.config['user'],
            password=self.config.get('password', ''),
            connect_timeout=self.config.get('timeout', 30),
        )
        
        if not self.pool:
            raise RuntimeError("Failed to create PostgreSQL connection pool")
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute SQL query.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
        
        Returns:
            Query result dictionary
        """
        return self.execute_readonly(sql, params)
    
    def execute_readonly(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute read-only query (enforced).
        
        Args:
            sql: SQL query string
            params: Optional query parameters
        
        Returns:
            Query result dictionary
        """
        conn = self.pool.getconn()
        start_time = time.time()
        
        try:
            # Set to read-only
            conn.set_session(readonly=True, autocommit=False)
            
            # Set statement timeout
            timeout_ms = self.config.get('query_timeout', 30) * 1000
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {timeout_ms}")
            
            # Execute query
            with conn.cursor() as cur:
                cur.execute(sql, params)
                
                # Get column names
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                else:
                    columns = []
                
                # Fetch results
                data = cur.fetchall()
                
                # Convert to list of dictionaries
                rows = [dict(zip(columns, row)) for row in data] if columns else []
                
                duration_ms = (time.time() - start_time) * 1000
                
                return {
                    'data': rows,
                    'columns': columns,
                    'rows_returned': len(rows),
                    'rows_scanned': cur.rowcount if cur.rowcount >= 0 else len(rows),
                    'duration_ms': duration_ms,
                }
                
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            raise RuntimeError(f"Query execution failed: {str(e)}") from e
        finally:
            self.pool.putconn(conn)
    
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get table schema.
        
        Args:
            table_name: Table name
        
        Returns:
            Schema dictionary
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                # Get column information
                cur.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = []
                for row in cur.fetchall():
                    columns.append({
                        'name': row[0],
                        'type': row[1],
                        'nullable': row[2] == 'YES',
                        'default': row[3],
                    })
                
                return {
                    'table_name': table_name,
                    'columns': columns,
                }
        finally:
            self.pool.putconn(conn)
    
    def list_tables(self) -> List[str]:
        """
        List all tables.
        
        Returns:
            List of table names
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                
                return [row[0] for row in cur.fetchall()]
        finally:
            self.pool.putconn(conn)
    
    def validate_query(self, sql: str) -> bool:
        """
        Validate query syntax.
        
        Args:
            sql: SQL query string
        
        Returns:
            True if valid, False otherwise
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                # Use EXPLAIN to validate without executing
                cur.execute(f"EXPLAIN {sql}")
                return True
        except Exception:
            return False
        finally:
            self.pool.putconn(conn)
    
    def estimate_cost(self, sql: str) -> Dict[str, Any]:
        """
        Estimate query execution cost.
        
        Args:
            sql: SQL query string
        
        Returns:
            Cost estimate dictionary
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                # Use EXPLAIN ANALYZE to get cost estimate
                cur.execute(f"EXPLAIN ANALYZE {sql}")
                explain_output = '\n'.join([row[0] for row in cur.fetchall()])
                
                # Parse cost from explain output
                # This is a simplified version - in production, parse properly
                cost_match = None
                for line in explain_output.split('\n'):
                    if 'cost=' in line.lower():
                        # Extract cost range
                        import re
                        match = re.search(r'cost=([\d.]+)\.\.([\d.]+)', line)
                        if match:
                            cost_match = {
                                'startup_cost': float(match.group(1)),
                                'total_cost': float(match.group(2)),
                            }
                            break
                
                return {
                    'estimated_cost': cost_match,
                    'explain_output': explain_output,
                }
        finally:
            self.pool.putconn(conn)


class MySQLExecutor(DatabaseExecutor):
    """MySQL database executor."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MySQL executor.
        
        Args:
            config: Database configuration dictionary
        """
        try:
            import pymysql
        except ImportError:
            raise ImportError("pymysql is required for MySQL support")
        
        self.config = config
        self.pool = None
        self._create_pool()
    
    def _create_pool(self):
        """Create connection pool."""
        import pymysql
        from pymysql import cursors
        
        # Simple connection for now - in production, use proper pooling
        self.connection_config = {
            'host': self.config['host'],
            'port': self.config.get('port', 3306),
            'user': self.config['user'],
            'password': self.config.get('password', ''),
            'database': self.config['database'],
            'charset': 'utf8mb4',
            'cursorclass': cursors.DictCursor,
            'read_timeout': self.config.get('timeout', 30),
        }
    
    def _get_connection(self):
        """Get database connection."""
        import pymysql
        return pymysql.connect(**self.connection_config)
    
    def execute_readonly(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute read-only query."""
        conn = self._get_connection()
        start_time = time.time()
        
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                data = cur.fetchall()
                
                # Get column names
                columns = list(data[0].keys()) if data else []
                
                duration_ms = (time.time() - start_time) * 1000
                
                return {
                    'data': data,
                    'columns': columns,
                    'rows_returned': len(data),
                    'rows_scanned': cur.rowcount if cur.rowcount >= 0 else len(data),
                    'duration_ms': duration_ms,
                }
        finally:
            conn.close()
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute query."""
        return self.execute_readonly(sql, params)
    
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DESCRIBE {table_name}")
                columns = []
                for row in cur.fetchall():
                    columns.append({
                        'name': row['Field'],
                        'type': row['Type'],
                        'nullable': row['Null'] == 'YES',
                        'default': row['Default'],
                    })
                return {
                    'table_name': table_name,
                    'columns': columns,
                }
        finally:
            conn.close()
    
    def list_tables(self) -> List[str]:
        """List all tables."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                return [list(row.values())[0] for row in cur.fetchall()]
        finally:
            conn.close()
    
    def validate_query(self, sql: str) -> bool:
        """Validate query syntax."""
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN {sql}")
                return True
        except Exception:
            return False
        finally:
            if 'conn' in locals():
                conn.close()
    
    def estimate_cost(self, sql: str) -> Dict[str, Any]:
        """Estimate query cost."""
        return {'estimated_cost': None, 'explain_output': ''}


class SQLiteExecutor(DatabaseExecutor):
    """SQLite database executor."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQLite executor.
        
        Args:
            config: Database configuration dictionary
        """
        import sqlite3
        self.config = config
        self.db_path = config.get('database') or config.get('path', ':memory:')
    
    def _get_connection(self):
        """Get database connection."""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        return conn
    
    def execute_readonly(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute read-only query."""
        conn = self._get_connection()
        start_time = time.time()
        
        try:
            cur = conn.cursor()
            cur.execute(sql, params if params else [])
            rows = cur.fetchall()
            
            # Get column names
            columns = [description[0] for description in cur.description] if cur.description else []
            
            # Convert to list of dictionaries
            data = [dict(zip(columns, row)) for row in rows] if columns else []
            
            duration_ms = (time.time() - start_time) * 1000
            
            return {
                'data': data,
                'columns': columns,
                'rows_returned': len(data),
                'rows_scanned': len(data),
                'duration_ms': duration_ms,
            }
        finally:
            conn.close()
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute query."""
        return self.execute_readonly(sql, params)
    
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema."""
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table_name})")
            columns = []
            for row in cur.fetchall():
                columns.append({
                    'name': row[1],
                    'type': row[2],
                    'nullable': not row[3],
                    'default': row[4],
                })
            return {
                'table_name': table_name,
                'columns': columns,
            }
        finally:
            conn.close()
    
    def list_tables(self) -> List[str]:
        """List all tables."""
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
    
    def validate_query(self, sql: str) -> bool:
        """Validate query syntax."""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(f"EXPLAIN QUERY PLAN {sql}")
            return True
        except Exception:
            return False
        finally:
            if 'conn' in locals():
                conn.close()
    
    def estimate_cost(self, sql: str) -> Dict[str, Any]:
        """Estimate query cost."""
        return {'estimated_cost': None, 'explain_output': ''}

