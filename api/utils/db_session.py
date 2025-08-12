"""
Database session management for DuckDB analytical queries.

This module provides persistent database connection management and session-based
context storage for follow-up queries with proper cleanup and resource management.
"""

import duckdb
import logging
import time
import uuid
from typing import Dict, List, Any, Optional
from pathlib import Path
import threading
from contextlib import contextmanager
from datetime import datetime, date

logger = logging.getLogger(__name__)


def serialize_datetime_objects(obj):
    """
    Recursively convert datetime objects to ISO format strings for JSON serialization.
    
    Args:
        obj: Object that may contain datetime objects
        
    Returns:
        Object with datetime objects converted to strings
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_datetime_objects(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_datetime_objects(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(serialize_datetime_objects(item) for item in obj)
    else:
        return obj


class DuckDBSessionError(Exception):
    """Base exception for DuckDB session errors."""
    pass


class EnhancedDuckDBSession:
    """
    Manages persistent, robust database connections and context for analytical queries.
    
    This enhanced class provides:
    - Robust connection handling with health checks and retry logic
    - Connection persistence across requests within the same session
    - Automatic reconnection for dropped connections
    - Query execution with retry logic for failed database functions
    """
    
    def __init__(self, db_path: str, session_id: Optional[str] = None, retry_count: int = 3, connection_timeout: int = 30):
        """
        Initialize a new enhanced DuckDB session.
        
        Args:
            db_path: Path to the DuckDB database file
            session_id: Optional session identifier, generates UUID if not provided
            retry_count: Number of times to retry failed operations
            connection_timeout: Timeout for establishing a connection
        
        Raises:
            DuckDBSessionError: If database file doesn't exist or can't be accessed
        """
        self.db_path = Path(db_path)
        self.session_id = session_id or str(uuid.uuid4())
        self.connection = None
        self.schema_info = None
        self.query_history = []
        self.created_at = time.time()
        self.last_accessed = time.time()
        self.retry_count = retry_count
        self.connection_timeout = connection_timeout
        self._lock = threading.Lock()
        
        if not self.db_path.exists():
            raise DuckDBSessionError(f"Database file not found: {db_path}")
        
        if not self.db_path.is_file():
            raise DuckDBSessionError(f"Path is not a file: {db_path}")
        
        logger.info(f"Created Enhanced DuckDB session {self.session_id} for {db_path}")

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Get a robust database connection with health checks and retry logic.
        
        Returns:
            DuckDB connection object
            
        Raises:
            DuckDBSessionError: If connection cannot be established
        """
        with self._lock:
            for attempt in range(self.retry_count):
                try:
                    if self.connection and self.test_connection():
                        self.last_accessed = time.time()
                        return self.connection
                    
                    self.connection = duckdb.connect(str(self.db_path), read_only=True)
                    logger.info(f"Established new DuckDB connection for session {self.session_id}")
                    self.last_accessed = time.time()
                    return self.connection
                
                except Exception as e:
                    logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                    if attempt == self.retry_count - 1:
                        raise DuckDBSessionError(f"Failed to establish connection after {self.retry_count} attempts: {e}")
                    time.sleep(0.5 * (attempt + 1))
            
            raise DuckDBSessionError("Failed to get a database connection.")

    def test_connection(self) -> bool:
        """
        Test if the current database connection is healthy.
        
        Returns:
            True if the connection is healthy, False otherwise
        """
        if not self.connection:
            return False
        try:
            # Execute a simple query to test the connection
            self.connection.execute("SELECT 1")
            return True
        except Exception as e:
            logger.warning(f"Connection test failed for session {self.session_id}: {e}")
            return False

    def reset_connection(self):
        """
        Reset the database connection.
        """
        with self._lock:
            if self.connection:
                try:
                    self.connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection during reset: {e}")
                finally:
                    self.connection = None
            logger.info(f"Connection for session {self.session_id} has been reset.")
    
    def execute_query(self, sql: str, is_retry: bool = False) -> Dict[str, Any]:
        """
        Execute single query with error handling and result formatting.
        
        Args:
            sql: SQL query string to execute
            is_retry: Flag to indicate if this is a retry attempt
            
        Returns:
            Dictionary containing query results and metadata:
            {
                'sql': str,
                'success': bool,
                'data': List[Dict[str, Any]],
                'columns': List[str],
                'row_count': int,
                'execution_time_ms': float,
                'error': Optional[str]
            }
        """
        start_time = time.time()
        result = {
            'sql': sql,
            'success': False,
            'data': [],
            'columns': [],
            'row_count': 0,
            'execution_time_ms': 0.0,
            'error': None
        }
        
        try:
            conn = self.get_connection()
            
            # Execute query and fetch results
            query_result = conn.execute(sql)
            
            # Get column names
            if query_result.description:
                result['columns'] = [desc[0] for desc in query_result.description]
            
            # Fetch all rows
            rows = query_result.fetchall()
            
            # Convert to list of dictionaries with datetime serialization
            if rows and result['columns']:
                raw_data = [
                    dict(zip(result['columns'], row)) for row in rows
                ]
                # Serialize datetime objects to strings for JSON compatibility
                result['data'] = serialize_datetime_objects(raw_data)
            
            result['row_count'] = len(rows)
            result['success'] = True
            
            # Add to query history only on the first attempt
            if not is_retry:
                self.query_history.append({
                    'sql': sql,
                    'timestamp': time.time(),
                    'row_count': result['row_count'],
                    'success': True
                })
            
            logger.info(f"Query executed successfully in session {self.session_id}: {result['row_count']} rows")
            
        except Exception as e:
            error_msg = str(e)
            result['error'] = error_msg
            
            # Add failed query to history only on the first attempt
            if not is_retry:
                self.query_history.append({
                    'sql': sql,
                    'timestamp': time.time(),
                    'error': error_msg,
                    'success': False
                })
            
            logger.error(f"Query execution failed in session {self.session_id}: {error_msg}")
        
        finally:
            result['execution_time_ms'] = (time.time() - start_time) * 1000
        
        return result
    
    def execute_query_with_retry(self, sql: str) -> Dict[str, Any]:
        """
        Execute a query with retry logic for failed database functions.
        
        Args:
            sql: SQL query string to execute
            
        Returns:
            Dictionary containing query results and metadata
        """
        for attempt in range(self.retry_count):
            try:
                result = self.execute_query(sql, is_retry=(attempt > 0))
                
                # If successful or error is not a database function error, return result
                if result['success'] or "database function error" not in str(result.get('error', '')).lower():
                    return result
                
                logger.warning(f"Database function error on attempt {attempt + 1}, resetting connection and retrying.")
                self.reset_connection()
                
            except Exception as e:
                logger.error(f"Unexpected error during query execution with retry: {e}")
                if attempt == self.retry_count - 1:
                    return {
                        'sql': sql, 'success': False, 'error': str(e),
                        'data': [], 'columns': [], 'row_count': 0, 'execution_time_ms': 0.0
                    }
        
        # Fallback if all retries fail
        return {
            'sql': sql, 'success': False, 'error': 'All retry attempts failed for database function error.',
            'data': [], 'columns': [], 'row_count': 0, 'execution_time_ms': 0.0
        }

    def execute_queries(self, sql_queries: List[str]) -> List[Dict[str, Any]]:
        """
        Execute multiple queries in sequence with retry logic.
        
        Args:
            sql_queries: List of SQL query strings to execute
            
        Returns:
            List of query result dictionaries
        """
        results = []
        for sql in sql_queries:
            result = self.execute_query_with_retry(sql)
            results.append(result)
            
            # Stop execution if a query fails
            if not result['success']:
                logger.warning(f"Stopping query execution due to failure in session {self.session_id}")
                break
        
        return results
    
    def get_schema_info(self) -> Dict[str, Any]:
        """
        Get cached schema information or extract it from the database.
        
        Returns:
            Dictionary containing database schema information
        """
        if self.schema_info is None:
            try:
                conn = self.get_connection()
                
                # Get table names
                tables_result = conn.execute("SHOW TABLES").fetchall()
                table_names = [row[0] for row in tables_result]
                
                schema_info = {
                    'tables': table_names,
                    'schemas': {},
                    'row_counts': {},
                    'primary_table': table_names[0] if table_names else None
                }
                
                # Get detailed information for each table
                for table_name in table_names:
                    # Get column information
                    columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
                    columns = []
                    
                    for col_row in columns_result:
                        col_info = {
                            'name': col_row[0],
                            'type': col_row[1],
                            'nullable': col_row[2] == 'YES' if len(col_row) > 2 else True
                        }
                        columns.append(col_info)
                    
                    schema_info['schemas'][table_name] = columns
                    
                    # Get row count
                    try:
                        count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                        schema_info['row_counts'][table_name] = count_result[0] if count_result else 0
                    except Exception as e:
                        logger.warning(f"Could not get row count for table {table_name}: {e}")
                        schema_info['row_counts'][table_name] = 0
                
                self.schema_info = schema_info
                logger.info(f"Extracted schema info for session {self.session_id}: {len(table_names)} tables")
                
            except Exception as e:
                logger.error(f"Failed to extract schema info for session {self.session_id}: {e}")
                self.schema_info = {'tables': [], 'schemas': {}, 'row_counts': {}, 'primary_table': None}
        
        return self.schema_info
    
    def get_query_history(self) -> List[Dict[str, Any]]:
        """
        Get the query history for this session.
        
        Returns:
            List of query history entries
        """
        return self.query_history.copy()
    
    def get_session_info(self) -> Dict[str, Any]:
        """
        Get session information and statistics.
        
        Returns:
            Dictionary containing session metadata
        """
        return {
            'session_id': self.session_id,
            'db_path': str(self.db_path),
            'created_at': self.created_at,
            'last_accessed': self.last_accessed,
            'query_count': len(self.query_history),
            'successful_queries': sum(1 for q in self.query_history if q.get('success', False)),
            'failed_queries': sum(1 for q in self.query_history if not q.get('success', True)),
            'connected': self.connection is not None
        }
    
    def cleanup(self):
        """
        Clean up database connection and resources.
        """
        with self._lock:
            if self.connection:
                try:
                    self.connection.close()
                    logger.info(f"Closed database connection for session {self.session_id}")
                except Exception as e:
                    logger.error(f"Error closing connection for session {self.session_id}: {e}")
                finally:
                    self.connection = None
            
            # Clear cached data
            self.schema_info = None
            self.query_history.clear()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup()
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.cleanup()
        except Exception:
            pass  # Ignore errors during cleanup in destructor


class DuckDBSessionManager:
    """
    Manages multiple DuckDB sessions with automatic cleanup.
    """
    
    def __init__(self, max_sessions: int = 100, session_timeout: int = 3600):
        """
        Initialize session manager.
        
        Args:
            max_sessions: Maximum number of concurrent sessions
            session_timeout: Session timeout in seconds (default: 1 hour)
        """
        self.sessions: Dict[str, EnhancedDuckDBSession] = {}
        self.max_sessions = max_sessions
        self.session_timeout = session_timeout
        self._lock = threading.Lock()
        
        logger.info(f"Initialized DuckDB session manager (max_sessions={max_sessions}, timeout={session_timeout}s)")
    
    def create_session(self, db_path: str, session_id: Optional[str] = None) -> EnhancedDuckDBSession:
        """
        Create a new database session.
        
        Args:
            db_path: Path to the DuckDB database file
            session_id: Optional session identifier
            
        Returns:
            DuckDBSession instance
            
        Raises:
            DuckDBSessionError: If session cannot be created
        """
        with self._lock:
            # Clean up expired sessions
            self._cleanup_expired_sessions()
            
            # Check session limit
            if len(self.sessions) >= self.max_sessions:
                raise DuckDBSessionError(f"Maximum number of sessions ({self.max_sessions}) reached")
            
            # Create new session
            session = EnhancedDuckDBSession(db_path, session_id)
            self.sessions[session.session_id] = session
            
            logger.info(f"Created session {session.session_id} (total sessions: {len(self.sessions)})")
            return session
    
    def get_session(self, session_id: str) -> Optional[EnhancedDuckDBSession]:
        """
        Get an existing session by ID.
        
        Args:
            session_id: Session identifier
            
        Returns:
            DuckDBSession instance or None if not found
        """
        with self._lock:
            session = self.sessions.get(session_id)
            if session:
                # Check if session has expired
                if time.time() - session.last_accessed > self.session_timeout:
                    logger.info(f"Session {session_id} has expired, removing")
                    self._remove_session(session_id)
                    return None
                
                session.last_accessed = time.time()
            
            return session
    
    def remove_session(self, session_id: str) -> bool:
        """
        Remove a session by ID.
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if session was removed, False if not found
        """
        with self._lock:
            return self._remove_session(session_id)
    
    def _remove_session(self, session_id: str) -> bool:
        """
        Internal method to remove a session (assumes lock is held).
        """
        session = self.sessions.pop(session_id, None)
        if session:
            session.cleanup()
            logger.info(f"Removed session {session_id} (remaining sessions: {len(self.sessions)})")
            return True
        return False
    
    def _cleanup_expired_sessions(self):
        """
        Clean up expired sessions (assumes lock is held).
        """
        current_time = time.time()
        expired_sessions = [
            session_id for session_id, session in self.sessions.items()
            if current_time - session.last_accessed > self.session_timeout
        ]
        
        for session_id in expired_sessions:
            self._remove_session(session_id)
        
        if expired_sessions:
            logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
    
    def get_session_count(self) -> int:
        """
        Get the current number of active sessions.
        
        Returns:
            Number of active sessions
        """
        with self._lock:
            return len(self.sessions)
    
    def cleanup_all(self):
        """
        Clean up all sessions.
        """
        with self._lock:
            session_ids = list(self.sessions.keys())
            for session_id in session_ids:
                self._remove_session(session_id)
            
            logger.info("Cleaned up all sessions")


# Global session manager instance
session_manager = DuckDBSessionManager()


@contextmanager
def get_db_session(db_path: str, session_id: Optional[str] = None):
    """
    Context manager for database sessions.
    
    Args:
        db_path: Path to the DuckDB database file
        session_id: Optional session identifier
        
    Yields:
        DuckDBSession instance
        
    Example:
        with get_db_session('/path/to/db.duckdb') as session:
            result = session.execute_query('SELECT * FROM table')
    """
    session = None
    try:
        if session_id:
            session = session_manager.get_session(session_id)
        
        if session is None:
            session = session_manager.create_session(db_path, session_id)
        
        yield session
        
    finally:
        # Note: We don't automatically cleanup the session here
        # as it may be reused for follow-up queries
        pass
