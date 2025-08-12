import unittest
from unittest.mock import patch, MagicMock, call
import time
import os
import duckdb
from api.utils.db_session import EnhancedDuckDBSession, DuckDBSessionManager, DuckDBSessionError

class TestEnhancedDuckDBSession(unittest.TestCase):

    def setUp(self):
        # Create a dummy database file for testing
        self.db_path = "test_db.duckdb"
        conn = duckdb.connect(self.db_path)
        conn.execute("CREATE TABLE test_table (id INTEGER, value VARCHAR);")
        conn.execute("INSERT INTO test_table VALUES (1, 'test');")
        conn.close()

    def tearDown(self):
        # Remove the dummy database file
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_successful_connection(self):
        """Test that a connection is successfully established."""
        session = EnhancedDuckDBSession(self.db_path)
        conn = session.get_connection()
        self.assertIsNotNone(conn)
        session.cleanup()

    @patch('api.utils.db_session.duckdb.connect')
    def test_connection_retry_logic(self, mock_connect):
        """Test the connection retry logic."""
        # Simulate connection failure on the first two attempts
        mock_connect.side_effect = [
            duckdb.OperationalError("Connection failed"),
            duckdb.OperationalError("Connection failed again"),
            MagicMock()  # Successful connection on the third attempt
        ]
        
        session = EnhancedDuckDBSession(self.db_path, retry_count=3)
        
        with patch('api.utils.db_session.time.sleep') as mock_sleep:
            conn = session.get_connection()
            self.assertIsNotNone(conn)
            self.assertEqual(mock_connect.call_count, 3)
            mock_sleep.assert_has_calls([call(0.5), call(1.0)])

    @patch('api.utils.db_session.duckdb.connect')
    def test_connection_failure_after_retries(self, mock_connect):
        """Test that a DuckDBSessionError is raised after all retries fail."""
        mock_connect.side_effect = duckdb.OperationalError("Persistent connection failure")
        
        session = EnhancedDuckDBSession(self.db_path, retry_count=3)
        
        with self.assertRaises(DuckDBSessionError):
            session.get_connection()
        self.assertEqual(mock_connect.call_count, 3)

    def test_execute_query_with_retry_success(self):
        """Test successful query execution with the retry mechanism."""
        session = EnhancedDuckDBSession(self.db_path)
        result = session.execute_query_with_retry("SELECT * FROM test_table;")
        self.assertTrue(result['success'])
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['id'], 1)

    @patch.object(EnhancedDuckDBSession, 'execute_query')
    def test_execute_query_with_database_function_error_retry(self, mock_execute_query):
        """Test retry logic on a 'database function error'."""
        # Simulate a database function error on the first call, then success
        mock_execute_query.side_effect = [
            {'success': False, 'error': 'database function error: something went wrong'},
            {'success': True, 'data': [{'id': 1, 'value': 'test'}], 'columns': ['id', 'value'], 'row_count': 1}
        ]
        
        session = EnhancedDuckDBSession(self.db_path, retry_count=2)
        
        with patch.object(session, 'reset_connection') as mock_reset:
            result = session.execute_query_with_retry("SELECT * FROM test_table;")
            self.assertTrue(result['success'])
            self.assertEqual(mock_execute_query.call_count, 2)
            mock_reset.assert_called_once()

if __name__ == '__main__':
    unittest.main()
