import pytest
import os
from unittest.mock import Mock, patch
import pandas as pd
from redshift_connector import Connection, Cursor
from aws_connector.redshift import RedshiftConfig, RedConn
from aws_connector.exceptions import ConnectionError, QueryError

# Test data
TEST_CONFIG = {
    'host': 'test-cluster.redshift.amazonaws.com',
    'username': 'testuser',
    'password': 'testpass',
    'database': 'testdb',
    'port': 5439,
    'timeout': 30,
    'ssl': True,
    'max_retries': 3,
    'retry_delay': 5
}

@pytest.fixture
def mock_connection():
    """Fixture to create a mock Redshift connection"""
    mock_conn = Mock(spec=Connection)
    mock_cursor = Mock(spec=Cursor)
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn

@pytest.fixture
def mock_cursor():
    """Fixture to create a mock Redshift cursor"""
    mock_cursor = Mock(spec=Cursor)
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    return mock_cursor

class TestRedshiftConfig:
    def test_init_with_direct_values(self):
        """Test initialization with direct values"""
        config = RedshiftConfig(**TEST_CONFIG)
        assert config.host == TEST_CONFIG['host']
        assert config.username == TEST_CONFIG['username']
        assert config.password == TEST_CONFIG['password']
        assert config.database == TEST_CONFIG['database']
        assert config.port == TEST_CONFIG['port']
        assert config.timeout == TEST_CONFIG['timeout']
        assert config.ssl == TEST_CONFIG['ssl']
        assert config.max_retries == TEST_CONFIG['max_retries']
        assert config.retry_delay == TEST_CONFIG['retry_delay']

    def test_init_with_defaults(self):
        """Test initialization with default values"""
        config = RedshiftConfig(
            host='test-host',
            username='test-user',
            password='test-pass',
            database='test-db'
        )
        assert config.port == 5439
        assert config.timeout == 30
        assert config.ssl is True
        assert config.max_retries == 3
        assert config.retry_delay == 5

    def test_from_env(self):
        """Test initialization from environment variables"""
        env_vars = {
            'REDSHIFT_HOST': 'env-host',
            'REDSHIFT_USERNAME': 'env-user',
            'REDSHIFT_PASSWORD': 'env-pass',
            'REDSHIFT_DATABASE': 'env-db',
            'REDSHIFT_PORT': '5438',
            'REDSHIFT_TIMEOUT': '20',
            'REDSHIFT_SSL': 'false',
            'REDSHIFT_MAX_RETRIES': '5',
            'REDSHIFT_RETRY_DELAY': '10'
        }
        
        with patch.dict(os.environ, env_vars):
            config = RedshiftConfig.from_env()
            assert config.host == 'env-host'
            assert config.username == 'env-user'
            assert config.password == 'env-pass'
            assert config.database == 'env-db'
            assert config.port == 5438
            assert config.timeout == 20
            assert config.ssl is False
            assert config.max_retries == 5
            assert config.retry_delay == 10

    def test_validate_valid_config(self):
        """Test validation of valid configuration"""
        config = RedshiftConfig(**TEST_CONFIG)
        config.validate()  # Should not raise any exception

    @pytest.mark.parametrize(
        "invalid_field,invalid_value,expected_error",
        [
            ('host', '', "Host cannot be empty"),
            ('username', '', "Username cannot be empty"),
            ('password', '', "Password cannot be empty"),
            ('database', '', "Database cannot be empty"),
            ('port', 0, "Port must be a positive number"),
            ('timeout', 0, "Timeout must be a positive number"),
            ('max_retries', -1, "Max retries cannot be negative"),
            ('retry_delay', -1, "Retry delay cannot be negative"),
        ]
    )
    def test_validate_invalid_config(self, invalid_field, invalid_value, expected_error):
        """Test validation of invalid configurations"""
        config_dict = TEST_CONFIG.copy()
        config_dict[invalid_field] = invalid_value
        config = RedshiftConfig(**config_dict)
        
        with pytest.raises(ValueError) as exc_info:
            config.validate()
        assert str(exc_info.value) == expected_error

class TestRedConn:
    @pytest.fixture
    def red_conn(self):
        """Fixture to create a RedConn instance"""
        return RedConn(**TEST_CONFIG)

    def test_init(self, red_conn):
        """Test initialization of RedConn"""
        assert red_conn.config.host == TEST_CONFIG['host']
        assert red_conn.config.username == TEST_CONFIG['username']
        assert red_conn.config.password == TEST_CONFIG['password']
        assert red_conn.config.database == TEST_CONFIG['database']
        assert red_conn.conn is None

    @patch('aws_connector.redshift.redshift_connector.connect')
    def test_connect_success(self, mock_connect, red_conn, mock_connection):
        """Test successful connection"""
        mock_connect.return_value = mock_connection
        conn = red_conn.connect()
        assert conn == mock_connection
        assert red_conn.conn == mock_connection
        mock_connect.assert_called_once()

    @patch('aws_connector.redshift.redshift_connector.connect')
    def test_connect_retry(self, mock_connect, red_conn, mock_connection):
        """Test connection with retry"""
        mock_connect.side_effect = [Exception("Connection failed"), mock_connection]
        conn = red_conn.connect()
        assert conn == mock_connection
        assert mock_connect.call_count == 2

    @patch('aws_connector.redshift.redshift_connector.connect')
    def test_connect_failure(self, mock_connect, red_conn):
        """Test connection failure after all retries"""
        mock_connect.side_effect = Exception("Connection failed")
        with pytest.raises(ConnectionError) as exc_info:
            red_conn.connect()
        assert "Failed to connect after" in str(exc_info.value)

    def test_context_manager(self, red_conn, mock_connection):
        """Test context manager functionality"""
        with patch.object(red_conn, '_get_connection', return_value=mock_connection):
            with red_conn as conn:
                assert conn == red_conn
                assert red_conn.conn == mock_connection
            assert red_conn.conn is None

    def test_close(self, red_conn, mock_connection):
        """Test connection close"""
        red_conn.conn = mock_connection
        red_conn.close()
        assert red_conn.conn is None
        mock_connection.close.assert_called_once()

    @patch('pandas.read_sql')
    def test_fetch_success(self, mock_read_sql, red_conn, mock_connection):
        """Test successful query execution"""
        expected_df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_read_sql.return_value = expected_df
        red_conn.conn = mock_connection
        
        result = red_conn.fetch("SELECT * FROM test_table")
        assert result.equals(expected_df)
        mock_read_sql.assert_called_once()

    @patch('pandas.read_sql')
    def test_fetch_with_chunksize(self, mock_read_sql, red_conn, mock_connection):
        """Test query execution with chunksize"""
        expected_dfs = [
            pd.DataFrame({'col1': [1, 2]}),
            pd.DataFrame({'col1': [3, 4]})
        ]
        mock_read_sql.return_value = expected_dfs
        red_conn.conn = mock_connection
        
        result = red_conn.fetch("SELECT * FROM test_table", chunksize=2)
        assert list(result) == expected_dfs
        mock_read_sql.assert_called_once()

    @patch('aws_connector.redshift.redshift_connector.connect')
    def test_fetch_no_connection(self, mock_connect, red_conn):
        """Test query execution without connection"""
        mock_connect.side_effect = Exception("Connection failed")
        with pytest.raises(ConnectionError) as exc_info:
            red_conn.fetch("SELECT * FROM test_table")
        assert "Failed to connect after" in str(exc_info.value)

    @patch('pandas.read_sql')
    def test_fetch_error(self, mock_read_sql, red_conn, mock_connection):
        """Test query execution with error"""
        mock_read_sql.side_effect = Exception("Query failed")
        red_conn.conn = mock_connection
        
        with pytest.raises(QueryError) as exc_info:
            red_conn.fetch("SELECT * FROM test_table")
        assert "Error executing query" in str(exc_info.value)

    def test_execute_statements_sequential(self, red_conn, mock_connection, mock_cursor):
        """Test sequential statement execution"""
        red_conn.conn = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        statements = [
            "CREATE TABLE test1 (id INT)",
            "CREATE TABLE test2 (id INT)"
        ]
        
        results = red_conn.execute_statements(statements)
        assert len(results) == 2
        assert all(r['success'] for r in results)
        assert mock_cursor.execute.call_count == 2
        mock_cursor.__enter__.assert_called()
        mock_cursor.__exit__.assert_called()

    def test_execute_statements_parallel(self, red_conn):
        """Test parallel statement execution"""
        with patch.object(red_conn, '_execute_single_statement') as mock_execute:
            mock_execute.return_value = {
                'success': True,
                'statement': 'test',
                'duration': 0.1
            }
            
            statements = [
                "CREATE TABLE test1 (id INT)",
                "CREATE TABLE test2 (id INT)"
            ]
            
            results = red_conn.execute_statements(statements, parallel=True)
            assert len(results) == 2
            assert all(r['success'] for r in results)
            assert mock_execute.call_count == 2

    def test_execute_statements_error(self, red_conn, mock_connection, mock_cursor):
        """Test statement execution with error"""
        red_conn.conn = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Execution failed")
        
        results = red_conn.execute_statements("CREATE TABLE test (id INT)")
        assert len(results) == 1
        assert not results[0]['success']
        assert "Execution failed" in results[0]['error']
        mock_cursor.__enter__.assert_called()
        mock_cursor.__exit__.assert_called() 

    def test_echo_property(self, red_conn):
        """Test echo property functionality"""
        red_conn.echo = True
        assert red_conn.echo is True
        
        mock_conn = Mock()
        with patch('aws_connector.redshift.redshift_connector.connect', return_value=mock_conn), \
             patch('pandas.read_sql', return_value=pd.DataFrame()), \
             patch('aws_connector.redshift.logger.info') as mock_logger:
            red_conn.fetch("SELECT * FROM test_table", echo=True)
            mock_logger.assert_called()

    def test_connection_property(self, red_conn, mock_connection):
        """Test connection property"""
        red_conn.conn = mock_connection
        assert red_conn.connection == mock_connection
        
        red_conn.conn = None
        with pytest.raises(ConnectionError):
            _ = red_conn.connection

    def test_cursor_property(self, red_conn, mock_connection, mock_cursor):
        """Test cursor property"""
        red_conn.conn = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        assert red_conn.cursor == mock_cursor
        
        red_conn.conn = None
        with pytest.raises(ConnectionError):
            _ = red_conn.cursor 