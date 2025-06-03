import pandas as pd
import redshift_connector
from redshift_connector import Connection
from typing import Union, List, Dict, Optional, Any, Generator
import time
from concurrent.futures import ThreadPoolExecutor
import logging
from dataclasses import dataclass
import os
import uuid
from .exceptions import (
    AWSConnectorError,
    RedshiftError,
    ConnectionError,
    QueryError,
    CredentialError
)

# Configure module logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

@dataclass
class RedshiftConfig:
    """Configuration for Redshift connection.
    
    This class can be initialized with direct values or from environment variables.
    Environment variables take precedence over direct values.
    
    Environment Variables:
        REDSHIFT_HOST: The hostname or endpoint of the Redshift cluster
        REDSHIFT_USERNAME: The username for authentication
        REDSHIFT_PASSWORD: The password for authentication
        REDSHIFT_DATABASE: The name of the database to connect to
        REDSHIFT_PORT: The port number for the Redshift cluster
        REDSHIFT_TIMEOUT: Connection timeout in seconds
        REDSHIFT_SSL: Whether to use SSL for the connection
        REDSHIFT_MAX_RETRIES: Maximum number of connection retries
        REDSHIFT_RETRY_DELAY: Delay between retries in seconds
    
    Examples:
        ```python
        # Direct initialization
        config = RedshiftConfig(
            host="my-cluster.xxxxx.region.redshift.amazonaws.com",
            username="admin",
            password="secret",
            database="mydb"
        )
        
        # From environment variables
        config = RedshiftConfig.from_env()
        
        # Mixed initialization
        config = RedshiftConfig(host="my-cluster").from_env()
        ```
    """
    host: str
    username: str
    password: str
    database: str
    port: int = 5439
    timeout: int = 30
    ssl: bool = True
    max_retries: int = 3
    retry_delay: int = 5
    
    @classmethod
    def from_env(cls) -> 'RedshiftConfig':
        """Create a configuration from environment variables.
        
        Returns:
            RedshiftConfig: A new configuration instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        return cls(
            host=os.environ.get('REDSHIFT_HOST', ''),
            username=os.environ.get('REDSHIFT_USERNAME', ''),
            password=os.environ.get('REDSHIFT_PASSWORD', ''),
            database=os.environ.get('REDSHIFT_DATABASE', ''),
            port=int(os.environ.get('REDSHIFT_PORT', '5439')),
            timeout=int(os.environ.get('REDSHIFT_TIMEOUT', '30')),
            ssl=os.environ.get('REDSHIFT_SSL', 'true').lower() == 'true',
            max_retries=int(os.environ.get('REDSHIFT_MAX_RETRIES', '3')),
            retry_delay=int(os.environ.get('REDSHIFT_RETRY_DELAY', '5'))
        )
    
    def validate(self) -> None:
        """Validate the configuration parameters.
        
        Raises:
            ValueError: If any required parameters are missing or invalid
        """
        if not self.host:
            raise ValueError("Host cannot be empty")
        if not self.username:
            raise ValueError("Username cannot be empty")
        if not self.password:
            raise ValueError("Password cannot be empty")
        if not self.database:
            raise ValueError("Database cannot be empty")
        if self.port <= 0:
            raise ValueError("Port must be a positive number")
        if self.timeout <= 0:
            raise ValueError("Timeout must be a positive number")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.retry_delay < 0:
            raise ValueError("Retry delay cannot be negative")

class RedConn:
    """
    A class to handle Redshift database connections and operations.
    Implements context manager protocol for safe resource management.
    
    Testing:
        For testing purposes, you can override the following methods:
        - _get_connection(): Override to return a mock connection
        - _get_cursor(): Override to return a mock cursor
        
        Example:
            ```python
            class MockRedConn(RedConn):
                def _get_connection(self):
                    return MockConnection()
            ```
    """
    
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        database: str,
        port: int = 5439,
        timeout: int = 30,
        ssl: bool = True,
        max_retries: int = 3,
        retry_delay: int = 5
    ):
        """
        Initialize the RedConn class with database credentials.

        Args:
            host (str): The hostname or endpoint of the Redshift cluster
            username (str): The username for authentication
            password (str): The password for authentication
            database (str): The name of the database to connect to
            port (int): The port number for the Redshift cluster (default: 5439)
            timeout (int): Connection timeout in seconds (default: 30)
            ssl (bool): Whether to use SSL for the connection (default: True)
            max_retries (int): Maximum number of connection retries (default: 3)
            retry_delay (int): Delay between retries in seconds (default: 5)
        """
        self.config = RedshiftConfig(
            host=host,
            username=username,
            password=password,
            database=database,
            port=port,
            timeout=timeout,
            ssl=ssl,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        self.conn: Optional[Connection] = None
        self.echo: bool = False
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate the configuration parameters"""
        if not self.config.host:
            raise ValueError("Host cannot be empty")
        if not self.config.username:
            raise ValueError("Username cannot be empty")
        if not self.config.password:
            raise ValueError("Password cannot be empty")
        if not self.config.database:
            raise ValueError("Database cannot be empty")
        if self.config.port <= 0:
            raise ValueError("Port must be a positive number")
        if self.config.timeout <= 0:
            raise ValueError("Timeout must be a positive number")
        if self.config.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.config.retry_delay < 0:
            raise ValueError("Retry delay cannot be negative")

    def _get_connection(self) -> Connection:
        """Get a database connection. Override this method for testing.
        
        Returns:
            Connection: A database connection instance
            
        Raises:
            ConnectionError: If there's an error establishing the connection
        """
        return redshift_connector.connect(
            host=self.config.host,
            user=self.config.username,
            password=self.config.password,
            database=self.config.database,
            port=self.config.port,
            timeout=self.config.timeout,
            ssl=self.config.ssl
        )
    
    def _get_cursor(self) -> redshift_connector.Cursor:
        """Get a database cursor. Override this method for testing.
        
        Returns:
            redshift_connector.Cursor: A database cursor instance
            
        Raises:
            ConnectionError: If there's no active connection
        """
        if self.conn is None:
            raise ConnectionError("No active database connection")
        return self.conn.cursor()
    
    @property
    def connection(self) -> Connection:
        """Get the current database connection.
        
        Returns:
            Connection: The current database connection
            
        Raises:
            ConnectionError: If there's no active connection
        """
        if self.conn is None:
            raise ConnectionError("No active database connection")
        return self.conn
    
    @property
    def cursor(self) -> redshift_connector.Cursor:
        """Get the current database cursor.
        
        Returns:
            redshift_connector.Cursor: The current database cursor
            
        Raises:
            ConnectionError: If there's no active connection
        """
        return self._get_cursor()

    def connect(self) -> Connection:
        """
        Establishes a connection to a Redshift database using credentials.
        Implements retry logic for transient failures.

        Returns:
            Connection: A connection object to the Redshift database

        Raises:
            ConnectionError: If there's an error establishing the connection after retries
        """
        for attempt in range(self.config.max_retries):
            try:
                self.conn = self._get_connection()
                return self.conn
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise ConnectionError(f"Failed to connect after {self.config.max_retries} attempts: {str(e)}")
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.config.retry_delay)

    def __enter__(self) -> 'RedConn':
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.close()

    def close(self) -> None:
        """
        Closes the database connection if it exists.
        """
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.conn = None

    def fetch(
        self,
        query: str,
        echo: Optional[bool] = None,
        chunksize: Optional[int] = None
    ) -> Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]:
        """
        Executes a SQL query using the connection and returns the results.

        Args:
            query (str): The SQL query to be executed
            echo (Optional[bool]): Whether to print the query before execution
            chunksize (Optional[int]): Size of chunks to read data in. If None, reads all data at once.
                                     When specified, returns a generator of DataFrames.

        Returns:
            Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]: 
                If chunksize is None, returns the result as a DataFrame.
                If chunksize is specified, returns a generator of DataFrames.

        Raises:
            QueryError: If there's an error executing the query
            ConnectionError: If there's an error with the database connection
        """
        if self.conn is None:
            self.connect()

        query = query.replace(";", "")
        echo = echo if echo is not None else self.echo

        if echo:
            logger.info(f"Executing query: {query}")

        try:
            if chunksize:
                return pd.read_sql(sql=query, con=self.conn, chunksize=chunksize)
            else:
                return pd.read_sql(sql=query, con=self.conn)
        
        except Exception as e:
            raise QueryError(f"Error executing query: {str(e)}")

    def execute_statements(
        self, 
        statements: Union[str, List[str]], 
        parallel: bool = False,
        echo: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Executes one or more SQL statements in Redshift and returns the results.

        Args:
            statements (Union[str, List[str]]): A single SQL statement or list of SQL statements
            parallel (bool): If True, executes statements in parallel using separate connections.
                           If False, executes statements sequentially using the same connection.
            echo (Optional[bool]): Whether to print the query before execution

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing execution results.
            Each dictionary has the following keys:
                - 'success' (bool): Whether the statement executed successfully
                - 'statement' (str): The SQL statement that was executed
                - 'error' (str): Error message if execution failed (only present if success is False)
                - 'duration' (float): Time taken to execute the statement in seconds
        """
        if isinstance(statements, str):
            statements = [statements]

        start_time = time.time()
        echo = echo if echo is not None else self.echo

        if not parallel:
            if self.conn is None:
                self.connect()

            results = []
            for statement in statements:
                statement_start_time = time.time()
                with self.conn.cursor() as connection:
                    try:
                        connection.execute(statement)
                        self.conn.commit()  # Explicitly commit the transaction
                        results.append({
                            "success": True, 
                            "statement": statement,
                            "duration": time.time() - statement_start_time
                        })
                        if echo:
                            logger.info(f"Success: Statement = {statement}, Duration = {time.time() - statement_start_time} seconds")
                    except Exception as e:
                        results.append({
                            "success": False, 
                            "statement": statement, 
                            "error": str(e),
                            "duration": time.time() - statement_start_time
                        })
                        if echo:
                            logger.error(f"Failed: Statement = {statement}, Duration = {time.time() - statement_start_time} seconds")
        else:
            with ThreadPoolExecutor(max_workers=len(statements)) as executor:
                results = list(executor.map(self._execute_single_statement, statements))

        end_time = time.time()
        if echo:
            logger.info(f"Total time taken to execute {len(statements)} statements: {end_time - start_time} seconds")
        return results

    def _execute_single_statement(self, stmt: str) -> Dict[str, Any]:
        """
        Execute a single statement in its own connection.

        Args:
            stmt (str): The SQL statement to execute

        Returns:
            Dict[str, Any]: Dictionary containing execution results with keys:
                - 'success' (bool): Whether the statement executed successfully
                - 'statement' (str): The SQL statement that was executed
                - 'error' (str): Error message if execution failed (only present if success is False)
                - 'duration' (float): Time taken to execute the statement in seconds
        """
        statement_start_time = time.time()
        
        # Create a new connection for this thread
        conn = self._get_connection()

        try:
            cursor = conn.cursor()
            cursor.execute(stmt)
            conn.commit()
            result = {
                "statement": stmt,
                "success": True,
                "duration": time.time() - statement_start_time
            }
            if self.echo:
                logger.info(f"Success: Statement = {stmt}, Duration = {time.time() - statement_start_time} seconds")
            return result

        except Exception as e:
            result = {
                "statement": stmt,
                "success": False,
                "error": str(e),
                "duration": time.time() - statement_start_time
            }
            if self.echo:
                logger.error(f"Failed: Statement = {stmt}, Duration = {time.time() - statement_start_time} seconds")
            return result
        
        finally:
            cursor.close()
            conn.close()
