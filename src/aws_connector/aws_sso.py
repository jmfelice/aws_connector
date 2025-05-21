import subprocess
from typing import Optional
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
from dataclasses import dataclass
import os
import time
from .exceptions import (
    AWSConnectorError,
    CredentialError,
    AuthenticationError
)
from .utils import setup_logging

# Configure logging
logger = setup_logging(__name__)

@dataclass
class SSOConfig:
    """
    Configuration for AWS SSO authentication.
    
    This class can be initialized with direct values or from environment variables.
    Environment variables take precedence over direct values.
    
    Environment Variables:
        AWS_EXEC_FILE_PATH: Path to the AWS CLI executable
        AWS_CREDENTIALS_DB_PATH: Path to the credentials database
        AWS_SSO_REFRESH_WINDOW: Hours between credential refreshes
        AWS_SSO_MAX_RETRIES: Maximum number of authentication retries
        AWS_SSO_RETRY_DELAY: Delay between retries in seconds
    
    Examples:
        ```python
        # Default configuration
        config = SSOConfig()  # Uses all default values
        
        # Custom configuration
        config = SSOConfig(
            aws_exec_file_path="/custom/path/to/aws",
            db_path=Path("./custom/path/credentials.db"),
            refresh_window_hours=12,  # Refresh every 12 hours
            max_retries=5,  # More retries
            retry_delay=10  # Longer delay between retries
        )
        
        # From environment variables
        config = SSOConfig.from_env()
        ```
    """
    aws_exec_file_path: str = r'C:\Program Files\Amazon\AWSCLIV2\aws.exe'
    db_path: Path = Path("./data/aws_credentials.db")
    refresh_window_hours: int = 6
    max_retries: int = 3
    retry_delay: int = 5
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self.validate()
    
    @classmethod
    def from_env(cls) -> 'SSOConfig':
        """Create a configuration from environment variables.
        
        Returns:
            SSOConfig: A new configuration instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        return cls(
            aws_exec_file_path=os.environ.get('AWS_EXEC_FILE_PATH', r'C:\Program Files\Amazon\AWSCLIV2\aws.exe'),
            db_path=Path(os.environ.get('AWS_CREDENTIALS_DB_PATH', './data/aws_credentials.db')),
            refresh_window_hours=int(os.environ.get('AWS_SSO_REFRESH_WINDOW', '6')),
            max_retries=int(os.environ.get('AWS_SSO_MAX_RETRIES', '3')),
            retry_delay=int(os.environ.get('AWS_SSO_RETRY_DELAY', '5'))
        )
    
    def validate(self) -> None:
        """Validate the configuration parameters.
        
        Raises:
            ValueError: If any required parameters are missing or invalid
        """
        if not self.aws_exec_file_path:
            raise ValueError("AWS executable path cannot be empty")
        if not self.db_path:
            raise ValueError("Database path cannot be empty")
        if self.refresh_window_hours <= 0:
            raise ValueError("Refresh window must be a positive number")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.retry_delay < 0:
            raise ValueError("Retry delay cannot be negative")

class AWSsso:
    """
    A class to handle AWS SSO authentication and credential management.
    This class provides functionality to refresh AWS SSO credentials and track their validity.
    
    Examples:
        ```python
        # Example 1: Using SSOConfig
        from aws_connector.aws_sso import SSOConfig, AWSsso
        
        # Create config from environment variables
        config = SSOConfig.from_env()
        
        # Initialize connector with config
        sso = AWSsso(
            aws_exec_file_path=config.aws_exec_file_path,
            db_path=config.db_path,
            refresh_window_hours=config.refresh_window_hours,
            max_retries=config.max_retries,
            retry_delay=config.retry_delay
        )
        
        # Get credential expiration time
        expiration = sso.get_expiration_time()
        print(f"Credentials will expire at: {expiration}")
        
        # Example 2: Direct initialization without SSOConfig
        sso = AWSsso(
            aws_exec_file_path=r'C:\Program Files\Amazon\AWSCLIV2\aws.exe',
            db_path=Path('./data/custom_credentials.db'),
            refresh_window_hours=12,  # Refresh every 12 hours
            max_retries=5,           # More retries
            retry_delay=10           # Longer delay between retries
        )
        
        # Ensure credentials are valid before AWS operations
        try:
            sso.ensure_valid_credentials()
            # Proceed with AWS operations
        except AuthenticationError as e:
            print(f"Failed to authenticate: {e}")
        
        # Example 3: Using SSOConfig with parameter overrides
        config = SSOConfig.from_env()
        sso = AWSsso(
            **config.__dict__,           # Unpack config attributes
            refresh_window_hours=12,     # Override refresh window
            max_retries=5,              # Override retries
            retry_delay=10              # Override delay
        )
        
        # Monitor credential status
        last_refresh = sso.get_last_refresh_time()
        if last_refresh:
            time_since_refresh = datetime.now() - last_refresh
            print(f"Time since last refresh: {time_since_refresh}")
        ```
    """
    
    def __init__(
        self,
        aws_exec_file_path: str = r'C:\Program Files\Amazon\AWSCLIV2\aws.exe',
        db_path: Path = Path("./data/aws_credentials.db"),
        refresh_window_hours: int = 6,
        max_retries: int = 3,
        retry_delay: int = 5
    ):
        """
        Initialize the AWS SSO handler.
        
        Args:
            aws_exec_file_path (str): Path to the AWS CLI executable
            db_path (Path): Path to the credentials database
            refresh_window_hours (int): Hours between credential refreshes
            max_retries (int): Maximum number of authentication retries
            retry_delay (int): Delay between retries in seconds
        
        Raises:
            ValueError: If configuration parameters are invalid
            CredentialError: If there's an error initializing the database
        """
        self.config = SSOConfig(
            aws_exec_file_path=aws_exec_file_path,
            db_path=db_path,
            refresh_window_hours=refresh_window_hours,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        self._last_refresh_time: Optional[datetime] = None
        self._expiration_time: Optional[datetime] = None
        self._db_connection: Optional[sqlite3.Connection] = None
        self._init_db()
    
    def _init_db(self) -> None:
        """
        Initialize the SQLite database for storing credential timestamps.
        If the database exists and contains timestamps, initialize the cached timestamps.
        
        Raises:
            CredentialError: If there's an error creating the database or table
        """
        try:
            # First check if the parent directory exists
            if not self.config.db_path.parent.exists():
                try:
                    self.config.db_path.parent.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    raise CredentialError(f"Error creating database directory: {str(e)}")

            # Try to connect to the database
            try:
                conn = sqlite3.connect(self.config.db_path)
            except sqlite3.Error as e:
                raise CredentialError(f"Error connecting to database: {str(e)}")

            try:
                cursor = conn.cursor()
                
                # Create table if it doesn't exist
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS credential_timestamps (
                        id INTEGER PRIMARY KEY,
                        last_refresh TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
                
                # If table exists, get the latest timestamp
                cursor.execute('SELECT last_refresh FROM credential_timestamps ORDER BY id DESC LIMIT 1')
                result = cursor.fetchone()
                
                if result:
                    self._last_refresh_time = datetime.fromisoformat(result[0])
                    self._expiration_time = self._last_refresh_time + timedelta(hours=self.config.refresh_window_hours)
                    logger.info(f"Initialized timestamps from database. Last refresh: {self._last_refresh_time}")
                else:
                    logger.info("No existing timestamps found in database")
                
                logger.info(f"Initialized credential database at {self.config.db_path}")
            except sqlite3.Error as e:
                raise CredentialError(f"Error initializing database tables: {str(e)}")
            finally:
                conn.close()
        except Exception as e:
            error_msg = f"Error initializing credential database: {str(e)}"
            logger.error(error_msg)
            raise CredentialError(error_msg)
    
    def __enter__(self) -> 'AWSsso':
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - ensure database connection is closed."""
        self._close_db_connection()
    
    def _get_db_connection(self) -> sqlite3.Connection:
        """Get a database connection, creating one if it doesn't exist.
        
        Returns:
            sqlite3.Connection: A database connection instance
            
        Raises:
            CredentialError: If there's an error creating the connection
        """
        if not self._db_connection:
            try:
                self._db_connection = sqlite3.connect(self.config.db_path)
            except sqlite3.Error as e:
                raise CredentialError(f"Error creating database connection: {str(e)}")
        return self._db_connection
    
    def _close_db_connection(self) -> None:
        """Close the database connection if it exists."""
        if self._db_connection:
            try:
                self._db_connection.close()
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
            finally:
                self._db_connection = None
    
    @property
    def should_refresh_credentials(self) -> bool:
        """Check if credentials need to be refreshed.
        
        Returns:
            bool: True if credentials need to be refreshed, False otherwise
        """
        if self._expiration_time is None:
            return True
        return datetime.now() > self._expiration_time
    
    def get_expiration_time(self) -> Optional[datetime]:
        """Get the expiration time of the current credentials.
        
        Returns:
            Optional[datetime]: The expiration time of the credentials, or None if not set
        """
        return self._expiration_time
    
    def get_last_refresh_time(self) -> Optional[datetime]:
        """Get the last time the credentials were refreshed.
        
        Returns:
            Optional[datetime]: The last refresh time, or None if not set
        """
        return self._last_refresh_time
    
    def ensure_valid_credentials(self) -> bool:
        """Ensure that the AWS SSO credentials are valid.
        If credentials are expired or about to expire, they will be refreshed.
        
        Returns:
            bool: True if credentials are valid or were successfully refreshed
            
        Raises:
            AuthenticationError: If there's an error during SSO authentication
            CredentialError: If there's an error updating the timestamp
        """
        if self.should_refresh_credentials:
            return self.refresh_credentials()
        return True

    def refresh_credentials(self) -> bool:
        """
        Refresh AWS SSO credentials using the AWS CLI if they need refreshing.
        
        Returns:
            bool: True if credentials are valid or were successfully refreshed, False otherwise
            
        Raises:
            AuthenticationError: If there's an error during SSO authentication
            CredentialError: If there's an error updating the timestamp
        """
        # Attempt to refresh credentials
        for attempt in range(self.config.max_retries):
            try:
                result = subprocess.run(
                    [self.config.aws_exec_file_path, 'sso', 'login'],
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    logger.info("Successfully authenticated with AWS SSO")
                    # Update cached timestamps
                    self._last_refresh_time = datetime.now()
                    self._expiration_time = self._last_refresh_time + timedelta(hours=self.config.refresh_window_hours)
                    
                    # Update database with new timestamp
                    try:
                        conn = self._get_db_connection()
                        try:
                            cursor = conn.cursor()
                            cursor.execute(
                                'INSERT INTO credential_timestamps (last_refresh) VALUES (?)',
                                (self._last_refresh_time.isoformat(),)
                            )
                            conn.commit()
                            logger.info("Successfully updated SSO credential refresh timestamp")
                            return True
                        except sqlite3.Error as e:
                            error_msg = f"Error updating SSO credential timestamp: {str(e)}"
                            logger.error(error_msg)
                            raise CredentialError(error_msg)
                        finally:
                            conn.close()
                            self._db_connection = None
                    except Exception as e:
                        error_msg = f"Error updating SSO credential timestamp: {str(e)}"
                        logger.error(error_msg)
                        raise CredentialError(error_msg)
                        
                if attempt < self.config.max_retries - 1:
                    logger.warning(f"SSO authentication attempt {attempt + 1} failed, retrying...")
                    time.sleep(self.config.retry_delay)
            except subprocess.CalledProcessError as e:
                if attempt == self.config.max_retries - 1:
                    raise AuthenticationError(f"Failed to refresh SSO credentials after {self.config.max_retries} attempts: {str(e)}")
                logger.warning(f"SSO authentication attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.config.retry_delay)
            except FileNotFoundError as e:
                error_msg = f"AWS CLI executable not found at: {self.config.aws_exec_file_path}"
                logger.error(error_msg)
                raise AuthenticationError(error_msg)
        
        raise AuthenticationError(f"Failed to refresh SSO credentials after {self.config.max_retries} attempts")

