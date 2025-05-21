import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
import tempfile
import shutil
import subprocess

from aws_connector.aws_sso import SSOConfig, AWSsso
from aws_connector.exceptions import CredentialError, AuthenticationError

@pytest.fixture
def temp_db_dir():
    """Create a temporary directory for test database."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def mock_aws_cli():
    """Mock AWS CLI executable path."""
    return r'C:\mock\aws.exe'

@pytest.fixture
def sso_config(temp_db_dir, mock_aws_cli):
    """Create a test SSOConfig instance."""
    return SSOConfig(
        aws_exec_file_path=mock_aws_cli,
        db_path=Path(temp_db_dir) / 'test_credentials.db',
        refresh_window_hours=1,
        max_retries=2,
        retry_delay=1
    )

class TestSSOConfig:
    def test_default_config(self):
        """Test default configuration values."""
        config = SSOConfig()
        assert config.aws_exec_file_path == r'C:\Program Files\Amazon\AWSCLIV2\aws.exe'
        assert config.db_path == Path('./data/aws_credentials.db')
        assert config.refresh_window_hours == 6
        assert config.max_retries == 3
        assert config.retry_delay == 5

    def test_custom_config(self, temp_db_dir, mock_aws_cli):
        """Test custom configuration values."""
        config = SSOConfig(
            aws_exec_file_path=mock_aws_cli,
            db_path=Path(temp_db_dir) / 'test.db',
            refresh_window_hours=12,
            max_retries=5,
            retry_delay=10
        )
        assert config.aws_exec_file_path == mock_aws_cli
        assert config.db_path == Path(temp_db_dir) / 'test.db'
        assert config.refresh_window_hours == 12
        assert config.max_retries == 5
        assert config.retry_delay == 10

    def test_from_env(self, temp_db_dir, mock_aws_cli):
        """Test configuration from environment variables."""
        env_vars = {
            'AWS_EXEC_FILE_PATH': mock_aws_cli,
            'AWS_CREDENTIALS_DB_PATH': str(Path(temp_db_dir) / 'env.db'),
            'AWS_SSO_REFRESH_WINDOW': '12',
            'AWS_SSO_MAX_RETRIES': '5',
            'AWS_SSO_RETRY_DELAY': '10'
        }
        with patch.dict(os.environ, env_vars):
            config = SSOConfig.from_env()
            assert config.aws_exec_file_path == mock_aws_cli
            assert config.db_path == Path(temp_db_dir) / 'env.db'
            assert config.refresh_window_hours == 12
            assert config.max_retries == 5
            assert config.retry_delay == 10

    def test_validation(self):
        """Test configuration validation."""
        with pytest.raises(ValueError, match="AWS executable path cannot be empty"):
            SSOConfig(aws_exec_file_path="")
        
        with pytest.raises(ValueError, match="Database path cannot be empty"):
            SSOConfig(db_path=None)
        
        with pytest.raises(ValueError, match="Refresh window must be a positive number"):
            SSOConfig(refresh_window_hours=0)
        
        with pytest.raises(ValueError, match="Max retries cannot be negative"):
            SSOConfig(max_retries=-1)
        
        with pytest.raises(ValueError, match="Retry delay cannot be negative"):
            SSOConfig(retry_delay=-1)

class TestAWSsso:
    @pytest.fixture
    def mock_subprocess(self):
        """Mock subprocess.run for AWS CLI commands."""
        with patch('subprocess.run') as mock:
            mock.return_value = MagicMock(returncode=0, stdout="Success", stderr="")
            yield mock

    def test_init(self, sso_config):
        """Test AWSsso initialization."""
        sso = None
        try:
            sso = AWSsso(
                aws_exec_file_path=sso_config.aws_exec_file_path,
                db_path=sso_config.db_path,
                refresh_window_hours=sso_config.refresh_window_hours,
                max_retries=sso_config.max_retries,
                retry_delay=sso_config.retry_delay
            )
            assert sso.config.aws_exec_file_path == sso_config.aws_exec_file_path
            assert sso.config.db_path == sso_config.db_path
            assert sso.config.refresh_window_hours == sso_config.refresh_window_hours
            assert sso.config.max_retries == sso_config.max_retries
            assert sso.config.retry_delay == sso_config.retry_delay
            assert sso.config.db_path.exists()
        finally:
            if sso:
                sso._close_db_connection()

    def test_should_refresh_credentials_no_previous_refresh(self, sso_config):
        """Test should_refresh_credentials when no previous refresh exists."""
        with AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=sso_config.db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=sso_config.max_retries,
            retry_delay=sso_config.retry_delay
        ) as sso:
            assert sso.should_refresh_credentials is True

    def test_should_refresh_credentials_with_recent_refresh(self, sso_config):
        """Test should_refresh_credentials with recent refresh."""
        sso = None
        try:
            sso = AWSsso(
                aws_exec_file_path=sso_config.aws_exec_file_path,
                db_path=sso_config.db_path,
                refresh_window_hours=sso_config.refresh_window_hours,
                max_retries=sso_config.max_retries,
                retry_delay=sso_config.retry_delay
            )
            
            # Insert a recent timestamp
            conn = sqlite3.connect(sso.config.db_path)
            try:
                cursor = conn.cursor()
                cursor.execute(
                    'INSERT INTO credential_timestamps (last_refresh) VALUES (?)',
                    (datetime.now().isoformat(),)
                )
                conn.commit()
            finally:
                conn.close()
            
            # Reinitialize to load the timestamp
            sso._init_db()
            
            assert sso.should_refresh_credentials is False
        finally:
            if sso:
                sso._close_db_connection()

    def test_should_refresh_credentials_with_old_refresh(self, sso_config):
        """Test should_refresh_credentials with old refresh."""
        sso = None
        try:
            sso = AWSsso(
                aws_exec_file_path=sso_config.aws_exec_file_path,
                db_path=sso_config.db_path,
                refresh_window_hours=sso_config.refresh_window_hours,
                max_retries=sso_config.max_retries,
                retry_delay=sso_config.retry_delay
            )
            
            # Insert an old timestamp
            conn = sqlite3.connect(sso.config.db_path)
            try:
                cursor = conn.cursor()
                old_time = (datetime.now() - timedelta(hours=2)).isoformat()
                cursor.execute(
                    'INSERT INTO credential_timestamps (last_refresh) VALUES (?)',
                    (old_time,)
                )
                conn.commit()
            finally:
                conn.close()
            
            # Reinitialize to load the timestamp
            sso._init_db()
            
            assert sso.should_refresh_credentials is True
        finally:
            if sso:
                sso._close_db_connection()

    def test_get_expiration_time(self, sso_config):
        """Test get_expiration_time method."""
        with AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=sso_config.db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=sso_config.max_retries,
            retry_delay=sso_config.retry_delay
        ) as sso:
            # Initially should be None
            assert sso.get_expiration_time() is None

            # After setting a refresh time
            refresh_time = datetime.now()
            sso._last_refresh_time = refresh_time
            sso._expiration_time = refresh_time + timedelta(hours=sso_config.refresh_window_hours)
            assert sso.get_expiration_time() == sso._expiration_time

    def test_get_last_refresh_time(self, sso_config):
        """Test get_last_refresh_time method."""
        with AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=sso_config.db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=sso_config.max_retries,
            retry_delay=sso_config.retry_delay
        ) as sso:
            # Initially should be None
            assert sso.get_last_refresh_time() is None

            # After setting a refresh time
            refresh_time = datetime.now()
            sso._last_refresh_time = refresh_time
            assert sso.get_last_refresh_time() == refresh_time

    def test_refresh_credentials_success(self, sso_config, mock_subprocess):
        """Test successful credential refresh."""
        with AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=sso_config.db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=sso_config.max_retries,
            retry_delay=sso_config.retry_delay
        ) as sso:
            assert sso.refresh_credentials() is True
            mock_subprocess.assert_called_once_with(
                [sso_config.aws_exec_file_path, 'sso', 'login'],
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
            # Verify database was updated
            with sso._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM credential_timestamps')
                assert cursor.fetchone()[0] == 1

    def test_refresh_credentials_failure(self, sso_config, mock_subprocess):
        """Test failed credential refresh."""
        mock_subprocess.return_value.returncode = 1
        with AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=sso_config.db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=sso_config.max_retries,
            retry_delay=sso_config.retry_delay
        ) as sso:
            with pytest.raises(AuthenticationError):
                sso.refresh_credentials()

    def test_ensure_valid_credentials(self, sso_config, mock_subprocess):
        """Test ensure_valid_credentials method."""
        with AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=sso_config.db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=sso_config.max_retries,
            retry_delay=sso_config.retry_delay
        ) as sso:
            # Should refresh when no credentials exist
            assert sso.ensure_valid_credentials() is True
            mock_subprocess.assert_called_once()

            # Should not refresh when credentials are valid
            mock_subprocess.reset_mock()
            assert sso.ensure_valid_credentials() is True
            mock_subprocess.assert_not_called()

    def test_db_directory_creation(self, sso_config):
        """Test that the directory and database are created if they do not exist."""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / 'test_credentials.db'
        # Remove the directory to simulate non-existence
        shutil.rmtree(temp_dir)
        # Should not raise an error, should create the directory and DB
        sso = AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=sso_config.max_retries,
            retry_delay=sso_config.retry_delay
        )
        assert db_path.parent.exists()
        # The DB file may not exist until a write, but the directory should exist
        sso._close_db_connection()
        # Clean up
        if db_path.exists():
            db_path.unlink()
        if db_path.parent.exists():
            shutil.rmtree(db_path.parent)

    def test_db_connection_error_during_operation(self, sso_config, mock_subprocess):
        """Test database connection error during operation."""
        # Create a temporary directory that will be deleted
        temp_dir = tempfile.mkdtemp()
        try:
            # Create an AWSsso instance with a path in the temp directory
            db_path = Path(temp_dir) / 'test_credentials.db'
            sso = AWSsso(
                aws_exec_file_path=sso_config.aws_exec_file_path,
                db_path=db_path,
                refresh_window_hours=sso_config.refresh_window_hours,
                max_retries=sso_config.max_retries,
                retry_delay=sso_config.retry_delay
            )
            
            # Delete the temp directory to force a connection error
            shutil.rmtree(temp_dir)
            
            # Mock the AWS CLI command to succeed
            mock_subprocess.return_value.returncode = 0
            
            # Try to refresh credentials which should fail due to database error
            with pytest.raises(CredentialError) as exc_info:
                sso.refresh_credentials()
            assert "Error updating SSO credential timestamp" in str(exc_info.value)
        finally:
            # Clean up in case the test fails
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            if sso:
                sso._close_db_connection()

    def test_aws_cli_not_found(self, sso_config):
        """Test AWS CLI not found error handling."""
        with AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=sso_config.db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=sso_config.max_retries,
            retry_delay=sso_config.retry_delay
        ) as sso:
            with patch('subprocess.run', side_effect=FileNotFoundError()):
                with pytest.raises(AuthenticationError, match="AWS CLI executable not found"):
                    sso.refresh_credentials()

    def test_retry_mechanism(self, sso_config):
        """Test retry mechanism for failed AWS CLI commands."""
        with AWSsso(
            aws_exec_file_path=sso_config.aws_exec_file_path,
            db_path=sso_config.db_path,
            refresh_window_hours=sso_config.refresh_window_hours,
            max_retries=2,
            retry_delay=0.1  # Short delay for testing
        ) as sso:
            mock_subprocess = MagicMock()
            mock_subprocess.side_effect = [
                subprocess.CalledProcessError(1, "aws sso login"),
                MagicMock(returncode=0, stdout="Success", stderr="")
            ]
            
            with patch('subprocess.run', mock_subprocess):
                assert sso.refresh_credentials() is True
                assert mock_subprocess.call_count == 2 