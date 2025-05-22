"""Tests for logging configuration and behavior."""

import logging
import pytest
from unittest.mock import patch, MagicMock
import sys
from io import StringIO

from aws_connector.s3 import S3Connector
from aws_connector.redshift import RedConn
from aws_connector.aws_sso import AWSsso

@pytest.fixture
def log_capture():
    """Capture log output for testing."""
    log_output = StringIO()
    handler = logging.StreamHandler(log_output)
    handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    
    yield log_output
    
    # Cleanup
    root_logger.removeHandler(handler)
    log_output.close()

def test_s3_logger_initialization():
    """Test that S3 module logger is properly initialized."""
    logger = logging.getLogger('aws_connector.s3')
    assert logger.name == 'aws_connector.s3'
    assert logger.level == logging.NOTSET  # Level should be set by application
    assert any(isinstance(h, logging.NullHandler) for h in logger.handlers)

def test_redshift_logger_initialization():
    """Test that Redshift module logger is properly initialized."""
    logger = logging.getLogger('aws_connector.redshift')
    assert logger.name == 'aws_connector.redshift'
    assert logger.level == logging.NOTSET  # Level should be set by application
    assert any(isinstance(h, logging.NullHandler) for h in logger.handlers)

def test_aws_sso_logger_initialization():
    """Test that AWS SSO module logger is properly initialized."""
    logger = logging.getLogger('aws_connector.aws_sso')
    assert logger.name == 'aws_connector.aws_sso'
    assert logger.level == logging.NOTSET  # Level should be set by application
    assert any(isinstance(h, logging.NullHandler) for h in logger.handlers)

def test_logging_propagation(log_capture):
    """Test that logs are properly propagated to the root logger."""
    # Configure logging
    logger = logging.getLogger('aws_connector.s3')
    logger.setLevel(logging.INFO)
    logger.propagate = True  # Ensure logs propagate to root logger
    
    # Create mock S3 client and resource
    mock_s3_client = MagicMock()
    mock_s3_resource = MagicMock()
    mock_s3_resource.meta.client = mock_s3_client
    
    # Mock both boto3.client and boto3.resource
    with patch('boto3.client', return_value=mock_s3_client), \
         patch('boto3.resource', return_value=mock_s3_resource):
        
        # Create S3 connector - this should not make any real AWS calls
        s3 = S3Connector(bucket='test-bucket', directory='test/')
        # Force a log message
        logger.info("Test log message")
        s3.s3_client  # This should trigger some logging
    
    # Check log output
    log_content = log_capture.getvalue()
    assert 'aws_connector.s3' in log_content

def test_log_level_control(log_capture):
    """Test that log levels can be controlled by the application."""
    # Set different log levels for different components
    s3_logger = logging.getLogger('aws_connector.s3')
    redshift_logger = logging.getLogger('aws_connector.redshift')
    
    s3_logger.setLevel(logging.DEBUG)
    redshift_logger.setLevel(logging.WARNING)
    
    # Ensure logs propagate
    s3_logger.propagate = True
    redshift_logger.propagate = True
    
    # Create mock S3 client and resource
    mock_s3_client = MagicMock()
    mock_s3_resource = MagicMock()
    mock_s3_resource.meta.client = mock_s3_client
    
    # Create connectors and trigger logging
    with patch('boto3.client', return_value=mock_s3_client), \
         patch('boto3.resource', return_value=mock_s3_resource):
        s3 = S3Connector(bucket='test-bucket', directory='test/')
        s3_logger.debug("Test debug message")  # Should be logged
        s3.s3_client
    
    with patch('redshift_connector.connect') as mock_connect:
        mock_connect.return_value = MagicMock()
        red = RedConn(
            host='test',
            username='test',
            password='test',
            database='test'
        )
        redshift_logger.info("Test info message")  # Should not be logged
        red.connect()
    
    # Check log output
    log_content = log_capture.getvalue()
    assert 'aws_connector.s3' in log_content
    assert 'aws_connector.redshift' not in log_content

def test_log_formatting(log_capture):
    """Test that log messages are properly formatted."""
    # Configure logging with a specific format
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler(log_capture)
    handler.setFormatter(formatter)
    
    logger = logging.getLogger('aws_connector.s3')
    logger.setLevel(logging.INFO)
    logger.propagate = True
    logger.addHandler(handler)
    
    # Create mock S3 client and resource
    mock_s3_client = MagicMock()
    mock_s3_resource = MagicMock()
    mock_s3_resource.meta.client = mock_s3_client
    
    # Trigger some logging
    with patch('boto3.client', return_value=mock_s3_client), \
         patch('boto3.resource', return_value=mock_s3_resource):
        s3 = S3Connector(bucket='test-bucket', directory='test/')
        logger.info("Test formatted message")
        s3.s3_client
    
    # Check log format
    log_content = log_capture.getvalue()
    assert 'aws_connector.s3 - INFO -' in log_content

def test_multiple_handlers(log_capture):
    """Test that multiple handlers can be used simultaneously."""
    # Create a file handler
    file_handler = logging.FileHandler('test.log')
    file_handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
    
    # Configure logger
    logger = logging.getLogger('aws_connector.s3')
    logger.setLevel(logging.INFO)
    logger.propagate = True
    logger.addHandler(file_handler)
    
    # Create mock S3 client and resource
    mock_s3_client = MagicMock()
    mock_s3_resource = MagicMock()
    mock_s3_resource.meta.client = mock_s3_client
    
    # Trigger some logging
    with patch('boto3.client', return_value=mock_s3_client), \
         patch('boto3.resource', return_value=mock_s3_resource):
        s3 = S3Connector(bucket='test-bucket', directory='test/')
        logger.info("Test multiple handlers message")
        s3.s3_client
    
    # Check console output
    log_content = log_capture.getvalue()
    assert 'aws_connector.s3' in log_content
    
    # Cleanup
    logger.removeHandler(file_handler)
    file_handler.close() 