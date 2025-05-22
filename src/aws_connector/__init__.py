"""AWS Connector package for interacting with AWS services.

This package provides connectors for various AWS services including S3 and Redshift.
It supports both standard AWS credentials and AWS SSO authentication.

Logging Configuration:
    The package uses Python's standard logging module. To configure logging in your application:

    ```python
    import logging
    import sys

    # Basic configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('aws_operations.log')
        ]
    )

    # Optional: Set specific log levels for different components
    logging.getLogger('aws_connector.s3').setLevel(logging.DEBUG)
    logging.getLogger('aws_connector.redshift').setLevel(logging.INFO)
    logging.getLogger('aws_connector.aws_sso').setLevel(logging.WARNING)
    ```

    The package uses the following logger names:
    - aws_connector.s3: For S3 operations
    - aws_connector.redshift: For Redshift operations
    - aws_connector.aws_sso: For AWS SSO operations
"""

__author__ = """Jared Felice"""
__email__ = 'jmfelice@icloud.com'
__version__ = '0.1.0'

from .redshift import RedConn, RedshiftConfig
from .s3 import S3Connector, S3Config
from .aws_sso import AWSsso, SSOConfig  
from .exceptions import (
    AWSConnectorError,
    RedshiftError,
    ConnectionError,
    QueryError,
    S3Error
)

__all__ = [
    'RedConn',
    'RedshiftConfig',
    'S3Connector',
    'AWSsso',
    'AWSConnectorError',
    'RedshiftError',
    'ConnectionError',
    'QueryError',
    'S3Error',
    'SSOConfig',
    'S3Config'
]
