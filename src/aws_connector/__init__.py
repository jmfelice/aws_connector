"""Top-level package for aws_connector.

This package provides connectors for various AWS services including Redshift, S3, and AWS SSO.
"""

__author__ = """Jared Felice"""
__email__ = 'jmfelice@icloud.com'
__version__ = '0.1.0'

from .redshift import RedConn, RedshiftConfig
from .s3 import S3Connector
from .aws_sso import AWSSSO
from .exceptions import (
    AWSConnectorError,
    RedshiftError,
    ConnectionError,
    QueryError,
    S3Error,
    SSOError
)

__all__ = [
    'RedConn',
    'RedshiftConfig',
    'S3Connector',
    'AWSSSO',
    'AWSConnectorError',
    'RedshiftError',
    'ConnectionError',
    'QueryError',
    'S3Error',
    'SSOError'
]
