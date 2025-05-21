import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from typing import Optional, List, Tuple, Dict, Union, Any, TypedDict
import os
from datetime import datetime
import tempfile
import pandas as pd
import time
from abc import ABC, abstractmethod
import logging
from botocore.config import Config
from dataclasses import dataclass
import uuid
from .exceptions import (
    AWSConnectorError,
    CredentialError,
    S3Error,
    UploadError,
    RedshiftError
)
from .utils import setup_logging
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

# Configure logging
logger = setup_logging(__name__)

@dataclass
class S3Config:
    """Configuration for S3 operations.
    
    This class can be initialized with direct values or from environment variables.
    Environment variables take precedence over direct values.
    
    Environment Variables:
        AWS_S3_BUCKET: The S3 bucket name
        AWS_S3_DIRECTORY: The directory path within the bucket
        AWS_IAM_ROLE: The IAM role ARN
        AWS_REGION: The AWS region
        AWS_KMS_KEY_ID: The KMS key ID for encryption
        AWS_MAX_RETRIES: Maximum number of retries for AWS operations
        AWS_TIMEOUT: Timeout in seconds for AWS operations
    
    Examples:
        ```python
        # Direct initialization
        config = S3Config(bucket="my-bucket", directory="data/")
        
        # From environment variables
        config = S3Config.from_env()
        
        # Mixed initialization
        config = S3Config(bucket="my-bucket").from_env()
        ```
    """
    bucket: str
    directory: str
    iam: Optional[str] = None
    region: Optional[str] = None
    kms_key_id: Optional[str] = None
    max_retries: int = 3
    timeout: int = 30
    
    @classmethod
    def from_env(cls) -> 'S3Config':
        """Create a configuration from environment variables.
        
        Returns:
            S3Config: A new configuration instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        return cls(
            bucket=os.environ.get('AWS_S3_BUCKET', ''),
            directory=os.environ.get('AWS_S3_DIRECTORY', ''),
            iam=os.environ.get('AWS_IAM_ROLE'),
            region=os.environ.get('AWS_REGION'),
            kms_key_id=os.environ.get('AWS_KMS_KEY_ID'),
            max_retries=int(os.environ.get('AWS_MAX_RETRIES', '3')),
            timeout=int(os.environ.get('AWS_TIMEOUT', '30'))
        )
    
    def validate(self) -> None:
        """Validate the configuration parameters.
        
        Raises:
            ValueError: If any required parameters are missing or invalid
        """
        if not self.bucket:
            raise ValueError("Bucket cannot be empty")
        if not self.directory:
            raise ValueError("Directory cannot be empty")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.timeout <= 0:
            raise ValueError("Timeout must be a positive number")
        if self.region and not self.region.strip():
            raise ValueError("Region cannot be empty if provided")
        if self.iam and not self.iam.strip():
            raise ValueError("IAM role cannot be empty if provided")
        if self.kms_key_id and not self.kms_key_id.strip():
            raise ValueError("KMS key ID cannot be empty if provided")

class S3Result(TypedDict):
    """Type definition for S3 operation results"""
    success: bool
    message: str
    error: Optional[str]

class S3Base(ABC):
    """
    Base class for S3 operations.
    Handles common S3 functionality and configuration.
    
    This class requires AWS credentials to be configured either through:
    - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    - AWS credentials file (~/.aws/credentials)
    - IAM role (if running on AWS infrastructure)
    
    The class supports both standard and KMS encryption for S3 operations.
    
    Testing:
        For testing purposes, you can override the following methods:
        - _get_s3_client(): Override to return a mock S3 client
        - _get_redshift_client(): Override to return a mock Redshift client
        
        Example:
            ```python
            class MockS3Base(S3Base):
                def _get_s3_client(self):
                    return MockS3Client()
            ```
    """
    
    def __init__(
        self,
        bucket: str,
        directory: str,
        iam: Optional[str] = None,
        region: Optional[str] = None,
        kms_key_id: Optional[str] = None,
        max_retries: int = 3,
        timeout: int = 30
    ):
        """
        Initialize the base S3 connector with bucket and directory information.
        
        Args:
            bucket (str): The name of the S3 bucket to use
            directory (str): The directory path within the S3 bucket
            iam (str, optional): The IAM account ID for Redshift operations
            region (str, optional): The AWS region for the S3 bucket
            kms_key_id (str, optional): KMS key ID for encryption
            max_retries (int): Maximum number of retries for AWS operations
            timeout (int): Timeout in seconds for AWS operations
            
        Raises:
            ValueError: If required parameters are missing or invalid
            CredentialError: If AWS credentials are not found
        """
        self.config = S3Config(
            bucket=bucket,
            directory=directory.rstrip('/') + '/',
            iam=iam,
            region=region,
            kms_key_id=kms_key_id,
            max_retries=max_retries,
            timeout=timeout
        )
        self._validate_config()
        self._initialize_s3()
    
    def _get_s3_client(self) -> boto3.client:
        """Get the S3 client. Override this method for testing.
        
        Returns:
            boto3.client: The S3 client instance
        """
        config = Config(
            retries=dict(max_attempts=self.config.max_retries),
            connect_timeout=self.config.timeout,
            read_timeout=self.config.timeout
        )
        return boto3.client("s3", config=config)
    
    def _get_redshift_client(self) -> boto3.client:
        """Get the Redshift client. Override this method for testing.
        
        Returns:
            boto3.client: The Redshift client instance
        """
        return boto3.client("redshift-data", region_name=self.config.region or "us-east-1")
    
    @property
    def s3_client(self) -> boto3.client:
        """Get the S3 client instance.
        
        Returns:
            boto3.client: The S3 client instance
        """
        return self._get_s3_client()
    
    @property
    def redshift_client(self) -> boto3.client:
        """Get the Redshift client instance.
        
        Returns:
            boto3.client: The Redshift client instance
        """
        return self._get_redshift_client()
    
    def _validate_config(self) -> None:
        """Validate the configuration parameters"""
        if not self.config.bucket:
            raise ValueError("Bucket cannot be empty")
        if not self.config.directory:
            raise ValueError("Directory cannot be empty")
        if self.config.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.config.timeout <= 0:
            raise ValueError("Timeout must be a positive number")
            
    def _initialize_s3(self) -> None:
        """Initialize the S3 client with proper configuration"""
        config = Config(
            retries=dict(max_attempts=self.config.max_retries),
            connect_timeout=self.config.timeout,
            read_timeout=self.config.timeout
        )
        
        try:
            self.s3 = boto3.resource("s3", config=config)
            # Verify credentials by making a test call
            self.s3.meta.client.head_bucket(Bucket=self.config.bucket)
        except NoCredentialsError:
            raise CredentialError(
                "AWS credentials not found. Please configure credentials using "
                "environment variables, AWS credentials file, or IAM role."
            )
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise ValueError(f"S3 bucket not found: {self.config.bucket}")
            elif error_code == '403':
                raise ValueError(f"Access denied to S3 bucket: {self.config.bucket}")
            else:
                raise UploadError(f"Error initializing S3 client: {str(e)}")
    
    def _upload_to_s3(self, local_path: str, s3_path: str) -> S3Result:
        """
        Internal method to handle the actual S3 upload.
        
        Args:
            local_path (str): The local file path to upload
            s3_path (str): The target path in S3

        Returns:
            S3Result: Upload status information

        Raises:
            FileNotFoundError: If the source file does not exist
            UploadError: If there's an error with the S3 operation
        """
        result: S3Result = {
            'success': False,
            'message': '',
            'error': None
        }

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Source file not found: {local_path}")

        try:
            # Prepare upload parameters
            upload_args = {
                'Filename': local_path,
                'Bucket': self.config.bucket,
                'Key': s3_path
            }
            
            # Add KMS encryption if configured
            if self.config.kms_key_id:
                upload_args['ServerSideEncryption'] = 'aws:kms'
                upload_args['SSEKMSKeyId'] = self.config.kms_key_id
            
            # Perform upload
            self.s3.meta.client.upload_file(**upload_args)
            
            result['success'] = True
            result['message'] = f"Successfully uploaded {local_path} to s3://{self.config.bucket}/{s3_path}"
            logger.info(result['message'])

        except ClientError as e:
            result['error'] = f"Error uploading file to S3: {e}"
            logger.error(result['error'])
            raise UploadError(result['error'])
        except Exception as e:
            result['error'] = f"Unexpected error: {e}"
            logger.error(result['error'])
            raise UploadError(result['error'])

        return result

    def _upload_to_redshift(
        self,
        source_file: str,
        redshift_schema_name: str,
        redshift_table_name: str,
        redshift_username: str,
        create_table_statement: Optional[str] = None,
        truncate_table_statement: Optional[str] = None,
        echo: bool = False
    ) -> S3Result:
        """
        Internal method to push data from S3 to Redshift.
        
        Args:
            source_file (str): The name of the source file in S3
            redshift_schema_name (str): The name of the schema in Redshift
            redshift_table_name (str): The name of the target table in Redshift
            redshift_username (str): The Redshift user executing the statements
            create_table_statement (str, optional): SQL statement to create the target table
            truncate_table_statement (str, optional): SQL statement to truncate the target table
            echo (bool): If True, print the COPY command that will be executed
            
        Returns:
            S3Result: Operation status information
            
        Raises:
            ValueError: If required parameters are missing
            RedshiftError: If there's an error with the Redshift operation
        """
        if not all([self.config.iam, self.config.region]):
            raise ValueError("IAM and region must be set for Redshift operations")

        client = boto3.client("redshift-data", region_name="us-east-1")

        copy_command = f"""
        COPY fisher_prod.{redshift_schema_name}.{redshift_table_name} FROM 's3://{self.config.bucket}/{self.config.directory}{source_file}' 
        IAM_ROLE 'arn:aws:iam::{self.config.iam}:role/{self.config.region}-{self.config.iam}-fisher-production' 
        FORMAT AS 
        CSV DELIMITER ',' 
        QUOTE '"' 
        IGNOREHEADER 1 
        REGION AS '{self.config.region}'
        TIMEFORMAT 'YYYY-MM-DD-HH.MI.SS'
        DATEFORMAT as 'YYYY-MM-DD'
        """

        if echo:
            logger.info("\nCOPY Command that will be executed:")
            logger.info(copy_command)
            logger.info("\n")

        sql_statements: List[str] = [
            create_table_statement,
            truncate_table_statement,
            copy_command
        ]

        result: S3Result = {
            'success': False,
            'message': '',
            'error': None
        }

        try:
            statement_result = client.batch_execute_statement(
                ClusterIdentifier="fisher-production",
                Database="fisher_prod",
                DbUser=redshift_username,
                Sqls=[stmt for stmt in sql_statements if stmt is not None],
                StatementName=f"{redshift_schema_name}.{redshift_table_name}_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                WithEvent=False
            )
            
            execution_id = statement_result['Id']
            
            while True:
                status = client.describe_statement(Id=execution_id)
                if status['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(2)
            
            if status['Status'] == 'FINISHED':
                result['success'] = True
                result['message'] = f"Successfully pushed s3://{self.config.bucket}/{self.config.directory}{source_file} to Redshift: {redshift_schema_name}.{redshift_table_name}"
            else:
                result['success'] = False
                result['error'] = f"Redshift COPY operation failed with status: {status['Status']}. Error: {status.get('Error', 'Unknown error')}"
            
            logger.info(f"\n{result['message'] if result['success'] else result['error']}\n")
            
        except ClientError as e:
            result['success'] = False
            result['error'] = f"Redshift API error: {str(e)}"
            logger.error(f"\n{result['error']}\n")
            raise RedshiftError(result['error'])
        except Exception as e:
            result['success'] = False
            result['error'] = f"Unexpected error: {str(e)}"
            logger.error(f"\n{result['error']}\n")
            raise RedshiftError(result['error'])

        return result

    @abstractmethod
    def upload_to_s3(self, data: Any, **kwargs) -> S3Result:
        """
        Abstract method for uploading data to S3.
        Must be implemented by subclasses.
        
        Args:
            data: The data to upload
            **kwargs: Additional arguments specific to the implementation
            
        Returns:
            S3Result: Upload status information
            
        Raises:
            UploadError: If there's an error with the upload operation
        """
        pass


class S3FileConnector(S3Base):
    """
    S3 connector for handling file uploads.
    """
    
    def upload_to_s3(self, local_path: str, **kwargs) -> S3Result:
        """
        Upload a file to S3.
        
        Args:
            local_path (str): The local file path to upload
            **kwargs: Additional arguments (not used in this implementation)
            
        Returns:
            S3Result: Upload status information
            
        Raises:
            FileNotFoundError: If the source file does not exist
            UploadError: If there's an error with the S3 operation
        """
        file_name = os.path.basename(local_path)
        s3_path = f"{self.config.directory}{file_name}"
        return self._upload_to_s3(local_path, s3_path)


class S3DataFrameConnector(S3Base):
    """
    S3 connector for handling DataFrame uploads.
    """
    
    def upload_to_s3(
        self,
        df: pd.DataFrame,
        table_name: str,
        temp_file_name: Optional[str] = None,
        **kwargs
    ) -> S3Result:
        """
        Upload a DataFrame to S3 as a CSV file.
        
        Args:
            df (pd.DataFrame): The DataFrame to upload
            table_name (str): Name to use for the file prefix
            temp_file_name (str, optional): Custom name for the temporary file
            **kwargs: Additional arguments (not used in this implementation)
            
        Returns:
            S3Result: Upload status information
            
        Raises:
            ValueError: If the DataFrame is empty
            IOError: If there's an error writing the CSV file
            UploadError: If there's an error with the S3 operation
        """
        if df.empty:
            raise ValueError("DataFrame is empty. Nothing to upload.")

        if temp_file_name:
            local_path = tempfile.gettempdir() + "\\" + os.path.splitext(temp_file_name)[0] + ".csv"
            file_name = os.path.splitext(temp_file_name)[0] + ".csv"
        else:
            temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, prefix=f"{table_name}_", suffix='.csv')
            local_path = temp_file.name
            file_name = os.path.split(local_path)[-1]
            temp_file.close()

        try:
            df.to_csv(local_path, index=False, date_format="%Y-%m-%d")
            s3_path = f"{self.config.directory}{file_name}"
            result = self._upload_to_s3(local_path, s3_path)
            
            # Clean up temporary file
            try:
                os.remove(local_path)
            except OSError as e:
                logger.warning(f"Could not delete temporary file: {e}")
                
            return result
            
        except Exception as e:
            # Clean up temporary file in case of error
            try:
                os.remove(local_path)
            except OSError:
                pass
            raise UploadError(f"Error uploading DataFrame: {str(e)}")


class S3Connector(S3Base):
    """
    Main S3 connector that combines file and DataFrame functionality.
    This class provides a unified interface for both file and DataFrame operations.
    
    Examples:
        ```python
        # Example 1: Using S3Config
        from aws_connector.s3 import S3Config, S3Connector
        
        # Create config from environment variables
        config = S3Config.from_env()
        
        # Initialize connector with config
        s3 = S3Connector(
            bucket=config.bucket,
            directory=config.directory,
            iam=config.iam,
            region=config.region,
            kms_key_id=config.kms_key_id
        )
        
        # Upload a file to S3
        result = s3.upload_to_s3("path/to/local/file.csv")
        
        # Upload a DataFrame to Redshift
        import pandas as pd
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        result = s3.upload_to_redshift(
            data=df,
            redshift_schema_name='my_schema',
            redshift_table_name='my_table',
            redshift_username='my_user'
        )
        
        # Example 2: Direct initialization without S3Config
        s3 = S3Connector(
            bucket='my-bucket',
            directory='data/',
            region='us-east-1',
            iam='123456789012',
            kms_key_id='arn:aws:kms:us-east-1:123456789012:key/abcd1234'
        )
        
        # Load existing S3 file to Redshift
        result = s3.load_from_s3_to_redshift(
            s3_file_path='data/2024/01/file.csv',
            redshift_schema_name='my_schema',
            redshift_table_name='my_table',
            redshift_username='my_user'
        )

        # Example 3: Using S3Config with additional parameters via **kwargs
        config = S3Config(
            bucket='my-bucket',
            directory='data/',
            region='us-east-1'
        )
        
        # Initialize with config and override/add parameters
        s3 = S3Connector(
            **config.__dict__,  # Unpack config attributes
            max_retries=5,      # Override default retries
            timeout=60,         # Override default timeout
            kms_key_id='arn:aws:kms:us-east-1:123456789012:key/abcd1234'  # Add KMS key
        )
        
        # Upload DataFrame with custom temp file name
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        result = s3.upload_to_redshift(
            data=df,
            redshift_schema_name='my_schema',
            redshift_table_name='my_table',
            redshift_username='my_user',
            temp_file_name='custom_upload.csv',  # Custom temp file name
            echo=True  # Print COPY command
        )
        ```
    """
    
    def __init__(
        self,
        bucket: str,
        directory: str,
        iam: Optional[str] = None,
        region: Optional[str] = None,
        kms_key_id: Optional[str] = None,
        max_retries: int = 3,
        timeout: int = 30
    ):
        super().__init__(bucket, directory, iam, region, kms_key_id, max_retries, timeout)
        self.file_connector = S3FileConnector(bucket, directory, iam, region, kms_key_id, max_retries, timeout)
        self.df_connector = S3DataFrameConnector(bucket, directory, iam, region, kms_key_id, max_retries, timeout)
    
    def upload_to_s3(self, data: Union[str, pd.DataFrame], **kwargs) -> S3Result:
        """
        Upload data to S3. Automatically detects the type of data and uses the appropriate connector.
        
        Args:
            data: Either a file path (str) or a pandas DataFrame
            **kwargs: Additional arguments passed to the specific connector
            
        Returns:
            S3Result: Upload status information
            
        Raises:
            TypeError: If the data type is not supported
            FileNotFoundError: If the source file does not exist
            UploadError: If there's an error with the S3 operation
        """
        if isinstance(data, str):
            return self.file_connector.upload_to_s3(data, **kwargs)
        elif isinstance(data, pd.DataFrame):
            return self.df_connector.upload_to_s3(data, **kwargs)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}. Expected str (file path) or pd.DataFrame")
    
    def load_from_s3_to_redshift(
        self,
        s3_file_path: str,
        redshift_schema_name: str,
        redshift_table_name: str,
        redshift_username: str,
        create_table_statement: Optional[str] = None,
        truncate_table_statement: Optional[str] = None,
        echo: bool = False
    ) -> S3Result:
        """
        Load data directly from S3 to Redshift without uploading a file or DataFrame first.
        
        Args:
            s3_file_path (str): The path to the file in S3 (relative to the bucket and directory)
            redshift_schema_name (str): The name of the schema in Redshift
            redshift_table_name (str): The name of the table in Redshift
            redshift_username (str): The username for Redshift connection
            create_table_statement (str, optional): SQL statement to create the target table
            truncate_table_statement (str, optional): SQL statement to truncate the target table
            echo (bool): If True, print the COPY command that will be executed
            
        Returns:
            S3Result: Operation status information
            
        Raises:
            RedshiftError: If there's an error with the S3 or Redshift operation
            ValueError: If the file doesn't exist in S3
            
        Example:
            # Load data from 'data/2024/01/file.csv' in S3 to Redshift
            result = s3.load_from_s3_to_redshift(
                s3_file_path='data/2024/01/file.csv',
                redshift_schema_name='your_schema',
                redshift_table_name='your_table',
                redshift_username='your_username',
                truncate_table_statement='TRUNCATE TABLE fisher_prod.your_schema.your_table;'
            )
        """
        # Ensure the file exists in S3
        try:
            self.s3.meta.client.head_object(
                Bucket=self.config.bucket,
                Key=f"{self.config.directory}{s3_file_path}"
            )
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return {
                    'success': False,
                    'message': '',
                    'error': f"File not found in S3: s3://{self.config.bucket}/{self.config.directory}{s3_file_path}"
                }
            else:
                return {
                    'success': False,
                    'message': '',
                    'error': f"Error checking S3 file: {str(e)}"
                }

        # Load directly to Redshift
        return self._upload_to_redshift(
            source_file=s3_file_path,
            redshift_schema_name=redshift_schema_name,
            redshift_table_name=redshift_table_name,
            redshift_username=redshift_username,
            create_table_statement=create_table_statement,
            truncate_table_statement=truncate_table_statement,
            echo=echo
        )
    
    def upload_to_redshift(
        self,
        data: Union[str, pd.DataFrame, List[Union[str, pd.DataFrame]]],
        redshift_username: str,
        redshift_schema_name: Union[str, List[str]],
        redshift_table_name: Union[str, List[str]],
        create_table_statement: Optional[Union[str, List[Optional[str]]]] = None,
        truncate_table_statement: Optional[Union[str, List[Optional[str]]]] = None,
        temp_file_name: Optional[Union[str, List[Optional[str]]]] = None,
        echo: bool = False,
        parallel: bool = False,
        max_workers: int = 4
    ) -> Union[S3Result, List[S3Result]]:
        """
        Upload data to Redshift via S3. Handles both single items and lists of items.
        When given a list, can process items in parallel or sequentially.
        
        Args:
            data: Either a single item (file path or DataFrame) or a list of items
            redshift_username (str): The username for Redshift connection
            redshift_schema_name: Either a single schema name or a list matching data length
            redshift_table_name: Either a single table name or a list matching data length
            create_table_statement: Optional SQL statement(s) to create table(s)
            truncate_table_statement: Optional SQL statement(s) to truncate table(s)
            temp_file_name: Optional name(s) for temporary file(s)
            echo (bool): If True, print the COPY command that will be executed
            parallel (bool): If True and data is a list, process items in parallel
            max_workers (int): Maximum number of parallel workers when parallel=True
            
        Returns:
            Union[S3Result, List[S3Result]]: Single result or list of results
            
        Raises:
            ValueError: If list lengths don't match when using lists
            TypeError: If the data type is not supported
            FileNotFoundError: If any source file does not exist
            UploadError: If there's an error with the S3 operation
            RedshiftError: If there's an error with the Redshift operation
            
        Example:
            # Upload a single DataFrame with truncate
            df = pd.DataFrame({'col1': [1, 2, 3]})
            result = s3.upload_to_redshift(
                data=df,
                redshift_schema_name='my_schema',
                redshift_table_name='my_table',
                redshift_username='my_user',
                truncate_table_statement='TRUNCATE TABLE fisher_prod.my_schema.my_table;'
            )
            
            # Upload multiple files with different truncate statements
            files = ['file1.csv', 'file2.csv']
            schemas = ['schema1', 'schema2']
            tables = ['table1', 'table2']
            truncate_stmts = [
                'TRUNCATE TABLE fisher_prod.schema1.table1;',
                'TRUNCATE TABLE fisher_prod.schema2.table2;'
            ]
            results = s3.upload_to_redshift(
                data=files,
                redshift_schema_name=schemas,
                redshift_table_name=tables,
                redshift_username='my_user',
                truncate_table_statement=truncate_stmts,
                parallel=True
            )
        """
        # Convert single item to list for uniform processing
        items = [data] if not isinstance(data, list) else data
        
        # Convert single schema/table names to lists if needed
        schemas = [redshift_schema_name] if not isinstance(redshift_schema_name, list) else redshift_schema_name
        tables = [redshift_table_name] if not isinstance(redshift_table_name, list) else redshift_table_name
        
        # Convert optional parameters to lists if needed
        table_stmts = [create_table_statement] if not isinstance(create_table_statement, list) else create_table_statement
        truncate_stmts = [truncate_table_statement] if not isinstance(truncate_table_statement, list) else truncate_table_statement
        temp_files = [temp_file_name] if not isinstance(temp_file_name, list) else temp_file_name
        
        # Validate list lengths
        if isinstance(data, list):
            if len(schemas) != len(items):
                raise ValueError(f"Number of schema names ({len(schemas)}) must match number of data items ({len(items)})")
            if len(tables) != len(items):
                raise ValueError(f"Number of table names ({len(tables)}) must match number of data items ({len(items)})")
            if create_table_statement is not None and len(table_stmts) != len(items):
                raise ValueError(f"Number of create table statements ({len(table_stmts)}) must match number of data items ({len(items)})")
            if truncate_table_statement is not None and len(truncate_stmts) != len(items):
                raise ValueError(f"Number of truncate table statements ({len(truncate_stmts)}) must match number of data items ({len(items)})")
            if temp_file_name is not None and len(temp_files) != len(items):
                raise ValueError(f"Number of temp file names ({len(temp_files)}) must match number of data items ({len(items)})")
        
        if parallel and len(items) > 1:
            # Process items in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for i, item in enumerate(items):
                    future = executor.submit(
                        self._upload_single_to_redshift,
                        item=item,
                        redshift_schema_name=schemas[i],
                        redshift_table_name=tables[i],
                        redshift_username=redshift_username,
                        create_table_statement=table_stmts[i],
                        truncate_table_statement=truncate_stmts[i],
                        temp_file_name=temp_files[i] if temp_file_name is not None else None,
                        echo=echo
                    )
                    futures.append(future)
                
                # Collect results
                results = []
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        results.append({
                            'success': False,
                            'message': '',
                            'error': f"Error in parallel upload: {str(e)}"
                        })
                
                return results if isinstance(data, list) else results[0]
        else:
            # Process items sequentially
            results = []
            for i, item in enumerate(items):
                result = self._upload_single_to_redshift(
                    item=item,
                    redshift_schema_name=schemas[i],
                    redshift_table_name=tables[i],
                    redshift_username=redshift_username,
                    create_table_statement=table_stmts[i],
                    truncate_table_statement=truncate_stmts[i],
                    temp_file_name=temp_files[i] if temp_file_name is not None else None,
                    echo=echo
                )
                results.append(result)
            
            return results if isinstance(data, list) else results[0]

    def _upload_single_to_redshift(
        self,
        item: Union[str, pd.DataFrame],
        redshift_schema_name: str,
        redshift_table_name: str,
        redshift_username: str,
        create_table_statement: Optional[str],
        truncate_table_statement: Optional[str],
        temp_file_name: Optional[str],
        echo: bool
    ) -> S3Result:
        """Helper method to upload a single item to Redshift."""
        if isinstance(item, str):
            # For file uploads, just use the filename
            source_file = os.path.basename(item)
            # First upload the file
            upload_result = self.file_connector.upload_to_s3(item)
            if not upload_result['success']:
                return upload_result
        else:
            # For DataFrame uploads
            upload_result = self.df_connector.upload_to_s3(
                df=item,
                table_name=redshift_table_name,
                temp_file_name=temp_file_name
            )
            if not upload_result['success']:
                return upload_result
            source_file = os.path.basename(upload_result['message'].split(' to ')[-1])

        # Push to Redshift
        return self._upload_to_redshift(
            source_file=source_file,
            redshift_schema_name=redshift_schema_name,
            redshift_table_name=redshift_table_name,
            redshift_username=redshift_username,
            create_table_statement=create_table_statement,
            truncate_table_statement=truncate_table_statement,
            echo=echo
        ) 