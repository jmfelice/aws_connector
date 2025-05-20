# AWS Connector

A comprehensive Python library for seamless interaction with AWS services, specifically designed for Redshift, S3, and AWS SSO integration. This package provides robust tools for data pipeline operations, large dataset processing, and secure AWS service authentication.

## Features

- **Redshift Integration**: Easy connection and query execution with Amazon Redshift
- **S3 Operations**: Simple data upload and management in S3 buckets
- **AWS SSO Support**: Built-in AWS SSO authentication and credential management
- **Error Handling**: Comprehensive error handling and exception management
- **Large Dataset Support**: Chunked processing for handling large datasets efficiently
- **Configuration Management**: Flexible configuration options for all AWS services

## Installation

```bash
pip install aws-connector
```

## Quick Start

```python
from aws_connector import RedConn, S3Connector, AWSsso

# Initialize AWS SSO
sso = AWSsso()
sso.ensure_valid_credentials()

# Connect to Redshift
redshift = RedConn(
    host="your-cluster.xxxxx.region.redshift.amazonaws.com",
    username="admin",
    password="secret",
    database="mydb"
)

# Query data
with redshift as conn:
    df = conn.fetch("SELECT * FROM my_table LIMIT 10")

# Upload to S3
s3 = S3Connector(
    bucket="my-bucket",
    directory="data/"
)
result = s3.upload_to_s3(df, table_name="my_table")
```

## Key Components

### RedConn
The Redshift connector provides a simple interface for:
- Executing SQL queries
- Managing database connections
- Handling large datasets with chunked processing
- Transaction management

### S3Connector
The S3 connector offers functionality for:
- Uploading data to S3 buckets
- Managing file paths and directories
- Handling upload errors and retries
- Supporting various file formats

### AWSsso
The AWS SSO integration provides:
- Automatic credential management
- Session handling
- Secure authentication
- Credential refresh

## Examples

### Basic Data Pipeline
```python
from aws_connector import RedConn, S3Connector, AWSsso

# Initialize connections
sso = AWSsso()
sso.ensure_valid_credentials()

redshift = RedConn(
    host="your-cluster.xxxxx.region.redshift.amazonaws.com",
    username="admin",
    password="secret",
    database="mydb"
)

s3 = S3Connector(
    bucket="my-bucket",
    directory="data/"
)

# Execute pipeline
with redshift as conn:
    df = conn.fetch("SELECT * FROM my_table")
    result = s3.upload_to_s3(df, table_name="my_table")
```

### Large Dataset Processing
```python
with redshift as conn:
    # Process data in chunks of 10000 rows
    for chunk in conn.fetch("SELECT * FROM large_table", chunksize=10000):
        result = s3.upload_to_s3(chunk, table_name="processed_data")
```

## Error Handling

The library includes comprehensive error handling for common scenarios:
- Connection errors
- Query execution errors
- Upload failures
- Authentication issues

## Contributing

Contributions are welcome! Please read our contributing guidelines in the `docs/contributing.rst` file.

## Documentation

For detailed documentation, including configuration options and advanced usage examples, please visit our [documentation](docs/index.rst).

## License

This project is licensed under the MIT License - see the LICENSE file for details. 