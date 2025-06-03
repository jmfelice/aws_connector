.. AWS Connector documentation master file, created by
   sphinx-quickstart on Mon May 19 15:18:14 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

AWS Connector Documentation
=========================

Welcome to the AWS Connector documentation! This package provides a comprehensive set of tools for interacting with AWS services, specifically focusing on Redshift, S3, and AWS SSO integration.

The package is designed to be easy to use while providing robust error handling and configuration management. It supports both direct credential management and AWS SSO authentication.

Installation
-----------

You can install the package directly from GitHub:

.. code-block:: bash

    pip install git+https://github.com/your-org/aws-connector.git

Quick Start
----------

Here's a quick example of how to use the package:

.. code-block:: python

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

Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules/redshift
   modules/s3
   modules/aws_sso
   configuration
   examples
   contributing

