"""
Delivery layer — S3 -> PostgreSQL
===================================
Reads gold/ Parquet files and writes them into PostgreSQL on the EC2 instance.
This Lambda must run inside the VPC to reach the EC2/PostgreSQL instance.

PostgreSQL connection is configured via environment variables:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    raise NotImplementedError("Delivery layer — not yet implemented")
