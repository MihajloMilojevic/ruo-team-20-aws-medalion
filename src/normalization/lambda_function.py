"""
Silver layer — Data normalization
===================================
Reads raw JSON from bronze/hacker_news/, normalizes it, and writes
Parquet files into silver/ partitions.

Normalization covers:
  - Timestamp alignment to UTC ISO-8601
  - HTML tag removal from text fields
  - Flattening of nested structures (e.g. the kids array in HN posts)
  - Deduplication by item ID
  - Building posts and users tables according to a 3NF schema
"""

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    raise NotImplementedError("Silver layer — not yet implemented")
