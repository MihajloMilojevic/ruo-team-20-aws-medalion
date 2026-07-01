"""
Gold layer — shared helpers
=============================
Thin wrappers around awswrangler so lambda_function.py stays focused on the
metric logic (GOLD_LAYER_HANDOFF.md, section 5.4).
"""

from __future__ import annotations

import logging

logger = logging.getLogger()


def safe_read_parquet(path: str, dataset: bool = True, filters=None):
    """Reads a Silver/Gold Parquet path, returning an empty DataFrame instead
    of raising when the prefix doesn't exist yet (e.g. no data for that day,
    or a table that hasn't been written by any normalization run so far)."""
    import pandas as pd
    import awswrangler as wr

    try:
        kwargs = {"dataset": dataset}
        if filters:
            kwargs["filters"] = filters
        return wr.s3.read_parquet(path, **kwargs)
    except Exception as e:
        logger.info(f"No data at {path} ({e.__class__.__name__}), treating as empty")
        return pd.DataFrame()


def write_gold(df, bucket: str, table: str, partition_cols: list[str] | None) -> int:
    """Writes a Gold metric table.
    - With partition_cols: overwrite_partitions, dataset=True — reruns replace
      only the touched date/platform partitions (same idempotency contract as
      Silver).
    - Without partition_cols (e.g. the static top_x_users_followers table):
      a single flat overwrite.
    Empty results are skipped rather than written, per handoff doc 5.5 ("write
    an empty-but-schema-correct DataFrame or skip the partition, don't crash").
    """
    if df is None or df.empty:
        logger.info(f"[{table}] nothing to write (empty result)")
        return 0

    import awswrangler as wr

    path = f"s3://{bucket}/gold/{table}/"
    if partition_cols:
        wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            mode="overwrite_partitions",
            partition_cols=partition_cols,
        )
    else:
        wr.s3.to_parquet(df=df, path=path, dataset=False, mode="overwrite")

    return len(df)
