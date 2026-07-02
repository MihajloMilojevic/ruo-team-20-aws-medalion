"""
Gold layer — shared helpers
=============================
Thin wrappers around awswrangler so lambda_function.py stays focused on the
metric logic (GOLD_LAYER_HANDOFF.md, section 5.4).
"""

from __future__ import annotations

import logging

logger = logging.getLogger()


def safe_read_parquet(path: str, dataset: bool = True, filters=None, dedupe_subset=None):
    """Reads a Silver/Gold Parquet path, returning an empty DataFrame instead
    of raising when the prefix doesn't exist yet (e.g. no data for that day,
    or a table that hasn't been written by any normalization run so far).

    dedupe_subset: as of the Silver-layer performance rewrite (see
    KNOWLEDGE.md 9.11), normalization_hn/normalization_x no longer
    read-modify-write Silver tables on every invocation — each run just
    writes its own small Parquet file. That means the same entity (a user,
    a hashtag, a URL, ...) can legitimately appear in more than one file if
    it showed up on more than one day, so any table read here that's keyed
    by a primary key should collapse those with drop_duplicates(). This is
    the one place that cost is paid now — once per Gold run, on however much
    data is actually being read — instead of on every single Silver write.
    """
    import pandas as pd
    import awswrangler as wr

    try:
        kwargs = {"dataset": dataset}
        if filters:
            kwargs["filters"] = filters
        df = wr.s3.read_parquet(path, **kwargs)
    except Exception as e:
        # Logging only e.__class__.__name__ here previously hid the actual
        # cause (a TypeError from a partition-dtype mismatch looked
        # identical to "no files found yet") — log the real message and a
        # traceback so a genuine bug doesn't get silently treated as
        # "no data".
        logger.warning(f"No data at {path} ({e.__class__.__name__}: {e}), treating as empty", exc_info=True)
        return pd.DataFrame()

    if dedupe_subset and not df.empty:
        cols = [dedupe_subset] if isinstance(dedupe_subset, str) else list(dedupe_subset)
        cols = [c for c in cols if c in df.columns]
        if cols:
            df = df.drop_duplicates(subset=cols, keep="last").reset_index(drop=True)
    return df


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
        # dataset=False requires `path` to be an exact object key, not a
        # prefix -- but `path` here is a folder ("gold/{table}/"). This was
        # always latently broken; it just never fired before because
        # top_x_users_followers had no non-empty X data to write until now.
        # dataset=True with mode="overwrite" handles a table-folder path
        # correctly even with no partition_cols: it clears the folder and
        # writes the new file(s) under it.
        wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")

    return len(df)
