"""
Delivery layer — S3 -> PostgreSQL
===================================
Reads the Gold Parquet tables written by the analytics Lambda and loads them
into the PostgreSQL instance running on EC2, so Apache Superset can chart
them (Superset can't read Parquet from S3 directly — see the project spec,
section 4).

Runs inside the VPC: S3 is reached through the Gateway Endpoint, PostgreSQL
through the security-group rule between the Lambda SG and the EC2 SG on
port 5432. Connection details come from environment variables set by
Terraform from the EC2 instance's private IP:

    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

Event contract (mirrors the rest of the pipeline):

    {"date": "YYYY-MM-DD"}   reload just that date's rows in every
                             date-scoped table (DELETE WHERE date = X, then
                             INSERT — same replace-on-rerun idempotency
                             contract Silver and Gold already use).
                             Defaults to yesterday (UTC) if omitted.

    {"full_refresh": true}   TRUNCATE every table and reload the entire
                             gold/ prefix. This is the one-shot to run after
                             the historical Gold backfill completes, instead
                             of invoking once per date.

top_x_users_followers has no date column (it's a static top-10 snapshot),
so it is fully replaced on every run regardless of mode.

Dependencies come from two layers, nothing is bundled in package.zip:
  - AWSSDKPandas-Python312 (AWS-hosted): pandas + awswrangler for Parquet
  - delivery_common (ours): pg8000, a pure-Python PostgreSQL driver
"""

import logging
import os
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# One PostgreSQL table per Gold metric table. `columns` fixes both the DDL
# and the column order used for reading/inserting; the DataFrame is indexed
# by these names, so gold-side column order doesn't matter. `date_scoped`
# selects the idempotency strategy (per-date delete+insert vs. full replace).
GOLD_TABLES = {
    "daily_post_counts": {
        "columns": [
            ("date", "DATE"),
            ("platform", "TEXT"),
            ("post_type", "TEXT"),
            ("count", "BIGINT"),
        ],
        "primary_key": ["date", "platform", "post_type"],
        "date_scoped": True,
    },
    "daily_users_metric": {
        "columns": [
            ("date", "DATE"),
            ("platform", "TEXT"),
            ("total_users", "BIGINT"),
            ("new_users", "BIGINT"),
        ],
        "primary_key": ["date", "platform"],
        "date_scoped": True,
    },
    "top_x_users_followers": {
        "columns": [
            ("rank", "INTEGER"),
            ("username", "TEXT"),
            ("followers_count", "BIGINT"),
            ("platform", "TEXT"),
        ],
        "primary_key": ["rank"],
        "date_scoped": False,
    },
    "top_hn_users_karma": {
        "columns": [
            ("date", "DATE"),
            ("rank", "INTEGER"),
            ("username", "TEXT"),
            ("karma_score", "BIGINT"),
        ],
        "primary_key": ["date", "rank"],
        "date_scoped": True,
    },
    "bottom_hn_users_karma": {
        "columns": [
            ("date", "DATE"),
            ("rank", "INTEGER"),
            ("username", "TEXT"),
            ("karma_score", "BIGINT"),
        ],
        "primary_key": ["date", "rank"],
        "date_scoped": True,
    },
    "top_hn_jobs_score": {
        "columns": [
            ("date", "DATE"),
            ("rank", "INTEGER"),
            ("post_id", "TEXT"),
            ("title", "TEXT"),
            ("score", "BIGINT"),
        ],
        "primary_key": ["date", "rank"],
        "date_scoped": True,
    },
    "top_hn_stories_score": {
        "columns": [
            ("date", "DATE"),
            ("rank", "INTEGER"),
            ("post_id", "TEXT"),
            ("title", "TEXT"),
            ("score", "BIGINT"),
        ],
        "primary_key": ["date", "rank"],
        "date_scoped": True,
    },
    "data_quality_score": {
        "columns": [
            ("date", "DATE"),
            ("table_name", "TEXT"),
            ("non_null_pct", "DOUBLE PRECISION"),
            ("row_count", "BIGINT"),
            ("column_count", "BIGINT"),
        ],
        "primary_key": ["date", "table_name"],
        "date_scoped": True,
    },
}

_INT_TYPES = ("BIGINT", "INTEGER")


def parse_target_date(event: dict) -> str:
    if "date" in event:
        try:
            datetime.strptime(event["date"], "%Y-%m-%d")
            logger.info(f"Using date from event: {event['date']}")
            return event["date"]
        except ValueError:
            logger.warning(f"Invalid date '{event['date']}', falling back to yesterday")
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def read_gold(bucket: str, table: str, date_str: str | None):
    """Reads one Gold table, optionally narrowed to a single date partition.

    partition_filter (not `filters`) deliberately: it receives the partition
    values as strings exactly as they appear in the Hive folder names, so
    there is no partition-dtype inference to fight — the same class of
    TypeError that bit the analytics Lambda's Silver reads (see
    gold_common.safe_read_parquet / KNOWLEDGE.md) can't happen here.
    Returns an empty DataFrame when the prefix doesn't exist yet.
    """
    import awswrangler as wr
    import pandas as pd

    path = f"s3://{bucket}/gold/{table}/"
    kwargs = {"dataset": True}
    if date_str is not None:
        kwargs["partition_filter"] = lambda p: p.get("date") == date_str
    try:
        return wr.s3.read_parquet(path, **kwargs)
    except Exception as e:
        logger.warning(
            f"No data at {path} ({e.__class__.__name__}: {e}), treating as empty",
            exc_info=True,
        )
        return pd.DataFrame()


def to_records(df, columns):
    """DataFrame -> list of plain-Python tuples in DDL column order.

    Explicit casting matters: awswrangler hands back numpy scalars
    (numpy.int64, numpy.float64) and partition columns come back as pandas
    Categorical — none of which pg8000 knows how to adapt. Everything is
    converted to int/float/str/None here so the driver only ever sees
    native Python types.
    """
    import pandas as pd

    names = [name for name, _ in columns]
    records = []
    for row in df[names].itertuples(index=False, name=None):
        record = []
        for value, (_, pg_type) in zip(row, columns):
            if value is None or pd.isna(value):
                record.append(None)
            elif pg_type in _INT_TYPES:
                record.append(int(value))
            elif pg_type == "DOUBLE PRECISION":
                record.append(float(value))
            else:
                # TEXT and DATE both go over the wire as strings —
                # dates are already ISO "YYYY-MM-DD" partition values.
                record.append(str(value))
        records.append(tuple(record))
    return records


def create_table_sql(table: str, cfg: dict) -> str:
    cols = ", ".join(f'"{name}" {pg_type}' for name, pg_type in cfg["columns"])
    pk = ", ".join(f'"{c}"' for c in cfg["primary_key"])
    return f'CREATE TABLE IF NOT EXISTS "{table}" ({cols}, PRIMARY KEY ({pk}))'


def load_table(cursor, bucket: str, table: str, cfg: dict,
               date_str: str, full_refresh: bool) -> int:
    """Loads one Gold table into PostgreSQL. Returns the row count inserted.

    The caller commits/rolls back — each table is its own transaction, so a
    failure in one table never leaves another half-loaded, and never blocks
    the remaining tables from loading (same per-step isolation the analytics
    Lambda uses for its metrics).
    """
    per_date = cfg["date_scoped"] and not full_refresh

    logger.info(f"[{table}] reading gold ({'date=' + date_str if per_date else 'full table'})...")
    df = read_gold(bucket, table, date_str if per_date else None)
    logger.info(f"[{table}] {len(df)} row(s) read")

    cursor.execute(create_table_sql(table, cfg))

    if df.empty:
        # Nothing in Gold for this scope — leave whatever is already in
        # Postgres untouched, mirroring write_gold's skip-empty behavior.
        logger.info(f"[{table}] nothing to load (empty result)")
        return 0

    missing = [name for name, _ in cfg["columns"] if name not in df.columns]
    if missing:
        raise ValueError(f"gold/{table} is missing expected column(s): {missing}")

    if per_date:
        cursor.execute(f'DELETE FROM "{table}" WHERE "date" = %s', (date_str,))
    else:
        # full refresh, or the static top_x_users_followers snapshot
        cursor.execute(f'TRUNCATE TABLE "{table}"')

    records = to_records(df, cfg["columns"])
    placeholders = ", ".join(["%s"] * len(cfg["columns"]))
    col_list = ", ".join(f'"{name}"' for name, _ in cfg["columns"])
    cursor.executemany(
        f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})',
        records,
    )
    return len(records)


def lambda_handler(event, context):
    import pg8000.dbapi

    bucket = os.environ["DATA_LAKE_BUCKET"]
    event = event or {}
    full_refresh = bool(event.get("full_refresh"))
    target_date = parse_target_date(event)

    logger.info(
        f"Delivering gold -> PostgreSQL "
        f"({'FULL REFRESH' if full_refresh else 'date ' + target_date})"
    )

    # timeout=10 so a wrong host/SG misconfiguration fails fast with a clear
    # error instead of silently eating the whole Lambda timeout — the exact
    # failure mode the missing S3 egress rule produced once already.
    conn = pg8000.dbapi.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        timeout=10,
    )

    summary: dict[str, int] = {}
    errors: dict[str, str] = {}
    try:
        cursor = conn.cursor()
        for table, cfg in GOLD_TABLES.items():
            try:
                summary[table] = load_table(
                    cursor, bucket, table, cfg, target_date, full_refresh
                )
                conn.commit()
                logger.info(f"[{table}] committed {summary[table]} row(s)")
            except Exception as e:
                conn.rollback()
                logger.exception(f"{table} failed: {e}")
                errors[table] = f"{type(e).__name__}: {e}"
    finally:
        conn.close()

    logger.info(f"Delivery summary: {summary}")
    if errors:
        logger.warning(f"Tables with errors: {errors}")
    return {
        "date": target_date,
        "full_refresh": full_refresh,
        "summary": summary,
        "errors": errors,
    }
