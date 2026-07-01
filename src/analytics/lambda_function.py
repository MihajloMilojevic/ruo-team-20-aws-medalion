"""
Gold layer — Transformation and metrics
=========================================
Reads Parquet from silver/ (written by normalization_hn / normalization_x)
and computes the 8 spec metrics + the Data Quality KPI (see
GOLD_LAYER_HANDOFF.md, section 3), writing each as partitioned Parquet under
gold/. One Lambda, one pass over Silver — see the handoff doc section 1 for
why this isn't split per-metric.

Optional event parameter:
    date (str): date in YYYY-MM-DD format to compute daily HN metrics for.
                Defaults to yesterday if not provided (same default as
                ingestion/normalization_hn, so a plain daily invoke picks up
                what normalization_hn just wrote).

X metrics (daily_users_metric's X rows) are computed across every historical
date present in Silver, not just the target day — X data is bulk-uploaded and
sparse, so a single "yesterday" run would leave the X charts empty (handoff
doc 5.2).
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from gold_common import safe_read_parquet, write_gold

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SILVER_TABLES = [
    "users", "posts", "post_engagement", "hashtags",
    "post_hashtags", "urls", "post_urls", "user_snapshots", "sources",
]


def parse_target_date(event: dict) -> datetime:
    if "date" in event:
        try:
            date = datetime.strptime(event["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            logger.info(f"Using date from event: {event['date']}")
            return date
        except ValueError:
            logger.warning(f"Invalid date '{event['date']}', falling back to yesterday")
    return datetime.now(timezone.utc) - timedelta(days=1)


# ── Metric 1 — daily_post_counts ─────────────────────────────────────────────

def metric_daily_post_counts(posts_day, target_date: datetime):
    import pandas as pd

    cols = ["date", "platform", "post_type", "count"]
    if posts_day.empty:
        return pd.DataFrame(columns=cols)

    grouped = (
        posts_day.groupby(["platform", "post_type"])
        .size()
        .reset_index(name="count")
    )
    grouped.insert(0, "date", target_date.strftime("%Y-%m-%d"))
    return grouped[cols]


# ── Metrics 2+3 — daily_users_metric ─────────────────────────────────────────

def _first_seen_dates(users_df, posts_all, platform: str):
    """Returns a Series indexed by user_id -> the date that user was first
    observed: their account_created_at date if known, else the date of their
    earliest post on this platform (handoff doc 5.3's documented fallback).
    Users with neither signal are excluded from the new_users timeline —
    a documented limitation rather than an attempt to backdate them.

    Built from plain dicts/python date objects rather than pandas Series
    arithmetic — combining an all-NaT datetime column with an object column
    via combine_first/concat silently upcasts to datetime64, which then
    can't be compared against a plain datetime.date later on."""
    import pandas as pd

    if users_df.empty or "user_id" not in users_df.columns:
        return pd.Series(dtype="object")

    def to_date(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        try:
            ts = pd.to_datetime(value, utc=True, errors="coerce")
        except (ValueError, TypeError):
            return None
        if ts is None or pd.isna(ts):
            return None
        return ts.date()

    # Earliest post date per author, on this platform — the fallback signal.
    first_post: dict[str, object] = {}
    platform_posts = (
        posts_all[posts_all["platform"] == platform]
        if not posts_all.empty and "platform" in posts_all.columns
        else posts_all.iloc[0:0]
    )
    if not platform_posts.empty:
        for uid, created_at in zip(platform_posts["author_user_id"], platform_posts["created_at"]):
            if not uid:
                continue
            d = to_date(created_at)
            if d is None:
                continue
            if uid not in first_post or d < first_post[uid]:
                first_post[uid] = d

    # account_created_at, where present, takes priority over the post fallback.
    combined = dict(first_post)
    account_col = users_df["account_created_at"] if "account_created_at" in users_df.columns else None
    for uid, account_created_at in zip(users_df["user_id"], account_col if account_col is not None else []):
        d = to_date(account_created_at)
        if d is not None:
            combined[uid] = d

    return pd.Series(combined, dtype="object")


def metric_daily_users(users_hn, users_x, posts_all, target_date: datetime):
    """HN emits one row for the target date only (daily cron cadence).
    X emits one row per historical date present in its first-seen timeline,
    so the X series in Superset isn't a single near-empty point
    (handoff doc 5.2/5.3)."""
    import pandas as pd

    cols = ["date", "platform", "total_users", "new_users"]
    rows = []

    for platform, users_df in (("HackerNews", users_hn), ("X", users_x)):
        first_seen = _first_seen_dates(users_df, posts_all, platform)
        if first_seen.empty:
            continue

        sorted_dates = first_seen.sort_values()
        target_dates = [target_date.date()] if platform == "HackerNews" else sorted(first_seen.unique())

        for d in target_dates:
            rows.append({
                "date":         d.isoformat(),
                "platform":     platform,
                "total_users":  int((sorted_dates <= d).sum()),
                "new_users":    int((sorted_dates == d).sum()),
            })

    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


# ── Metric 4 — top_x_users_followers ─────────────────────────────────────────

def metric_top_x_followers(users_x):
    import pandas as pd

    cols = ["rank", "username", "followers_count", "platform"]
    if users_x.empty or "followers_count" not in users_x.columns:
        return pd.DataFrame(columns=cols)

    df = users_x.dropna(subset=["followers_count"]).copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    df = df.sort_values("followers_count", ascending=False).head(10).reset_index(drop=True)
    df["rank"] = df.index + 1
    df["platform"] = "X"
    return df[cols]


# ── Metrics 5+6 — top / bottom hn_users_karma ────────────────────────────────

def metric_hn_karma(users_hn, target_date: datetime, ascending: bool):
    """Empty-but-schema-correct until karma is backfilled — karma_score is
    not present in the Algolia payload (handoff doc §2, recommended option a)."""
    import pandas as pd

    cols = ["date", "rank", "username", "karma_score"]
    if users_hn.empty or "karma_score" not in users_hn.columns:
        return pd.DataFrame(columns=cols)

    df = users_hn.dropna(subset=["karma_score"])
    if df.empty:
        return pd.DataFrame(columns=cols)

    df = df.sort_values("karma_score", ascending=ascending).head(10).reset_index(drop=True)
    df["rank"] = df.index + 1
    df["date"] = target_date.strftime("%Y-%m-%d")
    return df[cols]


# ── Metrics 7+8 — top_hn_jobs_score / top_hn_stories_score ──────────────────

def metric_top_hn_by_score(posts_day, post_type: str, target_date: datetime):
    import pandas as pd

    cols = ["date", "rank", "post_id", "title", "score"]
    if posts_day.empty:
        return pd.DataFrame(columns=cols)

    df = posts_day[
        (posts_day.get("platform") == "HackerNews") & (posts_day.get("post_type") == post_type)
    ]
    df = df.dropna(subset=["score"])
    if df.empty:
        return pd.DataFrame(columns=cols)

    df = df.sort_values("score", ascending=False).head(10).reset_index(drop=True)
    df["rank"] = df.index + 1
    df["date"] = target_date.strftime("%Y-%m-%d")
    return df[cols]


# ── KPI — data_quality_score ─────────────────────────────────────────────────

def metric_data_quality(bucket: str, target_date: datetime):
    """Percentage of non-null cells per Silver table. HN's structurally-null
    columns (karma, followers, is_verified) drag `users` down — that's
    expected: it reflects source coverage, not a normalization bug
    (handoff doc section 3, KPI note)."""
    import pandas as pd

    rows = []
    for table in SILVER_TABLES:
        df = safe_read_parquet(f"s3://{bucket}/silver/{table}/", dataset=True)
        row_count = len(df)
        col_count = len(df.columns) if row_count else 0

        non_null_pct = None
        if row_count and col_count:
            non_null_pct = round((1 - df.isnull().sum().sum() / (row_count * col_count)) * 100, 2)

        rows.append({
            "date":          target_date.strftime("%Y-%m-%d"),
            "table_name":    table,
            "non_null_pct":  non_null_pct,
            "row_count":     row_count,
            "column_count":  col_count,
        })

    return pd.DataFrame(rows)


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    bucket = os.environ["DATA_LAKE_BUCKET"]
    event = event or {}
    target_date = parse_target_date(event)

    y, m, d = target_date.year, f"{target_date.month:02d}", f"{target_date.day:02d}"
    logger.info(f"Computing Gold metrics for {target_date.strftime('%Y-%m-%d')}")

    posts_day = safe_read_parquet(
        f"s3://{bucket}/silver/posts/",
        filters=[("year", "=", str(y)), ("month", "=", m), ("day", "=", d)],
    )
    posts_all = safe_read_parquet(f"s3://{bucket}/silver/posts/")
    users_hn = safe_read_parquet(f"s3://{bucket}/silver/users/platform=HackerNews/", dataset=False)
    users_x = safe_read_parquet(f"s3://{bucket}/silver/users/platform=X/", dataset=False)

    summary = {
        "daily_post_counts": write_gold(
            metric_daily_post_counts(posts_day, target_date),
            bucket, "daily_post_counts", ["platform", "date"],
        ),
        "daily_users_metric": write_gold(
            metric_daily_users(users_hn, users_x, posts_all, target_date),
            bucket, "daily_users_metric", ["platform", "date"],
        ),
        "top_x_users_followers": write_gold(
            metric_top_x_followers(users_x),
            bucket, "top_x_users_followers", None,
        ),
        "top_hn_users_karma": write_gold(
            metric_hn_karma(users_hn, target_date, ascending=False),
            bucket, "top_hn_users_karma", ["date"],
        ),
        "bottom_hn_users_karma": write_gold(
            metric_hn_karma(users_hn, target_date, ascending=True),
            bucket, "bottom_hn_users_karma", ["date"],
        ),
        "top_hn_jobs_score": write_gold(
            metric_top_hn_by_score(posts_day, "job", target_date),
            bucket, "top_hn_jobs_score", ["date"],
        ),
        "top_hn_stories_score": write_gold(
            metric_top_hn_by_score(posts_day, "story", target_date),
            bucket, "top_hn_stories_score", ["date"],
        ),
        "data_quality_score": write_gold(
            metric_data_quality(bucket, target_date),
            bucket, "data_quality_score", ["date"],
        ),
    }

    logger.info(f"Gold write summary: {summary}")
    return {"date": target_date.strftime("%Y-%m-%d"), "summary": summary}
