"""
Gold layer — Transformation and metrics
=========================================
Reads Parquet from silver/ (written by normalization_hn / normalization_x)
and computes the 8 spec metrics + the Data Quality KPI (see
GOLD_LAYER_HANDOFF.md, section 3), writing each as partitioned Parquet under
gold/. One Lambda, one pass over Silver — see the handoff doc section 1 for
why this isn't split per-metric.

Required event parameter for a full backfill workflow:
    date (str): date in YYYY-MM-DD format to compute metrics for, for BOTH
                platforms. Defaults to yesterday if not provided (same
                default as ingestion/normalization_hn).

Every date-scoped metric — including daily_users_metric for X — computes
strictly for this one target date now, uniformly across both platforms.
(Previously X was special-cased to compute across every historical date in
one run, to avoid an empty chart on a single "yesterday" invoke — removed
by request: the intended workflow now is to invoke this Lambda once per
date across the desired range as a one-time backfill once the project's
data collection is complete, e.g. from 2020-07-24 onward now that Congress
support has been removed and COVID is the earliest X source.)
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

# Primary key per Silver table, used to collapse the "one small file per
# Silver run" layout back into one row per entity when Gold reads a table in
# full (see gold_common.safe_read_parquet's dedupe_subset docstring).
# user_snapshots is intentionally excluded — it's a genuine append-only time
# series, multiple rows per user are expected and meaningful there.
SILVER_PRIMARY_KEYS = {
    "users":           "user_id",
    "posts":           "post_id",
    "post_engagement": "post_id",
    "hashtags":        "hashtag_id",
    "post_hashtags":   ["post_id", "hashtag_id"],
    "urls":            "url_id",
    "post_urls":       ["post_id", "url_id"],
    "sources":         "source_id",
}


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

    Merged from plain dicts/python date objects rather than pandas Series
    arithmetic — combining an all-NaT datetime column with an object column
    via combine_first/concat silently upcasts to datetime64, which then
    can't be compared against a plain datetime.date later on (see
    KNOWLEDGE.md 9.10).

    Date PARSING deliberately avoids pandas/pyarrow's native datetime
    machinery entirely (no pd.to_datetime, vectorized or scalar). Both a
    per-scalar pd.to_datetime() loop and a vectorized pd.to_datetime(series)
    call crashed the process at exactly this step in production — same
    logical spot either way, which points at pandas' own datetime-parsing
    C code in this Lambda environment, not at looping vs. vectorizing.
    Our created_at/account_created_at values are never arbitrary strings
    needing pandas' flexible format inference in the first place — every
    writer in this project (silver_common.epoch_to_iso /
    str_datetime_to_iso) always emits datetime.fromtimestamp(...).isoformat()
    or an equivalent strict ISO-8601 string, or None. Python's stdlib
    datetime.fromisoformat() parses that directly with no numpy/pyarrow
    C-extension involvement at all.
    """
    import pandas as pd
    from datetime import datetime as _dt

    if users_df.empty or "user_id" not in users_df.columns:
        return pd.Series(dtype="object")

    def parse_iso_date(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        try:
            return _dt.fromisoformat(str(value)).date()
        except ValueError:
            return None

    def parsed_dates(series: "pd.Series"):
        # .map() still iterates per-element, but through stdlib
        # datetime.fromisoformat rather than pandas' own datetime parser —
        # that's the part that crashed twice, not the iteration itself.
        return series.map(parse_iso_date)

    # Earliest post date per author, on this platform — the fallback signal.
    logger.info(f"[_first_seen_dates:{platform}] filtering posts_all by platform...")
    first_post: dict[str, object] = {}
    platform_posts = (
        posts_all[posts_all["platform"] == platform]
        if not posts_all.empty and "platform" in posts_all.columns
        else posts_all.iloc[0:0]
    )
    logger.info(f"[_first_seen_dates:{platform}] {len(platform_posts)} post(s) for this platform")
    if not platform_posts.empty:
        logger.info(f"[_first_seen_dates:{platform}] parsing post created_at dates...")
        post_dates = parsed_dates(platform_posts["created_at"])
        logger.info(f"[_first_seen_dates:{platform}] merging post-based first-seen dates...")
        for uid, d in zip(platform_posts["author_user_id"], post_dates):
            if not uid or d is None:
                continue
            if uid not in first_post or d < first_post[uid]:
                first_post[uid] = d
    logger.info(f"[_first_seen_dates:{platform}] first_post has {len(first_post)} entries")

    # account_created_at, where present, takes priority over the post fallback.
    combined = dict(first_post)
    if "account_created_at" in users_df.columns:
        logger.info(f"[_first_seen_dates:{platform}] parsing account_created_at dates...")
        account_dates = parsed_dates(users_df["account_created_at"])
        logger.info(f"[_first_seen_dates:{platform}] merging account-based first-seen dates...")
        for uid, d in zip(users_df["user_id"], account_dates):
            if d is not None:
                combined[uid] = d
    logger.info(f"[_first_seen_dates:{platform}] combined has {len(combined)} entries")

    return pd.Series(combined, dtype="object")



def metric_daily_users(users_hn, users_x, posts_all, target_date: datetime):
    """Total/new users for target_date, for both platforms uniformly.
    Backfilling the full X history is done by invoking this Lambda once per
    date over the desired range, not by this function computing every date
    in one run — see the module docstring for why that special-case was
    removed."""
    import pandas as pd

    cols = ["date", "platform", "total_users", "new_users"]
    rows = []
    d = target_date.date()

    for platform, users_df in (("HackerNews", users_hn), ("X", users_x)):
        logger.info(f"[daily_users_metric] {platform}: computing first-seen dates ({len(users_df)} user row(s))...")
        first_seen = _first_seen_dates(users_df, posts_all, platform)
        logger.info(f"[daily_users_metric] {platform}: first_seen has {len(first_seen)} entries")
        if first_seen.empty:
            continue

        sorted_dates = first_seen.sort_values()
        rows.append({
            "date":         d.isoformat(),
            "platform":     platform,
            "total_users":  int((sorted_dates <= d).sum()),
            "new_users":    int((sorted_dates == d).sum()),
        })
        logger.info(f"[daily_users_metric] {platform}: done")

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
        logger.info(f"[data_quality] reading {table}...")
        df = safe_read_parquet(
            f"s3://{bucket}/silver/{table}/",
            dataset=True,
            dedupe_subset=SILVER_PRIMARY_KEYS.get(table),
        )
        logger.info(f"[data_quality] {table}: {len(df)} row(s)")
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

    logger.info(f"Reading silver/posts/ (day partition: year={y}/month={m}/day={d})")
    # Reading the exact partition path instead of filters=[("year","=",str(y)),...]
    # deliberately: pyarrow's Hive partition discovery infers year/month/day
    # as int32 from the folder names (year=2026 -> 2026 the int), but the old
    # filters approach compared that against string values ("2026") -- a
    # TypeError on every single run, silently swallowed by safe_read_parquet
    # into "treating as empty". Reading the folder directly sidesteps
    # partition-type inference entirely.
    posts_day = safe_read_parquet(
        f"s3://{bucket}/silver/posts/year={y}/month={m}/day={d}/",
        dedupe_subset="post_id",
    )
    logger.info(f"posts_day: {len(posts_day)} row(s)")

    logger.info("Reading silver/posts/ (full history)")
    posts_all = safe_read_parquet(f"s3://{bucket}/silver/posts/", dedupe_subset="post_id")
    logger.info(f"posts_all: {len(posts_all)} row(s)")
    # metric_daily_users (via _first_seen_dates) only ever reads
    # author_user_id/platform/created_at from this. The full read includes
    # content_text/title/url for every post in the table's history, which
    # was sitting in memory for no reason right alongside users_hn/users_x —
    # likely what pushed a 512MB Lambda into an OOM-adjacent native crash.
    _posts_all_cols = [c for c in ("author_user_id", "platform", "created_at") if c in posts_all.columns]
    posts_all = posts_all[_posts_all_cols].copy() if _posts_all_cols else posts_all

    logger.info("Reading silver/users/platform=HackerNews/")
    # dataset=True, not False: normalization_hn now writes one file per run
    # under this platform folder (see silver_common.put_parquet), so this
    # folder will hold multiple files as soon as it's been run more than
    # once. dataset=False requires an exact single object key and only
    # ever worked here by coincidence while exactly one file existed.
    users_hn = safe_read_parquet(
        f"s3://{bucket}/silver/users/platform=HackerNews/", dedupe_subset="user_id",
    )
    logger.info(f"users_hn: {len(users_hn)} row(s)")

    logger.info("Reading silver/users/platform=X/")
    users_x = safe_read_parquet(
        f"s3://{bucket}/silver/users/platform=X/", dedupe_subset="user_id",
    )
    logger.info(f"users_x: {len(users_x)} row(s)")

    # Each metric is computed and written as its own logged step, wrapped in
    # its own try/except. Two reasons: (1) a normal Python exception in one
    # metric no longer takes down every other metric in the run — you get
    # partial Gold output plus a clear traceback instead of nothing; (2) if
    # something crashes the whole process at the native level (segfault —
    # not catchable in Python at all), the LAST "Computing ..." line that
    # made it to CloudWatch before the process died tells you exactly which
    # metric to investigate, since nothing else narrows it down.
    steps = [
        ("daily_post_counts", lambda: metric_daily_post_counts(posts_day, target_date), ["platform", "date"]),
        ("daily_users_metric", lambda: metric_daily_users(users_hn, users_x, posts_all, target_date), ["platform", "date"]),
        ("top_x_users_followers", lambda: metric_top_x_followers(users_x), None),
        ("top_hn_users_karma", lambda: metric_hn_karma(users_hn, target_date, ascending=False), ["date"]),
        ("bottom_hn_users_karma", lambda: metric_hn_karma(users_hn, target_date, ascending=True), ["date"]),
        ("top_hn_jobs_score", lambda: metric_top_hn_by_score(posts_day, "job", target_date), ["date"]),
        ("top_hn_stories_score", lambda: metric_top_hn_by_score(posts_day, "story", target_date), ["date"]),
        ("data_quality_score", lambda: metric_data_quality(bucket, target_date), ["date"]),
    ]

    summary: dict[str, int] = {}
    errors: dict[str, str] = {}
    for name, compute_fn, partition_cols in steps:
        logger.info(f"Computing {name}...")
        try:
            df = compute_fn()
            logger.info(f"Computed {name}: {len(df)} row(s)")
            summary[name] = write_gold(df, bucket, name, partition_cols)
            logger.info(f"Wrote {name}")
        except Exception as e:
            logger.exception(f"{name} failed: {e}")
            errors[name] = f"{type(e).__name__}: {e}"

    logger.info(f"Gold write summary: {summary}")
    if errors:
        logger.warning(f"Gold metrics with errors: {errors}")
    return {"date": target_date.strftime("%Y-%m-%d"), "summary": summary, "errors": errors}
