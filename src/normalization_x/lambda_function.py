"""
Silver layer — X (Twitter) normalization
==========================================
Reads the Bronze X data prepared locally by build_chunks.py and uploaded to
bronze/x/ (see SILVER_LAYER_HANDOFF.md, section 3), normalizes it, and
writes the shared Silver schema (section 4) as Parquet under silver/ — the
SAME tables normalization_hn writes.

Scope: COVID and Bitcoin (gpreda-style CSV) only. Congress support has been
removed from the project — there is no NDJSON/Twitter-API-JSON branch here
anymore, dispatch is purely CSV.

Bronze layout recap:
    bronze/x/year=YYYY/month=MM/day=DD/covid.csv      <- gpreda CSV
    bronze/x/year=YYYY/month=MM/day=DD/bitcoin.csv    <- gpreda CSV
    bronze/x/year=YYYY/month=MM/day=DD/_SUCCESS

Every X day is single-source (COVID and Bitcoin cover non-overlapping date
ranges), so the parser to use is dispatched purely by filename.

Performance rewrite (see KNOWLEDGE.md 9.11), mirroring normalization_hn:
  - No pandas anywhere — CSV rows are read with the stdlib `csv` module and
    processed as plain dicts, dropping the pandas `iterrows()` loop (one of
    the slowest common pandas patterns) entirely.
  - No read-modify-write "upsert" — every output table is written as one
    small Parquet object per (day, source) at a deterministic key
    (silver_common.put_parquet), instead of reading back the whole existing
    table on every invocation. See put_parquet's docstring for the full
    rationale; this was the actual cause of Silver Lambda timeouts.
  - Malformed CSV rows (the Bitcoin dataset has some) are skipped
    defensively by comparing field count against the header, replacing the
    old `pandas.read_csv(engine="python", on_bad_lines="skip")` fallback.

Invocation modes (event):
    {"date": "YYYY-MM-DD"}                    -> normalize a single day
    {"prefix": "year=.../month=.../day=..."}  -> normalize an explicit partition
    {"full_scan": true}                       -> walk every day partition under bronze/x/
    (X datasets are bulk-uploaded by hand, so unlike HN there is no
    "yesterday" default — the caller must say what to process.)
"""

import csv
import io
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from silver_common import (
    check_success,
    extract_hashtags,
    extract_urls,
    parse_hashtag_list,
    put_parquet,
    run_tag,
    silver_key,
    stable_id,
    strip_html,
    str_datetime_to_iso,
    synth_post_id,
    url_domain,
    user_id_for,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3", config=Config(max_pool_connections=20))

PLATFORM = "X"


# ── Partition discovery ────────────────────────────────────────────────────────

def date_to_prefix(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return f"year={d.year}/month={d.month:02d}/day={d.day:02d}"


def list_day_partitions(bucket: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix="bronze/x/year="):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/_SUCCESS"):
                prefixes.append(obj["Key"][len("bronze/x/"):-len("/_SUCCESS")])
    return sorted(prefixes)


def list_partition_files(bucket: str, full_prefix: str) -> set[str]:
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{full_prefix}/")
    return {obj["Key"].split("/")[-1] for obj in resp.get("Contents", []) if obj["Key"]}


# ── small parsing helpers (stdlib only) ─────────────────────────────────────────

def _clean(v) -> str | None:
    if v is None:
        return None
    v = v.strip()
    return v or None


def _to_int(v) -> int | None:
    v = _clean(v)
    if v is None:
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def _to_bool(v) -> bool | None:
    v = _clean(v)
    if v is None:
        return None
    return v.strip().lower() in ("true", "1", "yes")


# ── gpreda CSV parser (covid.csv / bitcoin.csv — identical column set) ──────────

EXPECTED_COLUMNS = {
    "user_name", "user_location", "user_description", "user_created",
    "user_followers", "user_friends", "user_favourites", "user_verified",
    "date", "text", "hashtags", "source", "is_retweet",
}


def iter_gpreda_rows(raw_bytes: bytes):
    """Yields dict rows from a gpreda-format CSV, skipping malformed rows
    (wrong field count relative to the header) instead of raising — the
    stdlib replacement for pandas' engine="python", on_bad_lines="skip"."""
    text_stream = io.StringIO(raw_bytes.decode("utf-8", errors="replace"))
    reader = csv.reader(text_stream)
    try:
        header = next(reader)
    except StopIteration:
        return
    n_cols = len(header)
    skipped = 0
    for raw_row in reader:
        if len(raw_row) != n_cols:
            skipped += 1
            continue
        yield dict(zip(header, raw_row))
    if skipped:
        logger.warning(f"Skipped {skipped} malformed row(s)")


def process_gpreda_csv(bucket: str, key: str, source_dataset: str) -> dict:
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_bytes = obj["Body"].read()

    posts_rows, snapshot_rows = [], []
    hashtag_map: dict[str, dict] = {}
    post_hashtag_rows = []
    url_map: dict[str, dict] = {}
    post_url_rows = []
    sources_seen: dict[str, str] = {}
    user_latest: dict[str, dict] = {}

    for r in iter_gpreda_rows(raw_bytes):
        user_name = _clean(r.get("user_name"))
        if not user_name:
            continue

        date_str = _clean(r.get("date"))
        created_at = str_datetime_to_iso(date_str) if date_str else None

        text = _clean(r.get("text"))
        content_text = strip_html(text)

        post_id = synth_post_id(user_name, date_str or "", text or "")
        author_user_id = user_id_for(PLATFORM, user_name)

        source_name = _clean(r.get("source"))
        source_id = None
        if source_name:
            source_id = stable_id("source", source_name)
            sources_seen[source_id] = source_name

        is_retweet = bool(_to_bool(r.get("is_retweet")))

        posts_rows.append({
            "post_id":        post_id,
            "author_user_id": author_user_id,
            "platform":       PLATFORM,
            "post_type":      "retweet" if is_retweet else "tweet",
            "content_text":   content_text,
            "title":          None,
            "url":            None,
            "created_at":     created_at,
            "score":          None,
            "parent_post_id": None,
            "root_post_id":   None,
            "lang":           None,
            "source_id":      source_id,
            "is_retweet":     is_retweet,
            "source_dataset": source_dataset,
        })

        followers = _to_int(r.get("user_followers"))
        snapshot_rows.append({
            "user_id":         author_user_id,
            "captured_at":     created_at,
            "followers_count": followers,
            "karma_score":     None,
        })

        tags = set(parse_hashtag_list(r.get("hashtags"))) | set(extract_hashtags(text))
        for tag in tags:
            hashtag_id = stable_id("hashtag", tag)
            hashtag_map[hashtag_id] = {"hashtag_id": hashtag_id, "tag": tag}
            post_hashtag_rows.append({"post_id": post_id, "hashtag_id": hashtag_id})

        for url in extract_urls(text):
            url_id = stable_id("url", url)
            url_map[url_id] = {"url_id": url_id, "url": url, "domain": url_domain(url)}
            post_url_rows.append({"post_id": post_id, "url_id": url_id})

        # 5.6: followers_count varies per tweet row — keep only the latest-by-date
        # row for the `users` table itself; the full series lives in user_snapshots.
        prev = user_latest.get(user_name)
        if prev is None or (created_at or "") >= (prev["_created_at"] or ""):
            user_latest[user_name] = {
                "user_id":            author_user_id,
                "username":           user_name,
                "display_name":       user_name,
                "platform":           PLATFORM,
                "karma_score":        None,
                "followers_count":    followers,
                "friends_count":      _to_int(r.get("user_friends")),
                "favourites_count":   _to_int(r.get("user_favourites")),
                "statuses_count":     None,
                "is_verified":        _to_bool(r.get("user_verified")),
                "account_created_at": str_datetime_to_iso(_clean(r.get("user_created"))),
                "location":           _clean(r.get("user_location")),
                "description":        _clean(r.get("user_description")),
                "source_dataset":     source_dataset,
                "_created_at":        created_at,
            }

    users_rows = [{k: v for k, v in u.items() if k != "_created_at"} for u in user_latest.values()]
    source_rows = [{"source_id": sid, "name": name} for sid, name in sources_seen.items()]

    return {
        "posts":          posts_rows,
        "users":          users_rows,
        "post_engagement": [],  # gpreda CSVs have no per-tweet engagement counts
        "user_snapshots": snapshot_rows,
        "hashtags":       list(hashtag_map.values()),
        "post_hashtags":  post_hashtag_rows,
        "urls":           list(url_map.values()),
        "post_urls":      post_url_rows,
        "sources":        source_rows,
    }


# ── Writing ───────────────────────────────────────────────────────────────────

def write_result_tables(bucket: str, result: dict, fallback_date: datetime, source_dataset: str) -> dict:
    """Every table is one Parquet object at a deterministic key scoped to
    this (day, source_dataset) — see put_parquet's docstring. Unlike the old
    upsert helpers, nothing here reads existing Silver data back."""
    tag = run_tag(fallback_date, "x", source_dataset)
    y, m, d = fallback_date.year, f"{fallback_date.month:02d}", f"{fallback_date.day:02d}"

    writes = {
        "posts":           (silver_key("posts", tag, {"year": y, "month": m, "day": d}), result["posts"]),
        "users":           (silver_key("users", tag, {"platform": PLATFORM}), result["users"]),
        "post_engagement": (silver_key("post_engagement", tag), result["post_engagement"]),
        "user_snapshots":  (silver_key("user_snapshots", tag), result["user_snapshots"]),
        "hashtags":        (silver_key("hashtags", tag), result["hashtags"]),
        "post_hashtags":   (silver_key("post_hashtags", tag), result["post_hashtags"]),
        "urls":            (silver_key("urls", tag), result["urls"]),
        "post_urls":       (silver_key("post_urls", tag), result["post_urls"]),
        "sources":         (silver_key("sources", tag), result["sources"]),
    }

    summary = {}
    for name, (key, rows) in writes.items():
        count = put_parquet(s3, bucket, name, key, rows)
        if count:
            summary[name] = count
    return summary


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    bucket = os.environ["DATA_LAKE_BUCKET"]
    event = event or {}

    if "prefix" in event:
        day_prefixes = [event["prefix"].strip("/")]
    elif "date" in event:
        day_prefixes = [date_to_prefix(event["date"])]
    elif event.get("full_scan"):
        day_prefixes = list_day_partitions(bucket)
    else:
        return {
            "message": "no_target",
            "hint": "Provide 'date' (YYYY-MM-DD), an explicit 'prefix', or 'full_scan': true",
        }

    logger.info(f"Processing {len(day_prefixes)} X partition(s)")

    totals: dict[str, int] = defaultdict(int)
    processed, skipped = [], []

    for day_prefix in day_prefixes:
        full_prefix = f"bronze/x/{day_prefix}"

        if not check_success(s3, bucket, full_prefix):
            logger.warning(f"No _SUCCESS marker at {full_prefix}, skipping")
            skipped.append(day_prefix)
            continue

        try:
            year = int(day_prefix.split("year=")[1].split("/")[0])
            month = int(day_prefix.split("month=")[1].split("/")[0])
            day = int(day_prefix.split("day=")[1].split("/")[0])
            fallback_date = datetime(year, month, day, tzinfo=timezone.utc)
        except (IndexError, ValueError):
            fallback_date = datetime.now(timezone.utc)

        filenames = list_partition_files(bucket, full_prefix)

        if "covid.csv" in filenames:
            source_dataset = "covid"
            result = process_gpreda_csv(bucket, f"{full_prefix}/covid.csv", source_dataset)
        elif "bitcoin.csv" in filenames:
            source_dataset = "bitcoin"
            result = process_gpreda_csv(bucket, f"{full_prefix}/bitcoin.csv", source_dataset)
        else:
            logger.warning(f"No recognized data file in {full_prefix}: {filenames}")
            skipped.append(day_prefix)
            continue

        summary = write_result_tables(bucket, result, fallback_date, source_dataset)
        for table, count in summary.items():
            totals[table] += count
        processed.append(day_prefix)
        logger.info(f"[{day_prefix}] {summary}")

    return {
        "processed_partitions": processed,
        "skipped_partitions":   skipped,
        "totals":               dict(totals),
    }
