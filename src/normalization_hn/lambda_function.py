"""
Silver layer — Hacker News normalization
==========================================
Reads the per-type Bronze JSON files written by the ingestion Lambda
(bronze/hacker_news/year=YYYY/month=MM/day=DD/{type}.json), normalizes them,
and writes the shared Silver schema (see SILVER_LAYER_HANDOFF.md, section 4)
as partitioned Parquet under silver/.

Populates: users, posts, post_engagement, hashtags, post_hashtags, urls,
post_urls. Does not touch sources (HN has no client/device field) or
user_snapshots (HN karma is not present in the Algolia payload — left null,
see handoff doc 2.1).

_SUCCESS check:
    The partition is skipped if bronze/hacker_news/.../_SUCCESS is missing,
    mirroring the write-atomicity contract from the ingestion Lambda.

Optional event parameter:
    date (str): date in YYYY-MM-DD format to normalize. Defaults to
                yesterday if not provided (same default as ingestion, so a
                plain daily invoke normalizes what ingestion just wrote).
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

from silver_common import (
    check_success,
    epoch_to_iso,
    extract_hashtags,
    extract_urls,
    stable_id,
    strip_html,
    upsert_dimension_table,
    upsert_users,
    url_domain,
    user_id_for,
    write_partitioned,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

ITEM_TYPES = ["comment", "story", "ask_hn", "job", "poll"]
PLATFORM = "HackerNews"
SOURCE_DATASET = "hackernews"


def parse_target_date(event: dict) -> datetime:
    if "date" in event:
        try:
            date = datetime.strptime(event["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            logger.info(f"Using date from event: {event['date']}")
            return date
        except ValueError:
            logger.warning(f"Invalid date '{event['date']}', falling back to yesterday")
    return datetime.now(timezone.utc) - timedelta(days=1)


def bronze_prefix(date: datetime) -> str:
    return f"bronze/hacker_news/year={date.year}/month={date.month:02d}/day={date.day:02d}"


def read_type_file(bucket: str, prefix: str, item_type: str) -> list[dict]:
    key = f"{prefix}/{item_type}.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.warning(f"Could not read {key}: {e}")
        return []

    body = json.loads(obj["Body"].read())
    return body.get("items", [])


def normalize_item(item_type: str, item: dict) -> dict:
    """Maps one raw HN item to a `posts` row."""
    author = item.get("author")
    content_text = strip_html(item.get("text"))
    title = item.get("title")

    return {
        "post_id":         str(item.get("objectID")),
        "author_user_id":  user_id_for(PLATFORM, author),
        "platform":        PLATFORM,
        "post_type":       item_type,
        "content_text":    content_text,
        "title":           title,
        "url":             item.get("url"),
        "created_at":      epoch_to_iso(item.get("created_at_i")),
        "score":           item.get("points"),
        "parent_post_id":  str(item["parent_id"]) if item.get("parent_id") is not None else None,
        "root_post_id":    str(item["story_id"]) if item.get("story_id") is not None else None,
        "lang":            None,
        "source_id":       None,
        "is_retweet":      False,
        "source_dataset":  SOURCE_DATASET,
        # kept only to compute post_engagement / partitioning, dropped before write
        "_num_comments":   item.get("num_comments"),
        "_author":         author,
    }


def lambda_handler(event, context):
    bucket = os.environ["DATA_LAKE_BUCKET"]
    date = parse_target_date(event)
    prefix = bronze_prefix(date)

    logger.info(f"Normalizing HN partition: {prefix}")

    if not check_success(s3, bucket, prefix):
        logger.warning(f"No _SUCCESS marker at {prefix}, skipping")
        return {"message": "no_success_marker", "prefix": prefix}

    import pandas as pd

    all_items: list[tuple[str, dict]] = []
    for item_type in ITEM_TYPES:
        items = read_type_file(bucket, prefix, item_type)
        logger.info(f"[{item_type}] read {len(items)} item(s)")
        all_items.extend((item_type, item) for item in items)

    if not all_items:
        logger.warning("No items found in partition, nothing to normalize")
        return {"message": "no_data", "prefix": prefix}

    posts_rows = [normalize_item(item_type, item) for item_type, item in all_items]

    # Deduplicate by objectID (handoff doc 5.4) — last write wins.
    posts_by_id: dict[str, dict] = {row["post_id"]: row for row in posts_rows}
    posts_rows = list(posts_by_id.values())

    # ── users ──────────────────────────────────────────────────────────────
    usernames = sorted({row["_author"] for row in posts_rows if row["_author"]})
    users_df = pd.DataFrame([
        {
            "user_id":             user_id_for(PLATFORM, username),
            "username":            username,
            "display_name":        username,
            "platform":            PLATFORM,
            "karma_score":         None,  # not present in the Algolia payload, see handoff 2.1
            "followers_count":     None,
            "friends_count":       None,
            "favourites_count":    None,
            "statuses_count":      None,
            "is_verified":         None,
            "account_created_at":  None,
            "location":            None,
            "description":         None,
            "source_dataset":      SOURCE_DATASET,
        }
        for username in usernames
    ])

    # ── post_engagement (stories/jobs have points + num_comments) ───────────
    engagement_rows = [
        {
            "post_id":       row["post_id"],
            "favorite_count": None,
            "retweet_count":  None,
            "reply_count":    None,
            "num_comments":   row["_num_comments"],
        }
        for row in posts_rows
        if row["_num_comments"] is not None
    ]
    engagement_df = pd.DataFrame(engagement_rows)

    # ── hashtags / post_hashtags ─────────────────────────────────────────────
    hashtag_rows, post_hashtag_rows = [], []
    for row in posts_rows:
        tags = extract_hashtags(f"{row['title'] or ''} {row['content_text'] or ''}")
        for tag in tags:
            hashtag_id = stable_id("hashtag", tag)
            hashtag_rows.append({"hashtag_id": hashtag_id, "tag": tag})
            post_hashtag_rows.append({"post_id": row["post_id"], "hashtag_id": hashtag_id})
    hashtags_df = pd.DataFrame(hashtag_rows)
    post_hashtags_df = pd.DataFrame(post_hashtag_rows)

    # ── urls / post_urls ──────────────────────────────────────────────────────
    url_rows, post_url_rows = [], []
    for row in posts_rows:
        candidate_urls = set(extract_urls(row["content_text"]))
        if row["url"]:
            candidate_urls.add(row["url"])
        for url in candidate_urls:
            url_id = stable_id("url", url)
            url_rows.append({"url_id": url_id, "url": url, "domain": url_domain(url)})
            post_url_rows.append({"post_id": row["post_id"], "url_id": url_id})
    urls_df = pd.DataFrame(url_rows)
    post_urls_df = pd.DataFrame(post_url_rows)

    # ── posts (drop helper columns, add partition columns) ───────────────────
    posts_df = pd.DataFrame(posts_rows).drop(columns=["_num_comments", "_author"])
    created = pd.to_datetime(posts_df["created_at"], utc=True, errors="coerce")
    posts_df["year"] = created.dt.year.fillna(date.year).astype(int)
    posts_df["month"] = created.dt.month.fillna(date.month).astype(int).map(lambda m: f"{m:02d}")
    posts_df["day"] = created.dt.day.fillna(date.day).astype(int).map(lambda d: f"{d:02d}")

    summary = {
        "posts":          write_partitioned(posts_df, bucket, "posts", ["year", "month", "day"]),
        "users":          upsert_users(bucket, PLATFORM, users_df),
        "post_engagement": upsert_dimension_table(bucket, "post_engagement", engagement_df, ["post_id"]),
        "hashtags":       upsert_dimension_table(bucket, "hashtags", hashtags_df, ["hashtag_id"]),
        "post_hashtags":  upsert_dimension_table(bucket, "post_hashtags", post_hashtags_df, ["post_id", "hashtag_id"]),
        "urls":           upsert_dimension_table(bucket, "urls", urls_df, ["url_id"]),
        "post_urls":      upsert_dimension_table(bucket, "post_urls", post_urls_df, ["post_id", "url_id"]),
    }

    logger.info(f"Silver write summary: {summary}")
    return {"prefix": prefix, "summary": summary}
