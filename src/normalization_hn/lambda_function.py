"""
Silver layer — Hacker News normalization
==========================================
Reads the per-type Bronze JSON files written by the ingestion Lambda
(bronze/hacker_news/year=YYYY/month=MM/day=DD/{type}.json), normalizes them,
and writes the shared Silver schema (see SILVER_LAYER_HANDOFF.md, section 4)
as Parquet under silver/.

Populates: users, posts, post_engagement, hashtags, post_hashtags, urls,
post_urls. Does not touch sources (HN has no client/device field) or
user_snapshots (HN karma is not present in the Algolia payload — left null,
see handoff doc 2.1).

Performance rewrite (see KNOWLEDGE.md 9.11):
  - The 5 Bronze type files are fetched concurrently instead of sequentially.
  - All 7 output tables are written concurrently, each as one small Parquet
    object at a deterministic key (silver_common.put_parquet) instead of the
    old pattern of reading back the ENTIRE existing table, merging, and
    rewriting it on every single invocation. That read-modify-write was the
    actual cause of the 5-minute timeout — it made runtime grow with the
    size of the whole table's history, not with one day's worth of data.
  - No pandas anywhere in this Lambda — rows are built and processed as
    plain Python dicts/lists in a single pass, and written with pyarrow.

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

from silver_common import (
    check_success,
    epoch_to_iso,
    extract_hashtags,
    extract_urls,
    put_parquet,
    run_tag,
    silver_key,
    stable_id,
    strip_html,
    url_domain,
    user_id_for,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# max_pool_connections raised above boto3's default of 10 so the concurrent
# reads/writes below don't queue up waiting for a free HTTP connection.
s3 = boto3.client("s3", config=Config(max_pool_connections=20))

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


def read_all_types(bucket: str, prefix: str) -> dict[str, list[dict]]:
    """Fetches all 5 Bronze type files concurrently. These are independent
    GetObject calls with no data dependency between them, so the previous
    sequential version spent most of its wall-clock time just waiting on
    network I/O here for no reason."""
    results: dict[str, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=len(ITEM_TYPES)) as pool:
        futures = {pool.submit(read_type_file, bucket, prefix, t): t for t in ITEM_TYPES}
        for future in as_completed(futures):
            item_type = futures[future]
            items = future.result()
            logger.info(f"[{item_type}] read {len(items)} item(s)")
            results[item_type] = items
    return results


def normalize_item(item_type: str, item: dict) -> dict:
    """Maps one raw HN item to a `posts` row (plus a couple of helper keys
    consumed once below and stripped before writing)."""
    return {
        "post_id":         str(item.get("objectID")),
        "platform":        PLATFORM,
        "post_type":       item_type,
        "content_text":    strip_html(item.get("text")),
        "title":           item.get("title"),
        "url":             item.get("url"),
        "created_at":      epoch_to_iso(item.get("created_at_i")),
        "score":           item.get("points"),
        "parent_post_id":  str(item["parent_id"]) if item.get("parent_id") is not None else None,
        "root_post_id":    str(item["story_id"]) if item.get("story_id") is not None else None,
        "lang":            None,
        "source_id":       None,
        "is_retweet":      False,
        "source_dataset":  SOURCE_DATASET,
        "author_user_id":  user_id_for(PLATFORM, item.get("author")),
        # helper-only, popped in the single pass below, never written out
        "_author":         item.get("author"),
        "_num_comments":   item.get("num_comments"),
    }


def lambda_handler(event, context):
    bucket = os.environ["DATA_LAKE_BUCKET"]
    date = parse_target_date(event)
    prefix = bronze_prefix(date)
    tag = run_tag(date, "hn")

    logger.info(f"Normalizing HN partition: {prefix}")

    if not check_success(s3, bucket, prefix):
        logger.warning(f"No _SUCCESS marker at {prefix}, skipping")
        return {"message": "no_success_marker", "prefix": prefix}

    type_results = read_all_types(bucket, prefix)
    all_items = [(t, item) for t, items in type_results.items() for item in items]

    if not all_items:
        logger.warning("No items found in partition, nothing to normalize")
        return {"message": "no_data", "prefix": prefix}

    # Deduplicate by objectID (handoff doc 5.4) — last write wins.
    posts_by_id: dict[str, dict] = {}
    for item_type, item in all_items:
        row = normalize_item(item_type, item)
        posts_by_id[row["post_id"]] = row

    # Single pass over the deduplicated items builds every output table at
    # once — the original version looped over posts_rows separately for
    # users, engagement, hashtags, and urls (4 extra full passes).
    seen_users: set[str] = set()
    posts_rows: list[dict] = []
    users_rows: list[dict] = []
    engagement_rows: list[dict] = []
    hashtags_by_id: dict[str, dict] = {}
    post_hashtag_rows: list[dict] = []
    urls_by_id: dict[str, dict] = {}
    post_url_rows: list[dict] = []

    for row in posts_by_id.values():
        author = row.pop("_author")
        num_comments = row.pop("_num_comments")

        if author and author not in seen_users:
            seen_users.add(author)
            users_rows.append({
                "user_id":             user_id_for(PLATFORM, author),
                "username":            author,
                "display_name":        author,
                "platform":            PLATFORM,
                "karma_score":         None,  # not in the Algolia payload, see handoff 2.1
                "followers_count":     None,
                "friends_count":       None,
                "favourites_count":    None,
                "statuses_count":      None,
                "is_verified":         None,
                "account_created_at":  None,
                "location":            None,
                "description":         None,
                "source_dataset":      SOURCE_DATASET,
            })

        if num_comments is not None:
            engagement_rows.append({
                "post_id":        row["post_id"],
                "favorite_count": None,
                "retweet_count":  None,
                "reply_count":    None,
                "num_comments":   num_comments,
            })

        for hashtag in extract_hashtags(f"{row['title'] or ''} {row['content_text'] or ''}"):
            hashtag_id = stable_id("hashtag", hashtag)
            hashtags_by_id[hashtag_id] = {"hashtag_id": hashtag_id, "tag": hashtag}
            post_hashtag_rows.append({"post_id": row["post_id"], "hashtag_id": hashtag_id})

        candidate_urls = set(extract_urls(row["content_text"]))
        if row["url"]:
            candidate_urls.add(row["url"])
        for url in candidate_urls:
            url_id = stable_id("url", url)
            urls_by_id[url_id] = {"url_id": url_id, "url": url, "domain": url_domain(url)}
            post_url_rows.append({"post_id": row["post_id"], "url_id": url_id})

        posts_rows.append(row)

    # HN Bronze is already partitioned by day, so every item in this
    # invocation belongs to the same year/month/day — no per-row timestamp
    # parsing needed to compute the partition (the old pandas
    # to_datetime/.dt.year/.dt.month pass is gone entirely).
    writes = {
        "posts": (
            silver_key("posts", tag, {"year": date.year, "month": f"{date.month:02d}", "day": f"{date.day:02d}"}),
            posts_rows,
        ),
        "users":          (silver_key("users", tag, {"platform": PLATFORM}), users_rows),
        "post_engagement": (silver_key("post_engagement", tag), engagement_rows),
        "hashtags":       (silver_key("hashtags", tag), list(hashtags_by_id.values())),
        "post_hashtags":  (silver_key("post_hashtags", tag), post_hashtag_rows),
        "urls":           (silver_key("urls", tag), list(urls_by_id.values())),
        "post_urls":      (silver_key("post_urls", tag), post_url_rows),
    }

    summary: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=len(writes)) as pool:
        futures = {
            pool.submit(put_parquet, s3, bucket, name, key, rows): name
            for name, (key, rows) in writes.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            summary[name] = future.result()

    logger.info(f"Silver write summary: {summary}")
    return {"prefix": prefix, "summary": summary}
