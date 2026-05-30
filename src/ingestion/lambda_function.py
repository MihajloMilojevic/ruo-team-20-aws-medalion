"""
Bronze layer — Hacker News ingestion
=====================================
Collects all items from HN for a given day using the Algolia Search API
(https://hn.algolia.com/api), which supports filtering by timestamp.

Each item type is written to a separate JSON file:
    bronze/hacker_news/year=YYYY/month=MM/day=DD/comment.json
    bronze/hacker_news/year=YYYY/month=MM/day=DD/story.json
    ...

Write atomicity:
    S3 has no native transactions, so we use the _SUCCESS marker pattern.
    All files are written first; _SUCCESS is written only after all succeed.
    The normalization Lambda will not process a partition without _SUCCESS.
    If any write fails, already-written files are deleted (rollback).

Optional event parameter:
    date (str): date in YYYY-MM-DD format to collect data for.
                Defaults to yesterday if not provided.
"""

from contextlib import contextmanager
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Generator

import boto3
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HN_SEARCH_URL    = "https://hn.algolia.com/api/v1/search_by_date"
HITS_PER_PAGE    = 1000
RATE_LIMIT_SLEEP = 0.2

# High-volume types (comments) use shorter time windows to stay under
# Algolia's hard limit of 1000 results per query.
WINDOW_HOURS: dict[str, int] = {
    "comment": 3,
    "story":   12,
    "ask_hn":  24,
    "job":     24,
    "poll":    24,
}

s3   = boto3.client("s3")
http = urllib3.PoolManager()


def parse_target_date(event: dict) -> datetime:
    """
    Determines the target date for data collection.
    Parses the 'date' key from the event if present, otherwise defaults to yesterday.
    """
    if "date" in event:
        try:
            date = datetime.strptime(event["date"], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            logger.info(f"Using date from event: {event['date']}")
            return date
        except ValueError:
            logger.warning(
                f"Invalid date format '{event['date']}', "
                f"expected YYYY-MM-DD. Falling back to yesterday."
            )

    return datetime.now(timezone.utc) - timedelta(days=1)


def s3_prefix(date: datetime) -> str:
    return (
        f"bronze/hacker_news/"
        f"year={date.year}/"
        f"month={date.month:02d}/"
        f"day={date.day:02d}"
    )


def time_windows(
    day: datetime, window_hours: int
) -> Generator[tuple[int, int], None, None]:
    """
    Splits a day into time windows of the given width and yields
    Unix timestamp pairs (from, to) for each window.
    """
    delta        = timedelta(hours=window_hours)
    window_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day   = day.replace(hour=23, minute=59, second=59, microsecond=999999)

    while window_start < end_of_day:
        window_end = min(window_start + delta, end_of_day)
        yield int(window_start.timestamp()), int(window_end.timestamp())
        window_start = window_end


def fetch_page(item_type: str, ts_from: int, ts_to: int, page: int) -> dict:
    url = (
        f"{HN_SEARCH_URL}"
        f"?tags={item_type}"
        f"&numericFilters=created_at_i>{ts_from},created_at_i<{ts_to}"
        f"&hitsPerPage={HITS_PER_PAGE}"
        f"&page={page}"
    )

    try:
        resp = http.request("GET", url, timeout=30.0)
    except urllib3.exceptions.HTTPError as e:
        logger.error(f"Network error [{item_type} p{page}]: {e}")
        return {}

    if resp.status == 429:
        logger.warning("Rate limited (429) — waiting 2s before retrying")
        time.sleep(2.0)
        return fetch_page(item_type, ts_from, ts_to, page)

    if resp.status != 200:
        logger.error(f"Unexpected status {resp.status} [{item_type} p{page}]")
        return {}

    return json.loads(resp.data.decode("utf-8"))


def fetch_type(item_type: str, day: datetime) -> list[dict]:
    window_hours = WINDOW_HOURS.get(item_type, 24)
    windows      = list(time_windows(day, window_hours))
    all_items: list[dict] = []

    logger.info(f"[{item_type}] {len(windows)} window(s) × {window_hours}h")

    for idx, (ts_from, ts_to) in enumerate(windows):
        page = 0

        while True:
            data     = fetch_page(item_type, ts_from, ts_to, page)
            hits     = data.get("hits", [])
            nb_pages = data.get("nbPages", 1)
            nb_hits  = data.get("nbHits", 0)

            all_items.extend(hits)

            if nb_hits > HITS_PER_PAGE and page == 0:
                logger.warning(
                    f"[{item_type}] window {idx + 1} has {nb_hits} items — "
                    f"consider reducing window_hours for this type"
                )

            logger.info(
                f"  [{item_type}] window {idx + 1}/{len(windows)}, "
                f"page {page + 1}/{nb_pages} → {len(hits)} items"
            )

            if page >= nb_pages - 1:
                break

            page += 1
            time.sleep(RATE_LIMIT_SLEEP)

    logger.info(f"[{item_type}] total: {len(all_items)}")
    return all_items


def upload_type(
    bucket: str,
    prefix: str,
    item_type: str,
    items: list[dict],
    collected_at: str,
    date: datetime,
) -> str:
    """Writes one item type to its own JSON file."""
    key = f"{prefix}/{item_type}.json"

    s3.put_object(
        Bucket      = bucket,
        Key         = key,
        ContentType = "application/json",
        Body        = json.dumps(
            {
                "source":       "hacker_news",
                "item_type":    item_type,
                "collected_at": collected_at,
                "date":         date.strftime("%Y-%m-%d"),
                "total_items":  len(items),
                "items":        items,
            },
            ensure_ascii=False,
        ),
    )

    return key


def rollback(bucket: str, written_keys: list[str]) -> None:
    """
    Deletes all already-written files if the upload failed partway through.
    Best-effort — logs errors but does not re-raise them.
    """
    for key in written_keys:
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            logger.info(f"Rollback: deleted {key}")
        except Exception as e:
            logger.error(f"Rollback: could not delete {key}: {e}")


@contextmanager
def s3_transaction(bucket: str):
    """
    Context manager that deletes all S3 objects written so far on exception.
    Used as a clean alternative to a try/except rollback block in the handler.
    """
    written: list[str] = []
    try:
        yield written
    except Exception:
        rollback(bucket, written)
        raise


def lambda_handler(event, context):
    bucket       = os.environ["DATA_LAKE_BUCKET"]
    date         = parse_target_date(event)
    collected_at = datetime.now(timezone.utc).isoformat()
    prefix       = s3_prefix(date)

    logger.info(f"Collecting data for: {date.strftime('%Y-%m-%d')}")

    summary: dict[str, int] = {}

    with s3_transaction(bucket) as written_keys:
        for item_type in WINDOW_HOURS:
            # Fetch and upload inline — only one type is held in memory at a time.
            items = fetch_type(item_type, date)
            summary[item_type] = len(items)

            if items:
                key = upload_type(bucket, prefix, item_type, items, collected_at, date)
                written_keys.append(key)
                logger.info(f"Written: s3://{bucket}/{key}")

        total = sum(summary.values())
        logger.info(f"Total: {total} | {summary}")

        if not total:
            logger.warning("No data for the given day, skipping S3 write")
            return {
                "message": "no_data",
                "date":    date.strftime("%Y-%m-%d"),
            }

        # _SUCCESS marker — signals that ALL files were written successfully.
        # The normalization Lambda must not process a partition without this file.
        success_key = f"{prefix}/_SUCCESS"
        s3.put_object(Bucket=bucket, Key=success_key, Body=b"")
        logger.info(f"Written: s3://{bucket}/{success_key}")

    return {
        "date":        date.strftime("%Y-%m-%d"),
        "prefix":      prefix,
        "total_items": total,
        "summary":     summary,
    }
