"""
Silver layer — X (Twitter) normalization
==========================================
Reads the Bronze X data prepared locally by build_chunks.py and uploaded to
bronze/x/ (see SILVER_LAYER_HANDOFF.md, section 3), normalizes it, and
writes the shared Silver schema (section 4) as partitioned Parquet under
silver/ — the SAME tables normalization_hn writes, via
mode="overwrite_partitions" so the two Lambdas never clobber each other's
partitions.

Bronze layout recap:
    bronze/x/congress_users.json                     <- NDJSON, whole file, root-level
    bronze/x/year=YYYY/month=MM/day=DD/covid.csv      <- gpreda CSV
    bronze/x/year=YYYY/month=MM/day=DD/bitcoin.csv    <- gpreda CSV
    bronze/x/year=YYYY/month=MM/day=DD/congress.json  <- NDJSON tweets
    bronze/x/year=YYYY/month=MM/day=DD/_SUCCESS

Every X day is single-source (the 3 datasets cover non-overlapping date
ranges), so the parser to use is dispatched purely by filename.

Invocation modes (event):
    {"date": "YYYY-MM-DD"}           -> normalize a single day
    {"prefix": "year=.../month=.../day=..."} -> normalize an explicit partition
    {"full_scan": true}              -> walk every day partition under bronze/x/
    (X datasets are bulk-uploaded by hand, so unlike HN there is no
    "yesterday" default — the caller must say what to process.)
"""

import io
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3

from silver_common import (
    check_success,
    extract_hashtags,
    extract_urls,
    parse_hashtag_list,
    stable_id,
    strip_html,
    str_datetime_to_iso,
    synth_post_id,
    upsert_dimension_table,
    upsert_users,
    url_domain,
    user_id_for,
    write_partitioned,
    append_partitioned,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

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



# ── gpreda CSV parser (covid.csv / bitcoin.csv — identical column set) ──────────

def process_gpreda_csv(bucket: str, key: str, source_dataset: str) -> dict:
    import pandas as pd

    obj = s3.get_object(Bucket=bucket, Key=key)
    # engine="python" + on_bad_lines="skip": defensive against malformed rows,
    # same fallback build_chunks.py already applies to the Bitcoin dataset.
    df_raw = pd.read_csv(io.BytesIO(obj["Body"].read()), engine="python", on_bad_lines="skip")

    posts_rows, snapshot_rows = [], []
    hashtag_rows, post_hashtag_rows = [], []
    url_rows, post_url_rows = [], []
    sources_seen: dict[str, str] = {}
    user_latest: dict[str, dict] = {}

    for _, r in df_raw.iterrows():
        user_name = r.get("user_name")
        if pd.isna(user_name) or not str(user_name).strip():
            continue
        user_name = str(user_name).strip()

        date_raw = r.get("date")
        date_str = str(date_raw) if pd.notna(date_raw) else None
        created_at = str_datetime_to_iso(date_str) if date_str else None

        text = r.get("text") if pd.notna(r.get("text")) else None
        content_text = strip_html(text)

        post_id = synth_post_id(user_name, date_str or "", text or "")
        author_user_id = user_id_for(PLATFORM, user_name)

        source_name = str(r.get("source")).strip() if pd.notna(r.get("source")) else None
        source_id = None
        if source_name:
            source_id = stable_id("source", source_name)
            sources_seen[source_id] = source_name

        is_retweet = bool(r.get("is_retweet")) if pd.notna(r.get("is_retweet")) else False

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

        followers = r.get("user_followers")
        followers = int(followers) if pd.notna(followers) else None
        snapshot_rows.append({
            "user_id":         author_user_id,
            "captured_at":     created_at,
            "followers_count": followers,
            "karma_score":     None,
        })

        tags = set(parse_hashtag_list(r.get("hashtags"))) | set(extract_hashtags(text))
        for tag in tags:
            hashtag_id = stable_id("hashtag", tag)
            hashtag_rows.append({"hashtag_id": hashtag_id, "tag": tag})
            post_hashtag_rows.append({"post_id": post_id, "hashtag_id": hashtag_id})

        for url in extract_urls(text):
            url_id = stable_id("url", url)
            url_rows.append({"url_id": url_id, "url": url, "domain": url_domain(url)})
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
                "friends_count":      int(r["user_friends"]) if pd.notna(r.get("user_friends")) else None,
                "favourites_count":   int(r["user_favourites"]) if pd.notna(r.get("user_favourites")) else None,
                "statuses_count":     None,
                "is_verified":        bool(r["user_verified"]) if pd.notna(r.get("user_verified")) else None,
                "account_created_at": str_datetime_to_iso(str(r["user_created"])) if pd.notna(r.get("user_created")) else None,
                "location":           r.get("user_location") if pd.notna(r.get("user_location")) else None,
                "description":        r.get("user_description") if pd.notna(r.get("user_description")) else None,
                "source_dataset":     source_dataset,
                "_created_at":        created_at,
            }

    users_rows = [{k: v for k, v in u.items() if k != "_created_at"} for u in user_latest.values()]
    source_rows = [{"source_id": sid, "name": name} for sid, name in sources_seen.items()]

    return {
        "posts":          pd.DataFrame(posts_rows),
        "users":          pd.DataFrame(users_rows),
        "engagement":     pd.DataFrame(),  # gpreda CSVs have no per-tweet engagement counts
        "snapshots":      pd.DataFrame(snapshot_rows),
        "hashtags":       pd.DataFrame(hashtag_rows),
        "post_hashtags":  pd.DataFrame(post_hashtag_rows),
        "urls":           pd.DataFrame(url_rows),
        "post_urls":      pd.DataFrame(post_url_rows),
        "sources":        pd.DataFrame(source_rows),
    }


# ── Writing ───────────────────────────────────────────────────────────────────

def add_date_partitions(df, fallback_date: datetime):
    import pandas as pd

    created = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["year"] = created.dt.year.fillna(fallback_date.year).astype(int)
    df["month"] = created.dt.month.fillna(fallback_date.month).astype(int).map(lambda m: f"{m:02d}")
    df["day"] = created.dt.day.fillna(fallback_date.day).astype(int).map(lambda d: f"{d:02d}")
    return df


def write_result_tables(bucket: str, result: dict, fallback_date: datetime) -> dict:
    summary = {}

    posts_df = result.get("posts")
    if posts_df is not None and not posts_df.empty:
        posts_df = add_date_partitions(posts_df, fallback_date)
        summary["posts"] = write_partitioned(posts_df, bucket, "posts", ["year", "month", "day"])

    if result.get("users") is not None and not result["users"].empty:
        summary["users"] = upsert_users(bucket, PLATFORM, result["users"])

    if result.get("engagement") is not None and not result["engagement"].empty:
        summary["post_engagement"] = upsert_dimension_table(
            bucket, "post_engagement", result["engagement"], ["post_id"]
        )

    if result.get("snapshots") is not None and not result["snapshots"].empty:
        summary["user_snapshots"] = append_partitioned(result["snapshots"], bucket, "user_snapshots")

    if result.get("hashtags") is not None and not result["hashtags"].empty:
        summary["hashtags"] = upsert_dimension_table(bucket, "hashtags", result["hashtags"], ["hashtag_id"])

    if result.get("post_hashtags") is not None and not result["post_hashtags"].empty:
        summary["post_hashtags"] = upsert_dimension_table(
            bucket, "post_hashtags", result["post_hashtags"], ["post_id", "hashtag_id"]
        )

    if result.get("urls") is not None and not result["urls"].empty:
        summary["urls"] = upsert_dimension_table(bucket, "urls", result["urls"], ["url_id"])

    if result.get("post_urls") is not None and not result["post_urls"].empty:
        summary["post_urls"] = upsert_dimension_table(
            bucket, "post_urls", result["post_urls"], ["post_id", "url_id"]
        )

    if result.get("sources") is not None and not result["sources"].empty:
        summary["sources"] = upsert_dimension_table(bucket, "sources", result["sources"], ["source_id"])

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
    users_cache = None  # loaded lazily, once, only if a congress.json day is found

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
        result = None

        if "covid.csv" in filenames:
            result = process_gpreda_csv(bucket, f"{full_prefix}/covid.csv", "covid")
        elif "bitcoin.csv" in filenames:
            result = process_gpreda_csv(bucket, f"{full_prefix}/bitcoin.csv", "bitcoin")
        else:
            logger.warning(f"No recognized data file in {full_prefix}: {filenames}")
            skipped.append(day_prefix)
            continue

        summary = write_result_tables(bucket, result, fallback_date)
        for table, count in summary.items():
            totals[table] += count
        processed.append(day_prefix)
        logger.info(f"[{day_prefix}] {summary}")

    return {
        "processed_partitions": processed,
        "skipped_partitions":   skipped,
        "totals":               dict(totals),
    }
