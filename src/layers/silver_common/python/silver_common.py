"""
Silver layer — shared helpers
==============================
Used by both normalization_hn and normalization_x, packaged as the
`silver_common` Lambda Layer (src/layers/silver_common/). Only the Layer
copy should exist now — the old per-Lambda local copies of this file
(src/normalization_hn/silver_common.py, src/normalization_x/silver_common.py)
must be deleted, or Python will import THOSE instead of the Layer version
(the function's own deployment package is resolved before /opt/python on
sys.path), silently undoing every fix below.

Covers:
  - Timestamp normalization (epoch + string -> UTC ISO-8601)
  - HTML stripping (regex/stdlib only — no BeautifulSoup)
  - Hashtag / URL extraction
  - Deterministic ID generation (users, dimension rows, synthesized post ids)
  - A single Parquet write primitive (pyarrow) with no pandas dependency and
    no read-modify-write "upsert" step — see put_parquet() docstring for why.
"""

from __future__ import annotations

import ast
import hashlib
import html as html_lib
import io
import logging
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()

# Fixed namespace so the same (platform, username) pair always resolves to
# the same UUID, across separate Lambda invocations and across HN/X runs.
# This is what makes downstream de-duplication (by the reader, see
# gold_common.py) work without ever needing to read Silver back in here.
_USER_UUID_NAMESPACE = uuid.UUID("6f9c2e3a-9b1e-4b6a-8f2d-2a6a8c9d1e00")

_HASHTAG_RE = re.compile(r"#(\w+)")
_URL_RE = re.compile(r"https?://\S+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


# ── Text cleaning ────────────────────────────────────────────────────────────

def strip_html(text: str | None) -> str | None:
    """Removes HTML tags from a text field with a plain regex + stdlib entity
    unescape. Previously used BeautifulSoup, which parses a full DOM tree per
    call — measurably slow across thousands of HN comments per invocation.
    HN/tweet text is simple inline markup (<p>, <i>, <a>, &amp; ...), so a
    tag-stripping regex plus html.unescape gives the same practical result
    for a fraction of the cost, and drops the bs4 dependency entirely."""
    if not text:
        return text
    cleaned = _HTML_TAG_RE.sub(" ", text)
    cleaned = html_lib.unescape(cleaned)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned or None


# ── Timestamps ────────────────────────────────────────────────────────────────

def epoch_to_iso(ts) -> str | None:
    """Unix epoch seconds (int/str/float) -> UTC ISO-8601 string."""
    if ts is None or ts == "":
        return None
    try:
        return datetime.fromtimestamp(int(float(ts)), tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        logger.warning(f"Could not parse epoch timestamp: {ts!r}")
        return None


def str_datetime_to_iso(value: str | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str | None:
    """Parses a 'YYYY-MM-DD HH:MM:SS' string (assumed UTC) -> UTC ISO-8601 string."""
    if not value:
        return None
    try:
        dt = datetime.strptime(str(value).strip(), fmt).replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        logger.warning(f"Could not parse datetime string: {value!r}")
        return None


# ── Hashtags & URLs ───────────────────────────────────────────────────────────

def extract_hashtags(text: str | None) -> list[str]:
    """Naive #hashtag extraction from free text (used for HN/X)."""
    if not text:
        return []
    return sorted({tag.lower() for tag in _HASHTAG_RE.findall(text)})


def parse_hashtag_list(raw) -> list[str]:
    """Parses gpreda's Python-list-as-string hashtags column, e.g. "['a','b']".
    Uses ast.literal_eval, NOT json — the column is Python repr, not JSON."""
    if raw is None:
        return []
    raw = str(raw).strip()
    if not raw or raw.lower() == "nan" or raw == "[]":
        return []
    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, (list, tuple)):
            return sorted({str(tag).lower() for tag in parsed if tag})
    except (ValueError, SyntaxError):
        logger.warning(f"Could not parse hashtag list: {raw!r}")
    return []


def extract_urls(text: str | None) -> list[str]:
    """Extracts http(s) URLs referenced in free text."""
    if not text:
        return []
    return sorted(set(_URL_RE.findall(text)))


def url_domain(url: str) -> str | None:
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc or None
    except ValueError:
        return None


# ── Deterministic IDs ─────────────────────────────────────────────────────────

def user_id_for(platform: str, username: str | None) -> str | None:
    """Deterministic UUID5 for a (platform, username) pair."""
    if not username:
        return None
    key = f"{platform}:{username}".strip().lower()
    return str(uuid.uuid5(_USER_UUID_NAMESPACE, key))


def stable_id(*parts: str) -> str:
    """Deterministic short id for dimension rows (hashtags/urls/sources),
    so the same value always maps to the same PK across separate runs."""
    digest = hashlib.sha256("|".join(p or "" for p in parts).encode("utf-8")).hexdigest()
    return digest[:24]


def synth_post_id(*parts: str) -> str:
    """Synthesizes a stable post_id for sources with no native tweet id
    (gpreda COVID/Bitcoin CSVs) by hashing user + timestamp + text."""
    digest = hashlib.sha256("|".join(p or "" for p in parts).encode("utf-8")).hexdigest()
    return digest[:32]


def run_tag(date: datetime, *extra: str) -> str:
    """Deterministic filename tag for a given target date (+ optional extra
    qualifiers, e.g. platform/source). Re-running the same day with the same
    extras always produces the same S3 key, which is what makes put_parquet's
    plain overwrite idempotent without ever reading the object first."""
    tag = f"{date.year:04d}{date.month:02d}{date.day:02d}"
    if extra:
        tag += "_" + "_".join(str(e) for e in extra if e)
    return tag


# ── Explicit Silver schemas ───────────────────────────────────────────────────
# Without a pinned schema, pyarrow infers each column's type from whatever
# values happen to be in that one call's rows. A day where every `posts` row
# has url=None (e.g. a comments-only HN day) gets `url` typed as pyarrow's
# `null` type; a day with a story gets `url: string`. Two Parquet files for
# the same table then have genuinely incompatible schemas, and unioning them
# on read (awswrangler/pyarrow dataset scan) either raises a bare TypeError
# or, depending on which columns/files are involved, segfaults instead of
# raising cleanly. Pinning one schema per table here means every file for
# that table — no matter what happened to be null that day — has identical
# column types, so reads never hit a schema conflict in the first place.
SCHEMAS: dict[str, "pa.Schema"] = {
    "posts": pa.schema([
        ("post_id", pa.string()),
        ("author_user_id", pa.string()),
        ("platform", pa.string()),
        ("post_type", pa.string()),
        ("content_text", pa.string()),
        ("title", pa.string()),
        ("url", pa.string()),
        ("created_at", pa.string()),
        ("score", pa.int64()),
        ("parent_post_id", pa.string()),
        ("root_post_id", pa.string()),
        ("lang", pa.string()),
        ("source_id", pa.string()),
        ("is_retweet", pa.bool_()),
        ("source_dataset", pa.string()),
    ]),
    "users": pa.schema([
        ("user_id", pa.string()),
        ("username", pa.string()),
        ("display_name", pa.string()),
        ("platform", pa.string()),
        ("karma_score", pa.int64()),
        ("followers_count", pa.int64()),
        ("friends_count", pa.int64()),
        ("favourites_count", pa.int64()),
        ("statuses_count", pa.int64()),
        ("is_verified", pa.bool_()),
        ("account_created_at", pa.string()),
        ("location", pa.string()),
        ("description", pa.string()),
        ("source_dataset", pa.string()),
    ]),
    "post_engagement": pa.schema([
        ("post_id", pa.string()),
        ("favorite_count", pa.int64()),
        ("retweet_count", pa.int64()),
        ("reply_count", pa.int64()),
        ("num_comments", pa.int64()),
    ]),
    "hashtags": pa.schema([
        ("hashtag_id", pa.string()),
        ("tag", pa.string()),
    ]),
    "post_hashtags": pa.schema([
        ("post_id", pa.string()),
        ("hashtag_id", pa.string()),
    ]),
    "urls": pa.schema([
        ("url_id", pa.string()),
        ("url", pa.string()),
        ("domain", pa.string()),
    ]),
    "post_urls": pa.schema([
        ("post_id", pa.string()),
        ("url_id", pa.string()),
    ]),
    "sources": pa.schema([
        ("source_id", pa.string()),
        ("name", pa.string()),
    ]),
    "user_snapshots": pa.schema([
        ("user_id", pa.string()),
        ("captured_at", pa.string()),
        ("followers_count", pa.int64()),
        ("karma_score", pa.int64()),
    ]),
}


# ── S3 / Parquet ──────────────────────────────────────────────────────────────

def check_success(s3, bucket: str, prefix: str) -> bool:
    """Checks for the _SUCCESS marker written by the ingestion / build_chunks
    step. Partitions without it are considered incomplete and are skipped."""
    from botocore.exceptions import ClientError
    try:
        s3.head_object(Bucket=bucket, Key=f"{prefix}/_SUCCESS")
        return True
    except ClientError:
        return False


def put_parquet(s3, bucket: str, table: str, key: str, rows: list[dict]) -> int:
    """Writes `rows` (a plain list of dicts) as a single Parquet object at an
    exact S3 key, via pyarrow directly — no pandas, no awswrangler.

    `table` selects the pinned schema from SCHEMAS above, so every file ever
    written for that table has identical column types — see the SCHEMAS
    comment for why that matters (a bare per-call type-inferred schema is
    what caused the Gold Lambda's TypeError/segfault when reading multiple
    days' files back as one dataset).

    This is the ONE write primitive for every Silver table now. It replaces
    the old upsert_users / upsert_dimension_table / write_partitioned /
    append_partitioned helpers, all of which did a read-modify-write: read
    the *entire* existing table from S3, concat the new rows in pandas,
    drop_duplicates, and write the whole thing back. That made every single
    invocation's runtime scale with the size of the WHOLE table (which only
    grows), not with the size of the one day being processed — this was the
    actual cause of the 5-minute timeout, far more than pandas overhead or
    sequential S3 reads.

    The replacement contract: the caller builds a deterministic key (see
    run_tag()) that encodes the partition + the day/source being processed.
    Re-running that exact day just overwrites its own small object — cheap,
    no read required. Different days/sources get different keys, so nothing
    is ever clobbered across runs. The tradeoff is that a table like `users`
    or `hashtags` ends up as many small per-run files instead of one
    continuously-deduplicated file, and the same entity (same user, same
    hashtag) can legitimately appear in more than one file if it showed up
    on more than one day. That's resolved once, cheaply, on the READ side —
    see gold_common.safe_read_parquet's dedupe_subset — rather than on every
    single write.
    """
    if not rows:
        return 0
    schema = SCHEMAS.get(table)
    if schema is None:
        logger.warning(f"No pinned schema for table '{table}' — falling back to inferred types")
    arrow_table = pa.Table.from_pylist(rows, schema=schema)
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf, compression="snappy")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return arrow_table.num_rows


def silver_key(table: str, tag: str, partitions: dict[str, str] | None = None) -> str:
    """Builds a Hive-style S3 key under silver/<table>/ so awswrangler /
    Athena / Glue can still auto-discover partitions exactly as before —
    only the file-naming/upsert strategy changed, not the on-disk layout."""
    parts = "".join(f"{k}={v}/" for k, v in (partitions or {}).items())
    return f"silver/{table}/{parts}data_{tag}.parquet"
