"""
Silver layer — shared helpers
==============================
Used by both normalization_hn and normalization_x. LocalStack Community has
no Lambda Layer support (see KNOWLEDGE.md 6.2), so this file is copied
verbatim into both src/normalization_hn/ and src/normalization_x/ and
packaged with each Lambda individually — the same pattern used for
notifier.py before it was removed. On real AWS a shared Layer could take
over instead; the duplication is a LocalStack-only workaround.

Covers:
  - Timestamp normalization (epoch + string -> UTC ISO-8601)
  - HTML stripping
  - Hashtag / URL extraction
  - Deterministic ID generation (users, synthesized post ids)
  - Parquet read/write/upsert helpers around awswrangler
"""

from __future__ import annotations

import ast
import hashlib
import logging
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

try:
    from bs4 import BeautifulSoup
    _HAS_BS4 = True
except ImportError:  # pragma: no cover - bs4 is in requirements.txt
    _HAS_BS4 = False

logger = logging.getLogger()

# Fixed namespace so the same (platform, username) pair always resolves to
# the same UUID, across separate Lambda invocations and across HN/X runs.
# This is what makes the users-table upsert (handoff doc, 5.5) idempotent.
_USER_UUID_NAMESPACE = uuid.UUID("6f9c2e3a-9b1e-4b6a-8f2d-2a6a8c9d1e00")

_HASHTAG_RE = re.compile(r"#(\w+)")
_URL_RE = re.compile(r"https?://\S+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")


# ── Text cleaning ────────────────────────────────────────────────────────────

def strip_html(text: str | None) -> str | None:
    """Removes HTML tags from a text field. Falls back to a regex if bs4 fails."""
    if not text:
        return text
    try:
        if _HAS_BS4:
            cleaned = BeautifulSoup(text, "html.parser").get_text(separator=" ")
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            return cleaned or None
    except Exception:
        pass
    cleaned = _HTML_TAG_RE.sub(" ", text)
    return re.sub(r"\s+", " ", cleaned).strip() or None


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
    """Naive #hashtag extraction from free text (used for HN/Congress)."""
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


# ── S3 / Parquet helpers ──────────────────────────────────────────────────────

def check_success(s3, bucket: str, prefix: str) -> bool:
    """Checks for the _SUCCESS marker written by the ingestion / build_chunks
    step. Partitions without it are considered incomplete and are skipped."""
    from botocore.exceptions import ClientError
    try:
        s3.head_object(Bucket=bucket, Key=f"{prefix}/_SUCCESS")
        return True
    except ClientError:
        return False


def write_partitioned(df, bucket: str, table: str, partition_cols: list[str]) -> int:
    """Writes a Silver fact table, overwriting only the partitions present in
    `df`. Safe to re-run for the same day — reruns replace, not duplicate."""
    if df is None or df.empty:
        return 0
    import awswrangler as wr

    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/silver/{table}/",
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=partition_cols,
    )
    return len(df)


def append_partitioned(df, bucket: str, table: str, partition_cols: list[str] | None = None) -> int:
    """Appends rows to a Silver table that behaves as a time series
    (post_engagement, user_snapshots) rather than a per-day overwrite."""
    if df is None or df.empty:
        return 0
    import awswrangler as wr

    kwargs = {"dataset": True, "mode": "append"}
    if partition_cols:
        kwargs["partition_cols"] = partition_cols

    wr.s3.to_parquet(df=df, path=f"s3://{bucket}/silver/{table}/", **kwargs)
    return len(df)


def upsert_dimension_table(bucket: str, table: str, new_df, dedup_cols: list[str]) -> int:
    """Read-modify-write upsert for small, unpartitioned dimension / bridge
    tables (hashtags, urls, sources, post_hashtags, post_urls). Reads the
    existing table (if any), unions with new rows, de-duplicates on the given
    key columns, and overwrites the table as a whole. Fine at this project's
    scale; would need a smarter merge strategy at real production volume."""
    if new_df is None or new_df.empty:
        return 0
    import pandas as pd
    import awswrangler as wr

    path = f"s3://{bucket}/silver/{table}/"
    try:
        existing = wr.s3.read_parquet(path, dataset=False)
        combined = pd.concat([existing, new_df], ignore_index=True)
    except Exception:
        combined = new_df

    combined = combined.drop_duplicates(subset=dedup_cols, keep="last").reset_index(drop=True)
    wr.s3.to_parquet(df=combined, path=path, dataset=False, mode="overwrite")
    return len(combined)


def upsert_users(bucket: str, platform: str, new_users_df) -> int:
    """Merges new_users_df into silver/users/platform=<platform>/, de-duping
    on username and keeping the most recently observed row (handoff doc 5.5).
    A full read-modify-write of that single platform partition — cheap enough
    at this project's data volume, and avoids a separate dedup pass later."""
    if new_users_df is None or new_users_df.empty:
        return 0
    import pandas as pd
    import awswrangler as wr

    path = f"s3://{bucket}/silver/users/platform={platform}/"
    try:
        existing = wr.s3.read_parquet(path, dataset=False)
        combined = pd.concat([existing, new_users_df], ignore_index=True)
    except Exception:
        combined = new_users_df

    combined = combined.drop_duplicates(subset=["username"], keep="last").reset_index(drop=True)
    combined["platform"] = platform

    wr.s3.to_parquet(
        df=combined,
        path=f"s3://{bucket}/silver/users/",
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=["platform"],
    )
    return len(combined)
