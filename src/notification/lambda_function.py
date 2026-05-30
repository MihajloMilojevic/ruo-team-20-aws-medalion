"""
Notification Lambda
====================
Receives messages from the SNS topic and forwards them to the Discord webhook.
Two message types are handled:

  1. CloudWatch alarm — auto-generated JSON from AWS (timeout, memory exceeded,
     and other errors that cannot be caught in code)

  2. notify_on_error decorator — structured JSON with stack trace sent by
     Lambda functions via utils/notifier.py

DISCORD_WEBHOOK_URL is the only place in the entire project that holds the
webhook URL — all other Lambdas publish to SNS, not directly to Discord.
"""

import json
import logging
import os

import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

http = urllib3.PoolManager()


def _discord_embed(title: str, fields: list[dict], color: int = 15158332) -> dict:
    return {"embeds": [{"title": title, "color": color, "fields": fields}]}


def handle_decorator_message(data: dict) -> dict:
    """Formats a message coming from the notify_on_error decorator."""
    stack = data.get("stack_trace", "N/A")

    # Discord code blocks have a 1000-character limit.
    if len(stack) > 1000:
        stack = "...\n" + stack[-1000:]

    return _discord_embed(
        title  = f"🔴 Lambda error: {data.get('source', 'unknown')}",
        fields = [
            {"name": "Error type",  "value": data.get("error_type", "N/A"), "inline": True},
            {"name": "Message",     "value": data.get("message", "N/A"),    "inline": True},
            {"name": "Stack trace", "value": f"```{stack}```",              "inline": False},
        ],
    )


def handle_cloudwatch_alarm(data: dict) -> dict:
    """Formats a CloudWatch alarm message (timeout, OOM, etc.)."""
    return _discord_embed(
        title  = f"⚠️ CloudWatch alarm: {data.get('AlarmName', 'unknown')}",
        fields = [
            {"name": "Reason",   "value": data.get("NewStateReason", "N/A"), "inline": False},
            {"name": "Status",   "value": data.get("NewStateValue", "N/A"),  "inline": True},
            {"name": "Previous", "value": data.get("OldStateValue", "N/A"),  "inline": True},
            {"name": "Time",     "value": data.get("StateChangeTime", "N/A"),"inline": False},
        ],
    )


def parse_sns_message(raw_message: str) -> dict:
    try:
        data = json.loads(raw_message)

        # Messages from the notify_on_error decorator have a "source" key.
        if "source" in data:
            return handle_decorator_message(data)

        # CloudWatch alarms have an "AlarmName" key.
        if "AlarmName" in data:
            return handle_cloudwatch_alarm(data)

        # Unknown format — display raw content.
        return _discord_embed(
            title  = "⚠️ Unknown notification",
            fields = [{"name": "Content", "value": raw_message[:1000], "inline": False}],
            color  = 16776960,  # yellow
        )

    except json.JSONDecodeError:
        return _discord_embed(
            title  = "⚠️ Unknown notification",
            fields = [{"name": "Content", "value": raw_message[:1000], "inline": False}],
            color  = 16776960,
        )


def send_discord(webhook_url: str, payload: dict) -> bool:
    try:
        resp = http.request(
            "POST",
            webhook_url,
            body    = json.dumps(payload).encode("utf-8"),
            headers = {"Content-Type": "application/json"},
            timeout = 10.0,
        )

        if resp.status not in (200, 204):
            logger.error(f"Discord returned status {resp.status}")
            return False

        return True

    except urllib3.exceptions.HTTPError as e:
        logger.error(f"Could not reach Discord: {e}")
        return False


def lambda_handler(event, context):
    webhook_url = os.environ["DISCORD_WEBHOOK_URL"]
    results     = []

    for record in event.get("Records", []):
        raw_message = record.get("Sns", {}).get("Message", "{}")
        payload     = parse_sns_message(raw_message)
        success     = send_discord(webhook_url, payload)

        results.append({"sent": success})

    return {"statusCode": 200, "body": json.dumps(results)}
