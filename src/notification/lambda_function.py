"""
Notification Lambda
====================
Forwards error notifications to the Discord webhook. Invoked two ways:

  1. Via SNS (event has a "Records" key) — CloudWatch alarm messages:
     auto-generated JSON from AWS for timeout, memory exceeded, and other
     errors that cannot be caught in code, or that happen outside a
     pipeline execution (manual invokes).

  2. Directly by the Step Functions state machine's NotifyFailure task
     (no "Records" key, has a "failed_step" key) — rich failure detail
     including the failing Lambda's errorType/errorMessage/stackTrace,
     which Step Functions receives natively in the Catch's $.Cause. This
     is how VPC Lambdas get full-detail notifications despite having no
     network path to SNS: Step Functions is an AWS service outside the
     VPC and invokes this Lambda through the Lambda control plane.

The legacy notify_on_error decorator format (a "source" key inside an SNS
message) is still parsed for backward compatibility.

DISCORD_WEBHOOK_URL is the only place in the entire project that holds the
webhook URL — all other components publish to SNS or Step Functions, not
directly to Discord.
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


def handle_step_functions_failure(data: dict) -> dict:
    """Formats a direct invocation from the state machine's NotifyFailure task.

    Expected shape (see modules/orchestration/templates/pipeline.asl.json):
      failed_step:   which pipeline step's Catch fired
      error:         the Catch's $.Error (e.g. "NotImplementedError",
                     "States.Timeout", "Lambda.ServiceException")
      cause:         the Catch's $.Cause — for Lambda function errors this is
                     a JSON string with errorMessage/errorType/stackTrace
      execution:     execution name, for finding the run in the console
      state_machine: state machine name
    """
    error_type = data.get("error", "N/A")
    message    = "N/A"
    stack      = "N/A"

    # For Lambda function errors, Cause is the Lambda's JSON error output.
    # For infrastructure-level errors (States.Timeout, throttling, IAM),
    # it's a plain string — shown as the message, with no stack trace.
    raw_cause = data.get("cause", "")
    try:
        cause = json.loads(raw_cause)
        error_type = cause.get("errorType", error_type)
        message    = cause.get("errorMessage", "N/A")
        trace      = cause.get("stackTrace") or cause.get("trace")
        if trace:
            stack = "".join(trace) if isinstance(trace, list) else str(trace)
    except (json.JSONDecodeError, TypeError):
        if raw_cause:
            message = str(raw_cause)

    # Discord code blocks have a 1000-character limit.
    if len(stack) > 1000:
        stack = "...\n" + stack[-1000:]

    return _discord_embed(
        title  = f"🔴 Pipeline failed at: {data.get('failed_step', 'unknown')}",
        fields = [
            {"name": "Error type", "value": error_type,                          "inline": True},
            {"name": "Message",    "value": str(message)[:1000],                 "inline": True},
            {"name": "Execution",  "value": data.get("execution", "N/A"),        "inline": False},
            {"name": "Stack trace","value": f"```{stack}```",                    "inline": False},
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
    event       = event or {}
    results     = []

    if "Records" in event:
        # SNS path — CloudWatch alarms (and legacy decorator messages).
        for record in event["Records"]:
            raw_message = record.get("Sns", {}).get("Message", "{}")
            payload     = parse_sns_message(raw_message)
            results.append({"sent": send_discord(webhook_url, payload)})
    else:
        # Direct invocation — the state machine's NotifyFailure task.
        payload = handle_step_functions_failure(event)
        results.append({"sent": send_discord(webhook_url, payload)})

    return {"statusCode": 200, "body": json.dumps(results)}
