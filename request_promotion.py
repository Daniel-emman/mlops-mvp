import os
import json
from datetime import datetime, timezone

import boto3
import botocore
import requests

s3 = boto3.client("s3")

DEV_BUCKET = os.environ["DEV_BUCKET"]
CONFIG_BUCKET = os.environ["CONFIG_BUCKET"]
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")  # optional
sns = boto3.client("sns") if SNS_TOPIC_ARN else None


def _utc_now():
    return datetime.now(timezone.utc).isoformat()

def _get_user_config(username: str) -> dict:
    """
    Reads s3://CONFIG_BUCKET/{username}/config.json
    Expected keys:
      - SLACK_WEBHOOK_URL
      - CREATOR_EMAIL (optional)
    """
    key = f"{username}/config.json"
    try:
        resp = s3.get_object(Bucket=CONFIG_BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        raise RuntimeError(f"Missing user config at s3://{CONFIG_BUCKET}/{key}")
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"Error reading user config: {e}")

def _read_logs(bucket: str, key: str) -> list:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return []

def _write_logs(bucket: str, key: str, logs: list):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(logs),
        ContentType="application/json"
    )

def _notify_slack(webhook_url: str, text: str):
    try:
        requests.post(webhook_url, json={"text": text}, timeout=5)
    except Exception as e:
        # Don't fail the Lambda because Slack is down
        print(f"Slack notify error: {e}")

def _notify_email(subject: str, message: str):
    if sns:
        try:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
        except Exception as e:
            print(f"SNS email notify error: {e}")


def request_promotion(event: dict):
    # Validate
    for k in ("user", "model"):
        if k not in event:
            return {"statusCode": 400, "body": json.dumps({"error": f"Missing '{k}' in event."})}

    username = event["user"]
    model = event["model"]
    version = str(event.get("version", "1"))
    note = event.get("note", "")

    # Fetch per-user config
    cfg = _get_user_config(username)
    slack_webhook = cfg.get("SLACK_WEBHOOK_URL")
    creator_email = cfg.get("CREATOR_EMAIL")  # optional

    key = f"{model}/{version}/logs.json"
    logs = _read_logs(DEV_BUCKET, key)

    entry = {
        "timestamp": _utc_now(),
        "model": model,
        "version": version,
        "from_env": "develop",
        "to_env": "qa",
        "status": "PENDING_APPROVAL",
        "note": note,
        "requested_by": username
    }
    logs.append(entry)
    _write_logs(DEV_BUCKET, key, logs)

    # Notifications
    msg = f":rocket: Promotion requested for *{model}* v{version} (Dev → QA) by *{username}*."
    if slack_webhook:
        _notify_slack(slack_webhook, msg)
    _notify_email(subject=f"Promotion requested: {model} v{version}", message=msg)

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Promotion request logged", "log": entry})
    }
