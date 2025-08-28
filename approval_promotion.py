import os
import json
from datetime import datetime, timezone

import boto3
import botocore
import requests

s3 = boto3.client("s3")

DEV_BUCKET = os.environ["DEV_BUCKET"]
QA_BUCKET = os.environ["QA_BUCKET"]
PROD_BUCKET = os.environ["PROD_BUCKET"]
CONFIG_BUCKET = os.environ["CONFIG_BUCKET"]
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")  # optional
sns = boto3.client("sns") if SNS_TOPIC_ARN else None


def _utc_now():
    return datetime.now(timezone.utc).isoformat()

def _get_user_config(username: str) -> dict:
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
        print(f"Slack notify error: {e}")

def _notify_email(subject: str, message: str):
    if sns:
        try:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
        except Exception as e:
            print(f"SNS email notify error: {e}")

def _copy_prefix(source_bucket: str, target_bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            src = {"Bucket": source_bucket, "Key": obj["Key"]}
            s3.copy_object(CopySource=src, Bucket=target_bucket, Key=obj["Key"])


def approve_promotion(event: dict):
    # Validate
    for k in ("user", "model"):
        if k not in event:
            return {"statusCode": 400, "body": json.dumps({"error": f"Missing '{k}' in event."})}

    username = event["user"]            # approver identity (you can validate in UI)
    model = event["model"]
    version = str(event.get("version", "1"))
    to_env = event.get("to_env", "qa").lower()
    if to_env not in ("qa", "prod"):
        return {"statusCode": 400, "body": json.dumps({"error": "to_env must be 'qa' or 'prod'."})}

    # Fetch the requesting user's config for notifications (or use approver's; here we use requester's)
    # In your UI, pass event["requester"] if approver is a different person than requester.
    requester = event.get("requester", username)
    requester_cfg = _get_user_config(requester)
    slack_webhook = requester_cfg.get("SLACK_WEBHOOK_URL")

    # Decide source/target buckets
    source_bucket = DEV_BUCKET if to_env == "qa" else QA_BUCKET
    target_bucket = QA_BUCKET if to_env == "qa" else PROD_BUCKET

    # Copy all artifacts for this model/version
    prefix = f"{model}/{version}/"
    _copy_prefix(source_bucket, target_bucket, prefix)

    # Merge logs: start with whatever is in source, then append approval, write to target
    log_key = f"{model}/{version}/logs.json"
    src_logs = _read_logs(source_bucket, log_key)
    tgt_logs = _read_logs(target_bucket, log_key)
    combined = src_logs if src_logs else tgt_logs

    approval_entry = {
        "timestamp": _utc_now(),
        "model": model,
        "version": version,
        "from_env": "develop" if to_env == "qa" else "qa",
        "to_env": to_env,
        "status": "APPROVED",
        "approved_by": username
    }
    combined.append(approval_entry)
    _write_logs(target_bucket, log_key, combined)

    # Optional: append a mirror entry back to source to reflect the decision there as well
    source_logs = _read_logs(source_bucket, log_key)
    source_logs.append({
        "timestamp": _utc_now(),
        "model": model,
        "version": version,
        "from_env": "develop" if to_env == "qa" else "qa",
        "to_env": to_env,
        "status": "APPROVAL_RECORDED",
        "approved_by": username
    })
    _write_logs(source_bucket, log_key, source_logs)

    # Notify requester that approval is done
    msg = f":white_check_mark: *Approved* → *{model}* v{version} promoted to *{to_env.upper()}* by *{username}*."
    if slack_webhook:
        _notify_slack(slack_webhook, msg)
    _notify_email(subject=f"Promotion approved: {model} v{version} → {to_env.upper()}", message=msg)

    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Promotion to {to_env} approved", "log": approval_entry})
    }
