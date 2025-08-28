import os
import json

import boto3
import botocore

s3 = boto3.client("s3")

DEV_BUCKET = os.environ["DEV_BUCKET"]
QA_BUCKET = os.environ["QA_BUCKET"]
PROD_BUCKET = os.environ["PROD_BUCKET"]

def _read_logs(bucket: str, key: str) -> list:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return []
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return []
        raise

def get_logs(event: dict):
    if "model" not in event:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing 'model'."})}
    model = event["model"]
    version = str(event.get("version", "1"))
    key = f"{model}/{version}/logs.json"

    all_logs = []
    for b in (DEV_BUCKET, QA_BUCKET, PROD_BUCKET):
        all_logs.extend(_read_logs(b, key))

    # Sort by timestamp if present
    try:
        all_logs.sort(key=lambda x: x.get("timestamp", ""))
    except Exception:
        pass

    return {
        "statusCode": 200,
        "body": json.dumps(all_logs)
    }
