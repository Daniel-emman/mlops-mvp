import boto3, json, os

s3 = boto3.client("s3")
CONFIG_BUCKET = os.environ.get("CONFIG_BUCKET", "mlops-user-configs")

def get_user_config(username):
    key = f"{username}/config.json"
    response = s3.get_object(Bucket=CONFIG_BUCKET, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))
