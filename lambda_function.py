import json
from request_promotion import request_promotion
from approval_promotion import approve_promotion
from get_logs import get_logs

def lambda_handler(event, context):
    """
    Event contract:
      - action: "promote" | "approve" | "logs"
      - user: username who initiated the action (required for promote/approve)
      - model: model name (e.g., "demo.dev.dummy_model")
      - version: string version (e.g., "1")
      - note: optional (promote)
      - to_env: "qa" or "prod" (approve; default "qa")
    """
    action = (event or {}).get("action")
    if action == "promote":
        return request_promotion(event)
    elif action == "approve":
        return approve_promotion(event)
    elif action == "logs":
        return get_logs(event)
    else:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid or missing 'action'."})
        }
