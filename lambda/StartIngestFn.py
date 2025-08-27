import json, os, re, boto3

sqs = boto3.client("sqs")
QUEUE_URL = os.environ.get("QUEUE_URL", "")
USERNAME_RE = re.compile(r"^[A-Za-z0-9-]{1,39}$")

def _parse_body(event):
    """
    Accepts:
    - API Gateway (proxy): event['body'] is a JSON string
    - Non-proxy / direct invoke: event may already be a dict with fields
    """
    if isinstance(event, dict):
        b = event.get("body")
        if isinstance(b, str) and b.strip():
            try:
                return json.loads(b)
            except json.JSONDecodeError:
                pass
        # direct invoke or non-proxy
        if "username" in event:
            return event
    return {}

def _resp(code, obj):
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(obj)
    }

def lambda_handler(event, context):
    body = _parse_body(event)
    username = (body.get("username") or "").strip()
    max_items = int(body.get("max_items") or 200)

    if not USERNAME_RE.match(username):
        return _resp(400, {"error": "invalid username"})
    if not QUEUE_URL:
        return _resp(500, {"error": "QUEUE_URL not set"})

    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({"username": username.lower(), "max_items": max_items})
    )

    return _resp(202, {"status": "enqueued", "username": username, "max_items": max_items})
