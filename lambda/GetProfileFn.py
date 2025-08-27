import os, json, boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

TABLE = os.environ["TABLE_NAME"]
ddb = boto3.resource("dynamodb").Table(TABLE)

def _to_native(obj):
    """Recursively convert DynamoDB Decimals to int/float for JSON serialization."""
    if isinstance(obj, list):
        return [_to_native(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

def lambda_handler(event, context):
    path = event.get("pathParameters") or {}
    username = (path.get("username") or "").lower().strip()
    if not username:
        return {"statusCode": 400, "headers":{"Content-Type":"application/json"}, "body": json.dumps({"error":"username required"})}

    pk = f"USER#{username}"

    # latest profile snapshot
    prof = ddb.query(
        KeyConditionExpression=Key("PK").eq(pk) & Key("SK").begins_with("PROFILE#"),
        ScanIndexForward=False, Limit=1
    )["Items"]
    profile = prof[0].get("data", {}) if prof else {}

    # all repos (you wrote each repo as an item)
    repos = ddb.query(
        KeyConditionExpression=Key("PK").eq(pk) & Key("SK").begins_with("REPO#"),
        Limit=2000
    )["Items"]
    repo_names = [r.get("name") for r in repos if r.get("name")]

    # top 10 by stars (kept for completeness)
    repos_sorted = sorted(repos, key=lambda r: int(r.get("stargazers_count", 0) or 0), reverse=True)[:10]

    # recent events (may be empty)
    events = ddb.query(
        KeyConditionExpression=Key("PK").eq(pk) & Key("SK").begins_with("EVENT#"),
        ScanIndexForward=False, Limit=50
    )["Items"]

    latest_activity = None
    if events:
        latest_activity = events[0].get("created_at")
    elif profile:
        latest_activity = profile.get("updated_at")

    body = {
        "profile": profile,
        "repositoriesCount": len(repo_names),
        "repositories": repo_names,
        "topRepositories": [{
            "name": r.get("name"),
            "stars": int(r.get("stargazers_count", 0) or 0),
            "language": r.get("primary_language"),
            "url": r.get("url")
        } for r in repos_sorted],
        "recentActivity": [{
            "type": e.get("type"),
            "repo": e.get("repo"),
            "at": e.get("created_at")
        } for e in events],
        "latestActivityAt": latest_activity
    }

    body_native = _to_native(body)
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin":"*"},
        "body": json.dumps(body_native)
    }
