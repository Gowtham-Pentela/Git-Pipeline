import os, json, time, gzip, io, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

BUCKET = os.environ["BUCKET_NAME"]
TABLE = os.environ["TABLE_NAME"]
SECRET_NAME = os.environ["GITHUB_SECRET_NAME"]
UA = os.environ.get("USER_AGENT", "gh-pipeline-lab/1.0")
MAX_ITEMS_DEFAULT = int(os.environ.get("MAX_ITEMS_DEFAULT", "200"))

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb").Table(TABLE)
secrets = boto3.client("secretsmanager")

def _github_token():
    s = secrets.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(s["SecretString"])
    return secret["token"]

def _get(url, headers):
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read()
            return r.getcode(), dict(r.headers), body
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()

def _write_ndjson(prefix, rows):
    if not rows:
        print("SKIP write", prefix, "0 rows")
        return
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in rows:
            gz.write((json.dumps(r, separators=(",", ":")) + "\n").encode("utf-8"))
    key = f"{prefix}/part-{int(time.time())}.ndjson.gz"
    s3.put_object(
        Bucket=BUCKET, Key=key, Body=buf.getvalue(),
        ContentType="application/json", ContentEncoding="gzip"
    )
    print("WROTE", f"s3://{BUCKET}/{key}", "rows:", len(rows))

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def lambda_handler(event, context):
    record = event["Records"][0]
    msg = json.loads(record["body"])
    username = (msg.get("username") or "").lower().strip()
    max_items = int(msg.get("max_items") or MAX_ITEMS_DEFAULT)
    if not username:
        print("No username provided")
        return {"ok": False, "error": "username missing"}

    token = _github_token()
    base = "https://api.github.com"
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": UA,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    run_id = _now_iso()
    raw_prefix = f"raw/user={username}/dt={run_id[:10]}"

    # 1) Profile
    code, hdrs, body = _get(f"{base}/users/{username}", headers)
    print("profile status", code, "rate", hdrs.get("X-RateLimit-Limit"), hdrs.get("X-RateLimit-Remaining"))
    if code != 200:
        raise Exception(f"profile fetch failed: {code} {body[:200]}")
    profile = json.loads(body)
    _write_ndjson(f"{raw_prefix}/profile", [profile])
    ddb.put_item(Item={
        "PK": f"USER#{username}",
        "SK": f"PROFILE#{run_id}",
        "data": {
            "login": profile.get("login"),
            "name": profile.get("name"),
            "followers": profile.get("followers"),
            "public_repos": profile.get("public_repos"),
            "updated_at": profile.get("updated_at")
        }
    })

    # 2) Repos (paginate)
    repos_all, page, fetched = [], 1, 0
    while True:
        code, hdrs, body = _get(f"{base}/users/{username}/repos?per_page=100&page={page}", headers)
        print("repos page", page, "status", code, "rate", hdrs.get("X-RateLimit-Remaining"))
        if code != 200:
            raise Exception(f"repos fetch failed: {code} {body[:200]}")
        chunk = json.loads(body)
        if not chunk:
            break
        repos_all.extend(chunk)
        fetched += len(chunk)
        page += 1
        if fetched >= max_items:
            break
        time.sleep(0.2)

    _write_ndjson(f"{raw_prefix}/repos", repos_all)
    for r in repos_all[:max_items]:
        ddb.put_item(Item={
            "PK": f"USER#{username}",
            "SK": f"REPO#{r['id']}",
            "name": r.get("name"),
            "full_name": r.get("full_name"),
            "stargazers_count": r.get("stargazers_count", 0),
            "forks_count": r.get("forks_count", 0),
            "primary_language": r.get("language"),
            "updated_at": r.get("updated_at"),
            "url": r.get("html_url"),
        })

    # 3) Public events
    events_all, page, fetched = [], 1, 0
    while True:
        code, hdrs, body = _get(f"{base}/users/{username}/events/public?per_page=100&page={page}", headers)
        print("events page", page, "status", code, "rate", hdrs.get("X-RateLimit-Remaining"))
        if code == 404:
            break
        if code != 200:
            break
        echunk = json.loads(body)
        if not echunk:
            break
        events_all.extend(echunk)
        fetched += len(echunk)
        page += 1
        if fetched >= max_items:
            break
        time.sleep(0.2)

    _write_ndjson(f"{raw_prefix}/events", events_all)
    for ev in events_all[:max_items]:
        ddb.put_item(Item={
            "PK": f"USER#{username}",
            "SK": f"EVENT#{ev['id']}",
            "type": ev.get("type"),
            "repo": (ev.get("repo") or {}).get("name"),
            "created_at": ev.get("created_at"),
        })

    # 4) Run metadata
    ddb.put_item(Item={
        "PK": f"USER#{username}",
        "SK": f"RUN#{run_id}",
        "summary": {"repos": len(repos_all), "events": len(events_all), "profile": True},
        "status": "OK"
    })
    return {"ok": True, "username": username}
