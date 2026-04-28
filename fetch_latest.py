"""Find the most recently modified file under the WhaTap S3 bucket
prefix `package/latest/` and post the filename, download URL, KST
timestamp, and size to a Slack channel via Incoming Webhook.

Required env:
    SLACK_WEBHOOK_URL   Slack Incoming Webhook URL.

Optional env:
    PREFIX              S3 prefix to scan. Default: "package/latest/".
    DRY_RUN             If "1"/"true", skip the Slack POST and just print.
"""

from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

# Path-style regional endpoint. The CDN-fronted host (repo.whatap.io)
# silently ignores `prefix` / `max-keys`, so we go straight to S3.
S3_ENDPOINT = "https://s3.ap-northeast-2.amazonaws.com/repo.whatap.io/"
PUBLIC_DOWNLOAD_PREFIX = "https://repo.whatap.io/"
DEFAULT_PREFIX = "package/latest/"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
KST = timezone(timedelta(hours=9))


def list_objects(prefix: str) -> list[dict]:
    """List S3 objects directly under `prefix` (using delimiter='/'),
    paginating if needed."""
    session = requests.Session()
    objects: list[dict] = []
    marker = ""
    while True:
        params = {"prefix": prefix, "delimiter": "/", "max-keys": "1000"}
        if marker:
            params["marker"] = marker
        resp = session.get(S3_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        page_keys: list[str] = []
        for c in root.findall("s3:Contents", S3_NS):
            key = c.findtext("s3:Key", default="", namespaces=S3_NS)
            last_modified = c.findtext("s3:LastModified", default="", namespaces=S3_NS)
            size = int(c.findtext("s3:Size", default="0", namespaces=S3_NS))
            objects.append({"key": key, "last_modified": last_modified, "size": size})
            page_keys.append(key)

        is_truncated = root.findtext("s3:IsTruncated", default="false", namespaces=S3_NS).lower() == "true"
        if not is_truncated or not page_keys:
            break
        next_marker = root.findtext("s3:NextMarker", default="", namespaces=S3_NS)
        marker = next_marker or page_keys[-1]
    return objects


def find_latest(objects: list[dict]) -> Optional[dict]:
    return max(objects, key=lambda o: o["last_modified"]) if objects else None


def to_kst_string(iso_utc: str) -> str:
    """'2026-04-27T07:08:23.000Z' -> '2026-04-27 16:08:23 KST'."""
    dt = datetime.strptime(iso_utc, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")


def human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def build_payload(file_info: dict) -> dict:
    key = file_info["key"]
    filename = key.rsplit("/", 1)[-1]
    download_url = PUBLIC_DOWNLOAD_PREFIX + key
    timestamp_kst = to_kst_string(file_info["last_modified"])
    size_str = human_size(file_info["size"])

    return {
        "text": f"WhaTap latest package: {filename}",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "WhaTap 최신 패키지"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*파일명*\n<{download_url}|{filename}>"},
                    {"type": "mrkdwn", "text": f"*Timestamp (KST)*\n`{timestamp_kst}`"},
                    {"type": "mrkdwn", "text": f"*Size*\n{size_str}"},
                    {"type": "mrkdwn", "text": f"*URL*\n{download_url}"},
                ],
            },
        ],
    }


def post_to_slack(webhook_url: str, payload: dict) -> None:
    resp = requests.post(webhook_url, json=payload, timeout=30)
    resp.raise_for_status()


def main() -> int:
    prefix = os.environ.get("PREFIX", DEFAULT_PREFIX)
    dry_run = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")

    objects = list_objects(prefix)
    if not objects:
        print(f"ERROR: no files found under '{prefix}'", file=sys.stderr)
        return 1

    latest = find_latest(objects)
    payload = build_payload(latest)

    print(f"scanned files:     {len(objects)}")
    print(f"latest key:        {latest['key']}")
    print(f"latest timestamp:  {to_kst_string(latest['last_modified'])}")
    print(f"latest size:       {human_size(latest['size'])}")
    print(f"download url:      {PUBLIC_DOWNLOAD_PREFIX + latest['key']}")

    if dry_run:
        print("DRY_RUN=1, skipping Slack post")
        return 0

    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("ERROR: SLACK_WEBHOOK_URL is not set", file=sys.stderr)
        return 1

    post_to_slack(webhook, payload)
    print("posted to Slack")
    return 0


if __name__ == "__main__":
    sys.exit(main())
