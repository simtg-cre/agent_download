"""For each configured S3 prefix on the WhaTap repo, find the most
recently modified file and post the filename, download URL, KST
timestamp, and size to a Slack channel via Incoming Webhook.

Required env:
    SLACK_WEBHOOK_URL   Slack Incoming Webhook URL.

Optional env:
    PREFIXES            Comma-separated list of S3 prefixes to scan.
                        Default: PREFIXES_DEFAULT below.
    DRY_RUN             If "1"/"true", skip the Slack POST and just print.
"""

from __future__ import annotations

import os
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

# Spacing between consecutive Slack POSTs. Incoming Webhooks are throttled
# to roughly 1 msg/sec per hook; without a small gap, the second message
# can come back 429 and silently never appear in the channel.
SLACK_POST_GAP_SEC = 1.2

# Path-style regional endpoint. The CDN-fronted host (repo.whatap.io)
# silently ignores `prefix` / `max-keys`, so we go straight to S3.
S3_ENDPOINT = "https://s3.ap-northeast-2.amazonaws.com/repo.whatap.io/"
PUBLIC_DOWNLOAD_PREFIX = "https://repo.whatap.io/"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
KST = timezone(timedelta(hours=9))

PREFIXES_DEFAULT = [
    "package/latest/",
    "rum-onpremise-allinone/",
]

# Friendly section header per prefix. Falls back to the prefix string.
PREFIX_LABELS = {
    "package/latest/": "WhaTap 최신 패키지 (package/latest)",
    "rum-onpremise-allinone/": "RUM 온프레미스 All-in-one",
}


def list_objects(prefix: str, session: requests.Session) -> list[dict]:
    """List S3 objects directly under `prefix` (delimiter='/'), paginating if needed."""
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


def display_width(s: str) -> int:
    """Width in monospace cells. CJK glyphs count as 2."""
    return sum(2 if unicodedata.east_asian_width(c) in ("F", "W") else 1 for c in s)


def pad_right(s: str, target_width: int) -> str:
    return s + " " * max(0, target_width - display_width(s))


def human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def build_payload(prefix: str, file_info: dict) -> dict:
    key = file_info["key"]
    filename = key.rsplit("/", 1)[-1]
    download_url = PUBLIC_DOWNLOAD_PREFIX + key
    timestamp_kst = to_kst_string(file_info["last_modified"])
    size_str = human_size(file_info["size"])
    label = PREFIX_LABELS.get(prefix, prefix.rstrip("/"))

    rows = [
        ("파일명", filename),
        ("Timestamp", timestamp_kst),
        ("Size", size_str),
    ]
    label_width = max(display_width(k) for k, _ in rows)
    table_lines = [f"{pad_right(k, label_width)} | {v}" for k, v in rows]
    table_block = "```\n" + "\n".join(table_lines) + "\n```"

    return {
        "text": f"{label}: {filename}",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": label},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": table_block},
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f":arrow_down: <{download_url}|{filename}>"},
                ],
            },
        ],
    }


def post_to_slack(webhook_url: str, payload: dict) -> None:
    resp = requests.post(webhook_url, json=payload, timeout=30)
    resp.raise_for_status()


def process_prefix(prefix: str, session: requests.Session, webhook: Optional[str], dry_run: bool) -> bool:
    """Returns True on success, False on failure."""
    print(f"\n=== prefix: {prefix} ===")
    try:
        objects = list_objects(prefix, session)
    except Exception as e:
        print(f"ERROR listing '{prefix}': {e}", file=sys.stderr)
        return False

    if not objects:
        print(f"WARN: no files under '{prefix}'", file=sys.stderr)
        return False

    latest = find_latest(objects)
    payload = build_payload(prefix, latest)

    print(f"scanned files:     {len(objects)}")
    print(f"latest key:        {latest['key']}")
    print(f"latest timestamp:  {to_kst_string(latest['last_modified'])}")
    print(f"latest size:       {human_size(latest['size'])}")
    print(f"download url:      {PUBLIC_DOWNLOAD_PREFIX + latest['key']}")

    if dry_run:
        print("DRY_RUN=1, skipping Slack post")
        return True

    if not webhook:
        print("ERROR: SLACK_WEBHOOK_URL is not set", file=sys.stderr)
        return False

    try:
        post_to_slack(webhook, payload)
    except Exception as e:
        print(f"ERROR posting '{prefix}' to Slack: {e}", file=sys.stderr)
        return False

    print("posted to Slack")
    return True


def main() -> int:
    prefixes_env = os.environ.get("PREFIXES", "").strip()
    if prefixes_env:
        prefixes = [p.strip() for p in prefixes_env.split(",") if p.strip()]
    else:
        prefixes = list(PREFIXES_DEFAULT)

    dry_run = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
    webhook = os.environ.get("SLACK_WEBHOOK_URL")

    if not dry_run and not webhook:
        print("ERROR: SLACK_WEBHOOK_URL is not set", file=sys.stderr)
        return 1

    session = requests.Session()
    failures: list[str] = []
    for i, prefix in enumerate(prefixes):
        if i > 0 and not dry_run:
            time.sleep(SLACK_POST_GAP_SEC)
        if not process_prefix(prefix, session, webhook, dry_run):
            failures.append(prefix)
    if failures:
        print(f"\nFAILED prefixes: {failures}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
