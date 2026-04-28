"""For each configured source, fetch its latest package info and post a
Slack message with filename, download URL, KST timestamp, and (where
available) version and size.

Sources:
  - S3 prefix `package/latest/` on repo.whatap.io
  - S3 prefix `rum-onpremise-allinone/` on repo.whatap.io
  - Java agent via maven-metadata-local.xml + fixed download URL on api.whatap.io

Required env:
    SLACK_WEBHOOK_URL   Slack Incoming Webhook URL.

Optional env:
    DRY_RUN             If "1"/"true", skip the Slack POST and just print.
"""

from __future__ import annotations

import os
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

import requests

S3_ENDPOINT = "https://s3.ap-northeast-2.amazonaws.com/repo.whatap.io/"
PUBLIC_DOWNLOAD_PREFIX = "https://repo.whatap.io/"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
KST = timezone(timedelta(hours=9))

# Slack Incoming Webhooks throttle to ~1 msg/sec per hook, but in practice
# bursts get rejected even at 1.2s. 2.5s leaves comfortable headroom.
SLACK_POST_GAP_SEC = 2.5


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

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


def utc_iso_to_kst_string(iso_utc: str) -> str:
    """'2026-04-27T07:08:23.000Z' -> '2026-04-27 16:08:23 KST'."""
    dt = datetime.strptime(iso_utc, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")


def maven_lastupdated_to_kst_string(stamp: str) -> str:
    """Maven's <lastUpdated>20260409020705</lastUpdated> (UTC) -> KST string."""
    dt = datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")


# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------
#
# Each source returns a dict shaped like:
#   {
#     "label":         str,                # Slack header text
#     "filename":      str,                # display filename
#     "download_url":  str,                # clickable link
#     "timestamp_kst": str,                # already KST-formatted
#     "size":          Optional[int],      # bytes, or None to omit Size row
#     "version":       Optional[str],      # version, or None to omit Version row
#   }


def list_s3_objects(prefix: str, session: requests.Session) -> list[dict]:
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


def s3_prefix_source(prefix: str, label: str) -> Callable[[requests.Session], dict]:
    def fetch(session: requests.Session) -> dict:
        objs = list_s3_objects(prefix, session)
        if not objs:
            raise RuntimeError(f"no files found under '{prefix}'")
        latest = max(objs, key=lambda o: o["last_modified"])
        key = latest["key"]
        return {
            "label": label,
            "filename": key.rsplit("/", 1)[-1],
            "download_url": PUBLIC_DOWNLOAD_PREFIX + key,
            "timestamp_kst": utc_iso_to_kst_string(latest["last_modified"]),
            "size": latest["size"],
            "version": None,
        }

    return fetch


def java_agent_source(session: requests.Session) -> dict:
    """Read maven-metadata-local.xml for the latest version + lastUpdated.
    The download URL is the fixed alias on api.whatap.io."""
    metadata_url = (
        PUBLIC_DOWNLOAD_PREFIX
        + "maven/io/whatap/whatap.agent/maven-metadata-local.xml"
    )
    download_url = "https://api.whatap.io/agent/whatap.agent.java.tar.gz"

    resp = session.get(metadata_url, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    versioning = root.find("versioning")
    if versioning is None:
        raise RuntimeError(f"<versioning> missing in {metadata_url}")
    release = versioning.findtext("release", default="").strip()
    last_updated = versioning.findtext("lastUpdated", default="").strip()
    if not release:
        raise RuntimeError(f"<release> missing in {metadata_url}")

    return {
        "label": "Java 에이전트 (whatap.agent)",
        "filename": "whatap.agent.java.tar.gz",
        "download_url": download_url,
        "timestamp_kst": maven_lastupdated_to_kst_string(last_updated) if last_updated else "(unknown)",
        "size": None,
        "version": release,
    }


SOURCES: list[Callable[[requests.Session], dict]] = [
    s3_prefix_source("package/latest/", "WhaTap 최신 패키지 (package/latest)"),
    s3_prefix_source("rum-onpremise-allinone/", "RUM 온프레미스 All-in-one"),
    java_agent_source,
]


# ---------------------------------------------------------------------------
# slack
# ---------------------------------------------------------------------------

def build_payload(info: dict) -> dict:
    rows: list[tuple[str, str]] = [("파일명", info["filename"])]
    if info.get("version"):
        rows.append(("Version", info["version"]))
    rows.append(("Timestamp", info["timestamp_kst"]))
    if info.get("size") is not None:
        rows.append(("Size", human_size(info["size"])))

    label_width = max(display_width(k) for k, _ in rows)
    table_lines = [f"{pad_right(k, label_width)} | {v}" for k, v in rows]
    table_block = "```\n" + "\n".join(table_lines) + "\n```"

    return {
        "text": f"{info['label']}: {info['filename']}",
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": info["label"]}},
            {"type": "section", "text": {"type": "mrkdwn", "text": table_block}},
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f":arrow_down: <{info['download_url']}|{info['filename']}>"},
                ],
            },
        ],
    }


def post_to_slack(webhook_url: str, payload: dict) -> None:
    """POST to Slack, logging status + body. Retries once on 429 honoring Retry-After."""
    for attempt in (1, 2):
        resp = requests.post(webhook_url, json=payload, timeout=30)
        body = (resp.text or "").strip()
        print(f"  slack response (attempt {attempt}): status={resp.status_code} body={body[:200]!r}")
        if resp.status_code == 429 and attempt == 1:
            wait = float(resp.headers.get("Retry-After", "5"))
            print(f"  rate-limited; sleeping {wait}s before retry")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def process_source(
    source: Callable[[requests.Session], dict],
    session: requests.Session,
    webhook: Optional[str],
    dry_run: bool,
) -> bool:
    try:
        info = source(session)
    except Exception as e:
        print(f"ERROR fetching source: {e}", file=sys.stderr)
        return False

    print(f"\n=== {info['label']} ===")
    print(f"  filename:  {info['filename']}")
    if info.get("version"):
        print(f"  version:   {info['version']}")
    print(f"  timestamp: {info['timestamp_kst']}")
    if info.get("size") is not None:
        print(f"  size:      {human_size(info['size'])}")
    print(f"  url:       {info['download_url']}")

    payload = build_payload(info)

    if dry_run:
        print("  DRY_RUN=1, skipping Slack post")
        return True
    if not webhook:
        print("  ERROR: SLACK_WEBHOOK_URL is not set", file=sys.stderr)
        return False
    try:
        post_to_slack(webhook, payload)
    except Exception as e:
        print(f"  ERROR posting to Slack: {e}", file=sys.stderr)
        return False
    print("  posted to Slack")
    return True


def main() -> int:
    dry_run = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not dry_run and not webhook:
        print("ERROR: SLACK_WEBHOOK_URL is not set", file=sys.stderr)
        return 1

    session = requests.Session()
    failures = 0
    for i, source in enumerate(SOURCES):
        if i > 0 and not dry_run:
            time.sleep(SLACK_POST_GAP_SEC)
        if not process_source(source, session, webhook, dry_run):
            failures += 1
    if failures:
        print(f"\n{failures} source(s) failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
