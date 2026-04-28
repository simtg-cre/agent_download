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
import re
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


def versioned_filename_source(
    prefix: str,
    label: str,
    pattern: re.Pattern,
    version_format: str,
) -> Callable[[requests.Session], dict]:
    """Picks the file under `prefix` whose name matches `pattern` and has the
    highest numeric version (tuple of capture groups). `version_format` is a
    str format applied to the version tuple, e.g. "{0}.{1}-{2}" for RPM
    `2.9-13` or "{0}.{1}.{2}" for DEB `2.9.13`. Files at this prefix often
    share a single LastModified, so ordering by timestamp is unreliable —
    the version number embedded in the filename is the source of truth.
    """
    def fetch(session: requests.Session) -> dict:
        objs = list_s3_objects(prefix, session)
        candidates: list[tuple[tuple[int, ...], dict]] = []
        for o in objs:
            name = o["key"].rsplit("/", 1)[-1]
            m = pattern.match(name)
            if m:
                ver = tuple(int(g) for g in m.groups())
                candidates.append((ver, o))
        if not candidates:
            raise RuntimeError(f"no files matching {pattern.pattern!r} under '{prefix}'")
        candidates.sort(key=lambda x: x[0], reverse=True)
        version_tuple, latest = candidates[0]
        return {
            "label": label,
            "filename": latest["key"].rsplit("/", 1)[-1],
            "download_url": PUBLIC_DOWNLOAD_PREFIX + latest["key"],
            "timestamp_kst": utc_iso_to_kst_string(latest["last_modified"]),
            "size": latest["size"],
            "version": version_format.format(*version_tuple),
        }
    return fetch


def fixed_filename_source(
    prefix: str,
    filename: str,
    label: str,
) -> Callable[[requests.Session], dict]:
    """For prefixes that hold a single canonical filename (e.g. windows/whatap_infra.zip)."""
    def fetch(session: requests.Session) -> dict:
        objs = list_s3_objects(prefix, session)
        target = next(
            (o for o in objs if o["key"].rsplit("/", 1)[-1] == filename),
            None,
        )
        if target is None:
            raise RuntimeError(f"'{filename}' not found under '{prefix}'")
        return {
            "label": label,
            "filename": filename,
            "download_url": PUBLIC_DOWNLOAD_PREFIX + target["key"],
            "timestamp_kst": utc_iso_to_kst_string(target["last_modified"]),
            "size": target["size"],
            "version": None,
        }
    return fetch


def make_java_agent_source(label: str) -> Callable[[requests.Session], dict]:
    """Read maven-metadata-local.xml for the latest version + lastUpdated.
    The download URL is the fixed alias on api.whatap.io."""
    def fetch(session: requests.Session) -> dict:
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
            "label": label,
            "filename": "whatap.agent.java.tar.gz",
            "download_url": download_url,
            "timestamp_kst": maven_lastupdated_to_kst_string(last_updated) if last_updated else "(unknown)",
            "size": None,
            "version": release,
        }
    return fetch


CATEGORIES: list[dict] = [
    {
        "title": "수집서버",
        "summary_prefix": "수집서버",
        "sources": [
            s3_prefix_source("package/latest/", "수집서버"),
        ],
    },
    {
        "title": "브라우저",
        "summary_prefix": "브라우저",
        "sources": [
            s3_prefix_source("rum-onpremise-allinone/", "브라우저"),
        ],
    },
    {
        "title": "Java 에이전트",
        "summary_prefix": "Java",
        "sources": [
            make_java_agent_source("Java"),
        ],
    },
    {
        "title": "서버 에이전트 (RHEL 계열)",
        "summary_prefix": "RHEL",
        "sources": [
            versioned_filename_source(
                "centos/latest/x86_64/",
                "x86_64",
                re.compile(r"^whatap-infra-(\d+)\.(\d+)-(\d+)\.x86_64\.rpm$"),
                "{0}.{1}-{2}",
            ),
            versioned_filename_source(
                "centos/latest/aarch64/",
                "aarch64",
                re.compile(r"^whatap-infra-(\d+)\.(\d+)-(\d+)\.aarch64\.rpm$"),
                "{0}.{1}-{2}",
            ),
        ],
    },
    {
        "title": "서버 에이전트 (Ubuntu 계열)",
        "summary_prefix": "Ubuntu",
        "sources": [
            versioned_filename_source(
                "debian/unstable/",
                "amd64",
                re.compile(r"^whatap-infra_(\d+)\.(\d+)\.(\d+)_amd64\.deb$"),
                "{0}.{1}.{2}",
            ),
            versioned_filename_source(
                "debian/unstable/",
                "arm64",
                re.compile(r"^whatap-infra_(\d+)\.(\d+)\.(\d+)_arm64\.deb$"),
                "{0}.{1}.{2}",
            ),
        ],
    },
    {
        "title": "서버 에이전트 (Windows)",
        "summary_prefix": "Windows",
        "sources": [
            fixed_filename_source(
                "windows/",
                "whatap_infra.zip",
                "Windows",
            ),
        ],
    },
]


# ---------------------------------------------------------------------------
# slack
# ---------------------------------------------------------------------------

def _short_timestamp(ts_kst: str) -> str:
    """'2026-04-27 16:08:23 KST' -> '2026-04-27 16:08:23' (drop the KST suffix)."""
    return ts_kst.removesuffix(" KST")


def build_category_payload(category_title: str, infos: list[dict]) -> dict:
    # Drop the 구분 column when there is only one row (it'd just repeat
    # the category header).
    show_label_col = len(infos) > 1
    headers = (["구분"] if show_label_col else []) + ["파일명", "Version", "Timestamp", "Size"]
    rows = [
        ([info["label"]] if show_label_col else []) + [
            info["filename"],
            info.get("version") or "-",
            _short_timestamp(info["timestamp_kst"]),
            human_size(info["size"]) if info.get("size") is not None else "-",
        ]
        for info in infos
    ]

    widths = [
        max(display_width(headers[i]), max((display_width(r[i]) for r in rows), default=0))
        for i in range(len(headers))
    ]

    def fmt_row(cells: list[str]) -> str:
        return "  ".join(pad_right(c, widths[i]) for i, c in enumerate(cells))

    sep = ["-" * widths[i] for i in range(len(headers))]
    table_lines = [fmt_row(headers), fmt_row(sep)] + [fmt_row(r) for r in rows]
    table_block = "```\n" + "\n".join(table_lines) + "\n```"

    download_lines = "\n".join(
        f":arrow_down: <{info['download_url']}|{info['filename']}>"
        for info in infos
    )

    return {
        "text": f"{category_title} ({len(infos)})",
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": category_title}},
            {"type": "section", "text": {"type": "mrkdwn", "text": table_block}},
            {"type": "section", "text": {"type": "mrkdwn", "text": download_lines}},
        ],
    }


def build_summary_payload(collected: list[tuple[dict, dict]]) -> dict:
    """Build a final consolidated table covering every category at once.

    `collected` is a list of (category, info) tuples in the order they were
    processed. The 구분 column combines each category's summary_prefix with
    the per-source label when the category has multiple sources, and is just
    the prefix when single-source."""
    headers = ["구분", "파일명", "Version", "Timestamp", "Size"]
    rows: list[list[str]] = []
    for category, info in collected:
        multi = len(category["sources"]) > 1
        label = (
            f"{category['summary_prefix']} {info['label']}"
            if multi
            else category["summary_prefix"]
        )
        rows.append([
            label,
            info["filename"],
            info.get("version") or "-",
            _short_timestamp(info["timestamp_kst"]),
            human_size(info["size"]) if info.get("size") is not None else "-",
        ])

    widths = [
        max(display_width(headers[i]), max((display_width(r[i]) for r in rows), default=0))
        for i in range(len(headers))
    ]

    def fmt_row(cells: list[str]) -> str:
        return "  ".join(pad_right(c, widths[i]) for i, c in enumerate(cells))

    sep = ["-" * widths[i] for i in range(len(headers))]
    table_lines = [fmt_row(headers), fmt_row(sep)] + [fmt_row(r) for r in rows]
    table_block = "```\n" + "\n".join(table_lines) + "\n```"

    download_lines = "\n".join(
        f":arrow_down: <{info['download_url']}|{info['filename']}>"
        for _, info in collected
    )

    title = "\U0001F4CB 전체 다운로드 한눈에 보기"
    return {
        "text": title,
        "blocks": [
            {"type": "divider"},
            {"type": "header", "text": {"type": "plain_text", "text": title}},
            {"type": "section", "text": {"type": "mrkdwn", "text": table_block}},
            {"type": "section", "text": {"type": "mrkdwn", "text": download_lines}},
        ],
    }


def build_title_payload() -> dict:
    """Header-only message used as a daily title above the package list."""
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    title = f"\U0001F4E6 WhaTap 일일 패키지 알림 - {now_kst}"
    return {
        "text": title,
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": title}},
            {"type": "divider"},
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

def process_category(
    category: dict,
    session: requests.Session,
    webhook: Optional[str],
    dry_run: bool,
) -> tuple[bool, list[dict]]:
    """Fetch every source in the category, build one combined Slack message
    with a multi-row table, and post it. Returns (success, infos) — infos
    is the list of successfully fetched source dicts (empty on full failure)
    so the caller can build a final summary across all categories."""
    title = category["title"]
    print(f"\n=== {title} ===")
    infos: list[dict] = []
    fetch_failures = 0
    for source in category["sources"]:
        try:
            info = source(session)
        except Exception as e:
            fetch_failures += 1
            print(f"  ERROR fetching source: {e}", file=sys.stderr)
            continue
        infos.append(info)
        print(
            f"  ok: [{info['label']}] {info['filename']}"
            + (f"  v{info['version']}" if info.get("version") else "")
            + f"  {_short_timestamp(info['timestamp_kst'])}"
            + (f"  {human_size(info['size'])}" if info.get("size") is not None else "")
        )

    if not infos:
        print("  no rows fetched; skipping Slack post", file=sys.stderr)
        return False, []

    payload = build_category_payload(title, infos)

    if dry_run:
        print("  DRY_RUN=1, skipping Slack post")
        return fetch_failures == 0, infos
    if not webhook:
        print("  ERROR: SLACK_WEBHOOK_URL is not set", file=sys.stderr)
        return False, infos
    try:
        post_to_slack(webhook, payload)
    except Exception as e:
        print(f"  ERROR posting to Slack: {e}", file=sys.stderr)
        return False, infos
    print("  posted to Slack")
    return fetch_failures == 0, infos


def main() -> int:
    # Make local prints survive non-UTF-8 consoles (Windows cp949 etc.).
    # No-op on UTF-8 environments like GitHub Actions runners.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

    dry_run = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not dry_run and not webhook:
        print("ERROR: SLACK_WEBHOOK_URL is not set", file=sys.stderr)
        return 1

    session = requests.Session()
    failures = 0

    title_payload = build_title_payload()
    print(f"=== title ===\n  {title_payload['text']}")
    if dry_run:
        print("  DRY_RUN=1, skipping Slack post")
    elif webhook:
        try:
            post_to_slack(webhook, title_payload)
            print("  posted to Slack")
        except Exception as e:
            print(f"  ERROR posting title to Slack: {e}", file=sys.stderr)
            failures += 1

    collected: list[tuple[dict, dict]] = []
    for category in CATEGORIES:
        if not dry_run:
            time.sleep(SLACK_POST_GAP_SEC)
        success, infos = process_category(category, session, webhook, dry_run)
        if not success:
            failures += 1
        for info in infos:
            collected.append((category, info))

    # Final consolidated summary across every category we successfully fetched.
    if collected:
        if not dry_run:
            time.sleep(SLACK_POST_GAP_SEC)
        summary_payload = build_summary_payload(collected)
        print(f"\n=== summary ({len(collected)} rows) ===")
        if dry_run:
            print("  DRY_RUN=1, skipping Slack post")
        elif webhook:
            try:
                post_to_slack(webhook, summary_payload)
                print("  posted to Slack")
            except Exception as e:
                print(f"  ERROR posting summary to Slack: {e}", file=sys.stderr)
                failures += 1

    if failures:
        print(f"\n{failures} message(s) failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
