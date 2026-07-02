#!/usr/bin/env python
"""One-time honest backfill of first_seen_date on Cuyahoga records.

Problem (found by tests/test_daily_refresh_safety.py on 2026-07-01):
only 42 of 11,416 Cuyahoga records carried a first_seen_date, because
scraper/counties/cuyahoga.py only stamped it on standalone sheriff
imports. Today's Leads therefore undercounted Cuyahoga.

Honesty rules for the backfill (no fabricated recency, nothing ever
falsely shows as a Today's Lead):
  1. Prefer the record's own real past date: date_filed / filed (a
     documented court/violation date).
  2. Else use last_updated (a real system timestamp) - but only if it
     is strictly before today.
  3. Else, if the record already existed in the previous git commit of
     dashboard/cuyahoga/records.json, use that commit's date (proof it
     existed at least since then).
  4. Records that genuinely cannot be dated keep first_seen_date empty
     rather than getting a made-up date.
Every backfilled record is tagged first_seen_backfilled=true so the
approximation is documented in the data itself.

Run once, review the summary, commit. Safe to re-run (idempotent - it
only touches records with an empty first_seen_date).
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TARGET = REPO / "dashboard" / "cuyahoga" / "records.json"
DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")


def extract_date(value) -> str:
    m = DATE_RE.match(str(value or "").strip())
    return m.group(1) if m else ""


def previous_commit_membership() -> tuple[set, str]:
    """Keys of records in the previous committed version + that commit's date."""
    try:
        log = subprocess.run(
            ["git", "log", "-2", "--pretty=%H %cs", "--", str(TARGET.relative_to(REPO))],
            cwd=REPO, capture_output=True, text=True)
        lines = [l for l in log.stdout.strip().splitlines() if l]
        if len(lines) < 2:
            return set(), ""
        prev_hash, prev_date = lines[1].split()
        show = subprocess.run(
            ["git", "show", f"{prev_hash}:dashboard/cuyahoga/records.json"],
            cwd=REPO, capture_output=True, text=True)
        payload = json.loads(show.stdout)
        keys = set()
        for rec in payload.get("records", []):
            for f in ("parcel_id", "case_number", "complaint_number", "doc_num"):
                v = str(rec.get(f) or "").strip().upper()
                if v:
                    keys.add(f"{f}:{v}")
        return keys, prev_date
    except Exception as e:
        print(f"warning: could not read previous commit: {e}")
        return set(), ""


def main() -> int:
    payload = json.loads(TARGET.read_text(encoding="utf-8"))
    records = payload.get("records", [])
    today = datetime.now().date().isoformat()
    prev_keys, prev_date = previous_commit_membership()

    counts = {"already": 0, "filed": 0, "last_updated": 0, "prev_commit": 0, "left_empty": 0}
    for rec in records:
        if str(rec.get("first_seen_date") or "").strip():
            counts["already"] += 1
            continue
        filed = extract_date(rec.get("date_filed") or rec.get("filed"))
        lu = extract_date(rec.get("last_updated"))
        chosen = ""
        if filed and filed < today:
            chosen, bucket = filed, "filed"
        elif lu and lu < today:
            chosen, bucket = lu, "last_updated"
        else:
            in_prev = any(
                f"{f}:{str(rec.get(f) or '').strip().upper()}" in prev_keys
                for f in ("parcel_id", "case_number", "complaint_number", "doc_num")
                if str(rec.get(f) or "").strip())
            if in_prev and prev_date:
                chosen, bucket = prev_date, "prev_commit"
            else:
                counts["left_empty"] += 1
                continue
        rec["first_seen_date"] = chosen
        rec["first_seen_backfilled"] = True
        counts[bucket] += 1

    TARGET.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"total": len(records), **counts}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
