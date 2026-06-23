#!/usr/bin/env python
"""Shared helper: safely read-merge-write one county's entry in
dashboard/refresh_status.json without touching the other counties'
entries. Called by .github/workflows/scrape.yml (Summit),
.github/workflows/cuyahoga_refresh.yml (Cuyahoga), and
tools/refresh_montgomery_weekday.py (Montgomery) - the only three
places that should ever write to this file.

Field definitions:
  data_updated_at     - only changes when records.json's data actually
                        changed this run. Never written unless the
                        caller passes --data-updated-at explicitly
                        (which the caller should only do after confirming
                        a real change); otherwise the previous value is
                        preserved untouched.
  last_checked_at     - always set to the real time this script runs.
                        This is the whole point of the file: prove a
                        check happened even when nothing changed.
  source_dataset_date - date of the actual source file/data, when one
                        exists (Montgomery's dated CSV). Optional;
                        preserved untouched if not passed.
  status              - one of success / no_change / skipped_stale_source
                        / failed / blocked / manual_needed.
  message             - short human-readable reason, required.

Honesty rules:
  - Never fabricates data_updated_at - it is only set when the caller
    passes it, and the caller is only supposed to pass it after a real,
    confirmed records.json change.
  - Never fabricates source_dataset_date - same rule.
  - last_checked_at is the only field this script always overwrites,
    because reporting "we checked, here's what we found" is its purpose.

Usage:
  python tools/update_refresh_status.py --county summit --status success \
      --message "Scraper ran and found updated data" \
      --data-updated-at 2026-06-23T00:13:47+00:00
  python tools/update_refresh_status.py --county montgomery --status skipped_stale_source \
      --message "Source CSV still dated 2026-06-04 - no newer export available" \
      --source-dataset-date 2026-06-04
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
STATUS_PATH = REPO / "dashboard" / "refresh_status.json"

COUNTIES = ("summit", "cuyahoga", "montgomery")
VALID_STATUSES = ("success", "no_change", "skipped_stale_source", "failed", "blocked", "manual_needed")


def load() -> dict:
    if STATUS_PATH.is_file():
        try:
            return json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--county", required=True, choices=COUNTIES)
    p.add_argument("--status", required=True, choices=VALID_STATUSES)
    p.add_argument("--message", required=True)
    p.add_argument("--data-updated-at", default=None,
                   help="pass only after confirming records.json genuinely changed this run")
    p.add_argument("--source-dataset-date", default=None)
    args = p.parse_args(argv)

    data = load()
    entry = data.get(args.county, {})
    entry.setdefault("data_updated_at", "")
    entry.setdefault("source_dataset_date", "")
    entry["last_checked_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if args.data_updated_at:
        entry["data_updated_at"] = args.data_updated_at
    if args.source_dataset_date:
        entry["source_dataset_date"] = args.source_dataset_date
    entry["status"] = args.status
    entry["message"] = args.message
    data[args.county] = entry

    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({args.county: entry}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
