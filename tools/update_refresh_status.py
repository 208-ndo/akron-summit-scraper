#!/usr/bin/env python
"""Shared helper: safely read-merge-write one county's entry in
dashboard/refresh_status.json without touching the other counties'
entries. Called by .github/workflows/daily_refresh_all.yml and
tools/run_daily_refresh_all.py (the orchestrator) for Summit/Cuyahoga,
and by tools/refresh_montgomery_weekday.py for Montgomery - the only
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
  status              - one of success_updated / success_no_change /
                        failed / blocked / stale_source / manual_needed.
  message             - short human-readable reason, required.
  records_before      - record count before this run's pull, if known.
  records_after       - record count after this run's pull, if known.
  new_today           - records with a real first-seen date of today
                        (not just touched/updated today).
  error               - short error detail on failure; empty otherwise.

Honesty rules:
  - Never fabricates data_updated_at - it is only set when the caller
    passes it, and the caller is only supposed to pass it after a real,
    confirmed records.json change.
  - Never fabricates source_dataset_date - same rule.
  - last_checked_at is the only field this script always overwrites,
    because reporting "we checked, here's what we found" is its purpose.

Usage:
  python tools/update_refresh_status.py --county summit --status success_updated \
      --message "Scraper ran and found updated data" \
      --data-updated-at 2026-06-23T00:13:47+00:00 \
      --records-before 3533 --records-after 3547 --new-today 14
  python tools/update_refresh_status.py --county montgomery --status stale_source \
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
VALID_STATUSES = ("success_updated", "success_no_change", "failed", "blocked", "stale_source", "manual_needed")


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
    p.add_argument("--records-before", type=int, default=None)
    p.add_argument("--records-after", type=int, default=None)
    p.add_argument("--new-today", type=int, default=None)
    p.add_argument("--error", default=None)
    args = p.parse_args(argv)

    data = load()
    entry = data.get(args.county, {})
    entry.setdefault("data_updated_at", "")
    entry.setdefault("source_dataset_date", "")
    entry.setdefault("records_before", 0)
    entry.setdefault("records_after", 0)
    entry.setdefault("new_today", 0)
    entry.setdefault("error", "")

    entry["last_checked_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if args.data_updated_at:
        entry["data_updated_at"] = args.data_updated_at
    if args.source_dataset_date:
        entry["source_dataset_date"] = args.source_dataset_date
    if args.records_before is not None:
        entry["records_before"] = args.records_before
    if args.records_after is not None:
        entry["records_after"] = args.records_after
    if args.new_today is not None:
        entry["new_today"] = args.new_today
    entry["error"] = args.error or ""
    entry["status"] = args.status
    entry["message"] = args.message
    data[args.county] = entry

    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({args.county: entry}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
