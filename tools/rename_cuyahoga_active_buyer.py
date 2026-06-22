#!/usr/bin/env python
"""Step 36: rename Cuyahoga's "Cash Buyer Candidate" wording to "Active
Buyer Candidate" in the already-generated dashboard/cuyahoga/records.json.

enrich_transfer_history() (scraper/counties/cuyahoga.py) flags a record as
a buyer candidate purely from owner-name-looks-like-an-entity +
one recent transfer + a nonzero sale price - no mortgage/lien data, so it
cannot confirm cash payment. "Cash Buyer Candidate" overclaimed what the
data supports; "Active Buyer Candidate" is accurate (matches the Summit
Step 34 rename rationale).

Renames only the display/data text:
  - flags / tags entries: "Cash Buyer Candidate" -> "Active Buyer Candidate"
  - distress_sources entry: "cash_buyer_candidate" -> "active_buyer_candidate"
  - buyer_type: "Cash Buyer Candidate" -> "Active Buyer Candidate"

Leaves the internal boolean field key cash_buyer_candidate (and
confirmed_cash_buyer) unchanged - those are backward-compatible internal
identifiers, not displayed text, and renaming them is unnecessary risk.
Does not touch count/scoring logic, doc_type, or any other field.
"""
from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET = REPO_ROOT / "dashboard" / "cuyahoga" / "records.json"

OLD_TITLE = "Cash Buyer Candidate"
NEW_TITLE = "Active Buyer Candidate"
OLD_SLUG = "cash_buyer_candidate"
NEW_SLUG = "active_buyer_candidate"


def rename_record_fields(rec: dict) -> int:
    changed = 0
    for field in ("flags", "tags"):
        values = rec.get(field) or []
        new_values = [v.replace(OLD_TITLE, NEW_TITLE) if isinstance(v, str) and OLD_TITLE in v else v for v in values]
        if new_values != values:
            rec[field] = new_values
            changed += 1
    distress_sources = rec.get("distress_sources") or []
    new_distress = [NEW_SLUG if v == OLD_SLUG else v for v in distress_sources]
    if new_distress != distress_sources:
        rec["distress_sources"] = new_distress
        changed += 1
    buyer_type = rec.get("buyer_type")
    if isinstance(buyer_type, str) and OLD_TITLE in buyer_type:
        rec["buyer_type"] = buyer_type.replace(OLD_TITLE, NEW_TITLE)
        changed += 1
    return changed


def main() -> int:
    payload = json.loads(TARGET.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    before_count = len(records)
    touched = sum(1 for r in records if rename_record_fields(r))
    after_count = len(records)
    if after_count != before_count:
        print("ABORT: record count changed, not writing")
        return 1
    TARGET.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[rename] {TARGET}: {before_count} records, {touched} relabeled (Cash Buyer Candidate -> Active Buyer Candidate)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
