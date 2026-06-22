#!/usr/bin/env python
"""Step 34: rename Summit's "Cash Buyer" wording to "Active Buyer", and
remove the doc_type=CASHBUYER records that leaked into seller-distress
category exports (tax_delinquent.json, hot_stack.json).

Summit's build_cash_buyer_leads() (scraper/fetch.py) identifies repeat
buyers from real SC750 conveyance data (3+ unique parcels in the trailing
365 days). That's real active-buyer behavior, but the source has no
mortgage/lien data, so it cannot confirm the purchase was actually cash.
"Cash Buyer" overclaims; "Active Buyer" is what the data supports.

This is a one-time data migration for the already-generated JSON/CSV
files (scraper/fetch.py and index.html are fixed separately so future
runs produce the correct wording and isolation from the start).

Does not touch Cuyahoga or Montgomery files. Does not remove any
canonical active-buyer record (cash_buyers.json / records.json) - only
removes the duplicate copies that leaked into category files that should
be seller-distress only.
"""
from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

RENAME_PAIRS = [
    ("dashboard/cash_buyers.json", []),
    ("data/cash_buyers.json", []),
    ("dashboard/records.json", []),
    ("data/records.json", []),
    ("data/records.enriched.json", []),
]

LEAK_FILES = [
    "dashboard/tax_delinquent.json",
    "data/tax_delinquent.json",
    "dashboard/hot_stack.json",
    "data/hot_stack.json",
]

CSV_FILES = [
    "data/records.enriched.csv",
    "data/ghl_export.csv",
]

OLD = "Cash Buyer"
NEW = "Active Buyer"


def rename_record_fields(rec: dict) -> int:
    if rec.get("doc_type") != "CASHBUYER":
        return 0
    changed = 0
    for field in ("flags", "tags"):
        values = rec.get(field) or []
        new_values = [v.replace(OLD, NEW) if isinstance(v, str) and OLD in v else v for v in values]
        if new_values != values:
            rec[field] = new_values
            changed += 1
    cat_label = rec.get("cat_label")
    if isinstance(cat_label, str) and OLD in cat_label:
        rec["cat_label"] = cat_label.replace(OLD, NEW)
        changed += 1
    return changed


def process_rename_file(rel_path: str) -> None:
    path = REPO_ROOT / rel_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    touched = sum(1 for r in records if rename_record_fields(r))
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[rename] {rel_path}: {len(records)} records, {touched} CASHBUYER records relabeled")


def process_leak_file(rel_path: str) -> None:
    path = REPO_ROOT / rel_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    before = len(records)
    kept = [r for r in records if r.get("doc_type") != "CASHBUYER"]
    removed = before - len(kept)
    payload["records"] = kept
    payload["total"] = len(kept)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[leak-fix] {rel_path}: before={before} after={len(kept)} removed={removed} (CASHBUYER records)")


def process_csv_file(rel_path: str) -> None:
    path = REPO_ROOT / rel_path
    text = path.read_text(encoding="utf-8")
    count = text.count(OLD)
    new_text = text.replace(OLD, NEW)
    path.write_text(new_text, encoding="utf-8")
    print(f"[rename] {rel_path}: {count} occurrences of '{OLD}' replaced")


def main() -> int:
    for rel_path, _ in RENAME_PAIRS:
        process_rename_file(rel_path)
    for rel_path in LEAK_FILES:
        process_leak_file(rel_path)
    for rel_path in CSV_FILES:
        process_csv_file(rel_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
