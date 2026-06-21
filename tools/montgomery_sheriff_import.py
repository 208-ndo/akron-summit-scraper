#!/usr/bin/env python
"""Adapter: county-data-factory Montgomery sheriff-sale legal-notice JSONL ->
akron-summit dashboard/montgomery/records.json sheriff-sale leads.

Maps raw_payload.attributes from a county-data-factory raw scrape
(e.g. montgomery_dayton_sheriff_sales.jsonl) into the same record schema
dashboard/montgomery/records.json already uses, and appends them to the
existing Montgomery lead set (no dedup needed -- this source has zero
parcel overlap with the existing tax/code-violation leads).

MAPPER ONLY. Never fabricates: every value is read from the source JSONL.
Owner/defendant is recovered by regex from the legal-notice free text
(`notice_description`), since the parser-extracted `defendant` field is
frequently a truncated first-token fragment (e.g. "CHRISTOPHER" instead of
"CHRISTOPHER M BRIDEWELL"). Records missing case_number or a cleanly
parseable defendant are skipped rather than imported with guessed values.

Usage:
  python tools/montgomery_sheriff_import.py \
    --in /path/montgomery_dayton_sheriff_sales.jsonl \
    --out dashboard/montgomery/records.json \
    --today 2026-06-20
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path

_ENTITY = re.compile(r"\b(LLC|L\.L\.C|INC|CORP|CO|COMPANY|LTD|LLP|LP|PLLC|TRUST|TR|"
                     r"ENTERPRISES|HOLDINGS|PROPERTIES|GROUP|PARTNERS|ASSOCIATES|"
                     r"REALTY|INVESTMENTS|CAPITAL|LIMITED|BANK|FUND)\b", re.I)
_ADDR = re.compile(r"^\s*(.+?),\s*([A-Za-z .'-]+),\s*([A-Z]{2})\s+([0-9][0-9-]*)\s*$")
_DEFENDANT = re.compile(r"VS\.?\s+(.+?)\s*,?\s*et\s+al\.?", re.I)


def _owner_type(name: str) -> str:
    return "entity" if name and _ENTITY.search(name) else ("individual" if name else "")


def extract_defendant(notice: str | None) -> str | None:
    if not notice:
        return None
    m = _DEFENDANT.search(notice)
    if not m:
        return None
    name = m.group(1).strip()
    return name if len(name) >= 2 else None


def parse_address(full_address: str):
    m = _ADDR.match(full_address or "")
    if not m:
        return {"street": (full_address or "").strip(), "city": "", "state": "OH", "zip": ""}
    return {"street": m.group(1).strip(), "city": m.group(2).strip(),
            "state": m.group(3).strip(), "zip": m.group(4).split("-")[0].strip()}


def auction_urgency(sale_date: str, today: date):
    try:
        sale = datetime.strptime(sale_date, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None
    days = (sale - today).days
    if days < 0:
        return {"days": days, "label": "past sale", "urgency": "gray"}
    if days == 0:
        return {"days": days, "label": "sale today", "urgency": "hot"}
    return {"days": days, "label": f"{days} days left",
            "urgency": "hot" if days <= 3 else "warn" if days <= 7 else "gold" if days <= 30 else "green"}


def map_record(raw: dict, today: date) -> dict | None:
    a = raw.get("raw_payload", {}).get("attributes", {})
    case_number = (a.get("case_number") or "").strip()
    defendant = extract_defendant(a.get("notice_description"))
    if not case_number or not defendant:
        return None

    addr = parse_address(a.get("full_property_address") or "")
    appraised = a.get("appraised_value")
    sale_date = (a.get("sale_date") or "").strip()
    source_url = (raw.get("source_url") or a.get("detail_url") or "").strip()
    urgency = auction_urgency(sale_date, today) or {}
    pulled = (raw.get("first_seen_at") or raw.get("source_fetched_at") or "")[:10]
    year_built = a.get("year_built")

    return {
        "county": "Montgomery County",
        "source_county_key": "montgomery",
        "market_area": "Dayton Metro",
        "property_address": addr["street"],
        "prop_address": addr["street"],
        "property_state": addr["state"] or "OH",
        "prop_state": addr["state"] or "OH",
        "property_city": addr["city"],
        "prop_city": addr["city"],
        "property_zip": addr["zip"],
        "prop_zip": addr["zip"],
        "owner": defendant,
        "owner_name": defendant,
        "owner_type": _owner_type(defendant),
        "mailing_address": "",
        "parcel_id": (a.get("parcel_id") or "").strip(),
        "case_number": case_number,
        "doc_number": case_number,
        "lead_type": "Sheriff Sale",
        "cat_label": "Sheriff Sale",
        "doc_type": "SHERIFFSALE",
        "distress_sources": ["sheriff_sale"],
        "distress_count": 1,
        "flags": ["Sheriff Sale", "Sale scheduled"],
        "hot_stack": False,
        "seller_score": 0,
        "score": 0,
        "lead_score": 0,
        "amount": float(appraised) if appraised is not None else None,
        "appraised_value": float(appraised) if appraised is not None else None,
        "tax_delinquent_amount": None,
        "estimated_value": None,
        "assessed_value": None,
        "estimated_equity": None,
        "estimated_arrears": None,
        "last_sale_price": None,
        "last_sale_date": "",
        "sheriff_sale_date": sale_date,
        "auction_days_until": urgency.get("days"),
        "auction_countdown": urgency.get("label", ""),
        "auction_urgency": urgency.get("urgency", ""),
        "code_violation_count": 0,
        "code_violation_types": [],
        "code_violation_status": "",
        "stacked_distress_score": 0,
        "is_vacant_home": False,
        "entity_owner": _owner_type(defendant) == "entity",
        "is_absentee": False,
        "is_out_of_state": False,
        "tired_landlord": False,
        "tags": ["Sheriff Sale", "Sale scheduled"],
        "year_built": int(year_built) if year_built else None,
        "property_age": (today.year - int(year_built)) if year_built else None,
        "living_area_sqft": None,
        "square_feet": None,
        "bedrooms": None,
        "bathrooms": None,
        "lot_acres": None,
        "lot_size": None,
        "property_type": None,
        "land_use": None,
        "building_style": None,
        "assessed_total": None,
        "enrichment_status": "",
        "matched_owner_name": "",
        "source_url": source_url,
        "public_records_url": source_url,
        "lead_id": (raw.get("raw_record_id") or "").strip(),
        "date_filed": "",
        "filed": "",
        "first_seen_date": pulled,
        "pulled_date": pulled,
        "last_updated": pulled,
        "mailing_city": "",
        "mailing_state": "",
        "mailing_zip": "",
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--today", required=True, help="YYYY-MM-DD; reference date for auction-urgency labels")
    args = p.parse_args(argv)

    today = datetime.strptime(args.today, "%Y-%m-%d").date()

    src = Path(args.inp)
    raw_records = []
    with open(src, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                raw_records.append(json.loads(line))

    skipped = 0
    mapped = []
    for raw in raw_records:
        rec = map_record(raw, today)
        if rec is None:
            skipped += 1
            continue
        mapped.append(rec)

    out_path = Path(args.out)
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    payload["records"].extend(mapped)
    payload["record_count"] = len(payload["records"])
    payload["total"] = len(payload["records"])
    payload["sheriff_sale_count"] = sum(1 for r in payload["records"] if "sheriff_sale" in (r.get("distress_sources") or []))

    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[montgomery_sheriff_import] read {len(raw_records)} raw records, "
          f"imported {len(mapped)}, skipped {skipped} (missing case_number/owner)")
    print(f"[montgomery_sheriff_import] {out_path}: total={payload['total']} "
          f"sheriff_sale_count={payload['sheriff_sale_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
