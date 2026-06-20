#!/usr/bin/env python
"""Adapter: county-data-factory dashboard lead CSV -> akron-summit records.json.

Maps a factory lead-export CSV (the dashboard CSV_COLUMNS shape, e.g.
leads_montgomery_oh_2026-06-04.csv) into the same record schema the existing
Summit/Cuyahoga dashboard/<county>/records.json files use, so index.html's
normalizeCountyRecord() + getListSignals() populate owner/property/type/amount
and the tab counts.

MAPPER ONLY. Never fabricates: every value is read from the CSV. Columns that
do not exist (e.g. per-record filing date in this export) are left empty/null,
and the dashboard's own fallbacks handle display. Distress signals are derived
only from real columns (patterns, tax_delinquent_amount, code_violation_count,
sheriff/foreclosure flags).

Usage:
  python tools/factory_csv_to_records.py \
    --in /path/leads_montgomery_oh_2026-06-04.csv \
    --out dashboard/montgomery/records.json \
    --county "Montgomery County" --county-key montgomery \
    --market "Dayton Metro" --source-url https://www.mcohio.org/1521/Delinquent-List \
    --fetched-at 2026-06-04
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path


_ENTITY = re.compile(r"\b(LLC|L\.L\.C|INC|CORP|CO|COMPANY|LTD|LLP|LP|PLLC|TRUST|TR|"
                     r"ENTERPRISES|HOLDINGS|PROPERTIES|GROUP|PARTNERS|ASSOCIATES|"
                     r"REALTY|INVESTMENTS|CAPITAL|LIMITED|BANK|FUND)\b", re.I)
_MAIL = re.compile(r"^\s*(.+?),\s*([A-Za-z .'-]+),\s*([A-Z]{2})\s+([0-9][0-9-]*)\s*$")


def _f(v):
    """Parse a money/number string to float, or None."""
    if v is None:
        return None
    s = str(v).replace("$", "").replace(",", "").strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _jsonlist(v):
    s = (v or "").strip()
    if not s:
        return []
    try:
        out = json.loads(s)
        return out if isinstance(out, list) else [out]
    except (json.JSONDecodeError, TypeError):
        return [s]


def _owner_type(name: str) -> str:
    return "entity" if name and _ENTITY.search(name) else ("individual" if name else "")


def _parse_mailing(v: str):
    m = _MAIL.match(v or "")
    if not m:
        return {"mailing_city": "", "mailing_state": "", "mailing_zip": ""}
    return {"mailing_city": m.group(2).strip(), "mailing_state": m.group(3).strip(),
            "mailing_zip": m.group(4).split("-")[0].strip()}


def map_row(row: dict, county: str, county_key: str, market: str) -> dict:
    owner = (row.get("owner_label") or "").strip()
    street = (row.get("address") or "").strip()
    parcel = (row.get("real_parcel_id") or row.get("parcel_key") or "").strip()
    patterns = [p.lower() for p in _jsonlist(row.get("patterns"))]
    tax_amt = _f(row.get("tax_delinquent_amount"))
    cv_count = int(_f(row.get("code_violation_count")) or 0)
    cv_types = _jsonlist(row.get("code_violation_types"))
    gov_lists = int(_f(row.get("government_list_count")) or 0)
    sheriff = (row.get("sheriff_sale_flag") or "").strip().lower() in ("true", "1", "yes")
    foreclosure = (row.get("foreclosure_notice_flag") or "").strip().lower() in ("true", "1", "yes")

    # distress signals — derived ONLY from real columns, as human labels the
    # dashboard's normalizeListLabel() recognizes.
    distress, flags = [], []
    if "tax" in patterns or (tax_amt or 0) > 0:
        distress.append("Tax Delinquent")
    if "code" in patterns or cv_count > 0:
        distress.append("Code Violation")
    if sheriff:
        distress.append("Sheriff Sale")
    if foreclosure:
        distress.append("Foreclosure")
    is_vacant = any("VACANT" in str(t).upper() for t in cv_types)
    if is_vacant:
        distress.append("Vacant Home")
    flags = list(distress)
    distress_count = gov_lists or len({d for d in distress if d != "Vacant Home"})

    src_urls = _jsonlist(row.get("source_urls"))
    source_url = (src_urls[0] if src_urls else "") or (row.get("source_code_violation_hub_url") or "")
    score = int(_f(row.get("analyzer_score")) or _f(row.get("lead_score")) or 0)

    rec = {
        "county": county,
        "source_county_key": county_key,
        "market_area": market,
        # property (street is real; city/zip are county-context-only in this
        # export, so left empty — the dashboard falls back to primary_city).
        "property_address": street,
        "prop_address": street,
        "property_state": "OH",
        "prop_state": "OH",
        "property_city": "",
        "property_zip": "",
        # owner + mailing
        "owner": owner,
        "owner_name": owner,
        "owner_type": _owner_type(owner),
        "mailing_address": (row.get("owner_mailing_address") or "").strip(),
        "parcel_id": parcel,
        # classification / signals
        "lead_type": " + ".join([d for d in distress if d != "Vacant Home"]) or "Distress Lead",
        "cat_label": " + ".join([d for d in distress if d != "Vacant Home"]) or "Distress Lead",
        "doc_type": "SHERIFF" if sheriff else ("TAX" if (tax_amt or 0) > 0 else "CODEVIOLATION"),
        "distress_sources": distress,
        "distress_count": distress_count,
        "flags": flags,
        "hot_stack": (row.get("lead_tier") or "").strip() == "Hot",
        "seller_score": score,
        "score": score,
        "lead_score": int(_f(row.get("lead_score")) or 0),
        # money (real only; no fabricated equity/arrears)
        "tax_delinquent_amount": tax_amt,
        "estimated_value": _f(row.get("assessed_value")),
        "assessed_value": _f(row.get("assessed_value")),
        "estimated_equity": None,
        "estimated_arrears": None,
        "last_sale_price": _f(row.get("last_sale_price")),
        "last_sale_date": (row.get("last_sale_date") or "").strip(),
        # code-violation detail (real)
        "code_violation_count": cv_count,
        "code_violation_types": cv_types,
        "code_violation_status": (row.get("code_violation_status") or "").strip(),
        "stacked_distress_score": int(_f(row.get("stacked_distress_score")) or 0),
        "is_vacant_home": is_vacant,
        # provenance / links (real)
        "source_url": source_url,
        "public_records_url": source_url,
        "lead_id": (row.get("lead_id") or "").strip(),
        # no real per-record filing date in this export -> honest empty
        "date_filed": (row.get("event_date") or "").strip(),
        "filed": (row.get("event_date") or "").strip(),
    }
    rec.update(_parse_mailing(rec["mailing_address"]))
    return rec


def build(rows, county, county_key, market, source_url, fetched_at):
    recs = [map_row(r, county, county_key, market) for r in rows]
    def cnt(pred):
        return sum(1 for r in recs if pred(r))
    return {
        "source": f"County Data Factory — {county} stacked distress leads",
        "source_url": source_url,
        "fetched_at": fetched_at,
        "record_count": len(recs),
        "total": len(recs),
        "with_address": cnt(lambda r: bool(r["property_address"])),
        "with_mail_address": cnt(lambda r: bool(r["mailing_address"])),
        "hot_stack_count": cnt(lambda r: r["hot_stack"] or (r["distress_count"] or 0) >= 2),
        "tax_delinquent_count": cnt(lambda r: "Tax Delinquent" in r["distress_sources"]),
        "code_violation_count": cnt(lambda r: "Code Violation" in r["distress_sources"]),
        "sheriff_sale_count": cnt(lambda r: "Sheriff Sale" in r["distress_sources"]),
        "vacant_home_count": cnt(lambda r: r["is_vacant_home"]),
        "records": recs,
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--county", required=True)
    p.add_argument("--county-key", required=True)
    p.add_argument("--market", default="")
    p.add_argument("--source-url", default="")
    p.add_argument("--fetched-at", default="")
    a = p.parse_args(argv)

    src = Path(a.inp)
    with open(src, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    payload = build(rows, a.county, a.county_key, a.market, a.source_url, a.fetched_at)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[csv->records] {a.county_key}: {payload['record_count']} records "
          f"(owner={sum(1 for r in payload['records'] if r['owner'])}, "
          f"addr={payload['with_address']}, hot_stack={payload['hot_stack_count']}) -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
