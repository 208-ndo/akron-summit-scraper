#!/usr/bin/env python
"""Adapter: county-data-factory master export -> akron-summit dashboard.

Reads a county-data-factory "master stacked leads" dashboard envelope
(exports/<slug>/<slug>_dashboard_master.json) and writes a
dashboard/<county>/records.json file in the shape this site's
index.html `normalizeCountyRecord()` expects.

This is a MAPPER ONLY. It does not modify the factory source data and it
never fabricates fields — values it cannot derive from the factory record
are left empty/null (the dashboard handles those honestly). Distress
signals come straight from the factory record's boolean flags.

Usage:
    python tools/factory_county_import.py \
        --in /path/to/county-data-factory/exports/clark_oh/clark_oh_dashboard_master.json \
        --out dashboard/clark/records.json \
        --county "Clark County" --county-key clark --primary-city Springfield
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


# factory boolean flag -> (distress_sources token, short human label)
_DISTRESS = [
    ("sheriff_sale",   "sheriff_sale",   "Sheriff Sale"),
    ("tax_delinquent", "tax_delinquent", "Tax Delinquent"),
    ("code_violation", "code_violation", "Code Violation"),
    ("probate",        "probate",        "Probate"),
]
# owner/occupancy flags surfaced as additional distress_sources tokens
_OWNER_FLAGS = [
    ("absentee",     "absentee"),
    ("out_of_state", "out_of_state"),
    ("entity_owner", "entity_owner"),
]


def _first(seq):
    return seq[0] if isinstance(seq, list) and seq else ""


def map_record(r: dict, county_label: str, county_key: str, primary_city: str) -> dict:
    """Map one factory master lead into a dashboard record. Honest nulls;
    every distress flag comes from the factory record itself."""
    distress_sources = [tok for flag, tok, _ in _DISTRESS if r.get(flag)]
    distress_sources += [tok for flag, tok in _OWNER_FLAGS if r.get(flag)]
    labels = [lbl for flag, _, lbl in _DISTRESS if r.get(flag)]
    lead_type = " + ".join(labels) if labels else "Distress Lead"
    priority = r.get("priority") or ""

    return {
        "county": county_label,
        "source_county_key": county_key,
        "city": r.get("property_city") or primary_city,
        "property_address": r.get("property_address") or "",
        "property_city": r.get("property_city") or "",
        "property_state": r.get("property_state") or "OH",
        "property_zip": r.get("property_zip") or "",
        "mailing_address": r.get("mailing_address") or "",
        "mailing_city": r.get("mailing_city") or "",
        "mailing_state": r.get("mailing_state") or "",
        "mailing_zip": r.get("mailing_zip") or "",
        "owner_name": r.get("owner_name") or "",
        "owner_type": r.get("owner_entity_type")
                      or ("entity" if r.get("entity_owner") else "individual"),
        "parcel_id": r.get("parcel_id") or "",
        "lead_type": lead_type,
        "cat_label": lead_type,
        "distress_sources": distress_sources,
        "distress_count": r.get("distress_count") or len(distress_sources),
        "hot_stack": priority == "Hot",
        "seller_score": r.get("score") or 0,
        "score": r.get("score") or 0,
        "priority": priority,
        "case_number": _first(r.get("case_numbers")),
        # carried-through factory context (extra fields are preserved by
        # normalizeCountyRecord's Object.assign) — never fabricated:
        "source_types": r.get("source_types") or [],
        "reason": r.get("reason") or "",
        "recommended_next_action": r.get("recommended_next_action") or "",
        "needs_manual_review": bool(r.get("needs_manual_review")),
        "lead_id": r.get("lead_id") or "",
        # estimated_value / equity / arrears intentionally omitted — the
        # factory master export does not carry them; no fabrication.
    }


def build_payload(envelope: dict, county_label: str, county_key: str,
                  primary_city: str) -> dict:
    records = envelope.get("records") or []
    mapped = [map_record(r, county_label, county_key, primary_city) for r in records]
    return {
        "source": f"County Data Factory — {county_label} master stacked leads",
        "source_url": "",
        "fetched_at": envelope.get("generated_at") or "",
        "record_count": len(mapped),
        "hot_count": sum(1 for r in mapped if r["hot_stack"]),
        "records": mapped,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--in", dest="inp", required=True,
                   help="Path to factory <slug>_dashboard_master.json")
    p.add_argument("--out", required=True,
                   help="Output dashboard/<county>/records.json path")
    p.add_argument("--county", required=True, help="County label, e.g. 'Clark County'")
    p.add_argument("--county-key", required=True, help="County key, e.g. 'clark'")
    p.add_argument("--primary-city", default="", help="Primary city fallback")
    args = p.parse_args(argv)

    src = Path(args.inp)
    if not src.is_file():
        print(f"ERROR: factory export not found: {src}", file=sys.stderr)
        return 1
    envelope = json.loads(src.read_text(encoding="utf-8"))
    if envelope.get("source") != "master":
        print(f"WARNING: expected a master export (source=='master'); "
              f"got source={envelope.get('source')!r}", file=sys.stderr)

    payload = build_payload(envelope, args.county, args.county_key, args.primary_city)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
                   encoding="utf-8")
    print(f"[factory-import] {args.county_key}: wrote {payload['record_count']} "
          f"records ({payload['hot_count']} hot) -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
