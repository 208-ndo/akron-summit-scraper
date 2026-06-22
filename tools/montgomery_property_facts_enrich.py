#!/usr/bin/env python
"""Enrich dashboard/montgomery/records.json with real bedrooms/bathrooms/
square_feet/year_built/property_type/lot_acres from the Montgomery County
Auditor's public GIS/CAMA layer.

Source: Montgomery County Auditor GIS, VantagePoints/AUDGIS_B1, layer 7
(SDE.mc_parcel_polygon joined to SDE.WEB_CAMA -- the county's own CAMA
system, served publicly with no auth):
  https://gis.mcohio.org/server/rest/services/VantagePoints/AUDGIS_B1/MapServer/7/query

Joins by normalized TAXPINNO == normalized parcel_id (both sides stripped
of whitespace, uppercased, since spacing varies between the two sources).
Queries only the parcel IDs already present in records.json, in batches,
rather than bulk-downloading the whole county.

Never invents a value: a field is only set when (a) the source has a real,
non-zero value and (b) the existing record field is currently missing/
null/empty. Existing populated values are left untouched -- this is a gap
fill, not a re-derivation.

Usage:
  python tools/montgomery_property_facts_enrich.py --out dashboard/montgomery/records.json
  python tools/montgomery_property_facts_enrich.py --out dashboard/montgomery/records.json --dry-run
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import requests

LAYER_URL = "https://gis.mcohio.org/server/rest/services/VantagePoints/AUDGIS_B1/MapServer/7/query"
OUT_FIELDS = [
    "SDE.mc_parcel_polygon.TAXPINNO",
    "SDE.WEB_CAMA.DWEL_RMBED",
    "SDE.WEB_CAMA.DWEL_FIXBATH",
    "SDE.WEB_CAMA.DWEL_FIXHALF",
    "SDE.WEB_CAMA.DWEL_SFLA",
    "SDE.WEB_CAMA.DWEL_YRBLT",
    "SDE.WEB_CAMA.LUC",
    "SDE.WEB_CAMA.ACRES",
]
BATCH_SIZE = 100


def normalize_pin(pin) -> str:
    return re.sub(r"\s+", "", str(pin or "").strip().upper())


def has_value(v) -> bool:
    return v is not None and v != "" and v != 0 and v != 0.0


def fetch_cama_batch(pins: list[str]) -> list[dict]:
    quoted = ",".join(f"'{p}'" for p in pins)
    params = {
        "where": f"TAXPINNO IN ({quoted})",
        "outFields": ",".join(OUT_FIELDS),
        "returnGeometry": "false",
        "f": "json",
    }
    resp = requests.get(LAYER_URL, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(f"ArcGIS query error: {payload['error']}")
    return [f["attributes"] for f in payload.get("features", [])]


def build_cama_lookup(parcel_ids: list[str]) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    # the source TAXPINNO retains its original spacing; we query using the
    # records.json spelling directly since exact-match IN() needs the real
    # string, then index results by normalized form for lookup robustness.
    raw_pins = sorted({p.strip() for p in parcel_ids if p and p.strip()})
    for i in range(0, len(raw_pins), BATCH_SIZE):
        batch = raw_pins[i : i + BATCH_SIZE]
        attrs_list = fetch_cama_batch(batch)
        for attrs in attrs_list:
            pin = normalize_pin(attrs.get("SDE.mc_parcel_polygon.TAXPINNO"))
            if pin:
                lookup[pin] = attrs
    return lookup


def derive_facts(attrs: dict) -> dict:
    facts: dict = {}

    bedrooms = attrs.get("SDE.WEB_CAMA.DWEL_RMBED")
    if has_value(bedrooms):
        facts["bedrooms"] = int(bedrooms)

    full_baths = attrs.get("SDE.WEB_CAMA.DWEL_FIXBATH") or 0
    half_baths = attrs.get("SDE.WEB_CAMA.DWEL_FIXHALF") or 0
    if has_value(full_baths) or has_value(half_baths):
        facts["full_baths"] = int(full_baths)
        facts["half_baths"] = int(half_baths)
        facts["bathrooms"] = int(full_baths) + 0.5 * int(half_baths)

    sqft = attrs.get("SDE.WEB_CAMA.DWEL_SFLA")
    if has_value(sqft):
        facts["square_feet"] = int(sqft)

    year_built = attrs.get("SDE.WEB_CAMA.DWEL_YRBLT")
    if has_value(year_built):
        facts["year_built"] = int(year_built)

    luc = attrs.get("SDE.WEB_CAMA.LUC")
    if has_value(luc):
        facts["property_type"] = str(luc).strip()

    acres_raw = attrs.get("SDE.WEB_CAMA.ACRES")
    try:
        acres = float(acres_raw)
        if acres > 0:
            facts["lot_acres"] = acres
    except (TypeError, ValueError):
        pass

    return facts


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", required=True, help="path to dashboard/montgomery/records.json")
    p.add_argument("--dry-run", action="store_true", help="report counts without writing the file")
    args = p.parse_args(argv)

    out_path = Path(args.out)
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    before_count = len(records)

    parcel_ids = [r.get("parcel_id") for r in records if r.get("parcel_id")]
    lookup = build_cama_lookup(parcel_ids)
    print(f"[montgomery_property_facts_enrich] queried {len(set(normalize_pin(p) for p in parcel_ids))} "
          f"distinct parcel IDs, matched {len(lookup)} in source")

    filled = {"bedrooms": 0, "bathrooms": 0, "square_feet": 0, "year_built": 0,
              "property_type": 0, "lot_acres": 0}
    matched_records = 0

    for rec in records:
        pin = normalize_pin(rec.get("parcel_id"))
        attrs = lookup.get(pin)
        if not attrs:
            continue
        matched_records += 1
        facts = derive_facts(attrs)

        if "bedrooms" in facts and not has_value(rec.get("bedrooms")):
            rec["bedrooms"] = facts["bedrooms"]
            filled["bedrooms"] += 1
        if "bathrooms" in facts and not has_value(rec.get("bathrooms")):
            rec["bathrooms"] = facts["bathrooms"]
            rec["full_baths"] = facts["full_baths"]
            rec["half_baths"] = facts["half_baths"]
            filled["bathrooms"] += 1
        if "square_feet" in facts and not has_value(rec.get("square_feet")) and not has_value(rec.get("living_area_sqft")):
            rec["square_feet"] = facts["square_feet"]
            rec["living_area_sqft"] = facts["square_feet"]
            filled["square_feet"] += 1
        if "year_built" in facts and not has_value(rec.get("year_built")):
            rec["year_built"] = facts["year_built"]
            filled["year_built"] += 1
        if "property_type" in facts and not has_value(rec.get("property_type")) and not has_value(rec.get("land_use")):
            rec["property_type"] = facts["property_type"]
            rec["land_use"] = facts["property_type"]
            filled["property_type"] += 1
        if "lot_acres" in facts and not has_value(rec.get("lot_acres")) and not has_value(rec.get("lot_size")):
            rec["lot_acres"] = facts["lot_acres"]
            rec["lot_size"] = facts["lot_acres"]
            filled["lot_acres"] += 1

    after_count = len(records)
    print(f"[montgomery_property_facts_enrich] records: before={before_count} after={after_count} "
          f"(matched={matched_records})")
    print(f"[montgomery_property_facts_enrich] fields filled: {filled}")

    if args.dry_run:
        print("[montgomery_property_facts_enrich] --dry-run: not writing output")
        return 0

    if after_count != before_count:
        print("[montgomery_property_facts_enrich] ABORT: record count changed, not writing", file=sys.stderr)
        return 1

    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[montgomery_property_facts_enrich] wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
