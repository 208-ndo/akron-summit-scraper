from __future__ import annotations

import argparse
import json
import re
import shutil
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = REPO_ROOT / "dashboard" / "cuyahoga" / "records.json"
SOURCE_NAME = "Cleveland Open Data - Complaint Violation Notices"
SOURCE_URL = "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/Complaint_Violation_Notices/FeatureServer/0"
QUERY_URL = f"{SOURCE_URL}/query"
OWNER_SOURCE_NAME = "Cuyahoga MyPlace SingleSearchParcel"
OWNER_LOOKUP_URL = "https://myplace.cuyahogacounty.gov/MyPlaceService.svc/SingleSearchParcel/{parcel}?city=75"
ENTITY_TERMS = (
    "LLC",
    "LTD",
    "INC",
    "CORP",
    "COMPANY",
    "HOLDINGS",
    "PROPERTIES",
    "TRUST",
    "ESTATE",
    "GROUP",
    " LP",
    " LLP",
)


def fetch_features(limit: int) -> list[dict]:
    params = {
        "where": "PRIMARY_ADDRESS IS NOT NULL",
        "outFields": "*",
        "returnGeometry": "false",
        "orderByFields": "FILE_DATE DESC",
        "resultRecordCount": str(limit),
        "f": "json",
    }
    url = f"{QUERY_URL}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return [feature.get("attributes") or feature for feature in payload.get("features", [])]


def parse_arcgis_date(value) -> str:
    if value in (None, ""):
        return ""
    try:
        return datetime.fromtimestamp(int(value) / 1000, timezone.utc).date().isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def title_city(value: str) -> str:
    return " ".join(part.capitalize() for part in value.split())


def parse_primary_address(value: str) -> tuple[str, str, str, str]:
    parts = [part.strip() for part in str(value or "").split(",") if part.strip()]
    street = parts[0] if parts else ""
    city = title_city(parts[1]) if len(parts) > 1 else "Cleveland"
    state = parts[2].upper() if len(parts) > 2 else "OH"
    zip_code = ""
    if len(parts) > 3:
        match = re.search(r"\b\d{5}(?:-\d{4})?\b", parts[3])
        zip_code = match.group(0) if match else ""
    return street, city or "Cleveland", state or "OH", zip_code


def normalize_record(row: dict, fetched_at: str) -> dict:
    street, city, state, zip_code = parse_primary_address(row.get("PRIMARY_ADDRESS"))
    filed = parse_arcgis_date(row.get("FILE_DATE"))
    complaint_url = row.get("COMPLAINT_ACCELA_CITIZEN_ACCESS_URL") or ""
    violation_url = row.get("VIOLATION_ACCELA_CITIZEN_ACCESS_URL") or ""
    public_url = violation_url or complaint_url or SOURCE_URL
    record_id = row.get("RECORD_ID") or ""
    violation_number = row.get("VIOLATION_NUMBER") or ""
    parcel_id = row.get("PARCEL_NUMBER") or ""

    return {
        "county": "Cuyahoga County",
        "city": city,
        "source_county_key": "cuyahoga",
        "source_city_key": city.lower().replace(" ", "_"),
        "market_area": "Cleveland Metro",
        "property_address": street,
        "property_city": city,
        "property_state": state,
        "property_zip": zip_code,
        "prop_address": street,
        "prop_city": city,
        "prop_state": state,
        "prop_zip": zip_code,
        "mailing_address": "",
        "mailing_city": "",
        "mailing_state": "",
        "mailing_zip": "",
        "owner_name": "",
        "owner": "",
        "owner_type": "",
        "parcel_id": parcel_id,
        "case_number": record_id,
        "complaint_number": record_id,
        "violation_number": violation_number,
        "violation_status": row.get("VIOLATION_APP_STATUS") or "",
        "lead_type": "Cleveland Housing Pain",
        "cat_label": "Cleveland Housing Pain",
        "doc_type": "CODEVIOLATION",
        "doc_num": violation_number or record_id,
        "distress_sources": ["code_violation", "cleveland_housing_pain"],
        "distress_count": 1,
        "hot_stack": False,
        "tired_landlord_plus": False,
        "seller_score": 55,
        "score": 55,
        "subject_to_score": 0,
        "estimated_value": None,
        "estimated_equity": None,
        "estimated_arrears": None,
        "public_records_url": public_url,
        "source_url": SOURCE_URL,
        "source_name": SOURCE_NAME,
        "date_filed": filed,
        "filed": filed,
        "last_updated": fetched_at,
        "flags": ["Code violation", "Cleveland Housing Pain"],
        "neighborhood": row.get("DW_Neighborhood") or "",
        "ward": row.get("DW_Ward2026") or row.get("DW_Ward") or "",
    }


def build_payload(limit: int) -> dict:
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    records = [normalize_record(row, fetched_at) for row in fetch_features(limit)]
    return {
        "source": SOURCE_NAME,
        "source_url": SOURCE_URL,
        "fetched_at": fetched_at,
        "record_count": len(records),
        "code_violation_count": len(records),
        "records": records,
    }


def has_value(value) -> bool:
    return value not in (None, "")


def owner_type(owner: str) -> str:
    if not owner:
        return "unknown"
    owner_key = f" {owner.upper()} "
    if any(term in owner_key for term in ENTITY_TERMS):
        return "entity"
    return "individual"


def parse_owner_lookup(raw: str) -> dict:
    try:
        outer = json.loads(raw)
        data = json.loads(outer) if isinstance(outer, str) else outer
    except json.JSONDecodeError:
        return {}
    if not data or not isinstance(data, list) or not data[0]:
        return {}
    first = data[0][0] if isinstance(data[0], list) else data[0]
    text = first.get("returndata", "") if isinstance(first, dict) else ""
    parts = [part.strip() for part in text.split("|")]
    if len(parts) < 5:
        return {}
    return {
        "parcel_id": parts[0],
        "owner_name": parts[1],
        "property_address": parts[2],
        "property_city": parts[3],
        "property_state": "OH",
        "property_zip": parts[4],
    }


def fetch_owner_lookup(parcel_id: str) -> dict:
    safe_parcel = urllib.parse.quote(str(parcel_id).strip())
    url = OWNER_LOOKUP_URL.format(parcel=safe_parcel)
    with urllib.request.urlopen(url, timeout=30) as response:
        return parse_owner_lookup(response.read().decode("utf-8"))


def fill_if_blank(record: dict, field: str, value) -> None:
    if has_value(value) and not has_value(record.get(field)):
        record[field] = value


def enrich_record(record: dict, timestamp: str) -> str:
    if record.get("source_county_key") != "cuyahoga":
        return "skipped"
    parcel_id = record.get("parcel_id") or ""
    if not parcel_id:
        record["enrichment_source"] = OWNER_SOURCE_NAME
        record["enrichment_timestamp"] = timestamp
        record["enrichment_status"] = "no_parcel"
        return "no_hit"
    try:
        owner_data = fetch_owner_lookup(parcel_id)
    except Exception:
        record["enrichment_source"] = OWNER_SOURCE_NAME
        record["enrichment_timestamp"] = timestamp
        record["enrichment_status"] = "failed"
        return "failed"
    record["enrichment_source"] = OWNER_SOURCE_NAME
    record["enrichment_timestamp"] = timestamp
    if not owner_data.get("owner_name"):
        record["enrichment_status"] = "no_hit"
        fill_if_blank(record, "owner_type", "unknown")
        return "no_hit"
    fill_if_blank(record, "owner_name", owner_data.get("owner_name"))
    fill_if_blank(record, "owner", owner_data.get("owner_name"))
    fill_if_blank(record, "source_owner_name", owner_data.get("owner_name"))
    fill_if_blank(record, "property_address", owner_data.get("property_address"))
    fill_if_blank(record, "property_city", owner_data.get("property_city"))
    fill_if_blank(record, "property_state", owner_data.get("property_state"))
    fill_if_blank(record, "property_zip", owner_data.get("property_zip"))
    fill_if_blank(record, "prop_address", owner_data.get("property_address"))
    fill_if_blank(record, "prop_city", owner_data.get("property_city"))
    fill_if_blank(record, "prop_state", owner_data.get("property_state"))
    fill_if_blank(record, "prop_zip", owner_data.get("property_zip"))
    fill_if_blank(record, "owner_type", owner_type(owner_data.get("owner_name", "")))
    record["enrichment_status"] = "enriched"
    return "enriched"


def enrich_owners(limit: int) -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".records.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)
    counts = {"attempted": 0, "enriched": 0, "no_hit": 0, "failed": 0, "skipped": 0}
    for record in records:
        if counts["attempted"] >= limit:
            break
        if record.get("source_county_key") != "cuyahoga":
            counts["skipped"] += 1
            continue
        counts["attempted"] += 1
        status = enrich_record(record, timestamp)
        counts[status] = counts.get(status, 0) + 1
    payload["owner_enrichment"] = {
        "source": OWNER_SOURCE_NAME,
        "timestamp": timestamp,
        "limit": limit,
        **counts,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    counts["backup_path"] = str(backup_path)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Cuyahoga/Cleveland dashboard records.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--enrich-owners", action="store_true")
    args = parser.parse_args()
    if args.enrich_owners:
        result = enrich_owners(max(1, min(args.limit, 1000)))
        print(json.dumps(result, indent=2))
        return
    payload = build_payload(max(1, min(args.limit, 1000)))
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {payload['record_count']} records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
