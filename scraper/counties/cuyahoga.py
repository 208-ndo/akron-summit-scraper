from __future__ import annotations

import argparse
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = REPO_ROOT / "dashboard" / "cuyahoga" / "records.json"
SOURCE_NAME = "Cleveland Open Data - Complaint Violation Notices"
SOURCE_URL = "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/Complaint_Violation_Notices/FeatureServer/0"
QUERY_URL = f"{SOURCE_URL}/query"


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Cuyahoga/Cleveland dashboard records.")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    payload = build_payload(max(1, min(args.limit, 1000)))
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {payload['record_count']} records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
