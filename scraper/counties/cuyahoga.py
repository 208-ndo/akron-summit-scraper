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
ACTIVE_CONDEMNATIONS_URL = "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/Current_Condemnations/FeatureServer/0"
ACTIVE_CONDEMNATIONS_QUERY_URL = f"{ACTIVE_CONDEMNATIONS_URL}/query"
VIOLATION_STATUS_URL = "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/Violation_Status_History/FeatureServer/0"
VIOLATION_STATUS_QUERY_URL = f"{VIOLATION_STATUS_URL}/query"
PROPERTY_VALUE_URL = "https://myplace.cuyahogacounty.gov/MyPlaceService.svc/ParcelsAndValuesByAnySearchByAndCity/{parcel}?searchBy=Parcel&city=99"
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


def clean_parcel(value) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_address_key(value) -> str:
    text = str(value or "").upper()
    text = re.sub(r",?\s*CLEVELAND\s*,?\s*OH\s*(\d{5})?", "", text)
    for src, dst in {
        "STREET": "ST",
        "AVENUE": "AVE",
        "ROAD": "RD",
        "DRIVE": "DR",
        "PLACE": "PL",
        "BOULEVARD": "BLVD",
    }.items():
        text = re.sub(rf"\b{src}\b", dst, text)
    return re.sub(r"[^A-Z0-9]", "", text)


def record_key(record: dict) -> str:
    parcel = clean_parcel(record.get("parcel_id"))
    if parcel:
        return f"parcel:{parcel}"
    address = normalize_address_key(record.get("property_address") or record.get("prop_address"))
    city = str(record.get("property_city") or record.get("prop_city") or "Cleveland").strip().lower()
    return f"addr:{address}:{city}" if address else ""


def unique_values(values) -> list:
    out = []
    for value in values or []:
        if value in (None, ""):
            continue
        if value not in out:
            out.append(value)
    return out


def add_unique(record: dict, field: str, values: list) -> None:
    record[field] = unique_values((record.get(field) or []) + values)


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


def fetch_active_condemnations() -> list[dict]:
    records = []
    offset = 0
    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "resultRecordCount": "1000",
            "resultOffset": str(offset),
            "f": "json",
        }
        url = f"{ACTIVE_CONDEMNATIONS_QUERY_URL}?{urllib.parse.urlencode(params)}"
        with urllib.request.urlopen(url, timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if "error" in payload:
            raise RuntimeError(payload["error"])
        features = [feature.get("attributes") or feature for feature in payload.get("features", [])]
        records.extend(features)
        if not payload.get("exceededTransferLimit") or not features:
            return records
        offset += len(features)


def fetch_arcgis_records(query_url: str, limit: int, order_by: str | None = None) -> list[dict]:
    records = []
    offset = 0
    while len(records) < limit:
        page_size = min(1000, limit - len(records))
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "resultRecordCount": str(page_size),
            "resultOffset": str(offset),
            "f": "json",
        }
        if order_by:
            params["orderByFields"] = order_by
        url = f"{query_url}?{urllib.parse.urlencode(params)}"
        with urllib.request.urlopen(url, timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if "error" in payload:
            raise RuntimeError(payload["error"])
        features = [feature.get("attributes") or feature for feature in payload.get("features", [])]
        records.extend(features)
        if not payload.get("exceededTransferLimit") or not features:
            break
        offset += len(features)
    return records


def normalize_condemnation_record(row: dict, fetched_at: str) -> dict:
    street, city, state, zip_code = parse_primary_address(row.get("Address"))
    parcel_id = clean_parcel(row.get("DW_Parcel") or row.get("Parcel_Number"))
    date_filed = parse_arcgis_date(row.get("Condemnation_Date"))
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
        "owner_name": "Unknown",
        "owner": "Unknown",
        "owner_type": "unknown",
        "parcel_id": parcel_id,
        "case_number": str(row.get("ObjectId") or ""),
        "complaint_number": "",
        "violation_number": "",
        "violation_status": row.get("Active_Condemnation") or "Yes",
        "lead_type": "Active Condemnation",
        "cat_label": "Active Condemnation",
        "doc_type": "CODEVIOLATION",
        "doc_num": str(row.get("ObjectId") or parcel_id),
        "distress_sources": ["active_condemnation"],
        "distress_count": 1,
        "hot_stack": False,
        "tired_landlord_plus": False,
        "seller_score": 55,
        "score": 55,
        "subject_to_score": 0,
        "public_records_url": ACTIVE_CONDEMNATIONS_URL,
        "source_url": ACTIVE_CONDEMNATIONS_URL,
        "source_name": "Cleveland Open Data Active Condemnations",
        "date_filed": date_filed,
        "filed": date_filed,
        "last_updated": fetched_at,
        "flags": ["Active Condemnation", "Vacant", "Unsafe"],
        "tags": ["Active Condemnation", "Vacant", "Unsafe"],
        "active_condemnation": True,
        "condemnation_status": row.get("Active_Condemnation") or "Yes",
        "condemnation_case_number": str(row.get("ObjectId") or ""),
        "condemnation_date": date_filed,
        "condemnation_source": "Cleveland Open Data Active Condemnations",
        "condemnation_source_url": ACTIVE_CONDEMNATIONS_URL,
        "neighborhood": row.get("DW_Neighborhood") or "",
        "ward": row.get("DW_Ward2026") or row.get("DW_Ward") or "",
    }


def normalize_violation_status_record(row: dict, fetched_at: str) -> dict:
    street, city, state, zip_code = parse_primary_address(row.get("PRIMARY_ADDRESS"))
    parcel_id = clean_parcel(row.get("DW_Parcel") or row.get("PARCEL_NUMBER"))
    filed = parse_arcgis_date(row.get("FILE_DATE"))
    task_date = parse_arcgis_date(row.get("TASK_DATE"))
    issue_date = parse_arcgis_date(row.get("ISSUE_DATE"))
    status = row.get("TASK_STATUS") or ""
    violation_number = row.get("RECORD_ID") or ""
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
        "owner_name": "Unknown",
        "owner": "Unknown",
        "owner_type": "unknown",
        "parcel_id": parcel_id,
        "case_number": violation_number,
        "complaint_number": violation_number,
        "violation_number": violation_number,
        "violation_status": status,
        "lead_type": "Cleveland Housing Pain",
        "cat_label": "Cleveland Housing Pain",
        "doc_type": "CODEVIOLATION",
        "doc_num": violation_number,
        "distress_sources": ["code_violation", "cleveland_housing_pain", "building_violation_status"],
        "distress_count": 1,
        "hot_stack": False,
        "tired_landlord_plus": False,
        "seller_score": 55,
        "score": 55,
        "subject_to_score": 0,
        "public_records_url": row.get("ACCELA_CITIZEN_ACCESS_URL") or VIOLATION_STATUS_URL,
        "source_url": VIOLATION_STATUS_URL,
        "source_name": "Cleveland Open Data Building Violation Status History",
        "date_filed": issue_date or filed,
        "filed": issue_date or filed,
        "last_updated": fetched_at,
        "flags": ["Code violation", "Cleveland Housing Pain"],
        "tags": [],
        "building_violation_status": status,
        "building_violation_task_name": row.get("TASK_NAME") or "",
        "building_violation_task_date": task_date,
        "building_violation_type": row.get("TYPE_OF_VIOLATION") or "",
        "building_violation_occupancy_or_use": row.get("OCCUPANCY_OR_USE") or "",
        "building_violation_source_url": VIOLATION_STATUS_URL,
        "neighborhood": row.get("DW_Neighborhood") or "",
        "ward": row.get("DW_Ward2026") or row.get("DW_Ward") or "",
    }


def has_value(value) -> bool:
    return value not in (None, "")


def has_good_value(value) -> bool:
    return has_value(value) and str(value).strip().upper() != "UNKNOWN"


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


def parse_property_value_lookup(raw: str) -> dict:
    try:
        outer = json.loads(raw)
        data = json.loads(outer) if isinstance(outer, str) else outer
    except json.JSONDecodeError:
        return {}
    if not data or not isinstance(data, list) or not data[0]:
        return {}
    first = data[0][0] if isinstance(data[0], list) else data[0]
    if not isinstance(first, dict):
        return {}
    return {
        "parcel_id": first.get("PARCEL_ID") or "",
        "owner_name": first.get("DEEDED_OWNER") or "",
        "property_address": str(first.get("PHYSICAL_ADDRESS") or "").strip(),
        "property_city": title_city(str(first.get("PARCEL_CITY") or "")),
        "property_state": "OH",
        "property_zip": str(first.get("PARCEL_ZIP") or "").strip(),
        "certified_tax_total": first.get("CERTIFIED_TAX_TOTAL"),
    }


def fetch_property_value_lookup(parcel_id: str) -> dict:
    safe_parcel = urllib.parse.quote(str(parcel_id).strip())
    url = PROPERTY_VALUE_URL.format(parcel=safe_parcel)
    with urllib.request.urlopen(url, timeout=30) as response:
        return parse_property_value_lookup(response.read().decode("utf-8"))


def fill_if_blank(record: dict, field: str, value) -> None:
    if has_value(value) and not has_good_value(record.get(field)):
        record[field] = value


def set_if_value(record: dict, field: str, value) -> None:
    if has_value(value):
        record[field] = value


def is_unknown_owner(record: dict) -> bool:
    owner = str(record.get("owner_name") or record.get("owner") or "").strip().upper()
    return not owner or owner == "UNKNOWN"


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


def enrich_property_value(record: dict, timestamp: str) -> str:
    if record.get("source_county_key") != "cuyahoga":
        return "skipped"
    parcel_id = record.get("parcel_id") or ""
    if not parcel_id:
        record["property_value_enrichment_status"] = "no_parcel"
        return "no_hit"
    try:
        value_data = fetch_property_value_lookup(parcel_id)
    except Exception:
        record["property_value_enrichment_status"] = "failed"
        record["property_value_enrichment_timestamp"] = timestamp
        return "failed"
    record["property_value_enrichment_source"] = "Cuyahoga MyPlace ParcelsAndValuesByAnySearchByAndCity"
    record["property_value_enrichment_timestamp"] = timestamp
    if not value_data:
        record["property_value_enrichment_status"] = "no_hit"
        return "no_hit"
    fill_if_blank(record, "owner_name", value_data.get("owner_name"))
    fill_if_blank(record, "owner", value_data.get("owner_name"))
    fill_if_blank(record, "source_owner_name", value_data.get("owner_name"))
    fill_if_blank(record, "property_address", value_data.get("property_address"))
    fill_if_blank(record, "property_city", value_data.get("property_city"))
    fill_if_blank(record, "property_state", value_data.get("property_state"))
    fill_if_blank(record, "property_zip", value_data.get("property_zip"))
    fill_if_blank(record, "prop_address", value_data.get("property_address"))
    fill_if_blank(record, "prop_city", value_data.get("property_city"))
    fill_if_blank(record, "prop_state", value_data.get("property_state"))
    fill_if_blank(record, "prop_zip", value_data.get("property_zip"))
    fill_if_blank(record, "owner_type", owner_type(value_data.get("owner_name", "")))
    set_if_value(record, "certified_tax_total", value_data.get("certified_tax_total"))
    set_if_value(record, "tax_total", value_data.get("certified_tax_total"))
    try:
        tax_total = float(value_data.get("certified_tax_total") or 0)
    except (TypeError, ValueError):
        tax_total = 0
    if tax_total > 0:
        record["tax_pressure"] = True
        add_unique(record, "distress_sources", ["tax_pressure"])
        add_unique(record, "flags", ["Tax Pressure"])
        add_unique(record, "tags", ["Tax Pressure"])
    record["property_value_enrichment_status"] = "enriched"
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


def merge_record(existing: dict, incoming: dict) -> dict:
    for field, value in incoming.items():
        if field in ("distress_sources", "flags", "tags"):
            add_unique(existing, field, value if isinstance(value, list) else [value])
        elif field.startswith("condemnation_") or field == "active_condemnation":
            if has_value(value):
                existing[field] = value
        elif field.startswith("building_violation_") or field in ("violation_status",):
            if has_value(value):
                existing[field] = value
        elif field == "last_updated":
            if has_value(value):
                existing[field] = value
        elif has_value(value) and not has_value(existing.get(field)):
            existing[field] = value
    return existing


def apply_stack_tags(record: dict) -> None:
    sources = unique_values(record.get("distress_sources") or [])
    record["distress_sources"] = sources
    counted_sources = [source for source in sources if source != "cleveland_housing_pain"]
    record["distress_count"] = max(int(record.get("distress_count") or 0), len(counted_sources))
    if record.get("active_condemnation"):
        record["distress_count"] = max(int(record.get("distress_count") or 0), 2)
        add_unique(record, "flags", ["Active Condemnation", "Vacant", "Unsafe"])
        add_unique(record, "tags", ["Active Condemnation", "Vacant", "Unsafe"])
    if str(record.get("owner_type") or "").lower() == "entity":
        record["entity_owner"] = True
        add_unique(record, "flags", ["Tired Landlord"])
        add_unique(record, "tags", ["Tired Landlord"])
    if record.get("tax_pressure"):
        add_unique(record, "flags", ["Tax Pressure"])
        add_unique(record, "tags", ["Tax Pressure"])
    if int(record.get("distress_count") or 0) >= 2 or record.get("active_condemnation"):
        add_unique(record, "flags", ["Cuyahoga Hot Stack"])
        add_unique(record, "tags", ["Cuyahoga Hot Stack"])


def expand_stacks(limit: int, owner_limit: int, violation_limit: int = 5000, property_limit: int = 1000) -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2d.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)

    merged = {}
    for record in payload.get("records") or []:
        key = record_key(record)
        if key:
            merged[key] = record

    housing_records = [normalize_record(row, timestamp) for row in fetch_features(limit)]
    for record in housing_records:
        key = record_key(record)
        if key:
            merged[key] = merge_record(merged[key], record) if key in merged else record

    violation_rows = fetch_arcgis_records(VIOLATION_STATUS_QUERY_URL, violation_limit, "TASK_DATE DESC")
    matched_violations = 0
    standalone_violations = 0
    for record in [normalize_violation_status_record(row, timestamp) for row in violation_rows]:
        key = record_key(record)
        if not key:
            continue
        if key in merged:
            matched_violations += 1
            merged[key] = merge_record(merged[key], record)
        else:
            standalone_violations += 1
            merged[key] = record

    condemnation_rows = fetch_active_condemnations()
    matched_condemnations = 0
    standalone_condemnations = 0
    for record in [normalize_condemnation_record(row, timestamp) for row in condemnation_rows]:
        key = record_key(record)
        if not key:
            continue
        if key in merged:
            matched_condemnations += 1
            merged[key] = merge_record(merged[key], record)
        else:
            standalone_condemnations += 1
            merged[key] = record

    owner_counts = {"attempted": 0, "enriched": 0, "no_hit": 0, "failed": 0, "skipped": 0}
    for record in merged.values():
        if owner_counts["attempted"] >= owner_limit:
            break
        if record.get("source_county_key") != "cuyahoga" or not is_unknown_owner(record):
            continue
        owner_counts["attempted"] += 1
        status = enrich_record(record, timestamp)
        owner_counts[status] = owner_counts.get(status, 0) + 1

    property_counts = {"attempted": 0, "enriched": 0, "no_hit": 0, "failed": 0, "skipped": 0}
    seen_parcels = set()
    for record in merged.values():
        if property_counts["attempted"] >= property_limit:
            break
        parcel = clean_parcel(record.get("parcel_id"))
        if record.get("source_county_key") != "cuyahoga" or not parcel or parcel in seen_parcels:
            continue
        seen_parcels.add(parcel)
        property_counts["attempted"] += 1
        status = enrich_property_value(record, timestamp)
        property_counts[status] = property_counts.get(status, 0) + 1

    records = list(merged.values())
    for record in records:
        apply_stack_tags(record)

    payload.update(
        {
            "fetched_at": timestamp,
            "record_count": len(records),
            "code_violation_count": sum(1 for r in records if "code_violation" in (r.get("distress_sources") or [])),
            "records": records,
            "phase_2d_stack_expansion": {
                "timestamp": timestamp,
                "housing_records_pulled": len(housing_records),
                "violation_status_rows_pulled": len(violation_rows),
                "matched_violation_status_records": matched_violations,
                "standalone_violation_status_records": standalone_violations,
                "active_condemnations_pulled": len(condemnation_rows),
                "matched_active_condemnations": matched_condemnations,
                "standalone_active_condemnations": standalone_condemnations,
                "owner_enrichment": owner_counts,
                "property_value_enrichment": property_counts,
                "backup_path": str(backup_path),
            },
        }
    )
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2d_stack_expansion"] | {"total_records": len(records)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Cuyahoga/Cleveland dashboard records.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--enrich-owners", action="store_true")
    parser.add_argument("--expand-stacks", action="store_true")
    parser.add_argument("--owner-limit", type=int, default=250)
    parser.add_argument("--violation-limit", type=int, default=5000)
    parser.add_argument("--property-limit", type=int, default=1000)
    args = parser.parse_args()
    if args.expand_stacks:
        result = expand_stacks(
            max(1, min(args.limit, 1000)),
            max(1, min(args.owner_limit, 1000)),
            max(1, min(args.violation_limit, 10000)),
            max(1, min(args.property_limit, 2500)),
        )
        print(json.dumps(result, indent=2))
        return
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
