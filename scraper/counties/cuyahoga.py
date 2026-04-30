from __future__ import annotations

import argparse
import html
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
DEMOLITION_PERMITS_URL = "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/Demolition_Permits/FeatureServer/0"
DEMOLITION_PERMITS_QUERY_URL = f"{DEMOLITION_PERMITS_URL}/query"
PUBLIC_HEALTH_COMPLAINTS_URL = "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/CDPH_Complaints/FeatureServer/0"
PUBLIC_HEALTH_COMPLAINTS_QUERY_URL = f"{PUBLIC_HEALTH_COMPLAINTS_URL}/query"
PROPERTY_VALUE_URL = "https://myplace.cuyahogacounty.gov/MyPlaceService.svc/ParcelsAndValuesByAnySearchByAndCity/{parcel}?searchBy=Parcel&city=99"
LEGACY_TAXES_URL = "https://myplace.cuyahogacounty.gov/MainPage/LegacyTaxes"
SHERIFF_SEARCH_URL = "https://cpdocket.cp.cuyahogacounty.gov/SheriffSearch/"
PROPERTY_DATA_URL = "https://myplace.cuyahogacounty.gov/MainPage/PropertyData"
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


def normalize_owner_key(value) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


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


def normalize_demolition_record(row: dict, fetched_at: str) -> dict:
    street, city, state, zip_code = parse_primary_address(row.get("PRIMARY_ADDRESS"))
    parcel_id = clean_parcel(row.get("DW_Parcel") or row.get("PRIMARY_PARCEL"))
    permit_number = str(row.get("PERMIT_ID") or "")
    filed = parse_arcgis_date(row.get("FILE_DATE"))
    issued = parse_arcgis_date(row.get("ISSUED_DATE"))
    closed = parse_arcgis_date(row.get("CLOSED_DATE"))
    public_url = row.get("ACCELA_CITIZEN_ACCESS_URL") or DEMOLITION_PERMITS_URL
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
        "owner_name": row.get("owner_name") or "Unknown",
        "owner": row.get("owner_name") or "Unknown",
        "owner_type": owner_type(row.get("owner_name") or ""),
        "parcel_id": parcel_id,
        "case_number": permit_number,
        "lead_type": "Demolition Permit",
        "cat_label": "Demolition Permit",
        "doc_type": "DEMOLITION",
        "doc_num": permit_number,
        "distress_sources": ["demolition"],
        "distress_count": 1,
        "hot_stack": False,
        "tired_landlord_plus": False,
        "seller_score": 55,
        "score": 55,
        "subject_to_score": 0,
        "public_records_url": public_url,
        "source_url": DEMOLITION_PERMITS_URL,
        "source_name": "Cleveland Open Data Demolition Permits",
        "date_filed": issued or filed,
        "filed": issued or filed,
        "last_updated": fetched_at,
        "flags": ["Demolition", "Blight Pressure", "Unsafe"],
        "tags": ["Demolition", "Blight Pressure", "Unsafe"],
        "demolition_permit": True,
        "demolition_status": "Closed" if closed else "Issued",
        "demolition_permit_number": permit_number,
        "demolition_date": closed or issued or filed,
        "demolition_source": "official Cleveland Open Data",
        "demolition_source_url": DEMOLITION_PERMITS_URL,
        "demolition_contractor": row.get("Contrator_Business_Name") or "",
        "demolition_job_value": row.get("Job_Value"),
        "neighborhood": row.get("DW_Neighborhood") or "",
        "ward": row.get("DW_Ward2026") or row.get("DW_Ward") or "",
    }


def nuisance_tags(complaint_type: str) -> list[str]:
    text = str(complaint_type or "").strip().lower()
    if not text:
        return []
    if any(term in text for term in ("animal nuisance", "farm animal", "food", "odor", "other")):
        return []
    tags = ["Public Health Complaint"]
    if any(term in text for term in ("unsanitary", "health condition", "nuisance")):
        tags.extend(["Environmental Nuisance", "Unsafe / Nuisance Pressure"])
    if any(term in text for term in ("rodent", "vermin", "insect")):
        tags.extend(["Rodent / Vermin", "Unsafe / Nuisance Pressure"])
    if any(term in text for term in ("garbage", "refuse", "waste", "grass", "weed")):
        tags.extend(["Garbage / High Grass", "Environmental Nuisance"])
    if "standing water" in text:
        tags.extend(["Standing Water", "Environmental Nuisance"])
    if "sewage" in text:
        tags.extend(["Sewage", "Unsafe / Nuisance Pressure"])
    if "mold" in text:
        tags.extend(["Unsafe / Nuisance Pressure"])
    return unique_values(tags) if len(tags) > 1 else []


def complaint_address(row: dict) -> str:
    primary = str(row.get("problem_address") or "").strip()
    if re.search(r"[A-Za-z]", primary):
        return primary
    street_name = str(row.get("problem_street_name") or "").strip()
    street_type = str(row.get("problem_street_type") or "").strip()
    if street_type and re.search(rf"\b{re.escape(street_type)}\.?$", street_name, flags=re.IGNORECASE):
        street_type = ""
    parts = [
        primary,
        row.get("problem_street_direction"),
        street_name,
        street_type,
    ]
    return " ".join(str(part or "").strip() for part in parts if str(part or "").strip())


def normalize_public_health_complaint_record(row: dict, fetched_at: str) -> dict | None:
    tags = nuisance_tags(row.get("complaint_type"))
    if not tags:
        return None
    street = complaint_address(row)
    city = title_city(str(row.get("problem_city") or "Cleveland"))
    zip_code = str(row.get("problem_zip_code") or "").strip()
    parcel_id = clean_parcel(row.get("dw_parcel") or row.get("permanent_parcel_number"))
    complaint_number = str(row.get("complaint_number") or row.get("id") or row.get("ObjectId") or "")
    submit_date = parse_arcgis_date(row.get("submit_datetime") or row.get("submit_date"))
    if not parcel_id and not street:
        return None
    return {
        "county": "Cuyahoga County",
        "city": city,
        "source_county_key": "cuyahoga",
        "source_city_key": city.lower().replace(" ", "_"),
        "market_area": "Cleveland Metro",
        "property_address": street,
        "property_city": city,
        "property_state": "OH",
        "property_zip": zip_code,
        "prop_address": street,
        "prop_city": city,
        "prop_state": "OH",
        "prop_zip": zip_code,
        "owner_name": "Unknown",
        "owner": "Unknown",
        "owner_type": "unknown",
        "parcel_id": parcel_id,
        "case_number": complaint_number,
        "complaint_number": complaint_number,
        "lead_type": "Public Health Complaint",
        "cat_label": "Public Health Complaint",
        "doc_type": "NUISANCE",
        "doc_num": complaint_number,
        "distress_sources": ["nuisance_complaint"],
        "distress_count": 1,
        "hot_stack": False,
        "tired_landlord_plus": False,
        "seller_score": 55,
        "score": 55,
        "subject_to_score": 0,
        "public_records_url": PUBLIC_HEALTH_COMPLAINTS_URL,
        "source_url": PUBLIC_HEALTH_COMPLAINTS_URL,
        "source_name": "Cleveland CDPH Public Health Complaints",
        "date_filed": submit_date,
        "filed": submit_date,
        "last_updated": fetched_at,
        "flags": tags,
        "tags": tags,
        "public_health_complaint": True,
        "nuisance_complaint": True,
        "nuisance_status": row.get("complaint_status") or "",
        "nuisance_type": row.get("complaint_type") or "",
        "nuisance_outcome": row.get("complaint_outcome") or "",
        "nuisance_date": submit_date,
        "nuisance_case_number": complaint_number,
        "nuisance_source": "Cleveland CDPH Public Health Complaints",
        "nuisance_source_url": PUBLIC_HEALTH_COMPLAINTS_URL,
        "neighborhood": row.get("dw_neighborhood") or "",
        "ward": row.get("dw_ward_2026") or row.get("dw_ward") or row.get("ward_number") or "",
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


def parse_money(value) -> float | None:
    text = str(value or "").replace("$", "").replace(",", "").strip()
    if text in ("", ".00"):
        return 0.0 if text == ".00" else None
    try:
        return float(text)
    except ValueError:
        return None


def parse_short_date(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return datetime.strptime(text, "%m/%d/%Y").date().isoformat()
    except ValueError:
        try:
            return datetime.strptime(text, "%m/%d/%Y %I:%M:%S %p").date().isoformat()
        except ValueError:
            return text


def is_recent_transfer(value: str) -> bool:
    text = parse_short_date(value)
    match = re.match(r"^(\d{4})-", text or "")
    return bool(match and int(match.group(1)) >= 2020)


def html_to_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw or "")
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def extract_between(text: str, start: str, end: str) -> str:
    pattern = rf"{re.escape(start)}\s+(.*?)\s+{re.escape(end)}"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def split_tax_mailing_address(block: str, owner_name: str = "") -> dict:
    text = re.sub(r"\s+", " ", block or "").strip()
    owner = str(owner_name or "").strip()
    if owner and text.upper().startswith(owner.upper()):
        text = text[len(owner) :].strip()
    match = re.search(r"^(.+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", text, flags=re.IGNORECASE)
    if not match:
        return {}
    before_city_state, state, zip_code = match.groups()
    tokens = before_city_state.split()
    suffixes = {"ST", "STREET", "AVE", "AVENUE", "RD", "ROAD", "DR", "DRIVE", "LN", "LANE", "CT", "COURT", "BLVD", "PKWY", "WAY", "PL", "PLACE", "CIR", "CIRCLE", "TER", "TRL"}
    split_at = -1
    for index, token in enumerate(tokens):
        if token.upper().rstrip(".") in suffixes:
            split_at = index + 1
    if split_at <= 0 or split_at >= len(tokens):
        return {"mailing_address": before_city_state.strip(), "mailing_state": state.upper(), "mailing_zip": zip_code}
    return {
        "mailing_address": " ".join(tokens[:split_at]).strip(),
        "mailing_city": title_city(" ".join(tokens[split_at:])),
        "mailing_state": state.upper(),
        "mailing_zip": zip_code,
    }


def yn_flag(text: str, label: str) -> str:
    match = re.search(rf"{re.escape(label)}\s+([YN])\b", text, flags=re.IGNORECASE)
    return match.group(1).upper() if match else ""


def parse_legacy_tax_bill(raw: str) -> dict:
    text = html_to_text(raw)
    if "Tax Balance Summary" not in text:
        return {}
    assessed = re.search(r"Taxable Assessed Values.*?Total Value\s+\$?([\d,.]+)", text, flags=re.IGNORECASE)
    market = re.search(r"Taxable Market Values.*?Total Value\s+\$?([\d,.]+)", text, flags=re.IGNORECASE)
    balance = re.search(
        r"Tax Balance Summary\s+Charges\s+\$?([\d,.]+)\s+Payments\s+\$?([\d,.]+)\s+Balance Due\s+\$?([\d,.]+)",
        text,
        flags=re.IGNORECASE,
    )
    tax_year = re.search(r"Tax Year\s+(\d{4})\s+Pay\s+(\d{4})", text, flags=re.IGNORECASE)
    owner_match = re.search(r"Deeded Owner\s+(.+?)\s+Tax Mailing Address", text, flags=re.IGNORECASE)
    owner_name = owner_match.group(1).strip() if owner_match else ""
    mailing = split_tax_mailing_address(extract_between(text, "Tax Mailing Address", "Description"), owner_name)
    data = {
        "owner_name": owner_name,
        "assessed_value": parse_money(assessed.group(1)) if assessed else None,
        "market_value": parse_money(market.group(1)) if market else None,
        "tax_charges": parse_money(balance.group(1)) if balance else None,
        "tax_payments": parse_money(balance.group(2)) if balance else None,
        "tax_balance_due": parse_money(balance.group(3)) if balance else None,
        "tax_year": tax_year.group(1) if tax_year else "",
        "tax_pay_year": tax_year.group(2) if tax_year else "",
        "tax_foreclosure_flag": yn_flag(text, "Foreclosure"),
        "tax_certificate_pending_flag": yn_flag(text, "Cert. Pending"),
        "tax_certificate_sold_flag": yn_flag(text, "Cert. Sold"),
        "tax_payment_plan_flag": yn_flag(text, "Payment Plan"),
    }
    data.update(mailing)
    return {key: value for key, value in data.items() if has_value(value)}


def fetch_legacy_tax_bill(parcel_id: str) -> dict:
    parcel = clean_parcel(parcel_id)
    form = urllib.parse.urlencode(
        {
            "hdnTaxesParcelId": parcel,
            "hdnTaxesListId": "0",
            "hdnTaxesButtonClicked": "Tax Bill",
            "hdnTaxesSearchChoice": "Parcel",
            "hdnTaxesSearchText": parcel,
            "hdnTaxesSearchCity": "75",
            "hdnTaxYear": "",
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        LEGACY_TAXES_URL,
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        return parse_legacy_tax_bill(response.read().decode("utf-8", errors="replace"))


def table_texts(block: str, table_class: str) -> list[str]:
    table_match = re.search(rf'<table[^>]+class="{re.escape(table_class)}[^"]*"[^>]*>(.*?)</table>', block or "", flags=re.IGNORECASE | re.DOTALL)
    if not table_match:
        return []
    return [
        re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", cell))).strip()
        for cell in re.findall(r"<td[^>]*>(.*?)</td>", table_match.group(1), flags=re.IGNORECASE | re.DOTALL)
    ]


def parse_property_transfers(raw: str) -> list[dict]:
    sections = re.split(r"<h3>\s*Transfer Date:\s*", raw or "", flags=re.IGNORECASE)
    transfers = []
    for section in sections[1:]:
        heading_match = re.match(r"([^<]+)</h3>\s*<div>(.*?)(?=<h3>\s*Transfer Date:|$)", section, flags=re.IGNORECASE | re.DOTALL)
        if not heading_match:
            continue
        heading_date, block = heading_match.groups()
        data_values = [
            re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", value))).strip()
            for value in re.findall(r'<span class="dataValue">(.*?)</span>', block, flags=re.IGNORECASE | re.DOTALL)
        ]
        transfer_cells = table_texts(block, "transferTable")
        party_cells = table_texts(block, "grantees")
        if len(transfer_cells) < 4:
            continue
        transfers.append(
            {
                "transfer_date": parse_short_date(data_values[0] if data_values else heading_date),
                "transfer_af_number": data_values[1] if len(data_values) > 1 else "",
                "parcel_id": clean_parcel(transfer_cells[0]),
                "deed_type": transfer_cells[1],
                "last_sale_amount": parse_money(transfer_cells[3]),
                "conveyance_fee": parse_money(transfer_cells[4]) if len(transfer_cells) > 4 else None,
                "multiple_sale_parcels": transfer_cells[6] if len(transfer_cells) > 6 else "",
                "grantee_name": party_cells[0] if party_cells else "",
                "grantor_name": party_cells[1] if len(party_cells) > 1 else "",
            }
        )
    return transfers


def fetch_property_transfers(parcel_id: str) -> list[dict]:
    parcel = clean_parcel(parcel_id)
    form = urllib.parse.urlencode(
        {
            "hdnParcelId": parcel,
            "hdnListId": "0",
            "hdnButtonClicked": "Transfers",
            "hdnSearchChoice": "Parcel",
            "hdnSearchText": parcel,
            "hdnSearchCity": "75",
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        PROPERTY_DATA_URL,
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "Mozilla/5.0"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return parse_property_transfers(response.read().decode("utf-8", errors="replace"))


def fetch_sheriff_search_home(opener) -> str:
    request = urllib.request.Request(SHERIFF_SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
    with opener.open(request, timeout=45) as response:
        return response.read().decode("utf-8", errors="replace")


def hidden_form_fields(raw: str) -> dict:
    fields = {}
    for match in re.finditer(r"<input[^>]+>", raw or "", flags=re.IGNORECASE):
        tag = match.group(0)
        name_match = re.search(r'name="([^"]+)"', tag, flags=re.IGNORECASE)
        if not name_match:
            continue
        value_match = re.search(r'value="([^"]*)"', tag, flags=re.IGNORECASE)
        fields[html.unescape(name_match.group(1))] = html.unescape(value_match.group(1) if value_match else "")
    return fields


def parse_sheriff_sale_dates(raw: str, limit: int) -> list[tuple[str, str]]:
    options = []
    for value, label in re.findall(r'<option value="([^"]*)">([^<]*)</option>', raw or "", flags=re.IGNORECASE):
        value = html.unescape(value).strip()
        label = html.unescape(label).strip()
        if value and label:
            options.append((value, label))
    return options[:limit]


def post_sheriff_search(opener, home_html: str, sale_date_value: str = "") -> str:
    fields = hidden_form_fields(home_html)
    fields.update(
        {
            "ctl00$SheetContentPlaceHolder$c_search1$ddlSaleDate": sale_date_value,
            "ctl00$SheetContentPlaceHolder$c_search1$SearchStringDateFrom": "",
            "ctl00$SheetContentPlaceHolder$c_search1$SearchStringDateTo": "",
            "ctl00$SheetContentPlaceHolder$c_search1$SrchSearchString": "",
            "ctl00$SheetContentPlaceHolder$c_search1$rblSrchOptions": "CaseNum",
            "ctl00$SheetContentPlaceHolder$c_search1$btnSearch": "Start Search",
        }
    )
    request = urllib.request.Request(
        SHERIFF_SEARCH_URL,
        data=urllib.parse.urlencode(fields).encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "Mozilla/5.0"},
    )
    with opener.open(request, timeout=75) as response:
        return response.read().decode("utf-8", errors="replace")


def span_text(block: str, name: str, index: str) -> str:
    pattern = rf'id="[^"]*{re.escape(name)}_{re.escape(index)}"[^>]*>(.*?)</(?:span|a)>'
    match = re.search(pattern, block or "", flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    text = re.sub(r"<[^>]+>", " ", match.group(1))
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def parse_sheriff_results(raw: str, sale_label: str) -> list[dict]:
    case_matches = list(re.finditer(r'gvSaleSummary_lnkCaseNum_(\d+)"[^>]*>([^<]+)<', raw or "", flags=re.IGNORECASE))
    records = []
    for pos, match in enumerate(case_matches):
        index = match.group(1)
        block_end = case_matches[pos + 1].start() if pos + 1 < len(case_matches) else len(raw)
        block = raw[match.start() : block_end]
        parcel_match = re.search(r"RedirectToSheriff\((\d+)\).*?(\d{3}-\d{2}-\d{3})</a>", block, flags=re.DOTALL)
        parcel_id = parcel_match.group(2) if parcel_match else ""
        sale_date = span_text(block, "lblSaleDate2", index)
        status = span_text(block, "lblStatus", index)
        address = re.sub(r"\s+", " ", span_text(block, "lblAddress", index)).strip()
        if not address:
            continue
        record = {
            "county": "Cuyahoga County",
            "source_county_key": "cuyahoga",
            "market_area": "Cleveland Metro",
            "property_address": address,
            "property_state": "OH",
            "parcel_id": clean_parcel(parcel_id),
            "lead_type": "Foreclosure / Sheriff Sale",
            "foreclosure": True,
            "sheriff_sale": True,
            "foreclosure_case_number": html.unescape(match.group(2)).strip(),
            "sheriff_sale_date": parse_short_date(sale_date),
            "foreclosure_status": status,
            "foreclosure_source_url": SHERIFF_SEARCH_URL,
            "foreclosure_source_name": "Cuyahoga Clerk of Courts Sheriff Sale Results",
            "sheriff_sale_type": sale_label,
            "foreclosure_plaintiff": span_text(block, "lblPlaintiffName", index),
            "foreclosure_defendant": span_text(block, "lblDefendant", index),
            "foreclosure_attorney": span_text(block, "lblPlaintiffAtty", index),
            "foreclosure_property_type": span_text(block, "lblPropertyType", index),
            "foreclosure_description": span_text(block, "lblDescription", index),
            "foreclosure_appraised_value": parse_money(span_text(block, "lblAppraised", index)),
            "sheriff_minimum_bid": parse_money(span_text(block, "lblOpeningBid", index)),
            "distress_sources": ["foreclosure", "sheriff_sale"],
            "flags": ["Foreclosure", "Sheriff Sale", "Auction Pressure"],
            "tags": ["Foreclosure", "Sheriff Sale", "Auction Pressure"],
        }
        records.append({key: value for key, value in record.items() if has_value(value)})
    return records


def fetch_sheriff_sale_records(date_limit: int, record_limit: int) -> tuple[list[dict], int]:
    import http.cookiejar

    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    home = fetch_sheriff_search_home(opener)
    sale_dates = parse_sheriff_sale_dates(home, date_limit)
    records = []
    for value, label in sale_dates:
        if len(records) >= record_limit:
            break
        page = post_sheriff_search(opener, home, value)
        records.extend(parse_sheriff_results(page, label))
    deduped = {}
    for record in records:
        key = record.get("foreclosure_case_number") or record_key(record)
        if key and key not in deduped:
            deduped[key] = record
        if len(deduped) >= record_limit:
            break
    return list(deduped.values()), len(sale_dates)


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


def apply_investor_owner_flags(record: dict) -> None:
    owner = str(record.get("owner_name") or record.get("owner") or "").strip()
    if str(record.get("owner_type") or "").lower() == "entity" or owner_type(owner) == "entity":
        record["owner_type"] = "entity"
        record["entity_owner"] = True
        record["investor_owner"] = True
        add_unique(record, "flags", ["Entity Owner", "Investor Owner"])
        add_unique(record, "tags", ["Entity Owner", "Investor Owner"])


def enrich_transfer_history(record: dict, timestamp: str) -> str:
    if record.get("source_county_key") != "cuyahoga":
        return "skipped"
    apply_investor_owner_flags(record)
    parcel_id = clean_parcel(record.get("parcel_id"))
    if not parcel_id:
        record["transfer_enrichment_status"] = "no_parcel"
        return "no_hit"
    try:
        transfers = fetch_property_transfers(parcel_id)
    except Exception:
        record["transfer_enrichment_status"] = "failed"
        record["transfer_enrichment_timestamp"] = timestamp
        return "failed"
    record["transfer_source"] = "Cuyahoga MyPlace PropertyData Transfers"
    record["transfer_source_url"] = PROPERTY_DATA_URL
    record["transfer_enrichment_timestamp"] = timestamp
    if not transfers:
        record["transfer_enrichment_status"] = "no_hit"
        return "no_hit"

    transfer = transfers[0]
    set_if_value(record, "last_sale_date", transfer.get("transfer_date"))
    set_if_value(record, "last_transfer_date", transfer.get("transfer_date"))
    set_if_value(record, "last_sale_amount", transfer.get("last_sale_amount"))
    set_if_value(record, "buyer_name", transfer.get("grantee_name"))
    set_if_value(record, "grantee_name", transfer.get("grantee_name"))
    set_if_value(record, "seller_name", transfer.get("grantor_name"))
    set_if_value(record, "grantor_name", transfer.get("grantor_name"))
    set_if_value(record, "deed_type", transfer.get("deed_type"))
    set_if_value(record, "transfer_af_number", transfer.get("transfer_af_number"))
    set_if_value(record, "conveyance_fee", transfer.get("conveyance_fee"))
    record["transfer_count"] = len(transfers)
    record["confirmed_cash_buyer"] = False

    sale_amount = transfer.get("last_sale_amount")
    if record.get("investor_owner") and is_recent_transfer(transfer.get("transfer_date")) and isinstance(sale_amount, (int, float)) and sale_amount > 0:
        record["cash_buyer_candidate"] = True
        record["buyer_type"] = record.get("buyer_type") or "Cash Buyer Candidate"
        add_unique(record, "distress_sources", ["cash_buyer_candidate"])
        add_unique(record, "flags", ["Cash Buyer Candidate"])
        add_unique(record, "tags", ["Cash Buyer Candidate"])

    apply_absentee_owner_flags(record)
    apply_prime_deal_flag(record)
    apply_stack_tags(record)
    record["transfer_enrichment_status"] = "enriched"
    return "enriched"


def record_signal_text(record: dict) -> str:
    values = []
    for field in ("distress_sources", "flags", "tags"):
        values.extend(record.get(field) or [])
    values.extend(
        str(record.get(field) or "")
        for field in (
            "lead_type",
            "doc_type",
            "type",
            "violation_status",
            "condemnation_status",
            "foreclosure_status",
        )
    )
    return " ".join(values).lower()


def apply_prime_deal_flag(record: dict) -> list[str]:
    if record.get("source_county_key") != "cuyahoga":
        return []
    text = record_signal_text(record)
    property_pain = any(
        marker in text
        for marker in ("code_violation", "code violation", "cleveland_housing_pain", "housing pain", "active_condemnation", "active condemnation", "vacant", "unsafe", "demolition", "blight pressure", "nuisance_complaint", "public health complaint", "environmental nuisance")
    ) or bool(record.get("active_condemnation"))
    if not property_pain:
        return []
    groups = []
    if record.get("tax_delinquent") or record.get("tax_pressure") or any(marker in text for marker in ("tax_delinquent", "tax delinquent", "tax_pressure", "tax pressure")):
        groups.append("tax pressure")
    if record.get("absentee_owner") or record.get("is_absentee") or record.get("out_of_state_owner") or record.get("is_out_of_state") or record.get("entity_owner") or record.get("tired_landlord"):
        groups.append("ownership pain")
    if record.get("foreclosure") or record.get("sheriff_sale") or any(marker in text for marker in ("foreclosure", "sheriff_sale", "sheriff sale")):
        groups.append("legal pressure")
    if record.get("active_condemnation") or record.get("demolition_permit") or any(marker in text for marker in ("active_condemnation", "active condemnation", "vacant", "unsafe", "demolition", "blight pressure", "nuisance_complaint", "environmental nuisance")):
        groups.append("severe property condition")
    if record.get("cash_buyer_candidate") or record.get("investor_owner"):
        groups.append("investor/cash signal")
    groups = unique_values(groups)
    if len(groups) >= 2:
        record["prime_deal"] = True
        record["prime_deal_reason"] = "; ".join(groups)
        add_unique(record, "flags", ["Prime Deal"])
        add_unique(record, "tags", ["Prime Deal"])
    return groups


def calculate_cuyahoga_stack_score(record: dict) -> int:
    if record.get("source_county_key") != "cuyahoga":
        return int(record.get("score") or record.get("seller_score") or 0)
    text = record_signal_text(record)
    score = 45
    distress_count = int(record.get("distress_count") or 0)
    score += min(distress_count, 6) * 5
    if record.get("prime_deal"):
        score += 10
    if record.get("tax_delinquent") or record.get("tax_pressure") or any(marker in text for marker in ("tax_delinquent", "tax delinquent", "tax_pressure", "tax pressure")):
        score += 10
    if record.get("foreclosure") or record.get("sheriff_sale") or any(marker in text for marker in ("foreclosure", "sheriff_sale", "sheriff sale")):
        score += 12
    if record.get("active_condemnation") or record.get("demolition_permit") or any(marker in text for marker in ("active_condemnation", "active condemnation", "unsafe", "demolition", "blight pressure")):
        score += 12
    elif record.get("nuisance_complaint") or "nuisance_complaint" in text or "environmental nuisance" in text:
        score += 6
    elif "vacant" in text:
        score += 8
    if record.get("absentee_owner") or record.get("is_absentee") or record.get("out_of_state_owner") or record.get("is_out_of_state"):
        score += 7
    if record.get("entity_owner") or record.get("investor_owner") or record.get("tired_landlord"):
        score += 6
    if record.get("cash_buyer_candidate"):
        score += 4
    return min(100, max(0, score))


def apply_cuyahoga_stack_score(record: dict) -> int:
    score = calculate_cuyahoga_stack_score(record)
    record["stack_score"] = score
    record["lead_score"] = score
    record["seller_score"] = score
    record["score"] = score
    return score


def enrich_legacy_tax_bill(record: dict, timestamp: str) -> str:
    if record.get("source_county_key") != "cuyahoga":
        return "skipped"
    parcel_id = clean_parcel(record.get("parcel_id"))
    if not parcel_id:
        record["tax_delinquent_enrichment_status"] = "no_parcel"
        return "no_hit"
    try:
        tax_data = fetch_legacy_tax_bill(parcel_id)
    except Exception:
        record["tax_delinquent_enrichment_status"] = "failed"
        record["tax_delinquent_enrichment_timestamp"] = timestamp
        return "failed"
    record["tax_delinquent_source"] = "Cuyahoga MyPlace LegacyTaxes"
    record["tax_delinquent_source_url"] = LEGACY_TAXES_URL
    record["tax_delinquent_enrichment_timestamp"] = timestamp
    if not tax_data:
        record["tax_delinquent_enrichment_status"] = "no_hit"
        return "no_hit"

    fill_if_blank(record, "owner_name", tax_data.get("owner_name"))
    fill_if_blank(record, "owner", tax_data.get("owner_name"))
    fill_if_blank(record, "source_owner_name", tax_data.get("owner_name"))
    fill_if_blank(record, "mailing_address", tax_data.get("mailing_address"))
    fill_if_blank(record, "mailing_city", tax_data.get("mailing_city"))
    fill_if_blank(record, "mailing_state", tax_data.get("mailing_state"))
    fill_if_blank(record, "mailing_zip", tax_data.get("mailing_zip"))
    fill_if_blank(record, "owner_type", owner_type(record.get("owner_name") or tax_data.get("owner_name") or ""))
    set_if_value(record, "assessed_value", tax_data.get("assessed_value"))
    set_if_value(record, "market_value", tax_data.get("market_value"))
    set_if_value(record, "estimated_value", tax_data.get("market_value") if not has_good_value(record.get("estimated_value")) else record.get("estimated_value"))
    set_if_value(record, "tax_charges", tax_data.get("tax_charges"))
    set_if_value(record, "tax_payments", tax_data.get("tax_payments"))
    set_if_value(record, "tax_balance_due", tax_data.get("tax_balance_due"))
    set_if_value(record, "tax_delinquent_amount", tax_data.get("tax_balance_due"))
    set_if_value(record, "tax_year", tax_data.get("tax_year"))
    set_if_value(record, "tax_pay_year", tax_data.get("tax_pay_year"))
    set_if_value(record, "tax_foreclosure_flag", tax_data.get("tax_foreclosure_flag"))
    set_if_value(record, "tax_certificate_pending_flag", tax_data.get("tax_certificate_pending_flag"))
    set_if_value(record, "tax_certificate_sold_flag", tax_data.get("tax_certificate_sold_flag"))
    set_if_value(record, "tax_payment_plan_flag", tax_data.get("tax_payment_plan_flag"))

    balance_due = tax_data.get("tax_balance_due")
    if isinstance(balance_due, (int, float)) and balance_due > 0:
        record["tax_delinquent"] = True
        record["tax_pressure"] = True
        record["tax_status"] = "Balance Due"
        add_unique(record, "distress_sources", ["tax_delinquent"])
        add_unique(record, "flags", ["Tax Delinquent", "Tax Pressure"])
        add_unique(record, "tags", ["Tax Delinquent", "Tax Pressure"])
    if tax_data.get("tax_certificate_pending_flag") == "Y":
        record["tax_certificate"] = True
        add_unique(record, "distress_sources", ["tax_certificate"])
        add_unique(record, "flags", ["Tax Certificate"])
        add_unique(record, "tags", ["Tax Certificate"])
    if tax_data.get("tax_certificate_sold_flag") == "Y":
        record["tax_certificate"] = True
        record["tax_lien"] = True
        add_unique(record, "distress_sources", ["tax_certificate"])
        add_unique(record, "flags", ["Tax Certificate", "Tax Lien"])
        add_unique(record, "tags", ["Tax Certificate", "Tax Lien"])
    if tax_data.get("tax_foreclosure_flag") == "Y":
        record["tax_foreclosure"] = True
        add_unique(record, "flags", ["Tax Foreclosure"])
        add_unique(record, "tags", ["Tax Foreclosure"])
    apply_absentee_owner_flags(record)
    apply_stack_tags(record)
    record["tax_delinquent_enrichment_status"] = "enriched"
    return "enriched"


def enrich_tax_values(limit: int) -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2f.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)
    records = payload.get("records") or []
    counts = {"attempted": 0, "enriched": 0, "no_hit": 0, "failed": 0, "skipped": 0}
    seen_parcels = set()
    for record in records:
        if counts["attempted"] >= limit:
            break
        if record.get("source_county_key") != "cuyahoga":
            counts["skipped"] += 1
            continue
        parcel = clean_parcel(record.get("parcel_id"))
        if not parcel or parcel in seen_parcels:
            continue
        seen_parcels.add(parcel)
        counts["attempted"] += 1
        status = enrich_property_value(record, timestamp)
        counts[status] = counts.get(status, 0) + 1
        apply_stack_tags(record)
        apply_absentee_owner_flags(record)
    payload["phase_2f_tax_value_enrichment"] = {
        "timestamp": timestamp,
        "source": "Cuyahoga MyPlace ParcelsAndValuesByAnySearchByAndCity",
        "assessed_market_value_status": "not_available_in_tested_public_endpoint",
        "limit": limit,
        **counts,
        "backup_path": str(backup_path),
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2f_tax_value_enrichment"]


def enrich_tax_delinquency(limit: int) -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2f-tax-delinquent.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)
    records = payload.get("records") or []
    counts = {"attempted": 0, "enriched": 0, "no_hit": 0, "failed": 0, "skipped": 0}
    seen_parcels = set()
    for record in records:
        if counts["attempted"] >= limit:
            break
        if record.get("source_county_key") != "cuyahoga":
            counts["skipped"] += 1
            continue
        parcel = clean_parcel(record.get("parcel_id"))
        if not parcel or parcel in seen_parcels:
            continue
        seen_parcels.add(parcel)
        counts["attempted"] += 1
        status = enrich_legacy_tax_bill(record, timestamp)
        counts[status] = counts.get(status, 0) + 1
    tax_delinquent_count = sum(1 for record in records if record.get("tax_delinquent"))
    tax_certificate_count = sum(1 for record in records if record.get("tax_certificate"))
    tax_lien_count = sum(1 for record in records if record.get("tax_lien"))
    hot_stack_count = sum(
        1
        for record in records
        if record.get("source_county_key") == "cuyahoga"
        and ("Cuyahoga Hot Stack" in (record.get("tags") or []) or int(record.get("distress_count") or 0) >= 2)
    )
    payload["phase_2f_tax_delinquent_enrichment"] = {
        "timestamp": timestamp,
        "source": "Cuyahoga MyPlace LegacyTaxes",
        "source_url": LEGACY_TAXES_URL,
        "limit": limit,
        **counts,
        "tax_delinquent_count": tax_delinquent_count,
        "tax_certificate_count": tax_certificate_count,
        "tax_lien_count": tax_lien_count,
        "cuyahoga_hot_stack_count": hot_stack_count,
        "backup_path": str(backup_path),
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2f_tax_delinquent_enrichment"]


def enrich_foreclosure_stack(limit: int, date_limit: int) -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2g-foreclosure.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)

    records = payload.get("records") or []
    parcel_index = {}
    address_index = {}
    owner_candidates = {}
    for record in records:
        if record.get("source_county_key") != "cuyahoga":
            continue
        parcel = clean_parcel(record.get("parcel_id"))
        if parcel:
            parcel_index[parcel] = record
        address = normalize_address_key(record.get("property_address") or record.get("prop_address"))
        if address:
            address_index[address] = record
        owner = normalize_owner_key(record.get("owner_name") or record.get("owner"))
        if len(owner) > 5:
            owner_candidates.setdefault(owner, []).append(record)
    owner_index = {owner: items[0] for owner, items in owner_candidates.items() if len(items) == 1}

    sheriff_records, sale_dates_checked = fetch_sheriff_sale_records(date_limit, limit)
    matched = 0
    standalone = 0
    for incoming in sheriff_records:
        target = None
        parcel = clean_parcel(incoming.get("parcel_id"))
        address = normalize_address_key(incoming.get("property_address"))
        owner = normalize_owner_key(incoming.get("foreclosure_defendant"))
        if parcel and parcel in parcel_index:
            target = parcel_index[parcel]
        elif address and address in address_index:
            target = address_index[address]
        elif owner and owner in owner_index:
            target = owner_index[owner]

        if target:
            merge_record(target, incoming)
            apply_absentee_owner_flags(target)
            apply_stack_tags(target)
            matched += 1
            continue

        if incoming.get("property_address"):
            incoming["source_city_key"] = incoming.get("source_city_key") or ""
            incoming["property_city"] = incoming.get("property_city") or ""
            incoming["city"] = incoming.get("city") or ""
            incoming["property_state"] = incoming.get("property_state") or "OH"
            incoming["last_updated"] = timestamp
            apply_stack_tags(incoming)
            records.append(incoming)
            if parcel:
                parcel_index[parcel] = incoming
            if address:
                address_index[address] = incoming
            standalone += 1

    foreclosure_count = sum(1 for record in records if record.get("foreclosure"))
    sheriff_sale_count = sum(1 for record in records if record.get("sheriff_sale"))
    hot_stack_count = sum(
        1
        for record in records
        if record.get("source_county_key") == "cuyahoga"
        and ("Cuyahoga Hot Stack" in (record.get("tags") or []) or int(record.get("distress_count") or 0) >= 2)
    )
    payload.update(
        {
            "records": records,
            "record_count": len(records),
            "fetched_at": timestamp,
            "phase_2g_foreclosure_enrichment": {
                "timestamp": timestamp,
                "source": "Cuyahoga Clerk of Courts Sheriff Sale Results",
                "source_url": SHERIFF_SEARCH_URL,
                "sale_dates_checked": sale_dates_checked,
                "records_pulled": len(sheriff_records),
                "matched": matched,
                "standalone": standalone,
                "foreclosure_count": foreclosure_count,
                "sheriff_sale_count": sheriff_sale_count,
                "cuyahoga_hot_stack_count": hot_stack_count,
                "backup_path": str(backup_path),
            },
        }
    )
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2g_foreclosure_enrichment"]


def enrich_cash_buyer_signals(limit: int) -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2h-cash-buyer.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)
    records = payload.get("records") or []
    counts = {"attempted": 0, "enriched": 0, "no_hit": 0, "failed": 0, "skipped": 0}
    seen_parcels = set()
    for record in records:
        if record.get("source_county_key") == "cuyahoga":
            apply_investor_owner_flags(record)
        if counts["attempted"] >= limit:
            continue
        if record.get("source_county_key") != "cuyahoga":
            counts["skipped"] += 1
            continue
        parcel = clean_parcel(record.get("parcel_id"))
        if not parcel or parcel in seen_parcels:
            continue
        seen_parcels.add(parcel)
        counts["attempted"] += 1
        status = enrich_transfer_history(record, timestamp)
        counts[status] = counts.get(status, 0) + 1

    investor_owner_count = sum(1 for record in records if record.get("source_county_key") == "cuyahoga" and record.get("investor_owner"))
    candidate_count = sum(1 for record in records if record.get("source_county_key") == "cuyahoga" and record.get("cash_buyer_candidate"))
    confirmed_count = sum(1 for record in records if record.get("source_county_key") == "cuyahoga" and record.get("confirmed_cash_buyer"))
    payload["phase_2h_cash_buyer_enrichment"] = {
        "timestamp": timestamp,
        "source": "Cuyahoga MyPlace PropertyData Transfers",
        "source_url": PROPERTY_DATA_URL,
        "limit": limit,
        **counts,
        "investor_owner_count": investor_owner_count,
        "cash_buyer_candidate_count": candidate_count,
        "confirmed_cash_buyer_count": confirmed_count,
        "backup_path": str(backup_path),
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2h_cash_buyer_enrichment"]


def apply_prime_deal_flags() -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2i-prime-deal.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)
    records = payload.get("records") or []
    sample_reasons = []
    for record in records:
        if record.get("source_county_key") != "cuyahoga":
            continue
        apply_investor_owner_flags(record)
        apply_absentee_owner_flags(record)
        groups = apply_prime_deal_flag(record)
        apply_stack_tags(record)
        apply_cuyahoga_stack_score(record)
        if record.get("prime_deal") and len(sample_reasons) < 10:
            sample_reasons.append(
                {
                    "parcel_id": record.get("parcel_id"),
                    "property_address": record.get("property_address"),
                    "owner_name": record.get("owner_name"),
                    "prime_deal_reason": record.get("prime_deal_reason") or "; ".join(groups),
                }
            )
    prime_count = sum(1 for record in records if record.get("source_county_key") == "cuyahoga" and record.get("prime_deal"))
    payload["phase_2i_prime_deal_flags"] = {
        "timestamp": timestamp,
        "prime_deal_count": prime_count,
        "sample_reasons": sample_reasons,
        "backup_path": str(backup_path),
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2i_prime_deal_flags"]


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
        elif field.startswith("foreclosure_") or field.startswith("sheriff_") or field in ("foreclosure", "sheriff_sale"):
            if has_value(value):
                existing[field] = value
        elif field.startswith("demolition_") or field == "demolition_permit":
            if has_value(value):
                existing[field] = value
        elif field.startswith("nuisance_") or field in ("public_health_complaint", "nuisance_complaint"):
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
    if record.get("foreclosure"):
        add_unique(record, "distress_sources", ["foreclosure"])
        add_unique(record, "flags", ["Foreclosure", "Auction Pressure"])
        add_unique(record, "tags", ["Foreclosure", "Auction Pressure"])
    if record.get("sheriff_sale"):
        add_unique(record, "distress_sources", ["sheriff_sale"])
        add_unique(record, "flags", ["Sheriff Sale", "Auction Pressure"])
        add_unique(record, "tags", ["Sheriff Sale", "Auction Pressure"])
    if record.get("demolition_permit"):
        add_unique(record, "distress_sources", ["demolition"])
        add_unique(record, "flags", ["Demolition", "Blight Pressure", "Unsafe"])
        add_unique(record, "tags", ["Demolition", "Blight Pressure", "Unsafe"])
    if record.get("nuisance_complaint") or record.get("public_health_complaint"):
        add_unique(record, "distress_sources", ["nuisance_complaint"])
        add_unique(record, "flags", nuisance_tags(record.get("nuisance_type")) or ["Public Health Complaint"])
        add_unique(record, "tags", nuisance_tags(record.get("nuisance_type")) or ["Public Health Complaint"])
    if int(record.get("distress_count") or 0) >= 2 or record.get("active_condemnation"):
        add_unique(record, "flags", ["Cuyahoga Hot Stack"])
        add_unique(record, "tags", ["Cuyahoga Hot Stack"])


def has_distress(record: dict) -> bool:
    text = " ".join(
        str(value)
        for value in (record.get("distress_sources") or []) + (record.get("flags") or []) + (record.get("tags") or [])
    ).lower()
    return any(
        marker in text
        for marker in (
            "code_violation",
            "code violation",
            "housing pain",
            "vacant",
            "condemnation",
            "building_violation_status",
        )
    )


def apply_absentee_owner_flags(record: dict) -> None:
    owner = str(record.get("owner_name") or record.get("owner") or "").strip()
    if owner and owner_type(owner) == "entity":
        record["owner_type"] = "entity"
        record["entity_owner"] = True
        add_unique(record, "flags", ["Entity Owner"])
        add_unique(record, "tags", ["Entity Owner"])

    mailing_address = str(record.get("mailing_address") or "").strip()
    mailing_state = str(record.get("mailing_state") or "").strip().upper()
    property_address = str(record.get("property_address") or record.get("prop_address") or "").strip()
    if mailing_address and property_address and normalize_address_key(mailing_address) != normalize_address_key(property_address):
        record["absentee_owner"] = True
        record["is_absentee"] = True
        add_unique(record, "flags", ["Absentee"])
        add_unique(record, "tags", ["Absentee"])
    if mailing_state and mailing_state != "OH":
        record["out_of_state_owner"] = True
        record["is_out_of_state"] = True
        add_unique(record, "flags", ["Out-of-State"])
        add_unique(record, "tags", ["Out-of-State"])

    if has_distress(record) and (record.get("entity_owner") or record.get("absentee_owner")):
        record["tired_landlord"] = True
        record["tired_landlord_plus"] = True
        add_unique(record, "flags", ["Tired Landlord Plus"])
        add_unique(record, "tags", ["Tired Landlord Plus"])


def apply_absentee_flags() -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2e.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)
    records = payload.get("records") or []
    for record in records:
        if record.get("source_county_key") == "cuyahoga":
            apply_absentee_owner_flags(record)
            apply_stack_tags(record)
    payload["phase_2e_absentee_enrichment"] = {
        "timestamp": timestamp,
        "source": "Cuyahoga MyPlace/Fiscal public owner fields; mailing endpoint not exposed in tested public services",
        "mailing_endpoint_status": "not_available",
        "records_processed": len(records),
        "backup_path": str(backup_path),
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2e_absentee_enrichment"]


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


def enrich_demolition_permits(limit: int) -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2k-demolition.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)

    merged = {}
    for record in payload.get("records") or []:
        key = record_key(record)
        if key:
            merged[key] = record

    demolition_rows = fetch_arcgis_records(DEMOLITION_PERMITS_QUERY_URL, limit, "ISSUED_DATE DESC")
    matched = 0
    standalone = 0
    samples = []
    for record in [normalize_demolition_record(row, timestamp) for row in demolition_rows]:
        key = record_key(record)
        if not key:
            continue
        if key in merged:
            matched += 1
            merged[key] = merge_record(merged[key], record)
        else:
            standalone += 1
            merged[key] = record
        apply_stack_tags(merged[key])
        apply_prime_deal_flag(merged[key])
        apply_cuyahoga_stack_score(merged[key])
        if len(samples) < 5:
            sample = merged[key]
            samples.append(
                {
                    "property_address": sample.get("property_address"),
                    "parcel_id": sample.get("parcel_id"),
                    "demolition_permit_number": sample.get("demolition_permit_number"),
                    "demolition_date": sample.get("demolition_date"),
                    "distress_count": sample.get("distress_count"),
                    "score": sample.get("score"),
                }
            )

    records = list(merged.values())
    for record in records:
        if record.get("source_county_key") == "cuyahoga":
            apply_stack_tags(record)
            apply_prime_deal_flag(record)
            apply_cuyahoga_stack_score(record)

    demolition_count = sum(1 for record in records if record.get("source_county_key") == "cuyahoga" and record.get("demolition_permit"))
    prime_count = sum(1 for record in records if record.get("source_county_key") == "cuyahoga" and record.get("prime_deal"))
    payload.update(
        {
            "fetched_at": timestamp,
            "record_count": len(records),
            "records": records,
            "phase_2k_demolition_permit_stack": {
                "timestamp": timestamp,
                "source": "Cleveland Open Data Demolition Permits",
                "source_url": DEMOLITION_PERMITS_URL,
                "records_pulled": len(demolition_rows),
                "matched_count": matched,
                "standalone_added_count": standalone,
                "demolition_permit_count": demolition_count,
                "prime_deal_count": prime_count,
                "sample_records": samples,
                "backup_path": str(backup_path),
            },
        }
    )
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2k_demolition_permit_stack"] | {"total_records": len(records)}


def enrich_nuisance_complaints(limit: int) -> dict:
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    backup_path = OUTPUT_PATH.with_suffix(f".phase2m-nuisance.{timestamp.replace(':', '').replace('+', 'Z')}.bak.json")
    shutil.copy2(OUTPUT_PATH, backup_path)

    merged = {}
    for record in payload.get("records") or []:
        key = record_key(record)
        if key:
            merged[key] = record

    rows = fetch_arcgis_records(PUBLIC_HEALTH_COMPLAINTS_QUERY_URL, limit, "submit_datetime DESC")
    matched = 0
    standalone = 0
    skipped = 0
    samples = []
    for record in (normalize_public_health_complaint_record(row, timestamp) for row in rows):
        if not record:
            skipped += 1
            continue
        key = record_key(record)
        if not key:
            skipped += 1
            continue
        if key in merged:
            matched += 1
            merged[key] = merge_record(merged[key], record)
        else:
            standalone += 1
            merged[key] = record
        target = merged[key]
        apply_stack_tags(target)
        apply_prime_deal_flag(target)
        apply_cuyahoga_stack_score(target)
        if len(samples) < 5:
            samples.append(
                {
                    "property_address": target.get("property_address"),
                    "parcel_id": target.get("parcel_id"),
                    "nuisance_type": target.get("nuisance_type"),
                    "nuisance_status": target.get("nuisance_status"),
                    "nuisance_date": target.get("nuisance_date"),
                    "distress_count": target.get("distress_count"),
                    "score": target.get("score"),
                }
            )

    records = list(merged.values())
    for record in records:
        if record.get("source_county_key") == "cuyahoga":
            apply_stack_tags(record)
            apply_prime_deal_flag(record)
            apply_cuyahoga_stack_score(record)

    nuisance_count = sum(1 for record in records if record.get("source_county_key") == "cuyahoga" and record.get("nuisance_complaint"))
    prime_count = sum(1 for record in records if record.get("source_county_key") == "cuyahoga" and record.get("prime_deal"))
    payload.update(
        {
            "fetched_at": timestamp,
            "record_count": len(records),
            "records": records,
            "phase_2m_nuisance_complaint_stack": {
                "timestamp": timestamp,
                "source": "Cleveland CDPH Public Health Complaints",
                "source_url": PUBLIC_HEALTH_COMPLAINTS_URL,
                "records_pulled": len(rows),
                "matched_count": matched,
                "standalone_added_count": standalone,
                "skipped_non_strong_category_count": skipped,
                "nuisance_complaint_count": nuisance_count,
                "prime_deal_count": prime_count,
                "sample_records": samples,
                "backup_path": str(backup_path),
            },
        }
    )
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload["phase_2m_nuisance_complaint_stack"] | {"total_records": len(records)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Cuyahoga/Cleveland dashboard records.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--enrich-owners", action="store_true")
    parser.add_argument("--expand-stacks", action="store_true")
    parser.add_argument("--apply-absentee-flags", action="store_true")
    parser.add_argument("--enrich-tax-values", action="store_true")
    parser.add_argument("--enrich-tax-delinquency", action="store_true")
    parser.add_argument("--enrich-foreclosures", action="store_true")
    parser.add_argument("--enrich-cash-buyers", action="store_true")
    parser.add_argument("--apply-prime-deals", action="store_true")
    parser.add_argument("--enrich-demolition-permits", action="store_true")
    parser.add_argument("--enrich-nuisance-complaints", action="store_true")
    parser.add_argument("--owner-limit", type=int, default=250)
    parser.add_argument("--violation-limit", type=int, default=5000)
    parser.add_argument("--property-limit", type=int, default=1000)
    parser.add_argument("--sale-date-limit", type=int, default=45)
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
    if args.apply_absentee_flags:
        result = apply_absentee_flags()
        print(json.dumps(result, indent=2))
        return
    if args.enrich_tax_values:
        result = enrich_tax_values(max(1, min(args.property_limit, 5000)))
        print(json.dumps(result, indent=2))
        return
    if args.enrich_tax_delinquency:
        result = enrich_tax_delinquency(max(1, min(args.property_limit, 1000)))
        print(json.dumps(result, indent=2))
        return
    if args.enrich_foreclosures:
        result = enrich_foreclosure_stack(max(1, min(args.property_limit, 750)), max(1, min(args.sale_date_limit, 90)))
        print(json.dumps(result, indent=2))
        return
    if args.enrich_cash_buyers:
        result = enrich_cash_buyer_signals(max(1, min(args.property_limit, 1000)))
        print(json.dumps(result, indent=2))
        return
    if args.apply_prime_deals:
        result = apply_prime_deal_flags()
        print(json.dumps(result, indent=2))
        return
    if args.enrich_demolition_permits:
        result = enrich_demolition_permits(max(1, min(args.limit, 5000)))
        print(json.dumps(result, indent=2))
        return
    if args.enrich_nuisance_complaints:
        result = enrich_nuisance_complaints(max(1, min(args.limit, 5000)))
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
