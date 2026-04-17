from datetime import datetime
import json
import re
import tempfile
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
DASHBOARD_DIR = ROOT_DIR / "dashboard"
CONFIG_PATH = ROOT_DIR / "tracerfy.config.js"
TRACE_FIELDS = (
    "traced_owner_name",
    "traced_owner",
    "phone_primary",
    "phone_primary_type",
    "phone_secondary",
    "phone_secondary_type",
    "phone_tertiary",
    "phone_tertiary_type",
    "email_primary",
    "traced_email",
    "traced_mailing_address",
    "traced_mailing_city",
    "traced_mailing_state",
    "traced_mailing_zip",
    "skip_trace_status",
    "skip_trace_source",
    "has_phone",
    "has_email",
    "skip_trace_timestamp",
    "Phone 1",
    "Phone 1 Type",
    "Phone 2",
    "Phone 2 Type",
    "Phone 3",
    "Phone 3 Type",
    "Email",
    "Skip Trace Source",
)
TRACE_SOURCE = "Tracerfy"


def utc_timestamp():
    return datetime.utcnow().isoformat() + "Z"


def clean_config_value(value):
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""
    if cleaned.upper().startswith("TRACERFY_") or cleaned.upper().startswith("REPLACE_"):
        return ""
    return cleaned


def extract_config_value(text, field_name):
    pattern = rf"{re.escape(field_name)}\s*:\s*['\"]([^'\"]+)['\"]"
    match = re.search(pattern, text)
    return clean_config_value(match.group(1) if match else "")


def read_tracerfy_config():
    text = CONFIG_PATH.read_text(encoding="utf-8")
    api_base = extract_config_value(text, "apiBase") or "https://tracerfy.com/v1/api"
    api_key = extract_config_value(text, "apiKey") or extract_config_value(text, "apiToken")
    return {"api_base": api_base.rstrip("/"), "api_key": api_key}


def unique_list(items):
    seen = set()
    values = []
    for item in items or []:
        value = str(item or "").strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values


def normalize_match_value(value):
    return re.sub(r"\s+", " ", str(value or "").strip()).upper()


def normalize_address_value(value):
    return re.sub(r"[^A-Z0-9]+", " ", normalize_match_value(value)).strip()


def normalize_property_address(value):
    tokens = normalize_address_value(value).split()
    while len(tokens) > 2 and tokens[-1].isdigit():
        tokens.pop()
    return " ".join(tokens)


def lead_key(record):
    return "|".join(
        [
            str(record.get("doc_num") or ""),
            str(record.get("parcel_id") or ""),
            normalize_property_address(record.get("prop_address")),
            str(record.get("prop_zip") or ""),
        ]
    ).upper()


def legacy_lead_key(record):
    return "|".join(
        [
            str(record.get("doc_num") or ""),
            str(record.get("parcel_id") or ""),
            str(record.get("owner") or ""),
            normalize_property_address(record.get("prop_address")),
            str(record.get("prop_zip") or ""),
        ]
    ).upper()


def record_match_keys(record):
    record = record if isinstance(record, dict) else {}
    parcel_id = normalize_match_value(record.get("parcel_id"))
    prop_address = normalize_property_address(record.get("prop_address"))
    prop_city = normalize_match_value(record.get("prop_city"))
    prop_state = normalize_match_value(record.get("prop_state"))
    prop_zip = normalize_match_value(record.get("prop_zip"))
    keys = []
    seen = set()

    def add_key(value):
        key = normalize_match_value(value)
        if not key or key in seen:
            return
        seen.add(key)
        keys.append(key)

    add_key(lead_key(record))
    add_key(legacy_lead_key(record))
    if parcel_id:
        add_key(f"PARCEL:{parcel_id}")
    if prop_address and prop_zip:
        add_key(f"PROPERTY_ZIP:{prop_address}|{prop_zip}")
    if prop_address and prop_city and prop_state:
        add_key(f"PROPERTY:{prop_address}|{prop_city}|{prop_state}")
    if prop_address:
        add_key(f"PROPERTY_ONLY:{prop_address}")
    return keys


def normalize_list_label(value):
    label = str(value or "").strip().lower()
    if not label:
        return ""
    if "hot stack" in label or "new this week" in label or "candidate" in label:
        return ""
    if "sheriff" in label:
        return "sheriff_sale"
    if "foreclos" in label:
        return "foreclosure"
    if "tax delin" in label:
        return "tax_delinquent"
    if "code viol" in label or "nuisance" in label:
        return "code_violation"
    if "absentee" in label:
        return "absentee"
    if "out-of-state" in label or "out of state" in label:
        return "out_of_state"
    if "vacant land" in label:
        return "vacant_land"
    if "vacant home" in label or "vacant building" in label:
        return "vacant_home"
    if "inherited" in label:
        return "inherited"
    if "probate" in label or "estate" in label:
        return "probate"
    if "subject-to" in label or "subject to" in label:
        return "subject_to"
    if "eviction" in label:
        return "eviction"
    if "divorce" in label:
        return "divorce"
    if "fire damage" in label:
        return "fire_damage"
    return re.sub(r"(^_+|_+$)", "", re.sub(r"[^a-z0-9]+", "_", label))


def get_list_signals(record):
    signals = set()
    for value in unique_list(record.get("distress_sources")):
        normalized = normalize_list_label(value)
        if normalized:
            signals.add(normalized)
    for value in unique_list(record.get("flags")):
        normalized = normalize_list_label(value)
        if normalized:
            signals.add(normalized)
    if record.get("is_absentee"):
        signals.add("absentee")
    if record.get("is_out_of_state"):
        signals.add("out_of_state")
    if record.get("is_inherited"):
        signals.add("inherited")
    if record.get("is_vacant_home"):
        signals.add("vacant_home")
    if record.get("is_vacant_land"):
        signals.add("vacant_land")
    if record.get("doc_type") == "SHERIFF":
        signals.add("sheriff_sale")
    if record.get("doc_type") == "PRO":
        signals.add("probate")
    if "subject-to" in str(record.get("cat_label") or "").lower():
        signals.add("subject_to")
    return sorted(signals)


def to_number(value):
    if value in (None, ""):
        return 0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0


def apply_derived_lead_fields(record):
    lead = dict(record)
    list_count = lead.get("list_count")
    if list_count in (None, ""):
        list_count = len(get_list_signals(lead))
    distress_count = lead.get("distress_count")
    if distress_count in (None, ""):
        distress_count = 0
    lead["list_count"] = int(to_number(list_count))
    lead["distress_count"] = int(to_number(distress_count))
    lead["skip_trace_eligible"] = (
        to_number(lead.get("score")) >= 65
        and lead["list_count"] >= 2
        and lead["distress_count"] >= 2
    )
    return lead


def next_distinct_phone(primary_phone, candidates):
    for value in unique_list(candidates):
        if value != primary_phone:
            return value
    return ""


def next_distinct_value(existing_values, candidates):
    seen = {str(value or "").strip() for value in existing_values if str(value or "").strip()}
    for value in unique_list(candidates):
        if value not in seen:
            return value
    return ""


def extract_first_nonempty(payload, keys):
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def first_list_value(*sequences):
    for sequence in sequences:
        values = unique_list(sequence)
        if values:
            return values[0]
    return ""


def apply_trace_field_aliases(record):
    mapped = dict(record)
    phone_candidates = unique_list(
        [
            mapped.get("Phone 1"),
            mapped.get("Phone1"),
            mapped.get("phone1"),
            mapped.get("phone_primary"),
            mapped.get("Phone 2"),
            mapped.get("Phone2"),
            mapped.get("phone2"),
            mapped.get("phone_secondary"),
            mapped.get("Phone 3"),
            mapped.get("Phone3"),
            mapped.get("phone3"),
            mapped.get("phone_tertiary"),
            *(mapped.get("traced_phones") or []),
            *(mapped.get("phones") or []),
        ]
    )
    primary_phone = extract_first_nonempty(
        mapped, ("phone_primary", "Phone 1", "Phone1", "phone1")
    ) or (phone_candidates[0] if phone_candidates else "")
    secondary_phone = extract_first_nonempty(
        mapped, ("phone_secondary", "Phone 2", "Phone2", "phone2")
    ) or next_distinct_value([primary_phone], phone_candidates)
    tertiary_phone = extract_first_nonempty(
        mapped, ("phone_tertiary", "Phone 3", "Phone3", "phone3")
    ) or next_distinct_value([primary_phone, secondary_phone], phone_candidates)
    primary_phone_type = extract_first_nonempty(
        mapped, ("phone_primary_type", "Phone 1 Type")
    ) or first_list_value(
        [(mapped.get("traced_phone_types") or [""])[0]],
        [(mapped.get("phone_types") or [""])[0]],
    )
    secondary_phone_type = extract_first_nonempty(
        mapped, ("phone_secondary_type", "Phone 2 Type")
    ) or first_list_value(
        [(mapped.get("traced_phone_types") or ["", ""])[1]],
        [(mapped.get("phone_types") or ["", ""])[1]],
    )
    tertiary_phone_type = extract_first_nonempty(
        mapped, ("phone_tertiary_type", "Phone 3 Type")
    ) or first_list_value(
        [(mapped.get("traced_phone_types") or ["", "", ""])[2]],
        [(mapped.get("phone_types") or ["", "", ""])[2]],
    )
    traced_owner_name = extract_first_nonempty(
        mapped, ("traced_owner_name", "traced_owner", "Traced Owner")
    )
    email_primary = extract_first_nonempty(mapped, ("email_primary", "Email", "traced_email")) or first_list_value(
        mapped.get("traced_emails") or [],
        mapped.get("emails") or [],
    )
    traced_mailing_address = extract_first_nonempty(
        mapped, ("traced_mailing_address", "traced_mail_address", "Traced Mailing Address")
    )
    traced_mailing_city = extract_first_nonempty(
        mapped, ("traced_mailing_city", "traced_mail_city", "Traced Mailing City")
    )
    traced_mailing_state = extract_first_nonempty(
        mapped, ("traced_mailing_state", "traced_mail_state", "Traced Mailing State")
    )
    traced_mailing_zip = extract_first_nonempty(
        mapped, ("traced_mailing_zip", "traced_mail_zip", "Traced Mailing Zip")
    )
    has_phone = mapped.get("has_phone")
    if not isinstance(has_phone, bool):
        has_phone = bool(primary_phone or secondary_phone or tertiary_phone)
    has_email = mapped.get("has_email")
    if not isinstance(has_email, bool):
        has_email = bool(email_primary)
    skip_traced = mapped.get("skip_traced")
    skip_trace_hit = mapped.get("skip_trace_hit")
    skip_trace_status = extract_first_nonempty(
        mapped, ("skip_trace_status", "Skip Trace Status")
    )
    if not skip_trace_status:
        if skip_traced is True:
            skip_trace_status = "success" if (has_phone or has_email) else "no_contact"
        elif skip_trace_hit is False:
            skip_trace_status = "no_hit"
    skip_trace_source = extract_first_nonempty(
        mapped, ("skip_trace_source", "Skip Trace Source")
    )
    if not skip_trace_source and (has_phone or has_email or skip_trace_status):
        skip_trace_source = TRACE_SOURCE
    skip_trace_timestamp = extract_first_nonempty(
        mapped, ("skip_trace_timestamp", "skip_trace_at", "Skip Trace Timestamp")
    )
    mapped.update(
        {
            "traced_owner_name": traced_owner_name,
            "traced_owner": traced_owner_name,
            "phone_primary": primary_phone,
            "phone_primary_type": primary_phone_type,
            "phone_secondary": secondary_phone,
            "phone_secondary_type": secondary_phone_type,
            "phone_tertiary": tertiary_phone,
            "phone_tertiary_type": tertiary_phone_type,
            "email_primary": email_primary,
            "traced_email": email_primary,
            "traced_mailing_address": traced_mailing_address,
            "traced_mailing_city": traced_mailing_city,
            "traced_mailing_state": traced_mailing_state,
            "traced_mailing_zip": traced_mailing_zip,
            "skip_trace_status": skip_trace_status,
            "skip_trace_source": skip_trace_source,
            "skip_trace_timestamp": skip_trace_timestamp,
            "has_phone": has_phone,
            "has_email": has_email,
            "Phone 1": primary_phone,
            "Phone 1 Type": primary_phone_type,
            "Phone 2": secondary_phone,
            "Phone 2 Type": secondary_phone_type,
            "Phone 3": tertiary_phone,
            "Phone 3 Type": tertiary_phone_type,
            "Email": email_primary,
            "Skip Trace Source": skip_trace_source,
        }
    )
    return mapped


def looks_like_trace_payload(payload):
    if not isinstance(payload, dict):
        return False
    if isinstance(payload.get("persons"), list) and payload["persons"]:
        return True
    if isinstance(payload.get("person"), dict):
        return True
    if isinstance(payload.get("owner"), dict):
        return True
    return any(
        key in payload
        for key in (
            "phones",
            "emails",
            "mailing_address",
            "first_name",
            "last_name",
            "full_name",
            "name",
            "owner_name",
            "person_name",
        )
    )


def extract_nested_payload(response_data):
    queue = [response_data]
    seen = set()
    while queue:
        payload = queue.pop(0)
        payload_id = id(payload)
        if payload_id in seen:
            continue
        seen.add(payload_id)
        if looks_like_trace_payload(payload):
            return payload
        if isinstance(payload, dict):
            for key in ("data", "result", "results", "matches", "records", "items"):
                candidate = payload.get(key)
                if isinstance(candidate, dict):
                    queue.append(candidate)
                elif isinstance(candidate, list):
                    queue.extend(item for item in candidate if isinstance(item, (dict, list)))
        elif isinstance(payload, list):
            queue.extend(item for item in payload if isinstance(item, (dict, list)))
    return response_data if isinstance(response_data, dict) else {}


def extract_person_candidates(payload):
    candidates = []
    for key in ("persons",):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(item for item in value if isinstance(item, dict))
    for key in ("person", "owner"):
        value = payload.get(key)
        if isinstance(value, dict):
            candidates.append(value)
    if isinstance(payload, dict):
        candidates.append(payload)
    return candidates


def extract_person_name(candidate):
    first_name = extract_first_nonempty(
        candidate,
        ("first_name", "firstname", "owner_first_name", "person_first_name"),
    )
    last_name = extract_first_nonempty(
        candidate,
        ("last_name", "lastname", "owner_last_name", "person_last_name"),
    )
    full_name = " ".join(v for v in (first_name, last_name) if v).strip()
    return full_name or extract_first_nonempty(
        candidate,
        ("owner_name", "full_name", "name", "person_name"),
    )


def extract_phone_entries(candidate):
    entries = []
    for phone in candidate.get("phones") or []:
        if isinstance(phone, dict):
            number = extract_first_nonempty(phone, ("number", "phone"))
            if number:
                entries.append((number, extract_first_nonempty(phone, ("type", "phone_type"))))
        else:
            number = str(phone or "").strip()
            if number:
                entries.append((number, ""))
    fallback_entries = [
        (
            extract_first_nonempty(candidate, ("primary_phone", "mobile_1", "mobile_2", "landline_1", "phone", "phone_1")),
            extract_first_nonempty(candidate, ("primary_phone_type", "phone_type")),
        ),
        (str(candidate.get("mobile_1") or "").strip(), "mobile"),
        (str(candidate.get("mobile_2") or "").strip(), "mobile"),
        (str(candidate.get("landline_1") or "").strip(), "landline"),
    ]
    for number, phone_type in fallback_entries:
        if number:
            entries.append((number, phone_type))
    unique_entries = []
    seen_numbers = set()
    for number, phone_type in entries:
        if number in seen_numbers:
            continue
        seen_numbers.add(number)
        unique_entries.append((number, phone_type))
    return unique_entries


def extract_email_candidates(candidate):
    emails = []
    for email in candidate.get("emails") or []:
        if isinstance(email, dict):
            value = extract_first_nonempty(email, ("email", "address"))
        else:
            value = str(email or "").strip()
        if value:
            emails.append(value)
    emails.extend(
        [
            extract_first_nonempty(candidate, ("email_1", "email", "primary_email")),
        ]
    )
    return unique_list(emails)


def extract_mailing_address(candidate):
    mailing = candidate.get("mailing_address")
    if isinstance(mailing, dict):
        return {
            "address": extract_first_nonempty(mailing, ("address", "address1", "line1", "street")),
            "city": extract_first_nonempty(mailing, ("city",)),
            "state": extract_first_nonempty(mailing, ("state",)),
        }
    if isinstance(mailing, str) and mailing.strip():
        return {"address": mailing.strip(), "city": "", "state": ""}
    return {
        "address": extract_first_nonempty(
            candidate, ("mail_address", "address_mail", "mail_address_1")
        ),
        "city": extract_first_nonempty(candidate, ("mail_city", "mailing_city")),
        "state": extract_first_nonempty(candidate, ("mail_state", "mailing_state")),
    }


def map_tracerfy_lookup_response(record, response_data):
    payload = extract_nested_payload(response_data)
    person_candidates = extract_person_candidates(payload)
    traced_owner_name = ""
    phone_entries = []
    email_candidates = []
    mailing_address = {"address": "", "city": "", "state": ""}
    for candidate in person_candidates:
        if not traced_owner_name:
            traced_owner_name = extract_person_name(candidate)
        if not phone_entries:
            phone_entries = extract_phone_entries(candidate)
        if not email_candidates:
            email_candidates = extract_email_candidates(candidate)
        if not any(mailing_address.values()):
            mailing_address = extract_mailing_address(candidate)
    primary_phone = phone_entries[0][0] if phone_entries else ""
    primary_type = phone_entries[0][1] if phone_entries else ""
    secondary_phone = phone_entries[1][0] if len(phone_entries) > 1 else ""
    secondary_type = phone_entries[1][1] if len(phone_entries) > 1 else ""
    email_primary = email_candidates[0] if email_candidates else ""
    trace_data = {
        "traced_owner_name": traced_owner_name,
        "phone_primary": primary_phone,
        "phone_primary_type": primary_type,
        "phone_secondary": secondary_phone,
        "phone_secondary_type": secondary_type,
        "email_primary": email_primary,
        "traced_mailing_address": mailing_address["address"],
        "traced_mailing_city": mailing_address["city"],
        "traced_mailing_state": mailing_address["state"],
        "skip_trace_source": TRACE_SOURCE,
        "skip_trace_timestamp": utc_timestamp(),
    }
    trace_data["has_phone"] = bool(trace_data["phone_primary"] or trace_data["phone_secondary"])
    trace_data["has_email"] = bool(trace_data["email_primary"])
    had_any_value = any(
        [
            trace_data["traced_owner_name"],
            trace_data["phone_primary"],
            trace_data["phone_secondary"],
            trace_data["email_primary"],
            trace_data["traced_mailing_address"],
            trace_data["traced_mailing_city"],
            trace_data["traced_mailing_state"],
        ]
    )
    trace_data["skip_trace_status"] = (
        "success"
        if trace_data["has_phone"] or trace_data["has_email"]
        else ("no_contact" if had_any_value else "no_hit")
    )
    if not str(record.get("owner") or "").strip() and trace_data["traced_owner_name"]:
        trace_data["owner"] = trace_data["traced_owner_name"]
    return apply_trace_field_aliases(apply_derived_lead_fields({**record, **trace_data}))


def atomic_write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def iter_lead_json_paths():
    seen = set()
    for parent in (DATA_DIR, DASHBOARD_DIR):
        for path in sorted(parent.glob("*.json")):
            if path.name == "match_report.json":
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            yield path


def merge_trace_data(record, trace_data):
    merged = dict(record)
    for field in TRACE_FIELDS:
        if field in trace_data:
            merged[field] = trace_data[field]
    return apply_trace_field_aliases(apply_derived_lead_fields(merged))


def get_records_payload(payload):
    if isinstance(payload, list):
        return payload, False
    if isinstance(payload, dict) and isinstance(payload.get("records"), list):
        return payload["records"], True
    return None, False


def extract_trace_payload(record):
    if not isinstance(record, dict):
        return None
    trace_payload = {field: record[field] for field in TRACE_FIELDS if field in record}
    if not any(
        trace_payload.get(field)
        for field in (
            "traced_owner_name",
            "traced_owner",
            "phone_primary",
            "phone_secondary",
            "phone_tertiary",
            "email_primary",
            "traced_email",
            "traced_mailing_address",
            "traced_mailing_city",
            "traced_mailing_state",
            "traced_mailing_zip",
            "skip_trace_status",
        )
    ):
        return None
    return trace_payload


def sync_dashboard_trace_fields_from_data(match_keys=None):
    trace_index = {}
    for path in sorted(DATA_DIR.glob("*.json")):
        if path.name == "match_report.json":
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        records, _ = get_records_payload(payload)
        if records is None:
            continue
        for item in records:
            trace_payload = extract_trace_payload(item)
            if not trace_payload:
                continue
            for key in record_match_keys(item):
                trace_index[key] = trace_payload

    if not trace_index:
        return []

    normalized_match_keys = set()
    for value in match_keys or []:
        cleaned = normalize_match_value(value)
        if cleaned:
            normalized_match_keys.add(cleaned)

    updated_files = []
    for path in sorted(DASHBOARD_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        records, _ = get_records_payload(payload)
        if records is None:
            continue
        changed = False
        for idx, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            item_keys = record_match_keys(item)
            if normalized_match_keys and normalized_match_keys.isdisjoint(item_keys):
                continue
            trace_payload = next((trace_index[key] for key in item_keys if key in trace_index), None)
            if not trace_payload:
                continue
            merged = merge_trace_data(item, trace_payload)
            if merged == item:
                continue
            records[idx] = merged
            changed = True
        if changed:
            atomic_write_json(path, payload)
            updated_files.append(str(path.relative_to(ROOT_DIR)).replace("\\", "/"))
    return updated_files


def persist_trace_data(lead, trace_data, explicit_keys=None):
    keys = record_match_keys(lead)
    for value in explicit_keys or []:
        cleaned = normalize_match_value(value)
        if cleaned:
            keys.add(cleaned)
    updated_files = []
    for path in iter_lead_json_paths():
        payload = json.loads(path.read_text(encoding="utf-8"))
        records, _ = get_records_payload(payload)
        if records is None:
            continue
        changed = False
        for idx, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            item_keys = record_match_keys(item)
            if keys.isdisjoint(item_keys):
                continue
            records[idx] = merge_trace_data(item, trace_data)
            changed = True
        if changed:
            atomic_write_json(path, payload)
            updated_files.append(str(path.relative_to(ROOT_DIR)).replace("\\", "/"))
    for path in sync_dashboard_trace_fields_from_data(keys):
        if path not in updated_files:
            updated_files.append(path)
    return updated_files


def perform_tracerfy_lookup(lookup_request):
    config = read_tracerfy_config()
    if not config["api_key"]:
        raise RuntimeError("Tracerfy apiKey/apiToken is not configured in tracerfy.config.js")
    endpoint = f"{config['api_base']}/trace/lookup/"
    request_body = json.dumps(lookup_request).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=request_body,
        headers={
            "Authorization": f"Bearer {config['api_key']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Tracerfy lookup failed ({exc.code}): {body[:300]}") from exc


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT_DIR), **kwargs)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_POST(self):
        if self.path.rstrip("/") != "/api/tracerfy/trace":
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
            return
        try:
            content_length = int(self.headers.get("Content-Length") or "0")
            raw_body = self.rfile.read(content_length).decode("utf-8")
            body = json.loads(raw_body or "{}")
            lead = body.get("lead") if isinstance(body.get("lead"), dict) else body
            if not isinstance(lead, dict):
                raise ValueError("Request body must include a lead object")
            lead = apply_derived_lead_fields(lead)
            if not lead.get("skip_trace_eligible"):
                trace_data = apply_derived_lead_fields(
                    {
                        **lead,
                        "skip_trace_status": lead.get("skip_trace_status") or "",
                        "skip_trace_source": lead.get("skip_trace_source") or "",
                        "skip_trace_timestamp": lead.get("skip_trace_timestamp") or "",
                    }
                )
                self.respond_json({"trace_data": trace_data, "updated_files": []})
                return
            if not (lead.get("prop_address") and lead.get("prop_city") and lead.get("prop_state")):
                trace_data = apply_derived_lead_fields(
                    {
                        **lead,
                        "skip_trace_status": "missing_address",
                        "skip_trace_source": TRACE_SOURCE,
                        "skip_trace_timestamp": utc_timestamp(),
                    }
                )
                updated_files = persist_trace_data(
                    lead,
                    trace_data,
                    [body.get("lead_key"), body.get("legacy_lead_key")],
                )
                self.respond_json({"trace_data": trace_data, "updated_files": updated_files})
                return
            lookup_request = body.get("trace_lookup") if isinstance(body.get("trace_lookup"), dict) else {}
            if not lookup_request:
                lookup_request = {
                    "address": lead.get("prop_address") or "",
                    "city": lead.get("prop_city") or "",
                    "state": lead.get("prop_state") or "",
                }
            response_data = perform_tracerfy_lookup(lookup_request)
            trace_data = map_tracerfy_lookup_response(lead, response_data)
            updated_files = persist_trace_data(
                lead,
                trace_data,
                [body.get("lead_key"), body.get("legacy_lead_key")],
            )
            self.respond_json(
                {
                    "trace_data": trace_data,
                    "updated_files": updated_files,
                }
            )
        except Exception as exc:  # noqa: BLE001
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_GATEWAY)

    def respond_json(self, payload, status=HTTPStatus.OK):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def main():
    host = "127.0.0.1"
    port = 8765
    sync_dashboard_trace_fields_from_data()
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Serving dashboard at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
