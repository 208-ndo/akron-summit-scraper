import json
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "tracerfy.config.js"
TRACE_SOURCE = "Tracerfy"


def utc_timestamp():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values


def to_number(value):
    if value in (None, ""):
        return 0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0


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


def next_distinct_value(existing_values, candidates):
    seen = {str(value or "").strip() for value in existing_values if str(value or "").strip()}
    for value in unique_list(candidates):
        if value not in seen:
            return value
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
    phone_type_candidates = unique_list(
        [
            mapped.get("Phone 1 Type"),
            mapped.get("phone_primary_type"),
            mapped.get("Phone 2 Type"),
            mapped.get("phone_secondary_type"),
            mapped.get("Phone 3 Type"),
            mapped.get("phone_tertiary_type"),
            *(mapped.get("traced_phone_types") or []),
            *(mapped.get("phone_types") or []),
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
    ) or (phone_type_candidates[0] if phone_type_candidates else "")
    secondary_phone_type = extract_first_nonempty(
        mapped, ("phone_secondary_type", "Phone 2 Type")
    ) or (phone_type_candidates[1] if len(phone_type_candidates) > 1 else "")
    tertiary_phone_type = extract_first_nonempty(
        mapped, ("phone_tertiary_type", "Phone 3 Type")
    ) or (phone_type_candidates[2] if len(phone_type_candidates) > 2 else "")

    traced_owner_name = extract_first_nonempty(
        mapped, ("traced_owner_name", "traced_owner", "Traced Owner")
    )

    email_candidates = unique_list(
        [
            mapped.get("email_primary"),
            mapped.get("Email"),
            mapped.get("traced_email"),
            *(mapped.get("traced_emails") or []),
            *(mapped.get("emails") or []),
        ]
    )
    email_primary = first_list_value(email_candidates)

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
    if not skip_trace_source and (
        has_phone or has_email or traced_owner_name or traced_mailing_address or skip_trace_status
    ):
        skip_trace_source = TRACE_SOURCE

    skip_trace_timestamp = extract_first_nonempty(
        mapped, ("skip_trace_timestamp", "skip_trace_at", "Skip Trace Timestamp")
    )

    traced_phones = unique_list([primary_phone, secondary_phone, tertiary_phone] + phone_candidates)
    traced_phone_types = unique_list(
        [primary_phone_type, secondary_phone_type, tertiary_phone_type] + phone_type_candidates
    )
    traced_emails = unique_list([email_primary] + email_candidates)

    mapped.update(
        {
            "traced_owner_name": traced_owner_name,
            "traced_owner": traced_owner_name,
            "traced_phones": traced_phones,
            "traced_phone_types": traced_phone_types,
            "traced_emails": traced_emails,
            "phones": traced_phones,
            "phone_types": traced_phone_types,
            "emails": traced_emails,
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
    if any(
        payload.get(key)
        for key in (
            "persons", "people", "owners", "matches", "records", "items",
            "primary_phone", "mobile_1", "phone", "email", "email_1", "emails"
        )
    ):
        return True
    return bool(
        extract_first_nonempty(
            payload,
            (
                "first_name",
                "last_name",
                "full_name",
                "name",
                "owner_name",
                "person_name",
            ),
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
            for key in (
                "data", "result", "results", "matches", "records", "items",
                "record", "payload", "response", "person", "owner"
            ):
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

    def add_candidate(value):
        if isinstance(value, dict):
            candidates.append(value)
        elif isinstance(value, list):
            candidates.extend(item for item in value if isinstance(item, dict))

    add_candidate(payload.get("persons"))
    add_candidate(payload.get("matches"))
    add_candidate(payload.get("records"))
    add_candidate(payload.get("items"))
    add_candidate(payload.get("owners"))
    add_candidate(payload.get("people"))
    add_candidate(payload.get("person"))
    add_candidate(payload.get("owner"))

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
        ("owner_name", "full_name", "name", "person_name", "contact_name"),
    )


def extract_phone_entries(candidate):
    entries = []

    for key in ("phones", "phone_numbers", "contact_numbers"):
        values = candidate.get(key) or []
        for phone in values:
            if isinstance(phone, dict):
                number = extract_first_nonempty(
                    phone,
                    ("number", "phone", "value", "phone_number", "national", "e164"),
                )
                phone_type = extract_first_nonempty(phone, ("type", "phone_type", "label"))
                if number:
                    entries.append((number, phone_type))
            else:
                number = str(phone or "").strip()
                if number:
                    entries.append((number, ""))

    fallback_entries = [
        (
            extract_first_nonempty(
                candidate,
                (
                    "primary_phone", "mobile_1", "mobile_2", "landline_1", "phone",
                    "phone_1", "phone1", "best_phone", "cell_phone", "mobile_phone"
                ),
            ),
            extract_first_nonempty(candidate, ("primary_phone_type", "phone_type")),
        ),
        (str(candidate.get("mobile_1") or "").strip(), "mobile"),
        (str(candidate.get("mobile_2") or "").strip(), "mobile"),
        (str(candidate.get("landline_1") or "").strip(), "landline"),
        (str(candidate.get("phone_2") or candidate.get("phone2") or "").strip(), ""),
        (str(candidate.get("phone_3") or candidate.get("phone3") or "").strip(), ""),
    ]
    for number, phone_type in fallback_entries:
        if number:
            entries.append((number, phone_type))

    unique_entries = []
    seen_numbers = set()
    for number, phone_type in entries:
        number = str(number or "").strip()
        if not number or number in seen_numbers:
            continue
        seen_numbers.add(number)
        unique_entries.append((number, str(phone_type or "").strip()))
    return unique_entries


def extract_email_candidates(candidate):
    emails = []
    for key in ("emails", "email_addresses"):
        for email in candidate.get(key) or []:
            if isinstance(email, dict):
                value = extract_first_nonempty(email, ("email", "address", "value"))
            else:
                value = str(email or "").strip()
            if value:
                emails.append(value)
    emails.extend(
        [
            extract_first_nonempty(
                candidate, ("email_1", "email", "primary_email", "email1", "best_email")
            ),
        ]
    )
    return unique_list(emails)


def extract_mailing_address(candidate):
    for key in ("mailing_address", "mail_address", "address", "property_address"):
        mailing = candidate.get(key)
        if isinstance(mailing, dict):
            return {
                "address": extract_first_nonempty(
                    mailing, ("address", "address1", "line1", "street", "full")
                ),
                "city": extract_first_nonempty(mailing, ("city",)),
                "state": extract_first_nonempty(mailing, ("state",)),
                "zip": extract_first_nonempty(mailing, ("zip", "postal_code", "zipcode")),
            }
        if isinstance(mailing, str) and mailing.strip():
            return {"address": mailing.strip(), "city": "", "state": "", "zip": ""}

    return {
        "address": extract_first_nonempty(
            candidate,
            (
                "mail_address", "address_mail", "mail_address_1", "mailing_address_1",
                "mailing_street", "street"
            ),
        ),
        "city": extract_first_nonempty(candidate, ("mail_city", "mailing_city", "city")),
        "state": extract_first_nonempty(candidate, ("mail_state", "mailing_state", "state")),
        "zip": extract_first_nonempty(
            candidate, ("mail_zip", "mailing_zip", "zip", "postal_code", "zipcode")
        ),
    }


def map_tracerfy_lookup_response(record, response_data):
    payload = extract_nested_payload(response_data)
    person_candidates = extract_person_candidates(payload)

    traced_owner_name = ""
    all_phone_entries = []
    all_email_candidates = []
    mailing_address = {"address": "", "city": "", "state": "", "zip": ""}

    for candidate in person_candidates:
        if not traced_owner_name:
            traced_owner_name = extract_person_name(candidate)

        for number, phone_type in extract_phone_entries(candidate):
            if number not in [n for n, _ in all_phone_entries]:
                all_phone_entries.append((number, phone_type))

        for email in extract_email_candidates(candidate):
            if email not in all_email_candidates:
                all_email_candidates.append(email)

        if not mailing_address["address"]:
            mailing = extract_mailing_address(candidate)
            if any(mailing.values()):
                mailing_address = mailing

    primary_phone = all_phone_entries[0][0] if len(all_phone_entries) > 0 else ""
    primary_type = all_phone_entries[0][1] if len(all_phone_entries) > 0 else ""
    secondary_phone = all_phone_entries[1][0] if len(all_phone_entries) > 1 else ""
    secondary_type = all_phone_entries[1][1] if len(all_phone_entries) > 1 else ""
    tertiary_phone = all_phone_entries[2][0] if len(all_phone_entries) > 2 else ""
    tertiary_type = all_phone_entries[2][1] if len(all_phone_entries) > 2 else ""
    email_primary = all_email_candidates[0] if all_email_candidates else ""

    trace_data = {
        "traced_owner_name": traced_owner_name,
        "traced_owner": traced_owner_name,
        "traced_phones": [n for n, _ in all_phone_entries],
        "traced_phone_types": [t for _, t in all_phone_entries if t],
        "traced_emails": all_email_candidates,
        "phones": [n for n, _ in all_phone_entries],
        "phone_types": [t for _, t in all_phone_entries if t],
        "emails": all_email_candidates,
        "phone_primary": primary_phone,
        "phone_primary_type": primary_type,
        "phone_secondary": secondary_phone,
        "phone_secondary_type": secondary_type,
        "phone_tertiary": tertiary_phone,
        "phone_tertiary_type": tertiary_type,
        "email_primary": email_primary,
        "traced_email": email_primary,
        "traced_mailing_address": mailing_address["address"],
        "traced_mailing_city": mailing_address["city"],
        "traced_mailing_state": mailing_address["state"],
        "traced_mailing_zip": mailing_address["zip"],
        "skip_trace_source": TRACE_SOURCE,
        "skip_trace_timestamp": utc_timestamp(),
    }
    trace_data["has_phone"] = bool(primary_phone or secondary_phone or tertiary_phone)
    trace_data["has_email"] = bool(email_primary)

    had_any_value = any(
        [
            trace_data["traced_owner_name"],
            trace_data["phone_primary"],
            trace_data["phone_secondary"],
            trace_data["phone_tertiary"],
            trace_data["email_primary"],
            trace_data["traced_mailing_address"],
            trace_data["traced_mailing_city"],
            trace_data["traced_mailing_state"],
            trace_data["traced_mailing_zip"],
        ]
    )

    trace_data["skip_trace_status"] = "success" if had_any_value else "no_hit"

    if not str(record.get("owner") or "").strip() and trace_data["traced_owner_name"]:
        trace_data["owner"] = trace_data["traced_owner_name"]

    return apply_trace_field_aliases(apply_derived_lead_fields({**record, **trace_data}))


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
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Origin": "https://208-ndo.github.io",
            "Referer": "https://208-ndo.github.io/akron-summit-scraper/",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Tracerfy lookup failed ({exc.code}): {body[:300]}") from exc
