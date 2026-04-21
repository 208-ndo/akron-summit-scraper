"""
GoHighLevel Push Script
=======================
Reads the enriched CSV (after skip tracing) and pushes leads to GHL as contacts.

Workflow:
  1. Export CSV from dashboard (already BatchData-ready format)
  2. Upload to BatchData → get phones back
  3. Download enriched CSV from BatchData
  4. Run: python scraper/ghl_push.py --csv data/skip_traced.csv
  5. Contacts created in GHL with tags + SMS workflow triggered

Setup:
  - Set GHL_API_KEY in GitHub Secrets or .env file
  - Set GHL_LOCATION_ID in GitHub Secrets or .env file
  - Optionally set GHL_WORKFLOW_ID to auto-trigger SMS sequence

Usage:
  python scraper/ghl_push.py --csv data/ghl_export.csv
  python scraper/ghl_push.py --csv data/skip_traced.csv --dry-run
  python scraper/ghl_push.py --csv data/skip_traced.csv --tag "Hot Stack" --limit 50
"""
import argparse, csv, json, logging, os, re, time
from pathlib import Path
from typing import Dict, List, Optional

import requests

# -----------------------------------------------------------------------
# CONFIG — set via environment variables or GitHub Secrets
# -----------------------------------------------------------------------
GHL_API_KEY     = os.getenv("GHL_API_KEY", "")
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID", "")
GHL_WORKFLOW_ID = os.getenv("GHL_WORKFLOW_ID", "")  # optional — triggers SMS sequence

GHL_BASE = "https://services.leadconnectorhq.com"
HEADERS  = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Content-Type":  "application/json",
    "Version":       "2021-07-28",
}

# Rate limit: GHL allows ~10 req/sec. We stay safe at 3/sec.
RATE_LIMIT_DELAY = 0.35  # seconds between requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# -----------------------------------------------------------------------
# GHL TAG MAPPING
# Maps our lead flags → GHL tags (clean labels for your pipeline)
# -----------------------------------------------------------------------
FLAG_TO_TAG = {
    "🔥 Hot Stack":           "Hot Stack",
    "📍 Cross-List Match":    "Cross-List Match",
    "⚡ Sheriff sale scheduled": "Sheriff Sale",
    "Sheriff sale scheduled": "Sheriff Sale",
    "Tax delinquent":         "Tax Delinquent",
    "High tax debt":          "High Tax Debt",
    "Very high tax debt":     "Very High Tax Debt",
    "Absentee owner":         "Absentee Owner",
    "Out-of-state owner":     "Out-of-State Owner",
    "Vacant home":            "Vacant Home",
    "Code violation":         "Code Violation",
    "Probate / estate":       "Probate Estate",
    "Inherited property":     "Inherited Property",
    "Pre-foreclosure":        "Pre-Foreclosure",
    "Lis pendens":            "Lis Pendens",
    "Judgment lien":          "Judgment Lien",
    "🎯 Subject-To Candidate":"Subject-To Candidate",
    "⭐ Prime Subject-To":     "Prime Subject-To",
    "New this week":          "New This Week",
    "In foreclosure":         "In Foreclosure",
}

# Pipeline stage mapping based on lead type
LEAD_TYPE_TO_PIPELINE_STAGE = {
    "Sheriff Sale":      "Urgent - Sheriff Sale",
    "Pre-foreclosure":   "Pre-Foreclosure",
    "Lis Pendens":       "Pre-Foreclosure",
    "Tax Delinquent":    "Tax Delinquent",
    "Probate / Estate":  "Probate",
    "Code Violation":    "Code Violation",
    "Vacant Home":       "Vacant Property",
    "Tax Delinquent":    "Tax Delinquent",
}


def clean(v) -> str:
    if v is None: return ""
    return str(v).strip()


def parse_flags(flags_str: str) -> List[str]:
    if not flags_str: return []
    return [f.strip() for f in flags_str.split(";") if f.strip()]


def flags_to_tags(flags: List[str]) -> List[str]:
    tags = set()
    for f in flags:
        tag = FLAG_TO_TAG.get(f)
        if tag: tags.add(tag)
    return sorted(tags)


def parse_phone(raw: str) -> str:
    """Normalize phone to E.164 format (+1XXXXXXXXXX)."""
    if not raw: return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return ""


def split_name(full_name: str):
    parts = clean(full_name).split()
    if not parts: return "", ""
    if len(parts) == 1: return parts[0], ""
    return parts[0], " ".join(parts[1:])


def build_contact_payload(row: Dict, extra_tags: List[str] = None) -> Optional[dict]:
    """
    Build a GHL contact payload from a CSV row.
    Works with both our native export AND BatchData-enriched export.
    """
    # Name — try dedicated columns first, fall back to splitting full name
    first = clean(row.get("First Name", ""))
    last  = clean(row.get("Last Name", ""))
    if not first and not last:
        first, last = split_name(row.get("Owner", "") or row.get("owner", ""))

    if not first and not last:
        return None  # skip — no name to trace

    # Phone numbers — support both our format and BatchData enriched format
    phones = []
    for col in ["Phone 1","Phone1","phone1","PHONE1","Mobile Phone","Cell Phone",
                "Phone 2","Phone2","Landline","Home Phone"]:
        p = parse_phone(row.get(col, ""))
        if p and p not in phones: phones.append(p)

    # Email
    email = clean(row.get("Email","") or row.get("email","") or row.get("Email Address",""))

    # Addresses
    prop_addr  = clean(row.get("Property Address",""))
    prop_city  = clean(row.get("Property City",""))
    prop_state = clean(row.get("Property State","OH"))
    prop_zip   = clean(row.get("Property Zip",""))
    mail_addr  = clean(row.get("Mailing Address",""))
    mail_city  = clean(row.get("Mailing City",""))
    mail_state = clean(row.get("Mailing State",""))
    mail_zip   = clean(row.get("Mailing Zip",""))

    # Use mailing address as primary contact address (more likely to reach owner)
    contact_address = mail_addr or prop_addr
    contact_city    = mail_city or prop_city
    contact_state   = mail_state or prop_state
    contact_zip     = mail_zip or prop_zip

    # Tags
    flags_str = clean(row.get("Motivated Seller Flags","") or row.get("flags",""))
    flags     = parse_flags(flags_str)
    tags      = flags_to_tags(flags)
    if extra_tags: tags.extend(extra_tags)

    # Lead type for pipeline stage
    lead_type = clean(row.get("Lead Type","") or row.get("cat_label",""))

    # Score for custom field
    score     = clean(row.get("Seller Score","") or row.get("score",""))
    sto_score = clean(row.get("Subject-To Score","") or row.get("subject_to_score",""))
    dist_count= clean(row.get("Distress Count","") or row.get("distress_count",""))
    dist_srcs = clean(row.get("Distress Sources","") or row.get("distress_sources",""))
    doc_num   = clean(row.get("Document Number","") or row.get("doc_num",""))
    amount    = clean(row.get("Amount/Debt Owed","") or row.get("amount",""))
    est_equity= clean(row.get("Est Equity","") or row.get("est_equity",""))
    est_value = clean(row.get("Est Market Value","") or row.get("estimated_value",""))
    parcel_id = clean(row.get("Parcel ID","") or row.get("parcel_id",""))
    pub_url   = clean(row.get("Public Records URL","") or row.get("clerk_url",""))

    # Build notes string
    notes_parts = []
    if prop_addr:   notes_parts.append(f"Property: {prop_addr}, {prop_city}, {prop_state} {prop_zip}")
    if lead_type:   notes_parts.append(f"Lead Type: {lead_type}")
    if doc_num:     notes_parts.append(f"Doc #: {doc_num}")
    if amount:      notes_parts.append(f"Amount Owed: {amount}")
    if est_equity:  notes_parts.append(f"Est. Equity: {est_equity}")
    if est_value:   notes_parts.append(f"Est. Market Value: {est_value}")
    if dist_srcs:   notes_parts.append(f"Distress Sources: {dist_srcs}")
    if parcel_id:   notes_parts.append(f"Parcel ID: {parcel_id}")
    if pub_url:     notes_parts.append(f"Public Record: {pub_url}")

    payload = {
        "firstName":  first.title(),
        "lastName":   last.title(),
        "locationId": GHL_LOCATION_ID,
        "tags":       tags,
        "source":     "Akron Summit County Scraper",
    }

    if email:           payload["email"]   = email
    if contact_address: payload["address1"] = contact_address
    if contact_city:    payload["city"]     = contact_city
    if contact_state:   payload["state"]    = contact_state
    if contact_zip:     payload["postalCode"] = contact_zip

    if phones:
        payload["phone"] = phones[0]  # primary
        # GHL supports additionalPhones
        if len(phones) > 1:
            payload["additionalPhones"] = [{"number": p} for p in phones[1:]]

    if notes_parts:
        payload["customFields"] = [
            {"key": "lead_notes",    "field_value": "\n".join(notes_parts)},
            {"key": "seller_score",  "field_value": score},
            {"key": "subject_to_score", "field_value": sto_score},
            {"key": "distress_count","field_value": dist_count},
            {"key": "lead_type",     "field_value": lead_type},
            {"key": "parcel_id",     "field_value": parcel_id},
            {"key": "prop_address",  "field_value": prop_addr},
            {"key": "public_record_url", "field_value": pub_url},
        ]

    return payload


def upsert_contact(payload: dict, dry_run: bool = False) -> Optional[str]:
    """Create or update a contact in GHL. Returns contact ID."""
    if dry_run:
        logging.info("DRY RUN: Would create contact: %s %s | Tags: %s",
                     payload.get("firstName"), payload.get("lastName"), payload.get("tags"))
        return "dry-run-id"

    url = f"{GHL_BASE}/contacts/"
    try:
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        if resp.status_code in (200, 201):
            data = resp.json()
            contact_id = data.get("contact", {}).get("id") or data.get("id")
            logging.info("✅ Created contact: %s %s | ID: %s | Tags: %s",
                         payload.get("firstName"), payload.get("lastName"),
                         contact_id, payload.get("tags"))
            return contact_id
        elif resp.status_code == 422:
            # Contact may already exist — try to find and update
            return upsert_by_phone_or_email(payload, dry_run)
        else:
            logging.warning("GHL error %s for %s %s: %s",
                            resp.status_code, payload.get("firstName"),
                            payload.get("lastName"), resp.text[:200])
            return None
    except Exception as e:
        logging.warning("GHL request failed: %s", e)
        return None


def upsert_by_phone_or_email(payload: dict, dry_run: bool = False) -> Optional[str]:
    """Look up existing contact by phone/email and update tags."""
    if dry_run: return "dry-run-id"
    phone = payload.get("phone","")
    email = payload.get("email","")
    search_val = phone or email
    if not search_val: return None

    try:
        search_url = f"{GHL_BASE}/contacts/search/duplicate"
        params = {"locationId": GHL_LOCATION_ID}
        if phone: params["phone"] = phone
        if email: params["email"] = email
        resp = requests.get(search_url, headers=HEADERS, params=params, timeout=20)
        if resp.status_code == 200:
            contacts = resp.json().get("contacts", [])
            if contacts:
                cid = contacts[0].get("id")
                # Update existing contact with new tags
                update_url = f"{GHL_BASE}/contacts/{cid}"
                existing_tags = contacts[0].get("tags", [])
                merged_tags = list(set(existing_tags + payload.get("tags", [])))
                requests.put(update_url, headers=HEADERS,
                             json={"tags": merged_tags}, timeout=20)
                logging.info("↻ Updated existing contact %s with tags %s", cid, merged_tags)
                return cid
    except Exception as e:
        logging.warning("Lookup failed: %s", e)
    return None


def trigger_workflow(contact_id: str, dry_run: bool = False):
    """Trigger a GHL workflow (SMS sequence) for a contact."""
    if not GHL_WORKFLOW_ID: return
    if dry_run:
        logging.info("DRY RUN: Would trigger workflow %s for contact %s", GHL_WORKFLOW_ID, contact_id)
        return
    try:
        url = f"{GHL_BASE}/contacts/{contact_id}/workflow/{GHL_WORKFLOW_ID}"
        resp = requests.post(url, headers=HEADERS, timeout=20)
        if resp.status_code in (200, 201):
            logging.info("🚀 Triggered workflow for contact %s", contact_id)
        else:
            logging.warning("Workflow trigger failed %s: %s", resp.status_code, resp.text[:100])
    except Exception as e:
        logging.warning("Workflow trigger error: %s", e)


def push_to_ghl(csv_path: Path, dry_run: bool = False,
                filter_tag: str = "", limit: int = 0,
                trigger_sms: bool = False) -> dict:
    """
    Main function — reads CSV and pushes to GHL.

    Args:
        csv_path:    Path to enriched CSV
        dry_run:     If True, just log what would happen — don't actually push
        filter_tag:  Only push rows containing this flag (e.g. "Hot Stack")
        limit:       Max records to push (0 = all)
        trigger_sms: If True, trigger GHL workflow after creating contact
    """
    stats = {"total": 0, "pushed": 0, "skipped": 0, "errors": 0, "no_name": 0}

    if not dry_run and not GHL_API_KEY:
        logging.error("GHL_API_KEY not set. Run with --dry-run or set the environment variable.")
        return stats

    if not dry_run and not GHL_LOCATION_ID:
        logging.error("GHL_LOCATION_ID not set.")
        return stats

    logging.info("Reading CSV: %s", csv_path)
    rows = []
    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        logging.error("Could not read CSV: %s", e)
        return stats

    logging.info("Total rows in CSV: %s", len(rows))

    # Filter
    if filter_tag:
        rows = [r for r in rows if filter_tag.lower() in
                (r.get("Motivated Seller Flags","") or r.get("flags","")).lower()]
        logging.info("After filter '%s': %s rows", filter_tag, len(rows))

    if limit and limit > 0:
        rows = rows[:limit]
        logging.info("Limited to %s rows", len(rows))

    for i, row in enumerate(rows):
        stats["total"] += 1
        payload = build_contact_payload(row)
        if not payload:
            stats["no_name"] += 1
            continue

        contact_id = upsert_contact(payload, dry_run=dry_run)
        if contact_id:
            stats["pushed"] += 1
            if trigger_sms and contact_id != "dry-run-id":
                trigger_workflow(contact_id, dry_run=dry_run)
        else:
            stats["errors"] += 1

        # Rate limiting
        if not dry_run:
            time.sleep(RATE_LIMIT_DELAY)

        # Progress log every 25
        if (i+1) % 25 == 0:
            logging.info("Progress: %s/%s | Pushed: %s | Errors: %s",
                         i+1, len(rows), stats["pushed"], stats["errors"])

    logging.info("=== GHL Push Complete ===")
    logging.info("Total: %s | Pushed: %s | Skipped: %s | No Name: %s | Errors: %s",
                 stats["total"], stats["pushed"], stats["skipped"],
                 stats["no_name"], stats["errors"])
    return stats


def main():
    p = argparse.ArgumentParser(description="Push leads to GoHighLevel CRM")
    p.add_argument("--csv", required=True, help="Path to enriched CSV file")
    p.add_argument("--dry-run", action="store_true", help="Log what would happen without pushing")
    p.add_argument("--tag", default="", help="Only push rows with this flag (e.g. 'Hot Stack')")
    p.add_argument("--limit", type=int, default=0, help="Max records to push (0=all)")
    p.add_argument("--trigger-sms", action="store_true", help="Trigger GHL workflow after creating contact")
    p.add_argument("--api-key", default="", help="GHL API key (overrides GHL_API_KEY env var)")
    p.add_argument("--location-id", default="", help="GHL Location ID (overrides GHL_LOCATION_ID env var)")
    p.add_argument("--workflow-id", default="", help="GHL Workflow ID for SMS trigger")
    args = p.parse_args()

    # Allow overriding via CLI args
    global GHL_API_KEY, GHL_LOCATION_ID, GHL_WORKFLOW_ID, HEADERS
    if args.api_key:      GHL_API_KEY = args.api_key
    if args.location_id:  GHL_LOCATION_ID = args.location_id
    if args.workflow_id:  GHL_WORKFLOW_ID = args.workflow_id
    HEADERS["Authorization"] = f"Bearer {GHL_API_KEY}"

    push_to_ghl(
        csv_path=Path(args.csv),
        dry_run=args.dry_run,
        filter_tag=args.tag,
        limit=args.limit,
        trigger_sms=args.trigger_sms,
    )

if __name__ == "__main__":
    main()
