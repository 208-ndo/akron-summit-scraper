import asyncio
import csv
import io
import json
import logging
import re
import zipfile
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
OUTPUT_JSON_PATHS = [
    DATA_DIR / "records.json",
    DASHBOARD_DIR / "records.json",
]
OUTPUT_CSV_PATH = DATA_DIR / "ghl_export.csv"

LOOKBACK_DAYS = 90
SOURCE_NAME = "Akron / Summit County, Ohio"

CLERK_PORTAL_URL = "https://clerkweb.summitoh.net/"
PROBATE_URL = "https://search.summitohioprobate.com/eservices/"
CAMA_BASE_URL = "https://fiscaloffice.summitoh.net/index.php/documents-a-forms/viewcategory/10-cama"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

LEAD_TYPE_MAP = {
    "LP": "Lis Pendens",
    "NOFC": "Pre-foreclosure",
    "TAXDEED": "Tax Deed",
    "JUD": "Judgment",
    "CCJ": "Certified Judgment",
    "DRJUD": "Domestic Judgment",
    "LNCORPTX": "Corp Tax Lien",
    "LNIRS": "IRS Lien",
    "LNFED": "Federal Lien",
    "LN": "Lien",
    "LNMECH": "Mechanic Lien",
    "LNHOA": "HOA Lien",
    "MEDLN": "Medicaid Lien",
    "PRO": "Probate / Estate",
    "NOC": "Notice of Commencement",
    "RELLP": "Release Lis Pendens",
}

TARGET_DOC_TYPES = set(LEAD_TYPE_MAP.keys())


@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    cat: str = ""
    cat_label: str = ""
    owner: str = ""
    grantee: str = ""
    amount: Optional[float] = None
    legal: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "OH"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""
    clerk_url: str = ""
    flags: List[str] = None
    score: int = 0

    def __post_init__(self):
        if self.flags is None:
            self.flags = []


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)


def log_setup() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def retry_request(url: str, attempts: int = 3, timeout: int = 60) -> requests.Response:
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            logging.warning("Request failed (%s/%s) for %s: %s", attempt, attempts, url, exc)
    raise last_error


def clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def parse_amount(value: str) -> Optional[float]:
    if not value:
        return None
    match = re.sub(r"[^0-9.\-]", "", value)
    if not match:
        return None
    try:
        return float(match)
    except ValueError:
        return None


def normalize_name(name: str) -> str:
    return clean_text(name).upper()


def name_variants(name: str) -> List[str]:
    raw = normalize_name(name)
    if not raw:
        return []
    raw = raw.replace(",", " ")
    parts = [p for p in raw.split() if p]
    if len(parts) < 2:
        return [raw]
    first = parts[0]
    last = parts[-1]
    variants = {
        f"{first} {last}",
        f"{last} {first}",
        f"{last}, {first}",
        raw,
    }
    return [v.strip() for v in variants if v.strip()]


def score_record(record: LeadRecord) -> int:
    score = 30
    flag_points = min(len(record.flags) * 10, 40)
    score += flag_points

    flags_lower = {f.lower() for f in record.flags}
    if "lis pendens" in flags_lower and "pre-foreclosure" in flags_lower:
        score += 20

    if record.amount is not None:
        if record.amount > 100000:
            score += 15
        elif record.amount > 50000:
            score += 10

    if record.filed:
        try:
            filed_dt = date_parser.parse(record.filed)
            if filed_dt.date() >= (datetime.now().date() - timedelta(days=7)):
                score += 5
                if "New this week" not in record.flags:
                    record.flags.append("New this week")
        except Exception:
            pass

    if record.prop_address:
        score += 5

    return min(score, 100)


def category_flags(doc_type: str, owner: str = "") -> List[str]:
    flags = []
    dtype = doc_type.upper().strip()

    if dtype == "LP":
        flags.append("Lis pendens")
    if dtype == "NOFC":
        flags.append("Pre-foreclosure")
    if dtype in {"JUD", "CCJ", "DRJUD"}:
        flags.append("Judgment lien")
    if dtype in {"TAXDEED", "LNCORPTX", "LNIRS", "LNFED"}:
        flags.append("Tax lien")
    if dtype in {"LNMECH"}:
        flags.append("Mechanic lien")
    if dtype == "PRO":
        flags.append("Probate / estate")

    owner_upper = normalize_name(owner)
    if any(x in owner_upper for x in [" LLC", " INC", " CORP", " LTD", " TRUST", " COMPANY"]):
        flags.append("LLC / corp owner")

    return flags


def discover_cama_downloads() -> List[str]:
    """
    Pull the Summit CAMA page and extract downloadable ZIP/TXT links.
    """
    logging.info("Discovering Summit CAMA downloads...")
    response = retry_request(CAMA_BASE_URL)
    soup = BeautifulSoup(response.text, "lxml")
    urls = []

    for link in soup.select("a[href]"):
        href = link.get("href", "")
        text = clean_text(link.get_text(" "))
        if any(key in text.upper() for key in ["SC700", "SC701", "SC702", "SC705", "SC720", "SC731"]):
            if href.startswith("http"):
                urls.append(href)
            else:
                urls.append(requests.compat.urljoin(CAMA_BASE_URL, href))

    unique_urls = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    logging.info("Found %s CAMA file links", len(unique_urls))
    return unique_urls


def read_text_records_from_zip_bytes(content: bytes) -> Dict[str, List[dict]]:
    """
    Reads zipped txt/dat/csv files into a dict keyed by filename.
    Attempts pipe-delimited first, then tab, then comma.
    """
    datasets: Dict[str, List[dict]] = {}

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        for name in zf.namelist():
            lower = name.lower()
            if not lower.endswith((".txt", ".dat", ".csv")):
                continue

            try:
                raw = zf.read(name).decode("utf-8", errors="ignore")
            except Exception:
                continue

            rows = []
            sample = raw[:5000]
            delimiter = "|"
            if sample.count("\t") > sample.count("|"):
                delimiter = "\t"
            elif sample.count(",") > sample.count("|"):
                delimiter = ","

            reader = csv.DictReader(io.StringIO(raw), delimiter=delimiter)
            for row in reader:
                if any(v for v in row.values()):
                    rows.append({clean_text(k): clean_text(v) for k, v in row.items()})

            datasets[name] = rows

    return datasets


def build_parcel_index() -> Dict[str, dict]:
    """
    Download CAMA export files and build owner lookup index.
    """
    index: Dict[str, dict] = {}
    urls = discover_cama_downloads()

    own_rows = []
    mail_rows = []
    legal_rows = []
    parcel_rows = []

    for url in urls:
        try:
            response = retry_request(url)
            datasets = read_text_records_from_zip_bytes(response.content)
            for fname, rows in datasets.items():
                upper = fname.upper()
                if "SC700" in upper:
                    own_rows.extend(rows)
                elif "SC701" in upper:
                    mail_rows.extend(rows)
                elif "SC702" in upper:
                    legal_rows.extend(rows)
                elif "SC705" in upper or "SC731" in upper:
                    parcel_rows.extend(rows)
        except Exception as exc:
            logging.warning("Could not process CAMA file %s: %s", url, exc)

    parcel_by_id: Dict[str, dict] = {}

    def get_pid(row: dict) -> str:
        for key in ["PARID", "PARCELID", "PARCEL_ID", "PARCEL", "PID"]:
            if clean_text(row.get(key)):
                return clean_text(row.get(key))
        return ""

    for row in parcel_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id[pid] = parcel_by_id.get(pid, {})
        parcel_by_id[pid].update({
            "parcel_id": pid,
            "prop_address": clean_text(row.get("SITE_ADDR") or row.get("SITEADDR") or row.get("ADDRESS")),
            "prop_city": clean_text(row.get("SITE_CITY") or row.get("CITY")),
            "prop_zip": clean_text(row.get("SITE_ZIP") or row.get("ZIP")),
        })

    for row in own_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id[pid] = parcel_by_id.get(pid, {})
        owner = clean_text(row.get("OWNER") or row.get("OWN1") or row.get("OWNER_NAME"))
        parcel_by_id[pid].update({"owner": owner})

    for row in mail_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id[pid] = parcel_by_id.get(pid, {})
        parcel_by_id[pid].update({
            "mail_address": clean_text(row.get("ADDR_1") or row.get("MAILADR1") or row.get("MAIL_ADDR")),
            "mail_city": clean_text(row.get("CITY") or row.get("MAILCITY")),
            "mail_state": clean_text(row.get("STATE")),
            "mail_zip": clean_text(row.get("ZIP") or row.get("MAILZIP")),
        })

    for row in legal_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id[pid] = parcel_by_id.get(pid, {})
        parcel_by_id[pid]["legal"] = clean_text(
            row.get("LEGAL") or row.get("LEGAL_DESC") or row.get("LEGALDESCRIPTION")
        )

    for pid, record in parcel_by_id.items():
        owner = record.get("owner", "")
        for variant in name_variants(owner):
            if variant and variant not in index:
                index[variant] = record

    logging.info("Built parcel index with %s owner-name keys", len(index))
    return index


async def scrape_clerk_records() -> List[LeadRecord]:
    """
    Playwright-based starter scraper for Summit clerk site.
    This is built to be resilient and not crash if the page structure changes.
    """
    logging.info("Scraping clerk records...")
    results: List[LeadRecord] = []
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(CLERK_PORTAL_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(3000)

            content = await page.content()
            soup = BeautifulSoup(content, "lxml")

            text = soup.get_text(" ", strip=True)
            if "clerk" not in text.lower() and "court" not in text.lower():
                logging.warning("Clerk page did not load expected content.")
                await browser.close()
                return results

            # Generic link harvesting fallback.
            # This is intentionally broad because Summit may change structure.
            for link in soup.select("a[href]"):
                href = link.get("href", "")
                label = clean_text(link.get_text(" "))
                combined = f"{label} {href}".upper()

                matched_doc_type = None
                for doc_type in TARGET_DOC_TYPES:
                    if doc_type in combined:
                        matched_doc_type = doc_type
                        break

                if not matched_doc_type:
                    continue

                filed = datetime.now().strftime("%Y-%m-%d")
                if datetime.now() < cutoff:
                    continue

                record = LeadRecord(
                    doc_num=clean_text(label)[:80],
                    doc_type=matched_doc_type,
                    filed=filed,
                    cat=matched_doc_type,
                    cat_label=LEAD_TYPE_MAP.get(matched_doc_type, matched_doc_type),
                    owner="",
                    grantee="",
                    amount=None,
                    legal="",
                    clerk_url=requests.compat.urljoin(CLERK_PORTAL_URL, href),
                )
                record.flags = category_flags(record.doc_type, record.owner)
                record.score = score_record(record)
                results.append(record)

        except PlaywrightTimeoutError:
            logging.warning("Timeout while scraping clerk portal.")
        except Exception as exc:
            logging.warning("Clerk scrape failed: %s", exc)
        finally:
            await browser.close()

    deduped = []
    seen = set()
    for r in results:
        key = (r.doc_num, r.doc_type, r.clerk_url)
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    logging.info("Collected %s clerk records", len(deduped))
    return deduped


async def scrape_probate_records() -> List[LeadRecord]:
    logging.info("Scraping probate records...")
    records: List[LeadRecord] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(PROBATE_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(3000)

            content = await page.content()
            soup = BeautifulSoup(content, "lxml")

            links = soup.select("a[href]")
            for idx, link in enumerate(links[:25]):
                href = link.get("href", "")
                label = clean_text(link.get_text(" "))
                if not href:
                    continue

                record = LeadRecord(
                    doc_num=f"PRO-{idx+1}",
                    doc_type="PRO",
                    filed=datetime.now().strftime("%Y-%m-%d"),
                    cat="PRO",
                    cat_label=LEAD_TYPE_MAP["PRO"],
                    owner=label,
                    grantee="",
                    amount=None,
                    legal="",
                    clerk_url=requests.compat.urljoin(PROBATE_URL, href),
                )
                record.flags = category_flags(record.doc_type, record.owner)
                record.score = score_record(record)
                records.append(record)

        except Exception as exc:
            logging.warning("Probate scrape failed: %s", exc)
        finally:
            await browser.close()

    return records


def enrich_with_parcel_data(records: List[LeadRecord], parcel_index: Dict[str, dict]) -> List[LeadRecord]:
    enriched = []

    for record in records:
        try:
            matched = None
            for variant in name_variants(record.owner):
                if variant in parcel_index:
                    matched = parcel_index[variant]
                    break

            if matched:
                record.prop_address = record.prop_address or matched.get("prop_address", "")
                record.prop_city = record.prop_city or matched.get("prop_city", "")
                record.prop_zip = record.prop_zip or matched.get("prop_zip", "")
                record.mail_address = record.mail_address or matched.get("mail_address", "")
                record.mail_city = record.mail_city or matched.get("mail_city", "")
                record.mail_state = record.mail_state or matched.get("mail_state", "")
                record.mail_zip = record.mail_zip or matched.get("mail_zip", "")
                record.legal = record.legal or matched.get("legal", "")

            record.flags = list(dict.fromkeys(record.flags + category_flags(record.doc_type, record.owner)))
            record.score = score_record(record)
            enriched.append(record)
        except Exception as exc:
            logging.warning("Failed to enrich record %s: %s", record.doc_num, exc)
            enriched.append(record)

    return enriched


def dedupe_records(records: List[LeadRecord]) -> List[LeadRecord]:
    final = []
    seen = set()

    for record in records:
        key = (
            clean_text(record.doc_num),
            clean_text(record.doc_type),
            clean_text(record.owner),
            clean_text(record.filed),
        )
        if key in seen:
            continue
        seen.add(key)
        final.append(record)

    return final


def write_json(records: List[LeadRecord]) -> None:
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "date_range": {
            "from": (datetime.now() - timedelta(days=LOOKBACK_DAYS)).date().isoformat(),
            "to": datetime.now().date().isoformat(),
        },
        "total": len(records),
        "with_address": sum(1 for r in records if r.prop_address),
        "records": [asdict(r) for r in records],
    }

    for path in OUTPUT_JSON_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    logging.info("Wrote JSON outputs.")


def split_name(full_name: str) -> Tuple[str, str]:
    parts = clean_text(full_name).split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def write_ghl_csv(records: List[LeadRecord]) -> None:
    OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "First Name",
        "Last Name",
        "Mailing Address",
        "Mailing City",
        "Mailing State",
        "Mailing Zip",
        "Property Address",
        "Property City",
        "Property State",
        "Property Zip",
        "Lead Type",
        "Document Type",
        "Date Filed",
        "Document Number",
        "Amount/Debt Owed",
        "Seller Score",
        "Motivated Seller Flags",
        "Source",
        "Public Records URL",
    ]

    with OUTPUT_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for record in records:
            first, last = split_name(record.owner)
            writer.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": record.mail_address,
                "Mailing City": record.mail_city,
                "Mailing State": record.mail_state,
                "Mailing Zip": record.mail_zip,
                "Property Address": record.prop_address,
                "Property City": record.prop_city,
                "Property State": record.prop_state,
                "Property Zip": record.prop_zip,
                "Lead Type": record.cat_label,
                "Document Type": record.doc_type,
                "Date Filed": record.filed,
                "Document Number": record.doc_num,
                "Amount/Debt Owed": record.amount if record.amount is not None else "",
                "Seller Score": record.score,
                "Motivated Seller Flags": "; ".join(record.flags),
                "Source": SOURCE_NAME,
                "Public Records URL": record.clerk_url,
            })

    logging.info("Wrote GHL export CSV.")


async def main() -> None:
    ensure_dirs()
    log_setup()

    logging.info("Starting Summit County scraper run...")

    parcel_index = build_parcel_index()

    clerk_records = await scrape_clerk_records()
    probate_records = await scrape_probate_records()

    all_records = clerk_records + probate_records
    all_records = enrich_with_parcel_data(all_records, parcel_index)
    all_records = dedupe_records(all_records)
    all_records.sort(key=lambda r: (r.filed, r.score), reverse=True)

    write_json(all_records)
    write_ghl_csv(all_records)

    logging.info("Finished. Total records: %s", len(all_records))


if __name__ == "__main__":
    asyncio.run(main())
