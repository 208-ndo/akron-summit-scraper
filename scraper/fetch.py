import asyncio
import csv
import io
import json
import logging
import re
import zipfile
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
DEBUG_DIR = DATA_DIR / "debug"

OUTPUT_JSON_PATHS = [
    DATA_DIR / "records.json",
    DASHBOARD_DIR / "records.json",
]
OUTPUT_CSV_PATH = DATA_DIR / "ghl_export.csv"

LOOKBACK_DAYS = 90
SOURCE_NAME = "Akron / Summit County, Ohio"

CLERK_PORTAL_URL = "https://clerkweb.summitoh.net/"
CLERK_CASE_SEARCH_URL = "https://clerkweb.summitoh.net/record-search"
PROBATE_URL = "https://search.summitohioprobate.com/eservices/"
CAMA_PAGE_URL = "https://fiscaloffice.summitoh.net/index.php/documents-a-forms/viewcategory/10-cama"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
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
TARGET_DOC_TYPES_SORTED = sorted(TARGET_DOC_TYPES, key=len, reverse=True)

LIKELY_OWNER_KEYS = [
    "OWNER", "OWN1", "OWNER_NAME", "OWNERNAME", "OWNERNM", "NAME", "OWNNAM"
]
LIKELY_PROP_ADDR_KEYS = [
    "SITE_ADDR", "SITEADDR", "PROPERTY_ADDRESS", "PROPADDR", "ADDRESS", "LOCADDR", "SADDR"
]
LIKELY_PROP_CITY_KEYS = [
    "SITE_CITY", "CITY", "SITECITY", "PROPERTY_CITY", "SCITY"
]
LIKELY_PROP_ZIP_KEYS = [
    "SITE_ZIP", "ZIP", "SITEZIP", "PROPERTY_ZIP", "SZIP"
]
LIKELY_MAIL_ADDR_KEYS = [
    "ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILADDRESS", "MADDR1", "ADDRESS1", "MAILADD1"
]
LIKELY_MAIL_CITY_KEYS = [
    "MAILCITY", "CITY", "MCITY"
]
LIKELY_MAIL_STATE_KEYS = [
    "STATE", "MAILSTATE", "MSTATE"
]
LIKELY_MAIL_ZIP_KEYS = [
    "MAILZIP", "ZIP", "MZIP"
]
LIKELY_LEGAL_KEYS = [
    "LEGAL", "LEGAL_DESC", "LEGALDESCRIPTION", "LEGDESC"
]
LIKELY_PID_KEYS = [
    "PARID", "PARCELID", "PARCEL_ID", "PARCEL", "PID", "PARCELNO", "PAR_NO", "PAR_NUM"
]

IGNORE_TEXT_FRAGMENTS = [
    "skip to main content",
    "home currently selected",
    "click here",
    "wcag",
    "accessibility",
    "civil protection orders",
    "supreme court",
    "department of taxation",
    "marriage application",
    "common pleas",
    "domestic relations",
    "the county of summit",
]


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
    flags: List[str] = field(default_factory=list)
    score: int = 0


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def log_setup() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def save_debug_text(name: str, content: str) -> None:
    try:
        (DEBUG_DIR / name).write_text(content, encoding="utf-8")
    except Exception as exc:
        logging.warning("Could not write debug text %s: %s", name, exc)


def save_debug_json(name: str, payload) -> None:
    try:
        (DEBUG_DIR / name).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as exc:
        logging.warning("Could not write debug json %s: %s", name, exc)


def clean_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def retry_request(url: str, attempts: int = 3, timeout: int = 60) -> requests.Response:
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_error = exc
            logging.warning("Request failed (%s/%s) for %s: %s", attempt, attempts, url, exc)
    raise last_error


def normalize_name(name: str) -> str:
    name = clean_text(name).upper()
    name = re.sub(r"[^A-Z0-9,&.\- ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def name_variants(name: str) -> List[str]:
    raw = normalize_name(name)
    if not raw:
        return []

    raw_nocomma = raw.replace(",", " ")
    parts = [p for p in raw_nocomma.split() if p]
    variants = {raw}

    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
        variants.add(f"{first} {last}")
        variants.add(f"{last} {first}")
        variants.add(f"{last}, {first}")

    return [v.strip() for v in variants if v.strip()]


def parse_amount(value: str) -> Optional[float]:
    value = clean_text(value)
    if not value:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def safe_pick(row: dict, keys: List[str]) -> str:
    for key in keys:
        if key in row and clean_text(row.get(key)):
            return clean_text(row.get(key))
    upper_map = {str(k).upper(): k for k in row.keys()}
    for key in keys:
        if key.upper() in upper_map:
            real_key = upper_map[key.upper()]
            val = clean_text(row.get(real_key))
            if val:
                return val
    return ""


def get_pid(row: dict) -> str:
    return safe_pick(row, LIKELY_PID_KEYS)


def category_flags(doc_type: str, owner: str = "") -> List[str]:
    flags: List[str] = []
    dt = clean_text(doc_type).upper()
    owner_upper = normalize_name(owner)

    if dt == "LP":
        flags.append("Lis pendens")
    if dt == "NOFC":
        flags.append("Pre-foreclosure")
    if dt in {"JUD", "CCJ", "DRJUD"}:
        flags.append("Judgment lien")
    if dt in {"TAXDEED", "LNCORPTX", "LNIRS", "LNFED"}:
        flags.append("Tax lien")
    if dt == "LNMECH":
        flags.append("Mechanic lien")
    if dt == "PRO":
        flags.append("Probate / estate")

    corp_terms = [" LLC", " INC", " CORP", " CO ", " COMPANY", " TRUST", " LP", " LTD"]
    if any(term in f" {owner_upper} " for term in corp_terms):
        flags.append("LLC / corp owner")

    return list(dict.fromkeys(flags))


def score_record(record: LeadRecord) -> int:
    score = 30
    score += min(len(record.flags) * 10, 40)

    lower_flags = {flag.lower() for flag in record.flags}
    if "lis pendens" in lower_flags and "pre-foreclosure" in lower_flags:
        score += 20

    if record.amount is not None:
        if record.amount > 100000:
            score += 15
        elif record.amount > 50000:
            score += 10

    if record.filed:
        try:
            filed_dt = datetime.fromisoformat(record.filed)
            if filed_dt.date() >= (datetime.now().date() - timedelta(days=7)):
                if "New this week" not in record.flags:
                    record.flags.append("New this week")
                score += 5
        except Exception:
            pass

    if record.prop_address:
        score += 5

    return min(score, 100)


def discover_cama_downloads() -> List[str]:
    logging.info("Discovering Summit CAMA downloads...")
    response = retry_request(CAMA_PAGE_URL)
    soup = BeautifulSoup(response.text, "lxml")

    urls: List[str] = []
    wanted_codes = ["SC700", "SC701", "SC702", "SC705", "SC720", "SC731"]

    for link in soup.select("a[href]"):
        href = clean_text(link.get("href"))
        text = clean_text(link.get_text(" ")).upper()
        blob = f"{href} {text}".upper()

        if any(code in blob for code in wanted_codes):
            full_url = requests.compat.urljoin(CAMA_PAGE_URL, href)
            urls.append(full_url)

    deduped = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)

    logging.info("Found %s CAMA file links", len(deduped))
    save_debug_json("cama_links.json", deduped)
    return deduped


def looks_like_zip(content: bytes) -> bool:
    return len(content) >= 4 and content[:2] == b"PK"


def split_lines(text: str) -> List[str]:
    return [line.rstrip("\r") for line in text.splitlines() if clean_text(line)]


def choose_delimiter(sample_text: str) -> str:
    candidates = ["|", "\t", ","]
    counts = {d: sample_text.count(d) for d in candidates}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else "|"


def parse_delimited_text(raw_text: str) -> List[dict]:
    lines = split_lines(raw_text)
    if len(lines) < 2:
        return []

    sample = "\n".join(lines[:10])
    delim = choose_delimiter(sample)

    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter=delim)
    rows = []
    for row in reader:
        cleaned = {clean_text(k): clean_text(v) for k, v in row.items() if k is not None}
        if any(cleaned.values()):
            rows.append(cleaned)
    return rows


def parse_fixed_width_fallback(raw_text: str) -> List[dict]:
    lines = split_lines(raw_text)
    if not lines:
        return []
    rows = []
    for idx, line in enumerate(lines[:5000], start=1):
        rows.append({"RAW_LINE": clean_text(line), "ROW_NUM": str(idx)})
    return rows


def read_any_cama_payload(content: bytes, source_name: str) -> Dict[str, List[dict]]:
    datasets: Dict[str, List[dict]] = {}

    if looks_like_zip(content):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for member in zf.namelist():
                if member.endswith("/"):
                    continue
                try:
                    raw = zf.read(member).decode("utf-8", errors="ignore")
                except Exception:
                    continue

                parsed = parse_delimited_text(raw)
                if not parsed:
                    parsed = parse_fixed_width_fallback(raw)
                datasets[member] = parsed
        return datasets

    raw_text = content.decode("utf-8", errors="ignore")
    parsed = parse_delimited_text(raw_text)
    if not parsed:
        parsed = parse_fixed_width_fallback(raw_text)
    datasets[source_name] = parsed
    return datasets


def debug_dataset_rows(label: str, rows: List[dict]) -> None:
    sample = rows[:5]
    keys = sorted({k for row in sample for k in row.keys()})
    save_debug_json(f"{label}_sample_rows.json", sample)
    save_debug_json(f"{label}_sample_keys.json", keys)


def build_parcel_index() -> Dict[str, dict]:
    urls = discover_cama_downloads()

    own_rows: List[dict] = []
    mail_rows: List[dict] = []
    legal_rows: List[dict] = []
    parcel_rows: List[dict] = []

    for url in urls:
        try:
            response = retry_request(url)
            datasets = read_any_cama_payload(response.content, Path(url).name)

            for fname, rows in datasets.items():
                upper = fname.upper()

                if "SC700" in upper:
                    own_rows.extend(rows)
                    debug_dataset_rows("sc700_owndat", rows)
                elif "SC701" in upper:
                    mail_rows.extend(rows)
                    debug_dataset_rows("sc701_maildat", rows)
                elif "SC702" in upper:
                    legal_rows.extend(rows)
                    debug_dataset_rows("sc702_legdat", rows)
                elif "SC705" in upper or "SC731" in upper:
                    parcel_rows.extend(rows)
                    debug_dataset_rows("sc705_sc731_parcel", rows)

            logging.info("Loaded CAMA source %s", url)
        except Exception as exc:
            logging.warning("Could not process CAMA file %s: %s", url, exc)

    parcel_by_id: Dict[str, dict] = {}

    for row in parcel_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id.setdefault(pid, {})
        parcel_by_id[pid].update({
            "parcel_id": pid,
            "prop_address": safe_pick(row, LIKELY_PROP_ADDR_KEYS),
            "prop_city": safe_pick(row, LIKELY_PROP_CITY_KEYS),
            "prop_zip": safe_pick(row, LIKELY_PROP_ZIP_KEYS),
        })

    for row in own_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id.setdefault(pid, {})
        parcel_by_id[pid].update({
            "parcel_id": pid,
            "owner": safe_pick(row, LIKELY_OWNER_KEYS),
        })

    for row in mail_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id.setdefault(pid, {})
        parcel_by_id[pid].update({
            "parcel_id": pid,
            "mail_address": safe_pick(row, LIKELY_MAIL_ADDR_KEYS),
            "mail_city": safe_pick(row, LIKELY_MAIL_CITY_KEYS),
            "mail_state": safe_pick(row, LIKELY_MAIL_STATE_KEYS),
            "mail_zip": safe_pick(row, LIKELY_MAIL_ZIP_KEYS),
        })

    for row in legal_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id.setdefault(pid, {})
        parcel_by_id[pid].update({
            "parcel_id": pid,
            "legal": safe_pick(row, LIKELY_LEGAL_KEYS),
        })

    sample_parcel_records = list(parcel_by_id.values())[:10]
    save_debug_json("parcel_by_id_sample.json", sample_parcel_records)

    owner_index: Dict[str, dict] = {}
    owners_seen = []
    for record in parcel_by_id.values():
        owner = clean_text(record.get("owner"))
        if not owner:
            continue
        owners_seen.append(owner)
        for variant in name_variants(owner):
            owner_index.setdefault(variant, record)

    save_debug_json("owner_values_sample.json", owners_seen[:50])
    save_debug_json("owner_index_sample.json", list(owner_index.items())[:20])

    logging.info(
        "Built parcel index with %s owner-name keys from %s parcel rows / %s owner rows / %s mail rows / %s legal rows",
        len(owner_index), len(parcel_rows), len(own_rows), len(mail_rows), len(legal_rows)
    )
    return owner_index


def try_parse_date(text: str) -> Optional[str]:
    text = clean_text(text)
    if not text:
        return None

    patterns = [
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
        r"\b\d{1,2}-\d{1,2}-\d{2,4}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            raw = match.group(0)
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
                try:
                    return datetime.strptime(raw, fmt).date().isoformat()
                except ValueError:
                    continue
    return None


def extract_doc_type(text: str) -> Optional[str]:
    text_u = clean_text(text).upper()
    for doc_type in TARGET_DOC_TYPES_SORTED:
        if re.search(rf"\b{re.escape(doc_type)}\b", text_u):
            return doc_type
    return None


def text_is_noise(text: str) -> bool:
    t = clean_text(text).lower()
    if not t:
        return True
    if len(t) < 4:
        return True
    return any(fragment in t for fragment in IGNORE_TEXT_FRAGMENTS)


def parse_candidate_text_to_record(text: str, href: str, base_url: str, fallback_doc_num: str) -> Optional[LeadRecord]:
    text = clean_text(text)
    if text_is_noise(text):
        return None

    doc_type = extract_doc_type(text)
    if not doc_type:
        return None

    filed = try_parse_date(text) or datetime.now().date().isoformat()
    cutoff = datetime.now().date() - timedelta(days=LOOKBACK_DAYS)
    if datetime.fromisoformat(filed).date() < cutoff:
        return None

    amount_match = re.search(r"\$[\d,]+(?:\.\d{2})?", text)
    amount = parse_amount(amount_match.group(0)) if amount_match else None

    doc_num_match = re.search(r"\b\d{5,}\b", text)
    doc_num = doc_num_match.group(0) if doc_num_match else fallback_doc_num

    record = LeadRecord(
        doc_num=doc_num,
        doc_type=doc_type,
        filed=filed,
        cat=doc_type,
        cat_label=LEAD_TYPE_MAP.get(doc_type, doc_type),
        owner="",
        grantee="",
        amount=amount,
        legal="",
        clerk_url=requests.compat.urljoin(base_url, href) if href else base_url,
    )
    record.flags = category_flags(record.doc_type, record.owner)
    record.score = score_record(record)
    return record


async def scrape_clerk_records() -> List[LeadRecord]:
    logging.info("Scraping clerk records...")
    records: List[LeadRecord] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            urls_to_try = [CLERK_CASE_SEARCH_URL, CLERK_PORTAL_URL]

            for idx, url in enumerate(urls_to_try, start=1):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(4000)

                    title = await page.title()
                    html = await page.content()
                    save_debug_text(f"clerk_page_{idx}.html", html)
                    logging.info("Clerk page %s title: %s", idx, title)

                    soup = BeautifulSoup(html, "lxml")

                    rows = soup.find_all("tr")
                    for i, row in enumerate(rows):
                        text = clean_text(row.get_text(" "))
                        link = row.find("a", href=True)
                        href = clean_text(link.get("href")) if link else ""
                        rec = parse_candidate_text_to_record(text, href, url, f"CLK-TR-{idx}-{i+1}")
                        if rec:
                            records.append(rec)

                    links = soup.find_all("a", href=True)
                    for i, link in enumerate(links):
                        text = clean_text(link.get_text(" "))
                        href = clean_text(link.get("href"))
                        rec = parse_candidate_text_to_record(text, href, url, f"CLK-A-{idx}-{i+1}")
                        if rec:
                            records.append(rec)

                    blocks = soup.find_all(["div", "li", "span"])
                    for i, block in enumerate(blocks[:2500]):
                        text = clean_text(block.get_text(" "))
                        inner_link = block.find("a", href=True)
                        href = clean_text(inner_link.get("href")) if inner_link else ""
                        rec = parse_candidate_text_to_record(text, href, url, f"CLK-B-{idx}-{i+1}")
                        if rec:
                            records.append(rec)

                except Exception as inner_exc:
                    logging.warning("Clerk attempt failed for %s: %s", url, inner_exc)

        except PlaywrightTimeoutError:
            logging.warning("Timeout while scraping clerk portal.")
        except Exception as exc:
            logging.warning("Clerk scrape failed: %s", exc)
        finally:
            await browser.close()

    deduped: List[LeadRecord] = []
    seen = set()
    for record in records:
        key = (record.doc_num, record.doc_type, record.clerk_url)
        if key not in seen:
            seen.add(key)
            deduped.append(record)

    logging.info("Collected %s clerk records", len(deduped))
    return deduped


def valid_probate_candidate(text: str) -> bool:
    t = clean_text(text)
    t_u = t.upper()
    if not t or text_is_noise(t):
        return False

    good_contains = [
        "ESTATE OF",
        "IN THE MATTER OF",
        "GUARDIANSHIP OF",
        "DECEDENT",
        "FIDUCIARY",
        "ESTATE",
    ]
    return any(x in t_u for x in good_contains)


async def scrape_probate_records() -> List[LeadRecord]:
    logging.info("Scraping probate records...")
    records: List[LeadRecord] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(PROBATE_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(4000)

            title = await page.title()
            html = await page.content()
            save_debug_text("probate_page_1.html", html)
            logging.info("Probate page title: %s", title)

            soup = BeautifulSoup(html, "lxml")

            rows = soup.find_all("tr")
            for i, row in enumerate(rows):
                text = clean_text(row.get_text(" "))
                if not valid_probate_candidate(text):
                    continue
                link = row.find("a", href=True)
                href = clean_text(link.get("href")) if link else ""
                filed = try_parse_date(text) or datetime.now().date().isoformat()

                record = LeadRecord(
                    doc_num=f"PRO-TR-{i+1}",
                    doc_type="PRO",
                    filed=filed,
                    cat="PRO",
                    cat_label=LEAD_TYPE_MAP["PRO"],
                    owner=text[:180],
                    grantee="",
                    amount=None,
                    legal="",
                    clerk_url=requests.compat.urljoin(PROBATE_URL, href) if href else PROBATE_URL,
                )
                record.flags = category_flags(record.doc_type, record.owner)
                record.score = score_record(record)
                records.append(record)

            links = soup.find_all("a", href=True)
            for i, link in enumerate(links):
                text = clean_text(link.get_text(" "))
                if not valid_probate_candidate(text):
                    continue
                href = clean_text(link.get("href"))
                filed = try_parse_date(text) or datetime.now().date().isoformat()

                record = LeadRecord(
                    doc_num=f"PRO-A-{i+1}",
                    doc_type="PRO",
                    filed=filed,
                    cat="PRO",
                    cat_label=LEAD_TYPE_MAP["PRO"],
                    owner=text[:180],
                    grantee="",
                    amount=None,
                    legal="",
                    clerk_url=requests.compat.urljoin(PROBATE_URL, href) if href else PROBATE_URL,
                )
                record.flags = category_flags(record.doc_type, record.owner)
                record.score = score_record(record)
                records.append(record)

        except Exception as exc:
            logging.warning("Probate scrape failed: %s", exc)
        finally:
            await browser.close()

    deduped: List[LeadRecord] = []
    seen = set()
    for record in records:
        key = (record.doc_num, record.owner, record.clerk_url)
        if key not in seen:
            seen.add(key)
            deduped.append(record)

    logging.info("Collected %s probate records", len(deduped))
    return deduped


def enrich_with_parcel_data(records: List[LeadRecord], parcel_index: Dict[str, dict]) -> List[LeadRecord]:
    enriched: List[LeadRecord] = []

    for record in records:
        try:
            matched = None
            for variant in name_variants(record.owner):
                if variant in parcel_index:
                    matched = parcel_index[variant]
                    break

            if matched:
                record.prop_address = record.prop_address or clean_text(matched.get("prop_address"))
                record.prop_city = record.prop_city or clean_text(matched.get("prop_city"))
                record.prop_zip = record.prop_zip or clean_text(matched.get("prop_zip"))
                record.mail_address = record.mail_address or clean_text(matched.get("mail_address"))
                record.mail_city = record.mail_city or clean_text(matched.get("mail_city"))
                record.mail_state = record.mail_state or clean_text(matched.get("mail_state"))
                record.mail_zip = record.mail_zip or clean_text(matched.get("mail_zip"))
                record.legal = record.legal or clean_text(matched.get("legal"))

            record.flags = list(dict.fromkeys(record.flags + category_flags(record.doc_type, record.owner)))
            record.score = score_record(record)
            enriched.append(record)
        except Exception as exc:
            logging.warning("Failed to enrich record %s: %s", record.doc_num, exc)
            enriched.append(record)

    return enriched


def dedupe_records(records: List[LeadRecord]) -> List[LeadRecord]:
    final: List[LeadRecord] = []
    seen = set()

    for record in records:
        key = (
            clean_text(record.doc_num),
            clean_text(record.doc_type),
            clean_text(record.owner),
            clean_text(record.filed),
            clean_text(record.clerk_url),
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
        "with_address": sum(1 for record in records if clean_text(record.prop_address)),
        "records": [asdict(record) for record in records],
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

    with OUTPUT_CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
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
    all_records.sort(key=lambda record: (record.filed, record.score, record.doc_num), reverse=True)

    write_json(all_records)
    write_ghl_csv(all_records)

    logging.info("Finished. Total records: %s", len(all_records))


if __name__ == "__main__":
    asyncio.run(main())
