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

CLERK_RECORDS_URL = "https://clerk.summitoh.net/RecordsSearch/Disclaimer.asp?toPage=SelectDivision.asp"
PENDING_CIVIL_URL = "https://newcivilfilings.summitoh.net/"
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

LIKELY_OWNER_KEYS = [
    "OWNER1", "OWNER2", "OWNER", "OWN1", "OWNER_NAME", "OWNERNAME", "OWNERNM", "NAME", "OWNNAM"
]
LIKELY_PROP_ADDR_KEYS = [
    "SITE_ADDR", "SITEADDR", "PROPERTY_ADDRESS", "PROPADDR", "ADDRESS", "LOCADDR", "SADDR",
    "ADDRESS_1", "ADDRESS_2"
]
LIKELY_PROP_CITY_KEYS = [
    "SITE_CITY", "CITY", "SITECITY", "PROPERTY_CITY", "SCITY", "CITYNAME"
]
LIKELY_PROP_ZIP_KEYS = [
    "SITE_ZIP", "ZIP", "SITEZIP", "PROPERTY_ZIP", "SZIP"
]
LIKELY_MAIL_ADDR_KEYS = [
    "ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILADDRESS", "MADDR1", "ADDRESS1", "MAILADD1",
    "ADDRESS_1", "ADDRESS_2"
]
LIKELY_MAIL_CITY_KEYS = [
    "MAILCITY", "CITY", "MCITY", "CITYNAME"
]
LIKELY_MAIL_STATE_KEYS = [
    "STATE", "MAILSTATE", "MSTATE", "STATECODE"
]
LIKELY_MAIL_ZIP_KEYS = [
    "MAILZIP", "ZIP", "MZIP", "OWNER_ZIPCD1", "OWNER_ZIPCD2"
]
LIKELY_LEGAL_KEYS = [
    "LEGAL", "LEGAL_DESC", "LEGALDESCRIPTION", "LEGDESC"
]
LIKELY_PID_KEYS = [
    "PAIRD", "PARID", "PARCELID", "PARCEL_ID", "PARCEL", "PID", "PARCELNO", "PAR_NO", "PAR_NUM"
]

BAD_EXACT_OWNERS = {
    "Action", "Get Docs", "Date Added", "Party", "Plaintiff", "Defendant",
    "Search", "Home", "Select Division", "Welcome"
}

STATE_CODES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
    "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK",
    "OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"
}


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


def normalize_state(value: str) -> str:
    v = clean_text(value).upper()
    if not v or v in {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "00", "000", "-", "N/A", "NA", "NONE", "NULL"}:
        return ""
    v = re.sub(r"[^A-Z]", "", v)
    return v if v in STATE_CODES else ""


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
    name = re.sub(r"[^A-Z0-9,&.\- /']", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def build_owner_name(row: dict) -> str:
    owner1 = clean_text(row.get("OWNER1"))
    owner2 = clean_text(row.get("OWNER2"))
    if owner1 and owner2:
        return f"{owner1} {owner2}".strip()
    if owner1:
        return owner1
    if owner2:
        return owner2
    return safe_pick(row, LIKELY_OWNER_KEYS)


def build_mail_zip(row: dict) -> str:
    z1 = clean_text(row.get("OWNER_ZIPCD1"))
    z2 = clean_text(row.get("OWNER_ZIPCD2"))
    if z1 and z2:
        return f"{z1}-{z2}"
    if z1:
        return z1
    return safe_pick(row, LIKELY_MAIL_ZIP_KEYS)


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

        if len(parts) >= 3:
            middle = " ".join(parts[1:-1])
            variants.add(f"{first} {middle} {last}".strip())
            variants.add(f"{last}, {first} {middle}".strip())

        if len(parts) >= 3:
            variants.add(f"{parts[-1]} {parts[0]}")
            variants.add(f"{parts[0]} {parts[-1]}")

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

    corp_terms = [" LLC", " INC", " CORP", " CO ", " COMPANY", " TRUST", " LP", " LTD", " BANK "]
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


def build_prop_address_from_row(row: dict) -> str:
    adrno = clean_text(row.get("ADRNO"))
    adradd = clean_text(row.get("ADRADD"))
    adrdir = clean_text(row.get("ADRDIR"))
    adrstr = clean_text(row.get("ADRSTR"))
    adrsuf = clean_text(row.get("ADRSUF"))
    adrsuf2 = clean_text(row.get("ADRSUF2"))

    parts = [adrno, adradd, adrdir, adrstr, adrsuf, adrsuf2]
    address = " ".join(part for part in parts if part).strip()
    return re.sub(r"\s+", " ", address)


def build_prop_city_from_row(row: dict) -> str:
    return (
        clean_text(row.get("UDATE1"))
        or clean_text(row.get("CITY"))
        or safe_pick(row, LIKELY_PROP_CITY_KEYS)
    )


def build_prop_zip_from_row(row: dict) -> str:
    direct = clean_text(row.get("USER2"))
    if re.fullmatch(r"\d{5}", direct):
        return direct

    zip_raw = clean_text(row.get("ZIPCD"))
    m = re.search(r"(\d{5})", zip_raw)
    if m:
        return m.group(1)

    fallback = safe_pick(row, LIKELY_PROP_ZIP_KEYS)
    m2 = re.search(r"(\d{5})", fallback)
    return m2.group(1) if m2 else ""


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
                elif "SC701" in upper:
                    mail_rows.extend(rows)
                elif "SC702" in upper:
                    legal_rows.extend(rows)
                elif "SC705" in upper or "SC731" in upper:
                    parcel_rows.extend(rows)

            logging.info("Loaded CAMA source %s", url)
        except Exception as exc:
            logging.warning("Could not process CAMA file %s: %s", url, exc)

    save_debug_json("sc700_owndat_sample_rows.json", own_rows[:5])
    save_debug_json("sc701_maildat_sample_rows.json", mail_rows[:5])
    save_debug_json("sc702_legdat_sample_rows.json", legal_rows[:5])
    save_debug_json("sc705_sc731_parcel_sample_rows.json", parcel_rows[:5])

    parcel_by_id: Dict[str, dict] = {}

    for row in parcel_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id.setdefault(pid, {})
        parcel_by_id[pid].update({
            "parcel_id": pid,
            "prop_address": build_prop_address_from_row(row),
            "prop_city": build_prop_city_from_row(row),
            "prop_zip": build_prop_zip_from_row(row),
        })

    for row in own_rows:
        pid = get_pid(row)
        if not pid:
            continue
        parcel_by_id.setdefault(pid, {})
        parcel_by_id[pid].update({
            "parcel_id": pid,
            "owner": build_owner_name(row),
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
            "mail_state": normalize_state(safe_pick(row, LIKELY_MAIL_STATE_KEYS)),
            "mail_zip": build_mail_zip(row),
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

    owner_index: Dict[str, dict] = {}
    for record in parcel_by_id.values():
        owner = clean_text(record.get("owner"))
        if not owner:
            continue
        for variant in name_variants(owner):
            owner_index.setdefault(variant, record)

    save_debug_json("parcel_by_id_sample.json", list(parcel_by_id.values())[:25])
    save_debug_json("owner_index_sample.json", list(owner_index.items())[:500])
    save_debug_json("owner_values_sample.json", list(owner_index.keys())[:2000])

    logging.info(
        "Built parcel index with %s owner-name keys from %s parcel rows / %s owner rows / %s mail rows / %s legal rows",
        len(owner_index), len(parcel_rows), len(own_rows), len(mail_rows), len(legal_rows)
    )
    return owner_index


async def click_first_matching(page, selectors: List[str]) -> bool:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0:
                await locator.click()
                await page.wait_for_timeout(2500)
                return True
        except Exception:
            continue
    return False


def infer_doc_type_from_text(text: str) -> Optional[str]:
    t = clean_text(text).upper()

    if any(x in t for x in ["LIS PENDENS", " LP ", "LP-"]):
        return "LP"
    if any(x in t for x in ["NOTICE OF FORECLOSURE", "FORECLOS", "NOFC"]):
        return "NOFC"
    if any(x in t for x in ["CERTIFIED JUDGMENT", "DOMESTIC JUDGMENT", "JUDGMENT"]):
        return "JUD"
    if any(x in t for x in ["TAX DEED", "TAXDEED"]):
        return "TAXDEED"
    if any(x in t for x in ["IRS LIEN", "FEDERAL LIEN", "TAX LIEN"]):
        return "LNFED"
    if "MECHANIC LIEN" in t:
        return "LNMECH"
    if "LIEN" in t:
        return "LN"
    if "NOTICE OF COMMENCEMENT" in t:
        return "NOC"
    return None


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


def extract_case_number(text: str, fallback: str) -> str:
    text_u = clean_text(text).upper()
    patterns = [
        r"\b\d{2,4}[ -][A-Z]{1,6}[ -]\d{2,8}\b",
        r"\b[A-Z]{2,}[ ]\d{2}\b",
        r"\b\d{6,}\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, text_u)
        if m:
            return clean_text(m.group(0))
    return fallback


def split_caption(caption: str) -> Tuple[str, str]:
    cap = clean_text(caption)
    upper = cap.upper()

    separators = [" -VS- ", " VS. ", " VS ", " V. ", " V "]
    for sep in separators:
        if sep in upper:
            parts = re.split(re.escape(sep), cap, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                plaintiff = clean_text(parts[0])
                defendant = clean_text(parts[1])
                return plaintiff, defendant
    return "", ""


def clean_defendant_name(name: str) -> str:
    n = clean_text(name)
    if not n:
        return ""

    n = re.sub(r"\bAKA\b.*$", "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"\bET AL\b.*$", "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"\bUNKNOWN HEIRS OF\b", "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"\bUNKNOWN SPOUSE OF\b", "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"\bJOHN DOE\b", "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"\bJANE DOE\b", "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"\s+", " ", n).strip(" ,.-")

    if not n or n in BAD_EXACT_OWNERS:
        return ""
    return n


def looks_like_good_owner(name: str) -> bool:
    n = clean_text(name)
    if not n:
        return False
    if n in BAD_EXACT_OWNERS:
        return False
    if len(n) < 4:
        return False
    letters = sum(ch.isalpha() for ch in n)
    return letters >= 4


def extract_owner_and_grantee(cells: List[str]) -> Tuple[str, str, str]:
    row_text = clean_text(" ".join(cells))

    candidates = cells + [row_text]
    for candidate in candidates:
        plaintiff, defendant = split_caption(candidate)
        defendant = clean_defendant_name(defendant)
        plaintiff = clean_text(plaintiff)
        if looks_like_good_owner(defendant):
            return defendant.title(), plaintiff.title(), candidate

    return "", "", row_text


def parse_pending_civil_table(html: str, base_url: str, prefix: str) -> List[LeadRecord]:
    soup = BeautifulSoup(html, "lxml")
    records: List[LeadRecord] = []
    debug_rows: List[List[str]] = []

    tables = soup.find_all("table")
    for table_idx, table in enumerate(tables, start=1):
        rows = table.find_all("tr")
        for row_idx, row in enumerate(rows, start=1):
            cells = [clean_text(td.get_text(" ")) for td in row.find_all(["td", "th"])]
            if not cells:
                continue

            debug_rows.append(cells[:10])

            row_text = clean_text(" ".join(cells))
            doc_type = infer_doc_type_from_text(row_text)
            if doc_type not in {"NOFC", "LP", "JUD", "LN", "LNMECH", "LNFED", "NOC"}:
                continue

            filed = try_parse_date(row_text) or datetime.now().date().isoformat()
            cutoff = datetime.now().date() - timedelta(days=LOOKBACK_DAYS)
            if datetime.fromisoformat(filed).date() < cutoff:
                continue

            owner, grantee, source_caption = extract_owner_and_grantee(cells)
            if not owner:
                continue

            amount_match = re.search(r"\$[\d,]+(?:\.\d{2})?", row_text)
            amount = parse_amount(amount_match.group(0)) if amount_match else None

            link = row.find("a", href=True)
            href = clean_text(link.get("href")) if link else ""

            doc_num = extract_case_number(row_text, f"{prefix}-T{table_idx}-R{row_idx}")

            record = LeadRecord(
                doc_num=doc_num,
                doc_type=doc_type,
                filed=filed,
                cat=doc_type,
                cat_label=LEAD_TYPE_MAP.get(doc_type, doc_type),
                owner=owner,
                grantee=grantee,
                amount=amount,
                legal=clean_text(source_caption),
                clerk_url=requests.compat.urljoin(base_url, href) if href else base_url,
            )
            record.flags = category_flags(record.doc_type, record.owner)
            record.score = score_record(record)
            records.append(record)

    save_debug_json(f"{prefix.lower()}_table_cells_sample.json", debug_rows[:25])
    return records


async def scrape_pending_civil_records(page) -> List[LeadRecord]:
    logging.info("Scraping pending civil filings...")
    records: List[LeadRecord] = []

    try:
        await page.goto(PENDING_CIVIL_URL, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(4000)
        html1 = await page.content()
        save_debug_text("pending_civil_page_1.html", html1)
        records.extend(parse_pending_civil_table(html1, PENDING_CIVIL_URL, "PCF1"))

        clicked = await click_first_matching(page, [
            "text=Search",
            "text=Begin",
            "text=Continue",
            "input[type='submit']",
            "button",
            "a",
        ])
        if clicked:
            html2 = await page.content()
            save_debug_text("pending_civil_page_2.html", html2)
            records.extend(parse_pending_civil_table(html2, PENDING_CIVIL_URL, "PCF2"))

    except Exception as exc:
        logging.warning("Pending civil scrape failed: %s", exc)

    return records


async def scrape_clerk_records() -> List[LeadRecord]:
    logging.info("Scraping clerk records...")
    records: List[LeadRecord] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(CLERK_RECORDS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(4000)
            logging.info("Clerk records page 1 title: %s", await page.title())
            save_debug_text("clerk_records_page_1.html", await page.content())

            clicked = await click_first_matching(page, [
                "text=Click Here",
                "text=Begin",
                "text=Continue",
                "text=Accept",
                "text=Search",
                "input[type='submit']",
                "button",
                "a",
            ])
            if clicked:
                logging.info("Clerk records page 2 title: %s", await page.title())
                save_debug_text("clerk_records_page_2.html", await page.content())

                clicked_again = await click_first_matching(page, [
                    "text=Civil",
                    "text=General",
                    "text=Search",
                    "input[type='submit']",
                    "button",
                    "a",
                ])
                if clicked_again:
                    logging.info("Clerk records page 3 title: %s", await page.title())
                    save_debug_text("clerk_records_page_3.html", await page.content())

            records.extend(await scrape_pending_civil_records(page))

        except PlaywrightTimeoutError:
            logging.warning("Timeout while scraping clerk records.")
        except Exception as exc:
            logging.warning("Clerk scrape failed: %s", exc)
        finally:
            await browser.close()

    deduped: List[LeadRecord] = []
    seen = set()
    for record in records:
        normalized_doc = re.sub(r"^(PCF1|PCF2)-", "", clean_text(record.doc_num).upper())
        key = (
            normalized_doc,
            clean_text(record.doc_type).upper(),
            normalize_name(record.owner),
            clean_text(record.filed),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)

    logging.info("Collected %s clerk records", len(deduped))
    return deduped


def valid_probate_candidate(text: str) -> bool:
    t = clean_text(text)
    t_u = t.upper()
    if not t:
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

            save_debug_text("probate_page_1.html", await page.content())
            logging.info("Probate page 1 title: %s", await page.title())

            clicked = await click_first_matching(page, [
                "text=Click Here",
                "text=Begin",
                "text=Search",
                "a.anchorButton",
                "input[type='submit']",
                "button",
                "a",
            ])

            if clicked:
                save_debug_text("probate_page_2.html", await page.content())
                logging.info("Probate page 2 title: %s", await page.title())

                soup = BeautifulSoup(await page.content(), "lxml")
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
                        clerk_url=requests.compat.urljoin(page.url, href) if href else page.url,
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
        key = (clean_text(record.doc_num), normalize_name(record.owner), clean_text(record.filed))
        if key in seen:
            continue
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
                record.mail_state = record.mail_state or normalize_state(clean_text(matched.get("mail_state")))
                record.mail_zip = record.mail_zip or clean_text(matched.get("mail_zip"))
                record.legal = record.legal or clean_text(matched.get("legal"))

            record.mail_state = normalize_state(record.mail_state)
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
        normalized_doc = re.sub(r"^(PCF1|PCF2)-", "", clean_text(record.doc_num).upper())
        key = (
            normalized_doc,
            clean_text(record.doc_type).upper(),
            normalize_name(record.owner),
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
