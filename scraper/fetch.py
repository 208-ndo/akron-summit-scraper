import argparse
import asyncio
import csv
import io
import json
import logging
import re
import random
import zipfile
from collections import defaultdict
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

DEFAULT_OUTPUT_JSON_PATHS = [
    DATA_DIR / "records.json",
    DASHBOARD_DIR / "records.json",
]
DEFAULT_OUTPUT_CSV_PATH = DATA_DIR / "ghl_export.csv"
DEFAULT_ENRICHED_JSON_PATH = DATA_DIR / "records.enriched.json"
DEFAULT_ENRICHED_CSV_PATH = DATA_DIR / "records.enriched.csv"
DEFAULT_REPORT_PATH = DATA_DIR / "match_report.json"
DEFAULT_VACANT_JSON_PATH = DATA_DIR / "vacant_land.json"
DEFAULT_STACK_JSON_PATH = DATA_DIR / "hot_stack.json"

LOOKBACK_DAYS = 90
SOURCE_NAME = "Akron / Summit County, Ohio"

CLERK_RECORDS_URL = "https://clerk.summitoh.net/RecordsSearch/Disclaimer.asp?toPage=SelectDivision.asp"
PENDING_CIVIL_URL = "https://newcivilfilings.summitoh.net/"
PROBATE_URL = "https://search.summitohioprobate.com/eservices/"
CAMA_PAGE_URL = "https://fiscaloffice.summitoh.net/index.php/documents-a-forms/viewcategory/10-cama"
VACANT_BUILDING_URL = "https://www.akronohio.gov/government/boards_and_commissions/vacant_building_board.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

LEAD_TYPE_MAP = {
    "LP":       "Lis Pendens",
    "NOFC":     "Pre-foreclosure",
    "TAXDEED":  "Tax Deed",
    "JUD":      "Judgment",
    "CCJ":      "Certified Judgment",
    "DRJUD":    "Domestic Judgment",
    "LNCORPTX": "Corp Tax Lien",
    "LNIRS":    "IRS Lien",
    "LNFED":    "Federal Lien",
    "LN":       "Lien",
    "LNMECH":   "Mechanic Lien",
    "LNHOA":    "HOA Lien",
    "MEDLN":    "Medicaid Lien",
    "PRO":      "Probate / Estate",
    "NOC":      "Notice of Commencement",
    "RELLP":    "Release Lis Pendens",
    "TAXDELINQ":"Tax Delinquent",
    "VACANT":   "Vacant Property",
    "VACLAND":  "Vacant Land",
    "TAX":      "Tax Delinquent",
}

# Summit County LUC codes — Land Use Classification
VACANT_LAND_LUCS = {"500", "501", "502", "503"}

# Residential LUCs: single family, multi-family, condos, mobile homes
RESIDENTIAL_LUCS = {
    "510", "511", "512", "513", "514", "515",   # single family
    "520", "521", "522", "523",                   # 2-3 family
    "530", "531", "532", "533",                   # 4+ family / apartment
    "540",                                         # condo
    "550",                                         # mobile home
    "560",                                         # rural residential
}

# Vacant home LUCs — these are structures marked as vacant/abandoned in CAMA
VACANT_HOME_LUCS = {
    "510", "511", "512",   # single family — will cross-reference Akron vacant board
}

MAX_INFILL_ACRES = 2.0

LIKELY_OWNER_KEYS = [
    "OWNER1","OWNER2","OWNER","OWN1","OWNER_NAME","OWNERNAME","OWNERNM","NAME","OWNNAM",
    "OWNER 1","OWNER 2","TAXPAYER","TAXPAYER_NAME","MAILNAME","MAIL_NAME","NAME1","NAME2"
]
LIKELY_PROP_ADDR_KEYS = ["SITE_ADDR","SITEADDR","PROPERTY_ADDRESS","PROPADDR","ADDRESS","LOCADDR","SADDR"]
LIKELY_PROP_CITY_KEYS = ["SITE_CITY","CITY","SITECITY","PROPERTY_CITY","SCITY","CITYNAME","UDATE1"]
LIKELY_PROP_ZIP_KEYS  = ["SITE_ZIP","ZIP","SITEZIP","PROPERTY_ZIP","SZIP","USER2","ZIPCD","NOTE2"]
LIKELY_MAIL_ADDR_KEYS = ["MAIL_ADR1","ADDR_1","MAILADR1","MAIL_ADDR","MAILADDRESS","MADDR1","ADDRESS1","MAILADD1"]
LIKELY_MAIL_CITY_KEYS = ["NOTE1","MAILCITY","CITY","MCITY","CITYNAME"]
LIKELY_MAIL_STATE_KEYS= ["STATE","MAILSTATE","MSTATE","STATECODE"]
LIKELY_MAIL_ZIP_KEYS  = ["MAIL_PTR","MAILZIP","ZIP","MZIP","OWNER ZIPCD1","OWNER ZIPCD2","OWNER_ZIPCD1","OWNER_ZIPCD2"]
LIKELY_LEGAL_KEYS     = ["LEGAL","LEGAL_DESC","LEGALDESCRIPTION","LEGDESC"]
LIKELY_PID_KEYS       = ["PARID","PARCEL","PAIRD","PARCELID","PARCEL_ID","PID","PARCELNO","PAR_NO","PAR_NUM"]

BAD_EXACT_OWNERS = {
    "Action","Get Docs","Date Added","Party","Plaintiff","Defendant",
    "Search","Home","Select Division","Welcome",
    "EOY ROLL","LWALKER","AWHITE","NJARJABKA","CL_NJARJABKA","SCLB",
}

SC701_STATE_CODE_MAP = {"3":"OH","0":"","1":"","2":""}

STATE_CODES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
    "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK",
    "OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"
}
CORP_WORDS = {
    "LLC","INC","CORP","CO","COMPANY","TRUST","BANK","ASSOCIATION","NATIONAL",
    "LTD","LP","PLC","HOLDINGS","FUNDING","VENTURES","RESTORATION","SCHOOLS",
    "UNION","MORTGAGE","RECOVERY","BOARD","SERVICING"
}
NOISE_NAME_WORDS = {
    "AKA","ET","AL","UNKNOWN","HEIRS","SPOUSE","JOHN","JANE","DOE","ADMINISTRATOR",
    "EXECUTOR","FIDUCIARY","TRUSTEE","OR","THE","OF","SUCCESSOR","MERGER","TO","BY","ADMIN","ESTATE"
}

# Distress stacking — points per source type
DISTRESS_SOURCE_POINTS = {
    "foreclosure":30,"lis_pendens":30,"judgment":20,"lien":15,
    "tax_delinquent":25,"vacant_building":30,"vacant_land":25,
    "vacant_home":35,   # vacant residential = high priority
    "probate":20,"mechanic_lien":15,
}
STACK_BONUS = {2:15, 3:25, 4:40}

# -----------------------------------------------------------------------
# SKIP TRACE FILTERING RULES
# -----------------------------------------------------------------------
# Only skip-trace leads that meet ALL of these criteria:
#   1. Is a RESIDENTIAL property (LUC 510-560) OR a Vacant Home
#   2. Has a property address
#   3. Not already skip-traced
#   4. Meets at least ONE of:
#      a. Hot Stack (2+ distress sources)
#      b. Pre-foreclosure or Lis Pendens
#      c. Tax Delinquent + Absentee Owner
#      d. Vacant Home (on Akron vacant building board)
#      e. Probate

SKIP_TRACE_DOC_TYPES = {"LP", "NOFC", "JUD", "CCJ", "DRJUD", "PRO", "TAXDEED", "TAXDELINQ", "TAX"}

def should_skip_trace(record) -> bool:
    """
    Returns True only for HIGH-PRIORITY RESIDENTIAL leads.
    
    Rules:
    - Must be residential LUC (510-560) OR a vacant home on the board
    - Must have a property address
    - Must meet at least one priority trigger
    
    This keeps skip-trace volume manageable (~50-300 leads vs 3,600+).
    """
    if not record.prop_address:
        return False

    # Vacant land (500-503) is EXCLUDED unless it has a foreclosure/tax delinquent
    # In that case it's already a court filing lead and won't come through here
    is_residential = record.luc in RESIDENTIAL_LUCS
    is_vacant_home = "Vacant home" in record.flags
    is_vacant_land_only = record.luc in VACANT_LAND_LUCS and not record.hot_stack

    if is_vacant_land_only:
        return False

    if not is_residential and not is_vacant_home:
        # Allow court filing leads even without confirmed LUC (may be unmatched)
        is_court_filing = record.doc_type in SKIP_TRACE_DOC_TYPES
        if not is_court_filing:
            return False

    # Priority triggers — must meet at least one
    triggers = [
        record.hot_stack,
        record.doc_type in {"LP", "NOFC"},                           # foreclosure
        record.doc_type in {"JUD", "CCJ", "DRJUD"},                  # judgment
        record.doc_type == "PRO",                                     # probate
        record.doc_type in {"TAX", "TAXDELINQ", "TAXDEED"} and record.is_absentee,  # tax + absentee
        is_vacant_home,                                               # on vacant home board
        "Tax delinquent" in record.flags and record.is_absentee,
    ]
    return any(triggers)


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
    match_method: str = "unmatched"
    match_score: float = 0.0
    with_address: int = 0
    distress_sources: List[str] = field(default_factory=list)
    distress_count: int = 0
    hot_stack: bool = False
    luc: str = ""
    acres: str = ""
    is_vacant_land: bool = False
    is_vacant_home: bool = False
    is_absentee: bool = False
    phones: list = field(default_factory=list)
    phone_types: list = field(default_factory=list)
    emails: list = field(default_factory=list)
    skip_trace_source: str = ""


@dataclass
class VacantLandRecord:
    parcel_id: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "OH"
    prop_zip: str = ""
    owner: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""
    luc: str = ""
    acres: str = ""
    flags: List[str] = field(default_factory=list)
    score: int = 0
    distress_sources: List[str] = field(default_factory=list)
    distress_count: int = 0


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def log_setup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


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
    if not v:
        return ""
    if v in SC701_STATE_CODE_MAP:
        return SC701_STATE_CODE_MAP[v]
    if v in {"0","1","2","4","5","6","7","8","9","00","000","-","N/A","NA","NONE","NULL"}:
        return ""
    v = re.sub(r"[^A-Z]", "", v)
    return v if v in STATE_CODES else ""


def retry_request(url: str, attempts: int = 3, timeout: int = 60) -> requests.Response:
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_error = exc
            logging.warning("Request failed (%s/%s) for %s: %s", attempt, attempts, url, exc)
    raise last_error


def normalize_name(name: str) -> str:
    name = clean_text(name).upper()
    name = re.sub(r"[^A-Z0-9,&.\- /']", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def normalize_person_name(name: str) -> str:
    n = normalize_name(name)
    if not n:
        return ""
    for pat in [r"\bAKA\b.*$", r"\bET AL\b.*$", r"\bUNKNOWN HEIRS OF\b",
                r"\bUNKNOWN SPOUSE OF\b", r"\bUNKNOWN ADMINISTRATOR\b",
                r"\bEXECUTOR\b", r"\bFIDUCIARY\b", r"\bJOHN DOE\b", r"\bJANE DOE\b", r"\bTHE\b"]:
        n = re.sub(pat, "", n).strip()
    return re.sub(r"\s+", " ", n).strip(" ,.-")


def tokens_from_name(name: str) -> List[str]:
    n = normalize_person_name(name)
    if not n:
        return []
    return [t for t in re.split(r"[ ,/&.\-]+", n) if t and t not in NOISE_NAME_WORDS]


def likely_corporate_name(name: str) -> bool:
    return any(t in CORP_WORDS for t in set(tokens_from_name(name)))


def get_last_name(name: str) -> str:
    toks = tokens_from_name(name)
    return toks[-1] if toks else ""


def get_first_name(name: str) -> str:
    toks = tokens_from_name(name)
    return toks[0] if toks else ""


def get_first_initial(name: str) -> str:
    first = get_first_name(name)
    return first[:1] if first else ""


def same_first_name_or_initial(name_a: str, name_b: str) -> bool:
    fa, fb = get_first_name(name_a), get_first_name(name_b)
    if fa and fb and fa == fb:
        return True
    ia, ib = get_first_initial(name_a), get_first_initial(name_b)
    return bool(ia and ib and ia == ib)


def singularize_last_name(ln: str) -> str:
    ln = clean_text(ln).upper()
    if ln.endswith("IES") and len(ln) > 4: return ln[:-3] + "Y"
    if ln.endswith("ES") and len(ln) > 3:  return ln[:-2]
    if ln.endswith("S") and len(ln) > 3:   return ln[:-1]
    return ln


def last_names_compatible(a: str, b: str) -> bool:
    a_u, b_u = clean_text(a).upper(), clean_text(b).upper()
    if not a_u or not b_u: return False
    return a_u == b_u or singularize_last_name(a_u) == singularize_last_name(b_u)


def build_owner_name(row: dict) -> str:
    o1 = clean_text(row.get("OWNER1") or row.get("OWNER 1"))
    o2 = clean_text(row.get("OWNER2") or row.get("OWNER 2"))
    if o1 and o2: return re.sub(r"\s+", " ", f"{o1} {o2}".strip())
    return o1 or o2 or safe_pick(row, LIKELY_OWNER_KEYS)


def build_mail_zip(row: dict) -> str:
    mp = clean_text(row.get("MAIL_PTR"))
    if mp and re.fullmatch(r"\d{5}", mp): return mp
    z1 = clean_text(row.get("OWNER ZIPCD1") or row.get("OWNER_ZIPCD1"))
    z2 = clean_text(row.get("OWNER ZIPCD2") or row.get("OWNER_ZIPCD2"))
    if z1 and z2: return f"{z1}-{z2}"
    return z1 or safe_pick(row, LIKELY_MAIL_ZIP_KEYS)


def build_mail_city_sc701(row: dict) -> str:
    n1 = clean_text(row.get("NOTE1"))
    if n1 and len(n1) > 2 and not re.fullmatch(r"\d+", n1):
        return n1.title()
    for key in ["MAILCITY", "CITY", "MCITY"]:
        val = clean_text(row.get(key))
        if val and len(val) > 2 and not re.fullmatch(r"\d+", val):
            return val.title()
    return ""


def build_mail_state_sc701(row: dict) -> str:
    raw = clean_text(row.get("STATE") or "")
    mapped = SC701_STATE_CODE_MAP.get(raw)
    if mapped is not None: return mapped
    cleaned = re.sub(r"[^A-Z]", "", raw.upper())
    if cleaned in STATE_CODES: return cleaned
    return "OH" if clean_text(row.get("MAIL_ADR1")) else ""


def split_owner_chunks(name: str) -> List[str]:
    raw = normalize_person_name(name)
    if not raw: return []
    working = re.sub(r"\bET AL\b|\bAKA\b.*$", "", raw)
    working = re.sub(r"\s+", " ", working).strip(" ,;/")
    if not working: return []
    parts = re.split(r"\s*(?:;|/|\bAND\b|&)\s*", working)
    seen, result = set(), []
    for part in parts:
        p = normalize_person_name(part)
        if p and p not in seen:
            seen.add(p)
            result.append(p)
    return result or [working]


def name_variants(name: str) -> List[str]:
    raw = normalize_person_name(name)
    if not raw: return []
    suffixes = {"JR","SR","II","III","IV","V","ETAL","ET","AL"}
    joiner_noise = {"AND","&","OR"}
    variants = set()

    for chunk in split_owner_chunks(raw):
        working = re.sub(r"\bAND\b|\bOR\b|&", " ", chunk.replace(";", " ").replace("/", " "))
        working = re.sub(r"\s+", " ", working).strip()
        if not working: continue
        variants.update([chunk, working, working.replace(",", "")])
        comma_parts = [normalize_person_name(x) for x in chunk.split(",") if normalize_person_name(x)]

        def add_variants(parts):
            parts = [p for p in parts if p and p not in joiner_noise]
            while parts and parts[-1] in suffixes: parts = parts[:-1]
            if not parts: return
            full = " ".join(parts)
            if full: variants.add(full)
            if len(parts) == 1: variants.add(parts[0]); return
            first, last = parts[0], parts[-1]
            mids = parts[1:-1]
            mid = " ".join(mids)
            variants.update([f"{first} {last}", f"{last} {first}", f"{last}, {first}"])
            if mid:
                variants.update([f"{first} {mid} {last}", f"{last}, {first} {mid}", f"{last} {first} {mid}"])
                mi = " ".join(m[0] for m in mids if m)
                if mi:
                    variants.update([f"{first} {mi} {last}", f"{last}, {first} {mi}", f"{last} {first} {mi}"])

        if len(comma_parts) >= 2:
            last = comma_parts[0]
            rem = []
            for piece in comma_parts[1:]: rem.extend(piece.split())
            add_variants(rem + [last])
            ft = comma_parts[1].split()
            if ft:
                f = ft[0]
                variants.update([f"{f} {last}", f"{last} {f}", f"{last}, {f}"])
        else:
            add_variants([p for p in working.replace(",", " ").split() if p])

    final, seen = [], set()
    for v in variants:
        v = re.sub(r"\s+", " ", normalize_person_name(v)).strip(" ,")
        if v and v not in seen:
            seen.add(v)
            final.append(v)
    return final


def parse_amount(value: str) -> Optional[float]:
    value = clean_text(value)
    if not value: return None
    cleaned = re.sub(r"[^0-9.\-]", "", value)
    try: return float(cleaned) if cleaned else None
    except ValueError: return None


def safe_pick(row: dict, keys: List[str]) -> str:
    for key in keys:
        if key in row and clean_text(row.get(key)): return clean_text(row.get(key))
    upper_map = {str(k).upper(): k for k in row.keys()}
    for key in keys:
        if key.upper() in upper_map:
            val = clean_text(row.get(upper_map[key.upper()]))
            if val: return val
    return ""


def get_pid(row: dict) -> str:
    return safe_pick(row, LIKELY_PID_KEYS)


def parse_acres(raw: str) -> Optional[float]:
    raw = clean_text(raw)
    if not raw: return None
    try: return float(raw)
    except ValueError: return None


def is_infill_lot(luc: str, acres_raw: str) -> bool:
    if luc not in VACANT_LAND_LUCS: return False
    acres = parse_acres(acres_raw)
    return acres is None or acres <= MAX_INFILL_ACRES


def category_flags(doc_type: str, owner: str = "") -> List[str]:
    flags: List[str] = []
    dt = clean_text(doc_type).upper()
    ou = normalize_name(owner)
    if dt == "LP":                                      flags.append("Lis pendens")
    if dt == "NOFC":                                    flags.append("Pre-foreclosure")
    if dt in {"JUD","CCJ","DRJUD"}:                    flags.append("Judgment lien")
    if dt in {"TAXDEED","LNCORPTX","LNIRS","LNFED","TAXDELINQ","TAX"}: flags.append("Tax lien")
    if dt == "LNMECH":                                  flags.append("Mechanic lien")
    if dt == "PRO":                                     flags.append("Probate / estate")
    if dt in {"VACANT","VACLAND"}:                     flags.append("Vacant property")
    if any(t in f" {ou} " for t in [" LLC"," INC"," CORP"," CO "," COMPANY"," TRUST"," LP"," LTD"," BANK "]):
        flags.append("LLC / corp owner")
    return list(dict.fromkeys(flags))


def classify_distress_source(doc_type: str) -> Optional[str]:
    dt = clean_text(doc_type).upper()
    if dt in {"LP","RELLP"}:                return "lis_pendens"
    if dt == "NOFC":                        return "foreclosure"
    if dt in {"JUD","CCJ","DRJUD"}:        return "judgment"
    if dt in {"LN","LNHOA","LNFED","LNIRS","LNCORPTX","MEDLN"}: return "lien"
    if dt == "LNMECH":                      return "mechanic_lien"
    if dt in {"TAXDEED","TAXDELINQ","TAX"}: return "tax_delinquent"
    if dt == "PRO":                         return "probate"
    if dt in {"VACANT","VACLAND"}:          return "vacant_building"
    return None


def score_record(record: LeadRecord) -> int:
    score = 30
    lower_flags = {f.lower() for f in record.flags}
    flag_score = 0
    if "lis pendens" in lower_flags:        flag_score += 20
    if "pre-foreclosure" in lower_flags:    flag_score += 20
    if "judgment lien" in lower_flags:      flag_score += 15
    if "tax lien" in lower_flags:           flag_score += 15
    if "mechanic lien" in lower_flags:      flag_score += 10
    if "probate / estate" in lower_flags:   flag_score += 15
    if "vacant property" in lower_flags:    flag_score += 20
    if "vacant home" in lower_flags:        flag_score += 25   # bonus for vacant residential
    if "absentee owner" in lower_flags:     flag_score += 10
    score += min(flag_score, 50)
    if "lis pendens" in lower_flags and "pre-foreclosure" in lower_flags:
        score += 20
    if record.amount is not None:
        score += 15 if record.amount > 100000 else (10 if record.amount > 50000 else 0)
    if record.filed:
        try:
            if datetime.fromisoformat(record.filed).date() >= (datetime.now().date() - timedelta(days=7)):
                if "New this week" not in record.flags:
                    record.flags.append("New this week")
                score += 5
        except Exception:
            pass
    if record.prop_address:
        score += 5

    # DISTRESS STACK BONUS
    distress_count = len(set(record.distress_sources))
    record.distress_count = distress_count
    bonus_key = min(distress_count, 4)
    if bonus_key >= 2:
        score += STACK_BONUS.get(bonus_key, STACK_BONUS[4])
        record.hot_stack = True
        if "🔥 Hot Stack" not in record.flags:
            record.flags.append("🔥 Hot Stack")

    return min(score, 100)


# -----------------------------------------------------------------------
# VACANT BUILDING BOARD SCRAPER
# -----------------------------------------------------------------------
def scrape_vacant_building_addresses() -> List[str]:
    """
    Scrape the Akron Vacant Building Board — these are RESIDENTIAL homes
    that are officially registered as vacant/abandoned. Very high priority.
    Returns a list of normalized address strings.
    """
    addresses: List[str] = []
    try:
        resp = retry_request(VACANT_BUILDING_URL, timeout=30)
        soup = BeautifulSoup(resp.text, "lxml")
        text = soup.get_text(" ")
        addr_pattern = re.compile(
            r"\b(\d{2,5})\s+([NSEW]\.?\s+)?([A-Z][A-Za-z\.\s]{2,30})\s+(St|Ave|Rd|Dr|Blvd|Ln|Ct|Pl|Way|Ter|Cir|Pkwy)\.?\b",
            re.IGNORECASE
        )
        for match in addr_pattern.finditer(text):
            addr = re.sub(r"\s+", " ", match.group(0)).strip().upper()
            if addr and len(addr) > 8:
                addresses.append(addr)
        addresses = list(dict.fromkeys(addresses))
        logging.info("Found %s vacant building addresses from Akron board", len(addresses))
        save_debug_json("vacant_building_addresses.json", addresses)
    except Exception as exc:
        logging.warning("Could not scrape vacant building board: %s", exc)
    return addresses


# -----------------------------------------------------------------------
# DISTRESS STACKING
# -----------------------------------------------------------------------
def normalize_address_key(address: str) -> str:
    addr = clean_text(address).upper()
    for old, new in [("N.","N"),("S.","S"),("E.","E"),("W.","W")]:
        addr = addr.replace(old, new)
    addr = re.sub(r"[^A-Z0-9\s]", "", addr)
    return re.sub(r"\s+", " ", addr).strip()


def is_absentee_owner(prop_address: str, mail_address: str) -> bool:
    """
    Returns True if the mailing address is different from the property address.
    Absentee = owner doesn't live at the property (investor, out-of-state, LLC, etc).

    Rules:
    - PO Boxes = always absentee
    - Normalize both addresses (strip directionals, punctuation)
    - Compare street number + first street word
    - Different = absentee
    """
    if not prop_address or not mail_address:
        return False

    mail_upper = mail_address.upper()
    if re.search(r"\bP\.?\s*O\.?\s*BOX\b", mail_upper):
        return True

    prop_key = normalize_address_key(prop_address)
    mail_key = normalize_address_key(mail_address)

    if not prop_key or not mail_key:
        return False

    if prop_key == mail_key:
        return False

    def street_core(addr: str) -> str:
        parts = addr.split()
        if len(parts) >= 2:
            return " ".join(parts[:2])
        return addr

    prop_core = street_core(prop_key)
    mail_core = street_core(mail_key)

    if prop_core == mail_core:
        return False

    return True


def scrape_tax_delinquent_parcels() -> Dict[str, dict]:
    """
    Scrape the Akron Legal News delinquent tax list.
    Returns dict of parcel_id -> {owner, amount_owed, legal, source_url}

    Improvement over previous version:
    - Better entry splitting on bullet separators
    - More robust owner/legal parsing
    - Handles multi-line entries
    """
    DELINQUENT_INDEX_URL = "https://www.akronlegalnews.com/notices/delinquent_taxes"
    delinquent_parcels: Dict[str, dict] = {}

    try:
        logging.info("Scraping Akron Legal News delinquent tax index...")
        resp = retry_request(DELINQUENT_INDEX_URL, timeout=30)
        soup = BeautifulSoup(resp.text, "lxml")

        detail_links = []
        for link in soup.select("a[href]"):
            href = clean_text(link.get("href", ""))
            if "delinquent_taxes_detail" in href:
                full_url = requests.compat.urljoin(DELINQUENT_INDEX_URL, href)
                if full_url not in detail_links:
                    detail_links.append(full_url)

        logging.info("Found %s delinquent tax section pages to scrape", len(detail_links))

        for i, url in enumerate(detail_links):
            try:
                resp2 = retry_request(url, timeout=45)
                soup2 = BeautifulSoup(resp2.text, "lxml")
                raw_text = soup2.get_text(" ")

                entries = re.split(r"\s*[•·]\s*", raw_text)

                for entry in entries:
                    entry = clean_text(entry)
                    if not entry:
                        continue

                    m = re.match(r"^(\d{7})\s+([\d,]+\.?\d*)\s+(.+)$", entry, re.DOTALL)
                    if not m:
                        continue

                    pid = m.group(1)
                    try:
                        amount_owed = float(m.group(2).replace(",", ""))
                    except ValueError:
                        amount_owed = 0.0
                    rest = clean_text(m.group(3))

                    legal_keywords = ["LOT ", "TR ", "BLK ", "SEC ", "LOTS ", "ALLOTMENT",
                                      "SUB ", "PARCEL ", "PART ", "COND ", "UNIT "]
                    owner_part = rest
                    legal_part = ""

                    earliest = len(rest)
                    for kw in legal_keywords:
                        idx = rest.upper().find(kw)
                        if idx > 0 and idx < earliest:
                            earliest = idx

                    if earliest < len(rest):
                        owner_part = clean_text(rest[:earliest])
                        legal_part = clean_text(rest[earliest:])

                    owner_part = re.sub(r"\*+$", "", owner_part).strip()
                    owner_part = clean_text(owner_part)

                    if pid and owner_part and len(owner_part) >= 3:
                        delinquent_parcels[pid] = {
                            "parcel_id": pid,
                            "owner": owner_part,
                            "amount_owed": amount_owed,
                            "legal": legal_part[:200],
                            "source_url": url,
                        }

                if (i + 1) % 10 == 0:
                    logging.info("Scraped %s/%s delinquent tax sections, found %s parcels so far",
                                 i + 1, len(detail_links), len(delinquent_parcels))

            except Exception as exc:
                logging.warning("Could not scrape delinquent section %s: %s", url, exc)
                continue

    except Exception as exc:
        logging.warning("Could not scrape Akron Legal News delinquent list: %s", exc)

    logging.info("Scraped %s delinquent parcels from Akron Legal News", len(delinquent_parcels))
    save_debug_json("delinquent_parcels.json", list(delinquent_parcels.values())[:100])
    return delinquent_parcels


def build_distress_index(
    records: List[LeadRecord],
    vacant_addresses: List[str],
    vacant_land_pids: set,
    delinquent_parcels: Dict[str, dict],
) -> Dict[str, List[str]]:
    """
    Build address -> distress sources index.
    Sources:
      - Court filings (foreclosure, judgment, lien, etc.)
      - Akron vacant building board addresses  (vacant_home)
      - Vacant land parcel cross-reference
      - Tax delinquent parcel list
    """
    index: Dict[str, List[str]] = defaultdict(list)

    for record in records:
        if not record.prop_address: continue
        key = normalize_address_key(record.prop_address)
        if not key: continue
        source = classify_distress_source(record.doc_type)
        if source and source not in index[key]:
            index[key].append(source)
        if record.luc in VACANT_LAND_LUCS:
            if "vacant_land" not in index[key]:
                index[key].append("vacant_land")

    # Vacant building board addresses → vacant_home (not vacant_land)
    for addr in vacant_addresses:
        key = normalize_address_key(addr)
        if key and "vacant_home" not in index[key]:
            index[key].append("vacant_home")

    return dict(index)


def build_delinquent_address_index(
    parcel_rows: List[dict],
    mail_by_pid: Dict[str, dict],
    delinquent_parcels: Dict[str, dict],
) -> Dict[str, dict]:
    """
    Join delinquent parcel IDs → property addresses via CAMA parcel data.
    Returns address key -> delinquent parcel info.
    
    Improvement: now also tags whether the delinquent parcel is residential.
    """
    delinquent_addresses: Dict[str, dict] = {}
    if not delinquent_parcels:
        return delinquent_addresses

    delinquent_pid_set = set(delinquent_parcels.keys())

    for row in parcel_rows:
        pid = get_pid(row)
        if not pid or pid not in delinquent_pid_set:
            continue

        addr = build_prop_address_from_row(row)
        if addr:
            key = normalize_address_key(addr)
            if key:
                parcel_info = delinquent_parcels[pid].copy()
                parcel_info["prop_address"] = addr
                parcel_info["prop_zip"] = build_prop_zip_from_row(row)
                luc = clean_text(row.get("LUC", ""))
                parcel_info["luc"] = luc
                parcel_info["is_residential"] = luc in RESIDENTIAL_LUCS
                parcel_info["is_vacant_land"] = luc in VACANT_LAND_LUCS
                delinquent_addresses[key] = parcel_info

    logging.info("Mapped %s delinquent addresses from %s delinquent parcels",
                 len(delinquent_addresses), len(delinquent_parcels))
    save_debug_json("delinquent_addresses.json", list(delinquent_addresses.values())[:50])
    return delinquent_addresses


def apply_distress_stacking(
    records: List[LeadRecord],
    distress_index: Dict[str, List[str]],
    delinquent_addresses: Dict[str, dict],
    vacant_home_keys: set,
) -> List[LeadRecord]:
    """
    Apply distress stack scores and cross-reference all data sources.
    Now also detects vacant homes (residential properties on the Akron board).
    """
    for record in records:
        if not record.prop_address:
            continue
        key = normalize_address_key(record.prop_address)
        sources = list(distress_index.get(key, []))

        # Cross-reference: is this address also tax delinquent?
        delinquent_info = delinquent_addresses.get(key)
        if delinquent_info:
            if "tax_delinquent" not in sources:
                sources.append("tax_delinquent")
            if "Tax delinquent" not in record.flags:
                record.flags.append("Tax delinquent")
            if record.amount is None and delinquent_info.get("amount_owed"):
                record.amount = delinquent_info["amount_owed"]

        # Cross-reference: is this a vacant HOME (on Akron board + residential LUC)?
        is_vacant_home = key in vacant_home_keys and record.luc in (RESIDENTIAL_LUCS | VACANT_HOME_LUCS)
        if is_vacant_home:
            record.is_vacant_home = True
            if "vacant_home" not in sources:
                sources.append("vacant_home")
            if "Vacant home" not in record.flags:
                record.flags.append("Vacant home")

        record.distress_sources = list(set(sources))

        for source in record.distress_sources:
            if source == "vacant_home" and "Vacant home" not in record.flags:
                record.flags.append("Vacant home")
            if source == "vacant_land" and "Vacant land parcel" not in record.flags:
                record.flags.append("Vacant land parcel")
            if source == "tax_delinquent" and "Tax delinquent" not in record.flags:
                record.flags.append("Tax delinquent")

        record.score = score_record(record)

    return records


# -----------------------------------------------------------------------
# VACANT LAND LIST — only included if ALSO in foreclosure or tax delinquent
# -----------------------------------------------------------------------
def build_vacant_land_list(
    parcel_rows: List[dict],
    mail_by_pid: Dict[str, dict],
    delinquent_pid_set: set,
    foreclosure_pids: set,
) -> List[VacantLandRecord]:
    """
    Build list of vacant infill lots (≤ 2 acres).
    
    KEY CHANGE: Now only includes vacant land that is ALSO:
    - In foreclosure/lis pendens (court filing exists), OR
    - Tax delinquent (on Akron Legal News list)
    
    This cuts the 27k+ result set to a manageable distressed-only list.
    """
    vacant: List[VacantLandRecord] = []
    seen_pids: set = set()
    skipped_no_distress = 0

    for row in parcel_rows:
        luc = clean_text(row.get("LUC"))
        acres_raw = clean_text(row.get("ACRES"))
        if not is_infill_lot(luc, acres_raw):
            continue
        pid = get_pid(row)
        if not pid or pid in seen_pids:
            continue

        # NEW: Only include if ALSO distressed (foreclosure OR tax delinquent)
        is_foreclosure = pid in foreclosure_pids
        is_tax_delinquent = pid in delinquent_pid_set
        if not is_foreclosure and not is_tax_delinquent:
            skipped_no_distress += 1
            continue

        seen_pids.add(pid)
        prop_address = build_prop_address_from_row(row)
        prop_zip = build_prop_zip_from_row(row)
        mail_row = mail_by_pid.get(pid, {})
        mail_street = clean_text(mail_row.get("MAIL_ADR1")) if mail_row else ""
        mail_city   = build_mail_city_sc701(mail_row) if mail_row else ""
        mail_zip    = build_mail_zip(mail_row) if mail_row else ""
        mail_state  = build_mail_state_sc701(mail_row) if mail_row else ""
        owner       = build_owner_name(mail_row) if mail_row else ""
        if not prop_address and not mail_street:
            continue

        distress_sources = []
        flags = ["Vacant land", "Infill lot"]
        if is_foreclosure:
            distress_sources.append("foreclosure")
            flags.append("🔥 In foreclosure")
        if is_tax_delinquent:
            distress_sources.append("tax_delinquent")
            flags.append("Tax delinquent")

        rec = VacantLandRecord(
            parcel_id=pid, prop_address=prop_address, prop_city=mail_city,
            prop_state="OH", prop_zip=prop_zip, owner=owner,
            mail_address=mail_street, mail_city=mail_city,
            mail_state=mail_state or "OH", mail_zip=mail_zip,
            luc=luc, acres=acres_raw,
            flags=flags,
            score=50 if (is_foreclosure and is_tax_delinquent) else 45,
            distress_sources=distress_sources,
            distress_count=len(distress_sources),
        )
        vacant.append(rec)

    logging.info(
        "Found %s distressed vacant infill lots (≤2 acres) | Skipped %s non-distressed",
        len(vacant), skipped_no_distress
    )
    return vacant


# -----------------------------------------------------------------------
# VACANT HOME LIST — residential properties on Akron vacant building board
# -----------------------------------------------------------------------
def build_vacant_home_list(
    vacant_addresses: List[str],
    parcel_rows: List[dict],
    mail_by_pid: Dict[str, dict],
    delinquent_pid_set: set,
) -> List[LeadRecord]:
    """
    NEW: Build a list of VACANT HOMES — residential properties (LUC 510-560)
    that appear on the Akron Vacant Building Board.

    These are actual houses/structures sitting empty — different from vacant land.
    Very high value leads for investors.

    Cross-references CAMA parcel data to get owner + mailing address.
    """
    vacant_home_records: List[LeadRecord] = []
    seen_pids: set = set()

    # Build address -> pid lookup from parcel rows
    addr_to_pid: Dict[str, str] = {}
    pid_to_row: Dict[str, dict] = {}
    for row in parcel_rows:
        pid = get_pid(row)
        if not pid:
            continue
        pid_to_row[pid] = row
        luc = clean_text(row.get("LUC", ""))
        if luc not in RESIDENTIAL_LUCS:
            continue
        addr = build_prop_address_from_row(row)
        if addr:
            key = normalize_address_key(addr)
            if key:
                addr_to_pid[key] = pid

    matched = 0
    for vacant_addr in vacant_addresses:
        key = normalize_address_key(vacant_addr)
        pid = addr_to_pid.get(key)
        if not pid or pid in seen_pids:
            continue
        seen_pids.add(pid)

        row = pid_to_row.get(pid, {})
        luc = clean_text(row.get("LUC", ""))
        if luc not in RESIDENTIAL_LUCS:
            continue

        prop_address = build_prop_address_from_row(row)
        prop_city = build_prop_city_from_row(row)
        prop_zip = build_prop_zip_from_row(row)
        acres = clean_text(row.get("ACRES", ""))

        mail_row = mail_by_pid.get(pid, {})
        mail_address = clean_text(mail_row.get("MAIL_ADR1", "")) if mail_row else ""
        mail_city = build_mail_city_sc701(mail_row) if mail_row else ""
        mail_zip = build_mail_zip(mail_row) if mail_row else ""
        mail_state = build_mail_state_sc701(mail_row) if mail_row else "OH"
        owner = build_owner_name(mail_row) if mail_row else ""

        is_absentee = is_absentee_owner(prop_address, mail_address)
        is_tax_delinquent = pid in delinquent_pid_set

        flags = ["Vacant home", "Residential"]
        distress_sources = ["vacant_home"]
        if is_absentee:
            flags.append("Absentee owner")
        if is_tax_delinquent:
            flags.append("Tax delinquent")
            distress_sources.append("tax_delinquent")

        record = LeadRecord(
            doc_num=f"VHOME-{pid}",
            doc_type="VACANT",
            filed=datetime.now().date().isoformat(),
            cat="VACANT",
            cat_label="Vacant Home",
            owner=owner.title() if owner else "",
            prop_address=prop_address,
            prop_city=prop_city,
            prop_state="OH",
            prop_zip=prop_zip,
            mail_address=mail_address,
            mail_city=mail_city,
            mail_state=mail_state,
            mail_zip=mail_zip,
            clerk_url=VACANT_BUILDING_URL,
            flags=flags,
            distress_sources=distress_sources,
            distress_count=len(distress_sources),
            luc=luc,
            acres=acres,
            is_vacant_land=False,
            is_vacant_home=True,
            is_absentee=is_absentee,
            with_address=1 if prop_address else 0,
            match_method="vacant_home_board",
            match_score=1.0,
        )
        record.score = score_record(record)
        record.hot_stack = record.distress_count >= 2

        vacant_home_records.append(record)
        matched += 1

    logging.info(
        "Built %s vacant home leads from Akron board (matched to CAMA residential parcels)",
        matched
    )
    return vacant_home_records


# -----------------------------------------------------------------------
# CAMA FILE DISCOVERY AND PARSING
# -----------------------------------------------------------------------
def discover_cama_downloads() -> List[str]:
    logging.info("Discovering Summit CAMA downloads...")
    response = retry_request(CAMA_PAGE_URL)
    soup = BeautifulSoup(response.text, "lxml")
    wanted_codes = {"SC700","SC701","SC702","SC705","SC720","SC731"}
    urls: List[str] = []
    for link in soup.select("a[href]"):
        href = clean_text(link.get("href"))
        text = clean_text(link.get_text(" ")).upper()
        blob = f"{href} {text}".upper()
        if not any(code in blob for code in wanted_codes): continue
        full_url = requests.compat.urljoin(CAMA_PAGE_URL, href)
        if "/finish/" in full_url: urls.append(full_url)
        elif "/viewdownload/" in full_url: urls.append(full_url.replace("/viewdownload/", "/finish/"))
    deduped, seen = [], set()
    for url in urls:
        if url not in seen:
            seen.add(url); deduped.append(url)
    logging.info("Found %s CAMA file links", len(deduped))
    save_debug_json("cama_links.json", deduped)
    return deduped


def looks_like_zip(content: bytes) -> bool:
    return len(content) >= 4 and content[:2] == b"PK"


def split_lines(text: str) -> List[str]:
    return [line.rstrip("\r") for line in text.splitlines() if clean_text(line)]


def choose_delimiter(sample_text: str) -> str:
    candidates = ["|","\t",","]
    counts = {d: sample_text.count(d) for d in candidates}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else "|"


def parse_delimited_text(raw_text: str) -> List[dict]:
    lines = split_lines(raw_text)
    if len(lines) < 2: return []
    delim = choose_delimiter("\n".join(lines[:10]))
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter=delim)
    rows = []
    for row in reader:
        cleaned = {clean_text(k): clean_text(v) for k, v in row.items() if k is not None}
        if any(cleaned.values()): rows.append(cleaned)
    return rows


def parse_fixed_width_fallback(raw_text: str) -> List[dict]:
    lines = split_lines(raw_text)
    return [{"RAW_LINE": clean_text(line), "ROW_NUM": str(i)} for i, line in enumerate(lines[:5000], 1)]


def read_any_cama_payload(content: bytes, source_name: str) -> Dict[str, List[dict]]:
    datasets: Dict[str, List[dict]] = {}
    if looks_like_zip(content):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for member in zf.namelist():
                if member.endswith("/"): continue
                try: raw = zf.read(member).decode("utf-8", errors="ignore")
                except Exception: continue
                datasets[member] = parse_delimited_text(raw) or parse_fixed_width_fallback(raw)
        return datasets
    raw_text = content.decode("utf-8", errors="ignore")
    datasets[source_name] = parse_delimited_text(raw_text) or parse_fixed_width_fallback(raw_text)
    return datasets


def build_prop_address_from_row(row: dict) -> str:
    parts = [clean_text(row.get(k)) for k in ["ADRNO","ADRADD","ADRDIR","ADRSTR","ADRSUF","ADRSUF2"]]
    address = " ".join(p for p in parts if p).strip()
    return re.sub(r"\s+", " ", address) if address else safe_pick(row, LIKELY_PROP_ADDR_KEYS)


def build_prop_city_from_row(row: dict) -> str:
    return clean_text(row.get("UDATE1")) or clean_text(row.get("CITY")) or safe_pick(row, LIKELY_PROP_CITY_KEYS)


def build_prop_zip_from_row(row: dict) -> str:
    n2 = clean_text(row.get("NOTE2"))
    if n2 and re.fullmatch(r"\d{5}", n2): return n2
    u2 = clean_text(row.get("USER2"))
    if u2 and re.fullmatch(r"\d{5}", u2): return u2
    zr = clean_text(row.get("ZIPCD"))
    m = re.search(r"(\d{5})", zr)
    if m: return m.group(1)
    fb = safe_pick(row, LIKELY_PROP_ZIP_KEYS)
    m2 = re.search(r"(\d{5})", fb)
    return m2.group(1) if m2 else ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records",    default=str(DATA_DIR / "records.json"))
    parser.add_argument("--parcels",    default=str(DEBUG_DIR / "sc705_sc731_parcel_sample_rows.json"))
    parser.add_argument("--owner-index",dest="owner_index", default=str(DEBUG_DIR / "owner_values_sample.json"))
    parser.add_argument("--out-json",   dest="out_json",    default=str(DEFAULT_ENRICHED_JSON_PATH))
    parser.add_argument("--out-csv",    dest="out_csv",     default=str(DEFAULT_ENRICHED_CSV_PATH))
    parser.add_argument("--report",     dest="report",      default=str(DEFAULT_REPORT_PATH))
    return parser.parse_args()


def normalize_candidate_record(record: dict) -> dict:
    aliases, seen, clean_aliases = record.get("owner_aliases") or [], set(), []
    for alias in aliases:
        a = clean_text(alias)
        if a and a not in seen:
            seen.add(a); clean_aliases.append(a)
    return {
        "parcel_id":    clean_text(record.get("parcel_id")),
        "owner":        clean_text(record.get("owner")),
        "owner_aliases":clean_aliases,
        "prop_address": clean_text(record.get("prop_address")),
        "prop_city":    clean_text(record.get("prop_city")),
        "prop_zip":     clean_text(record.get("prop_zip")),
        "mail_address": clean_text(record.get("mail_address")),
        "mail_city":    clean_text(record.get("mail_city")),
        "mail_state":   normalize_state(clean_text(record.get("mail_state"))),
        "mail_zip":     clean_text(record.get("mail_zip")),
        "legal":        clean_text(record.get("legal")),
        "luc":          clean_text(record.get("luc")),
        "acres":        clean_text(record.get("acres")),
    }


def add_candidate(index: Dict[str, List[dict]], key: str, record: dict) -> None:
    k = clean_text(key)
    if k: index[k].append(record)


def add_owner_alias(record: dict, owner_name: str) -> None:
    owner_name = clean_text(owner_name)
    if not owner_name: return
    record.setdefault("owner_aliases", [])
    if owner_name not in record["owner_aliases"]:
        record["owner_aliases"].append(owner_name)
    if not clean_text(record.get("owner")):
        record["owner"] = owner_name


def is_sc701_clerk_code(value: str) -> bool:
    v = clean_text(value).upper()
    if not v: return True
    for pat in [r"^EOY\s+ROLL$",r"^CL_",r"^[A-Z]+WALKER$",r"^[A-Z]+WHITE$",r"^[A-Z]+JARJABKA$",r"^SCLB$",r"^LMRK$"]:
        if re.match(pat, v): return True
    return bool(re.match(r"\d{1,2}-[A-Z]{3}-\d{4}", v))


def extract_owner_aliases_from_row(row: dict) -> List[str]:
    aliases: List[str] = []
    is_mail_row = "MAIL_ADR1" in row or "MAIL_PTR" in row
    if not is_mail_row:
        for key in ["OWNER1","OWNER2","OWNER","OWN1","OWNER_NAME","OWNERNAME","OWNERNM",
                    "OWNER 1","OWNER 2","TAXPAYER","TAXPAYER_NAME","MAILNAME","MAIL_NAME","NAME1","NAME2"]:
            val = safe_pick(row, [key])
            if val: aliases.append(val)
        combined = build_owner_name(row)
        if combined: aliases.append(combined)

    deduped, seen = [], set()
    for alias in aliases:
        alias = clean_text(alias)
        if not alias or alias in BAD_EXACT_OWNERS: continue
        if is_sc701_clerk_code(alias): continue
        alias_u = normalize_name(alias)
        if re.fullmatch(r"\d{1,2}-[A-Z]{3}-\d{4}", alias_u): continue
        if re.fullmatch(r"\d{5}", alias_u): continue
        if re.fullmatch(r"[A-Z0-9_]+", alias_u) and "_" in alias_u: continue
        if len(alias_u) < 4: continue
        toks = tokens_from_name(alias_u)
        if not toks: continue
        if len(toks) == 1 and toks[0] in {"AKRON","BARBERTON","STOW","HUDSON","TWINSBURG","TALLMADGE",
                                           "CUYAHOGA","FALLS","MUNROE","SPRINGFIELD","NORTHFIELD"}: continue
        if not likely_corporate_name(alias_u) and len(toks) < 2: continue
        if alias_u not in seen:
            seen.add(alias_u); deduped.append(alias_u)
    return deduped


def build_parcel_indexes() -> Tuple[Dict,Dict,Dict,List[dict],Dict[str,dict]]:
    urls = discover_cama_downloads()
    own_rows, mail_rows, legal_rows, parcel_rows = [], [], [], []
    for url in urls:
        try:
            response = retry_request(url)
            datasets = read_any_cama_payload(response.content, Path(url).name)
            for fname, rows in datasets.items():
                upper = fname.upper()
                if "SC700" in upper:   own_rows.extend(rows)
                elif "SC701" in upper: mail_rows.extend(rows)
                elif "SC702" in upper: legal_rows.extend(rows)
                elif any(x in upper for x in ["SC705","SC731","SC720"]): parcel_rows.extend(rows)
            logging.info("Loaded CAMA source %s", url)
        except Exception as exc:
            logging.warning("Could not process CAMA file %s: %s", url, exc)

    save_debug_json("sc700_owndat_sample_rows.json",       own_rows[:25])
    save_debug_json("sc701_maildat_sample_rows.json",      mail_rows[:25])
    save_debug_json("sc702_legdat_sample_rows.json",       legal_rows[:25])
    save_debug_json("sc705_sc731_parcel_sample_rows.json", parcel_rows[:25])

    mail_by_pid: Dict[str, dict] = {}
    for row in mail_rows:
        pid = get_pid(row)
        if pid and pid not in mail_by_pid:
            mail_by_pid[pid] = row

    parcel_by_id: Dict[str, dict] = {}

    for row in parcel_rows:
        pid = get_pid(row)
        if not pid: continue
        parcel_by_id.setdefault(pid, {"parcel_id": pid, "owner_aliases": []})
        e = parcel_by_id[pid]
        e.update({
            "parcel_id":   pid,
            "prop_address":e.get("prop_address") or build_prop_address_from_row(row),
            "prop_city":   e.get("prop_city")    or build_prop_city_from_row(row),
            "prop_zip":    e.get("prop_zip")     or build_prop_zip_from_row(row),
            "luc":         e.get("luc")          or clean_text(row.get("LUC")),
            "acres":       e.get("acres")        or clean_text(row.get("ACRES")),
        })
        for alias in extract_owner_aliases_from_row(row): add_owner_alias(e, alias)

    for row in own_rows:
        pid = get_pid(row)
        if not pid: continue
        parcel_by_id.setdefault(pid, {"parcel_id": pid, "owner_aliases": []})
        for alias in extract_owner_aliases_from_row(row): add_owner_alias(parcel_by_id[pid], alias)

    for row in mail_rows:
        pid = get_pid(row)
        if not pid: continue
        parcel_by_id.setdefault(pid, {"parcel_id": pid, "owner_aliases": []})
        e = parcel_by_id[pid]
        ms  = clean_text(row.get("MAIL_ADR1")) or safe_pick(row, ["MAIL_ADR1","MAIL_ADDR","MAILADR1"])
        mc  = build_mail_city_sc701(row)
        mz  = build_mail_zip(row)
        mst = build_mail_state_sc701(row)
        if not e.get("mail_address") and ms:  e["mail_address"] = ms
        if not e.get("mail_city")    and mc:  e["mail_city"]    = mc
        if not e.get("mail_zip")     and mz:  e["mail_zip"]     = mz
        if not e.get("mail_state")   and mst: e["mail_state"]   = mst
        if not e.get("prop_city")    and mc:  e["prop_city"]    = mc

    for row in legal_rows:
        pid = get_pid(row)
        if not pid: continue
        parcel_by_id.setdefault(pid, {"parcel_id": pid, "owner_aliases": []})
        e = parcel_by_id[pid]
        e["legal"] = e.get("legal") or safe_pick(row, LIKELY_LEGAL_KEYS)
        for alias in extract_owner_aliases_from_row(row): add_owner_alias(e, alias)

    owner_index: Dict[str,List[dict]]      = defaultdict(list)
    last_name_index: Dict[str,List[dict]]  = defaultdict(list)
    first_last_index: Dict[str,List[dict]] = defaultdict(list)
    normalized_records = []
    seen_pid_last = defaultdict(set)
    seen_pid_fl   = defaultdict(set)
    seen_pid_own  = defaultdict(set)

    for raw_record in parcel_by_id.values():
        record = normalize_candidate_record(raw_record)
        all_aliases = list(record.get("owner_aliases") or [])
        owner = clean_text(record.get("owner"))
        if owner and owner not in all_aliases: all_aliases.append(owner)
        if not all_aliases: continue
        normalized_records.append(record)
        for alias_name in all_aliases:
            is_corp = likely_corporate_name(alias_name)
            chunks = split_owner_chunks(alias_name) or [alias_name]
            for chunk in chunks:
                variants = name_variants(chunk) or [normalize_person_name(chunk)]
                for variant in variants:
                    toks = tokens_from_name(variant)
                    if not is_corp and len(toks) < 2: continue
                    pid = record.get("parcel_id") or ""
                    if pid and pid in seen_pid_own[variant]: continue
                    if pid: seen_pid_own[variant].add(pid)
                    add_candidate(owner_index, variant, record)
                ln = get_last_name(chunk)
                fn = get_first_name(chunk)
                if ln:
                    pid = record.get("parcel_id") or ""
                    if not pid or pid not in seen_pid_last[ln]:
                        if pid: seen_pid_last[ln].add(pid)
                        last_name_index[ln].append(record)
                if fn and ln:
                    fl = f"{fn} {ln}"
                    pid = record.get("parcel_id") or ""
                    if not pid or pid not in seen_pid_fl[fl]:
                        if pid: seen_pid_fl[fl].add(pid)
                        first_last_index[fl].append(record)

    save_debug_json("parcel_by_id_sample.json",       normalized_records[:50])
    save_debug_json("owner_index_sample.json",         {k: v[:3] for k, v in list(owner_index.items())[:300]})
    save_debug_json("owner_values_sample.json",        list(owner_index.keys())[:5000])
    save_debug_json("last_name_index_sample.json",     {k: v[:3] for k, v in list(last_name_index.items())[:300]})
    logging.info(
        "Built parcel index: %s owner-name keys | %s parcels | %s owner rows | %s mail rows | %s legal rows",
        len(owner_index), len(parcel_rows), len(own_rows), len(mail_rows), len(legal_rows)
    )
    return owner_index, last_name_index, first_last_index, parcel_rows, mail_by_pid


# -----------------------------------------------------------------------
# PLAYWRIGHT SCRAPING
# -----------------------------------------------------------------------
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
    if any(x in t for x in ["LIS PENDENS"," LP ","LP-"]):           return "LP"
    if any(x in t for x in ["NOTICE OF FORECLOSURE","FORECLOS","NOFC"]): return "NOFC"
    if any(x in t for x in ["CERTIFIED JUDGMENT","DOMESTIC JUDGMENT","JUDGMENT"]): return "JUD"
    if any(x in t for x in ["TAX DEED","TAXDEED"]):                  return "TAXDEED"
    if any(x in t for x in ["IRS LIEN","FEDERAL LIEN","TAX LIEN"]):  return "LNFED"
    if "MECHANIC LIEN" in t:  return "LNMECH"
    if "LIEN" in t:           return "LN"
    if "NOTICE OF COMMENCEMENT" in t: return "NOC"
    return None


def try_parse_date(text: str) -> Optional[str]:
    text = clean_text(text)
    if not text: return None
    for pattern in [r"\b\d{4}-\d{2}-\d{2}\b", r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", r"\b\d{1,2}-\d{1,2}-\d{2,4}\b"]:
        match = re.search(pattern, text)
        if match:
            raw = match.group(0)
            for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%m-%d-%Y","%m-%d-%y"):
                try: return datetime.strptime(raw, fmt).date().isoformat()
                except ValueError: continue
    return None


def extract_case_number(text: str, fallback: str) -> str:
    text_u = clean_text(text).upper()
    for pattern in [r"\b\d{2,4}[ -][A-Z]{1,6}[ -]\d{2,8}\b", r"\b[A-Z]{2,}[ ]\d{2}\b", r"\b\d{6,}\b"]:
        m = re.search(pattern, text_u)
        if m: return clean_text(m.group(0))
    return fallback


def split_caption(caption: str) -> Tuple[str,str]:
    cap = clean_text(caption)
    upper = cap.upper()
    for sep in [" -VS- "," VS. "," VS "," V. "," V "]:
        if sep in upper:
            parts = re.split(re.escape(sep), cap, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                return clean_text(parts[0]), clean_text(parts[1])
    return "", ""


def clean_defendant_name(name: str) -> str:
    n = clean_text(name)
    if not n: return ""
    for pat in [r"\bAKA\b.*$",r"\bET AL\b.*$",r"\bUNKNOWN HEIRS OF\b",r"\bUNKNOWN SPOUSE OF\b",
                r"\bUNKNOWN ADMINISTRATOR\b",r"\bEXECUTOR\b",r"\bFIDUCIARY\b",r"\bJOHN DOE\b",r"\bJANE DOE\b"]:
        n = re.sub(pat, "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"\s+", " ", n).strip(" ,.-")
    return "" if (not n or n in BAD_EXACT_OWNERS) else n


def looks_like_good_owner(name: str) -> bool:
    n = clean_text(name)
    if not n or n in BAD_EXACT_OWNERS or len(n) < 4: return False
    return sum(ch.isalpha() for ch in n) >= 4


def extract_owner_and_grantee(cells: List[str]) -> Tuple[str,str,str]:
    row_text = clean_text(" ".join(cells))
    for candidate in cells + [row_text]:
        plaintiff, defendant = split_caption(candidate)
        defendant = clean_defendant_name(defendant)
        if looks_like_good_owner(defendant):
            return defendant.title(), clean_text(plaintiff).title(), candidate
    return "", "", row_text


def parse_pending_civil_table(html: str, base_url: str, prefix: str) -> List[LeadRecord]:
    soup = BeautifulSoup(html, "lxml")
    records: List[LeadRecord] = []
    debug_rows: List[List[str]] = []
    for t_idx, table in enumerate(soup.find_all("table"), 1):
        for r_idx, row in enumerate(table.find_all("tr"), 1):
            cells = [clean_text(td.get_text(" ")) for td in row.find_all(["td","th"])]
            if not cells: continue
            debug_rows.append(cells[:10])
            row_text = clean_text(" ".join(cells))
            doc_type = infer_doc_type_from_text(row_text)
            if doc_type not in {"NOFC","LP","JUD","LN","LNMECH","LNFED","NOC"}: continue
            filed = try_parse_date(row_text) or datetime.now().date().isoformat()
            if datetime.fromisoformat(filed).date() < (datetime.now().date() - timedelta(days=LOOKBACK_DAYS)): continue
            owner, grantee, source_caption = extract_owner_and_grantee(cells)
            if not owner: continue
            am = re.search(r"\$[\d,]+(?:\.\d{2})?", row_text)
            amount = parse_amount(am.group(0)) if am else None
            link = row.find("a", href=True)
            href = clean_text(link.get("href")) if link else ""
            doc_num = extract_case_number(row_text, f"{prefix}-T{t_idx}-R{r_idx}")
            record = LeadRecord(
                doc_num=doc_num, doc_type=doc_type, filed=filed, cat=doc_type,
                cat_label=LEAD_TYPE_MAP.get(doc_type, doc_type),
                owner=owner, grantee=grantee, amount=amount, legal=clean_text(source_caption),
                clerk_url=requests.compat.urljoin(base_url, href) if href else base_url,
            )
            record.flags = category_flags(record.doc_type, record.owner)
            ds = classify_distress_source(doc_type)
            if ds: record.distress_sources = [ds]
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
        if await click_first_matching(page, ["text=Search","text=Begin","text=Continue","input[type='submit']","button","a"]):
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
            save_debug_text("clerk_records_page_1.html", await page.content())
            if await click_first_matching(page, ["text=Click Here","text=Begin","text=Continue","text=Accept","text=Search","input[type='submit']","button","a"]):
                save_debug_text("clerk_records_page_2.html", await page.content())
                if await click_first_matching(page, ["text=Civil","text=General","text=Search","input[type='submit']","button","a"]):
                    save_debug_text("clerk_records_page_3.html", await page.content())
            records.extend(await scrape_pending_civil_records(page))
        except PlaywrightTimeoutError:
            logging.warning("Timeout while scraping clerk records.")
        except Exception as exc:
            logging.warning("Clerk scrape failed: %s", exc)
        finally:
            await browser.close()
    deduped, seen = [], set()
    for record in records:
        nd = re.sub(r"^(PCF1|PCF2)-", "", clean_text(record.doc_num).upper())
        key = (nd, clean_text(record.doc_type).upper(), normalize_name(record.owner), clean_text(record.filed))
        if key in seen: continue
        seen.add(key); deduped.append(record)
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
            await page.wait_for_timeout(4000)
            save_debug_text("probate_page_1.html", await page.content())
            if await click_first_matching(page, ["text=Click Here","text=Begin","text=Search","a.anchorButton","input[type='submit']","button","a"]):
                save_debug_text("probate_page_2.html", await page.content())
                soup = BeautifulSoup(await page.content(), "lxml")
                for i, row in enumerate(soup.find_all("tr")):
                    text = clean_text(row.get_text(" "))
                    if not any(x in text.upper() for x in ["ESTATE OF","IN THE MATTER OF","GUARDIANSHIP","DECEDENT","FIDUCIARY","ESTATE"]): continue
                    link = row.find("a", href=True)
                    href = clean_text(link.get("href")) if link else ""
                    filed = try_parse_date(text) or datetime.now().date().isoformat()
                    record = LeadRecord(
                        doc_num=f"PRO-TR-{i+1}", doc_type="PRO", filed=filed,
                        cat="PRO", cat_label=LEAD_TYPE_MAP["PRO"],
                        owner=text[:180], grantee="", amount=None, legal="",
                        clerk_url=requests.compat.urljoin(page.url, href) if href else page.url,
                    )
                    record.flags = category_flags(record.doc_type, record.owner)
                    record.distress_sources = ["probate"]
                    record.score = score_record(record)
                    records.append(record)
        except Exception as exc:
            logging.warning("Probate scrape failed: %s", exc)
        finally:
            await browser.close()
    deduped, seen = [], set()
    for record in records:
        key = (clean_text(record.doc_num), normalize_name(record.owner), clean_text(record.filed))
        if key in seen: continue
        seen.add(key); deduped.append(record)
    logging.info("Collected %s probate records", len(deduped))
    return deduped


# -----------------------------------------------------------------------
# PARCEL MATCHING
# -----------------------------------------------------------------------
def better_record(candidate: dict) -> int:
    score = 0
    if clean_text(candidate.get("prop_address")): score += 100
    if clean_text(candidate.get("mail_address")): score += 40
    if clean_text(candidate.get("mail_zip")):     score += 20
    if clean_text(candidate.get("mail_city")):    score += 15
    if clean_text(candidate.get("legal")):        score += 15
    if clean_text(candidate.get("prop_city")):    score += 10
    if clean_text(candidate.get("prop_zip")):     score += 10
    return score


def alias_list(candidate: dict) -> List[str]:
    aliases = list(candidate.get("owner_aliases") or [])
    owner = clean_text(candidate.get("owner"))
    if owner and owner not in aliases: aliases.append(owner)
    return aliases


def candidate_match_score(record_owner: str, candidate: dict) -> float:
    best = 0.0
    for co in alias_list(candidate):
        rt = set(tokens_from_name(record_owner))
        ct = set(tokens_from_name(co))
        if not rt or not ct: continue
        score = len(rt & ct) * 10.0
        rl, cl = get_last_name(record_owner), get_last_name(co)
        rf, cf = get_first_name(record_owner), get_first_name(co)
        if rl and cl and last_names_compatible(rl, cl): score += 25.0
        if rf and cf and rf == cf: score += 18.0
        elif same_first_name_or_initial(record_owner, co): score += 10.0
        if clean_text(candidate.get("prop_address")): score += 8.0
        if clean_text(candidate.get("mail_address")): score += 4.0
        if score > best: best = score
    return best


def choose_best_candidate(candidates: List[dict], record_owner: str = "") -> Optional[dict]:
    if not candidates: return None
    deduped = {}
    for c in candidates:
        key = clean_text(c.get("parcel_id")) or f"{clean_text(c.get('owner'))}|{clean_text(c.get('prop_address'))}"
        if key not in deduped or better_record(c) > better_record(deduped[key]): deduped[key] = c
    ranked = sorted(deduped.values(), key=lambda c: (candidate_match_score(record_owner, c), better_record(c)), reverse=True)
    return ranked[0] if ranked else None


def unique_best_by_score(candidates: List[dict], record_owner: str, min_gap: float = 12.0) -> Optional[dict]:
    if not candidates: return None
    deduped = {}
    for c in candidates:
        key = clean_text(c.get("parcel_id")) or f"{clean_text(c.get('owner'))}|{clean_text(c.get('prop_address'))}"
        if key not in deduped or better_record(c) > better_record(deduped[key]): deduped[key] = c
    ranked = sorted([(c, candidate_match_score(record_owner, c), better_record(c)) for c in deduped.values()],
                    key=lambda x: (x[1],x[2]), reverse=True)
    if not ranked: return None
    if len(ranked) == 1: return ranked[0][0]
    if ranked[0][1] >= ranked[1][1] + min_gap: return ranked[0][0]
    return None


def fuzzy_match_record(record: LeadRecord, owner_index, last_name_index, first_last_index) -> Tuple[Optional[dict],str,float]:
    owner = record.owner
    owner_variants = name_variants(owner)
    is_corp = likely_corporate_name(owner)
    for variant in owner_variants:
        if not is_corp and len(tokens_from_name(variant)) < 2: continue
        best = choose_best_candidate(owner_index.get(variant, []), owner)
        if best: return best, "exact_name_variant", 1.0
    fn, ln = get_first_name(owner), get_last_name(owner)
    ot = set(tokens_from_name(owner))
    if fn and ln:
        candidates = first_last_index.get(f"{fn} {ln}", [])
        best = unique_best_by_score(candidates, owner, 8.0) or choose_best_candidate(candidates, owner)
        if best: return best, "first_last_fallback", 0.95
    if fn and ln:
        best = choose_best_candidate(owner_index.get(f"{ln} {fn}", []), owner)
        if best: return best, "last_first_variant", 0.94
    if ln and not is_corp:
        candidates = last_name_index.get(ln, [])
        strong = [c for c in candidates if any(
            last_names_compatible(ln, get_last_name(co)) and
            len(ot & set(tokens_from_name(co))) >= 2 and
            same_first_name_or_initial(owner, co)
            for co in alias_list(c)
        )]
        best = unique_best_by_score(strong, owner, 6.0) or choose_best_candidate(strong, owner)
        if best: return best, "token_overlap_strict", 0.90
        unique_c, seen = [], set()
        for c in candidates:
            if not any(last_names_compatible(ln, get_last_name(co)) for co in alias_list(c)): continue
            key = clean_text(c.get("parcel_id")) or f"{clean_text(c.get('owner'))}|{clean_text(c.get('prop_address'))}"
            if key in seen: continue
            seen.add(key); unique_c.append(c)
        if len(unique_c) == 1: return unique_c[0], "last_name_unique_fallback", 0.82
        init_c = [c for c in unique_c if any(same_first_name_or_initial(owner, co) for co in alias_list(c))]
        best = unique_best_by_score(init_c, owner, 8.0)
        if best: return best, "last_name_initial_fallback", 0.84
        if candidates: return None, "no_property_match", 0.0
    return None, "unmatched", 0.0


def enrich_with_parcel_data(records, owner_index, last_name_index, first_last_index):
    enriched: List[LeadRecord] = []
    report = {
        "matched":0,"unmatched":0,"with_address":0,"with_mail_address":0,
        "match_methods":defaultdict(int),"sample_unmatched":[],"sample_no_property_match":[],
    }
    for record in records:
        try:
            matched, method, match_score = fuzzy_match_record(record, owner_index, last_name_index, first_last_index)
            if matched:
                record.prop_address = record.prop_address or clean_text(matched.get("prop_address"))
                record.prop_city    = record.prop_city    or clean_text(matched.get("prop_city"))
                record.prop_zip     = record.prop_zip     or clean_text(matched.get("prop_zip"))
                record.mail_address = record.mail_address or clean_text(matched.get("mail_address"))
                record.mail_city    = record.mail_city    or clean_text(matched.get("mail_city"))
                record.mail_zip     = record.mail_zip     or clean_text(matched.get("mail_zip"))
                record.legal        = record.legal        or clean_text(matched.get("legal"))
                record.mail_state   = record.mail_state   or normalize_state(clean_text(matched.get("mail_state"))) or "OH"
                record.luc          = record.luc          or clean_text(matched.get("luc"))
                record.acres        = record.acres        or clean_text(matched.get("acres"))
                record.match_method = method; record.match_score = match_score
                report["matched"] += 1; report["match_methods"][method] += 1
            else:
                record.match_method = method; record.match_score = 0.0
                report["unmatched"] += 1; report["match_methods"][method] += 1
                if method == "no_property_match":
                    if len(report["sample_no_property_match"]) < 25:
                        report["sample_no_property_match"].append({"doc_num":record.doc_num,"owner":record.owner})
                else:
                    if len(report["sample_unmatched"]) < 25:
                        report["sample_unmatched"].append({"doc_num":record.doc_num,"owner":record.owner})
            if record.luc in VACANT_LAND_LUCS:
                record.is_vacant_land = True
                if "Vacant land" not in record.flags: record.flags.append("Vacant land")
            record.mail_state   = normalize_state(record.mail_state) or ("OH" if record.mail_address else "")
            record.with_address = 1 if clean_text(record.prop_address) else 0

            record.is_absentee = is_absentee_owner(record.prop_address, record.mail_address)
            if record.is_absentee and "Absentee owner" not in record.flags:
                record.flags.append("Absentee owner")
            if record.with_address: report["with_address"] += 1
            if clean_text(record.mail_address): report["with_mail_address"] += 1
            record.flags = list(dict.fromkeys(record.flags + category_flags(record.doc_type, record.owner)))
            if record.match_method == "no_property_match" and "No property match" not in record.flags:
                record.flags.append("No property match")
            record.score = score_record(record)
            enriched.append(record)
        except Exception as exc:
            logging.warning("Failed to enrich record %s: %s", record.doc_num, exc)
            record.match_method = "unmatched"; record.match_score = 0.0
            record.with_address = 1 if clean_text(record.prop_address) else 0
            enriched.append(record)
    report["match_methods"] = dict(report["match_methods"])
    return enriched, report


def dedupe_records(records: List[LeadRecord]) -> List[LeadRecord]:
    final, seen = [], set()
    for record in records:
        nd = re.sub(r"^(PCF1|PCF2)-", "", clean_text(record.doc_num).upper())
        key = (nd, clean_text(record.doc_type).upper(), normalize_name(record.owner), clean_text(record.filed))
        if key in seen: continue
        seen.add(key); final.append(record)
    return final


# -----------------------------------------------------------------------
# OUTPUT WRITERS
# -----------------------------------------------------------------------
def build_payload(records: List[LeadRecord]) -> dict:
    hot = [r for r in records if r.hot_stack]
    return {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "date_range": {
            "from": (datetime.now() - timedelta(days=LOOKBACK_DAYS)).date().isoformat(),
            "to": datetime.now().date().isoformat(),
        },
        "total": len(records),
        "with_address": sum(1 for r in records if clean_text(r.prop_address)),
        "with_mail_address": sum(1 for r in records if clean_text(r.mail_address)),
        "hot_stack_count": len(hot),
        "vacant_home_count": sum(1 for r in records if r.is_vacant_home),
        "records": [asdict(r) for r in records],
    }


def write_json_outputs(records: List[LeadRecord], extra_json_path: Optional[Path] = None) -> None:
    payload = build_payload(records)
    paths = list(DEFAULT_OUTPUT_JSON_PATHS)
    if extra_json_path: paths.append(extra_json_path)
    seen = set()
    for path in paths:
        if str(path) in seen: continue
        seen.add(str(path))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.info("Wrote JSON outputs.")


def write_vacant_land_json(vacant: List[VacantLandRecord]) -> None:
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "total": len(vacant),
        "description": "Distressed vacant infill lots ≤ 2 acres — in foreclosure or tax delinquent only",
        "records": [asdict(r) for r in vacant],
    }
    for path in [DEFAULT_VACANT_JSON_PATH, DASHBOARD_DIR / "vacant_land.json"]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.info("Wrote vacant land JSON: %s records", len(vacant))


def write_hot_stack_json(records: List[LeadRecord]) -> None:
    hot = sorted([r for r in records if r.hot_stack], key=lambda r: (r.distress_count, r.score), reverse=True)
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "total": len(hot),
        "description": "Properties in 2+ distress sources — highest priority leads",
        "records": [asdict(r) for r in hot],
    }
    for path in [DEFAULT_STACK_JSON_PATH, DASHBOARD_DIR / "hot_stack.json"]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.info("Wrote hot stack JSON: %s records", len(hot))


def write_vacant_home_json(records: List[LeadRecord]) -> None:
    """Write standalone vacant home leads to their own JSON for the dashboard tab."""
    vacant_homes = sorted(
        [r for r in records if r.is_vacant_home],
        key=lambda r: (r.hot_stack, r.score), reverse=True
    )
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "total": len(vacant_homes),
        "description": "Vacant residential homes from Akron Vacant Building Board",
        "records": [asdict(r) for r in vacant_homes],
    }
    for path in [DATA_DIR / "vacant_homes.json", DASHBOARD_DIR / "vacant_homes.json"]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.info("Wrote vacant homes JSON: %s records", len(vacant_homes))


def split_name(full_name: str) -> Tuple[str, str]:
    parts = clean_text(full_name).split()
    if not parts: return "", ""
    if len(parts) == 1: return parts[0], ""
    return parts[0], " ".join(parts[1:])


def write_csv(records: List[LeadRecord], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Distress Sources","Distress Count","Hot Stack",
        "Vacant Land","Vacant Home","Absentee Owner",
        "Phone 1","Phone 1 Type","Phone 2","Phone 2 Type","Phone 3","Phone 3 Type",
        "Email","Skip Trace Source","LUC Code","Acres","Match Method","Match Score","Source","Public Records URL",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            first, last = split_name(record.owner)
            writer.writerow({
                "First Name":first,"Last Name":last,
                "Mailing Address":record.mail_address,"Mailing City":record.mail_city,
                "Mailing State":record.mail_state,"Mailing Zip":record.mail_zip,
                "Property Address":record.prop_address,"Property City":record.prop_city,
                "Property State":record.prop_state,"Property Zip":record.prop_zip,
                "Lead Type":record.cat_label,"Document Type":record.doc_type,
                "Date Filed":record.filed,"Document Number":record.doc_num,
                "Amount/Debt Owed":record.amount if record.amount is not None else "",
                "Seller Score":record.score,
                "Motivated Seller Flags":"; ".join(record.flags),
                "Distress Sources":"; ".join(record.distress_sources),
                "Distress Count":record.distress_count,
                "Hot Stack":"YES" if record.hot_stack else "",
                "Vacant Land":"YES" if record.is_vacant_land else "",
                "Vacant Home":"YES" if record.is_vacant_home else "",
                "Absentee Owner":"YES" if record.is_absentee else "",
                "Phone 1":record.phones[0] if len(record.phones) > 0 else "",
                "Phone 1 Type":record.phone_types[0] if len(record.phone_types) > 0 else "",
                "Phone 2":record.phones[1] if len(record.phones) > 1 else "",
                "Phone 2 Type":record.phone_types[1] if len(record.phone_types) > 1 else "",
                "Phone 3":record.phones[2] if len(record.phones) > 2 else "",
                "Phone 3 Type":record.phone_types[2] if len(record.phone_types) > 2 else "",
                "Email":record.emails[0] if record.emails else "",
                "Skip Trace Source":record.skip_trace_source,
                "LUC Code":record.luc,"Acres":record.acres,
                "Match Method":record.match_method,"Match Score":record.match_score,
                "Source":SOURCE_NAME,"Public Records URL":record.clerk_url,
            })
    logging.info("Wrote CSV: %s", csv_path)


def write_report(report: dict, report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logging.info("Wrote report: %s", report_path)


def build_tax_delinquent_leads(
    delinquent_parcels: Dict[str, dict],
    parcel_rows: List[dict],
    mail_by_pid: Dict[str, dict],
    vacant_land_pids: set,
) -> List[LeadRecord]:
    """
    Build RESIDENTIAL-ONLY tax delinquent leads from Akron Legal News.
    
    KEY CHANGE: Now filters to RESIDENTIAL LUCs only (510-560).
    Vacant land (500-503) is handled separately in the vacant land list.
    This cuts ~3,600 records down to ~500-800 residential properties.
    """
    leads: List[LeadRecord] = []
    skipped_non_residential = 0

    pid_to_row: Dict[str, dict] = {}
    for row in parcel_rows:
        pid = get_pid(row)
        if pid:
            pid_to_row[pid] = row

    for pid, delin_info in delinquent_parcels.items():
        row = pid_to_row.get(pid)
        if not row:
            continue

        # NEW: Only residential properties for the tax delinquent leads tab
        luc = clean_text(row.get("LUC", ""))
        if luc not in RESIDENTIAL_LUCS:
            skipped_non_residential += 1
            continue

        prop_address = build_prop_address_from_row(row)
        prop_city = build_prop_city_from_row(row)
        prop_zip = build_prop_zip_from_row(row)
        if not prop_address:
            continue

        mail_row = mail_by_pid.get(pid, {})
        mail_address = clean_text(mail_row.get("MAIL_ADR1", ""))
        mail_city = clean_text(mail_row.get("NOTE1", "")).title()
        mail_zip = clean_text(mail_row.get("MAIL_PTR", ""))
        mail_state = "OH"

        owner = delin_info.get("owner", "")
        amount_owed = delin_info.get("amount_owed", 0.0)
        acres = clean_text(row.get("ACRES", ""))
        is_absentee = is_absentee_owner(prop_address, mail_address)

        flags = ["Tax delinquent", "Residential"]
        if is_absentee:
            flags.append("Absentee owner")
        if amount_owed and amount_owed > 10000:
            flags.append("High tax debt")

        distress_sources = ["tax_delinquent"]

        record = LeadRecord(
            doc_num=f"TAX-{pid}",
            doc_type="TAX",
            filed="",
            cat="TAX",
            cat_label="Tax Delinquent",
            owner=owner.title(),
            amount=amount_owed,
            prop_address=prop_address,
            prop_city=prop_city,
            prop_state="OH",
            prop_zip=prop_zip,
            mail_address=mail_address,
            mail_city=mail_city,
            mail_state=mail_state,
            mail_zip=mail_zip,
            clerk_url=delin_info.get("source_url", ""),
            flags=flags,
            distress_sources=distress_sources,
            distress_count=len(distress_sources),
            luc=luc,
            acres=acres,
            is_vacant_land=False,
            is_absentee=is_absentee,
            with_address=1,
            match_method="tax_delinquent_direct",
            match_score=1.0,
        )
        record.score = score_record(record)
        leads.append(record)

    logging.info(
        "Built %s residential tax delinquent leads | Skipped %s non-residential parcels",
        len(leads), skipped_non_residential
    )
    return leads


# -----------------------------------------------------------------------
# SKIP TRACING — RESIDENTIAL PRIORITY ONLY
# -----------------------------------------------------------------------
async def skip_trace_one(page, first: str, last: str, city: str, state: str = "OH") -> dict:
    """
    Find phone numbers for one person using free public people search sites.
    Sources: TruePeopleSearch → FastPeopleSearch → CyberBackgroundChecks.
    """
    result = {"phones": [], "emails": [], "source": None, "skip_traced": False}
    phone_pattern = re.compile(r"\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}")

    name_slug = f"{first}-{last}".lower().replace(" ", "-")
    city_slug = city.lower().replace(" ", "-")

    sources = [
        {
            "name": "TruePeopleSearch",
            "url": f"https://www.truepeoplesearch.com/results?name={first}+{last}&citystatezip={city}+{state}",
            "phone_selector": "span[itemprop='telephone']",
            "email_selector": "a[href^='mailto']",
        },
        {
            "name": "FastPeopleSearch",
            "url": f"https://www.fastpeoplesearch.com/name/{name_slug}_{city_slug}-{state.lower()}",
            "phone_selector": "a[href^='tel']",
            "email_selector": "a[href^='mailto']",
        },
        {
            "name": "CyberBackgroundChecks",
            "url": f"https://www.cyberbackgroundchecks.com/people/{first}/{last}/{state.lower()}",
            "phone_selector": "span.phone",
            "email_selector": "span.email",
        },
    ]

    for source in sources:
        try:
            await page.goto(source["url"], wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(random.randint(3000, 7000))

            content = await page.content()
            soup = BeautifulSoup(content, "lxml")

            page_text = soup.get_text(" ").lower()
            if any(x in page_text for x in ["access denied", "blocked", "captcha", "robot", "unusual traffic"]):
                logging.warning("Skip trace blocked on %s", source["name"])
                continue

            mobile_phones = []
            landline_phones = []
            emails = []

            def classify_phone(num: str, context: str = "") -> str:
                ctx = context.lower()
                if any(x in ctx for x in ["mobile", "cell", "wireless", "smartphone"]):
                    return "mobile"
                if any(x in ctx for x in ["landline", "home", "work", "voip", "business"]):
                    return "landline"
                return "unknown"

            def normalize_phone(raw: str) -> str:
                digits = re.sub(r"\D", "", raw)
                if len(digits) == 11 and digits.startswith("1"):
                    digits = digits[1:]
                if len(digits) == 10:
                    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                return raw.strip()

            for el in soup.select(source["phone_selector"]):
                raw = clean_text(el.get_text(" ") or el.get("href", "")).replace("tel:", "").strip()
                if not phone_pattern.search(raw):
                    continue
                num = normalize_phone(raw)
                parent_text = ""
                try:
                    parent = el.find_parent(["div", "li", "span", "p"])
                    if parent:
                        parent_text = parent.get_text(" ").lower()
                except Exception:
                    pass
                phone_type = classify_phone(num, parent_text)
                if phone_type == "mobile":
                    mobile_phones.append(num)
                else:
                    landline_phones.append(num)

            if not mobile_phones and not landline_phones:
                full_text = soup.get_text(" ")
                for m in re.finditer(
                    r"(mobile|cell|wireless|landline|home|work)[:\s]+(\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4})",
                    full_text, re.IGNORECASE
                ):
                    label = m.group(1).lower()
                    num = normalize_phone(m.group(2))
                    if label in ("mobile", "cell", "wireless"):
                        mobile_phones.append(num)
                    else:
                        landline_phones.append(num)

            if not mobile_phones and not landline_phones:
                for match in phone_pattern.finditer(soup.get_text(" ")):
                    digits = re.sub(r"\D", "", match.group())
                    if len(digits) == 10 and not digits.startswith("000"):
                        landline_phones.append(normalize_phone(match.group()))
                        if len(landline_phones) >= 4:
                            break

            for el in soup.select(source["email_selector"]):
                href = el.get("href", "").replace("mailto:", "").strip()
                text = clean_text(el.get_text(" "))
                email = href or text
                if "@" in email and "." in email:
                    emails.append(email)

            mobile_phones = list(dict.fromkeys(mobile_phones))
            landline_phones = list(dict.fromkeys(landline_phones))
            all_phones = (mobile_phones + landline_phones)[:3]

            if all_phones:
                result["phones"] = all_phones
                result["phone_types"] = (
                    ["mobile"] * min(len(mobile_phones), 3) +
                    ["landline"] * max(0, 3 - len(mobile_phones))
                )[:len(all_phones)]
                result["emails"] = list(dict.fromkeys(emails))[:2]
                result["source"] = source["name"]
                result["skip_traced"] = True
                logging.info(
                    "Skip traced %s %s via %s → %s mobile, %s landline",
                    first, last, source["name"], len(mobile_phones), len(landline_phones)
                )
                return result

        except PlaywrightTimeoutError:
            logging.warning("Skip trace timeout on %s for %s %s", source["name"], first, last)
            continue
        except Exception as exc:
            logging.warning("Skip trace error on %s: %s", source["name"], exc)
            continue

    return result


async def skip_trace_leads(records: List[LeadRecord]) -> List[LeadRecord]:
    """
    Skip trace ONLY high-priority residential leads.
    
    Filter criteria (must meet ALL):
    - Residential LUC (510-560) OR vacant home OR court filing
    - Has property address
    - Not already traced
    - Meets at least one priority trigger (see should_skip_trace)
    
    This replaces the old "trace everything with an address" approach
    that sent 3,635 leads through — cutting to ~50-300 max.
    """
    eligible = [r for r in records if not r.phones and should_skip_trace(r)]

    # Safety cap: never skip-trace more than 500 in one run
    MAX_SKIP_TRACE = 500
    if len(eligible) > MAX_SKIP_TRACE:
        # Prioritize by score
        eligible.sort(key=lambda r: (r.hot_stack, r.distress_count, r.score), reverse=True)
        eligible = eligible[:MAX_SKIP_TRACE]
        logging.info("Capped skip trace at %s leads (from %s eligible)", MAX_SKIP_TRACE, len(eligible))

    if not eligible:
        logging.info("No eligible leads for skip tracing")
        return records

    logging.info(
        "Skip tracing %s residential/priority leads (filtered from %s total with address)",
        len(eligible),
        sum(1 for r in records if r.prop_address and not r.phones)
    )
    traced = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()

        for record in eligible:
            try:
                name_parts = (record.owner or "").split()
                if len(name_parts) < 2:
                    continue
                first = name_parts[0]
                last = name_parts[-1]

                # Skip obvious corporate/LLC owners — no person to trace
                if likely_corporate_name(record.owner):
                    logging.debug("Skipping corporate owner for skip trace: %s", record.owner)
                    continue

                city = record.prop_city or record.mail_city or "Akron"

                skip_result = await skip_trace_one(page, first, last, city)

                if skip_result["skip_traced"]:
                    record.phones = skip_result["phones"]
                    record.phone_types = skip_result.get("phone_types", [])
                    record.emails = skip_result["emails"]
                    record.skip_trace_source = skip_result["source"]
                    if "Phone found" not in record.flags:
                        record.flags.append("Phone found")
                    traced += 1

                # Polite delay between leads
                await page.wait_for_timeout(random.randint(5000, 12000))

            except Exception as exc:
                logging.warning("Skip trace failed for %s: %s", record.owner, exc)
                continue

        await browser.close()

    logging.info("Skip traced %s/%s eligible leads", traced, len(eligible))
    return records


# -----------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------
async def main() -> None:
    args = parse_args()
    ensure_dirs()
    log_setup()
    logging.info("Starting Summit County scraper run...")

    # 1. Build parcel indexes from CAMA
    owner_index, last_name_index, first_last_index, parcel_rows, mail_by_pid = build_parcel_indexes()

    # 2. Scrape court records (clerk + probate)
    clerk_records   = await scrape_clerk_records()
    probate_records = await scrape_probate_records()

    # 3. Scrape Akron vacant building registry (residential homes only)
    vacant_addresses = scrape_vacant_building_addresses()

    # 4. Scrape tax delinquent list from Akron Legal News
    delinquent_parcels = scrape_tax_delinquent_parcels()
    delinquent_pid_set = set(delinquent_parcels.keys())

    # 5. Build set of foreclosure parcel IDs (for vacant land cross-reference)
    #    We need to enrich records first to get parcel IDs, so we do this after enrichment
    all_records = clerk_records + probate_records
    all_records, report = enrich_with_parcel_data(all_records, owner_index, last_name_index, first_last_index)

    # 6. Build parcel ID sets for cross-referencing
    vacant_land_pids = set(
        get_pid(row) for row in parcel_rows
        if clean_text(row.get("LUC")) in VACANT_LAND_LUCS and get_pid(row)
    )
    # Extract parcel IDs from foreclosure/LP court records (after enrichment via match)
    # We use doc_type to identify foreclosure leads and their prop_address → pid lookup
    pid_to_row_quick: Dict[str, dict] = {}
    for row in parcel_rows:
        pid = get_pid(row)
        if pid:
            pid_to_row_quick[pid] = row

    # Build address→pid map for foreclosure cross-ref
    addr_to_pid_map: Dict[str, str] = {}
    for row in parcel_rows:
        pid = get_pid(row)
        if not pid: continue
        addr = build_prop_address_from_row(row)
        if addr:
            addr_to_pid_map[normalize_address_key(addr)] = pid

    foreclosure_pids: set = set()
    for record in all_records:
        if record.doc_type in {"LP", "NOFC", "TAXDEED"} and record.prop_address:
            key = normalize_address_key(record.prop_address)
            pid = addr_to_pid_map.get(key)
            if pid:
                foreclosure_pids.add(pid)

    logging.info("Vacant land parcel IDs: %s | Foreclosure pids: %s", len(vacant_land_pids), len(foreclosure_pids))

    # 7. Build delinquent address lookup
    delinquent_addresses = build_delinquent_address_index(parcel_rows, mail_by_pid, delinquent_parcels)

    # 8. Build vacant home key set (addresses on Akron board matching residential LUCs)
    #    Used by apply_distress_stacking to flag records as vacant_home
    addr_to_pid_res: Dict[str, str] = {}
    for row in parcel_rows:
        pid = get_pid(row)
        if not pid: continue
        luc = clean_text(row.get("LUC", ""))
        if luc not in RESIDENTIAL_LUCS: continue
        addr = build_prop_address_from_row(row)
        if addr:
            addr_to_pid_res[normalize_address_key(addr)] = pid

    vacant_home_keys = set()
    for addr in vacant_addresses:
        key = normalize_address_key(addr)
        if key in addr_to_pid_res:
            vacant_home_keys.add(key)

    logging.info("Vacant home addresses matched to residential parcels: %s", len(vacant_home_keys))

    # 9. Apply distress stacking
    distress_index = build_distress_index(all_records, vacant_addresses, vacant_land_pids, delinquent_parcels)
    all_records = apply_distress_stacking(all_records, distress_index, delinquent_addresses, vacant_home_keys)

    # 10. Build standalone tax delinquent leads — RESIDENTIAL ONLY
    tax_delin_leads = build_tax_delinquent_leads(
        delinquent_parcels, parcel_rows, mail_by_pid, vacant_land_pids
    )
    all_records = all_records + tax_delin_leads
    logging.info("Total records after adding residential tax delinquent leads: %s", len(all_records))

    # 11. Build standalone vacant home leads (from Akron board matched to CAMA)
    vacant_home_leads = build_vacant_home_list(
        vacant_addresses, parcel_rows, mail_by_pid, delinquent_pid_set
    )
    all_records = all_records + vacant_home_leads
    logging.info("Total records after adding vacant home leads: %s", len(all_records))

    # 12. Skip trace — RESIDENTIAL PRIORITY ONLY (not all 3,635 leads)
    all_records = await skip_trace_leads(all_records)

    # 13. Dedupe and sort: hot stack first, then distress count, then score
    all_records = dedupe_records(all_records)
    all_records.sort(key=lambda r: (r.hot_stack, r.distress_count, r.score, r.filed), reverse=True)

    # 14. Build vacant land list — DISTRESSED ONLY (foreclosure or tax delinquent)
    vacant_land = build_vacant_land_list(parcel_rows, mail_by_pid, delinquent_pid_set, foreclosure_pids)
    vacant_land.sort(key=lambda r: r.score, reverse=True)

    # 15. Write all outputs
    write_json_outputs(all_records, extra_json_path=Path(args.out_json))
    write_csv(all_records, DEFAULT_OUTPUT_CSV_PATH)
    if Path(args.out_csv) != DEFAULT_OUTPUT_CSV_PATH:
        write_csv(all_records, Path(args.out_csv))
    write_report(report, Path(args.report))
    write_vacant_land_json(vacant_land)
    write_hot_stack_json(all_records)
    write_vacant_home_json(all_records)

    hot_count       = sum(1 for r in all_records if r.hot_stack)
    tax_count       = sum(1 for r in all_records if "Tax delinquent" in r.flags)
    absentee_count  = sum(1 for r in all_records if r.is_absentee)
    vacant_home_cnt = sum(1 for r in all_records if r.is_vacant_home)
    traced_count    = sum(1 for r in all_records if r.phones)

    logging.info(
        "Finished. Total: %s | Prop: %s | Mail: %s | 🔥 Hot Stack: %s | "
        "💰 Tax Deln (residential): %s | 🏠 Vacant Homes: %s | "
        "📭 Absentee: %s | 📞 Skip Traced: %s | 🌿 Vacant Land (distressed): %s",
        len(all_records),
        sum(1 for r in all_records if r.prop_address),
        sum(1 for r in all_records if r.mail_address),
        hot_count, tax_count, vacant_home_cnt,
        absentee_count, traced_count, len(vacant_land),
    )


if __name__ == "__main__":
    asyncio.run(main())
