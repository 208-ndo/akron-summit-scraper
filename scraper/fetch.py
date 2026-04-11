"""
Akron / Summit County — Motivated Seller Intelligence Platform
=============================================================
Data sources (all public, no login required):
  1. Summit County Clerk of Courts  — Lis Pendens, Foreclosure, Judgment, Liens
  2. Akron Legal News Sheriff Sales — Properties scheduled for auction (HIGHEST urgency)
  3. Akron Legal News Tax Delinquent — All delinquent residential parcels
  4. Akron Vacant Building Board     — Officially registered vacant homes
  5. Akron Housing Appeals Board     — Code violations / nuisance orders
  6. Summit County CAMA              — Owner, address, value, sale history
  7. Summit County Probate           — Estate openings, cross-referenced to CAMA
     → Out-of-state executor = inherited + doesn't want it
     → Inherited + tax delinquent = hot stack
     → Inherited + vacant = prime deal

Distress Stacking: every property cross-referenced across ALL sources.
Output: records.json, category JSONs, ghl_export.csv (BatchSkipTracing ready)
"""
import argparse, asyncio, csv, io, json, logging, re, zipfile
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR      = Path(__file__).resolve().parent.parent
DATA_DIR      = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
DEBUG_DIR     = DATA_DIR / "debug"

# output paths
DEFAULT_OUTPUT_JSON_PATHS = [DATA_DIR/"records.json", DASHBOARD_DIR/"records.json"]
DEFAULT_OUTPUT_CSV_PATH   = DATA_DIR / "ghl_export.csv"
DEFAULT_ENRICHED_JSON_PATH= DATA_DIR / "records.enriched.json"
DEFAULT_ENRICHED_CSV_PATH = DATA_DIR / "records.enriched.csv"
DEFAULT_REPORT_PATH       = DATA_DIR / "match_report.json"

LOOKBACK_DAYS = 90
SOURCE_NAME   = "Akron / Summit County, Ohio"
OH_APPRECIATION = 0.04  # annual appreciation for equity estimation

# URLs
CLERK_RECORDS_URL    = "https://clerk.summitoh.net/RecordsSearch/Disclaimer.asp?toPage=SelectDivision.asp"
PENDING_CIVIL_URL    = "https://newcivilfilings.summitoh.net/"
PROBATE_URL          = "https://search.summitohioprobate.com/eservices/"
PROBATE_NEWS_URL     = "https://www.akronlegalnews.com/courts/probate_new_cases"
CAMA_PAGE_URL        = "https://fiscaloffice.summitoh.net/index.php/documents-a-forms/viewcategory/10-cama"
VACANT_BUILDING_URL  = "https://www.akronohio.gov/government/boards_and_commissions/vacant_building_board.php"
HOUSING_APPEALS_URL  = "https://www.akronohio.gov/government/boards_and_commissions/housing_appeals_board.php"
SHERIFF_SALES_URL    = "https://www.akronlegalnews.com/notices/sheriff_sale_abstracts"
DELINQUENT_INDEX_URL = "https://www.akronlegalnews.com/notices/delinquent_taxes"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}

LEAD_TYPE_MAP = {
    "LP":"Lis Pendens","NOFC":"Pre-foreclosure","TAXDEED":"Tax Deed",
    "JUD":"Judgment","CCJ":"Certified Judgment","DRJUD":"Domestic Judgment",
    "LNCORPTX":"Corp Tax Lien","LNIRS":"IRS Lien","LNFED":"Federal Lien",
    "LN":"Lien","LNMECH":"Mechanic Lien","LNHOA":"HOA Lien","MEDLN":"Medicaid Lien",
    "PRO":"Probate / Estate","NOC":"Notice of Commencement","RELLP":"Release Lis Pendens",
    "TAXDELINQ":"Tax Delinquent","TAX":"Tax Delinquent",
    "VHOME":"Vacant Home","VACLAND":"Vacant Land","VACANT":"Vacant Property",
    "SHERIFF":"Sheriff Sale","CODEVIOLATION":"Code Violation",
}

VACANT_LAND_LUCS = {"500","501","502","503"}
RESIDENTIAL_LUCS = {
    "510","511","512","513","514","515",
    "520","521","522","523",
    "530","531","532","533",
    "540","541","542",
    "550","551","560","561","570",
}
MAX_INFILL_ACRES = 2.0

LIKELY_OWNER_KEYS     = ["OWNER1","OWNER2","OWNER","OWN1","OWNER_NAME","OWNERNAME","OWNERNM","NAME","OWNNAM","OWNER 1","OWNER 2","TAXPAYER","TAXPAYER_NAME","MAILNAME","MAIL_NAME","NAME1","NAME2"]
LIKELY_PROP_ADDR_KEYS = ["SITE_ADDR","SITEADDR","PROPERTY_ADDRESS","PROPADDR","ADDRESS","LOCADDR","SADDR"]
LIKELY_PROP_CITY_KEYS = ["SITE_CITY","CITY","SITECITY","PROPERTY_CITY","SCITY","CITYNAME","UDATE1"]
LIKELY_PROP_ZIP_KEYS  = ["SITE_ZIP","ZIP","SITEZIP","PROPERTY_ZIP","SZIP","USER2","ZIPCD","NOTE2"]
LIKELY_MAIL_ZIP_KEYS  = ["MAIL_PTR","MAILZIP","ZIP","MZIP","OWNER ZIPCD1","OWNER ZIPCD2","OWNER_ZIPCD1","OWNER_ZIPCD2"]
LIKELY_LEGAL_KEYS     = ["LEGAL","LEGAL_DESC","LEGALDESCRIPTION","LEGDESC"]
LIKELY_PID_KEYS       = ["PARID","PARCEL","PAIRD","PARCELID","PARCEL_ID","PID","PARCELNO","PAR_NO","PAR_NUM"]
LIKELY_VALUE_KEYS     = ["APRTOT","APPRTOT","TOTALVAL","TOTAL_VAL","MKTVAL","MKTVAL1","APPRVAL","TOTALAPPR","APPR_TOT","BLDVAL","LNDVAL","SALVAL","SALEPRICE","SALE_PRICE","LASTSALE"]
LIKELY_SALE_YEAR_KEYS = ["SALEYR","SALE_YR","SALEYEAR","CONVYR","CONVEYYR","YRBUILT","YR_BUILT"]

BAD_EXACT_OWNERS     = {"Action","Get Docs","Date Added","Party","Plaintiff","Defendant","Search","Home","Select Division","Welcome","EOY ROLL","LWALKER","AWHITE","NJARJABKA","CL_NJARJABKA","SCLB"}
SC701_STATE_CODE_MAP = {"3":"OH","0":"","1":"","2":""}
STATE_CODES = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"}
CORP_WORDS = {"LLC","INC","CORP","CO","COMPANY","TRUST","BANK","ASSOCIATION","NATIONAL","LTD","LP","PLC","HOLDINGS","FUNDING","VENTURES","RESTORATION","SCHOOLS","UNION","MORTGAGE","RECOVERY","BOARD","SERVICING","PROPERTIES","REALTY","INVESTMENTS","CAPITAL","GROUP","PARTNERS","MANAGEMENT","ENTERPRISES"}
NOISE_NAME_WORDS = {"AKA","ET","AL","UNKNOWN","HEIRS","SPOUSE","JOHN","JANE","DOE","ADMINISTRATOR","EXECUTOR","FIDUCIARY","TRUSTEE","OR","THE","OF","SUCCESSOR","MERGER","TO","BY","ADMIN","ESTATE"}
STACK_BONUS = {2:15,3:25,4:40}


# -----------------------------------------------------------------------
# DATA CLASSES
# -----------------------------------------------------------------------
@dataclass
class LeadRecord:
    doc_num:str=""; doc_type:str=""; filed:str=""; cat:str=""; cat_label:str=""
    owner:str=""; grantee:str=""; amount:Optional[float]=None; legal:str=""
    prop_address:str=""; prop_city:str=""; prop_state:str="OH"; prop_zip:str=""
    mail_address:str=""; mail_city:str=""; mail_state:str=""; mail_zip:str=""
    clerk_url:str=""; flags:List[str]=field(default_factory=list); score:int=0
    match_method:str="unmatched"; match_score:float=0.0; with_address:int=0
    distress_sources:List[str]=field(default_factory=list); distress_count:int=0
    hot_stack:bool=False; luc:str=""; acres:str=""
    is_vacant_land:bool=False; is_vacant_home:bool=False
    is_absentee:bool=False; is_out_of_state:bool=False
    parcel_id:str=""
    # phones / email (filled by skip trace later)
    phones:list=field(default_factory=list); phone_types:list=field(default_factory=list)
    emails:list=field(default_factory=list); skip_trace_source:str=""
    # value / equity
    assessed_value:Optional[float]=None; estimated_value:Optional[float]=None
    last_sale_price:Optional[float]=None; last_sale_year:Optional[int]=None
    est_mortgage_balance:Optional[float]=None; est_equity:Optional[float]=None
    est_arrears:Optional[float]=None; est_payoff:Optional[float]=None
    subject_to_score:int=0; mortgage_signals:List[str]=field(default_factory=list)
    # source-specific
    sheriff_sale_date:str=""; appraised_value:Optional[float]=None; lender:str=""
    code_violation_case:str=""; code_violation_date:str=""
    # probate-specific
    decedent_name:str=""; executor_name:str=""; executor_state:str=""
    estate_value:Optional[float]=None; is_inherited:bool=False


@dataclass
class VacantLandRecord:
    parcel_id:str=""; prop_address:str=""; prop_city:str=""; prop_state:str="OH"; prop_zip:str=""
    owner:str=""; mail_address:str=""; mail_city:str=""; mail_state:str=""; mail_zip:str=""
    luc:str=""; acres:str=""; flags:List[str]=field(default_factory=list); score:int=0
    distress_sources:List[str]=field(default_factory=list); distress_count:int=0


# -----------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------
def ensure_dirs():
    for d in [DATA_DIR,DASHBOARD_DIR,DEBUG_DIR]: d.mkdir(parents=True,exist_ok=True)

def log_setup():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def save_debug_json(name,payload):
    try: (DEBUG_DIR/name).write_text(json.dumps(payload,indent=2),encoding="utf-8")
    except Exception as e: logging.warning("debug json %s: %s",name,e)

def save_debug_text(name,content):
    try: (DEBUG_DIR/name).write_text(content,encoding="utf-8")
    except Exception as e: logging.warning("debug text %s: %s",name,e)

def clean_text(v)->str:
    if v is None: return ""
    return re.sub(r"\s+"," ",str(v)).strip()

def normalize_state(v:str)->str:
    v=clean_text(v).upper()
    if not v: return ""
    if v in SC701_STATE_CODE_MAP: return SC701_STATE_CODE_MAP[v]
    if v in {"0","1","2","4","5","6","7","8","9","00","000","-","N/A","NA","NONE","NULL"}: return ""
    v=re.sub(r"[^A-Z]","",v)
    return v if v in STATE_CODES else ""

def retry_request(url,attempts=3,timeout=60):
    last=None
    for i in range(1,attempts+1):
        try:
            r=requests.get(url,headers=HEADERS,timeout=timeout,allow_redirects=True); r.raise_for_status(); return r
        except Exception as e: last=e; logging.warning("Request failed (%s/%s) %s: %s",i,attempts,url,e)
    raise last

def normalize_name(n:str)->str:
    n=clean_text(n).upper(); n=re.sub(r"[^A-Z0-9,&.\- /']"," ",n)
    return re.sub(r"\s+"," ",n).strip()

def normalize_person_name(n:str)->str:
    n=normalize_name(n)
    if not n: return ""
    for p in [r"\bAKA\b.*$",r"\bET AL\b.*$",r"\bUNKNOWN HEIRS OF\b",r"\bUNKNOWN SPOUSE OF\b",
              r"\bUNKNOWN ADMINISTRATOR\b",r"\bEXECUTOR\b",r"\bFIDUCIARY\b",r"\bJOHN DOE\b",r"\bJANE DOE\b",r"\bTHE\b"]:
        n=re.sub(p,"",n).strip()
    return re.sub(r"\s+"," ",n).strip(" ,.-")

def tokens_from_name(n:str)->List[str]:
    n=normalize_person_name(n)
    if not n: return []
    return [t for t in re.split(r"[ ,/&.\-]+",n) if t and t not in NOISE_NAME_WORDS]

def likely_corporate_name(n:str)->bool: return any(t in CORP_WORDS for t in set(tokens_from_name(n)))
def get_last_name(n:str)->str: t=tokens_from_name(n); return t[-1] if t else ""
def get_first_name(n:str)->str: t=tokens_from_name(n); return t[0] if t else ""
def get_first_initial(n:str)->str: f=get_first_name(n); return f[:1] if f else ""

def same_first_name_or_initial(a:str,b:str)->bool:
    fa,fb=get_first_name(a),get_first_name(b)
    if fa and fb and fa==fb: return True
    return bool(get_first_initial(a) and get_first_initial(b) and get_first_initial(a)==get_first_initial(b))

def singularize_last_name(ln:str)->str:
    ln=clean_text(ln).upper()
    if ln.endswith("IES") and len(ln)>4: return ln[:-3]+"Y"
    if ln.endswith("ES") and len(ln)>3: return ln[:-2]
    if ln.endswith("S") and len(ln)>3: return ln[:-1]
    return ln

def last_names_compatible(a,b)->bool:
    a,b=clean_text(a).upper(),clean_text(b).upper()
    if not a or not b: return False
    return a==b or singularize_last_name(a)==singularize_last_name(b)

def build_owner_name(row:dict)->str:
    o1=clean_text(row.get("OWNER1","") or row.get("OWNER 1",""))
    o2=clean_text(row.get("OWNER2","") or row.get("OWNER 2",""))
    if o1 and o2: return re.sub(r"\s+"," ",f"{o1} {o2}".strip())
    return o1 or o2 or safe_pick(row,LIKELY_OWNER_KEYS)

def build_mail_zip(row:dict)->str:
    mp=clean_text(row.get("MAIL_PTR",""))
    if mp and re.fullmatch(r"\d{5}",mp): return mp
    z1=clean_text(row.get("OWNER ZIPCD1","") or row.get("OWNER_ZIPCD1",""))
    z2=clean_text(row.get("OWNER ZIPCD2","") or row.get("OWNER_ZIPCD2",""))
    if z1 and z2: return f"{z1}-{z2}"
    return z1 or safe_pick(row,LIKELY_MAIL_ZIP_KEYS)

def build_mail_city_sc701(row:dict)->str:
    n1=clean_text(row.get("NOTE1",""))
    if n1 and len(n1)>2 and not re.fullmatch(r"\d+",n1): return n1.title()
    for k in ["MAILCITY","CITY","MCITY"]:
        v=clean_text(row.get(k,""))
        if v and len(v)>2 and not re.fullmatch(r"\d+",v): return v.title()
    return ""

def build_mail_state_sc701(row:dict)->str:
    raw=clean_text(row.get("STATE",""))
    mapped=SC701_STATE_CODE_MAP.get(raw)
    if mapped is not None: return mapped
    cleaned=re.sub(r"[^A-Z]","",raw.upper())
    if cleaned in STATE_CODES: return cleaned
    return "OH" if clean_text(row.get("MAIL_ADR1","")) else ""

def split_owner_chunks(name:str)->List[str]:
    raw=normalize_person_name(name)
    if not raw: return []
    working=re.sub(r"\bET AL\b|\bAKA\b.*$","",raw)
    working=re.sub(r"\s+"," ",working).strip(" ,;/")
    if not working: return []
    parts=re.split(r"\s*(?:;|/|\bAND\b|&)\s*",working)
    seen,result=set(),[]
    for p in parts:
        p=normalize_person_name(p)
        if p and p not in seen: seen.add(p); result.append(p)
    return result or [working]

def name_variants(name:str)->List[str]:
    raw=normalize_person_name(name)
    if not raw: return []
    suffixes={"JR","SR","II","III","IV","V","ETAL","ET","AL"}; joiner_noise={"AND","&","OR"}; variants=set()
    for chunk in split_owner_chunks(raw):
        working=re.sub(r"\bAND\b|\bOR\b|&"," ",chunk.replace(";","").replace("/"," "))
        working=re.sub(r"\s+"," ",working).strip()
        if not working: continue
        variants.update([chunk,working,working.replace(",","")])
        comma_parts=[normalize_person_name(x) for x in chunk.split(",") if normalize_person_name(x)]
        def add_variants(parts):
            parts=[p for p in parts if p and p not in joiner_noise]
            while parts and parts[-1] in suffixes: parts=parts[:-1]
            if not parts: return
            full=" ".join(parts)
            if full: variants.add(full)
            if len(parts)==1: variants.add(parts[0]); return
            first,last=parts[0],parts[-1]; mids=parts[1:-1]; mid=" ".join(mids)
            variants.update([f"{first} {last}",f"{last} {first}",f"{last}, {first}"])
            if mid:
                variants.update([f"{first} {mid} {last}",f"{last}, {first} {mid}",f"{last} {first} {mid}"])
                mi=" ".join(m[0] for m in mids if m)
                if mi: variants.update([f"{first} {mi} {last}",f"{last}, {first} {mi}",f"{last} {first} {mi}"])
        if len(comma_parts)>=2:
            last=comma_parts[0]; rem=[]
            for piece in comma_parts[1:]: rem.extend(piece.split())
            add_variants(rem+[last])
            ft=comma_parts[1].split()
            if ft: f=ft[0]; variants.update([f"{f} {last}",f"{last} {f}",f"{last}, {f}"])
        else:
            add_variants([p for p in working.replace(","," ").split() if p])
    final,seen=[],set()
    for v in variants:
        v=re.sub(r"\s+"," ",normalize_person_name(v)).strip(" ,")
        if v and v not in seen: seen.add(v); final.append(v)
    return final

def parse_amount(v:str)->Optional[float]:
    v=clean_text(v)
    if not v: return None
    c=re.sub(r"[^0-9.\-]","",v)
    try: return float(c) if c else None
    except ValueError: return None

def safe_pick(row:dict,keys:List[str])->str:
    for k in keys:
        if k in row and clean_text(row.get(k,"")): return clean_text(row.get(k,""))
    upper_map={str(k).upper():k for k in row.keys()}
    for k in keys:
        if k.upper() in upper_map:
            v=clean_text(row.get(upper_map[k.upper()],""))
            if v: return v
    return ""

def get_pid(row:dict)->str: return safe_pick(row,LIKELY_PID_KEYS)

def parse_acres(raw:str)->Optional[float]:
    raw=clean_text(raw)
    if not raw: return None
    try: return float(raw)
    except ValueError: return None

def is_infill_lot(luc:str,acres_raw:str)->bool:
    if luc not in VACANT_LAND_LUCS: return False
    acres=parse_acres(acres_raw); return acres is None or acres<=MAX_INFILL_ACRES

def normalize_address_key(address:str)->str:
    addr=clean_text(address).upper()
    for old,new in [("N.","N"),("S.","S"),("E.","E"),("W.","W"),("NORTH","N"),("SOUTH","S"),("EAST","E"),("WEST","W")]:
        addr=addr.replace(old,new)
    addr=re.sub(r"\b(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|BLVD|BOULEVARD|LN|LANE|CT|COURT|PL|PLACE|WAY|TER|TERRACE|CIR|CIRCLE|PKWY|PARKWAY)\b","",addr)
    addr=re.sub(r"[^A-Z0-9\s]","",addr)
    return re.sub(r"\s+"," ",addr).strip()

def is_absentee_owner(prop_address:str,mail_address:str,mail_state:str="")->bool:
    if not prop_address or not mail_address: return False
    if re.search(r"\bP\.?\s*O\.?\s*BOX\b",mail_address.upper()): return True
    state=normalize_state(mail_state)
    if state and state!="OH": return True
    pk=normalize_address_key(prop_address); mk=normalize_address_key(mail_address)
    if not pk or not mk or pk==mk: return False
    def core(a): parts=a.split(); return " ".join(parts[:2]) if len(parts)>=2 else a
    return core(pk)!=core(mk)

def is_out_of_state(mail_state:str)->bool:
    s=normalize_state(mail_state); return bool(s and s!="OH")

def category_flags(doc_type:str,owner:str="")->List[str]:
    flags=[]; dt=clean_text(doc_type).upper(); ou=normalize_name(owner)
    if dt=="LP":                                               flags.append("Lis pendens")
    if dt=="NOFC":                                             flags.append("Pre-foreclosure")
    if dt in {"JUD","CCJ","DRJUD"}:                           flags.append("Judgment lien")
    if dt in {"TAXDEED","LNCORPTX","LNIRS","LNFED","TAXDELINQ","TAX"}: flags.append("Tax lien")
    if dt=="LNMECH":                                           flags.append("Mechanic lien")
    if dt=="PRO":                                              flags.append("Probate / estate")
    if dt in {"VACANT","VACLAND","VHOME"}:                    flags.append("Vacant property")
    if dt=="SHERIFF":                                          flags.append("Sheriff sale scheduled")
    if dt=="CODEVIOLATION":                                    flags.append("Code violation")
    if any(t in f" {ou} " for t in [" LLC"," INC"," CORP"," CO "," COMPANY"," TRUST"," LP"," LTD"," BANK "]): flags.append("LLC / corp owner")
    return list(dict.fromkeys(flags))

def classify_distress_source(doc_type:str)->Optional[str]:
    dt=clean_text(doc_type).upper()
    if dt in {"LP","RELLP"}:                  return "lis_pendens"
    if dt=="NOFC":                            return "foreclosure"
    if dt in {"JUD","CCJ","DRJUD"}:          return "judgment"
    if dt in {"LN","LNHOA","LNFED","LNIRS","LNCORPTX","MEDLN"}: return "lien"
    if dt=="LNMECH":                          return "mechanic_lien"
    if dt in {"TAXDEED","TAXDELINQ","TAX"}:   return "tax_delinquent"
    if dt=="PRO":                             return "probate"
    if dt in {"VACANT","VACLAND","VHOME"}:    return "vacant_home"
    if dt=="SHERIFF":                         return "sheriff_sale"
    if dt=="CODEVIOLATION":                   return "code_violation"
    return None


# -----------------------------------------------------------------------
# MORTGAGE / EQUITY / SUBJECT-TO ESTIMATION
# -----------------------------------------------------------------------
def estimate_mortgage_data(record:"LeadRecord")->"LeadRecord":
    signals=[]; sto=0
    # establish market value
    market_val=None
    if record.last_sale_price and record.last_sale_price>5000:
        yrs=max(0,datetime.now().year-(record.last_sale_year or datetime.now().year))
        market_val=record.last_sale_price*((1+OH_APPRECIATION)**yrs)
    elif record.assessed_value and record.assessed_value>5000:
        market_val=record.assessed_value/0.35  # Ohio assessed = 35% of market
    elif record.appraised_value and record.appraised_value>5000:
        market_val=record.appraised_value  # sheriff appraisal = current market
    if market_val: record.estimated_value=round(market_val,2)

    # estimate remaining mortgage balance
    if record.last_sale_price and record.last_sale_year and record.last_sale_price>5000:
        yrs_elapsed=max(0,min(30,datetime.now().year-record.last_sale_year))
        orig=record.last_sale_price*0.80; mr=0.065/12; n=360; paid=yrs_elapsed*12
        if mr>0 and paid<n:
            bal=orig*((1+mr)**n-(1+mr)**paid)/((1+mr)**n-1)
            record.est_mortgage_balance=round(max(0,bal),2)
        elif paid>=n: record.est_mortgage_balance=0.0

    if record.estimated_value and record.est_mortgage_balance is not None:
        record.est_equity=round(record.estimated_value-record.est_mortgage_balance,2)

    # arrears and payoff
    if record.doc_type in {"LP","NOFC","TAXDEED","SHERIFF"} and record.amount and record.amount>0:
        record.est_arrears=record.amount
        record.est_payoff=record.est_mortgage_balance or record.amount
        signals.append(f"Arrears ~${record.est_arrears:,.0f}")
    if "Tax delinquent" in record.flags and record.amount and record.amount>0:
        record.est_arrears=(record.est_arrears or 0)+record.amount
        signals.append(f"Tax owed ~${record.amount:,.0f}")

    # subject-to scoring
    if record.est_equity is not None:
        if record.est_equity>50000:   sto+=30; signals.append("High equity 🏦")
        elif record.est_equity>20000: sto+=20; signals.append("Moderate equity")
        elif record.est_equity>0:     sto+=10
        else:                         signals.append("Underwater ⚠️")

    if record.doc_type in {"LP","NOFC","SHERIFF"}: sto+=25; signals.append("Active foreclosure")
    if record.doc_type=="PRO":                     sto+=20; signals.append("Estate / probate")
    if record.doc_type=="CODEVIOLATION":           sto+=15; signals.append("Code violation pressure")
    if record.is_inherited:                        sto+=20; signals.append("Inherited property")
    if record.executor_state and record.executor_state!="OH": sto+=15; signals.append(f"Out-of-state executor ({record.executor_state})")
    if record.is_absentee:                         sto+=15; signals.append("Absentee owner")
    if record.is_out_of_state:                     sto+=10; signals.append("Out-of-state owner")
    if record.is_vacant_home:                      sto+=20; signals.append("Vacant home")
    if "Tax delinquent" in record.flags:           sto+=15
    if "Code violation" in record.flags:           sto+=10

    if record.est_mortgage_balance and record.estimated_value:
        ltv=record.est_mortgage_balance/record.estimated_value
        if ltv<0.5:   sto+=20; signals.append("Low LTV <50%")
        elif ltv<0.7: sto+=10; signals.append("LTV <70%")
        elif ltv>0.95: signals.append("High LTV >95%")

    if sto>=50 and "🎯 Subject-To Candidate" not in record.flags:
        record.flags.append("🎯 Subject-To Candidate")
    if sto>=70 and "⭐ Prime Subject-To" not in record.flags:
        record.flags.append("⭐ Prime Subject-To")

    record.subject_to_score=min(sto,100); record.mortgage_signals=signals
    return record


def score_record(record:"LeadRecord")->int:
    score=30; lf={f.lower() for f in record.flags}; fs=0
    if "lis pendens" in lf:               fs+=20
    if "pre-foreclosure" in lf:           fs+=20
    if "judgment lien" in lf:             fs+=15
    if "tax lien" in lf:                  fs+=15
    if "mechanic lien" in lf:             fs+=10
    if "probate / estate" in lf:          fs+=15
    if "vacant home" in lf:               fs+=25
    if "vacant property" in lf:           fs+=15
    if "sheriff sale scheduled" in lf:    fs+=35   # HIGHEST — days from auction
    if "code violation" in lf:            fs+=20
    if "absentee owner" in lf:            fs+=10
    if "out-of-state owner" in lf:        fs+=12
    if "tax delinquent" in lf:            fs+=10
    if "high tax debt" in lf:             fs+=8
    if "inherited property" in lf:        fs+=15
    if "🎯 subject-to candidate" in lf:   fs+=15
    if "⭐ prime subject-to" in lf:        fs+=20
    score+=min(fs,70)
    if "lis pendens" in lf and "pre-foreclosure" in lf: score+=20
    if record.amount is not None:
        score+=15 if record.amount>100000 else (10 if record.amount>50000 else 5)
    if record.filed:
        try:
            if datetime.fromisoformat(record.filed).date()>=(datetime.now().date()-timedelta(days=7)):
                if "New this week" not in record.flags: record.flags.append("New this week")
                score+=5
        except: pass
    if record.prop_address: score+=5
    if record.mail_address: score+=3
    dc=len(set(record.distress_sources)); record.distress_count=dc
    bk=min(dc,4)
    if bk>=2:
        score+=STACK_BONUS.get(bk,STACK_BONUS[4]); record.hot_stack=True
        if "🔥 Hot Stack" not in record.flags: record.flags.append("🔥 Hot Stack")
    return min(score,100)


# -----------------------------------------------------------------------
# SCRAPERS
# -----------------------------------------------------------------------
def scrape_vacant_building_addresses()->List[str]:
    addresses=[]
    try:
        resp=retry_request(VACANT_BUILDING_URL,timeout=30); soup=BeautifulSoup(resp.text,"lxml")
        text=soup.get_text(" ")
        pat=re.compile(r"\b(\d{2,5})\s+([NSEW]\.?\s+)?([A-Z][A-Za-z\.\s]{2,30})\s+(St|Ave|Rd|Dr|Blvd|Ln|Ct|Pl|Way|Ter|Cir|Pkwy)\.?\b",re.IGNORECASE)
        for m in pat.finditer(text):
            addr=re.sub(r"\s+"," ",m.group(0)).strip().upper()
            if addr and len(addr)>8: addresses.append(addr)
        addresses=list(dict.fromkeys(addresses))
        logging.info("Vacant building addresses: %s",len(addresses))
        save_debug_json("vacant_building_addresses.json",addresses)
    except Exception as e: logging.warning("Vacant building scrape failed: %s",e)
    return addresses


def scrape_sheriff_sales()->List[LeadRecord]:
    """
    Scrape Akron Legal News sheriff sale abstracts.
    These owners are DAYS from losing everything — highest urgency leads.
    Includes: address, lender, defendant (owner), appraised value, sale date.
    """
    records:List[LeadRecord]=[]
    try:
        logging.info("Scraping sheriff sales...")
        resp=retry_request(SHERIFF_SALES_URL,timeout=30); soup=BeautifulSoup(resp.text,"lxml")
        current_sale_date=""

        for row in soup.find_all("tr"):
            cells=[clean_text(td.get_text(" ")) for td in row.find_all(["td","th"])]
            row_text=" ".join(cells)
            if not row_text or len(row_text)<15: continue

            # detect sale date headers
            dm=re.search(r"Properties for Sale on ([A-Za-z]+ \d+,?\s*\d{4})",row_text,re.IGNORECASE)
            if dm:
                try: current_sale_date=datetime.strptime(dm.group(1).replace(",","").strip(),"%B %d %Y").date().isoformat()
                except: current_sale_date=clean_text(dm.group(1))
                continue

            link=row.find("a",href=True)
            if not link: continue
            case_num=clean_text(link.get_text(" "))
            detail_url=requests.compat.urljoin(SHERIFF_SALES_URL,clean_text(link.get("href","")))

            # parse: "CASE - Lender v Owner Property located at ADDR. Appraised at $X"
            m=re.search(
                r"[-–]\s*(.+?)\s+v\s+(.+?)\s+Property located at\s+(.+?)\.\s+Appraised at\s+\$?([\d,]+)",
                row_text,re.IGNORECASE
            )
            if not m:
                am=re.search(r"Property located at\s+(.+?)\.\s+Appraised at\s+\$?([\d,]+)",row_text,re.IGNORECASE)
                if not am: continue
                prop_raw=clean_text(am.group(1)); appraised_raw=am.group(2)
                lender=""; owner=""
            else:
                lender=clean_text(m.group(1)); owner=clean_text(m.group(2))
                prop_raw=clean_text(m.group(3)); appraised_raw=m.group(4)

            # parse address
            prop_address=""; prop_city=""; prop_zip=""
            parts=[p.strip() for p in prop_raw.split(",")]
            if parts:
                prop_address=parts[0]
                if len(parts)>=2: prop_city=parts[1]
                if len(parts)>=3:
                    zm=re.search(r"\d{5}",parts[-1])
                    if zm: prop_zip=zm.group(0)
            if not prop_address or len(prop_address)<5: continue

            try: appraised=float(appraised_raw.replace(",",""))
            except: appraised=None

            # clean owner
            for pat in [r"\bet al\b.*$",r"\baka\b.*$",r"\bunknown\b.*$"]:
                owner=re.sub(pat,"",owner,flags=re.IGNORECASE).strip()
            owner=re.sub(r"\s+"," ",owner).strip(" ,.-")

            flags=["Sheriff sale scheduled","Foreclosure","🔥 Hot Stack"]
            if current_sale_date:
                try:
                    days_until=(datetime.fromisoformat(current_sale_date).date()-datetime.now().date()).days
                    if days_until<=7: flags.append("⚡ Sale this week!")
                    elif days_until<=14: flags.append("Sale in 2 weeks")
                except: pass

            rec=LeadRecord(
                doc_num=case_num or f"SHERIFF-{len(records)+1}",
                doc_type="SHERIFF",filed=current_sale_date,cat="SHERIFF",cat_label="Sheriff Sale",
                owner=owner.title() if owner else "",grantee=lender,lender=lender,
                amount=appraised,appraised_value=appraised,
                prop_address=prop_address,prop_city=prop_city,prop_state="OH",prop_zip=prop_zip,
                sheriff_sale_date=current_sale_date,clerk_url=detail_url,
                flags=flags,distress_sources=["sheriff_sale","foreclosure"],
                distress_count=2,hot_stack=True,
                with_address=1,match_method="sheriff_sale_direct",match_score=1.0,
            )
            rec=estimate_mortgage_data(rec); rec.score=score_record(rec)
            records.append(rec)

        logging.info("Sheriff sales: %s records",len(records))
        save_debug_json("sheriff_sales.json",[asdict(r) for r in records[:20]])
    except Exception as e: logging.warning("Sheriff sales scrape failed: %s",e)
    return records


def scrape_housing_appeals_board()->List[LeadRecord]:
    """
    Scrape Akron Housing Appeals Board — homes with active code violation orders.
    City has officially declared these unsafe / public nuisances.
    Owner must repair OR sell — extreme motivated seller signal.
    """
    records:List[LeadRecord]=[]
    try:
        logging.info("Scraping Housing Appeals Board...")
        resp=retry_request(HOUSING_APPEALS_URL,timeout=30); soup=BeautifulSoup(resp.text,"lxml")
        text=soup.get_text(" ")
        seen_cases=set()

        case_pat=re.compile(
            r"CASE\s*#\s*(\d+)\s*[–\-]+\s*(?:\([A-Z]+\)\s*[–\-]+\s*)?(\d{2,5}\s+[A-Z][A-Za-z\s\.]{3,40}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL|WAY|TER|CIR|PKWY|STREET|AVENUE|ROAD|DRIVE|LANE|COURT|PLACE)\.?(?:\s+\w+)?)",
            re.IGNORECASE
        )
        date_pat=re.compile(r"(?:orders? dated|inspected on)\s+(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})",re.IGNORECASE)

        for m in case_pat.finditer(text):
            case_num=m.group(1)
            if case_num in seen_cases: continue
            seen_cases.add(case_num)
            raw_addr=clean_text(m.group(2))

            surrounding=text[max(0,m.start()-100):m.end()+600]
            vdate=""
            dm=date_pat.search(surrounding)
            if dm:
                for fmt in ("%m/%d/%Y","%m/%d/%y","%m-%d-%Y","%m-%d-%y"):
                    try: vdate=datetime.strptime(dm.group(1),fmt).date().isoformat(); break
                    except: continue

            flags=["Code violation","Housing violation","City nuisance order"]
            rec=LeadRecord(
                doc_num=f"CODEVIOLATION-{case_num}",doc_type="CODEVIOLATION",
                filed=vdate,cat="CODEVIOLATION",cat_label="Code Violation",
                prop_address=raw_addr,prop_city="Akron",prop_state="OH",
                clerk_url=HOUSING_APPEALS_URL,flags=flags,
                distress_sources=["code_violation"],distress_count=1,
                code_violation_case=f"CASE #{case_num}",code_violation_date=vdate,
                with_address=1,match_method="housing_appeals_direct",match_score=0.9,
            )
            rec=estimate_mortgage_data(rec); rec.score=score_record(rec)
            records.append(rec)

        logging.info("Housing Appeals Board: %s code violation records",len(records))
        save_debug_json("code_violations.json",[asdict(r) for r in records[:20]])
    except Exception as e: logging.warning("Housing appeals scrape failed: %s",e)
    return records


def scrape_probate_leads(parcel_rows:List[dict],mail_by_pid:Dict[str,dict],
                          delinquent_pid_set:set,vacant_home_keys:set)->List[LeadRecord]:
    """
    Scrape Summit County Probate Court new estate filings via Akron Legal News.
    
    Key signals we look for:
      - Executor/administrator address is OUT OF STATE → they inherited and don't want it
      - Decedent's property found in CAMA → we know the address, value, mortgage
      - Estate property + tax delinquent → they're already behind and just want out
      - Estate property + vacant home → perfect — empty house, out-of-state heir
    
    Strategy: scrape estate name → search CAMA by last name of decedent → 
              cross-reference with tax delinquent + vacant lists.
    """
    records:List[LeadRecord]=[]
    try:
        logging.info("Scraping probate / estate leads...")
        resp=retry_request(PROBATE_NEWS_URL,timeout=30); soup=BeautifulSoup(resp.text,"lxml")
        text=soup.get_text(" ")

        # Pattern: "Estate of FIRSTNAME LASTNAME, deceased"
        estate_pat=re.compile(
            r"Estate of\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}),?\s+deceased",
            re.IGNORECASE
        )
        # Executor address pattern
        exec_pat=re.compile(
            r"(?:executor|administrator|fiduciary),?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}),\s*(.+?(?:[A-Z]{2}\s+\d{5}|[A-Z]{2}\s*\d{5}))",
            re.IGNORECASE
        )

        # Build last-name → parcel lookup for cross-referencing
        last_name_to_parcels:Dict[str,List[dict]]={} 
        for row in parcel_rows:
            owner=build_owner_name(row)
            if not owner: continue
            ln=get_last_name(normalize_name(owner))
            if ln and len(ln)>2:
                last_name_to_parcels.setdefault(ln,[]).append(row)

        seen_decedents=set()
        for m in estate_pat.finditer(text):
            decedent_name=clean_text(m.group(1))
            if decedent_name in seen_decedents: continue
            seen_decedents.add(decedent_name)

            surrounding=text[max(0,m.start()-50):m.end()+800]
            # extract executor info
            exec_m=exec_pat.search(surrounding)
            executor_name=""; executor_state=""; executor_address=""
            if exec_m:
                executor_name=clean_text(exec_m.group(1))
                executor_address=clean_text(exec_m.group(2))
                state_m=re.search(r"\b([A-Z]{2})\s+\d{5}",executor_address)
                if state_m: executor_state=state_m.group(1)

            # extract filing date
            vdate=""
            date_m=re.search(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b",surrounding)
            if date_m:
                for fmt in ("%m/%d/%Y","%m/%d/%y"):
                    try: vdate=datetime.strptime(date_m.group(1),fmt).date().isoformat(); break
                    except: continue
            if not vdate: vdate=datetime.now().date().isoformat()

            # skip if filed > LOOKBACK_DAYS ago
            try:
                if datetime.fromisoformat(vdate).date()<(datetime.now().date()-timedelta(days=LOOKBACK_DAYS)):
                    continue
            except: pass

            # try to find the decedent's property in CAMA by last name
            decedent_last=get_last_name(normalize_name(decedent_name))
            matched_parcels=last_name_to_parcels.get(decedent_last,[])

            if matched_parcels:
                # create a lead for each matched parcel (usually 1)
                for row in matched_parcels[:3]:  # cap at 3 matches per name
                    pid=get_pid(row)
                    luc=clean_text(row.get("LUC",""))
                    if luc not in RESIDENTIAL_LUCS: continue  # only residential

                    prop_address=build_prop_address_from_row(row)
                    prop_city=build_prop_city_from_row(row)
                    prop_zip=build_prop_zip_from_row(row)
                    if not prop_address: continue

                    mail_row=mail_by_pid.get(pid,{})
                    mail_address=clean_text(mail_row.get("MAIL_ADR1","")) if mail_row else ""
                    mail_city=build_mail_city_sc701(mail_row) if mail_row else ""
                    mail_zip=build_mail_zip(mail_row) if mail_row else ""
                    mail_state=build_mail_state_sc701(mail_row) if mail_row else ""

                    addr_key=normalize_address_key(prop_address)
                    is_tax_delin=pid in delinquent_pid_set
                    is_vhome=addr_key in vacant_home_keys

                    flags=["Probate / estate","Inherited property"]
                    ds=["probate"]
                    if executor_state and executor_state!="OH":
                        flags.append(f"Out-of-state executor ({executor_state})")
                        ds.append("out_of_state")
                    if is_tax_delin: flags.append("Tax delinquent"); ds.append("tax_delinquent")
                    if is_vhome: flags.append("Vacant home"); ds.append("vacant_home")

                    absentee=is_absentee_owner(prop_address,mail_address,mail_state)
                    oos=is_out_of_state(mail_state) or (executor_state not in ("","OH"))

                    if absentee: flags.append("Absentee owner")
                    if oos: flags.append("Out-of-state owner")

                    assessed=build_assessed_value_from_row(row)
                    sale_price,sale_year=build_sale_data_from_row(row)

                    rec=LeadRecord(
                        doc_num=f"PRO-{decedent_last}-{pid or len(records)+1}",
                        doc_type="PRO",filed=vdate,cat="PRO",cat_label="Probate / Estate",
                        owner=executor_name.title() if executor_name else decedent_name.title(),
                        decedent_name=decedent_name.title(),
                        executor_name=executor_name.title() if executor_name else "",
                        executor_state=executor_state,
                        is_inherited=True,
                        prop_address=prop_address,prop_city=prop_city,prop_state="OH",prop_zip=prop_zip,
                        mail_address=mail_address,mail_city=mail_city,
                        mail_state=normalize_state(mail_state) or "OH",mail_zip=mail_zip,
                        clerk_url=PROBATE_NEWS_URL,flags=flags,
                        distress_sources=list(set(ds)),distress_count=len(set(ds)),
                        luc=luc,parcel_id=pid or "",
                        is_vacant_home=is_vhome,is_absentee=absentee,is_out_of_state=oos,
                        with_address=1,match_method="probate_name_match",match_score=0.85,
                        assessed_value=assessed,last_sale_price=sale_price,last_sale_year=sale_year,
                    )
                    rec=estimate_mortgage_data(rec); rec.score=score_record(rec)
                    rec.hot_stack=rec.distress_count>=2
                    records.append(rec)
            else:
                # No CAMA match — still create a lead with what we have
                flags=["Probate / estate","Inherited property"]
                ds=["probate"]
                if executor_state and executor_state!="OH":
                    flags.append(f"Out-of-state executor ({executor_state})")
                rec=LeadRecord(
                    doc_num=f"PRO-{decedent_last}-{len(records)+1}",
                    doc_type="PRO",filed=vdate,cat="PRO",cat_label="Probate / Estate",
                    owner=executor_name.title() if executor_name else "",
                    decedent_name=decedent_name.title(),
                    executor_name=executor_name.title() if executor_name else "",
                    executor_state=executor_state,is_inherited=True,
                    clerk_url=PROBATE_NEWS_URL,flags=flags,
                    distress_sources=ds,distress_count=1,
                    match_method="probate_no_parcel",match_score=0.5,
                )
                rec=estimate_mortgage_data(rec); rec.score=score_record(rec)
                records.append(rec)

        logging.info(
            "Probate leads: %s total | %s with property matched | %s out-of-state executor | %s tax delinquent",
            len(records),
            sum(1 for r in records if r.parcel_id),
            sum(1 for r in records if r.executor_state and r.executor_state!="OH"),
            sum(1 for r in records if "Tax delinquent" in r.flags),
        )
        save_debug_json("probate_leads.json",[asdict(r) for r in records[:20]])
    except Exception as e: logging.warning("Probate scrape failed: %s",e)
    return records


def scrape_tax_delinquent_parcels()->Dict[str,dict]:
    parcels:Dict[str,dict]={}
    try:
        logging.info("Scraping tax delinquent list...")
        resp=retry_request(DELINQUENT_INDEX_URL,timeout=30); soup=BeautifulSoup(resp.text,"lxml"); links=[]
        for a in soup.select("a[href]"):
            href=clean_text(a.get("href",""))
            if "delinquent_taxes_detail" in href:
                full=requests.compat.urljoin(DELINQUENT_INDEX_URL,href)
                if full not in links: links.append(full)
        logging.info("Found %s delinquent tax pages",len(links))
        for i,url in enumerate(links):
            try:
                r2=retry_request(url,timeout=45); soup2=BeautifulSoup(r2.text,"lxml"); raw=soup2.get_text(" ")
                for entry in re.split(r"\s*[•·]\s*",raw):
                    entry=clean_text(entry)
                    if not entry: continue
                    m=re.match(r"^(\d{7})\s+([\d,]+\.?\d*)\s+(.+)$",entry,re.DOTALL)
                    if not m: continue
                    pid=m.group(1)
                    try: amt=float(m.group(2).replace(",",""))
                    except: amt=0.0
                    rest=clean_text(m.group(3)); owner_part=rest; legal_part=""
                    legal_kw=["LOT ","TR ","BLK ","SEC ","LOTS ","ALLOTMENT","SUB ","PARCEL ","PART ","COND ","UNIT "]
                    earliest=len(rest)
                    for kw in legal_kw:
                        idx=rest.upper().find(kw)
                        if 0<idx<earliest: earliest=idx
                    if earliest<len(rest):
                        owner_part=clean_text(rest[:earliest]); legal_part=clean_text(rest[earliest:])
                    owner_part=re.sub(r"\*+$","",owner_part).strip()
                    if pid and owner_part and len(owner_part)>=3:
                        parcels[pid]={"parcel_id":pid,"owner":owner_part,"amount_owed":amt,"legal":legal_part[:200],"source_url":url}
                if (i+1)%10==0: logging.info("Scraped %s/%s delinquent sections, %s parcels",i+1,len(links),len(parcels))
            except Exception as e: logging.warning("Delinquent section %s: %s",url,e)
    except Exception as e: logging.warning("Tax delinquent failed: %s",e)
    logging.info("Total delinquent parcels: %s",len(parcels))
    save_debug_json("delinquent_parcels.json",list(parcels.values())[:100])
    return parcels


# -----------------------------------------------------------------------
# CAMA
# -----------------------------------------------------------------------
def discover_cama_downloads()->List[str]:
    logging.info("Discovering CAMA downloads...")
    resp=retry_request(CAMA_PAGE_URL); soup=BeautifulSoup(resp.text,"lxml")
    wanted={"SC700","SC701","SC702","SC705","SC720","SC731"}; urls=[]
    for a in soup.select("a[href]"):
        href=clean_text(a.get("href","")); text=clean_text(a.get_text(" ")).upper()
        blob=f"{href} {text}".upper()
        if not any(c in blob for c in wanted): continue
        full=requests.compat.urljoin(CAMA_PAGE_URL,href)
        if "/finish/" in full: urls.append(full)
        elif "/viewdownload/" in full: urls.append(full.replace("/viewdownload/","/finish/"))
    deduped,seen=[],set()
    for u in urls:
        if u not in seen: seen.add(u); deduped.append(u)
    logging.info("Found %s CAMA file links",len(deduped)); return deduped

def parse_delimited_text(raw:str)->List[dict]:
    lines=[l.rstrip("\r") for l in raw.splitlines() if clean_text(l)]
    if len(lines)<2: return []
    candidates=["|","\t"," ,"]
    sample="\n".join(lines[:10]); delim=max(["|","\t",","],key=lambda d:sample.count(d))
    if sample.count(delim)==0: delim="|"
    rows=[]
    for row in csv.DictReader(io.StringIO("\n".join(lines)),delimiter=delim):
        cleaned={clean_text(k):clean_text(v) for k,v in row.items() if k is not None}
        if any(cleaned.values()): rows.append(cleaned)
    return rows

def read_any_cama_payload(content:bytes,source_name:str)->Dict[str,List[dict]]:
    datasets={}
    if len(content)>=4 and content[:2]==b"PK":
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            for member in z.namelist():
                if member.endswith("/"): continue
                try: raw=z.read(member).decode("utf-8",errors="ignore")
                except: continue
                rows=parse_delimited_text(raw)
                if rows: datasets[member]=rows
        return datasets
    raw=content.decode("utf-8",errors="ignore"); rows=parse_delimited_text(raw)
    if rows: datasets[source_name]=rows
    return datasets

def build_prop_address_from_row(row:dict)->str:
    parts=[clean_text(row.get(k,"")) for k in ["ADRNO","ADRADD","ADRDIR","ADRSTR","ADRSUF","ADRSUF2"]]
    addr=" ".join(p for p in parts if p).strip()
    return re.sub(r"\s+"," ",addr) if addr else safe_pick(row,LIKELY_PROP_ADDR_KEYS)

def build_prop_city_from_row(row:dict)->str:
    return clean_text(row.get("UDATE1","")) or clean_text(row.get("CITY","")) or safe_pick(row,LIKELY_PROP_CITY_KEYS)

def build_prop_zip_from_row(row:dict)->str:
    for k in ["NOTE2","USER2"]:
        v=clean_text(row.get(k,""))
        if v and re.fullmatch(r"\d{5}",v): return v
    zr=clean_text(row.get("ZIPCD","")); m=re.search(r"(\d{5})",zr)
    if m: return m.group(1)
    fb=safe_pick(row,LIKELY_PROP_ZIP_KEYS); m2=re.search(r"(\d{5})",fb)
    return m2.group(1) if m2 else ""

def build_assessed_value_from_row(row:dict)->Optional[float]:
    raw=safe_pick(row,LIKELY_VALUE_KEYS)
    if not raw: return None
    try:
        v=float(re.sub(r"[^0-9.]","",raw))
        return v if v>100 else None
    except: return None

def build_sale_data_from_row(row:dict)->Tuple[Optional[float],Optional[int]]:
    price_raw=safe_pick(row,["SALEPRICE","SALE_PRICE","LASTSALE","SALVAL"])
    year_raw=safe_pick(row,LIKELY_SALE_YEAR_KEYS)
    price=None; year=None
    if price_raw:
        try:
            p=float(re.sub(r"[^0-9.]","",price_raw))
            if p>5000: price=p
        except: pass
    if year_raw:
        try:
            y=int(re.sub(r"[^0-9]","",year_raw)[:4])
            if 1970<=y<=datetime.now().year: year=y
        except: pass
    return price,year

def is_sc701_clerk_code(v:str)->bool:
    v=clean_text(v).upper()
    if not v: return True
    for p in [r"^EOY\s+ROLL$",r"^CL_",r"^[A-Z]+WALKER$",r"^[A-Z]+WHITE$",r"^[A-Z]+JARJABKA$",r"^SCLB$",r"^LMRK$"]:
        if re.match(p,v): return True
    return bool(re.match(r"\d{1,2}-[A-Z]{3}-\d{4}",v))

def extract_owner_aliases_from_row(row:dict)->List[str]:
    aliases=[]
    is_mail="MAIL_ADR1" in row or "MAIL_PTR" in row
    if not is_mail:
        for k in LIKELY_OWNER_KEYS:
            v=safe_pick(row,[k])
            if v: aliases.append(v)
        cb=build_owner_name(row)
        if cb: aliases.append(cb)
    deduped,seen=[],set()
    for a in aliases:
        a=clean_text(a)
        if not a or a in BAD_EXACT_OWNERS or is_sc701_clerk_code(a): continue
        au=normalize_name(a)
        if re.fullmatch(r"\d{1,2}-[A-Z]{3}-\d{4}",au) or re.fullmatch(r"\d{5}",au): continue
        if re.fullmatch(r"[A-Z0-9_]+",au) and "_" in au: continue
        if len(au)<4: continue
        toks=tokens_from_name(au)
        if not toks: continue
        if len(toks)==1 and toks[0] in {"AKRON","BARBERTON","STOW","HUDSON","TWINSBURG","TALLMADGE","CUYAHOGA","FALLS","MUNROE","SPRINGFIELD","NORTHFIELD"}: continue
        if not likely_corporate_name(au) and len(toks)<2: continue
        if au not in seen: seen.add(au); deduped.append(au)
    return deduped

def add_candidate(index,key,record):
    k=clean_text(key)
    if k: index[k].append(record)

def add_owner_alias(record,owner_name):
    owner_name=clean_text(owner_name)
    if not owner_name: return
    record.setdefault("owner_aliases",[])
    if owner_name not in record["owner_aliases"]: record["owner_aliases"].append(owner_name)
    if not clean_text(record.get("owner","")): record["owner"]=owner_name

def normalize_candidate_record(r:dict)->dict:
    aliases=r.get("owner_aliases") or []; seen=[]; ca=[]
    for a in aliases:
        a=clean_text(a)
        if a and a not in seen: seen.append(a); ca.append(a)
    return {
        "parcel_id":clean_text(r.get("parcel_id","")), "owner":clean_text(r.get("owner","")),
        "owner_aliases":ca, "prop_address":clean_text(r.get("prop_address","")),
        "prop_city":clean_text(r.get("prop_city","")), "prop_zip":clean_text(r.get("prop_zip","")),
        "mail_address":clean_text(r.get("mail_address","")), "mail_city":clean_text(r.get("mail_city","")),
        "mail_state":normalize_state(clean_text(r.get("mail_state",""))), "mail_zip":clean_text(r.get("mail_zip","")),
        "legal":clean_text(r.get("legal","")), "luc":clean_text(r.get("luc","")),
        "acres":clean_text(r.get("acres","")),
        "assessed_value":r.get("assessed_value"), "last_sale_price":r.get("last_sale_price"),
        "last_sale_year":r.get("last_sale_year"),
    }

def build_parcel_indexes()->Tuple[Dict,Dict,Dict,List[dict],Dict[str,dict]]:
    urls=discover_cama_downloads()
    own_rows,mail_rows,legal_rows,parcel_rows=[],[],[],[]
    for url in urls:
        try:
            resp=retry_request(url)
            datasets=read_any_cama_payload(resp.content,Path(url).name)
            for fname,rows in datasets.items():
                u=fname.upper()
                if "SC700" in u: own_rows.extend(rows)
                elif "SC701" in u: mail_rows.extend(rows)
                elif "SC702" in u: legal_rows.extend(rows)
                elif any(x in u for x in ["SC705","SC731","SC720"]): parcel_rows.extend(rows)
            logging.info("Loaded CAMA %s",url)
        except Exception as e: logging.warning("CAMA %s: %s",url,e)
    save_debug_json("sc705_sc731_parcel_sample_rows.json",parcel_rows[:25])

    mail_by_pid:Dict[str,dict]={}
    for row in mail_rows:
        pid=get_pid(row)
        if pid and pid not in mail_by_pid: mail_by_pid[pid]=row

    parcel_by_id:Dict[str,dict]={}
    for row in parcel_rows:
        pid=get_pid(row)
        if not pid: continue
        parcel_by_id.setdefault(pid,{"parcel_id":pid,"owner_aliases":[]})
        e=parcel_by_id[pid]
        assessed=build_assessed_value_from_row(row); sale_price,sale_year=build_sale_data_from_row(row)
        e.update({
            "parcel_id":pid,
            "prop_address":e.get("prop_address","") or build_prop_address_from_row(row),
            "prop_city":e.get("prop_city","") or build_prop_city_from_row(row),
            "prop_zip":e.get("prop_zip","") or build_prop_zip_from_row(row),
            "luc":e.get("luc","") or clean_text(row.get("LUC","")),
            "acres":e.get("acres","") or clean_text(row.get("ACRES","")),
            "assessed_value":e.get("assessed_value") or assessed,
            "last_sale_price":e.get("last_sale_price") or sale_price,
            "last_sale_year":e.get("last_sale_year") or sale_year,
        })
        for a in extract_owner_aliases_from_row(row): add_owner_alias(e,a)

    for row in own_rows:
        pid=get_pid(row)
        if not pid: continue
        parcel_by_id.setdefault(pid,{"parcel_id":pid,"owner_aliases":[]})
        for a in extract_owner_aliases_from_row(row): add_owner_alias(parcel_by_id[pid],a)

    for row in mail_rows:
        pid=get_pid(row)
        if not pid: continue
        parcel_by_id.setdefault(pid,{"parcel_id":pid,"owner_aliases":[]})
        e=parcel_by_id[pid]
        ms=clean_text(row.get("MAIL_ADR1","")) or safe_pick(row,["MAIL_ADR1","MAIL_ADDR","MAILADR1"])
        mc=build_mail_city_sc701(row); mz=build_mail_zip(row); mst=build_mail_state_sc701(row)
        if not e.get("mail_address","") and ms: e["mail_address"]=ms
        if not e.get("mail_city","") and mc:    e["mail_city"]=mc
        if not e.get("mail_zip","") and mz:     e["mail_zip"]=mz
        if not e.get("mail_state","") and mst:  e["mail_state"]=mst
        if not e.get("prop_city","") and mc:    e["prop_city"]=mc

    for row in legal_rows:
        pid=get_pid(row)
        if not pid: continue
        parcel_by_id.setdefault(pid,{"parcel_id":pid,"owner_aliases":[]})
        e=parcel_by_id[pid]
        e["legal"]=e.get("legal","") or safe_pick(row,LIKELY_LEGAL_KEYS)
        for a in extract_owner_aliases_from_row(row): add_owner_alias(e,a)

    owner_index=defaultdict(list); last_name_index=defaultdict(list); first_last_index=defaultdict(list)
    seen_pid_last=defaultdict(set); seen_pid_fl=defaultdict(set); seen_pid_own=defaultdict(set)

    for raw_rec in parcel_by_id.values():
        rec=normalize_candidate_record(raw_rec)
        all_aliases=list(rec.get("owner_aliases") or [])
        owner=clean_text(rec.get("owner",""))
        if owner and owner not in all_aliases: all_aliases.append(owner)
        if not all_aliases: continue
        for alias in all_aliases:
            is_corp=likely_corporate_name(alias)
            for chunk in (split_owner_chunks(alias) or [alias]):
                for variant in (name_variants(chunk) or [normalize_person_name(chunk)]):
                    toks=tokens_from_name(variant)
                    if not is_corp and len(toks)<2: continue
                    pid=rec.get("parcel_id","")
                    if pid and pid in seen_pid_own[variant]: continue
                    if pid: seen_pid_own[variant].add(pid)
                    add_candidate(owner_index,variant,rec)
                ln=get_last_name(chunk); fn=get_first_name(chunk)
                if ln:
                    pid=rec.get("parcel_id","")
                    if not pid or pid not in seen_pid_last[ln]:
                        if pid: seen_pid_last[ln].add(pid)
                        last_name_index[ln].append(rec)
                if fn and ln:
                    fl=f"{fn} {ln}"; pid=rec.get("parcel_id","")
                    if not pid or pid not in seen_pid_fl[fl]:
                        if pid: seen_pid_fl[fl].add(pid)
                        first_last_index[fl].append(rec)

    logging.info("Parcel index: %s owner keys | %s parcels | %s mail rows",len(owner_index),len(parcel_rows),len(mail_rows))
    return owner_index,last_name_index,first_last_index,parcel_rows,mail_by_pid


# -----------------------------------------------------------------------
# LEAD BUILDERS
# -----------------------------------------------------------------------
def build_tax_delinquent_leads(delinquent_parcels,parcel_rows,mail_by_pid,vacant_home_keys)->List[LeadRecord]:
    leads=[]; skipped=0
    pid_to_row={get_pid(r):r for r in parcel_rows if get_pid(r)}
    for pid,info in delinquent_parcels.items():
        row=pid_to_row.get(pid)
        if not row: continue
        luc=clean_text(row.get("LUC",""))
        if luc not in RESIDENTIAL_LUCS: skipped+=1; continue
        prop_address=build_prop_address_from_row(row)
        prop_city=build_prop_city_from_row(row)
        prop_zip=build_prop_zip_from_row(row)
        if not prop_address: continue
        mail_row=mail_by_pid.get(pid,{})
        mail_address=clean_text(mail_row.get("MAIL_ADR1","")) if mail_row else ""
        mail_city=build_mail_city_sc701(mail_row) if mail_row else ""
        mail_zip=build_mail_zip(mail_row) if mail_row else ""
        mail_state=build_mail_state_sc701(mail_row) if mail_row else "OH"
        owner=info.get("owner",""); amt=info.get("amount_owed",0.0)
        acres=clean_text(row.get("ACRES",""))
        assessed=build_assessed_value_from_row(row); sale_price,sale_year=build_sale_data_from_row(row)
        absentee=is_absentee_owner(prop_address,mail_address,mail_state)
        oos=is_out_of_state(mail_state)
        addr_key=normalize_address_key(prop_address); vhome=addr_key in vacant_home_keys
        flags=["Tax delinquent","Residential"]; ds=["tax_delinquent"]
        if absentee: flags.append("Absentee owner")
        if oos: flags.append("Out-of-state owner")
        if vhome: flags.append("Vacant home"); ds.append("vacant_home")
        if amt and amt>10000: flags.append("High tax debt")
        if amt and amt>25000: flags.append("Very high tax debt")
        r=LeadRecord(
            doc_num=f"TAX-{pid}",doc_type="TAX",cat="TAX",cat_label="Tax Delinquent",
            owner=owner.title(),amount=amt,prop_address=prop_address,prop_city=prop_city,
            prop_state="OH",prop_zip=prop_zip,mail_address=mail_address,mail_city=mail_city,
            mail_state=normalize_state(mail_state) or "OH",mail_zip=mail_zip,
            clerk_url=info.get("source_url",""),flags=flags,distress_sources=ds,
            distress_count=len(ds),luc=luc,acres=acres,is_vacant_home=vhome,
            is_absentee=absentee,is_out_of_state=oos,with_address=1,
            match_method="tax_delinquent_direct",match_score=1.0,parcel_id=pid,
            assessed_value=assessed,last_sale_price=sale_price,last_sale_year=sale_year,
        )
        r=estimate_mortgage_data(r); r.score=score_record(r); r.hot_stack=r.distress_count>=2
        leads.append(r)
    logging.info("Tax delinquent: %s residential | %s skipped | %s absentee | %s OOS | %s vacant home",
                 len(leads),skipped,sum(1 for r in leads if r.is_absentee),
                 sum(1 for r in leads if r.is_out_of_state),sum(1 for r in leads if r.is_vacant_home))
    return leads

def build_vacant_home_list(vacant_addresses,parcel_rows,mail_by_pid,delinquent_pid_set,foreclosure_pids)->Tuple[List[LeadRecord],set]:
    records=[]; matched_keys=set(); seen_pids=set()
    addr_to_pid:Dict[str,str]={}; pid_to_row:Dict[str,dict]={}
    for row in parcel_rows:
        pid=get_pid(row)
        if not pid: continue
        pid_to_row[pid]=row
        if clean_text(row.get("LUC","")) not in RESIDENTIAL_LUCS: continue
        addr=build_prop_address_from_row(row)
        if addr:
            key=normalize_address_key(addr)
            if key: addr_to_pid[key]=pid
    for va in vacant_addresses:
        key=normalize_address_key(va); pid=addr_to_pid.get(key)
        if not pid:
            parts=key.split()
            if len(parts)>=2:
                short=" ".join(parts[:2])
                for k,p in addr_to_pid.items():
                    if k.startswith(short): pid=p; break
        if not pid or pid in seen_pids: continue
        seen_pids.add(pid); matched_keys.add(key)
        row=pid_to_row.get(pid,{}); luc=clean_text(row.get("LUC",""))
        prop_address=build_prop_address_from_row(row); prop_city=build_prop_city_from_row(row)
        prop_zip=build_prop_zip_from_row(row); acres=clean_text(row.get("ACRES",""))
        assessed=build_assessed_value_from_row(row); sale_price,sale_year=build_sale_data_from_row(row)
        mail_row=mail_by_pid.get(pid,{})
        mail_address=clean_text(mail_row.get("MAIL_ADR1","")) if mail_row else ""
        mail_city=build_mail_city_sc701(mail_row) if mail_row else ""
        mail_zip=build_mail_zip(mail_row) if mail_row else ""
        mail_state=build_mail_state_sc701(mail_row) if mail_row else "OH"
        owner=build_owner_name(mail_row) if mail_row else ""
        absentee=is_absentee_owner(prop_address,mail_address,mail_state)
        oos=is_out_of_state(mail_state)
        tax_delin=pid in delinquent_pid_set; foreclosure=pid in foreclosure_pids
        flags=["Vacant home","Residential"]; ds=["vacant_home"]
        if absentee: flags.append("Absentee owner")
        if oos: flags.append("Out-of-state owner")
        if tax_delin: flags.append("Tax delinquent"); ds.append("tax_delinquent")
        if foreclosure: flags.append("In foreclosure"); ds.append("foreclosure")
        rec=LeadRecord(
            doc_num=f"VHOME-{pid}",doc_type="VHOME",filed=datetime.now().date().isoformat(),
            cat="VHOME",cat_label="Vacant Home",owner=owner.title() if owner else "",
            prop_address=prop_address or va.title(),prop_city=prop_city,prop_state="OH",prop_zip=prop_zip,
            mail_address=mail_address,mail_city=mail_city,mail_state=normalize_state(mail_state) or "OH",mail_zip=mail_zip,
            clerk_url=VACANT_BUILDING_URL,flags=flags,distress_sources=ds,distress_count=len(ds),
            luc=luc,acres=acres,is_vacant_home=True,is_absentee=absentee,is_out_of_state=oos,
            with_address=1,match_method="vacant_home_board",match_score=1.0,parcel_id=pid,
            assessed_value=assessed,last_sale_price=sale_price,last_sale_year=sale_year,
        )
        rec=estimate_mortgage_data(rec); rec.score=score_record(rec); rec.hot_stack=rec.distress_count>=2
        records.append(rec)
    logging.info("Vacant homes: %s matched | %s absentee | %s OOS | %s tax delin | %s foreclosure",
                 len(records),sum(1 for r in records if r.is_absentee),sum(1 for r in records if r.is_out_of_state),
                 sum(1 for r in records if "Tax delinquent" in r.flags),sum(1 for r in records if "In foreclosure" in r.flags))
    return records,matched_keys

def build_vacant_land_list(parcel_rows,mail_by_pid,delinquent_pid_set,foreclosure_pids)->List[VacantLandRecord]:
    vacant=[]; seen=set()
    for row in parcel_rows:
        luc=clean_text(row.get("LUC","")); acres_raw=clean_text(row.get("ACRES",""))
        if not is_infill_lot(luc,acres_raw): continue
        pid=get_pid(row)
        if not pid or pid in seen: continue
        if pid not in delinquent_pid_set and pid not in foreclosure_pids: continue
        seen.add(pid)
        prop_address=build_prop_address_from_row(row); prop_zip=build_prop_zip_from_row(row)
        mail_row=mail_by_pid.get(pid,{})
        ms=clean_text(mail_row.get("MAIL_ADR1","")) if mail_row else ""
        mc=build_mail_city_sc701(mail_row) if mail_row else ""
        mz=build_mail_zip(mail_row) if mail_row else ""
        mst=build_mail_state_sc701(mail_row) if mail_row else ""
        owner=build_owner_name(mail_row) if mail_row else ""
        if not prop_address and not ms: continue
        flags=["Vacant land","Infill lot"]; ds=[]
        if pid in foreclosure_pids: flags.append("🔥 In foreclosure"); ds.append("foreclosure")
        if pid in delinquent_pid_set: flags.append("Tax delinquent"); ds.append("tax_delinquent")
        vacant.append(VacantLandRecord(
            parcel_id=pid,prop_address=prop_address,prop_city=mc,prop_state="OH",
            prop_zip=prop_zip,owner=owner,mail_address=ms,mail_city=mc,
            mail_state=mst or "OH",mail_zip=mz,luc=luc,acres=acres_raw,
            flags=flags,score=55 if len(ds)>=2 else 45,distress_sources=ds,distress_count=len(ds)
        ))
    logging.info("Distressed vacant land (infill ≤2ac): %s",len(vacant)); return vacant


# -----------------------------------------------------------------------
# PLAYWRIGHT SCRAPING
# -----------------------------------------------------------------------
async def click_first_matching(page,selectors):
    for s in selectors:
        try:
            loc=page.locator(s).first
            if await loc.count()>0: await loc.click(); await page.wait_for_timeout(2500); return True
        except: continue
    return False

def infer_doc_type_from_text(text:str)->Optional[str]:
    t=clean_text(text).upper()
    if any(x in t for x in ["LIS PENDENS"," LP ","LP-"]): return "LP"
    if any(x in t for x in ["NOTICE OF FORECLOSURE","FORECLOS","NOFC"]): return "NOFC"
    if any(x in t for x in ["CERTIFIED JUDGMENT","DOMESTIC JUDGMENT","JUDGMENT"]): return "JUD"
    if any(x in t for x in ["TAX DEED","TAXDEED"]): return "TAXDEED"
    if any(x in t for x in ["IRS LIEN","FEDERAL LIEN","TAX LIEN"]): return "LNFED"
    if "MECHANIC LIEN" in t: return "LNMECH"
    if "LIEN" in t: return "LN"
    if "NOTICE OF COMMENCEMENT" in t: return "NOC"
    return None

def try_parse_date(text:str)->Optional[str]:
    text=clean_text(text)
    if not text: return None
    for p in [r"\b\d{4}-\d{2}-\d{2}\b",r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",r"\b\d{1,2}-\d{1,2}-\d{2,4}\b"]:
        m=re.search(p,text)
        if m:
            raw=m.group(0)
            for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%m-%d-%Y","%m-%d-%y"):
                try: return datetime.strptime(raw,fmt).date().isoformat()
                except ValueError: continue
    return None

def extract_case_number(text,fallback):
    tu=clean_text(text).upper()
    for p in [r"\b\d{2,4}[ -][A-Z]{1,6}[ -]\d{2,8}\b",r"\b[A-Z]{2,}[ ]\d{2}\b",r"\b\d{6,}\b"]:
        m=re.search(p,tu)
        if m: return clean_text(m.group(0))
    return fallback

def split_caption(caption):
    cap=clean_text(caption); upper=cap.upper()
    for sep in [" -VS- "," VS. "," VS "," V. "," V "]:
        if sep in upper:
            parts=re.split(re.escape(sep),cap,maxsplit=1,flags=re.IGNORECASE)
            if len(parts)==2: return clean_text(parts[0]),clean_text(parts[1])
    return "",""

def clean_defendant_name(n):
    n=clean_text(n)
    if not n: return ""
    for p in [r"\bAKA\b.*$",r"\bET AL\b.*$",r"\bUNKNOWN HEIRS OF\b",r"\bUNKNOWN SPOUSE OF\b",
              r"\bUNKNOWN ADMINISTRATOR\b",r"\bEXECUTOR\b",r"\bFIDUCIARY\b",r"\bJOHN DOE\b",r"\bJANE DOE\b"]:
        n=re.sub(p,"",n,flags=re.IGNORECASE).strip()
    n=re.sub(r"\s+"," ",n).strip(" ,.-")
    return "" if (not n or n in BAD_EXACT_OWNERS) else n

def looks_like_good_owner(n):
    n=clean_text(n)
    if not n or n in BAD_EXACT_OWNERS or len(n)<4: return False
    return sum(c.isalpha() for c in n)>=4

def extract_owner_and_grantee(cells):
    rt=clean_text(" ".join(cells))
    for candidate in cells+[rt]:
        p,d=split_caption(candidate); d=clean_defendant_name(d)
        if looks_like_good_owner(d): return d.title(),clean_text(p).title(),candidate
    return "","",rt

def parse_pending_civil_table(html,base_url,prefix)->List[LeadRecord]:
    soup=BeautifulSoup(html,"lxml"); records=[]
    for ti,table in enumerate(soup.find_all("table"),1):
        for ri,row in enumerate(table.find_all("tr"),1):
            cells=[clean_text(td.get_text(" ")) for td in row.find_all(["td","th"])]
            if not cells: continue
            rt=clean_text(" ".join(cells)); dt=infer_doc_type_from_text(rt)
            if dt not in {"NOFC","LP","JUD","LN","LNMECH","LNFED","NOC"}: continue
            filed=try_parse_date(rt) or datetime.now().date().isoformat()
            if datetime.fromisoformat(filed).date()<(datetime.now().date()-timedelta(days=LOOKBACK_DAYS)): continue
            owner,grantee,src=extract_owner_and_grantee(cells)
            if not owner: continue
            am=re.search(r"\$[\d,]+(?:\.\d{2})?",rt); amt=parse_amount(am.group(0)) if am else None
            link=row.find("a",href=True); href=clean_text(link.get("href","")) if link else ""
            dn=extract_case_number(rt,f"{prefix}-T{ti}-R{ri}")
            rec=LeadRecord(
                doc_num=dn,doc_type=dt,filed=filed,cat=dt,cat_label=LEAD_TYPE_MAP.get(dt,dt),
                owner=owner,grantee=grantee,amount=amt,legal=clean_text(src),
                clerk_url=requests.compat.urljoin(base_url,href) if href else base_url,
            )
            rec.flags=category_flags(dt,owner); ds=classify_distress_source(dt)
            if ds: rec.distress_sources=[ds]
            rec.score=score_record(rec); records.append(rec)
    return records

async def scrape_pending_civil_records(page)->List[LeadRecord]:
    records=[]
    try:
        await page.goto(PENDING_CIVIL_URL,wait_until="domcontentloaded",timeout=90000); await page.wait_for_timeout(4000)
        h1=await page.content(); save_debug_text("pending_civil_page_1.html",h1)
        records.extend(parse_pending_civil_table(h1,PENDING_CIVIL_URL,"PCF1"))
        if await click_first_matching(page,["text=Search","text=Begin","text=Continue","input[type='submit']","button","a"]):
            h2=await page.content(); records.extend(parse_pending_civil_table(h2,PENDING_CIVIL_URL,"PCF2"))
    except Exception as e: logging.warning("Pending civil failed: %s",e)
    return records

async def scrape_clerk_records()->List[LeadRecord]:
    logging.info("Scraping clerk records..."); records=[]
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=True); page=await browser.new_page()
        try:
            await page.goto(CLERK_RECORDS_URL,wait_until="domcontentloaded",timeout=90000); await page.wait_for_timeout(4000)
            if await click_first_matching(page,["text=Click Here","text=Begin","text=Continue","text=Accept","text=Search","input[type='submit']","button","a"]):
                await click_first_matching(page,["text=Civil","text=General","text=Search","input[type='submit']","button","a"])
            records.extend(await scrape_pending_civil_records(page))
        except Exception as e: logging.warning("Clerk scrape failed: %s",e)
        finally: await browser.close()
    deduped,seen=[],set()
    for r in records:
        nd=re.sub(r"^(PCF1|PCF2)-","",clean_text(r.doc_num).upper())
        key=(nd,clean_text(r.doc_type).upper(),normalize_name(r.owner),clean_text(r.filed))
        if key in seen: continue
        seen.add(key); deduped.append(r)
    logging.info("Clerk records: %s",len(deduped)); return deduped


# -----------------------------------------------------------------------
# PARCEL MATCHING
# -----------------------------------------------------------------------
def better_record(c):
    s=0
    if clean_text(c.get("prop_address","")): s+=100
    if clean_text(c.get("mail_address","")): s+=40
    if clean_text(c.get("mail_zip","")): s+=20
    if clean_text(c.get("mail_city","")): s+=15
    if clean_text(c.get("legal","")): s+=15
    if c.get("last_sale_price"): s+=5
    if c.get("assessed_value"): s+=3
    return s

def alias_list(c):
    a=list(c.get("owner_aliases") or []); o=clean_text(c.get("owner",""))
    if o and o not in a: a.append(o); return a

def candidate_match_score(ro,c):
    best=0.0
    for co in alias_list(c):
        rt=set(tokens_from_name(ro)); ct=set(tokens_from_name(co))
        if not rt or not ct: continue
        s=len(rt&ct)*10.0; rl,cl=get_last_name(ro),get_last_name(co); rf,cf=get_first_name(ro),get_first_name(co)
        if rl and cl and last_names_compatible(rl,cl): s+=25.0
        if rf and cf and rf==cf: s+=18.0
        elif same_first_name_or_initial(ro,co): s+=10.0
        if clean_text(c.get("prop_address","")): s+=8.0
        if clean_text(c.get("mail_address","")): s+=4.0
        if s>best: best=s
    return best

def choose_best_candidate(candidates,ro=""):
    if not candidates: return None
    deduped={}
    for c in candidates:
        key=clean_text(c.get("parcel_id","")) or f"{clean_text(c.get('owner',''))}|{clean_text(c.get('prop_address',''))}"
        if key not in deduped or better_record(c)>better_record(deduped[key]): deduped[key]=c
    ranked=sorted(deduped.values(),key=lambda c:(candidate_match_score(ro,c),better_record(c)),reverse=True)
    return ranked[0] if ranked else None

def unique_best_by_score(candidates,ro,min_gap=12.0):
    if not candidates: return None
    deduped={}
    for c in candidates:
        key=clean_text(c.get("parcel_id","")) or f"{clean_text(c.get('owner',''))}|{clean_text(c.get('prop_address',''))}"
        if key not in deduped or better_record(c)>better_record(deduped[key]): deduped[key]=c
    ranked=sorted([(c,candidate_match_score(ro,c),better_record(c)) for c in deduped.values()],key=lambda x:(x[1],x[2]),reverse=True)
    if not ranked: return None
    if len(ranked)==1: return ranked[0][0]
    if ranked[0][1]>=ranked[1][1]+min_gap: return ranked[0][0]
    return None

def fuzzy_match_record(record,owner_index,last_name_index,first_last_index):
    owner=record.owner; is_corp=likely_corporate_name(owner)
    for v in name_variants(owner):
        if not is_corp and len(tokens_from_name(v))<2: continue
        best=choose_best_candidate(owner_index.get(v,[]),owner)
        if best: return best,"exact_name_variant",1.0
    fn,ln=get_first_name(owner),get_last_name(owner); ot=set(tokens_from_name(owner))
    if fn and ln:
        c=first_last_index.get(f"{fn} {ln}",[])
        best=unique_best_by_score(c,owner,8.0) or choose_best_candidate(c,owner)
        if best: return best,"first_last_fallback",0.95
        best=choose_best_candidate(owner_index.get(f"{ln} {fn}",[]),owner)
        if best: return best,"last_first_variant",0.94
    if ln and not is_corp:
        candidates=last_name_index.get(ln,[])
        strong=[c for c in candidates if any(
            last_names_compatible(ln,get_last_name(co)) and
            len(ot&set(tokens_from_name(co)))>=2 and same_first_name_or_initial(owner,co)
            for co in alias_list(c))]
        best=unique_best_by_score(strong,owner,6.0) or choose_best_candidate(strong,owner)
        if best: return best,"token_overlap_strict",0.90
        uc,seen=[],set()
        for c in candidates:
            if not any(last_names_compatible(ln,get_last_name(co)) for co in alias_list(c)): continue
            k=clean_text(c.get("parcel_id","")) or f"{clean_text(c.get('owner',''))}|{clean_text(c.get('prop_address',''))}"
            if k in seen: continue
            seen.add(k); uc.append(c)
        if len(uc)==1: return uc[0],"last_name_unique_fallback",0.82
        ic=[c for c in uc if any(same_first_name_or_initial(owner,co) for co in alias_list(c))]
        best=unique_best_by_score(ic,owner,8.0)
        if best: return best,"last_name_initial_fallback",0.84
        if candidates: return None,"no_property_match",0.0
    return None,"unmatched",0.0

def enrich_with_parcel_data(records,owner_index,last_name_index,first_last_index):
    enriched=[]
    report={"matched":0,"unmatched":0,"with_address":0,"with_mail_address":0,
            "match_methods":defaultdict(int),"sample_unmatched":[],"sample_no_property_match":[]}
    for record in records:
        try:
            matched,method,ms=fuzzy_match_record(record,owner_index,last_name_index,first_last_index)
            if matched:
                record.prop_address=record.prop_address or clean_text(matched.get("prop_address",""))
                record.prop_city=record.prop_city or clean_text(matched.get("prop_city",""))
                record.prop_zip=record.prop_zip or clean_text(matched.get("prop_zip",""))
                record.mail_address=record.mail_address or clean_text(matched.get("mail_address",""))
                record.mail_city=record.mail_city or clean_text(matched.get("mail_city",""))
                record.mail_zip=record.mail_zip or clean_text(matched.get("mail_zip",""))
                record.legal=record.legal or clean_text(matched.get("legal",""))
                record.mail_state=record.mail_state or normalize_state(clean_text(matched.get("mail_state",""))) or "OH"
                record.luc=record.luc or clean_text(matched.get("luc",""))
                record.acres=record.acres or clean_text(matched.get("acres",""))
                record.parcel_id=record.parcel_id or clean_text(matched.get("parcel_id",""))
                if not record.assessed_value: record.assessed_value=matched.get("assessed_value")
                if not record.last_sale_price: record.last_sale_price=matched.get("last_sale_price")
                if not record.last_sale_year:  record.last_sale_year=matched.get("last_sale_year")
                record.match_method=method; record.match_score=ms
                report["matched"]+=1; report["match_methods"][method]+=1
            else:
                record.match_method=method; record.match_score=0.0
                report["unmatched"]+=1; report["match_methods"][method]+=1
                if method=="no_property_match" and len(report["sample_no_property_match"])<25:
                    report["sample_no_property_match"].append({"doc_num":record.doc_num,"owner":record.owner})
                elif len(report["sample_unmatched"])<25:
                    report["sample_unmatched"].append({"doc_num":record.doc_num,"owner":record.owner})
            if record.luc in VACANT_LAND_LUCS:
                record.is_vacant_land=True
                if "Vacant land" not in record.flags: record.flags.append("Vacant land")
            record.mail_state=normalize_state(record.mail_state) or ("OH" if record.mail_address else "")
            record.with_address=1 if clean_text(record.prop_address) else 0
            record.is_absentee=is_absentee_owner(record.prop_address,record.mail_address,record.mail_state)
            record.is_out_of_state=is_out_of_state(record.mail_state)
            if record.is_absentee and "Absentee owner" not in record.flags: record.flags.append("Absentee owner")
            if record.is_out_of_state and "Out-of-state owner" not in record.flags: record.flags.append("Out-of-state owner")
            if record.with_address: report["with_address"]+=1
            if clean_text(record.mail_address): report["with_mail_address"]+=1
            record.flags=list(dict.fromkeys(record.flags+category_flags(record.doc_type,record.owner)))
            if record.match_method=="no_property_match" and "No property match" not in record.flags:
                record.flags.append("No property match")
            record=estimate_mortgage_data(record); record.score=score_record(record)
            enriched.append(record)
        except Exception as e:
            logging.warning("Enrich failed %s: %s",getattr(record,"doc_num","?"),e)
            try:
                record.with_address=1 if clean_text(record.prop_address) else 0
                enriched.append(record)
            except: pass
    report["match_methods"]=dict(report["match_methods"]); return enriched,report

def build_distress_index(records,vacant_home_keys):
    index=defaultdict(list)
    for r in records:
        if not r.prop_address: continue
        key=normalize_address_key(r.prop_address); src=classify_distress_source(r.doc_type)
        if src and src not in index[key]: index[key].append(src)
        if r.luc in VACANT_LAND_LUCS and "vacant_land" not in index[key]: index[key].append("vacant_land")
    for ak in vacant_home_keys:
        if "vacant_home" not in index[ak]: index[ak].append("vacant_home")
    return dict(index)

def build_delinquent_address_index(parcel_rows,mail_by_pid,delinquent_parcels):
    da={}; dpset=set(delinquent_parcels.keys())
    for row in parcel_rows:
        pid=get_pid(row)
        if not pid or pid not in dpset: continue
        addr=build_prop_address_from_row(row)
        if addr:
            key=normalize_address_key(addr)
            if key:
                info=delinquent_parcels[pid].copy()
                info["prop_address"]=addr; info["prop_zip"]=build_prop_zip_from_row(row)
                info["luc"]=clean_text(row.get("LUC",""))
                da[key]=info
    logging.info("Mapped %s delinquent addresses",len(da)); return da

def apply_distress_stacking(records,distress_index,delinquent_addresses,vacant_home_keys):
    for r in records:
        if not r.prop_address: continue
        key=normalize_address_key(r.prop_address); sources=list(distress_index.get(key,[]))
        di=delinquent_addresses.get(key)
        if di:
            if "tax_delinquent" not in sources: sources.append("tax_delinquent")
            if "Tax delinquent" not in r.flags: r.flags.append("Tax delinquent")
            if r.amount is None and di.get("amount_owed"): r.amount=di["amount_owed"]
        if key in vacant_home_keys and r.luc in RESIDENTIAL_LUCS:
            r.is_vacant_home=True
            if "vacant_home" not in sources: sources.append("vacant_home")
            if "Vacant home" not in r.flags: r.flags.append("Vacant home")
        r.distress_sources=list(set(sources)); r=estimate_mortgage_data(r); r.score=score_record(r)
    return records

def dedupe_records(records):
    final,seen=[],set()
    for r in records:
        nd=re.sub(r"^(PCF1|PCF2)-","",clean_text(r.doc_num).upper())
        key=(nd,clean_text(r.doc_type).upper(),normalize_name(r.owner),clean_text(r.filed))
        if key in seen: continue
        seen.add(key); final.append(r)
    return final


# -----------------------------------------------------------------------
# OUTPUT
# -----------------------------------------------------------------------
def split_name(n):
    parts=clean_text(n).split()
    if not parts: return "",""
    if len(parts)==1: return parts[0],""
    return parts[0]," ".join(parts[1:])

def write_json(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(payload,indent=2),encoding="utf-8")

def build_payload(records):
    return {
        "fetched_at":datetime.now(timezone.utc).isoformat(), "source":SOURCE_NAME,
        "date_range":{"from":(datetime.now()-timedelta(days=LOOKBACK_DAYS)).date().isoformat(),"to":datetime.now().date().isoformat()},
        "total":len(records),
        "with_address":sum(1 for r in records if r.prop_address),
        "with_mail_address":sum(1 for r in records if r.mail_address),
        "hot_stack_count":sum(1 for r in records if r.hot_stack),
        "vacant_home_count":sum(1 for r in records if r.is_vacant_home),
        "absentee_count":sum(1 for r in records if r.is_absentee),
        "out_of_state_count":sum(1 for r in records if r.is_out_of_state),
        "tax_delinquent_count":sum(1 for r in records if "Tax delinquent" in r.flags),
        "sheriff_sale_count":sum(1 for r in records if r.doc_type=="SHERIFF"),
        "probate_count":sum(1 for r in records if r.doc_type=="PRO"),
        "code_violation_count":sum(1 for r in records if r.doc_type=="CODEVIOLATION"),
        "subject_to_count":sum(1 for r in records if r.subject_to_score>=50),
        "prime_subject_to_count":sum(1 for r in records if r.subject_to_score>=70),
        "records":[asdict(r) for r in records],
    }

def write_json_outputs(records,extra_json_path=None):
    payload=build_payload(records); paths=list(DEFAULT_OUTPUT_JSON_PATHS)
    if extra_json_path: paths.append(extra_json_path)
    seen=set()
    for path in paths:
        if str(path) in seen: continue
        seen.add(str(path)); write_json(path,payload)
    logging.info("Wrote main JSON outputs")

def write_category_json(records):
    categories={
        "hot_stack":         [r for r in records if r.hot_stack],
        "sheriff_sales":     [r for r in records if r.doc_type=="SHERIFF"],
        "probate":           [r for r in records if r.doc_type=="PRO"],
        "code_violations":   [r for r in records if r.doc_type=="CODEVIOLATION"],
        "vacant_homes":      [r for r in records if r.is_vacant_home],
        "tax_delinquent":    [r for r in records if "Tax delinquent" in r.flags],
        "absentee":          [r for r in records if r.is_absentee],
        "out_of_state":      [r for r in records if r.is_out_of_state],
        "foreclosure":       [r for r in records if r.doc_type in {"LP","NOFC","TAXDEED"}],
        "subject_to":        [r for r in records if r.subject_to_score>=50],
        "prime_subject_to":  [r for r in records if r.subject_to_score>=70],
        "inherited":         [r for r in records if r.is_inherited],
    }
    descs={
        "hot_stack":        "🔥 2+ distress signals — highest priority",
        "sheriff_sales":    "⚡ Properties scheduled for sheriff auction — days from losing everything",
        "probate":          "⚖️ Estate / probate filings — inherited properties, out-of-state heirs",
        "code_violations":  "🏚 City-declared nuisance / housing code violations — repair or sell",
        "vacant_homes":     "🏠 Vacant residential homes (Akron Vacant Building Board)",
        "tax_delinquent":   "💰 Residential properties with unpaid property taxes",
        "absentee":         "📭 Owner mailing address differs from property",
        "out_of_state":     "🌎 Owner mails from out of state",
        "foreclosure":      "⚠️ Active foreclosure / lis pendens filings",
        "subject_to":       "🎯 Subject-to acquisition candidates (score ≥50)",
        "prime_subject_to": "⭐ Prime subject-to deals — high equity + motivated seller (score ≥70)",
        "inherited":        "🏛 Inherited properties — heirs may want quick sale",
    }
    output_paths={
        "hot_stack":        (DATA_DIR/"hot_stack.json",        DASHBOARD_DIR/"hot_stack.json"),
        "sheriff_sales":    (DATA_DIR/"sheriff_sales.json",    DASHBOARD_DIR/"sheriff_sales.json"),
        "probate":          (DATA_DIR/"probate.json",          DASHBOARD_DIR/"probate.json"),
        "code_violations":  (DATA_DIR/"code_violations.json",  DASHBOARD_DIR/"code_violations.json"),
        "vacant_homes":     (DATA_DIR/"vacant_homes.json",     DASHBOARD_DIR/"vacant_homes.json"),
        "tax_delinquent":   (DATA_DIR/"tax_delinquent.json",   DASHBOARD_DIR/"tax_delinquent.json"),
        "absentee":         (DATA_DIR/"absentee.json",         DASHBOARD_DIR/"absentee.json"),
        "out_of_state":     (DATA_DIR/"out_of_state.json",     DASHBOARD_DIR/"out_of_state.json"),
        "foreclosure":      (DATA_DIR/"foreclosure.json",      DASHBOARD_DIR/"foreclosure.json"),
        "subject_to":       (DATA_DIR/"subject_to.json",       DASHBOARD_DIR/"subject_to.json"),
        "prime_subject_to": (DATA_DIR/"prime_subject_to.json", DASHBOARD_DIR/"prime_subject_to.json"),
        "inherited":        (DATA_DIR/"inherited.json",        DASHBOARD_DIR/"inherited.json"),
    }
    for cat,recs in categories.items():
        recs_s=sorted(recs,key=lambda r:(r.hot_stack,r.distress_count,r.subject_to_score,r.score),reverse=True)
        payload={"fetched_at":datetime.now(timezone.utc).isoformat(),"source":SOURCE_NAME,
                 "category":cat,"description":descs[cat],"total":len(recs_s),
                 "records":[asdict(r) for r in recs_s]}
        for path in output_paths[cat]: write_json(path,payload)
        logging.info("Wrote %s: %s records",cat,len(recs_s))

def write_vacant_land_json(vacant):
    payload={"fetched_at":datetime.now(timezone.utc).isoformat(),"source":SOURCE_NAME,
             "total":len(vacant),"description":"Distressed vacant infill lots ≤2ac",
             "records":[asdict(r) for r in vacant]}
    for path in [DATA_DIR/"vacant_land.json",DASHBOARD_DIR/"vacant_land.json"]: write_json(path,payload)
    logging.info("Wrote vacant land: %s records",len(vacant))

def write_csv(records,csv_path):
    csv_path.parent.mkdir(parents=True,exist_ok=True)
    fieldnames=[
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Subject-To Score","Motivated Seller Flags","Distress Sources","Distress Count",
        "Hot Stack","Vacant Land","Vacant Home","Absentee Owner","Out-of-State Owner","Inherited",
        # mortgage / equity columns
        "Assessed Value","Est Market Value","Last Sale Price","Last Sale Year",
        "Est Mortgage Balance","Est Equity","Est Arrears","Est Payoff","Mortgage Signals",
        # probate columns
        "Decedent Name","Executor Name","Executor State",
        # other
        "Sheriff Sale Date","Lender","Code Violation Case",
        "Parcel ID","LUC Code","Acres","Match Method","Match Score",
        # skip trace (blank until skip traced)
        "Phone 1","Phone 1 Type","Phone 2","Phone 2 Type","Phone 3","Phone 3 Type",
        "Email","Skip Trace Source",
        "Source","Public Records URL",
    ]
    with csv_path.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fieldnames); w.writeheader()
        for r in records:
            fn,ln=split_name(r.owner)
            w.writerow({
                "First Name":fn,"Last Name":ln,
                "Mailing Address":r.mail_address,"Mailing City":r.mail_city,
                "Mailing State":r.mail_state,"Mailing Zip":r.mail_zip,
                "Property Address":r.prop_address,"Property City":r.prop_city,
                "Property State":r.prop_state,"Property Zip":r.prop_zip,
                "Lead Type":r.cat_label,"Document Type":r.doc_type,
                "Date Filed":r.filed,"Document Number":r.doc_num,
                "Amount/Debt Owed":f"${r.amount:,.2f}" if r.amount is not None else "",
                "Seller Score":r.score,"Subject-To Score":r.subject_to_score,
                "Motivated Seller Flags":"; ".join(r.flags),
                "Distress Sources":"; ".join(r.distress_sources),
                "Distress Count":r.distress_count,
                "Hot Stack":"YES" if r.hot_stack else "",
                "Vacant Land":"YES" if r.is_vacant_land else "",
                "Vacant Home":"YES" if r.is_vacant_home else "",
                "Absentee Owner":"YES" if r.is_absentee else "",
                "Out-of-State Owner":"YES" if r.is_out_of_state else "",
                "Inherited":"YES" if r.is_inherited else "",
                "Assessed Value":f"${r.assessed_value:,.0f}" if r.assessed_value else "",
                "Est Market Value":f"${r.estimated_value:,.0f}" if r.estimated_value else "",
                "Last Sale Price":f"${r.last_sale_price:,.0f}" if r.last_sale_price else "",
                "Last Sale Year":r.last_sale_year or "",
                "Est Mortgage Balance":f"${r.est_mortgage_balance:,.0f}" if r.est_mortgage_balance is not None else "",
                "Est Equity":f"${r.est_equity:,.0f}" if r.est_equity is not None else "",
                "Est Arrears":f"${r.est_arrears:,.0f}" if r.est_arrears is not None else "",
                "Est Payoff":f"${r.est_payoff:,.0f}" if r.est_payoff is not None else "",
                "Mortgage Signals":"; ".join(r.mortgage_signals),
                "Decedent Name":r.decedent_name,"Executor Name":r.executor_name,
                "Executor State":r.executor_state,
                "Sheriff Sale Date":r.sheriff_sale_date,"Lender":r.lender,
                "Code Violation Case":r.code_violation_case,
                "Parcel ID":r.parcel_id,"LUC Code":r.luc,"Acres":r.acres,
                "Match Method":r.match_method,"Match Score":r.match_score,
                "Phone 1":r.phones[0] if len(r.phones)>0 else "",
                "Phone 1 Type":r.phone_types[0] if len(r.phone_types)>0 else "",
                "Phone 2":r.phones[1] if len(r.phones)>1 else "",
                "Phone 2 Type":r.phone_types[1] if len(r.phone_types)>1 else "",
                "Phone 3":r.phones[2] if len(r.phones)>2 else "",
                "Phone 3 Type":r.phone_types[2] if len(r.phone_types)>2 else "",
                "Email":r.emails[0] if r.emails else "",
                "Skip Trace Source":r.skip_trace_source,
                "Source":SOURCE_NAME,"Public Records URL":r.clerk_url,
            })
    logging.info("Wrote CSV: %s",csv_path)

def write_report(report,report_path):
    report_path.parent.mkdir(parents=True,exist_ok=True)
    report["generated_at"]=datetime.now(timezone.utc).isoformat()
    report_path.write_text(json.dumps(report,indent=2),encoding="utf-8")

def parse_args():
    p=argparse.ArgumentParser()
    p.add_argument("--records",default=str(DATA_DIR/"records.json"))
    p.add_argument("--out-json",dest="out_json",default=str(DEFAULT_ENRICHED_JSON_PATH))
    p.add_argument("--out-csv",dest="out_csv",default=str(DEFAULT_ENRICHED_CSV_PATH))
    p.add_argument("--report",dest="report",default=str(DEFAULT_REPORT_PATH))
    return p.parse_args()


# -----------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------
async def main():
    args=parse_args(); ensure_dirs(); log_setup()
    logging.info("=== Akron Summit County — Motivated Seller Intelligence ===")

    # 1. CAMA parcel data
    owner_index,last_name_index,first_last_index,parcel_rows,mail_by_pid=build_parcel_indexes()

    # 2. Court records (clerk)
    clerk_records=await scrape_clerk_records()

    # 3. All non-playwright scrapers
    vacant_addresses   = scrape_vacant_building_addresses()
    sheriff_records    = scrape_sheriff_sales()
    code_vio_records   = scrape_housing_appeals_board()
    delinquent_parcels = scrape_tax_delinquent_parcels()
    delinquent_pid_set = set(delinquent_parcels.keys())

    # 4. Enrich clerk records with CAMA
    all_records,report=enrich_with_parcel_data(clerk_records,owner_index,last_name_index,first_last_index)

    # 5. Build cross-reference maps
    addr_to_pid:Dict[str,str]={}
    for row in parcel_rows:
        pid=get_pid(row)
        if not pid: continue
        addr=build_prop_address_from_row(row)
        if addr: addr_to_pid[normalize_address_key(addr)]=pid

    foreclosure_pids:set=set()
    for r in all_records:
        if r.doc_type in {"LP","NOFC","TAXDEED"} and r.prop_address:
            pid=addr_to_pid.get(normalize_address_key(r.prop_address))
            if pid: foreclosure_pids.add(pid)

    # Also add sheriff sale addresses to foreclosure pids
    for r in sheriff_records:
        if r.prop_address:
            pid=addr_to_pid.get(normalize_address_key(r.prop_address))
            if pid: foreclosure_pids.add(pid)

    logging.info("Foreclosure+Sheriff pids: %s",len(foreclosure_pids))

    # 6. Vacant homes
    vacant_home_leads,vacant_home_keys=build_vacant_home_list(
        vacant_addresses,parcel_rows,mail_by_pid,delinquent_pid_set,foreclosure_pids
    )

    # 7. Probate — needs vacant_home_keys and delinquent_pid_set for cross-ref
    probate_records=scrape_probate_leads(parcel_rows,mail_by_pid,delinquent_pid_set,vacant_home_keys)

    # 8. Distress stacking
    delinquent_addresses=build_delinquent_address_index(parcel_rows,mail_by_pid,delinquent_parcels)
    distress_index=build_distress_index(all_records,vacant_home_keys)
    all_records=apply_distress_stacking(all_records,distress_index,delinquent_addresses,vacant_home_keys)

    # 9. Tax delinquent residential leads
    tax_delin_leads=build_tax_delinquent_leads(delinquent_parcels,parcel_rows,mail_by_pid,vacant_home_keys)

    # 10. Merge all lead types
    all_records=(all_records + tax_delin_leads + vacant_home_leads +
                 sheriff_records + code_vio_records + probate_records)
    logging.info("Total before dedupe: %s",len(all_records))

    # 11. Dedupe + sort (sheriff sales first, then hot stack, then score)
    all_records=dedupe_records(all_records)
    all_records.sort(
        key=lambda r:(r.doc_type=="SHERIFF",r.hot_stack,r.distress_count,r.subject_to_score,r.score,r.filed),
        reverse=True
    )

    # 12. Vacant land
    vacant_land=build_vacant_land_list(parcel_rows,mail_by_pid,delinquent_pid_set,foreclosure_pids)
    vacant_land.sort(key=lambda r:r.score,reverse=True)

    # 13. Write all outputs
    write_json_outputs(all_records,extra_json_path=Path(args.out_json))
    write_category_json(all_records)
    write_vacant_land_json(vacant_land)
    write_csv(all_records,DEFAULT_OUTPUT_CSV_PATH)
    if Path(args.out_csv)!=DEFAULT_OUTPUT_CSV_PATH: write_csv(all_records,Path(args.out_csv))
    write_report(report,Path(args.report))

    logging.info(
        "=== DONE === Total:%s | ⚡Sheriff:%s | 🔥HotStack:%s | ⚖️Probate:%s | 🏚CodeVio:%s | "
        "🏠VacantHome:%s | 💰TaxDelin:%s | 📭Absentee:%s | 🌎OOS:%s | "
        "🏛Inherited:%s | 🎯SubjectTo:%s | ⭐PrimeSubTo:%s | 🌿VacantLand:%s",
        len(all_records),
        sum(1 for r in all_records if r.doc_type=="SHERIFF"),
        sum(1 for r in all_records if r.hot_stack),
        sum(1 for r in all_records if r.doc_type=="PRO"),
        sum(1 for r in all_records if r.doc_type=="CODEVIOLATION"),
        sum(1 for r in all_records if r.is_vacant_home),
        sum(1 for r in all_records if "Tax delinquent" in r.flags),
        sum(1 for r in all_records if r.is_absentee),
        sum(1 for r in all_records if r.is_out_of_state),
        sum(1 for r in all_records if r.is_inherited),
        sum(1 for r in all_records if r.subject_to_score>=50),
        sum(1 for r in all_records if r.subject_to_score>=70),
        len(vacant_land),
    )

if __name__=="__main__":
    asyncio.run(main())
