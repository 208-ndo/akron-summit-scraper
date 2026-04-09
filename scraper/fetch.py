#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch.py v10 - parcel matcher / enricher

Purpose
-------
Takes already-extracted lead records (records.json) and enriches them with:
- parcel / APN
- situs / property address
- mailing address
- owner fields
- better state cleanup
- match diagnostics

Designed for the current state you described:
- CAMA / parcel files working
- owner index working
- clerk extraction working
- names extracted correctly
- parcel/address matching still 0

Inputs
------
1) records.json
2) parcel/CAMA source (CSV or JSON)
3) owner index source (JSON or CSV)

Outputs
-------
- records.enriched.json
- records.enriched.csv
- match_report.json

Run
---
python fetch.py \
  --records records.json \
  --parcels parcels.csv \
  --owner-index owner_index.json \
  --out-json records.enriched.json \
  --out-csv records.enriched.csv

Notes
-----
- Uses only standard library.
- Safe to run repeatedly.
- Built to maximize match rate from owner names + address fallbacks.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import unicodedata
from collections import defaultdict, Counter
from copy import deepcopy
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

DEBUG = True

# Common entity suffixes to reduce noise in owner matching
OWNER_STOPWORDS = {
    "jr", "sr", "ii", "iii", "iv",
    "estate", "et", "al", "trust", "tr", "revocable", "living",
    "llc", "inc", "corp", "co", "company", "lp", "llp", "ltd",
    "bank", "association", "fka", "aka", "na"
}

# Junk values that should never survive as valid state codes
BAD_STATE_VALUES = {
    "", "0", "00", "000", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "na", "n/a", "none", "null", "unknown", "-"
}

# Street suffix normalization
STREET_ABBREV = {
    "street": "st", "st": "st",
    "avenue": "ave", "ave": "ave",
    "road": "rd", "rd": "rd",
    "drive": "dr", "dr": "dr",
    "lane": "ln", "ln": "ln",
    "court": "ct", "ct": "ct",
    "circle": "cir", "cir": "cir",
    "boulevard": "blvd", "blvd": "blvd",
    "place": "pl", "pl": "pl",
    "terrace": "ter", "ter": "ter",
    "parkway": "pkwy", "pkwy": "pkwy",
    "highway": "hwy", "hwy": "hwy",
    "way": "way",
    "trail": "trl", "trl": "trl",
    "center": "ctr", "ctr": "ctr",
}

DIRECTIONALS = {
    "north": "n", "south": "s", "east": "e", "west": "w",
    "northeast": "ne", "northwest": "nw", "southeast": "se", "southwest": "sw",
    "n": "n", "s": "s", "e": "e", "w": "w", "ne": "ne", "nw": "nw", "se": "se", "sw": "sw",
}

STATE_CODES = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la",
    "me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok",
    "or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc"
}

# Candidate keys to discover field mappings across parcel / owner files
PARCEL_CANDIDATES = ["parcel", "parcel_id", "parcelid", "apn", "pin", "tax_id", "ppn"]
OWNER_NAME_CANDIDATES = ["owner_name", "owner", "name", "owner1", "party_name", "taxpayer_name"]
SITE_ADDR_CANDIDATES = [
    "situs_address", "site_address", "property_address", "prop_address", "address", "location_address"
]
SITE_CITY_CANDIDATES = ["situs_city", "site_city", "property_city", "city"]
SITE_STATE_CANDIDATES = ["situs_state", "site_state", "property_state", "state"]
SITE_ZIP_CANDIDATES = ["situs_zip", "site_zip", "property_zip", "zip", "zipcode"]
MAIL_ADDR_CANDIDATES = ["mailing_address", "mail_address", "owner_address", "taxpayer_address"]
MAIL_CITY_CANDIDATES = ["mailing_city", "mail_city", "owner_city"]
MAIL_STATE_CANDIDATES = ["mailing_state", "mail_state", "owner_state"]
MAIL_ZIP_CANDIDATES = ["mailing_zip", "mail_zip", "owner_zip"]
CASE_NAME_CANDIDATES = ["defendant_name", "name", "owner_name", "party_name"]

# ------------------------------------------------------------
# UTIL
# ------------------------------------------------------------

def debug(*args: Any) -> None:
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr)

def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def dump_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def load_csv(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def dump_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    fieldnames = []
    seen = set()
    for row in rows:
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def load_any_table(path: str) -> List[Dict[str, Any]]:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".json":
        data = load_json(path)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("rows", "data", "records", "items"):
                if isinstance(data.get(key), list):
                    return data[key]
        raise ValueError(f"JSON not recognized as tabular list: {path}")
    if ext == ".csv":
        return load_csv(path)
    raise ValueError(f"Unsupported file type: {path}")

def first_present(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    lower_map = {str(k).strip().lower(): k for k in d.keys()}
    for k in keys:
        if k.lower() in lower_map:
            real_key = lower_map[k.lower()]
            val = d.get(real_key)
            if val is not None and str(val).strip() != "":
                return str(val).strip()
    return None

def first_key(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    lower_map = {str(k).strip().lower(): k for k in d.keys()}
    for k in keys:
        if k.lower() in lower_map:
            return lower_map[k.lower()]
    return None

def clean_text(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def normalize_name(name: Any) -> str:
    s = clean_text(name).lower()
    if not s:
        return ""

    # Remove punctuation but preserve spaces
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    parts = [p for p in s.split() if p]

    # remove stopwords
    filtered = [p for p in parts if p not in OWNER_STOPWORDS]
    return " ".join(filtered).strip()

def name_tokens(name: Any) -> Tuple[str, ...]:
    n = normalize_name(name)
    if not n:
        return tuple()
    parts = [p for p in n.split() if len(p) > 1]
    return tuple(parts)

def canonical_name_key(name: Any) -> str:
    toks = sorted(name_tokens(name))
    return " ".join(toks).strip()

def normalize_parcel(parcel: Any) -> str:
    s = clean_text(parcel).upper()
    if not s:
        return ""
    return re.sub(r"[^A-Z0-9]", "", s)

def normalize_zip(z: Any) -> str:
    s = clean_text(z)
    m = re.search(r"(\d{5})", s)
    return m.group(1) if m else ""

def normalize_state(st: Any) -> str:
    s = clean_text(st).lower()
    if s in BAD_STATE_VALUES:
        return ""
    s = re.sub(r"[^a-z]", "", s)
    if len(s) == 2 and s in STATE_CODES:
        return s.upper()

    state_name_to_code = {
        "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA","colorado":"CO",
        "connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA","hawaii":"HI","idaho":"ID",
        "illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS","kentucky":"KY","louisiana":"LA",
        "maine":"ME","maryland":"MD","massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
        "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV","newhampshire":"NH","newjersey":"NJ",
        "newmexico":"NM","newyork":"NY","northcarolina":"NC","northdakota":"ND","ohio":"OH","oklahoma":"OK",
        "oregon":"OR","pennsylvania":"PA","rhodeisland":"RI","southcarolina":"SC","southdakota":"SD",
        "tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT","virginia":"VA","washington":"WA",
        "westvirginia":"WV","wisconsin":"WI","wyoming":"WY","districtofcolumbia":"DC"
    }
    return state_name_to_code.get(s, "")

def normalize_house_num(addr: str) -> str:
    m = re.match(r"^\s*(\d+[A-Z]?)\b", addr or "", flags=re.I)
    return m.group(1).upper() if m else ""

def normalize_street(addr: Any) -> str:
    s = clean_text(addr).lower()
    if not s:
        return ""

    s = s.replace("#", " ")
    s = re.sub(r"\bapt\b|\bunit\b|\bste\b|\bsuite\b|\bfl\b|\bfloor\b", " ", s)
    s = re.sub(r"\bpo box\b", " pobox ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    parts = [p for p in s.split() if p]

    out = []
    for p in parts:
        if p in DIRECTIONALS:
            out.append(DIRECTIONALS[p])
        elif p in STREET_ABBREV:
            out.append(STREET_ABBREV[p])
        else:
            out.append(p)

    s = " ".join(out).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_address(addr: Any, city: Any = "", state: Any = "", zip_code: Any = "") -> str:
    street = normalize_street(addr)
    city = clean_text(city).lower()
    city = re.sub(r"[^a-z0-9\s]", " ", city).strip()
    st = normalize_state(state)
    zp = normalize_zip(zip_code)

    parts = [p for p in [street, city, st.lower() if st else "", zp] if p]
    return " | ".join(parts)

def address_without_zip(addr: Any, city: Any = "", state: Any = "") -> str:
    street = normalize_street(addr)
    city = clean_text(city).lower()
    city = re.sub(r"[^a-z0-9\s]", " ", city).strip()
    st = normalize_state(state)
    parts = [p for p in [street, city, st.lower() if st else ""] if p]
    return " | ".join(parts)

def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

def compact_join(*parts: Any) -> str:
    vals = [clean_text(p) for p in parts if clean_text(p)]
    return ", ".join(vals)

# ------------------------------------------------------------
# FIELD EXTRACTION
# ------------------------------------------------------------

def discover_fields(sample_row: Dict[str, Any], label: str) -> Dict[str, Optional[str]]:
    fields = {
        "parcel": first_key(sample_row, PARCEL_CANDIDATES),
        "owner_name": first_key(sample_row, OWNER_NAME_CANDIDATES),
        "site_addr": first_key(sample_row, SITE_ADDR_CANDIDATES),
        "site_city": first_key(sample_row, SITE_CITY_CANDIDATES),
        "site_state": first_key(sample_row, SITE_STATE_CANDIDATES),
        "site_zip": first_key(sample_row, SITE_ZIP_CANDIDATES),
        "mail_addr": first_key(sample_row, MAIL_ADDR_CANDIDATES),
        "mail_city": first_key(sample_row, MAIL_CITY_CANDIDATES),
        "mail_state": first_key(sample_row, MAIL_STATE_CANDIDATES),
        "mail_zip": first_key(sample_row, MAIL_ZIP_CANDIDATES),
    }
    debug(f"{label} discovered fields:", fields)
    return fields

def getv(row: Dict[str, Any], k: Optional[str]) -> str:
    if not k:
        return ""
    v = row.get(k)
    return clean_text(v)

# ------------------------------------------------------------
# INDEX BUILDERS
# ------------------------------------------------------------

def build_parcel_master(
    parcels_rows: List[Dict[str, Any]],
    owner_rows: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Create one unified parcel master with flexible field discovery.
    Owner rows can add/override owner and mailing fields onto parcel rows.
    """
    if not parcels_rows:
        raise ValueError("No parcel/CAMA rows loaded.")
    if not owner_rows:
        debug("Owner rows empty; continuing with parcel rows only.")

    p_fields = discover_fields(parcels_rows[0], "parcel")
    o_fields = discover_fields(owner_rows[0], "owner_index") if owner_rows else {
        "parcel": None, "owner_name": None, "site_addr": None, "site_city": None,
        "site_state": None, "site_zip": None, "mail_addr": None, "mail_city": None,
        "mail_state": None, "mail_zip": None
    }

    by_parcel_owner = {}
    for row in owner_rows:
        parcel = normalize_parcel(getv(row, o_fields["parcel"]))
        if parcel:
            by_parcel_owner[parcel] = row

    master = []

    for row in parcels_rows:
        parcel = normalize_parcel(getv(row, p_fields["parcel"]))
        o_row = by_parcel_owner.get(parcel)

        owner_name = getv(o_row, o_fields["owner_name"]) if o_row else ""
        if not owner_name:
            owner_name = getv(row, p_fields["owner_name"])

        site_addr = getv(row, p_fields["site_addr"])
        site_city = getv(row, p_fields["site_city"])
        site_state = getv(row, p_fields["site_state"])
        site_zip = getv(row, p_fields["site_zip"])

        mail_addr = ""
        mail_city = ""
        mail_state = ""
        mail_zip = ""

        if o_row:
            mail_addr = getv(o_row, o_fields["mail_addr"])
            mail_city = getv(o_row, o_fields["mail_city"])
            mail_state = getv(o_row, o_fields["mail_state"])
            mail_zip = getv(o_row, o_fields["mail_zip"])

        if not mail_addr:
            mail_addr = getv(row, p_fields["mail_addr"])
        if not mail_city:
            mail_city = getv(row, p_fields["mail_city"])
        if not mail_state:
            mail_state = getv(row, p_fields["mail_state"])
        if not mail_zip:
            mail_zip = getv(row, p_fields["mail_zip"])

        rec = {
            "parcel": parcel,
            "owner_name": clean_text(owner_name),
            "owner_name_norm": normalize_name(owner_name),
            "owner_name_key": canonical_name_key(owner_name),

            "site_addr": clean_text(site_addr),
            "site_city": clean_text(site_city),
            "site_state": normalize_state(site_state),
            "site_zip": normalize_zip(site_zip),

            "mail_addr": clean_text(mail_addr),
            "mail_city": clean_text(mail_city),
            "mail_state": normalize_state(mail_state),
            "mail_zip": normalize_zip(mail_zip),

            "site_addr_norm": normalize_address(site_addr, site_city, site_state, site_zip),
            "site_addr_nz": address_without_zip(site_addr, site_city, site_state),

            "mail_addr_norm": normalize_address(mail_addr, mail_city, mail_state, mail_zip),
            "mail_addr_nz": address_without_zip(mail_addr, mail_city, mail_state),

            "site_house_num": normalize_house_num(clean_text(site_addr)),
            "mail_house_num": normalize_house_num(clean_text(mail_addr)),

            "_parcel_source_row": row,
            "_owner_source_row": o_row or {},
        }

        master.append(rec)

    debug(f"Built parcel master rows: {len(master)}")
    return master, {"parcel_fields": p_fields, "owner_fields": o_fields}

def build_indexes(master: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_parcel = {}
    by_owner_norm = defaultdict(list)
    by_owner_key = defaultdict(list)
    by_site_addr = defaultdict(list)
    by_site_addr_nz = defaultdict(list)
    by_mail_addr = defaultdict(list)
    by_mail_addr_nz = defaultdict(list)
    by_house_num = defaultdict(list)

    owner_name_keys = 0

    for row in master:
        parcel = row["parcel"]
        if parcel:
            by_parcel[parcel] = row

        if row["owner_name_norm"]:
            by_owner_norm[row["owner_name_norm"]].append(row)
            owner_name_keys += 1

        if row["owner_name_key"]:
            by_owner_key[row["owner_name_key"]].append(row)

        if row["site_addr_norm"]:
            by_site_addr[row["site_addr_norm"]].append(row)

        if row["site_addr_nz"]:
            by_site_addr_nz[row["site_addr_nz"]].append(row)

        if row["mail_addr_norm"]:
            by_mail_addr[row["mail_addr_norm"]].append(row)

        if row["mail_addr_nz"]:
            by_mail_addr_nz[row["mail_addr_nz"]].append(row)

        if row["site_house_num"]:
            by_house_num[row["site_house_num"]].append(row)

    debug(f"Owner-name keys indexed: {owner_name_keys}")
    return {
        "by_parcel": by_parcel,
        "by_owner_norm": by_owner_norm,
        "by_owner_key": by_owner_key,
        "by_site_addr": by_site_addr,
        "by_site_addr_nz": by_site_addr_nz,
        "by_mail_addr": by_mail_addr,
        "by_mail_addr_nz": by_mail_addr_nz,
        "by_house_num": by_house_num,
    }

# ------------------------------------------------------------
# RECORD HELPERS
# ------------------------------------------------------------

def extract_best_name(rec: Dict[str, Any]) -> str:
    for key in [
        "defendant_name", "name", "owner_name", "party_name",
        "defendant", "case_name", "full_name"
    ]:
        val = rec.get(key)
        if val and clean_text(val):
            return clean_text(val)

    defendants = rec.get("defendants")
    if isinstance(defendants, list):
        for d in defendants:
            if isinstance(d, dict):
                nm = d.get("name")
                if nm and clean_text(nm):
                    return clean_text(nm)
            elif isinstance(d, str) and clean_text(d):
                return clean_text(d)

    return ""

def extract_candidate_addresses(rec: Dict[str, Any]) -> List[Tuple[str, str, str, str]]:
    """
    Returns a list of candidate tuples: (addr, city, state, zip)
    """
    candidates = []

    direct_sets = [
        ("property_address", "property_city", "property_state", "property_zip"),
        ("site_address", "site_city", "site_state", "site_zip"),
        ("address", "city", "state", "zip"),
        ("mailing_address", "mailing_city", "mailing_state", "mailing_zip"),
        ("mail_address", "mail_city", "mail_state", "mail_zip"),
        ("defendant_address", "defendant_city", "defendant_state", "defendant_zip"),
    ]

    for a, c, s, z in direct_sets:
        addr = clean_text(rec.get(a))
        city = clean_text(rec.get(c))
        state = clean_text(rec.get(s))
        zp = clean_text(rec.get(z))
        if addr:
            candidates.append((addr, city, state, zp))

    # nested address structures
    for key in ["addresses", "mailing_addresses", "property_addresses"]:
        vals = rec.get(key)
        if isinstance(vals, list):
            for item in vals:
                if isinstance(item, dict):
                    addr = clean_text(item.get("address") or item.get("street"))
                    city = clean_text(item.get("city"))
                    state = clean_text(item.get("state"))
                    zp = clean_text(item.get("zip") or item.get("zipcode"))
                    if addr:
                        candidates.append((addr, city, state, zp))

    # de-dupe
    out = []
    seen = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)

    return out

def clean_record_states(rec: Dict[str, Any]) -> None:
    for k in list(rec.keys()):
        if "state" in k.lower():
            rec[k] = normalize_state(rec.get(k))

def apply_match(rec: Dict[str, Any], match_row: Dict[str, Any], method: str, score: float) -> Dict[str, Any]:
    out = deepcopy(rec)

    out["parcel"] = match_row.get("parcel", "") or out.get("parcel", "")
    out["property_address"] = match_row.get("site_addr", "") or out.get("property_address", "")
    out["property_city"] = match_row.get("site_city", "") or out.get("property_city", "")
    out["property_state"] = match_row.get("site_state", "") or out.get("property_state", "")
    out["property_zip"] = match_row.get("site_zip", "") or out.get("property_zip", "")

    out["mailing_address"] = match_row.get("mail_addr", "") or out.get("mailing_address", "")
    out["mailing_city"] = match_row.get("mail_city", "") or out.get("mailing_city", "")
    out["mailing_state"] = match_row.get("mail_state", "") or out.get("mailing_state", "")
    out["mailing_zip"] = match_row.get("mail_zip", "") or out.get("mailing_zip", "")

    out["matched_owner_name"] = match_row.get("owner_name", "")
    out["match_method"] = method
    out["match_score"] = round(score, 4)

    out["with_address"] = 1 if (out.get("property_address") or out.get("mailing_address")) else 0

    clean_record_states(out)
    return out

# ------------------------------------------------------------
# MATCH LOGIC
# ------------------------------------------------------------

def choose_best(rows: List[Dict[str, Any]], rec_name: str, rec_addrs: List[Tuple[str, str, str, str]]) -> Optional[Tuple[Dict[str, Any], float]]:
    """
    Rank multiple candidate parcel rows.
    """
    if not rows:
        return None

    best = None
    best_score = -1.0

    rec_name_norm = normalize_name(rec_name)
    rec_name_key = canonical_name_key(rec_name)

    rec_addr_norms = [normalize_address(a, c, s, z) for a, c, s, z in rec_addrs if a]
    rec_addr_nz = [address_without_zip(a, c, s) for a, c, s, z in rec_addrs if a]

    for row in rows:
        score = 0.0

        # name similarity
        owner_norm = row.get("owner_name_norm", "")
        owner_key = row.get("owner_name_key", "")

        if rec_name_norm and owner_norm:
            score += similarity(rec_name_norm, owner_norm) * 0.65

        if rec_name_key and owner_key and rec_name_key == owner_key:
            score += 0.25

        # address similarity boosts
        for a in rec_addr_norms:
            if a and a == row.get("site_addr_norm"):
                score += 0.60
            if a and a == row.get("mail_addr_norm"):
                score += 0.55

        for a in rec_addr_nz:
            if a and a == row.get("site_addr_nz"):
                score += 0.42
            if a and a == row.get("mail_addr_nz"):
                score += 0.38

        # house number hint
        for a, _, _, _ in rec_addrs:
            hn = normalize_house_num(a)
            if hn and (hn == row.get("site_house_num") or hn == row.get("mail_house_num")):
                score += 0.08

        if score > best_score:
            best_score = score
            best = row

    if best is None:
        return None
    return best, best_score

def match_record(rec: Dict[str, Any], indexes: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str, float]:
    """
    Matching strategy order:
    1) direct parcel/APN
    2) exact property/site address
    3) exact mailing address
    4) exact owner-name normalized
    5) canonical token-set owner match
    6) fuzzy owner match among same house number candidates
    7) fuzzy owner match global (guarded)
    """
    clean_record_states(rec)

    # 1) direct parcel
    parcel = normalize_parcel(rec.get("parcel") or rec.get("apn") or rec.get("parcel_id") or rec.get("pin"))
    if parcel and parcel in indexes["by_parcel"]:
        return indexes["by_parcel"][parcel], "direct_parcel", 1.00

    rec_name = extract_best_name(rec)
    rec_name_norm = normalize_name(rec_name)
    rec_name_key = canonical_name_key(rec_name)
    rec_addrs = extract_candidate_addresses(rec)

    # 2) exact property/site address
    for addr, city, state, zp in rec_addrs:
        a = normalize_address(addr, city, state, zp)
        if a and a in indexes["by_site_addr"]:
            chosen = choose_best(indexes["by_site_addr"][a], rec_name, rec_addrs)
            if chosen:
                row, score = chosen
                return row, "exact_site_address", min(1.0, max(score, 0.95))

    # 3) exact mailing address
    for addr, city, state, zp in rec_addrs:
        a = normalize_address(addr, city, state, zp)
        if a and a in indexes["by_mail_addr"]:
            chosen = choose_best(indexes["by_mail_addr"][a], rec_name, rec_addrs)
            if chosen:
                row, score = chosen
                return row, "exact_mail_address", min(1.0, max(score, 0.93))

    # 4) exact owner-name normalized
    if rec_name_norm and rec_name_norm in indexes["by_owner_norm"]:
        chosen = choose_best(indexes["by_owner_norm"][rec_name_norm], rec_name, rec_addrs)
        if chosen:
            row, score = chosen
            if score >= 0.45:
                return row, "owner_norm_exact", min(1.0, max(score, 0.90))

    # 5) canonical token-set owner match
    if rec_name_key and rec_name_key in indexes["by_owner_key"]:
        chosen = choose_best(indexes["by_owner_key"][rec_name_key], rec_name, rec_addrs)
        if chosen:
            row, score = chosen
            if score >= 0.42:
                return row, "owner_token_key", min(1.0, max(score, 0.88))

    # 6) fuzzy owner match among same house number candidates
    house_candidates = []
    for addr, _, _, _ in rec_addrs:
        hn = normalize_house_num(addr)
        if hn:
            house_candidates.extend(indexes["by_house_num"].get(hn, []))
    if house_candidates:
        chosen = choose_best(house_candidates, rec_name, rec_addrs)
        if chosen:
            row, score = chosen
            if score >= 0.62:
                return row, "house_num_plus_name_fuzzy", score

    # 7) guarded global fuzzy owner match
    if rec_name_norm:
        maybe = []
        by_owner_norm = indexes["by_owner_norm"]
        for owner_norm, rows in by_owner_norm.items():
            sim = similarity(rec_name_norm, owner_norm)
            if sim >= 0.88:
                maybe.extend(rows)

        if maybe:
            chosen = choose_best(maybe, rec_name, rec_addrs)
            if chosen:
                row, score = chosen
                if score >= 0.72:
                    return row, "global_owner_fuzzy", score

    return None, "unmatched", 0.0

# ------------------------------------------------------------
# MAIN ENRICHMENT
# ------------------------------------------------------------

def enrich_records(records: List[Dict[str, Any]], indexes: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    enriched = []
    method_counter = Counter()
    with_address = 0
    with_parcel = 0
    bad_state_rows = 0

    for rec in records:
        rec = deepcopy(rec)
        clean_record_states(rec)

        match_row, method, score = match_record(rec, indexes)
        if match_row:
            out = apply_match(rec, match_row, method, score)
        else:
            out = deepcopy(rec)
            out["match_method"] = "unmatched"
            out["match_score"] = 0.0
            out["with_address"] = 1 if (out.get("property_address") or out.get("mailing_address")) else 0
            clean_record_states(out)

        if out.get("parcel"):
            with_parcel += 1
        if out.get("with_address") == 1:
            with_address += 1

        # sanity flag if garbage states still slip through
        bad_state = False
        for k in ["property_state", "mailing_state", "state", "mail_state"]:
            v = clean_text(out.get(k))
            if v and v.lower() in BAD_STATE_VALUES:
                bad_state = True
        if bad_state:
            bad_state_rows += 1

        method_counter[out.get("match_method", "unknown")] += 1
        enriched.append(out)

    report = {
        "input_records": len(records),
        "output_records": len(enriched),
        "with_parcel": with_parcel,
        "with_address": with_address,
        "unmatched": method_counter.get("unmatched", 0),
        "bad_state_rows_remaining": bad_state_rows,
        "match_methods": dict(method_counter),
    }
    return enriched, report

# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="fetch.py v10 parcel matcher")
    ap.add_argument("--records", required=True, help="Path to records.json")
    ap.add_argument("--parcels", required=True, help="Path to CAMA/parcel CSV or JSON")
    ap.add_argument("--owner-index", required=True, help="Path to owner index CSV or JSON")
    ap.add_argument("--out-json", default="records.enriched.json", help="Output enriched JSON path")
    ap.add_argument("--out-csv", default="records.enriched.csv", help="Output enriched CSV path")
    ap.add_argument("--report", default="match_report.json", help="Output match report JSON path")
    return ap.parse_args()

def main() -> None:
    args = parse_args()

    debug("Loading records:", args.records)
    records_obj = load_json(args.records)
    if isinstance(records_obj, dict):
        if isinstance(records_obj.get("records"), list):
            records = records_obj["records"]
        elif isinstance(records_obj.get("data"), list):
            records = records_obj["data"]
        else:
            raise ValueError("records.json must be a list or contain records/data list.")
    elif isinstance(records_obj, list):
        records = records_obj
    else:
        raise ValueError("records.json format not recognized.")

    debug("Loading parcels:", args.parcels)
    parcels_rows = load_any_table(args.parcels)

    debug("Loading owner index:", args.owner_index)
    owner_rows = load_any_table(args.owner_index)

    master, meta = build_parcel_master(parcels_rows, owner_rows)
    indexes = build_indexes(master)

    enriched, report = enrich_records(records, indexes)
    report["meta"] = meta
    report["parcel_master_rows"] = len(master)

    debug("Writing:", args.out_json)
    dump_json(args.out_json, enriched)

    debug("Writing:", args.out_csv)
    dump_csv(args.out_csv, enriched)

    debug("Writing:", args.report)
    dump_json(args.report, report)

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
