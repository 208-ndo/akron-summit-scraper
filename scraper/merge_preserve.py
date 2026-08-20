#!/usr/bin/env python
"""Merge-preserve layer for county records.json outputs.

WHY THIS EXISTS (root cause of the Summit freeze, diagnosed 2026-07-01):
scraper/fetch.py rebuilds the whole dataset from live sources on every
run. When a source returns fewer rows than before (a page fails, a
paginator breaks, or leads simply resolve off the live source), the new
file is SMALLER than the old one. The orchestrator's hard shrink guard
(tools/run_daily_refresh_all.py) then correctly refuses to ship the
smaller dataset - but because fetch.py never carried forward the
missing records, the guard fired on EVERY run, and the Summit dashboard
froze at 2026-06-23 (blocked: 3783 -> 3483, reverted daily).

THE FIX: instead of dropping records that vanished from the live pull,
carry them forward from the previous committed dashboard/records.json.
- Fresh records get last_seen_date = today.
- Previous records not present in the fresh pull are kept unchanged
  (score, flags, first_seen_date untouched - no scoring change) and
  tagged carried_forward=true with their last last_seen_date preserved.
- If the fresh pull is EMPTY and the previous file had data, the
  previous payload is returned untouched (never overwrite good data
  with an empty export caused by source failure).
- first_seen_date is never rewritten, so "Today's Leads" stays honest.
- Nothing is ever fabricated: every merged record existed either in the
  fresh pull or in the previously committed file.

This makes the record count monotonic, so the shrink guard passes and
fresh leads flow to the dashboard again, while history is preserved.

Match identity mirrors fetch.py's first_seen_match_keys(): doc_num,
parcel_id, owner|address, address|city|state - implemented here at the
plain-dict level so this module has no heavy imports and is unit-testable.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Fields that legitimately change run-to-run and should not count as a
# "real" data change when deciding last_updated_date.
_VOLATILE_FIELDS = {
    "last_seen_date", "last_updated_date", "pulled_date", "carried_forward",
    "dataset_fetched_at", "auction_countdown", "auction_days_until",
    "enrichment_timestamp",
}

_WS_RE = re.compile(r"\s+")


def _clean(v) -> str:
    return _WS_RE.sub(" ", str(v or "").strip())


def _norm(v) -> str:
    return _clean(v).upper()


def record_match_keys(rec: dict) -> List[str]:
    """Dict-level mirror of fetch.py first_seen_match_keys()."""
    keys: List[str] = []

    def add(value: str) -> None:
        value = _norm(value)
        if value and value not in keys:
            keys.append(value)

    add(rec.get("doc_num", ""))
    add(rec.get("parcel_id", ""))
    owner = _norm(rec.get("owner", ""))
    prop = _norm(rec.get("prop_address", ""))
    if owner and prop:
        add(f"{owner}|{prop}")
    if prop and rec.get("prop_city"):
        add(f"{prop}|{_norm(rec.get('prop_city'))}|{_norm(rec.get('prop_state'))}")
    return keys


_PCF_RE = re.compile(r"^PCF\d+-")


def dedupe_identity(rec: dict) -> tuple:
    """EXACT mirror of fetch.py dedupe_records() identity. Two records
    with different dedupe identities are distinct leads in the pipeline,
    so the carry-forward decision must use this same key - anything
    looser silently collapses records the pipeline keeps separate."""
    nd = _PCF_RE.sub("", _norm(rec.get("doc_num", "")))
    return (nd, _norm(rec.get("doc_type", "")), _norm(rec.get("owner", "")),
            _clean(rec.get("filed", "")))


def _comparable(rec: dict) -> str:
    slim = {k: v for k, v in rec.items() if k not in _VOLATILE_FIELDS}
    return json.dumps(slim, sort_keys=True, default=str)


def merge_payload_preserving_previous(new_payload: dict,
                                       previous_payload: Optional[dict],
                                       today: Optional[str] = None) -> dict:
    """Return new_payload with previous-only records carried forward.

    Never fabricates records. Never shrinks the dataset. Never touches
    first_seen_date, score, flags, or distress data on carried records.
    """
    today = today or datetime.now().date().isoformat()
    new_records = list(new_payload.get("records", []) or [])
    prev_records = list((previous_payload or {}).get("records", []) or [])

    # Safety valve: empty fresh pull + non-empty history = source failure.
    # Return the previous payload untouched rather than shipping a wipe.
    if not new_records and prev_records:
        logging.warning(
            "merge_preserve: fresh pull returned 0 records but previous file "
            "has %s - keeping previous payload untouched (source failure guard).",
            len(prev_records),
        )
        return previous_payload

    prev_fallback_seen = _clean((previous_payload or {}).get("fetched_at", ""))[:10]

    # Index previous records by match key for change detection.
    prev_by_key: Dict[str, dict] = {}
    for rec in prev_records:
        if not isinstance(rec, dict):
            continue
        for key in record_match_keys(rec):
            prev_by_key.setdefault(key, rec)

    # Stamp fresh records.
    seen_keys = set()
    seen_identities = set()
    for rec in new_records:
        if not isinstance(rec, dict):
            continue
        keys = record_match_keys(rec)
        seen_keys.update(keys)
        seen_identities.add(dedupe_identity(rec))
        rec["last_seen_date"] = today
        rec.pop("carried_forward", None)
        prev_match = next((prev_by_key[k] for k in keys if k in prev_by_key), None)
        if prev_match is None:
            rec["last_updated_date"] = today
        elif _comparable(rec) != _comparable(prev_match):
            rec["last_updated_date"] = today
        else:
            rec["last_updated_date"] = (
                prev_match.get("last_updated_date")
                or prev_match.get("last_seen_date")
                or prev_fallback_seen or today
            )

    # Carry forward previous records that vanished from the fresh pull.
    # Presence is judged by the pipeline's own strict dedupe identity so
    # distinct leads sharing a parcel/owner never collapse into one.
    carried: List[dict] = []
    carried_identity_dedupe = set()
    for rec in prev_records:
        if not isinstance(rec, dict):
            continue
        identity = dedupe_identity(rec)
        if not any(identity):
            # No usable identity at all - fall back to full-content
            # uniqueness so multiple blank-key records don't collapse.
            identity = ("__content__", _comparable(rec))
        if identity in seen_identities:
            continue
        if identity in carried_identity_dedupe:
            continue
        carried_identity_dedupe.add(identity)
        kept = dict(rec)  # never mutate history in place
        kept.setdefault("last_seen_date",
                        kept.get("pulled_date") or prev_fallback_seen or "")
        kept["carried_forward"] = True
        carried.append(kept)

    merged = new_records + carried
    out = dict(new_payload)
    out["records"] = merged
    out["total"] = len(merged)
    out["carried_forward_count"] = len(carried)
    if carried:
        logging.info(
            "merge_preserve: carried forward %s previous records missing from "
            "the fresh pull (%s fresh + %s carried = %s total).",
            len(carried), len(new_records), len(carried), len(merged),
        )
    return out


def _load_shard_utils():
    """Load scraper/shard_utils.py by file path (not by package import),
    matching how fetch.py loads this very module - keeps this working
    whether merge_preserve.py is imported normally, dynamically loaded
    via spec_from_file_location (as fetch.py and the test suite both do),
    or run from a different working directory."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "shard_utils", Path(__file__).resolve().parent / "shard_utils.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def merge_with_previous_file(new_payload: dict, previous_path: Path,
                              today: Optional[str] = None) -> dict:
    previous_payload = None
    try:
        if Path(previous_path).is_file():
            previous_payload = json.loads(Path(previous_path).read_text(encoding="utf-8"))
    except Exception as e:  # corrupt previous file must not kill the run
        logging.warning("merge_preserve: could not read %s: %s", previous_path, e)
    if not isinstance(previous_payload, dict):
        previous_payload = None
    elif previous_payload:
        # 2026-08-20 fix: once a records.json gets big enough to exceed
        # GitHub's 100MB push limit, tools/shard_dashboard_records.py
        # (via scraper/shard_utils.py) replaces the inline "records" list
        # with a small shard-file manifest. Expand it back to a normal
        # in-memory payload here so carry-forward keeps working exactly
        # as before - this function otherwise has no idea sharding
        # exists. No-op for a plain, unsharded previous file.
        try:
            previous_payload = _load_shard_utils().unshard_payload(
                previous_payload, Path(previous_path).parent)
        except Exception as e:
            logging.warning("merge_preserve: could not expand sharded %s: %s", previous_path, e)
    return merge_payload_preserving_previous(new_payload, previous_payload, today=today)
