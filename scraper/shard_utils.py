"""Sharding helpers for county records.json payloads.

WHY THIS EXISTS (root cause diagnosed 2026-08-20): scraper/merge_preserve.py
makes dashboard/records.json's record count monotonic on purpose (see its
own docstring) - it carries forward every record that has ever appeared so
the orchestrator's hard shrink guard never trips. That is correct behavior,
but it also means the file only ever grows. Summit's records.json crossed
GitHub's hard 100 MiB (104,857,600 byte) per-file push limit around
2026-07-28/29: every scheduled run since then scraped fine and even
"committed" locally, but the push to origin was silently rejected by
GitHub and tools/run_daily_refresh_all.py did not treat that as a
failure, so the Actions job stayed green while the dashboard quietly
froze on 2026-07-28 data for 23 days.

THE FIX: once a county's records.json would exceed a safe size, split its
records array into fixed-size shard files next to it and replace the
inline "records" list with a small manifest that points at them. Every
shard stays comfortably under the GitHub limit no matter how large the
dataset grows, and nothing is ever dropped - this only changes how the
data is packaged for git/HTTP, not what data exists.

A manifest looks like the original payload, minus the inline records:
    {
      ...same top-level metadata (fetched_at, source, total, ...)...
      "records": [],
      "sharded": true,
      "shard_dir": "records_shards",
      "shard_files": ["records_shards/records_0001.json", ...],
      "shard_count": N,
      "record_count": <true total, since "total" above is a scraper stat
                        that may mean something slightly different>
    }
Each shard file is `{"records": [...]}` - a plain slice of the original
array, in original order.

Any code that used to do `payload.get("records", [])` on a records.json
file should instead go through load_payload()/unshard_payload() here so
it transparently works whether or not that file happens to be sharded
right now. This module intentionally has no heavy imports so it stays
cheap to import from fetch.py, merge_preserve.py, and the daily
orchestrator alike.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

# ~20-25MB per shard at Summit's current ~112-field record width, which
# leaves large headroom under the 100 MiB GitHub hard limit even as the
# schema grows. Deliberately conservative rather than cutting it close.
DEFAULT_MAX_RECORDS_PER_SHARD = 6000


def is_sharded(payload: dict) -> bool:
    """True if `payload` is a shard manifest rather than a full payload."""
    return bool(isinstance(payload, dict) and payload.get("sharded") and isinstance(payload.get("shard_files"), list))


def shard_payload(payload: dict, out_path: Path, max_per_shard: int = DEFAULT_MAX_RECORDS_PER_SHARD) -> dict:
    """Split payload["records"] into shard files written next to
    `out_path` (e.g. dashboard/records_shards/records_0001.json for
    out_path=dashboard/records.json), and return the manifest payload
    that should be written to `out_path` itself. Does not write
    `out_path` - caller decides when/how to persist it (so this stays
    easy to unit test and easy to slot into the existing commit flow).
    """
    records = payload.get("records", []) if isinstance(payload, dict) else []
    shard_dir_name = out_path.stem + "_shards"
    shard_dir = out_path.parent / shard_dir_name
    shard_dir.mkdir(parents=True, exist_ok=True)

    # Clear out any shard files from a previous, larger run so the shard
    # count can shrink cleanly and nothing orphaned lingers on disk.
    prefix = out_path.stem + "_"
    for old in shard_dir.glob(f"{prefix}*.json"):
        old.unlink()

    shard_files = []
    if records:
        for start in range(0, len(records), max_per_shard):
            chunk = records[start:start + max_per_shard]
            idx = start // max_per_shard + 1
            shard_name = f"{prefix}{idx:04d}.json"
            shard_path = shard_dir / shard_name
            shard_path.write_text(
                json.dumps({"records": chunk}, ensure_ascii=False),
                encoding="utf-8",
            )
            shard_files.append(f"{shard_dir_name}/{shard_name}")

    manifest = dict(payload)
    manifest["records"] = []
    manifest["sharded"] = True
    manifest["shard_dir"] = shard_dir_name
    manifest["shard_files"] = shard_files
    manifest["shard_count"] = len(shard_files)
    manifest["record_count"] = len(records)
    return manifest


def unshard_payload(payload: dict, base_dir: Path) -> dict:
    """If `payload` is a shard manifest, read every shard file under
    `base_dir` and return an equivalent payload with a full inline
    "records" list, matching the shape all existing callers expect. If
    `payload` is already a plain (non-sharded) payload, return it
    unchanged - this makes every call site backward compatible with
    records.json files written before sharding existed.
    """
    if not is_sharded(payload):
        return payload
    records = []
    for rel in payload.get("shard_files", []):
        shard_path = base_dir / rel
        try:
            shard = json.loads(shard_path.read_text(encoding="utf-8"))
            recs = shard.get("records", []) if isinstance(shard, dict) else []
            if isinstance(recs, list):
                records.extend(recs)
        except Exception:
            # A missing/corrupt shard should degrade, not crash the run;
            # callers already tolerate a missing records.json entirely.
            continue
    full = dict(payload)
    full["records"] = records
    return full


def load_payload(path: Path) -> Optional[dict]:
    """Read a records.json-shaped file (sharded or not) from disk and
    always return a payload with the full inline "records" list
    populated. Returns None if the file is missing or unreadable.
    """
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return unshard_payload(payload, Path(path).parent)


def count_records(path: Path) -> Optional[int]:
    """Record count for a records.json-shaped file, sharded or not,
    without paying the cost of loading every shard when a manifest
    already carries the true count."""
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if is_sharded(payload):
        return int(payload.get("record_count") or 0)
    records = payload.get("records", [])
    return len(records) if isinstance(records, list) else None
