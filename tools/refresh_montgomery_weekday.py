#!/usr/bin/env python
"""Weekday wrapper for the Montgomery county-data refresh.

Pipeline (each step's real output feeds the next):
  1. tools/refresh_counties.py - regenerates dashboard/montgomery/records.json
     from the operator's real local CSV + auditor JSONL (Summit/Cuyahoga are
     untouched - logged as "external_pipeline").
  2. tools/montgomery_sheriff_import.py - re-appends Montgomery sheriff-sale
     leads from the local source JSONL on top of step 1's output (the CSV
     adapter has never included these - they are a separate, additive
     source). That script has no dedup of its own (it always does
     `records.extend(mapped)`), so if re-run against the same source JSONL
     as a prior run it would double the sheriff records - this wrapper
     dedupes SHERIFFSALE entries by case_number+parcel_id immediately after
     calling it (case_number alone, or case_number+address, is NOT unique:
     the real source has one case_number covering 19 separate parcels in a
     bulk tax-foreclosure sale, all sharing one generic street address with
     no house number - see dedupe_sheriff_records()).
  3. tools/montgomery_property_facts_enrich.py - re-applies real GIS
     bedrooms/bathrooms/year-built/property-type/lot-acres on top of step
     2's output (the CSV adapter unconditionally nulls bedrooms/bathrooms;
     the real values come from the Montgomery Auditor's public GIS layer).

  Sheriff-import runs BEFORE property-facts enrichment, not after: the
  sheriff script always creates fresh records with bedrooms=None
  (map_record() hardcodes this - that source has no property
  characteristics), and the GIS enrichment only fills currently-null
  fields on whatever records exist *at the time it runs*. Originally
  (commits f3677a7 then b9027bb) sheriff leads were added first and the
  GIS enrichment ran second, so it covered them too. An earlier version of
  this wrapper had the order reversed, which meant the 6 sheriff leads
  that have real bedroom/bathroom/sqft data in the committed file would
  never get re-enriched on a refresh - diagnosed and fixed in Step 41B2
  (see "before/after" investigation in that step's report).

Guards (split into hard floors and a tolerance-checked warning class):
  Hard floors - any decrease fails the run outright: total records,
  SHERIFFSALE count, CODEVIOLATION count. These must never shrink.

  Soft/tolerance - bedrooms-populated, bathrooms-populated,
  square_feet-populated: a drop of more than 5% from the snapshot fails
  the run; a drop of 5% or less is logged as a warning and the run is
  allowed to commit/push. These three depend on a live external GIS
  query that can have small, real day-to-day match variance independent
  of any bug, so an exact "must never decrease by even one" floor isn't
  appropriate for them the way it is for lead counts - but a large drop
  still indicates something is wrong and must still block the commit.

  On any hard-floor failure, the working tree is reverted to the last
  committed version via `git checkout --`, the failure is logged, and the
  run exits 1 without committing or pushing. This guard exists because a
  live test on 2026-06-23 proved the pipeline can silently drop data:
  running refresh_counties.py alone regenerated the file from the CSV
  adapter only, dropping all 50 SHERIFFSALE records and nulling every
  bedrooms/bathrooms value, then auto-committed and pushed that loss
  before being caught and reverted (commit b738151, reverted by ef9baa0).

Honesty rules:
  - Never marks a run "refreshed" unless a real source was actually found
    and read - refresh_counties.py already returns "skipped_source_missing"
    and leaves the file untouched when its CSV isn't present; this wrapper
    does not override that.
  - Never fabricates a timestamp - change detection is via `git diff`, not
    a manually written "today" value. If nothing changed, nothing is
    committed and no timestamp is touched. The one real "today" used is
    --today for montgomery_sheriff_import.py's auction-countdown labels,
    taken from the real system clock at run time, same as a human running
    it by hand.
  - On any guard failure, the existing dashboard/montgomery/records.json is
    restored exactly via `git checkout --`, never left partially refreshed.

Does not call GHL, skip trace, Tracerfy, SMS, a webhook, n8n, or any
campaign system - it only runs the three local Python scripts above and a
plain git add/commit/push of one file.

Intended to be invoked by Windows Task Scheduler at 06:00 and 13:30
Mountain, Monday-Friday.

Usage:
  python tools/refresh_montgomery_weekday.py
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

NO_COMMIT = False

REPO = Path(__file__).resolve().parents[1]
TARGET = REPO / "dashboard" / "montgomery" / "records.json"
LOG_PATH = REPO / "tools" / "logs" / "montgomery_refresh.log"
REFRESH_LOG_PATH = REPO / "dashboard" / "refresh_log.json"
REFRESH_SCRIPT = REPO / "tools" / "refresh_counties.py"
ENRICH_SCRIPT = REPO / "tools" / "montgomery_property_facts_enrich.py"
SHERIFF_IMPORT_SCRIPT = REPO / "tools" / "montgomery_sheriff_import.py"
UPDATE_STATUS_SCRIPT = REPO / "tools" / "update_refresh_status.py"
STATUS_PATH = REPO / "dashboard" / "refresh_status.json"

FACTORY = Path(os.environ.get("FACTORY_REPO", r"C:\Users\nodaysoff\county-data-factory"))
SHERIFF_SOURCE = FACTORY / "data" / "raw" / "montgomery_dayton_sheriff_sales.jsonl"

HARD_FIELDS = ("total", "sheriffsale", "codeviolation")
SOFT_FIELDS = ("bedrooms", "bathrooms", "square_feet")
SOFT_DROP_TOLERANCE = 0.05  # 5%


def log(message: str) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)


def git_file_changed(path: Path) -> bool:
    """True if `path` differs from what's currently committed (working tree vs HEAD)."""
    result = run(["git", "diff", "--quiet", "--", str(path.relative_to(REPO))])
    return result.returncode != 0


def revert_target() -> None:
    run(["git", "checkout", "--", str(TARGET.relative_to(REPO))])


def previous_source_dataset_date() -> str:
    try:
        data = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        return str(data.get("montgomery", {}).get("source_dataset_date") or "")
    except Exception:
        return ""


def count_new_today(path: Path) -> int:
    """Records with a real first-seen date of today - not just records
    touched/updated today. Honesty rule established in the dashboard's
    own Today's Leads fix (Steps 20/25): first_seen, never last_updated."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    today = datetime.now().date().isoformat()
    count = 0
    for r in payload.get("records", []):
        fsd = str(r.get("first_seen_date") or r.get("first_seen_at") or "")[:10]
        if fsd == today:
            count += 1
    return count


def read_data_updated_at_for_montgomery() -> str:
    """The real fetched_at just written to records.json by this run -
    only called from the success path, after confirming a real change."""
    try:
        payload = json.loads(TARGET.read_text(encoding="utf-8"))
        return str(payload.get("fetched_at") or "")
    except Exception as e:
        log(f"read_data_updated_at_for_montgomery: could not read {TARGET}: {e}")
        return ""


def update_status(status: str, message: str, data_updated_at: str | None = None,
                   source_dataset_date: str | None = None, records_before: int | None = None,
                   records_after: int | None = None, new_today: int | None = None,
                   error: str | None = None) -> bool:
    cmd = [
        sys.executable, str(UPDATE_STATUS_SCRIPT),
        "--county", "montgomery", "--status", status, "--message", message,
    ]
    if data_updated_at:
        cmd += ["--data-updated-at", data_updated_at]
    if source_dataset_date:
        cmd += ["--source-dataset-date", source_dataset_date]
    if records_before is not None:
        cmd += ["--records-before", str(records_before)]
    if records_after is not None:
        cmd += ["--records-after", str(records_after)]
    if new_today is not None:
        cmd += ["--new-today", str(new_today)]
    if error:
        cmd += ["--error", error]
    result = run(cmd)
    if result.stdout.strip():
        log(f"update_refresh_status.py stdout: {result.stdout.strip()}")
    if result.returncode != 0:
        log(f"FAILURE: update_refresh_status.py exited {result.returncode}. stderr: {result.stderr.strip()}")
        return False
    return True


def commit_status_only(exit_code: int) -> int:
    """For exit paths where dashboard/montgomery/records.json was never
    touched, or was already reverted - only refresh_status.json might
    need committing."""
    if NO_COMMIT:
        log("--no-commit set: leaving refresh_status.json written locally, not committing/pushing.")
        return exit_code
    if not git_file_changed(STATUS_PATH):
        log("No change to refresh_status.json. Exiting.")
        return exit_code
    add = run(["git", "add", "dashboard/refresh_status.json"])
    if add.returncode != 0:
        log(f"FAILURE: git add failed: {add.stderr.strip()}")
        return 1
    commit = run(["git", "commit", "-m", "[dashboard] Montgomery refresh status check"])
    if commit.returncode != 0:
        log(f"FAILURE: git commit failed: {commit.stderr.strip()}")
        return 1
    log(f"Committed: {commit.stdout.strip()}")
    push = run(["git", "push", "origin", "HEAD:main"])
    if push.returncode != 0:
        log(f"FAILURE: git push failed: {push.stderr.strip()}")
        return 1
    log("Pushed to origin/main.")
    return exit_code


def read_montgomery_refresh_log_status() -> dict | None:
    try:
        log_data = json.loads(REFRESH_LOG_PATH.read_text(encoding="utf-8"))
        return log_data.get("counties", {}).get("montgomery")
    except Exception:
        return None


def snapshot_counts(path: Path) -> dict[str, int] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"snapshot_counts: could not read/parse {path}: {e}")
        return None
    records = payload.get("records", [])
    return {
        "total": len(records),
        "sheriffsale": sum(1 for r in records if r.get("doc_type") == "SHERIFFSALE"),
        "codeviolation": sum(1 for r in records if r.get("doc_type") == "CODEVIOLATION"),
        "bedrooms": sum(1 for r in records if r.get("bedrooms") is not None),
        "bathrooms": sum(1 for r in records if r.get("bathrooms") is not None),
        "square_feet": sum(1 for r in records if r.get("square_feet") is not None),
    }


def dedupe_sheriff_records(path: Path) -> int:
    """montgomery_sheriff_import.py always does records.extend(mapped) with
    no dedup of its own. Collapse any duplicate SHERIFFSALE entries down to
    the first occurrence.

    Key is case_number + parcel_id, not case_number alone and not
    case_number + address: the real source legitimately has one
    case_number covering multiple distinct parcels under a single tax-
    foreclosure judgment (one case appears 19 times in the live JSONL -
    a bulk sale of 19 separate "Permanent parcel numbers" all sharing one
    generic street address, e.g. "WEST RIVERVIEW AVENUE, DAYTON, OH
    45402" with no house number). Each of those 19 has its own distinct
    parcel_id, which montgomery_sheriff_import.py's map_record() already
    carries through. Keying on case_number alone (or case_number +
    address) incorrectly collapsed 50 genuinely distinct parcels down to
    22-24 on the first two attempts at this dedup - caught both times by
    the guard check, never committed."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records", [])
    seen: set[str] = set()
    deduped = []
    removed = 0
    for r in records:
        if r.get("doc_type") == "SHERIFFSALE":
            case = r.get("case_number") or r.get("doc_number") or ""
            parcel = r.get("parcel_id") or r.get("lead_id") or ""
            key = f"{case}|{parcel}"
            if key in seen:
                removed += 1
                continue
            seen.add(key)
        deduped.append(r)
    if removed:
        payload["records"] = deduped
        payload["record_count"] = len(deduped)
        payload["total"] = len(deduped)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return removed


def main() -> int:
    global NO_COMMIT
    parser = argparse.ArgumentParser(description="Montgomery weekday refresh wrapper")
    parser.add_argument("--no-commit", action="store_true",
                         help="run the full pipeline and guards, write refresh_status.json locally, "
                              "but skip git add/commit/push")
    args = parser.parse_args()
    NO_COMMIT = args.no_commit

    log("=== Montgomery weekday refresh: start ===" + (" (--no-commit)" if NO_COMMIT else ""))

    before = snapshot_counts(TARGET)
    if before is None:
        log("FAILURE: could not snapshot existing dashboard/montgomery/records.json. Aborting.")
        update_status("failed", "Could not read existing records.json before refresh - aborted without touching it",
                       error="could not read records.json before refresh")
        return commit_status_only(1)
    log(f"Before: {before}")

    # 1. CSV + auditor adapter refresh
    result = run([sys.executable, str(REFRESH_SCRIPT)])
    if result.stdout.strip():
        log(f"refresh_counties.py stdout: {result.stdout.strip()}")
    if result.returncode != 0:
        log(f"refresh_counties.py exited {result.returncode}. stderr: {result.stderr.strip()}")

    montgomery_log_status = read_montgomery_refresh_log_status() or {}
    dataset_date = str(montgomery_log_status.get("dataset_date") or "")
    source_status = montgomery_log_status.get("status")

    if not TARGET.is_file():
        log("FAILURE: dashboard/montgomery/records.json missing after refresh_counties.py. Aborting.")
        update_status("failed", "records.json missing after refresh_counties.py step",
                       records_before=before["total"], error="records.json missing after CSV refresh step")
        return commit_status_only(1)

    if source_status == "skipped_source_missing":
        log("Montgomery source CSV not found - refresh_counties.py left records.json untouched.")
        update_status("manual_needed", "Montgomery source CSV not found in Downloads - refresh skipped, old data preserved",
                       records_before=before["total"], records_after=before["total"])
        return commit_status_only(0)

    # 2. Re-append sheriff-sale leads from the local source JSONL (before
    # enrichment, so the GIS pass below covers them too - see module docstring)
    if SHERIFF_SOURCE.is_file():
        today_str = datetime.now().date().isoformat()
        sheriff = run([
            sys.executable, str(SHERIFF_IMPORT_SCRIPT),
            "--in", str(SHERIFF_SOURCE),
            "--out", str(TARGET.relative_to(REPO)),
            "--today", today_str,
        ])
        if sheriff.stdout.strip():
            log(f"montgomery_sheriff_import.py stdout: {sheriff.stdout.strip()}")
        if sheriff.returncode != 0:
            log(f"montgomery_sheriff_import.py exited {sheriff.returncode}. stderr: {sheriff.stderr.strip()}")
        else:
            removed = dedupe_sheriff_records(TARGET)
            if removed:
                log(f"Deduplicated {removed} duplicate SHERIFFSALE record(s) after re-import.")
    else:
        log(f"WARNING: sheriff source not found at {SHERIFF_SOURCE} - skipping sheriff import step.")

    # 3. GIS property facts enrichment - runs last so it covers CODEVIOLATION
    # and freshly re-appended SHERIFFSALE records alike
    enrich = run([sys.executable, str(ENRICH_SCRIPT), "--out", str(TARGET.relative_to(REPO))])
    if enrich.stdout.strip():
        log(f"montgomery_property_facts_enrich.py stdout: {enrich.stdout.strip()}")
    if enrich.returncode != 0:
        log(f"montgomery_property_facts_enrich.py exited {enrich.returncode}. stderr: {enrich.stderr.strip()}")

    after = snapshot_counts(TARGET)
    if after is None:
        log("FAILURE: could not snapshot regenerated dashboard/montgomery/records.json.")
        revert_target()
        update_status("failed", "Could not read records.json after running the refresh pipeline - reverted",
                       records_before=before["total"], error="could not read records.json after refresh pipeline")
        return commit_status_only(1)
    log(f"After: {after}")

    hard_failures = [
        f"{field}: {after[field]} < {before[field]} (before)"
        for field in HARD_FIELDS
        if after[field] < before[field]
    ]
    if hard_failures:
        for failure in hard_failures:
            log(f"HARD GUARD FAILED: {failure}")
        log("Reverting dashboard/montgomery/records.json to last committed version. Exiting failed.")
        revert_target()
        update_status("blocked", "Hard guard failed (record count would have shrunk): " + "; ".join(hard_failures),
                       source_dataset_date=dataset_date or None, records_before=before["total"],
                       records_after=before["total"], error="; ".join(hard_failures))
        return commit_status_only(1)

    soft_failures = []
    for field in SOFT_FIELDS:
        before_count, after_count = before[field], after[field]
        if after_count >= before_count or before_count == 0:
            continue
        drop_fraction = (before_count - after_count) / before_count
        if drop_fraction > SOFT_DROP_TOLERANCE:
            soft_failures.append(
                f"{field}: {after_count} < {before_count} (before), "
                f"drop {drop_fraction:.1%} > {SOFT_DROP_TOLERANCE:.0%} tolerance"
            )
        else:
            log(f"WARNING: {field} dropped {before_count} -> {after_count} "
                f"({drop_fraction:.1%}, within {SOFT_DROP_TOLERANCE:.0%} tolerance) - allowing commit.")
    if soft_failures:
        for failure in soft_failures:
            log(f"SOFT GUARD FAILED (exceeds tolerance): {failure}")
        log("Reverting dashboard/montgomery/records.json to last committed version. Exiting failed.")
        revert_target()
        update_status("blocked", "Soft guard exceeded 5% tolerance: " + "; ".join(soft_failures),
                       source_dataset_date=dataset_date or None, records_before=before["total"],
                       records_after=before["total"], error="; ".join(soft_failures))
        return commit_status_only(1)

    log("All guards passed (hard floors held; any property-fact drop, if present, was within tolerance).")

    new_today = count_new_today(TARGET)

    if not git_file_changed(TARGET):
        prev_dataset_date = previous_source_dataset_date()
        if dataset_date and dataset_date == prev_dataset_date:
            log(f"No change to dashboard/montgomery/records.json - source CSV still dated {dataset_date}.")
            update_status("stale_source",
                           f"Source CSV still dated {dataset_date} - no newer export available in Downloads",
                           source_dataset_date=dataset_date, records_before=before["total"],
                           records_after=after["total"], new_today=new_today)
        else:
            log("No change to dashboard/montgomery/records.json. Exiting cleanly, no data commit.")
            update_status("success_no_change", "Refresh ran successfully; no new records found",
                           source_dataset_date=dataset_date or None, records_before=before["total"],
                           records_after=after["total"], new_today=new_today)
        return commit_status_only(0)

    log("dashboard/montgomery/records.json changed - staging, committing, pushing.")
    new_fetched_at = read_data_updated_at_for_montgomery()
    update_status("success_updated", "Refresh ran and found updated Montgomery data",
                   data_updated_at=new_fetched_at, source_dataset_date=dataset_date or None,
                   records_before=before["total"], records_after=after["total"], new_today=new_today)

    if NO_COMMIT:
        log("--no-commit set: pipeline completed and status written locally, skipping git add/commit/push.")
        return 0

    add = run(["git", "add", "dashboard/montgomery/records.json", "dashboard/refresh_status.json"])
    if add.returncode != 0:
        log(f"FAILURE: git add failed: {add.stderr.strip()}")
        return 1

    commit = run(["git", "commit", "-m", "[dashboard] weekday Montgomery refresh"])
    if commit.returncode != 0:
        log(f"FAILURE: git commit failed: {commit.stderr.strip()}")
        return 1
    log(f"Committed: {commit.stdout.strip()}")

    push = run(["git", "push", "origin", "HEAD:main"])
    if push.returncode != 0:
        log(f"FAILURE: git push failed: {push.stderr.strip()}")
        return 1
    log("Pushed to origin/main.")

    log("=== Montgomery weekday refresh: done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
