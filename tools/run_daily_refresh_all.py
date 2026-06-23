#!/usr/bin/env python
"""Single production entry point for the daily Ohio county refresh.

Runs Summit, Cuyahoga, and/or Montgomery, proves each one was actually
checked today (dashboard/refresh_status.json), and commits/pushes the
result only when explicitly told to.

Per county, every run:
  1. Snapshots the real record count before touching anything.
  2. Runs that county's existing safest refresh command (the same
     command already proven safe in .github/workflows/scrape.yml,
     cuyahoga_refresh.yml, and tools/refresh_montgomery_weekday.py -
     this script does not reimplement scraping logic, it orchestrates
     the existing, tested commands).
  3. Snapshots the record count after.
  4. Hard guard: if the count would shrink, revert the affected paths
     via `git checkout --` and report status=blocked. Never ships a
     smaller dataset.
  5. Updates dashboard/refresh_status.json via tools/update_refresh_status.py
     - last_checked_at always moves (a check happened); data_updated_at
       only moves if a real, confirmed change was found.
  6. Commits and pushes only when --commit/--push are passed, and only
     the files that county is allowed to touch.

Montgomery cannot run on a GitHub-hosted runner (its source CSV and
auditor JSONL exist only on this operator's machine - see
tools/REFRESH.md). When this script can't find those files, Montgomery
is reported manual_needed/stale_source rather than silently skipped or
faked.

Never calls GHL, skip trace, Tracerfy, SMS, a webhook, n8n, or any
campaign system - only the existing scraper/refresh commands and git.

Usage:
  python tools/run_daily_refresh_all.py --commit --push
  python tools/run_daily_refresh_all.py --county summit --county cuyahoga --commit --push
  python tools/run_daily_refresh_all.py --county montgomery --commit --push
  python tools/run_daily_refresh_all.py            # dry run, all counties, no git writes
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LOG_DIR = REPO / "tools" / "logs"
UPDATE_STATUS_SCRIPT = REPO / "tools" / "update_refresh_status.py"
STATUS_PATH = REPO / "dashboard" / "refresh_status.json"
MONTGOMERY_WRAPPER = REPO / "tools" / "refresh_montgomery_weekday.py"

ALL_COUNTIES = ("summit", "cuyahoga", "montgomery")


def run(cmd: list[str], cwd: Path = REPO) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def make_logger(county: str):
    log_path = LOG_DIR / f"{county}_daily_refresh.log"

    def log(message: str) -> None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        line = f"[{ts}] {message}"
        print(f"[{county}] {line}")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    return log


def count_records(path: Path) -> int | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return len(payload.get("records", []))
    except Exception:
        return None


def read_fetched_at(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return str(payload.get("fetched_at") or "")
    except Exception:
        return ""


def count_new_today(path: Path) -> int:
    """Records with a real first-seen date of today - never just
    touched/updated today (Today's Leads honesty rule, Steps 20/25)."""
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


def revert_paths(paths: list[str]) -> None:
    run(["git", "checkout", "--"] + paths)


def update_status(county: str, status: str, message: str, data_updated_at: str | None = None,
                   records_before: int | None = None, records_after: int | None = None,
                   new_today: int | None = None, error: str | None = None) -> None:
    cmd = [sys.executable, str(UPDATE_STATUS_SCRIPT), "--county", county, "--status", status, "--message", message]
    if data_updated_at:
        cmd += ["--data-updated-at", data_updated_at]
    if records_before is not None:
        cmd += ["--records-before", str(records_before)]
    if records_after is not None:
        cmd += ["--records-after", str(records_after)]
    if new_today is not None:
        cmd += ["--new-today", str(new_today)]
    if error:
        cmd += ["--error", error[:500]]
    run(cmd)


def git_push_with_retry(log) -> bool:
    push = run(["git", "push", "origin", "HEAD:main"])
    if push.returncode == 0:
        log("Pushed to origin/main.")
        return True
    log(f"Push failed, fetching and rebasing once before retry: {push.stderr.strip()}")
    run(["git", "fetch", "origin"])
    rebase = run(["git", "rebase", "origin/main"])
    if rebase.returncode != 0:
        log(f"FAILURE: rebase onto origin/main failed: {rebase.stderr.strip()}. "
            f"Commit exists locally but is NOT pushed - manual resolution needed.")
        return False
    retry = run(["git", "push", "origin", "HEAD:main"])
    if retry.returncode != 0:
        log(f"FAILURE: push retry after rebase failed: {retry.stderr.strip()}")
        return False
    log("Pushed to origin/main after rebase retry.")
    return True


def refresh_simple_county(county: str, command: list[str], add_paths: list[str], primary_file: Path,
                           commit_message: str, commit: bool, push: bool) -> dict:
    """Shared logic for Summit and Cuyahoga: one command, diff-check on
    add_paths, hard guard on record-count shrink, conditional commit/push."""
    log = make_logger(county)
    log(f"=== {county} daily refresh: start ===")

    before_count = count_records(primary_file)
    log(f"Before count: {before_count}")

    result = run(command)
    if result.stdout.strip():
        log(f"command stdout (truncated): {result.stdout.strip()[-2000:]}")
    if result.returncode != 0:
        log(f"FAILURE: refresh command exited {result.returncode}. stderr: {result.stderr.strip()[-2000:]}")
        update_status(county, "failed", f"{county} refresh command failed - see {county}_daily_refresh.log",
                       records_before=before_count or 0, records_after=before_count or 0,
                       error=result.stderr.strip()[:500] or f"exit code {result.returncode}")
        if commit:
            commit_status_file(county, log, push)
        return {"county": county, "status": "failed", "records_before": before_count, "records_after": before_count}

    after_count = count_records(primary_file)
    if after_count is None:
        log(f"FAILURE: could not read {primary_file} after refresh. Reverting.")
        revert_paths(add_paths)
        update_status(county, "failed", f"Could not read {primary_file.name} after refresh - reverted",
                       records_before=before_count or 0, records_after=before_count or 0,
                       error="could not read primary data file after refresh")
        if commit:
            commit_status_file(county, log, push)
        return {"county": county, "status": "failed", "records_before": before_count, "records_after": before_count}

    log(f"After count: {after_count}")

    if before_count is not None and after_count < before_count:
        log(f"HARD GUARD FAILED: {before_count} -> {after_count}. Reverting {add_paths}.")
        revert_paths(add_paths)
        update_status(county, "blocked", f"Record count would have shrunk {before_count} -> {after_count} - reverted",
                       records_before=before_count, records_after=before_count,
                       error=f"record count shrank {before_count} -> {after_count}")
        if commit:
            commit_status_file(county, log, push)
        return {"county": county, "status": "blocked", "records_before": before_count, "records_after": before_count}

    new_today = count_new_today(primary_file)

    add = run(["git", "add"] + add_paths)
    if add.returncode != 0:
        log(f"FAILURE: git add failed: {add.stderr.strip()}")
    diff = run(["git", "diff", "--cached", "--quiet", "--"] + add_paths)
    changed = diff.returncode != 0

    if changed:
        new_fetched_at = read_fetched_at(primary_file)
        log(f"Data changed - new fetched_at {new_fetched_at}")
        update_status(county, "success_updated", f"{county} refresh ran and found updated data",
                       data_updated_at=new_fetched_at, records_before=before_count or 0,
                       records_after=after_count, new_today=new_today)
        status = "success_updated"
        reason = ""
    else:
        log("No change detected after refresh.")
        update_status(county, "success_no_change", f"{county} refresh ran, no new data found",
                       records_before=before_count or 0, records_after=after_count, new_today=new_today)
        status = "success_no_change"
        reason = "Refresh ran successfully against the live source; the source had no new data this run."

    if not commit:
        log("--commit not set: leaving any changes staged/uncommitted locally.")
        return {"county": county, "status": status, "records_before": before_count, "records_after": after_count,
                "new_today": new_today, "no_new_leads_reason": reason}

    status_add = run(["git", "add", "dashboard/refresh_status.json"])
    diff2 = run(["git", "diff", "--cached", "--quiet"])
    if diff2.returncode == 0:
        log("Nothing staged to commit (data unchanged, status unchanged).")
        return {"county": county, "status": status, "records_before": before_count, "records_after": after_count,
                "new_today": new_today, "no_new_leads_reason": reason}

    commit_result = run(["git", "commit", "-m", commit_message])
    if commit_result.returncode != 0:
        log(f"FAILURE: git commit failed: {commit_result.stderr.strip()}")
        return {"county": county, "status": status, "records_before": before_count, "records_after": after_count,
                "new_today": new_today, "no_new_leads_reason": reason}
    log(f"Committed: {commit_result.stdout.strip()}")

    if push:
        git_push_with_retry(log)

    return {"county": county, "status": status, "records_before": before_count, "records_after": after_count,
            "new_today": new_today, "no_new_leads_reason": reason}


def commit_status_file(county: str, log, push: bool) -> None:
    add = run(["git", "add", "dashboard/refresh_status.json"])
    if add.returncode != 0:
        log(f"FAILURE: git add refresh_status.json failed: {add.stderr.strip()}")
        return
    diff = run(["git", "diff", "--cached", "--quiet"])
    if diff.returncode == 0:
        log("No status change to commit.")
        return
    commit_result = run(["git", "commit", "-m", f"[dashboard] record {county} refresh status"])
    if commit_result.returncode != 0:
        log(f"FAILURE: git commit failed: {commit_result.stderr.strip()}")
        return
    log(f"Committed: {commit_result.stdout.strip()}")
    if push:
        git_push_with_retry(log)


def refresh_summit(commit: bool, push: bool) -> dict:
    command = [
        sys.executable, "scraper/fetch.py",
        "--records", "data/records.json",
        "--out-json", "data/records.enriched.json",
        "--out-csv", "data/records.enriched.csv",
        "--report", "data/match_report.json",
        "--property-access-scope", "priority",
    ]
    return refresh_simple_county(
        county="summit",
        command=command,
        add_paths=["data/", "dashboard/", "trace_store.json"],
        primary_file=REPO / "dashboard" / "records.json",
        commit_message=f"Update scraper data {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        commit=commit, push=push,
    )


def refresh_cuyahoga(commit: bool, push: bool) -> dict:
    command = [sys.executable, "scraper/counties/cuyahoga.py", "--enrich-foreclosures"]
    return refresh_simple_county(
        county="cuyahoga",
        command=command,
        add_paths=["dashboard/cuyahoga/records.json"],
        primary_file=REPO / "dashboard" / "cuyahoga" / "records.json",
        commit_message="[scraper] refresh Cuyahoga data",
        commit=commit, push=push,
    )


def refresh_montgomery(commit: bool, push: bool) -> dict:
    log = make_logger("montgomery")
    log("=== montgomery daily refresh: start (delegating to tools/refresh_montgomery_weekday.py) ===")
    primary_file = REPO / "dashboard" / "montgomery" / "records.json"
    before_count = count_records(primary_file)

    cmd = [sys.executable, str(MONTGOMERY_WRAPPER)]
    if not commit:
        cmd.append("--no-commit")
    result = run(cmd)
    if result.stdout.strip():
        log(f"refresh_montgomery_weekday.py stdout (truncated): {result.stdout.strip()[-2000:]}")
    if result.returncode != 0:
        log(f"refresh_montgomery_weekday.py exited {result.returncode}. stderr: {result.stderr.strip()[-2000:]}")

    after_count = count_records(primary_file)

    if commit and push:
        # refresh_montgomery_weekday.py already committed+pushed internally
        # when it found a real change. Nothing further to do here.
        pass
    elif commit and not push:
        log("WARNING: --commit without --push is not separately supported for Montgomery - "
            "refresh_montgomery_weekday.py pushes immediately after any real commit it makes.")

    try:
        status_data = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        entry = status_data.get("montgomery", {})
    except Exception:
        entry = {}

    return {
        "county": "montgomery",
        "status": entry.get("status", "manual_needed"),
        "records_before": before_count,
        "records_after": after_count,
        "new_today": entry.get("new_today", 0),
        "wrapper_exit_code": result.returncode,
        "entry": entry,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--county", action="append", choices=ALL_COUNTIES,
                         help="repeatable; defaults to all three if omitted")
    parser.add_argument("--commit", action="store_true", help="commit real changes (and the status file)")
    parser.add_argument("--push", action="store_true", help="push commits to origin/main (requires --commit)")
    args = parser.parse_args(argv)

    counties = args.county or list(ALL_COUNTIES)
    commit = args.commit
    push = args.push and args.commit

    dispatch = {"summit": refresh_summit, "cuyahoga": refresh_cuyahoga, "montgomery": refresh_montgomery}

    results = {}
    for county in counties:
        try:
            results[county] = dispatch[county](commit, push)
        except Exception as e:
            log = make_logger(county)
            log(f"UNHANDLED EXCEPTION: {e}")
            update_status(county, "failed", f"Unhandled exception during {county} refresh", error=str(e)[:500])
            results[county] = {"county": county, "status": "failed", "error": str(e)}

    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
