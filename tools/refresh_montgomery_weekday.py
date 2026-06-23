#!/usr/bin/env python
"""Weekday wrapper for the Montgomery county-data refresh.

Runs tools/refresh_counties.py - which regenerates ONLY
dashboard/montgomery/records.json from the operator's real local
source files (Summit and Cuyahoga are logged there as
"external_pipeline" and never touched) - then commits and pushes
dashboard/montgomery/records.json *only if its content actually
changed*. Never commits dashboard/refresh_log.json: that file is
regenerated (with a fresh "ran_at") on every run regardless of
whether the underlying Montgomery data changed, so committing it
every time would create noise commits with no real content change.

Honesty rules (match refresh_counties.py's own design):
  - Never marks a run "refreshed" unless the real Montgomery CSV
    source was actually found and read - refresh_counties.py
    already returns "skipped_source_missing" and leaves the file
    untouched when the source CSV isn't present; this wrapper does
    not override that.
  - Never fabricates a timestamp - change detection is via
    `git diff`, not a manually written "today" value. If nothing
    changed, nothing is committed and no timestamp is touched.
  - On any failure (missing source, refresh_counties.py error, git
    error), the existing dashboard/montgomery/records.json is left
    exactly as-is and the failure is logged, never silently
    swallowed.

Intended to be invoked by Windows Task Scheduler at 06:00 and 13:30
Mountain, Monday-Friday. NOT scheduled yet - see tools/REFRESH.md.
This script has only been syntax-checked, not run end-to-end.

Usage:
  python tools/refresh_montgomery_weekday.py
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TARGET = REPO / "dashboard" / "montgomery" / "records.json"
LOG_PATH = REPO / "tools" / "logs" / "montgomery_refresh.log"
REFRESH_SCRIPT = REPO / "tools" / "refresh_counties.py"


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


def main() -> int:
    log("=== Montgomery weekday refresh: start ===")

    result = run([sys.executable, str(REFRESH_SCRIPT)])
    if result.stdout.strip():
        log(f"refresh_counties.py stdout: {result.stdout.strip()}")
    if result.returncode != 0:
        log(f"FAILURE: refresh_counties.py exited {result.returncode}. "
            f"stderr: {result.stderr.strip()}")
        log("Existing dashboard/montgomery/records.json left untouched. Exiting.")
        return 1

    if not TARGET.is_file():
        log("FAILURE: dashboard/montgomery/records.json missing after refresh. Exiting.")
        return 1

    if not git_file_changed(TARGET):
        log("No change to dashboard/montgomery/records.json. Exiting cleanly, no commit.")
        return 0

    log("dashboard/montgomery/records.json changed - staging, committing, pushing.")
    add = run(["git", "add", "dashboard/montgomery/records.json"])
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
