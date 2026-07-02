# Daily Refresh Audit — 2026-07-01

## Verdict

The daily workflow **was running on schedule the whole time**. The dashboard looked stale because of two silent failure modes, not because cron was broken:

| County | Status before audit | Root cause |
|---|---|---|
| Summit / Akron | **FROZEN since Jun 23** — every run `blocked` | fetch.py rebuilds the dataset from scratch; a source started returning ~300 fewer rows, output shrank 3783 → 3483, and the orchestrator's shrink guard (correctly) reverted the export **every single day**. The guard protected the data but permanently blocked updates. |
| Cuyahoga / Cleveland | Healthy — updated daily (Jul 1: 11,414 → 11,416) | n/a — its scraper merges into the existing file, so it never shrinks |
| Montgomery / Dayton | `stale_source` since Jun 4, last checked Jun 24 | Source is a CSV export that exists **only on the operator's Windows machine** (`C:\Users\...\Downloads\leads_montgomery_oh_*.csv`). It cannot run on a GitHub runner. Also, Montgomery was **not in the scheduled workflow at all**. |

A second, hidden bug: only **42 of 11,416** Cuyahoga records had a `first_seen_date`, so "Today's Leads" undercounted Cuyahoga (only standalone sheriff imports were ever stamped).

## Last successful runs (from git history / refresh_status.json)

- Summit: last real data update **2026-06-23 23:09 UTC**; checked daily since (all blocked)
- Cuyahoga: **2026-07-01 21:23 UTC** (success_updated, +2 records, 4 new today)
- Montgomery: data dated **2026-06-04**, last manual check **2026-06-24**

## Fixes made (this commit)

1. **`scraper/merge_preserve.py` (new)** — merge-preserve layer wired into `fetch.py::write_json_outputs`. Records that vanish from a fresh pull are carried forward from the previously committed `dashboard/records.json` (tagged `carried_forward: true`), so the dataset is monotonic, the shrink guard passes, and Summit unblocks. Verified against the live data: simulated the exact 3783 → 3483 shrink → merged output is 3783 (300 carried). An **empty pull never overwrites good data** (source-failure guard returns the previous payload untouched). No scoring change: carried records keep score, flags, and `first_seen_date` untouched.
2. **`last_seen_date` / `last_updated_date`** now stamped on every Summit record (fresh = today; `last_updated_date` only moves on a real field change).
3. **Cuyahoga `first_seen_date` fix** — `scraper/counties/cuyahoga.py` now stamps `first_seen_date` on every record at write time via `write_output()`. History was backfilled once by `tools/backfill_first_seen_cuyahoga.py` using honest past dates only (10,346 from filed dates, 1,028 from `last_updated`, 0 fabricated, all tagged `first_seen_backfilled: true`). Nothing old can falsely appear as a Today's Lead.
4. **Workflow** (`.github/workflows/daily_refresh_all.yml`) — now runs **all three counties**, the morning run fires **every day** (was weekdays only), and a **dry-run input** was added to `workflow_dispatch`. On the runner, Montgomery honestly reports `manual_needed` and moves its `last_checked_at` — it never fakes data (old data preserved by `refresh_counties.py`'s `skipped_source_missing` path).
5. **Dashboard stale warnings** (`index.html`, status line only — no UI redesign) — shows `⚠ BLOCKED / FAILED / STALE SOURCE / MANUAL NEEDED`, `⚠ DATA Nd OLD` when data is >3 days old, the source file date, and New Today count.
6. **Tests added** (`tests/`, 18 passing) — covering: no overwrite of good data on source failure, record count never shrinks, no fabricated records, first_seen preserved, Today's Leads counts only first-seen-today, one county's crash doesn't stop the others, status-file honesty, and every enabled county exports dashboard JSON.

## Exact commands to run manually

```bash
# Full daily refresh, all counties, commit+push (what CI runs):
python tools/run_daily_refresh_all.py --county summit --county cuyahoga --county montgomery --commit --push

# Dry run (no git writes at all):
python tools/run_daily_refresh_all.py

# One county:
python tools/run_daily_refresh_all.py --county summit --commit --push

# Montgomery (must run on the operator machine that has the CSV):
python tools/refresh_montgomery_weekday.py

# Tests:
python -m pytest tests/
```

## GitHub Actions schedule (after fix)

- `0 12 * * *` — 6:00 AM Mountain, **every day**, all 3 counties
- `30 19 * * 1-5` — 1:30 PM Mountain, weekdays
- Manual: Actions → "Daily Refresh - Summit, Cuyahoga & Montgomery" → Run workflow (optional dry-run checkbox)

## Will the live dashboard update daily now?

- **Summit: yes** — the merge-preserve fix removes the shrink-block that froze it.
- **Cuyahoga: yes** — already was; Today's Leads now honest.
- **Montgomery: no (by design)** — data only moves when you run the refresh on the machine holding the CSV, but the dashboard now *warns* it's stale instead of looking quietly fine. Highest-value fix: automate the Montgomery treasurer delinquent list + sheriff sale pulls in CI (see source gap matrix).
