# County Daily Refresh — Standard Operating Procedure

Status: **live as of 2026-06-23.** Summit and Cuyahoga refresh via GitHub
Actions; Montgomery refreshes via two Windows Scheduled Tasks on this
machine. All three are weekday-only, twice daily (6:00 AM and 1:30 PM
Mountain). This file describes the verified, currently-running state of
each county's refresh path plus the guard rules that protect it.

## Hard rules (apply to all three counties)

- **Updated timestamp means a real source pull succeeded.** No script in
  this repo is allowed to write `today` into a timestamp field unless that
  value came from an actual completed fetch.
- **"Today Pulled" means records first seen today** (`first_seen_date` /
  `first_seen_at`), not merely records that happened to be touched/updated
  today. (Established in Steps 20/25 of the dashboard work — see
  `recordFirstSeenDate()` / `countTodayLeads()` in `index.html`.)
- **If a source fails, keep the old data and log the failure.** Never
  overwrite good data with an empty or partial result.
- **If a county loses records unexpectedly, restore the previous
  `records.json` and fail the run.** Don't ship a smaller dataset silently.
- **If sheriff records vanish, restore and fail.**
- **If beds/baths/sqft disappear, restore and fail.**
- **No automatic texting. No GHL push. No skip trace. No paid credits. No
  fabricated dates.** Refresh automation only ever fetches and writes public
  county data — nothing in any of the three refresh paths below calls GHL,
  Tracerfy, SMS, a webhook, or n8n (verified by direct code search in this
  session's Step 40B audit).

---

## Summit County

| | |
|---|---|
| **Exact refresh command** | `python scraper/fetch.py --records data/records.json --out-json data/records.enriched.json --out-csv data/records.enriched.csv --report data/match_report.json --property-access-scope priority` |
| **GitHub workflow file** | `.github/workflows/scrape.yml` |
| **Schedule** | `0 12 * * 1-5` (6:00 AM Mountain) and `30 19 * * 1-5` (1:30 PM Mountain), weekdays. Also fires on `workflow_dispatch` and on any push to `main` touching `scraper/fetch.py` or the workflow file itself. |
| **Output file(s) changed** | `data/records.json`, `data/records.enriched.json`, `data/records.enriched.csv`, `data/match_report.json`, the per-category exports under `dashboard/*.json` and `data/*.json`, and `trace_store.json`. Commit step: `git add data/ dashboard/ trace_store.json`. |
| **What counts prove success** | `dashboard/records.json`'s top-level `fetched_at` advances to the run time; `record_count`/`total` reflects the real scrape; live dashboard Sheriff Sales / Tax Delinquent / Today Pulled counts move accordingly. |
| **How the timestamp is updated honestly** | `fetched_at` is set once, at the end of a completed run, to `datetime.now(timezone.utc).isoformat()` inside `build_payload()` — never written ahead of a successful fetch. Per-record `last_updated` only changes for records the run actually touched; `first_seen_date` is only set on genuinely new records (Step 20/25 fix — `recordSourceRefreshedDate()` / `recordFirstSeenDate()` in `index.html` read these as-is, nothing is back-filled to "today"). |
| **Env note** | `AUTO_SKIP_TRACE_DISABLED: '1'` is set in the workflow — auto skip-trace is off for every scheduled/CI run by design. |

## Cuyahoga County

| | |
|---|---|
| **Exact refresh command** | `python scraper/counties/cuyahoga.py --enrich-foreclosures` |
| **GitHub workflow file** | `.github/workflows/cuyahoga_refresh.yml` |
| **Schedule** | `0 12 * * 1-5` (6:00 AM Mountain) and `30 19 * * 1-5` (1:30 PM Mountain), weekdays. Also fires on `workflow_dispatch`. |
| **Output file(s) changed** | `dashboard/cuyahoga/records.json` only. Commit step is scoped to exactly that path (`git add dashboard/cuyahoga/records.json`) — Summit, Montgomery, Clark, `counties.json`, and UI files are never touched by this workflow. |
| **What counts prove success** | `fetched_at` advances; the function's own `matched` / `standalone` counters (logged to the Action's output) report how many sheriff-sale records were merged into existing parcels vs. added as new; live Sheriff Sales tab count (805 as of this session) and Today Pulled count move accordingly. |
| **How the timestamp is updated honestly** | `enrich_foreclosure_stack()` sets `target["last_updated"] = timestamp` only on records actually matched against the live sheriff-sale pull, and stamps `first_seen_date` only on genuinely new standalone records (Step 21 fix — before that fix, matched/merged records silently kept a stale `last_updated`, which is the bug this command exists to have already corrected). |
| **Critical "do not run" warning** | The *bare* command `python scraper/counties/cuyahoga.py` (no flags) is destructive — it falls through to `build_payload()`, which overwrites the entire file with a fresh **code-violations-only** payload (≤1000 records), discarding the other ~8,000 demolition/nuisance/sheriff records currently in the file. Only `--enrich-foreclosures` is safe for this scheduled refresh (confirmed by reading `build_payload()` directly before this workflow was written — see Step 40C). |

## Montgomery County

| | |
|---|---|
| **Exact local refresh command** | `python tools/refresh_montgomery_weekday.py` (wrapper) — never the bare `python tools/refresh_counties.py` directly, and never `tools/factory_csv_to_records.py` directly. |
| **Why it cannot fully run in GitHub Actions yet** | Its two real source files exist only on this operator's local machine and are not reachable from a GitHub-hosted runner: see "required local source files" below. `tools/REFRESH.md` documents this as the explicit reason; confirmed unchanged in this session's Step 39 audit. |
| **Required local source files** | `C:\Users\nodaysoff\Downloads\leads_montgomery_oh_*.csv` (dated lead export; the newest by filename date is used), `C:\Users\nodaysoff\county-data-factory\data\raw\montgomery_auditor_iasworld.jsonl` (auditor enrichment, optional but expected), and `C:\Users\nodaysoff\county-data-factory\data\raw\montgomery_dayton_sheriff_sales.jsonl` (sheriff-sale leads, re-imported every run). |
| **Output file changed** | `dashboard/montgomery/records.json` only. `dashboard/refresh_log.json` is regenerated locally on every run but is **never committed** — it gets a fresh `ran_at` every run regardless of whether real data changed, so committing it every time would be a noise commit. |
| **Pipeline order** | (1) `tools/refresh_counties.py` — CSV+auditor adapter, regenerates the base 719 code-violation records. (2) `tools/montgomery_sheriff_import.py` — re-appends the 50 sheriff-sale leads (deduped by `case_number+parcel_id`; `case_number` alone is not unique — one real case covers 19 distinct parcels in a bulk tax-foreclosure sale). (3) `tools/montgomery_property_facts_enrich.py` — GIS beds/baths/year-built/property-type/lot-acres, run **last** so it covers the sheriff records too (they're always created with `bedrooms: None`; enrichment only fills currently-null fields on whatever exists at the time it runs). |
| **What counts prove success** | Total record count, `CODEVIOLATION` count, `SHERIFFSALE` count, and bedrooms/bathrooms/square_feet-populated counts, each checked against the previous commit's real snapshot — not just "the script exited 0." |
| **Guard checks before commit/push** | Hard floors (any decrease reverts and fails, no tolerance): total records, `SHERIFFSALE` count, `CODEVIOLATION` count — compared dynamically against the snapshot taken from the currently committed file at the start of the run, not a hardcoded number. Soft/tolerance (bedrooms/bathrooms/square_feet-populated): a drop of more than 5% fails and reverts; a drop of 5% or less logs a warning and still allows the commit, since these depend on a live external GIS query with legitimate small day-to-day match variance. |
| **Why these specific guards exist** | Discovered live in this session (2026-06-23), in two stages. First: an early version of the wrapper ran `refresh_counties.py` alone, silently dropping all 50 `SHERIFFSALE` records and nulling every `bedrooms`/`bathrooms` value, then auto-committed and pushed that loss before being caught and reverted (`git revert`). Second, after adding sheriff re-import: a pipeline-ordering bug (enrichment running before sheriff import, so the 6 sheriff records with real GIS data never got re-enriched) caused a real-but-small drop that the guard correctly caught - root-caused via git history and fixed by reordering, not waved through with a tolerance. |
| **Windows Scheduled Tasks** | `AkronDashboardMontgomeryRefreshAM` (06:00 Mon-Fri) and `AkronDashboardMontgomeryRefreshPM` (13:30 Mon-Fri), both running `cmd.exe /c cd /d C:\Users\nodaysoff\akron-summit-scraper && python tools\refresh_montgomery_weekday.py`. Created and verified enabled in this session. |

---

## Command map

| County | Runs where | Command | Schedule | Output file | Commit scope | Risk guard |
|---|---|---|---|---|---|---|
| Summit | GitHub Actions (`scrape.yml`) | `python scraper/fetch.py --records data/records.json --out-json data/records.enriched.json --out-csv data/records.enriched.csv --report data/match_report.json --property-access-scope priority` | `0 12,19:30 * * 1-5` (6 AM / 1:30 PM Mountain, weekdays) + dispatch + push-on-`fetch.py`-change | `data/*.json`, `dashboard/*.json`, `trace_store.json` | `data/ dashboard/ trace_store.json` | None currently implemented — relies on the scraper's own honest per-record timestamping; no record-count floor guard exists yet for Summit. |
| Cuyahoga | GitHub Actions (`cuyahoga_refresh.yml`) | `python scraper/counties/cuyahoga.py --enrich-foreclosures` | `0 12,19:30 * * 1-5` (6 AM / 1:30 PM Mountain, weekdays) + dispatch | `dashboard/cuyahoga/records.json` | `dashboard/cuyahoga/records.json` only | None currently implemented — relies on `--enrich-foreclosures` being a safe merge (confirmed), but no automated floor/regression guard exists yet for Cuyahoga either. |
| Montgomery | Local machine (Windows Task Scheduler) | `python tools/refresh_montgomery_weekday.py` | `AkronDashboardMontgomeryRefreshAM` 06:00 + `AkronDashboardMontgomeryRefreshPM` 13:30, Mon-Fri | `dashboard/montgomery/records.json` only (never `dashboard/refresh_log.json`) | `dashboard/montgomery/records.json` only | Hard floors (no tolerance): total, SHERIFFSALE, CODEVIOLATION must not shrink vs. the last commit. Soft (5% tolerance, then hard fail): bedrooms/bathrooms/square_feet-populated. Revert + fail if any floor breached. |

---

## Remaining gaps (not blockers, but not yet built)

1. **Summit and Cuyahoga have no automated regression guard.** Both rely on
   the scraper/enrichment functions being correct by construction (verified
   manually this session), but neither workflow has a Montgomery-style
   record-count floor check before committing. A bad run on either could
   still silently ship a smaller/corrupted file via CI - Montgomery is the
   only county with an automated safety net so far.
2. **No alerting exists for a failed/guarded run.** All three paths log
   failures (Actions run logs, or `tools/logs/montgomery_refresh.log`
   locally) but nothing notifies a person when a guard fires or a source
   fails — failures are silent until someone checks.
3. **`tools/logs/montgomery_refresh.log` is local-only and gitignored** —
   if this machine's disk is lost or the task runs on a different machine,
   the run history goes with it. No log rotation either; the file grows
   unbounded.
