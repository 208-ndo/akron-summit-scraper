# Weekday county-data refresh (Mon–Fri 6:00 AM)

`tools/refresh_counties.py` regenerates the **enabled** counties' dashboard
data from their real local sources, writes `dashboard/refresh_log.json`, and
updates data only on a successful real refresh.

| County | How it refreshes | This script |
|---|---|---|
| Montgomery | from `Downloads/leads_montgomery_oh_*.csv` + factory auditor JSONL via the repo adapter | **refreshed here** |
| Summit | its own existing scraper workflow (`.github/workflows/scrape.yml`) | external (untouched) |
| Cuyahoga | its own Cleveland Open Data scraper pipeline | external (untouched) |
| Clark | disabled in `counties.json` | **skipped** (never enabled/faked) |

## Why not a GitHub Actions cron for Montgomery?

Montgomery's real source files (`Downloads/leads_montgomery_oh_*.csv`, the
county-data-factory auditor export) live **only on the operator's local
machine** — they are not in this repo and are not reachable from a GitHub
Actions runner. A CI job therefore cannot honestly regenerate Montgomery, so
we schedule the refresh **locally** and let the operator (or an existing
push step) commit the updated `dashboard/montgomery/records.json`.

Summit already refreshes in CI via the existing `scrape.yml`; that workflow
is left unchanged.

## Schedule it (Windows Task Scheduler, Mon–Fri 06:00)

Run once in an elevated PowerShell/Command Prompt (single line):

```
schtasks /Create /TN "AkronDashboardRefresh" /TR "python \"C:\Users\nodaysoff\akron-summit-scraper\tools\refresh_counties.py\"" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 06:00 /F
```

Then (optional) commit + push the refreshed data — e.g. a wrapper `.bat`:

```
cd /d C:\Users\nodaysoff\akron-summit-scraper
python tools\refresh_counties.py
git add dashboard/montgomery/records.json dashboard/refresh_log.json
git commit -m "[dashboard] weekday auto-refresh county data" && git push origin HEAD:main
```

Point the scheduled task at the `.bat` instead of `python ...` to include the
commit/push. Verify: `schtasks /Query /TN "AkronDashboardRefresh"`.

The script's runtime date logic (urgency countdowns, today's-leads) uses the
real current date at run time — nothing is hard-coded — so a daily/weekday run
keeps any date-derived fields current.
