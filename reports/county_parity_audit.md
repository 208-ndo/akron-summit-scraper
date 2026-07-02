# County Field Parity Audit — 2026-07-01

Field-population percentages measured directly against the live committed dashboard exports (`dashboard/records.json`, `dashboard/cuyahoga/records.json`, `dashboard/montgomery/records.json`) after this audit's fixes.

| Field | Summit (3,783) | Cuyahoga (11,416) | Montgomery (774) | Notes |
|---|---|---|---|---|
| Owner name | 76% | 92% | 100% | Summit gap is mostly on vacant-land/cash-buyer records with no owner match |
| Property address | 90% | 99% | 99% | Parity — good |
| City/state/zip | 95% | 93% | **7%** | **Montgomery gap** — CSV adapter isn't carrying city/zip through; fix in `tools/refresh_counties.py` mapping |
| Mailing address | 64% | **8%** | 78% | **Cuyahoga gap** — owner enrichment only covers 8% of parcels (throttle-limited, see source gap matrix #4) |
| Property type | **0%** | **0%** | 94% | **Summit + Cuyahoga gap** — field isn't populated by either scraper. Montgomery gets it from Auditor GIS |
| Beds/baths/sqft | 15% | **0%** | 76% | **Cuyahoga gap** — no GIS property-facts enrichment exists yet (Montgomery pattern could be reused: `montgomery_property_facts_enrich.py` → Cuyahoga's own MyPlace parcel API) |
| Lot size | 14% | **0%** | 90% | Same root cause as beds/baths |
| Year built | **0%** | **0%** | 77% | Same |
| Assessed value | 61% | 8% | 92% | Cuyahoga tax-value enrichment is throttle-limited (same as mailing address) |
| Amount owed / arrears | 67% | 7% | 94% | Same |
| Filed / sale / hearing date | 34% | 90% | **0%** | **Montgomery gap** — CSV adapter has no filed-date column; RealAuction/Treasurer scrape (source gap matrix #1) would fill this |
| Distress source tags | 100% | 100% | 100% | Parity |
| Seller score | 100% | 98% | 92% | Parity |
| Distress count | 97% | 100% | 100% | Parity |
| Absentee owner | 100% | 8% | 100% | **Cuyahoga gap** — same enrichment-coverage issue |
| Out-of-state owner | 100% | 1% | 100% | Same |
| Vacant flag | 100% | **0%** | 100% | **Cuyahoga gap** — no vacant-registry source exists yet (source gap matrix #2, high value) |
| Tired landlord / 2+ properties | 100% | 93% | **0%** | **Montgomery gap** — not derived in the CSV adapter |
| Skip-trace eligibility | 4% | **0%** | **0%** | See below — this is a scoring-rule gap, not a data gap |
| first_seen_date | 100% | 100% (after backfill) | 100% | Fixed this audit — Cuyahoga was 0.4% before backfill |

## Cross-county summary

**Strongest county:** Montgomery on property characteristics (beds/baths/year/lot/type all 76–94%) thanks to the Auditor GIS enrichment step — but weakest on location fields (city/zip 7%) and dates (filed 0%), both CSV-adapter mapping bugs, not missing sources.

**Weakest county:** Cuyahoga on property characteristics and owner-mailing enrichment (0% beds/baths/year/property-type, 8% mailing/absentee) — the ArcGIS violation/condemnation/demolition sources are strong, but there's no property-facts or full-owner enrichment pass yet, unlike Summit (CAMA) and Montgomery (Auditor GIS).

**Skip-trace eligibility near-zero everywhere (Summit 4%, Cuyahoga/Montgomery 0%):** this reflects that `skip_trace_eligible`-style fields are computed at the auto-skip-trace step (`hydrate_records_from_trace_store` / `auto_skip_trace_records` in `scraper/fetch.py`), which is **disabled in CI** (`AUTO_SKIP_TRACE_DISABLED=1` in the workflow env) to avoid burning skip-trace credits on every automated run. This is expected behavior, not a bug — skip tracing should stay a deliberate, budgeted step, not something that fires on every daily cron run. No fix applied; flagging for your awareness only.

## Recommended next-audit priorities (highest field-parity value first)

1. Montgomery CSV adapter: map city/zip and filed-date columns (two small, contained fixes — biggest parity gain for least work).
2. Cuyahoga: add a property-facts GIS enrichment pass reusing the Montgomery pattern.
3. Cuyahoga: raise the owner/tax-value enrichment throttle/limit so mailing address, absentee, and out-of-state flags move off single digits.
4. Cuyahoga: add the vacant-building ArcGIS layer (already in the source gap matrix as the #2 highest-value source).
