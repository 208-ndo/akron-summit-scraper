# Government / Public Distress Source Gap Matrix — 2026-07-01

Legend: ✅ scraper/importer exists and runs · 🟡 partial / indirect coverage · ❌ no importer · 🔒 blocked (CAPTCHA / login / PDF-only / manual download)

Freshness reflects the state at audit time. URLs are the sources the code actually hits (harvested from `scraper/fetch.py`, `scraper/counties/cuyahoga.py`, and the Montgomery tools) plus the official agency endpoints for the gaps.

## Summit County / Akron

| Source | Agency / URL | Access | Importer | Fresh | Blockers | Recommended fix |
|---|---|---|---|---|---|---|
| Tax delinquent | Akron Legal News delinquent-tax notices — akronlegalnews.com/notices/delinquent_taxes; Summit Fiscal Office CAMA files — fiscaloffice.summitoh.net | HTML + CSV/ZIP | ✅ | Was frozen by shrink-block → fixed | — | none |
| Tax foreclosure | Summit Clerk of Courts new civil filings — newcivilfilings.summitoh.net | HTML | ✅ (foreclosure/LP scrape) | fixed | — | none |
| Sheriff sale / foreclosure auction | Akron Legal News sheriff sale abstracts — akronlegalnews.com/notices/sheriff_sale_abstracts | HTML | ✅ | fixed | — | Add Summit Sheriff's RealAuction calendar (summit.sheriffsaleauction.ohio.gov) for sale dates/deposits ahead of the printed abstracts |
| Lis pendens / pre-foreclosure | Clerk new civil filings (LP/NOFC doc types) | HTML | ✅ | fixed | — | none |
| Code violations | City of Akron housing/code data (akronohio.gov) | HTML | ✅ | fixed | — | none |
| Vacant homes / registry | Akron Vacant Building Board — akronohio.gov | HTML | ✅ | fixed | — | none |
| Nuisance / unsafe / condemned / fire | Housing Appeals Board + fire damage leads (data/fire_damage.json) | HTML | ✅ | fixed | — | none |
| Absentee / out-of-state owner | Derived from CAMA owner vs mailing address | CSV | ✅ | fixed | — | none |
| Probate / estate | Summit Probate eServices — search.summitohioprobate.com; Akron Legal News probate new cases | HTML (JS) | ✅ (Playwright + fallback) | fixed | Occasional JS wall | none |
| Eviction filings | Akron Municipal Court | HTML | 🟡 (evictions.json exists; source fragile) | stale | Session-based search | Re-point to Akron Municipal Court records portal; mark manual-import if it adds a CAPTCHA |
| Rental complaints / landlord violations | Akron Rental Information (ARI) — ari.akronohio.gov | HTML | ✅ (complaint stacking) | fixed | Throttled | none |
| Water shutoff / utility lien | Akron Public Utilities | — | ❌ | — | Not published as records | Skip — not publicly available as a dataset |
| Permits / failed permits | Akron Legal News building permits — akronlegalnews.com/publicrecord/building_permits | HTML | 🟡 harvested, not scored | — | — | Low priority; add as distress modifier for expired permits |
| Land bank / demolition | Summit County Land Bank | HTML/PDF | ❌ | — | PDF lists | Manual import quarterly |
| Tax liens / municipal liens | Summit Fiscal Office | CSV (CAMA legdat) | 🟡 partial via CAMA | fixed | — | Parse lien columns already present in SC702 legdat |
| Divorce (bonus) | Akron Legal News domestic relations | HTML | ✅ | fixed | — | none |
| LLC / corporate owner flags | Derived from owner name | derived | ✅ | fixed | — | none |
| Repeat owner / tired landlord | Derived (owner portfolio flags) | derived | ✅ | fixed | — | none |

## Cuyahoga County / Cleveland

| Source | Agency / URL | Access | Importer | Fresh | Blockers | Recommended fix |
|---|---|---|---|---|---|---|
| Code violations | Cleveland Open Data — Complaint_Violation_Notices ArcGIS FeatureServer | ArcGIS API | ✅ (primary source) | ✅ Jul 1 | — | none |
| Condemnations | Current_Condemnations FeatureServer | ArcGIS API | ✅ | ✅ | — | none |
| Demolition permits | Demolition_Permits FeatureServer | ArcGIS API | ✅ | ✅ | — | none |
| Nuisance complaints | CDPH_Complaints FeatureServer | ArcGIS API | ✅ | ✅ | — | none |
| Sheriff sale / foreclosure auction | Cuyahoga cpdocket SheriffSearch — cpdocket.cp.cuyahogacounty.gov/SheriffSearch/ | HTML | ✅ (`--enrich-foreclosures`, daily) | ✅ | — | none |
| Tax delinquent | MyPlace LegacyTaxes — myplace.cuyahogacounty.gov | API | ✅ (enrich-tax-delinquency phase) | 🟡 only 7% of records carry amounts | Throttle limits | Raise `--property-limit` in a weekly deep-enrich run |
| Tax foreclosure | Board of Revision / cpdocket | HTML | 🟡 via sheriff docket | — | — | Add BOR tax-foreclosure docket scrape |
| Lis pendens / pre-foreclosure | Cuyahoga Clerk of Courts e-docket | HTML | ❌ | — | 🔒 CAPTCHA on civil search | Mark manual/import-only; sheriff docket partially covers downstream stage |
| Vacant homes / registry | Cleveland Building & Housing vacant/boarded (ArcGIS) | ArcGIS API | ❌ | — | — | **Highest-value add** — same ArcGIS pattern as violations |
| Probate / estate | Cuyahoga Probate Court case search | HTML | ❌ | — | 🔒 disclaimer + form flow | Second-highest value; likely needs Playwright — ask before adding |
| Eviction filings | Cleveland Housing Court | HTML | ❌ | — | 🔒 login for bulk | Manual/import-only |
| Absentee / out-of-state | MyPlace owner vs mailing (only 8% enriched) | API | 🟡 | partial | Throttle | Extend owner enrichment coverage (weekly batch) |
| Water shutoff / utility lien | Cleveland Water | — | ❌ | — | Not public | Skip |
| Land bank | Cuyahoga Land Bank property list — cuyahogalandbank.org | HTML/CSV | ❌ | — | — | Nice-to-have (comps/avoid list) |
| Tax liens / municipal liens | County Fiscal Officer certified list | HTML/PDF | 🟡 certified_tax_total field exists | partial | — | Extend LegacyTaxes pull |
| LLC / investor owner flags | Derived | derived | ✅ | ✅ | — | none |
| Repeat owner / tired landlord | Derived (investor_owner, 93%) | derived | ✅ | ✅ | — | none |

## Montgomery County / Dayton

| Source | Agency / URL | Access | Importer | Fresh | Blockers | Recommended fix |
|---|---|---|---|---|---|---|
| Combined lead CSV (tax delinq, code viol, vacancy…) | Operator-side pipeline → `leads_montgomery_oh_*.csv` | Manual CSV | ✅ adapter (`tools/refresh_counties.py`) | 🔒 stale — CSV dated 2026-06-04 | **Lives only on operator's machine** | Move the upstream generator into this repo/CI, or commit the CSV to `imports/montgomery_oh/` (path already checked first) |
| Tax delinquent | Montgomery Treasurer delinquent list — mcohio.org/1521/Delinquent-List | HTML/PDF | 🟡 via CSV only | stale | PDF-ish page | Direct scrape → **top automation candidate** |
| Sheriff sale / foreclosure auction | Montgomery Sheriff via RealAuction — montgomery.sheriffsaleauction.ohio.gov | HTML/JSONL | ✅ import (`tools/montgomery_sheriff_import.py`) from local JSONL | stale | Source JSONL is local-only | Scrape RealAuction directly in CI (public, no login for listings) |
| Property facts (beds/baths/year) | Montgomery Auditor GIS — gis.mcohio.org AUDGIS_B1/MapServer/7 | ArcGIS API | ✅ (`montgomery_property_facts_enrich.py`) | ✅ works in CI | — | none — this one already runs anywhere |
| Tax foreclosure / lis pendens | Montgomery Clerk of Courts PRO — pro.mcohio.org | HTML | ❌ | — | 🔒 disclaimer flow | Investigate; likely scriptable |
| Code violations | City of Dayton | via CSV | 🟡 | stale | — | Find Dayton open-data endpoint |
| Vacant registry | City of Dayton vacant/nuisance lists | via CSV | 🟡 | stale | — | same |
| Probate / estate | Montgomery Probate Court search | HTML | ❌ | — | 🔒 form flow | Manual for now |
| Evictions | Dayton Municipal Court | HTML | ❌ | — | 🔒 | Manual/import-only |
| Land bank | Montgomery County Land Bank (mclandbank.com) | HTML | ❌ | — | — | Low priority |
| Absentee / out-of-state / vacant flags | Derived in CSV adapter | derived | ✅ | ✅ logic | — | none |
| Tired landlord / portfolio | — | — | ❌ (0% populated) | — | — | Derive from Auditor GIS owner-name grouping (same pattern as Summit) |

## Next highest-value sources to add (ranked)

1. **Montgomery Treasurer delinquent list + RealAuction sheriff scrape in CI** — turns Montgomery from manual-only into a real daily county.
2. **Cleveland vacant/boarded ArcGIS layer** — one afternoon of work, same client code as existing Cuyahoga layers, unlocks the vacant flag (currently 0%).
3. **Cuyahoga Probate Court** — probate is your proven best Summit signal and is entirely missing in Cuyahoga (needs Playwright — ask first per your rules).
4. **Cuyahoga MyPlace mailing-address enrichment batch** — absentee/out-of-state detection is only at 8%/1%; the API already works, it's a coverage/throttle issue.
5. **Summit RealAuction sheriff calendar** — earlier + structured sale dates than the Legal News abstracts.
