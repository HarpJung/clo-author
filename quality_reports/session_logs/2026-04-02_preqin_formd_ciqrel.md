# Session Log: 2026-04-02 — Preqin Tests, Form D Parsing, CIQ Company Relationships

## Goal
Test VC fund-level effects of observer information, improve data linkages, and explore alternative approaches to the null CAR results.

---

## Key Discoveries This Session

### 1. CIQ Company Relationships Table (ciq.ciqcompanyrel)
**This is the most important data discovery.** The table has 14.1M rows of company-to-company relationships with types:
- **"Current Investment"**: 990,754 rows — VC currently holds investment in company
- **"Prior Investment"**: 644,189 rows — VC previously invested (exited)
- Has `percentownership` and `totalinvestment` (dollar amount)
- Our VCs are there: NEA=1,634 links, Sequoia=1,814, Kleiner=1,141, Andreessen=1,274
- **This directly confirms which VC invested in which company** — no more inference needed

### 2. Form D Related Persons (parsed from SEC EDGAR)
- Parsed 36 quarters (2017-2025) of Form D filings: 1.7M related persons, 474K issuers
- **277 confirmed observer-company-date links**: Person X listed as Director on Company A's Form D filing with exact sale date
- 138 unique observers, 120 unique companies confirmed
- 81 overlap with existing CIQ network; 58 are NEW links not in CIQ
- Relationship types: 71% Director, 29% Executive Officer

### 3. CIQ Data Dictionary (Full Scan)
Key tables in the `ciq` schema on WRDS:

| Table | Rows | Description |
|-------|------|-------------|
| ciq.ciqcompany | 37.5M | All companies (public + private + funds) |
| ciq.ciqprofessional | 12.2M | Person-company position links (our main data source) |
| ciq.ciqperson | 7.0M | Person details (name, year born) |
| ciq.ciqpersonbiography | 6.2M | Person biographies |
| **ciq.ciqcompanyrel** | **14.1M** | **Company relationships (investment, subsidiary, etc.)** |
| ciq.ciqcompanyreltype | 32 | Relationship type lookup |
| ciq.ciqkeydev | 33.8M | Key developments (events) |
| ciq.ciqkeydevtoobjecttoeventtype | 40.7M | Event-to-object-role mapping |
| ciq.ciqcompanyindustrytree | 95.0M | Industry classification hierarchy |
| ciq.ciqprotoprofunction | 19.9M | Professional function (person's specific role) |
| ciq.ciqtransaction | 2.2M | Financial transactions |
| ciq.ciqfincollection | 745.0M | Financial data collection |
| ciq.ciqfininstance | 56.6M | Financial instances |
| ciq.ciqsymbol | 30.5M | Security symbols/tickers |
| ciq.ciqsecurity | 941K | Security details |
| ciq.ciqtradingitem | 1.3M | Trading items |

### 4. Form D Raw Files Structure
Location: `Data/FormD/YYYY_QN/YYYYQN_d/`
Each quarter has 8 TSV files:
- **ISSUERS.tsv** — company raising money (CIK, entity name, state, year of inc)
- **RELATEDPERSONS.tsv** — directors/officers/promoters (first name, last name, relationship, address)
- **OFFERING.tsv** — deal details (sale date, amount offered/sold, industry, fund type, exemption type)
- RECIPIENTS.tsv — states where securities were sold
- SIGNATURES.tsv — who signed the filing
- FormD_metadata.json — filing metadata
- Available: 2017_Q1 through 2025_Q4 (36 quarters)
- Total: 1.7M related persons, 474K issuers, 467K offerings

---

## Preqin Test Results Summary

### Test v1: Baseline fund performance
- Delta TVPI: +0.0046 (HC1 p<0.001), dies with fund-clustering
- M&A × Post2020 TVPI: +0.0104 (HC1 p=0.009), marginal with clustering

### Test v2: Benchmark-adjusted + cashflow
- Has Distribution: +0.043 (HC1 p<0.001, fund-cl p=0.062, Fund FE p<0.001)
- Has Capital Call: -0.009 (robust to 4 of 6 specs)
- Delta DPI × Post2020: +0.218 (HC1 p=0.055)

### Test v3: Full 6-spec battery
- Has Distribution survives Fund FE (p<0.001)
- Delta DPI × Post2020 survives Fund FE (p=0.002)
- Fund-clustered SEs often blow up (p=1.000)

### Test v4: Bradley-style (between-quarter changes + event window)
- DPI spikes in event quarter, not before or after (HC1 p=0.007, firm-cl p=0.009)
- Within-fund: calls AND distributions both increase in event quarters (Fund FE p<0.01)
- NVCA DiD on DPI: +0.373 (Fund FE p=0.002)
- Performance acceleration (d2): null across all specs

### Test v5: Daily cashflow event study
- **Completely flat.** Distribution rate = 0.48%/day regardless of proximity to events
- The quarterly DPI result is not driven by daily timing around specific events

### DPI subset check
- DPI result entirely driven by 480 funds with cashflow data (+0.391, p=0.003)
- 811 funds without cashflow data: zero effect (-0.003, p=0.971)

### Time-bounded test (using PP dates to anchor observer timing)
- Bounded DPI Firm FE: -0.234 (p=0.001) — STRONGER than unbounded
- Bounded DPI DiD: +0.330 (p=0.012) — STRONGER than unbounded
- Bounded + vintage filter: similar direction but less power (half the sample)

---

## Three New Tests (from parallel agents)

### Test A: Daily Form 4 Event Study (Deep Dive)
- 92% of observers trade more in [-30,-1] vs baseline (Wilcoxon p<0.0001)
- Trading spike is 3.21x baseline, driven by SELLS (3.43x) not buys (2.38x)
- **NVCA DiD: Pre-2020 pre_30 = zero, Post-2020 pre_30 = +0.0039 (event-cl p=0.018)**
- Survives Person FE + HC1 (p<0.001)
- Returns after pre-event sells: -2.1% over 30 days (HC1 p<0.001, event-cl p=0.28)
- Same-industry vs different-industry: no difference (limited coverage)
- Funding events drive strongest pre-event trading

### Test B: Observer Count → Fund Performance (with controls)
- **Within-firm (Firm FE + Vintage FE): +0.056 TVPI (p<0.001), +0.84 IRR (p<0.001)**
- Survives GP experience controls for TVPI (+0.039, p=0.002) but not IRR
- Industry specialization (HHI interaction): marginal (p=0.07-0.08)
- Quintile analysis: Q5 vs Q1 = +20.1pp IRR (p<0.001)
- Fund terms (carry) don't change results

### Test C: Matched Company Outcomes
- CEM matched sample (1,178 treated, 3,369 controls, balanced on industry/vintage/size)
- **Observed cos raise +80% more funding (p<0.001)**
- **+1.0 more rounds (p<0.001)**
- **-27 fewer days between rounds (p=0.005)**
- **+0.49 higher stage reached (p<0.001)**
- Dose-response monotonic: 1→2→3+ observers, all p<0.001
- Exit rate lower (companies still active/growing)

### Test D: Network Structure
- **Event contagion within VC portfolios: +5.3pp co-occurrence, same-industry only (firm-cl p<0.05)**
- Diversified VCs outperform specialized (TVPI 2.23 vs 1.79, vintage FE p<0.001)
- Degree centrality predicts performance but attenuates with vintage FE
- Observer overlap (shared observers): not significant beyond industry effects

---

## Data Sources Inventory

### On WRDS (accessible with harperjung credentials)

| Database | Key Tables Used | What we got |
|----------|----------------|------------|
| **CIQ Professionals** | ciq.ciqprofessional, ciq.ciqperson | Observer records, all positions, VC affiliations |
| **CIQ Companies** | ciq.ciqcompany, ciq.ciqcompanytype, ciq.ciqcompanystatustype | Company details, types, status |
| **CIQ Company Relationships** | **ciq.ciqcompanyrel, ciq.ciqcompanyreltype** | **VC-to-portfolio-company investment links (NEW, not yet pulled for our VCs)** |
| **CIQ Key Developments** | ciq.ciqkeydev, ciq.ciqkeydevtoobjecttoeventtype | 400K events with role types |
| **CIQ Crosswalks** | ciq_common.wrds_cik | CIQ companyid → SEC CIK |
| **WRDS Person Linking** | wrdsapps_plink_boardex_ciq, wrdsapps_plink_trinsider_ciq | CIQ ↔ BoardEx, CIQ ↔ TR person matching |
| **BoardEx** | boardex.na_wrds_dir_profile_emp, boardex.na_wrds_company_names | Director positions with dates (datestartrole/dateendrole) |
| **ISS/RiskMetrics** | risk_directors.rmdirectors | Director positions with dirsince/year_term_ends |
| **CRSP** | crsp_a_stock.dsf (thru 2024), crsp.dsf_v2 (2025) | Daily stock returns 2010-2025 |
| **Compustat** | comp.funda, comp.company | Annual financials, SIC codes |
| **IBES** | ibes.statsumu_epsus | Analyst consensus EPS |
| **Form 4** | tfn.table1, tfn.table2 | Insider trades (31K) and derivatives (32K) |
| **13F** | tfn.s34 | Institutional holdings (124M rows, only 39 of our VCs file) |
| **Preqin** | preqin.preqinfunddetails (75K), preqin.preqinfundperformance (363K), preqin.cashflow (298K), preqin.preqindealsvc (382K), preqin.benchmarks (128K), preqin.preqinbenchmarkspc (158K), preqin.managerdetails (30K), preqin.managerinvestmenttypes (31K), preqin.investorportfolio (124K), preqin.investordetails (19K), preqin.fundterms (13K), preqin.fundhistoricperformance (146K) | Fund details, quarterly performance, cashflows, benchmarks/PME, VC deals, manager characteristics, LP data |

### On Disk (Data/ directory)

| Directory | Key Files | Description |
|-----------|----------|-------------|
| CIQ_Extract/ | 01-08 CSV files | Raw CIQ pulls (observers, directors, companies, events, crosswalks) |
| Panel_B_Outcomes/ | 01-04 | Compustat, CRSP monthly, IBES for observer companies |
| Panel_C_Network/ | 01-06 | Network edges, CRSP daily, industry codes, crosswalks |
| BoardEx/ | 3 files | Crosswalk, positions (with dates), companies |
| ISS/ | 2 files | Crosswalk, positions (with tenure years) |
| Form4/ | 2 files | Trades, derivatives |
| FormD/ | 36 quarterly directories | Raw Form D filings (ISSUERS, RELATEDPERSONS, OFFERING TSVs) |
| FormD_Parsed/ | 2 files | **Confirmed observer-company-date links (277), first Form D dates (117 cos)** |
| Preqin/ | 17 CSV files | Full Preqin data pull (fund details, performance, benchmarks, PME, cashflows, deals, manager details, investment types, LP data, fund terms) |
| Analysis_Ready/ | 26+ files | Pre-built CAR datasets by event type (from original analysis) |
| EDGAR_Extract/ | 3 files | S-1 exhibits, body analysis, EFTS hits |

### WRDS Tables Available But NOT Yet Pulled

| Table | Rows | What it could give us |
|-------|------|----------------------|
| **ciq.ciqcompanyrel** | **14.1M** | **Direct VC → portfolio company investment links (NEXT STEP)** |
| ciq.ciqtransaction | 2.2M | Financial transactions (may have deal details) |
| ciq.ciqpersonbiography | 6.2M | Person bios (could confirm observer roles) |
| ciq.ciqprotoprofunction | 19.9M | Professional function detail |
| ciq.ciqcompanyindustrytree | 95.0M | Detailed industry classification |
| wrdsapps_plink_twoiq_ciq | ? | ISS-to-CIQ person crosswalk (permission denied) |
| iss_directors_global.director_roles | 268K+ | Full ISS director data with dates (permission denied) |

---

## Scripts Created This Session

| Script | Purpose |
|--------|---------|
| regression_preqin_fund_performance.py | Baseline fund performance test (v1) |
| regression_preqin_v2.py | Benchmark-adjusted + cashflow approach |
| regression_preqin_v3_specs.py | Full 6-spec battery |
| regression_preqin_v4_bradley.py | Bradley-style between-quarter + event window |
| regression_preqin_v5_event_study.py | Daily cashflow event study |
| regression_dpi_subset_check.py | DPI result for cashflow vs non-cashflow funds |
| regression_preqin_time_bounded.py | Time-bounded test using PP dates |
| regression_form4_v2_deep.py | Form 4 deep dive (same-ind, returns, full specs) |
| regression_observer_perf_v2.py | Fund performance with GP controls, firm FE |
| regression_company_outcomes_v2.py | Matched sample company outcomes |
| regression_network_structure.py | Network overlap, concentration, contagion |
| parse_formd_observer_links.py | Form D parsing for confirmed links |
| match_preqin_validated.py | Geography-validated Preqin matching |
| sample_attrition_full.py | Full sample attrition table |
| pull_preqin_full.py + pull_preqin_remaining.py | Full 13-table Preqin pull |

---

## Next Steps
1. **Pull ciq.ciqcompanyrel for our VCs** — get confirmed VC-to-portfolio-company investment links
2. **Rebuild Preqin test with triple-confirmed links** — VC invested in company (ciqcompanyrel) + observer placed there (ciqprofessional) + Form D confirms timing
3. **Explore ciq.ciqtransaction** — may have deal-level data with dates
4. Commit all new scripts and data
