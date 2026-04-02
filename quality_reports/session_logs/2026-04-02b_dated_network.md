# Session Log: 2026-04-02 (continued) — Dated Observer Network

## What We Built

Parsed 8,535 CIQ transaction comments to extract investor names, matched to our VC firms, and cross-referenced with the observer database to build a **triple-confirmed, dated observer network**.

### Data Chain
```
ciq.ciqtransaction comments: "NEA invested in Puppet's Series C on 2019-05-29"
  → Regex parsing extracts "NEA" as investor
  → Name-matched to CIQ VC firm "New Enterprise Associates, Inc."
  → Cross-ref: Person X works at NEA + observes at Puppet
  → OUTPUT: Person X placed at Puppet ~2019-05-29 via NEA's Round 5
```

### Results
- 13,296 investor mentions extracted from 4,225 transactions
- 6,062 matched to CIQ VC firms (2,677 exact, 3,385 substring)
- **878 dated observer-company links** (625 observers, 458 VCs, 553 companies)
- 857 (97.6%) have exact investment dates
- 626 VC-company pairs confirmed by ALL THREE sources (inferred + ciqcompanyrel + transaction comments)

### Network Approach Comparison
| Approach | VC-company pairs | Dated? |
|----------|-----------------|--------|
| Original inferred (table_b) | 8,983 | No |
| ciqcompanyrel confirmed | 2,592 | No (current/prior flag only) |
| Transaction-comment confirmed | 675 | **Yes (exact date)** |
| All three combined | 626 | **Yes** |

## Complete Data Source Inventory

### For identifying observers
- `ciq.ciqprofessional`: title LIKE '%observer%' AND boardflag=1 → 5,570 records, 4,915 persons
- `ciq.ciqprotoprofunction`: position function start/end dates → 25.9% have start year for observer roles
- `ciq.ciqperson`: person names (firstname, lastname, yearborn)

### For identifying VC investments
- `ciq.ciqcompanyrel`: 327K confirmed VC investment links (type 1=Current, 2=Prior)
- `ciq.ciqtransaction`: 9,849 deals with dates, round numbers, amounts, investor names in comments
- Form D RELATEDPERSONS: 277 confirmed observer-company-date links from SEC filings

### For public company connections
- `ciq.ciqprofessional`: observer positions at public companies (97% Former, 3% Current)
- `boardex.na_wrds_dir_profile_emp`: exact start/end dates for 707 observers (63.5%)
- `risk_directors.rmdirectors`: ISS positions with dirsince/year_term_ends for 321 observers
- `tfn.table1/table2`: Form 4 insider trades at public companies (14K trades)

### For events
- `ciq_keydev.wrds_keydev`: 400K events with types, dates, role types, gvkey
- Form D filings: 474K issuers with filing dates

### For fund performance
- `preqin.preqinfunddetails`: 75K funds
- `preqin.preqinfundperformance`: 363K quarterly records
- `preqin.cashflow`: 298K capital call/distribution records
- `preqin.benchmarks` / `preqin.preqinbenchmarkspc`: benchmark PMEs

### For company outcomes
- `preqin.preqindealsvc`: 382K VC deals with dates, sizes, stages, industries

## All Tests Run and Results

### Test 1: CARs at Connected Public Firms → **NULL**
- Time-matched network: null across 84+ specs
- Stale connections drove original results

### Test 2: Preqin Fund Performance → **NULL with confirmed links**
- Inferred links: DPI significant but driven by 80% false connections
- Triple-confirmed: DPI null, TVPI × Post2020 marginal (p=0.057)

### Test 3: Form 4 Insider Trading → **NULL (compliance behavior)**
- Equal 30-day windows: flat trading rate
- 5-day windows: **buys decrease** in [-10,-1] (observers stop buying before events)
- Consistent with compliance/blackout behavior, not informed trading
- Window-length artifact in v2 produced spurious 3.21x finding

### Test 4: Company Outcomes → **ROBUST**
- CEM matched: +97% funding, +1.6 rounds (p<0.001 across 7 specs)
- Confirmed investment subsample: strengthens
- Within-company (Company FE): +86% larger deals after observer (p<0.001)
- NVCA 2020 DiD: observer premium SHRANK after 2020 (opposite to prediction)

### Test 5: Network Structure
- Same-industry event contagion: +9.8pp (firm-cl p<0.05)
- Diversified VCs outperform specialized

## Scripts Created
- parse_transaction_investors.py — parse comments, build dated network
- regression_form4_v5_equal_windows.py — fixed window-length artifact
- regression_form4_v6_timed_equal.py — time-matched + equal windows + full specs
- regression_form4_v7_5day_windows.py — 5-day granular event study
- regression_company_outcomes_v3_full_specs.py — 7 specs + confirmed subsample
- regression_company_outcomes_v4_timed.py — before vs after observer arrival
- regression_company_outcomes_v5_nvca_did.py — NVCA/Clayton DiD on deal sizes
- check_form4_date_coverage.py — date coverage diagnostic
- explore_ciq_transactions.py — transaction data exploration

## Next Steps
- Re-run all tests using the dated observer network (878 links with exact dates)
- Focus on company outcomes with dated network (the most robust finding)
- Consider paper structure given honest results
