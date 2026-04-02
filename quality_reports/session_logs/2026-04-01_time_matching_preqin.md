# Session Log: 2026-04-01 — Time-Matching, Expanded Sample, Preqin Integration

## Goal
Re-examine the information spillover results with properly time-matched observer network connections, expand the sample, and explore VC fund-level performance as an alternative outcome.

## Key Findings

### 1. Time-Matching Reveals Stale Connection Problem
- **73% of all "connected" observations in the original regressions were STALE** — the observer had left the public company before the event occurred
- Only ~15% of connections were confirmed active at the event date
- The original M&A Buyer result (+4.6%, p=0.001) was estimated from 28 same-industry observations, of which ~22 were stale
- When restricted to active-only connections, the M&A Buyer result had only 6 same-industry obs and was not significant

### 2. Time-Matched Results Are Null
- Pooled all-events regression with active-only person network: **null across all 84 specifications** (6 specs x 7 windows x 2 tests)
- NVCA 2020 DiD interaction (SameInd x Post2020): **null across all 42 specifications**
- Connected coefficient is actually **negative** at CAR[-30,-1] (~-1.1%, significant across all specs)
- The VC-level network (connections through any VC partner) also shows null results

### 3. US-Only Filter Applied
- 33.5% of observed companies were non-US (Norway, France, UK, etc.)
- Non-US observers include European labor representatives (mandatory governance roles, not VC observers)
- Restricted to US-only: 2,035 companies, 4,770 network edges, 1,113 observers

### 4. Expanded Sample (2010-2025)
- Extended from 2015-2025 to 2010-2025 by pulling CRSP daily returns for 2010-2014
- Added ISS/RiskMetrics director data (84 new observers with dated positions)
- Pre-2020 same-industry connected obs increased from 57 to **384**
- Results remain null with the expanded, well-powered sample

### 5. Preqin Integration for VC Fund Performance
- Matched 3,647 CIQ VCs to Preqin managers (exact name + substring with geography validation)
- Identified false positives in substring matches (Insight→FinSight, Polaris→Venture Partners Zurich)
- Decision: use only high+medium quality matches (1,612 VCs)
- Available: 1,140 VC funds with IRR data at 478 matched firms
- Full Preqin data pulled: 13 tables, 75K funds, 363K performance records, 128K benchmarks, 158K PME records, 298K cashflows, 382K VC deals

## Scripts Created
- `01_pull_all_wrds_data.py` — Consolidated WRDS pull (replaces 7 scripts)
- `02_build_network.py` — Consolidated network builder (replaces 3 scripts)
- `03_prepare_analysis.py` — Consolidated analysis prep
- `pull_ciq_raw_data.py` — Reconstruction script for raw CIQ files
- `pull_crsp_2010_2014.py` — CRSP daily returns 2010-2014
- `pull_iss_supplement.py` — ISS director matching
- `pull_preqin_full.py` / `pull_preqin_remaining.py` — Full Preqin data pull
- `match_preqin_validated.py` — Geography-validated Preqin matching
- `diagnose_stale_connections.py` — Active vs stale connection diagnostic
- `regression_two_networks.py` — Person-level vs VC-level comparison
- `regression_pooled_compare.py` — Always-on vs active-only
- `regression_pooled_did.py` — NVCA/Clayton DiD with all windows
- `regression_pooled_full_specs.py` — Full 6-spec battery
- `regression_expanded_sample.py` — 2010-2025 with ISS

## Data Files Created
- `Panel_C_Network/02b_supplemented_network_edges_us.csv` — US-only network
- `Panel_C_Network/02_observer_public_portfolio_edges_us.csv` — US-only original
- `Panel_C_Network/03_observer_vc_public_network.csv` — Three-way network
- `Panel_C_Network/06c_portfolio_crsp_daily_2010_2014.csv` — Extended returns
- `ISS/observer_iss_crosswalk.csv` — CIQ-to-ISS person matching
- `ISS/observer_iss_positions.csv` — ISS director positions
- `Preqin/` — 13 CSV files (fund details, performance, benchmarks, etc.)

## Paper Versions
- `Paper/` — Full proposal (double-spaced, 24pp, Chicago style)
- `Paper_Short/` — Short proposal (single-spaced, 9pp)
- `Paper2/` — Revised full proposal incorporating review feedback (29pp)
- `Paper_Short2/` — Revised short proposal (12pp)

## Decisions Made
- Limited sample to US-only (institutional framing is US-specific)
- Use high+medium quality Preqin matches only (drop substring "low" quality)
- Time-matching uses BoardEx dates (primary), ISS dates (supplement), CIQ current/former (fallback)
- Original CAR results are acknowledged as driven by stale connections
- Pivoting to explore VC fund-level performance as alternative outcome

## Open Questions / Next Steps
- Run fund performance test: do VC funds with observers at same-industry companies show better quarterly IRR changes after events?
- Consider private-to-private event contagion within VC portfolios
- Reframe paper: institutional contribution + methods cautionary tale + fund performance test
- Need to decide: is this a null result paper or do we pivot the research question?
