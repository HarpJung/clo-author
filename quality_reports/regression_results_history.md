# Test 3 Regression Results History

## Last updated: 2026-03-27

---

## Overview

Test 3 examines whether private information flows through board observer networks to connected public portfolio companies. The network: Observer sits on private Firm A's board → learns material information → information flows through observer's VC firm → reaches connected public Firm B → shows up as abnormal returns at Firm B before the event becomes public.

**Key terminology:**
- **CAR[-X,-1]**: Cumulative abnormal return from X trading days before the event to 1 day before. Measures pre-event information leakage.
- **connected**: Dummy = 1 if the portfolio company is linked to the event company through the observer network. Control group = all other portfolio stocks.
- **same_industry**: Dummy = 1 if event company and portfolio company share same 2-digit SIC code.
- **conn × same_ind**: Interaction — is the connection effect stronger for same-industry firms?
- **Market-adjusted**: CARs computed using abnormal returns (stock return minus equal-weighted portfolio mean).

**Data sources:**
- Events: CIQ Key Dev (press releases, SEC filings, news) and Form D (SEC capital raise filings)
- Network: CIQ person-level positions linking observers → VC firms → portfolio companies
- Returns: CRSP daily, 2015-2025 (dsf through 2024, dsf_v2 for 2025)

**Filters applied:**
- CIQ company type = "Private Company"
- Not CRSP-listed at event date (removes companies that were public when event occurred)
- Earnings announcements dropped (contaminated with foreign-listed companies)
- Conferences/calls dropped (noise, not board-level decisions)

---

## 1. Event-Type Results (The Headline Findings)

### Source: `regression_test3_expanded_events.py` and `regression_test3_expanded_robust.py`

Used 400K+ events from `wrds_keydev` with `objectroletype` (Target/Buyer/Seller).

### 1A. M&A Buyer — THE STRONGEST RESULT

**Setup:** Observer company is acquiring another company. Board approved the deal before announcement. Observer knows.

**Sample:** 105 events, 219 connected CARs (99 same-industry), ~8,955 total obs with control group.

**`conn × same_ind` at CAR[-30,-1] = +4.95%:**

| Spec | p-value |
|---|---|
| HC1 (no clustering) | 0.009*** |
| Event-cluster | 0.005*** |
| VC-cluster | <0.001*** |
| Stock-cluster | 0.005*** |
| Year FE + HC1 | 0.008*** |
| Year FE + Event-cluster | 0.006*** |
| Year FE + Stock-cluster | 0.006*** |

**Survives ALL 7 specs at p<0.01.** When the observer's company is acquiring another firm, same-industry connected portfolio companies earn +4.95% abnormal returns in the month before announcement vs non-connected and different-industry stocks.

**`conn × same_ind` at CAR[-10,-1] = +2.77%:**

| Spec | p-value |
|---|---|
| HC1 | 0.069* |
| Event-cluster | 0.037** |
| VC-cluster | 0.010** |
| Year FE + Event-cluster | 0.040** |

Survives event-clustering and VC-clustering.

### 1B. Bankruptcy — Large Effect, Marginally Robust

**Setup:** Observer company heading toward bankruptcy. Board discusses restructuring, asset sales, filing. Observer knows months before public disclosure.

**Sample:** 128 events, 224 connected CARs (33 same-industry), ~11,089 total obs.

**`conn × same_ind` at CAR[-30,-1] = +8.20%:**

| Spec | p-value |
|---|---|
| HC1 | 0.057* |
| Event-cluster | 0.047** |
| VC-cluster | <0.001*** |
| Stock-cluster | 0.001*** |
| Year FE + Event-cluster | 0.046** |

Coefficient is larger (+8.20%) but only 33 same-industry obs. Survives event-clustering at p<0.05. VC and stock clustering give p<0.001 (but may overstate significance with so few same-ind obs).

**Pattern across windows (event-clustered):**
- [-30,-1]: +8.20% (p=0.047**)
- [-20,-1]: +6.58% (p=0.055*)
- [-10,-1]: +4.95% (p=0.068*)
- [-5,-1]: +4.05% (p=0.074*)
- [-1,0]: +1.21% (p=0.039**)

Consistent positive pre-event drift that decays toward announcement.

### 1C. M&A Target — Tight Information, Late Leakage

**Setup:** Observer company is being acquired. The most confidential board-level decision.

**Sample:** 111 events, 174 connected CARs (48 same-industry), ~9,016 total obs.

**`conn × same_ind` at CAR[-1,0] = +1.08%:**

| Spec | p-value |
|---|---|
| HC1 | 0.204 |
| Event-cluster | 0.027** |
| VC-cluster | 0.129 |
| Year FE + Event-cluster | 0.025** |

Only significant at [-1,0] — information about being acquired leaks right at the announcement, not weeks in advance. Consistent with acquisition targets being tightly held secrets until the last moment.

### 1D. Exec/Board Changes — The Bread and Butter

**Setup:** Executive or board changes at observer company. General category, largest sample.

**Sample:** 1,884 events, 4,435 connected CARs, ~158,181 total obs.

**`connected` at CAR[-5,-1] = +0.24%:**

| Spec | p-value |
|---|---|
| HC1 | 0.023** |
| Event-cluster | 0.031** |
| VC-cluster | 0.026** |
| Stock-cluster | 0.032** |
| Year FE + HC1 | 0.022** |
| Year FE + Event-cluster | 0.029** |
| Year FE + Stock-cluster | 0.031** |

**Survives all 7 non-Stock-FE specs.** The `connected` dummy (not the interaction) is what matters here — connected firms outperform non-connected by 0.24% in the week before executive changes. Industry match doesn't matter for this event type.

### 1E. No Effect: Private Placements, Product/Client, CEO/CFO (individually)

These event types show no significant results in the control group regression with market-adjusted CARs and event-clustering. Information content is either too low or too noisy for the observer channel to produce detectable spillover.

---

## 2. Connected vs Non-Connected Control Group Test

### Source: `regression_test3_control_group.py` and `regression_test3_ciq_altspecs.py`

For each event, compute CARs at ALL ~1,155 portfolio stocks, flag connected vs non-connected.

### 2A. CIQ Events (2,424 events, ~1.9M total obs)

**Market-adjusted, event-clustered regression:**

| Window | connected | p | same_ind | p | conn × same | p |
|---|---|---|---|---|---|---|
| [-30,-1] | -0.11% | 0.667 | -0.23% | 0.263 | -0.68% | 0.631 |
| [-10,-1] | +0.15% | 0.304 | -0.10% | 0.493 | -0.27% | 0.733 |
| **[-5,-1]** | **+0.26%** | **0.013\*\*** | -0.02% | 0.814 | -0.05% | 0.902 |
| [-3,-1] | -0.01% | 0.930 | +0.02% | 0.853 | +0.51% | 0.238 |
| [-1,0] | -0.07% | 0.442 | -0.10% | 0.154 | +0.01% | 0.973 |

**Key finding:** `connected` at [-5,-1] = +0.26% (p=0.013) survives market-adjustment AND event-clustering. Connected firms outperform non-connected by 0.26% in the 5 days before CIQ press releases.

**Event-level collapse (market-adjusted, HC1):**

| Window | connected | p | same_ind | p |
|---|---|---|---|---|
| [-5,-1] | +0.13% | 0.271 | +0.15%* | 0.067 |
| [-3,-1] | +0.01% | 0.941 | **+0.24%\*\*** | 0.026 |
| [-2,-1] | -0.12% | 0.391 | **+0.20%\*\*** | 0.049 |

`same_industry` significant at event level for [-3,-1] and [-2,-1].

### 2B. Form D Events (962 events, ~784K total obs)

**Market-adjusted, event-clustered:** Nothing significant at any window.

**Event-level collapse (market-adjusted):**

| Window | connected | p |
|---|---|---|
| **[-1,0]** | **+0.35%** | **0.037\*\*** |

Connected firms show +0.35% higher returns right at the Form D filing date.

**Control group regression (Year FE, HC1, N~780K):**

| Window | connected | p | conn × same | p |
|---|---|---|---|---|
| **[-10,-1]** | -0.13% | 0.332 | **+1.24%** | **0.016\*\*** |
| [-30,-1] | -0.41% | 0.229 | +3.38% | 0.003*** |

`conn × same_ind` significant at [-10,-1] with HC1 and Stock FE — but dies with event-clustering.

---

## 3. NVCA 2020 Shock (Fiduciary Language Removal)

### Source: `regression_test3_nvca_shock.py`, `regression_test3_yrfe_only.py`

The NVCA removed fiduciary language from standard observer provisions in 2020. Prediction: spillover increases (observers share more freely).

**Same-industry CARs by period (connected sample, VC-clustered):**

| Period | CAR[-10,-1] | p | CAR[-3,-1] | p |
|---|---|---|---|---|
| Pre-2020 | +0.04% | 0.823 | +0.41% | 0.289 |
| Post-2020 | **+2.95%** | **<0.001\*\*\*** | **+1.70%** | **<0.001\*\*\*** |

**Interaction `same_ind × post_2020` (Year FE + VC-clustered):**

| Window | Coefficient | p | Placebo @Jan2024 |
|---|---|---|---|
| **CAR[-30,-1]** | **+2.86%** | **<0.001\*\*\*** | -0.40% (p=0.630) |
| **CAR[-10,-1]** | **+1.17%** | **0.007\*\*\*** | +0.32% (p=0.510) |
| CAR[-5,-1] | +0.79% | 0.083* | +0.41% (p=0.388) |

Placebo clean at all windows. Effect strongest at [-30,-1] and [-10,-1].

---

## 4. Clayton Act Jan 2025 Shock (DOJ/FTC Antitrust Extension)

### Source: `regression_test3_clayton_act.py`

DOJ/FTC explicitly extended Section 8 interlocking directorate rules to board observers. Prediction: same-industry spillover decreases (observers face antitrust scrutiny).

**Interaction `same_ind × post_jan2025` (Year FE + VC-clustered):**

| Window | Coefficient | p | Placebo @Jan2024 |
|---|---|---|---|
| CAR[-30,-1] | -11.56% | 0.005*** | +2.88% (p=0.237) |
| CAR[-20,-1] | -10.95% | <0.001*** | +5.19% (p=0.033**) |
| **CAR[-10,-1]** | **-10.19%** | **<0.001\*\*\*** | +1.61% (p=0.470) |
| CAR[-5,-1] | -1.94% | 0.088* | +0.41% (p=0.388) |
| CAR[-3,-1] | -2.49% | 0.007*** | +0.32% (p=0.510) |
| CAR[-2,-1] | -1.68% | 0.004*** | — |

Note: Clayton post-Jan2025 same-industry N is very small (~7 obs for means test). Interaction is identified from the full regression, not subsample means. Placebo at [-20,-1] is contaminated (p=0.033).

---

## 5. Two Shocks Side by Side

### Source: `regression_test3_clayton_act.py`

| Window | NVCA 2020 (expect +) | p | Clayton Jan25 (expect -) | p |
|---|---|---|---|---|
| CAR[-30,-1] | +2.05% | 0.246 | **-11.56%** | **0.005\*\*\*** |
| CAR[-20,-1] | +1.15% | 0.438 | **-10.95%** | **<0.001\*\*\*** |
| **CAR[-10,-1]** | **+3.46%** | **<0.001\*\*\*** | **-10.19%** | **<0.001\*\*\*** |
| **CAR[-5,-1]** | **+1.67%** | **0.040\*\*** | -1.94% | 0.088* |
| CAR[-3,-1] | -0.28% | 0.702 | **-2.49%** | **0.007\*\*\*** |

**CAR[-10,-1] is CONSISTENT:** NVCA positive (p=0.007), Clayton negative (p<0.001).

---

## 6. Four-Period Trajectory (Same-Industry CAR[-10,-1])

### Source: `regression_test3_period_means.py`

| Period | Regulatory state | Mean CAR | p (VC-cl) |
|---|---|---|---|
| Pre-2020 | Baseline | +0.04% | 0.823 |
| 2020-Dec 2024 | NVCA loosened | **+1.57%** | **0.002\*\*\*** |
| Jan-Sep 2025 | + Clayton tightened | **-2.56%** | **0.073\*** |
| Oct 2025+ | + NVCA re-tightened | -2.04% | 0.134 |

---

## 7. Form D as Alternative Event Source

### Source: `regression_test3_formd_full.py`, `regression_test3_formd_robust.py`, `regression_test3_formd_altspecs.py`

Form D = SEC capital raise filings. Definitively US private companies, clean dates, single event type.

**Connected means (VC-clustered, N~8,300):**

| Window | Overall | Same-ind (N~786) | Diff-ind |
|---|---|---|---|
| [-30,-1] | +3.42%*** | +3.08%*** | +3.46%*** |
| [-10,-1] | +0.36%** | **+1.27%\*\*\*** | +0.27%* |
| [-1,0] | +0.02% | **+0.85%\*\*\*** | -0.05% |
| [0,+5] | +0.24%** | +0.78%* | +0.19%* |

**Connected-only `same_industry` at CAR[-10,-1]:**

| Spec | p-value |
|---|---|
| HC1 | 0.054* |
| VC-cluster | 0.011** |
| Event-cluster | 0.447 |
| Market-adj + VC-cluster | 0.012** |
| Market-adj + Event-cluster | 0.355 |

Survives VC-clustering (p=0.011) and market-adjusted VC-clustering (p=0.012). Dies with event-clustering.

---

## 8. Methodological Notes

### Clustering
- **VC-firm clustering (~1,100-1,200 clusters):** Appropriate level for information flow story. Most results survive.
- **Event-clustering (~100-2,400 clusters depending on sample):** Most conservative. Accounts for within-event correlation. Only the strongest results survive (M&A Buyer, Bankruptcy, Exec/Board connected).
- **Stock-clustering (~130-650 clusters):** Controls for same stock appearing across events. Results similar to VC-clustering.
- **Two-way clustering:** Not implemented (Cameron-Gelbach-Miller approach attempted but numerical issues).

### Fixed Effects
- **Year FE:** Barely changes any results. Market-wide annual shocks not driving findings.
- **VC FE:** Dropped — only 13% of VCs (362/2,743) have both same and diff industry edges. Too thin for reliable within-VC estimation.
- **Stock FE:** Absorbs stock-level characteristics. Some results survive (bankruptcy), some don't (exec/board connected).

### Market Adjustment
- Equal-weighted portfolio mean return subtracted from each stock's daily return.
- Reduces within-event correlation (common market factor removed).
- Most results attenuate but key ones survive (M&A Buyer, CIQ connected at [-5,-1]).

### Sample Issues
- CIQ "Private Company" includes foreign-listed companies (Norwegian banks, French pharma, etc.)
- Earnings announcements contaminated — dropped from main analysis
- CRSP-listing filter removes events when company was publicly traded
- Same-industry sample is small (~30-800 depending on event type and sample)

---

## 9. Scripts Reference

| Script | What it does |
|---|---|
| `regression_test3_expanded_events.py` | 9 event groups, connected means + control group |
| `regression_test3_expanded_robust.py` | 4 key groups × 9 FE/cluster specs |
| `regression_test3_all_windows.py` | 10 fine-grained windows, CIQ events |
| `regression_test3_control_group.py` | CIQ connected vs non-connected (1.9M obs) |
| `regression_test3_ciq_altspecs.py` | CIQ market-adjusted, event-collapse, spreads |
| `regression_test3_by_event_type.py` | CIQ by event type (original 9 types) |
| `regression_test3_formd_full.py` | Form D full battery |
| `regression_test3_formd_robust.py` | Form D all FE/cluster combos |
| `regression_test3_formd_altspecs.py` | Form D market-adjusted, collapse, spreads |
| `regression_test3_nvca_shock.py` | NVCA 2020 interaction (full, ex-COVID, placebo) |
| `regression_test3_clayton_act.py` | Clayton Act Jan 2025 interaction |
| `regression_test3_two_shocks.py` | NVCA 2020 + NVCA Oct 2025 |
| `regression_test3_year_fe.py` | VC FE + Year FE double demeaning |
| `regression_test3_yrfe_only.py` | Year FE without VC FE |
| `regression_test3_period_means.py` | 4-period means + year-by-year |
| `regression_test3_period_breakdown.py` | VC FE coef by period |
| `regression_test3_multiwindow.py` | Original multi-window ([-60] to [0,+5]) |
| `regression_test3_fe.py` | VC FE 5 specs |
| `regression_test3_subsample_fe.py` | Subsample means with FE |
| `regression_test3_daily.py` | Daily CARs, initial VC clustering |
| `regression_test3_private_events.py` | Private events only |
| `regression_test3_formd_events.py` | Form D initial test |
| `pull_ciq_all_events_v2.py` | 400K events from wrds_keydev |
| `pull_crsp_2025_v2.py` | 2025 CRSP from dsf_v2 |

---

## 10. Data Files

| File | Description |
|---|---|
| `06_observer_company_key_events.csv` | Original 61,669 events (9 types) |
| `06b_observer_expanded_events.csv` | 118,626 events (45 types) |
| `06c_observer_all_events.csv` | 439,143 events (all types, with category) |
| `06d_observer_all_events_full.csv` | 400,886 events (wrds_keydev, with role type, gvkey) |
| `06_portfolio_crsp_daily.csv` | 2.2M rows, 2015-2025 |
| `06b_portfolio_crsp_daily_2025.csv` | 244K rows, 2025 only (from dsf_v2) |
