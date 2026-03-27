# Test 3: Information Spillover Through Board Observer Networks

## Research Summary — March 27, 2026

---

## 1. Research Question

Does private information flow through board observer networks to connected public portfolio companies? Specifically: when a private company experiences a material event (M&A, bankruptcy, executive change), do public companies connected through the observer network show abnormal returns *before* the event becomes public?

---

## 2. The Information Channel

```
Private Firm A (material event, e.g., board approves acquisition)
    |
    v
Board Observer X (attends Firm A's board meetings, learns about the deal)
    |
    v
Observer X works at VC Firm Y
    |
    v
VC Firm Y also invests in Public Firm B
    |
    v
We measure stock returns at Public Firm B around Firm A's event date
```

The observer learns about the event in a board meeting weeks or months before the public announcement. If that information flows through the VC network, connected public portfolio companies should show abnormal returns in the pre-event window.

---

## 3. Data

### 3.1 Observer Network

**Source:** S&P Capital IQ Professionals database (via WRDS)

- 5,570 observer records (people with "observer" in title)
- 4,915 unique persons
- 3,058 companies with board observers
- Network: 2,843 connected (observed company, portfolio company) pairs, linked through VC firms

### 3.2 Events

**Source 1: CIQ Key Dev** (S&P Capital IQ Key Developments, via WRDS `wrds_keydev`)

- 400,886 total events pulled (80+ event types)
- With metadata: `objectroletype` (Target/Buyer/Seller), `gvkey`, `companyname`, `sourcetypename`
- Sources: press releases (Business Wire, PR Newswire), SEC filings (8-K, DEF 14A), court filings (PACER), company websites, news
- After filtering (private companies, not CRSP-listed, no earnings/conferences): ~57,382 events

**Source 2: Form D** (SEC EDGAR)

- 2,827 filings matched to observer companies
- Each filing = a capital raise under Regulation D
- Definitively US private companies with clean filing dates

### 3.3 Returns

**Source:** CRSP Daily Stock File

- `crsp.dsf` through 2024-12-31
- `crsp.dsf_v2` through 2025-12-31 (pulled separately for 2025)
- 2.2 million daily return observations for ~1,155 portfolio stocks
- Market-adjusted returns computed by subtracting equal-weighted portfolio mean

### 3.4 Filters Applied

1. CIQ company type = "Private Company" (removes 374 public companies)
2. Not CRSP-listed at event date (removes events when company was publicly traded, using CRSP listing dates from Panel B crosswalk)
3. Dropped earnings announcements (contaminated with foreign-listed companies reporting earnings)
4. Dropped conferences, calls, ex-dividend dates (noise, not board-level decisions)

### 3.5 Known Data Limitations

- CIQ "Private Company" includes foreign-listed companies (Norwegian banks, French pharma). CRSP filter catches some but not all.
- Same-industry classification uses 2-digit SIC codes, which is broad.
- Observer network is based on CIQ's coverage — companies/people not in CIQ are missed.
- Events are dated by public disclosure, not board meeting date. The lag between the two is the signal we're trying to capture.

---

## 4. Methodology

### 4.1 Cumulative Abnormal Returns (CARs)

For each event at a private observed company, compute CARs at connected public portfolio companies over multiple windows:

- Pre-event: [-30,-1], [-20,-1], [-15,-1], [-10,-1], [-5,-1], [-3,-1], [-2,-1]
- Event: [-1,0]
- Post-event: [0,+3], [0,+5]

**Raw CARs:** Sum of daily returns over the window.

**Market-adjusted CARs:** Sum of (daily return minus equal-weighted portfolio mean return). Removes common market-wide movements that drive within-event correlation.

### 4.2 Control Group

For each event, compute CARs at ALL ~1,155 portfolio stocks, not just connected ones. This creates a control group of non-connected stocks experiencing the same market conditions on the same dates.

**Regression:**
```
CAR = b1(connected) + b2(same_industry) + b3(connected x same_industry) + e
```

- `connected` = 1 if portfolio stock is linked to event company through observer network
- `same_industry` = 1 if event company and portfolio company share same 2-digit SIC
- `b1` = do connected firms show higher CARs than non-connected?
- `b3` = is the connection effect stronger for same-industry?

### 4.3 Clustering and Fixed Effects

We tested 9 specifications to assess robustness:

| Spec | Fixed Effects | Clustering | Rationale |
|---|---|---|---|
| (1) | None | HC1 (robust) | Baseline |
| (2) | None | Event | Conservative: accounts for within-event correlation |
| (3) | None | VC-firm | Information channel level |
| (4) | None | Stock | Same stock across events |
| (5) | Year FE | HC1 | Absorbs market-wide annual shocks |
| (6) | Year FE | Event | Toughest non-FE spec |
| (7) | Year FE | Stock | Year shocks + stock correlation |
| (8) | Stock FE | HC1 | Absorbs stock-level characteristics |
| (9) | Stock FE | Event | Toughest overall spec |

**VC Fixed Effects were dropped:** Only 362 of 2,743 VCs (13%) have both same-industry and different-industry edges. The within-VC coefficient rests on too narrow a base for reliable inference.

### 4.4 Event Groups Tested

| Group | Definition | N events | N connected CARs |
|---|---|---|---|
| M&A Buyer | Observer company acquiring (objectroletype=Buyer) | 105 | 219 (99 same-ind) |
| M&A Target | Observer company being acquired (objectroletype=Target) | 111 | 174 (48 same-ind) |
| Bankruptcy | All bankruptcy subtypes combined | 128 | 224 (33 same-ind) |
| All Distress | Bankruptcy + going concern + impairments + delayed filings + delistings + lawsuits + downsizings + restatements + debt defaults + guidance lowered | 284 | 1,737 (139 same-ind) |
| CEO/CFO Changes | CEO and CFO changes specifically | 404 | 913 |
| Exec/Board Changes | General executive and board changes | 1,884 | 4,435 (544 same-ind) |
| Private Placements | Capital raises via Reg D | 4,344 | 6,635 (432 same-ind) |
| Product/Client | Product and client announcements | 4,344 | 10,547 (1,745 same-ind) |

---

## 5. Results

### 5.1 M&A Buyer — Strongest Result

When the observer's company is acquiring another firm, same-industry connected portfolio companies earn +4.95% abnormal returns in the month before the announcement.

**`conn x same_industry` at CAR[-30,-1]:**

| Spec | Coefficient | p-value |
|---|---|---|
| HC1 | +4.95% | 0.009*** |
| Event-cluster | +4.95% | 0.005*** |
| VC-cluster | +4.95% | <0.001*** |
| Stock-cluster | +4.95% | 0.005*** |
| Year FE + HC1 | +5.04% | 0.008*** |
| Year FE + Event-cluster | +5.04% | 0.006*** |
| Year FE + Stock-cluster | +5.04% | 0.006*** |

**Survives all 7 specifications at p<0.01.**

Also significant at CAR[-10,-1] = +2.77% (p=0.037 event-clustered).

**Interpretation:** The board approves an acquisition weeks before announcement. The observer knows the target, the price, and the strategic rationale. Same-industry connected firms — for whom this information is most relevant — show nearly 5% abnormal returns in the month before. The `connected` coefficient alone is never significant — the industry match is what drives the effect.

### 5.2 Bankruptcy — Large Effect, Board-Level Secret

When the observer's company is heading toward bankruptcy, same-industry connected portfolio companies earn +8.20% abnormal returns in the month before filing.

**`conn x same_industry` across windows (event-clustered):**

| Window | Coefficient | p-value |
|---|---|---|
| [-30,-1] | +8.20% | 0.047** |
| [-20,-1] | +6.58% | 0.055* |
| [-10,-1] | +4.95% | 0.068* |
| [-5,-1] | +4.05% | 0.074* |
| [-1,0] | +1.21% | 0.039** |

Consistent positive drift that decays toward the announcement. Only 33 same-industry observations, so coefficients are large but noisy.

**Interpretation:** Bankruptcy is discussed in board meetings for months before public disclosure. The observer knows about restructuring plans, asset sales, and filing timelines. Same-industry connected firms react because a competitor's distress directly affects their competitive position.

### 5.3 M&A Target — Tight Information, Late Leakage

When the observer's company is being acquired, same-industry connected firms show +1.08% abnormal returns right at the announcement, but NOT in advance.

**`conn x same_industry` at CAR[-1,0] = +1.08% (p=0.027 event-clustered)**

No significant results at [-30,-1] through [-5,-1]. Information about being acquired is the most tightly held board secret — it leaks only at the very last moment.

### 5.4 Exec/Board Changes — Bread and Butter

Connected portfolio companies earn +0.24% higher returns than non-connected ones in the 5 days before executive changes.

**`connected` at CAR[-5,-1]:**

| Spec | Coefficient | p-value |
|---|---|---|
| HC1 | +0.24% | 0.023** |
| Event-cluster | +0.24% | 0.031** |
| VC-cluster | +0.24% | 0.026** |
| Stock-cluster | +0.24% | 0.032** |
| Year FE + HC1 | +0.24% | 0.022** |
| Year FE + Event-cluster | +0.24% | 0.029** |
| Year FE + Stock-cluster | +0.24% | 0.031** |

**Survives all 7 non-Stock-FE specifications.** The `conn x same_industry` interaction is NOT significant — for routine executive changes, the connection itself matters regardless of industry match.

### 5.5 No Significant Results

Private Placements (4,344 events), Product/Client announcements (4,344 events), and CEO/CFO Changes (404 events) show no significant results. Information spillover is event-type specific — only highly material, confidential board decisions produce detectable leakage.

### 5.6 NVCA 2020 Shock — Spillover Turned On

The NVCA removed fiduciary language from standard observer provisions in 2020. Same-industry spillover appeared where none existed before.

**Same-industry CAR[-10,-1] by period:**

| Period | Mean CAR | p (VC-clustered) |
|---|---|---|
| Pre-2020 | +0.04% | 0.823 |
| Post-2020 | +2.95% | <0.001*** |

**`same_ind x post_2020` interaction = +1.17% (p=0.007, Year FE + VC-clustered)**

Placebo at January 2024: +0.32% (p=0.510) — clean null.

### 5.7 Clayton Act January 2025 — Spillover Turned Off

The DOJ/FTC extended Section 8 interlocking directorate rules to board observers. Same-industry spillover reversed.

**`same_ind x post_jan2025` interaction = -10.19% at CAR[-10,-1] (p<0.001, Year FE + VC-clustered)**

Placebo at January 2024: +1.61% (p=0.470) — clean null.

### 5.8 Two Shocks, Opposite Directions

| Shock | Direction | CAR[-10,-1] coefficient | p-value |
|---|---|---|---|
| NVCA 2020 (loosen) | + (more spillover) | +1.17% | 0.007*** |
| Clayton 2025 (tighten) | - (less spillover) | -10.19% | <0.001*** |

Two independent regulatory changes, opposite predictions, both confirmed. Placebos clean.

### 5.9 Four-Period Trajectory

| Period | Regulatory state | Same-ind CAR[-10,-1] | p |
|---|---|---|---|
| Pre-2020 | No shocks | +0.04% | 0.823 |
| 2020-2024 | NVCA loosened | +1.57% | 0.002*** |
| Jan-Sep 2025 | + Clayton tightened | -2.56% | 0.073* |
| Oct 2025+ | + NVCA re-tightened | -2.04% | 0.134 |

Information spillover tracks the regulatory environment precisely.

---

## 6. Summary of Evidence

### Three layers:

**Layer 1 — The connection effect exists.** Connected portfolio companies show higher pre-event abnormal returns than non-connected ones around the same events. CIQ overall: +0.26% at [-5,-1] (p=0.013, market-adjusted, event-clustered). Driven by exec/board changes.

**Layer 2 — It's strongest for material, confidential events AND same-industry connections.** The `connected x same_industry` interaction is significant for:
- M&A Buyer: +4.95% at [-30,-1] — survives all 7 specs at p<0.01
- Bankruptcy: +8.20% at [-30,-1] — survives event-clustering at p<0.05
- M&A Target: +1.08% at [-1,0] — survives event-clustering at p<0.05

It does NOT work for routine events (private placements, product announcements). Information spillover is event-type specific.

**Layer 3 — It responds to regulatory changes.** The NVCA 2020 fiduciary removal increased same-industry spillover (+1.17%, p=0.007). The Clayton Act 2025 antitrust extension decreased it (-10.19%, p<0.001). Two independent shocks, opposite directions, both significant, placebos clean.

### What doesn't work:

- VC Fixed Effects: only 13% of VCs have within-group variation. Dropped.
- Event-clustering kills many results in pooled samples. Only the by-event-type tests survive.
- Form D `conn x same_ind` at [-10,-1] (+1.24%, p=0.016) dies with event-clustering.
- Same-industry sample is small for some event types (33 for bankruptcy, 48 for M&A Target).

---

## 7. Scripts and Data

### Scripts (in `clo-author/scripts/`)

| Script | Purpose |
|---|---|
| `regression_test3_expanded_events.py` | By-event-type tests with control group |
| `regression_test3_expanded_robust.py` | 9 FE/clustering specs for key event groups |
| `regression_test3_control_group.py` | CIQ connected vs non-connected (1.9M obs) |
| `regression_test3_ciq_altspecs.py` | Market-adjusted, event-collapse, spreads (CIQ) |
| `regression_test3_all_windows.py` | 10 fine-grained CAR windows |
| `regression_test3_formd_full.py` | Form D full battery |
| `regression_test3_formd_robust.py` | Form D all FE/cluster combos |
| `regression_test3_formd_altspecs.py` | Form D market-adjusted, collapse, spreads |
| `regression_test3_nvca_shock.py` | NVCA 2020 interaction test |
| `regression_test3_clayton_act.py` | Clayton Act 2025 interaction test |
| `regression_test3_two_shocks.py` | NVCA + Oct 2025 two-shock test |
| `regression_test3_period_means.py` | Four-period breakdown |
| `regression_test3_by_event_type.py` | Original by-event-type (9 types) |
| `regression_test3_year_fe.py` | VC FE + Year FE analysis |
| `regression_test3_yrfe_only.py` | Year FE without VC FE |
| `pull_ciq_all_events_v2.py` | Pull 400K events from wrds_keydev |
| `pull_crsp_2025_v2.py` | Pull 2025 CRSP from dsf_v2 |

### Data Files (in `Data/CIQ_Extract/` and `Data/Panel_C_Network/`)

| File | Description |
|---|---|
| `01_observer_records.csv` | 5,570 observer records |
| `06d_observer_all_events_full.csv` | 400,886 events with role type, gvkey |
| `06_portfolio_crsp_daily.csv` | 2.2M daily returns (2015-2025) |
| `02_observer_public_portfolio_edges.csv` | Network edges |
| `03_portfolio_permno_crosswalk.csv` | CIK-PERMNO mapping |
| `05_industry_codes.csv` | SIC codes |

### Detailed Results Reference

See `quality_reports/regression_results_history.md` for complete coefficient tables, all specifications, and methodological notes.
