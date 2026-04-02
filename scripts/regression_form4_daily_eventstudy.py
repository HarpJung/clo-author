#!/usr/bin/env python3
"""
Form 4 Daily Event Study
========================
For each material event at a private observed company, examine whether the
board observer trades more at the *public* company where they are a director
in the days BEFORE the event becomes public.

Windows around event date:
  [-180,-91]  baseline
  [-90,-31]   pre-event wide
  [-30,-1]    pre-event narrow
  [0,+30]     post-event narrow
  [+31,+90]   post-event wide

Output: descriptive statistics, OLS regressions (HC1 + event-clustered SEs),
        profitability test, and pre-2020 vs post-2020 NVCA DiD.
"""

import sys, os, warnings
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.iolib.summary2 import summary_col

# ── paths ────────────────────────────────────────────────────────────────
BASE   = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data"
FORM4  = os.path.join(BASE, "Form4", "observer_form4_trades.csv")
XWALK  = os.path.join(BASE, "CIQ_Extract", "08_observer_tr_insider_crosswalk.csv")
EVENTS = os.path.join(BASE, "CIQ_Extract", "06d_observer_all_events_full.csv")
OBSREC = os.path.join(BASE, "CIQ_Extract", "01_observer_records.csv")
CODET  = os.path.join(BASE, "CIQ_Extract", "04_observer_company_details.csv")
NETWORK = os.path.join(BASE, "table_b_observer_network.csv")

OUT_DIR = r"C:\Users\hjung\Documents\Claude\CorpAcct\clo-author\quality_reports"
os.makedirs(OUT_DIR, exist_ok=True)

# ── event types to DROP (noise / recurring / non-material) ───────────────
NOISE_EVENTS = {
    "Conferences",
    "Company Conference Presentations",
    "Announcements of Earnings",
    "Earnings Release Date",
    "Earnings Calls",
    "Estimated Earnings Release Date (S&P Global Derived)",
    "Annual General Meeting",
    "Board Meeting",
    "Index Constituent Adds",
    "Index Constituent Drops",
    "Shareholder/Analyst Calls",
    "Special/Extraordinary Shareholders Meeting",
    "Analyst/Investor Day",
    "Delayed Earnings Announcements",
    "Sales/Trading Statement Release Date",
    "Sales/Trading Statement Calls",
    "Interim Management Statement Release Date",
    "Interim Management Statement Calls",
    "Announcement of Interim Management Statement",
    "Operating Results Release Date",
    "Operating Results Calls",
    "Guidance/Update Calls",
    "Fixed Income Calls",
    "M&A Calls",
    "Address Changes",
    "Ticker Changes",
    "Name Changes",
    "Exchange Changes",
    "Fiscal Year End Changes",
    "Legal Structure Changes",
    "Corporate Guidance - New/Confirmed",
    "Corporate Guidance - Raised",
    "Corporate Guidance - Lowered",
    "Announcements of Sales/Trading Statement",
    "Announcement of Operating Results",
    "Delayed SEC Filings",
}

print("=" * 80)
print("FORM 4 DAILY EVENT STUDY")
print("=" * 80)

# ══════════════════════════════════════════════════════════════════════════
# 1. LOAD DATA
# ══════════════════════════════════════════════════════════════════════════
print("\n[1] Loading data ...")

# -- Form 4 trades (keep only open-market purchases P and sales S) --------
f4 = pd.read_csv(FORM4, low_memory=False)
print(f"    Form 4 raw rows: {len(f4):,}")
# Keep P (purchase) and S (sale) only
f4 = f4[f4["trancode"].isin(["P", "S"])].copy()
f4["trandate"] = pd.to_datetime(f4["trandate"], errors="coerce")
f4 = f4.dropna(subset=["trandate", "personid"])
f4["personid"] = f4["personid"].astype(int)
print(f"    Form 4 after P/S filter: {len(f4):,}")
print(f"    Unique TR personids in Form 4: {f4['personid'].nunique():,}")

# -- Crosswalk TR -> CIQ -------------------------------------------------
xw = pd.read_csv(XWALK)
xw = xw[["tr_personid", "ciq_personid"]].dropna().drop_duplicates()
xw["tr_personid"]  = xw["tr_personid"].astype(int)
xw["ciq_personid"] = xw["ciq_personid"].astype(int)
print(f"    Crosswalk pairs: {len(xw):,}")

# Merge Form 4 with crosswalk
f4 = f4.merge(xw, left_on="personid", right_on="tr_personid", how="inner")
print(f"    Form 4 after crosswalk merge: {len(f4):,}  ({f4['ciq_personid'].nunique()} CIQ persons)")

# -- Observer network (tells us which private co each person observes) ----
net = pd.read_csv(NETWORK)
# Keep unique observer -> observed_company links
obs_links = net[["observer_personid", "observed_companyid"]].drop_duplicates()
obs_links.columns = ["ciq_personid", "observed_companyid"]
obs_links["ciq_personid"] = obs_links["ciq_personid"].astype(int)
obs_links["observed_companyid"] = obs_links["observed_companyid"].astype(int)
print(f"    Observer links: {len(obs_links):,}")

# -- Company details (to filter US private) --------------------------------
codet = pd.read_csv(CODET)
us_private = codet.loc[
    (codet["country"] == "United States") &
    (codet["companytypename"] == "Private Company"),
    "companyid"
].unique()
print(f"    US private companies: {len(us_private):,}")

# -- Material events (filter) ---------------------------------------------
ev = pd.read_csv(EVENTS, low_memory=False)
print(f"    Events raw rows: {len(ev):,}")
ev["announcedate"] = pd.to_datetime(ev["announcedate"], errors="coerce")
ev = ev.dropna(subset=["announcedate", "companyid"])
# Filter: US private, 2010+, drop noise
ev = ev[ev["companyid"].astype(int).isin(us_private)]
ev = ev[ev["announcedate"].dt.year >= 2010]
ev = ev[~ev["eventtype"].isin(NOISE_EVENTS)]
# Keep target events (objectroletype = Target typically role 1)
ev = ev[ev["keydevtoobjectroletypeid"] == 1]
ev = ev[["keydevid", "companyid", "companyname", "eventtype",
         "keydeveventtypeid", "announcedate"]].drop_duplicates()
ev["companyid"] = ev["companyid"].astype(int)
print(f"    Events after filters: {len(ev):,}")
print(f"    Unique event companies: {ev['companyid'].nunique():,}")
print(f"    Event types retained:")
for et, cnt in ev["eventtype"].value_counts().head(20).items():
    print(f"      {et:55s} {cnt:>6,}")

# ══════════════════════════════════════════════════════════════════════════
# 2. BUILD EVENT-OBSERVER-TRADE PANEL
# ══════════════════════════════════════════════════════════════════════════
print("\n[2] Building event-observer-trade panel ...")

# Link events to observers via observer network
ev_obs = ev.merge(obs_links, left_on="companyid", right_on="observed_companyid",
                  how="inner")
print(f"    Event-observer pairs (event x observer): {len(ev_obs):,}")
print(f"    Unique observers with events: {ev_obs['ciq_personid'].nunique():,}")

# For each event-observer pair, find the observer's Form 4 trades in [-180, +90]
# We do this via a merge on ciq_personid, then compute day gap
panel = ev_obs.merge(
    f4[["ciq_personid", "trandate", "trancode", "ticker", "cname",
        "shares", "tprice", "shares_adj", "tprice_adj"]],
    on="ciq_personid",
    how="inner"
)
panel["day_gap"] = (panel["trandate"] - panel["announcedate"]).dt.days
# Keep only trades within [-180, +90]
panel = panel[(panel["day_gap"] >= -180) & (panel["day_gap"] <= 90)].copy()
print(f"    Trades in [-180, +90] window: {len(panel):,}")

# Assign window bins
def assign_window(d):
    if -180 <= d <= -91:
        return "baseline_180_91"
    elif -90 <= d <= -31:
        return "pre_90_31"
    elif -30 <= d <= -1:
        return "pre_30_1"
    elif 0 <= d <= 30:
        return "post_0_30"
    elif 31 <= d <= 90:
        return "post_31_90"
    return None

panel["window"] = panel["day_gap"].apply(assign_window)
panel = panel.dropna(subset=["window"])

# Create indicators
panel["is_buy"]  = (panel["trancode"] == "P").astype(int)
panel["is_sell"] = (panel["trancode"] == "S").astype(int)
panel["trade"]   = 1

# Event identifier for clustering
panel["event_id"] = panel["keydevid"].astype(str) + "_" + panel["ciq_personid"].astype(str)

print(f"    Panel rows: {len(panel):,}")
print(f"    Unique events: {panel['keydevid'].nunique():,}")
print(f"    Unique observers: {panel['ciq_personid'].nunique():,}")
print(f"    Unique event-observer pairs: {panel['event_id'].nunique():,}")

# ══════════════════════════════════════════════════════════════════════════
# 3. DESCRIPTIVE: TRADING INTENSITY BY WINDOW
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[3] DESCRIPTIVE STATISTICS: Trading intensity by window")
print("=" * 80)

window_days = {
    "baseline_180_91": 90,
    "pre_90_31":       60,
    "pre_30_1":        30,
    "post_0_30":       31,
    "post_31_90":      60,
}
window_order = ["baseline_180_91", "pre_90_31", "pre_30_1", "post_0_30", "post_31_90"]
window_labels = {
    "baseline_180_91": "[-180,-91]",
    "pre_90_31":       "[-90,-31]",
    "pre_30_1":        "[-30,-1]",
    "post_0_30":       "[0,+30]",
    "post_31_90":      "[+31,+90]",
}

# 3a. Count trades per window
agg = panel.groupby("window").agg(
    n_trades = ("trade", "sum"),
    n_buys   = ("is_buy", "sum"),
    n_sells  = ("is_sell", "sum"),
    n_events = ("event_id", "nunique"),
).reindex(window_order)

agg["days_in_window"] = [window_days[w] for w in window_order]
agg["trades_per_event_day"] = agg["n_trades"] / (agg["n_events"] * agg["days_in_window"])
agg["buys_per_event_day"]   = agg["n_buys"]   / (agg["n_events"] * agg["days_in_window"])
agg["sells_per_event_day"]  = agg["n_sells"]  / (agg["n_events"] * agg["days_in_window"])
agg["buy_share"]            = agg["n_buys"] / agg["n_trades"]

print("\nPanel A: Trading Activity by Window")
print("-" * 100)
header = f"{'Window':>15s} {'Trades':>8s} {'Buys':>8s} {'Sells':>8s} {'Events':>8s} {'Trades/EvDay':>14s} {'Buys/EvDay':>12s} {'Sells/EvDay':>13s} {'BuyShare':>10s}"
print(header)
print("-" * 100)
for w in window_order:
    r = agg.loc[w]
    print(f"{window_labels[w]:>15s} {int(r['n_trades']):>8,} {int(r['n_buys']):>8,} "
          f"{int(r['n_sells']):>8,} {int(r['n_events']):>8,} "
          f"{r['trades_per_event_day']:>14.6f} {r['buys_per_event_day']:>12.6f} "
          f"{r['sells_per_event_day']:>13.6f} {r['buy_share']:>10.4f}")

# 3b. Normalize to baseline
print("\nPanel B: Normalized to Baseline [-180,-91] = 1.00")
print("-" * 60)
base_trade = agg.loc["baseline_180_91", "trades_per_event_day"]
base_buy   = agg.loc["baseline_180_91", "buys_per_event_day"]
base_sell  = agg.loc["baseline_180_91", "sells_per_event_day"]
print(f"{'Window':>15s} {'Trade Ratio':>12s} {'Buy Ratio':>12s} {'Sell Ratio':>12s}")
print("-" * 60)
for w in window_order:
    r = agg.loc[w]
    tr = r["trades_per_event_day"] / base_trade if base_trade > 0 else np.nan
    br = r["buys_per_event_day"]   / base_buy   if base_buy   > 0 else np.nan
    sr = r["sells_per_event_day"]  / base_sell   if base_sell  > 0 else np.nan
    print(f"{window_labels[w]:>15s} {tr:>12.4f} {br:>12.4f} {sr:>12.4f}")

# ══════════════════════════════════════════════════════════════════════════
# 4. BUILD DAILY-LEVEL PANEL FOR REGRESSIONS
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[4] Building daily-level regression panel ...")
print("=" * 80)

# For each event-observer pair, create a row for each day in [-180, +90]
# with trade_indicator, buy_indicator, sell_indicator

event_obs_pairs = panel[["keydevid", "ciq_personid", "event_id", "announcedate",
                          "companyid"]].drop_duplicates(subset=["event_id"])
print(f"    Event-observer pairs to expand: {len(event_obs_pairs):,}")

# Create day range
days = np.arange(-180, 91)  # -180 to +90 inclusive
daily_rows = []
for _, row in event_obs_pairs.iterrows():
    for d in days:
        daily_rows.append({
            "event_id": row["event_id"],
            "keydevid": row["keydevid"],
            "ciq_personid": row["ciq_personid"],
            "announcedate": row["announcedate"],
            "companyid": row["companyid"],
            "day_gap": d,
        })

daily = pd.DataFrame(daily_rows)
print(f"    Daily panel rows (before trade merge): {len(daily):,}")

# Count trades per event-observer-day
trade_daily = panel.groupby(["event_id", "day_gap"]).agg(
    n_trades = ("trade", "sum"),
    n_buys   = ("is_buy", "sum"),
    n_sells  = ("is_sell", "sum"),
).reset_index()

daily = daily.merge(trade_daily, on=["event_id", "day_gap"], how="left")
daily["n_trades"] = daily["n_trades"].fillna(0).astype(int)
daily["n_buys"]   = daily["n_buys"].fillna(0).astype(int)
daily["n_sells"]  = daily["n_sells"].fillna(0).astype(int)

# Indicators
daily["trade_ind"] = (daily["n_trades"] > 0).astype(int)
daily["buy_ind"]   = (daily["n_buys"]   > 0).astype(int)
daily["sell_ind"]  = (daily["n_sells"]  > 0).astype(int)

# Window dummies (baseline = [-180,-91])
daily["window"] = daily["day_gap"].apply(assign_window)
daily = daily.dropna(subset=["window"])
daily["pre_90"]  = (daily["window"] == "pre_90_31").astype(int)
daily["pre_30"]  = (daily["window"] == "pre_30_1").astype(int)
daily["post_30"] = (daily["window"] == "post_0_30").astype(int)
daily["post_90"] = (daily["window"] == "post_31_90").astype(int)

# Year for NVCA split
daily["event_year"] = pd.to_datetime(daily["announcedate"]).dt.year
daily["post_2020"]  = (daily["event_year"] >= 2020).astype(int)

print(f"    Daily panel final rows: {len(daily):,}")
print(f"    Mean trade_ind: {daily['trade_ind'].mean():.6f}")
print(f"    Mean buy_ind:   {daily['buy_ind'].mean():.6f}")
print(f"    Mean sell_ind:  {daily['sell_ind'].mean():.6f}")

# ══════════════════════════════════════════════════════════════════════════
# 5. REGRESSIONS
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[5] REGRESSIONS")
print("=" * 80)

def run_ols_with_cluster(y, X, cluster_var, label):
    """Run OLS with HC1 and event-clustered SEs, print both."""
    model = sm.OLS(y, sm.add_constant(X)).fit(cov_type="HC1")
    # Clustered
    try:
        model_cl = sm.OLS(y, sm.add_constant(X)).fit(
            cov_type="cluster", cov_kwds={"groups": cluster_var}
        )
    except Exception as e:
        model_cl = None
        print(f"    [Warning] Cluster SE failed: {e}")

    print(f"\n--- {label} ---")
    print(f"    N = {int(model.nobs):,}   R-sq = {model.rsquared:.6f}")
    print(f"    Baseline mean (const): {model.params['const']:.6f}")
    print()
    print(f"    {'Variable':>15s} {'Coef':>12s} {'HC1 SE':>12s} {'HC1 t':>10s} {'HC1 p':>10s}", end="")
    if model_cl is not None:
        print(f" {'Cl SE':>12s} {'Cl t':>10s} {'Cl p':>10s}", end="")
    print()
    print("    " + "-" * 95)

    for var in X.columns:
        coef = model.params[var]
        se1  = model.bse[var]
        t1   = model.tvalues[var]
        p1   = model.pvalues[var]
        stars1 = "***" if p1 < 0.01 else "**" if p1 < 0.05 else "*" if p1 < 0.10 else ""
        line = f"    {var:>15s} {coef:>12.6f} {se1:>12.6f} {t1:>10.3f} {p1:>10.4f}{stars1:>4s}"
        if model_cl is not None:
            se2 = model_cl.bse[var]
            t2  = model_cl.tvalues[var]
            p2  = model_cl.pvalues[var]
            stars2 = "***" if p2 < 0.01 else "**" if p2 < 0.05 else "*" if p2 < 0.10 else ""
            line += f" {se2:>12.6f} {t2:>10.3f} {p2:>10.4f}{stars2:>4s}"
        print(line)

    return model, model_cl

# ── 5a. Trade indicator ──────────────────────────────────────────────────
X_cols = ["pre_90", "pre_30", "post_30", "post_90"]

m1_hc, m1_cl = run_ols_with_cluster(
    daily["trade_ind"], daily[X_cols], daily["event_id"],
    "Any Trade Indicator ~ Window Dummies (baseline = [-180,-91])"
)

# ── 5b. Buy indicator ───────────────────────────────────────────────────
m2_hc, m2_cl = run_ols_with_cluster(
    daily["buy_ind"], daily[X_cols], daily["event_id"],
    "Buy Indicator ~ Window Dummies"
)

# ── 5c. Sell indicator ──────────────────────────────────────────────────
m3_hc, m3_cl = run_ols_with_cluster(
    daily["sell_ind"], daily[X_cols], daily["event_id"],
    "Sell Indicator ~ Window Dummies"
)

# ── 5d. Number of trades ────────────────────────────────────────────────
m4_hc, m4_cl = run_ols_with_cluster(
    daily["n_trades"], daily[X_cols], daily["event_id"],
    "Number of Trades ~ Window Dummies"
)

# ══════════════════════════════════════════════════════════════════════════
# 6. NVCA DiD (pre-2020 vs post-2020)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[6] NVCA DiD: Pre-2020 vs Post-2020")
print("=" * 80)

# Interaction terms
for w in X_cols:
    daily[f"{w}_x_post2020"] = daily[w] * daily["post_2020"]

X_did = X_cols + ["post_2020"] + [f"{w}_x_post2020" for w in X_cols]

m5_hc, m5_cl = run_ols_with_cluster(
    daily["trade_ind"], daily[X_did], daily["event_id"],
    "Trade Indicator ~ Window + Post2020 + Window x Post2020"
)

m6_hc, m6_cl = run_ols_with_cluster(
    daily["buy_ind"], daily[X_did], daily["event_id"],
    "Buy Indicator ~ Window + Post2020 + Window x Post2020"
)

m7_hc, m7_cl = run_ols_with_cluster(
    daily["sell_ind"], daily[X_did], daily["event_id"],
    "Sell Indicator ~ Window + Post2020 + Window x Post2020"
)

# ── 6b. Split-sample regressions ─────────────────────────────────────────
print("\n--- Split-sample: Pre-2020 ---")
pre20  = daily[daily["post_2020"] == 0]
post20 = daily[daily["post_2020"] == 1]
print(f"    Pre-2020  N = {len(pre20):,}")
print(f"    Post-2020 N = {len(post20):,}")

if len(pre20) > 100:
    run_ols_with_cluster(
        pre20["trade_ind"], pre20[X_cols], pre20["event_id"],
        "Trade Indicator ~ Window [PRE-2020 only]"
    )
    run_ols_with_cluster(
        pre20["buy_ind"], pre20[X_cols], pre20["event_id"],
        "Buy Indicator ~ Window [PRE-2020 only]"
    )

if len(post20) > 100:
    run_ols_with_cluster(
        post20["trade_ind"], post20[X_cols], post20["event_id"],
        "Trade Indicator ~ Window [POST-2020 only]"
    )
    run_ols_with_cluster(
        post20["buy_ind"], post20[X_cols], post20["event_id"],
        "Buy Indicator ~ Window [POST-2020 only]"
    )

# ══════════════════════════════════════════════════════════════════════════
# 7. PROFITABILITY TEST
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[7] PROFITABILITY TEST: Returns following buys")
print("=" * 80)

# For buys, compute dollar value and check if price data is available
buys = panel[panel["trancode"] == "P"].copy()
buys["dollar_value"] = buys["shares"].abs() * buys["tprice"].abs()
buys = buys.dropna(subset=["tprice"])

print(f"    Buys with price data: {len(buys):,}")
print(f"    Buys in [-30,-1]:    {(buys['window'] == 'pre_30_1').sum():,}")
print(f"    Buys in [-180,-91]:  {(buys['window'] == 'baseline_180_91').sum():,}")

# Since we don't have forward returns data, we report the trade-size comparison
# as a proxy for conviction
if len(buys) > 0:
    size_by_window = buys.groupby("window").agg(
        n_buys       = ("trade", "sum"),
        mean_shares  = ("shares", "mean"),
        median_shares = ("shares", "median"),
        mean_price   = ("tprice", "mean"),
        mean_dollar  = ("dollar_value", "mean"),
        median_dollar = ("dollar_value", "median"),
    ).reindex(window_order)

    print("\nPanel A: Buy Conviction (Trade Size) by Window")
    print("-" * 110)
    print(f"{'Window':>15s} {'N Buys':>8s} {'Mean Shares':>14s} {'Med Shares':>14s} "
          f"{'Mean Price':>12s} {'Mean $Val':>14s} {'Med $Val':>14s}")
    print("-" * 110)
    for w in window_order:
        if w in size_by_window.index and not pd.isna(size_by_window.loc[w, "n_buys"]):
            r = size_by_window.loc[w]
            print(f"{window_labels[w]:>15s} {int(r['n_buys']):>8,} {r['mean_shares']:>14,.0f} "
                  f"{r['median_shares']:>14,.0f} {r['mean_price']:>12.2f} "
                  f"{r['mean_dollar']:>14,.0f} {r['median_dollar']:>14,.0f}")
        else:
            print(f"{window_labels[w]:>15s}      ---")

    # Regression: dollar_value ~ pre_30 + pre_90 + post_30 + post_90
    buys_reg = buys[buys["window"].isin(window_order)].copy()
    buys_reg["pre_90"]  = (buys_reg["window"] == "pre_90_31").astype(int)
    buys_reg["pre_30"]  = (buys_reg["window"] == "pre_30_1").astype(int)
    buys_reg["post_30"] = (buys_reg["window"] == "post_0_30").astype(int)
    buys_reg["post_90"] = (buys_reg["window"] == "post_31_90").astype(int)
    buys_reg["event_id"] = buys_reg["keydevid"].astype(str) + "_" + buys_reg["ciq_personid"].astype(str)

    if len(buys_reg) > 50:
        # Log dollar value
        buys_reg["ln_dollar"] = np.log(buys_reg["dollar_value"].clip(lower=1))

        run_ols_with_cluster(
            buys_reg["ln_dollar"], buys_reg[X_cols], buys_reg["event_id"],
            "Log(Dollar Value of Buy) ~ Window Dummies"
        )

        run_ols_with_cluster(
            buys_reg["dollar_value"], buys_reg[X_cols], buys_reg["event_id"],
            "Dollar Value of Buy ~ Window Dummies"
        )

# ══════════════════════════════════════════════════════════════════════════
# 8. EVENT-TYPE HETEROGENEITY
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[8] EVENT-TYPE HETEROGENEITY")
print("=" * 80)

# Map event types to daily panel
ev_types = ev[["keydevid", "eventtype"]].drop_duplicates()
daily_et = daily.merge(ev_types, on="keydevid", how="left")

# Group event types into categories
ma_types = {"M&A Transaction Announcements", "M&A Transaction Closings",
            "M&A Rumors and Discussions", "M&A Transaction Cancellations"}
funding_types = {"Private Placements", "IPOs", "Follow-on Equity Offerings",
                 "Fixed Income Offerings", "Shelf Registration Filings",
                 "Seeking Financing/Partners", "Debt Financing Related"}
product_types = {"Product-Related Announcements", "Client Announcements",
                 "Business Expansions", "Strategic Alliances"}
mgmt_types   = {"Executive/Board Changes - Other", "Executive Changes - CEO",
                 "Executive Changes - CFO"}
distress_types = {"Bankruptcy - Filing", "Bankruptcy - Other",
                  "Bankruptcy \u2013 Reorganization", "Bankruptcy \u2013 Financing",
                  "Discontinued Operations/Downsizings", "Lawsuits & Legal Issues",
                  "Auditor Going Concern Doubts", "Impairments/Write Offs"}

def categorize_event(et):
    if et in ma_types:       return "M&A"
    if et in funding_types:  return "Funding"
    if et in product_types:  return "Product/Client"
    if et in mgmt_types:     return "Mgmt Changes"
    if et in distress_types: return "Distress"
    return "Other"

daily_et["event_cat"] = daily_et["eventtype"].apply(categorize_event)

for cat in ["M&A", "Funding", "Product/Client", "Mgmt Changes", "Distress"]:
    sub = daily_et[daily_et["event_cat"] == cat]
    if len(sub) > 500:
        print(f"\n  Category: {cat}  (N = {len(sub):,})")
        run_ols_with_cluster(
            sub["trade_ind"], sub[X_cols], sub["event_id"],
            f"Trade Indicator ~ Window [{cat}]"
        )
        run_ols_with_cluster(
            sub["buy_ind"], sub[X_cols], sub["event_id"],
            f"Buy Indicator ~ Window [{cat}]"
        )

# ══════════════════════════════════════════════════════════════════════════
# 9. SAME-INDUSTRY vs DIFF-INDUSTRY (if possible via company details)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[9] SUMMARY TABLE")
print("=" * 80)

# Compile a summary of key coefficients
print("\nKey pre-event coefficients (pre_30 = [-30,-1] vs baseline):")
print("-" * 70)
print(f"{'Outcome':>25s} {'Coef':>10s} {'HC1 SE':>10s} {'HC1 p':>10s} {'Stars':>6s}")
print("-" * 70)
for lbl, m in [("Any Trade", m1_hc), ("Buy", m2_hc), ("Sell", m3_hc), ("N Trades", m4_hc)]:
    c = m.params["pre_30"]
    s = m.bse["pre_30"]
    p = m.pvalues["pre_30"]
    st = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"{lbl:>25s} {c:>10.6f} {s:>10.6f} {p:>10.4f} {st:>6s}")

print(f"\n{'Outcome':>25s} {'Coef':>10s} {'Cl SE':>10s} {'Cl p':>10s} {'Stars':>6s}")
print("-" * 70)
for lbl, m in [("Any Trade", m1_cl), ("Buy", m2_cl), ("Sell", m3_cl), ("N Trades", m4_cl)]:
    if m is not None:
        c = m.params["pre_30"]
        s = m.bse["pre_30"]
        p = m.pvalues["pre_30"]
        st = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"{lbl:>25s} {c:>10.6f} {s:>10.6f} {p:>10.4f} {st:>6s}")

# ── NVCA DiD key interaction ─────────────────────────────────────────────
print("\nNVCA DiD: pre_30 x post_2020 interaction:")
print("-" * 70)
print(f"{'Outcome':>25s} {'Coef':>10s} {'HC1 SE':>10s} {'HC1 p':>10s} {'Stars':>6s}")
print("-" * 70)
for lbl, m in [("Any Trade", m5_hc), ("Buy", m6_hc), ("Sell", m7_hc)]:
    var = "pre_30_x_post2020"
    c = m.params[var]
    s = m.bse[var]
    p = m.pvalues[var]
    st = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"{lbl:>25s} {c:>10.6f} {s:>10.6f} {p:>10.4f} {st:>6s}")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
