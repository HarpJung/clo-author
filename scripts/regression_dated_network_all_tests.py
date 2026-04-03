"""
Dated Observer Network: Three Core Tests (A, B, C)

Uses the cleanest data: dated_observer_network.csv
Each row = VC Firm A invested in Company Y in Round N on a specific date,
           and Person X who works at VC A also observes at Company Y.

TEST A: Company Outcomes (before vs after exact investment date)
TEST B: Form 4 Insider Trading (equal 5-day windows, dated sample only)
TEST C: Preqin Fund Performance (dated links only)

All three tests: Overall + Same-Industry + Different-Industry subsamples.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import re
import warnings
warnings.filterwarnings("ignore")
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir  = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

# =====================================================================
# HELPERS
# =====================================================================

def stars(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return ""

def run_ols(y, X, cov_type="HC1", cov_kwds=None, label=""):
    """Run OLS and return dict of results."""
    if cov_kwds is None:
        cov_kwds = {}
    try:
        m = sm.OLS(y, X, missing="drop").fit(cov_type=cov_type, cov_kwds=cov_kwds)
        return m
    except Exception as e:
        return None

def print_coef(m, var, label, width=15):
    if m is None:
        print(f"  {label:<{width}} -- failed --")
        return
    b = m.params.get(var, np.nan)
    p = m.pvalues.get(var, np.nan)
    se = m.bse.get(var, np.nan)
    s = stars(p)
    n = int(m.nobs)
    print(f"  {label:<{width}} b={b:>8.4f}{s:<3} se={se:.4f} p={p:.3f}  N={n:,}")

def demean_fe(df, y_col, fe_col):
    """Within-group demeaning for FE."""
    means = df.groupby(fe_col)[y_col].transform("mean")
    return df[y_col] - means

def clean_name(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r'[,.\-\'\"&()!]', ' ', s.lower().strip())
    for suf in [" inc", " llc", " corp", " ltd", " co", " company", " corporation"]:
        s = s.replace(suf, "")
    return re.sub(r'\s+', ' ', s).strip()

noise_events = [
    "Announcements of Earnings", "Conferences", "Company Conference Presentations",
    "Earnings Calls", "Earnings Release Date",
    "Estimated Earnings Release Date (S&P Global Derived)",
]

STAGE_MAP = {
    "Seed": 1, "Angel": 1,
    "Series A": 2, "Series A/Round 1": 2,
    "Series B": 3, "Series B/Round 2": 3,
    "Series C": 4, "Series C/Round 3": 4,
    "Series D": 5, "Series D/Round 4": 5,
    "Series E": 6, "Series E/Round 5": 6,
    "Series F": 7, "Series G": 8, "Series H": 9,
    "Growth": 7, "Expansion": 6,
    "Venture Debt": 5,
    "PIPE": 8, "Grant": 1,
}

# =====================================================================
# LOAD COMMON DATA
# =====================================================================
print("=" * 90)
print("DATED OBSERVER NETWORK: THREE CORE TESTS")
print("=" * 90)

# --- Dated network ---
dated = pd.read_csv(os.path.join(data_dir, "Dated_Network/dated_observer_network.csv"))
dated["observer_personid"] = dated["observer_personid"].astype(str).str.replace(".0", "", regex=False)
dated["vc_companyid"] = dated["vc_companyid"].astype(str).str.replace(".0", "", regex=False)
dated["observed_companyid"] = dated["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
dated["investment_date"] = pd.to_datetime(dated["investment_date"], errors="coerce")
print(f"\nDated network: {len(dated):,} rows")
print(f"  Observers: {dated['observer_personid'].nunique():,}")
print(f"  VC firms: {dated['vc_companyid'].nunique():,}")
print(f"  Observed companies: {dated['observed_companyid'].nunique():,}")
print(f"  Date range: {dated['investment_date'].min()} to {dated['investment_date'].max()}")

# --- US private filter ---
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
co["companyid"] = co["companyid"].astype(str).str.replace(".0", "", regex=False)
us_priv = set(co[(co["country"] == "United States") &
                  (co["companytypename"] == "Private Company")]["companyid"])

# --- Observer records ---
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)

# --- Events ---
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events = events[events["companyid"].isin(us_priv)]
events = events[~events["eventtype"].isin(noise_events)]
events = events[events["announcedate"] >= "2010-01-01"]
events["material"] = events["eventtype"].apply(
    lambda x: "M&A" in str(x) or "Bankruptcy" in str(x) or
              "Executive/Board" in str(x) or "Restructuring" in str(x) or
              "Private Placements" in str(x) or "IPO" in str(x))
print(f"\nEvents loaded: {len(events):,} (US private, 2010+, no noise)")

# --- Industry codes ---
ind = pd.read_csv(os.path.join(data_dir, "Panel_C_Network/05_industry_codes.csv"))
ind["gvkey"] = ind["gvkey"].astype(str).str.replace(".0", "", regex=False).str.zfill(6)
ind["sic2"] = (ind["sic"].astype(str).str[:2]).astype(str)
gvkey_to_sic2 = dict(zip(ind["gvkey"], ind["sic2"]))

# Map gvkey from events to get SIC codes for observed companies
ev_gvkeys = events[["companyid", "gvkey"]].drop_duplicates()
ev_gvkeys["gvkey_str"] = ev_gvkeys["gvkey"].astype(str).str.replace(".0", "", regex=False).str.zfill(6)
co_to_sic2 = dict(zip(ev_gvkeys["companyid"], ev_gvkeys["gvkey_str"].map(gvkey_to_sic2)))

# Map VC firm SIC: use the VC companyid from dated network
# We approximate same-industry as same SIC-2 between VC's primary industry and observed company
vc_sic2 = {}
for _, r in dated.iterrows():
    vc_cid = r["vc_companyid"]
    obs_cid = r["observed_companyid"]
    vc_sic2[vc_cid] = co_to_sic2.get(obs_cid, None)  # Will be refined per-link

print(f"  Companies with SIC-2 codes: {sum(1 for v in co_to_sic2.values() if v is not None):,}")


# ##################################################################
#                        TEST A
#       COMPANY OUTCOMES (before vs after investment date)
# ##################################################################
print("\n" + "=" * 90)
print("TEST A: COMPANY OUTCOMES — DATED NETWORK")
print("(Preqin deals before vs. after exact investment_date)")
print("=" * 90)

# Load Preqin VC deals
deals = pd.read_csv(os.path.join(preqin_dir, "vc_deals_full.csv"), low_memory=False)
deals["deal_date"] = pd.to_datetime(deals["deal_date"], errors="coerce")
deals = deals.dropna(subset=["deal_date"])
us_deals = deals[deals["portfolio_company_country"].fillna("").str.contains("US|United States", case=False)].copy()
us_deals["nc"] = us_deals["portfolio_company_name"].fillna("").apply(clean_name)
print(f"\nPreqin US deals: {len(us_deals):,}")

# Match to observed companies by name
co_det = co[co["companyid"].isin(us_priv)].copy()
co_det["nc"] = co_det["companyname"].apply(clean_name)
ciq_name_to_id = dict(zip(co_det["nc"], co_det["companyid"]))
us_deals["ciq_cid"] = us_deals["nc"].map(ciq_name_to_id)

# Restrict to companies in the dated network
dated_cos = set(dated["observed_companyid"])
md = us_deals[us_deals["ciq_cid"].isin(dated_cos)].copy()
print(f"  Matched to dated network companies: {len(md):,} deals at {md['ciq_cid'].nunique():,} companies")

# For each dated company, earliest investment_date
co_inv_date = dated.groupby("observed_companyid")["investment_date"].min().to_dict()
md["inv_date"] = md["ciq_cid"].map(co_inv_date)
md = md.dropna(subset=["inv_date"])

# Treatment indicator: deal after investment date
md["post_observer"] = (md["deal_date"] >= md["inv_date"]).astype(int)
md["deal_year"] = md["deal_date"].dt.year

# DVs
md["has_size"] = md["deal_financing_size_usd"].notna() & (md["deal_financing_size_usd"] > 0)
md["ln_deal"] = np.log(md["deal_financing_size_usd"].clip(lower=0.01))
md["stage_ord"] = md["stage"].map(STAGE_MAP)

# Days to next round
md = md.sort_values(["ciq_cid", "deal_date"])
md["next_deal"] = md.groupby("ciq_cid")["deal_date"].shift(-1)
md["days_to_next"] = (md["next_deal"] - md["deal_date"]).dt.days

# NVCA DiD
md["post_2020"] = (md["deal_year"] >= 2020).astype(int)
md["postobs_x_post2020"] = md["post_observer"] * md["post_2020"]

# Same-industry subsample
md["obs_sic2"] = md["ciq_cid"].map(co_to_sic2)
# VC industry from dated network vc_name -> map to SIC via crosswalk or Preqin
# For now: mark as "same" if VC has *any* other observed company in same SIC-2
vc_to_sic2_set = {}
for _, r in dated.iterrows():
    vc = r["vc_companyid"]
    s = co_to_sic2.get(r["observed_companyid"])
    if s:
        vc_to_sic2_set.setdefault(vc, set()).add(s)

md["vc_cid"] = md["ciq_cid"].map(
    lambda c: dated[dated["observed_companyid"] == c]["vc_companyid"].iloc[0]
    if c in dated["observed_companyid"].values else None
)

# Alternative simpler approach: build VC primary SIC from modal observed company SIC
# Then flag same-industry if observed company SIC == VC modal SIC
vc_modal_sic = {}
for vc, sics in vc_to_sic2_set.items():
    # Use the most common SIC code across all observed companies
    vc_modal_sic[vc] = max(set(sics), key=lambda x: list(sics).count(x)) if sics else None

# For each deal, find the VC that invested in this company
deal_vc_map = dated.groupby("observed_companyid")["vc_companyid"].first().to_dict()
md["vc_cid"] = md["ciq_cid"].map(deal_vc_map)
md["vc_sic2"] = md["vc_cid"].map(vc_modal_sic)
md["same_ind"] = (md["obs_sic2"] == md["vc_sic2"]).astype(int)
has_ind = md["obs_sic2"].notna() & md["vc_sic2"].notna()

# Control group: US Preqin deals at companies NOT in the dated network
ctrl = us_deals[~us_deals["ciq_cid"].isin(dated_cos) & us_deals["ciq_cid"].notna()].copy()
ctrl["post_observer"] = 0
ctrl["deal_year"] = ctrl["deal_date"].dt.year
ctrl["has_size"] = ctrl["deal_financing_size_usd"].notna() & (ctrl["deal_financing_size_usd"] > 0)
ctrl["ln_deal"] = np.log(ctrl["deal_financing_size_usd"].clip(lower=0.01))
ctrl["stage_ord"] = ctrl["stage"].map(STAGE_MAP)
ctrl = ctrl.sort_values(["ciq_cid", "deal_date"])
ctrl["next_deal"] = ctrl.groupby("ciq_cid")["deal_date"].shift(-1)
ctrl["days_to_next"] = (ctrl["next_deal"] - ctrl["deal_date"]).dt.days
ctrl["post_2020"] = (ctrl["deal_year"] >= 2020).astype(int)
ctrl["postobs_x_post2020"] = 0  # control group has no observer
ctrl["inv_date"] = pd.NaT
ctrl["same_ind"] = 0

# Print sample sizes
print(f"\n  Treated (dated network): {len(md):,} deals, {md['ciq_cid'].nunique():,} cos")
print(f"    Before inv date: {(md['post_observer']==0).sum():,}")
print(f"    After inv date:  {(md['post_observer']==1).sum():,}")
print(f"  Control (not in network): {len(ctrl):,} deals, {ctrl['ciq_cid'].nunique():,} cos")

# --- DV 1: ln(deal_size) ---
for dv_label, dv_col, sample_filter in [
    ("ln(deal_size)", "ln_deal", "has_size"),
    ("days_to_next_round", "days_to_next", None),
    ("stage_ordinal", "stage_ord", None),
]:
    print(f"\n--- DV: {dv_label} ---")

    for sub_label, sub_mask_fn in [
        ("Overall", lambda d: pd.Series(True, index=d.index)),
        ("Same-Ind", lambda d: d["same_ind"] == 1),
        ("Diff-Ind", lambda d: (d["same_ind"] == 0) & d["obs_sic2"].notna() & d["vc_sic2"].notna()),
    ]:
        if sample_filter:
            sub = md[md[sample_filter] & sub_mask_fn(md)].copy()
        else:
            sub = md[md[dv_col].notna() & sub_mask_fn(md)].copy()

        if len(sub) < 30:
            print(f"  [{sub_label}] N={len(sub)} -- too small, skipping")
            continue

        y = sub[dv_col].astype(float)
        idx = y.dropna().index
        y = y.loc[idx]

        if len(y) < 20:
            print(f"  [{sub_label}] N={len(y)} after dropna -- too small")
            continue

        print(f"  [{sub_label}] N={len(y):,}  mean(pre)={sub.loc[idx & (sub['post_observer']==0).values, dv_col].mean():.3f}  mean(post)={sub.loc[idx & (sub['post_observer']==1).values, dv_col].mean():.3f}")

        for spec_label, xvars, extra_fe, cov, cov_kw in [
            ("HC1", ["post_observer"], None, "HC1", {}),
            ("Co-cl", ["post_observer"], None, "cluster", {"groups": sub.loc[idx, "ciq_cid"]}),
            ("CoFE+HC1", ["post_observer"], "ciq_cid", "HC1", {}),
            ("YrFE+Co-cl", ["post_observer"], None, "cluster", {"groups": sub.loc[idx, "ciq_cid"]}),
        ]:
            yy = y.copy()
            if extra_fe == "ciq_cid":
                # Company FE via demeaning
                gm = sub.loc[idx].groupby("ciq_cid")[dv_col].transform("mean")
                yy = y - gm.loc[idx]

            X = sub.loc[idx, xvars].copy().astype(float)
            if spec_label.startswith("YrFE"):
                yr_dum = pd.get_dummies(sub.loc[idx, "deal_year"], prefix="dy", drop_first=True).astype(float)
                X = pd.concat([X, yr_dum], axis=1)
            X = sm.add_constant(X)

            m = run_ols(yy, X, cov_type=cov, cov_kwds=cov_kw)
            print_coef(m, "post_observer", f"    {spec_label}")

    # NVCA 2020 DiD (overall only for space)
    print(f"\n  --- NVCA 2020 DiD: {dv_label} ---")
    if sample_filter:
        sub = md[md[sample_filter]].copy()
    else:
        sub = md[md[dv_col].notna()].copy()

    if len(sub) < 30:
        print(f"  N={len(sub)} -- too small")
        continue

    y = sub[dv_col].astype(float).dropna()
    idx = y.index

    for spec_label, xvars, cov, cov_kw in [
        ("HC1", ["post_observer", "post_2020", "postobs_x_post2020"], "HC1", {}),
        ("Co-cl", ["post_observer", "post_2020", "postobs_x_post2020"], "cluster", {"groups": sub.loc[idx, "ciq_cid"]}),
    ]:
        X = sub.loc[idx, xvars].copy().astype(float)
        X = sm.add_constant(X)
        m = run_ols(y, X, cov_type=cov, cov_kwds=cov_kw)
        if m is not None:
            bo = m.params.get("post_observer", np.nan)
            po = m.pvalues.get("post_observer", np.nan)
            bd = m.params.get("postobs_x_post2020", np.nan)
            pd_ = m.pvalues.get("postobs_x_post2020", np.nan)
            so, sd = stars(po), stars(pd_)
            print(f"    {spec_label:<12} b(post_obs)={bo:>7.4f}{so:<3} p={po:.3f}  b(obs x 2020)={bd:>7.4f}{sd:<3} p={pd_:.3f}  N={int(m.nobs):,}")
        else:
            print(f"    {spec_label:<12} -- failed --")

# --- Treated vs. Control comparison ---
print(f"\n--- Treated vs. Control (all US Preqin deals) ---")
pool_cols = ["ln_deal", "has_size", "deal_year", "post_observer", "ciq_cid"]
pool_t = md[md["has_size"]][pool_cols].copy()
pool_t["treated_co"] = 1
pool_c = ctrl[ctrl["has_size"]][pool_cols].copy()
pool_c["treated_co"] = 0
pool = pd.concat([pool_t, pool_c], ignore_index=True)
pool = pool.dropna(subset=["ln_deal"])

print(f"  Pooled: {len(pool):,} deals (treated={pool['treated_co'].sum():,}, control={(pool['treated_co']==0).sum():,})")
print(f"  Mean ln(deal): treated={pool[pool['treated_co']==1]['ln_deal'].mean():.3f}  control={pool[pool['treated_co']==0]['ln_deal'].mean():.3f}")

y = pool["ln_deal"]
for spec_label, xvars, cov, kw in [
    ("HC1", ["treated_co", "post_observer"], "HC1", {}),
    ("YrFE+HC1", ["treated_co", "post_observer"], "HC1", {}),
]:
    X = pool[["treated_co", "post_observer"]].copy().astype(float)
    if "YrFE" in spec_label:
        yr = pd.get_dummies(pool["deal_year"], prefix="dy", drop_first=True).astype(float)
        X = pd.concat([X, yr], axis=1)
    X = sm.add_constant(X)
    m = run_ols(y, X, cov_type=cov, cov_kwds=kw)
    if m:
        for v in ["treated_co", "post_observer"]:
            b, p = m.params.get(v, np.nan), m.pvalues.get(v, np.nan)
            print(f"    {spec_label:<12} b({v})={b:>7.4f}{stars(p):<3} p={p:.3f}")


# ##################################################################
#                        TEST B
#       FORM 4 INSIDER TRADING (5-day equal windows, dated only)
# ##################################################################
print("\n" + "=" * 90)
print("TEST B: FORM 4 INSIDER TRADING — DATED NETWORK")
print("(Equal 5-day windows, only events after investment_date)")
print("=" * 90)

# Load trades
trades = pd.read_csv(os.path.join(data_dir, "Form4/observer_form4_trades.csv"))
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce", format="mixed")
trades = trades.dropna(subset=["trandate"])
trades = trades[trades["trancode"].isin(["P", "S"])]

# Crosswalk
tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
trades["ciq_pid"] = trades["personid"].map(dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"])))
trades = trades.dropna(subset=["ciq_pid"])
print(f"\nForm 4 trades: {len(trades):,} (P/S, with CIQ match)")

# Build dated observer-company lookup
# observer_personid -> set of (observed_companyid, investment_date)
dated_obs_lookup = {}
for _, r in dated.iterrows():
    pid = r["observer_personid"]
    dated_obs_lookup.setdefault(pid, []).append(
        (r["observed_companyid"], r["investment_date"])
    )

# Observer -> observed companies (from the dated network)
dated_pids = set(dated["observer_personid"])
obs_us = obs[obs["companyid"].isin(us_priv) & obs["personid"].isin(dated_pids)]
obs_to_cos = {}
for _, r in obs_us.iterrows():
    obs_to_cos.setdefault(r["personid"], set()).add(r["companyid"])

# Material events at dated network companies
dated_obs_cids = set(dated["observed_companyid"])
mat_events = events[events["material"] & events["companyid"].isin(dated_obs_cids)].copy()
print(f"  Material events at dated network companies: {len(mat_events):,}")

# Build event-person pairs with dated network confirmation
# Only include pairs where:
#   1) Observer is in dated network
#   2) Event is at an observed company of this observer
#   3) Event date >= investment_date from dated network
pairs = []
for _, evt in mat_events.iterrows():
    ecid = evt["companyid"]
    edate = evt["announcedate"]

    # Find observers who observe at this company via dated network
    for _, r in dated[dated["observed_companyid"] == ecid].iterrows():
        pid = r["observer_personid"]
        inv_date = r["investment_date"]

        # Only include if event is after the investment date
        if pd.notna(inv_date) and edate >= inv_date:
            pairs.append({
                "event_id": evt["keydevid"],
                "event_date": edate,
                "event_companyid": ecid,
                "observer_pid": pid,
                "investment_date": inv_date,
            })

pairs_df = pd.DataFrame(pairs)
if len(pairs_df) > 0:
    pairs_df = pairs_df.drop_duplicates(subset=["event_id", "observer_pid"])

print(f"  Dated event-observer pairs: {len(pairs_df):,}")
print(f"  Unique observers: {pairs_df['observer_pid'].nunique():,}")
print(f"  Unique events: {pairs_df['event_id'].nunique():,}")

# Map CIQ personid -> TR personid for trade matching
ciq_to_tr = dict(zip(tr_xwalk["ciq_personid"], tr_xwalk["tr_personid"]))
tr_to_ciq = dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"]))

# Build event-level trading data with equal 5-day windows
# Baseline: [-60, -31] (30 days)
# Pre-event: [-30, -1] (30 days)
# Post-event: [0, +29] (30 days)
BASELINE = (-60, -31)
PRE      = (-30, -1)
POST     = (0, 29)

if len(pairs_df) > 0:
    # For each pair, count trades in each window
    rows = []
    traded_pids = set(trades["ciq_pid"].unique())

    for _, pair in pairs_df.iterrows():
        pid = pair["observer_pid"]
        edate = pair["event_date"]

        # Only process if person has any Form 4 trades
        if pid not in traded_pids:
            continue

        ptrades = trades[trades["ciq_pid"] == pid]

        for win_label, (lo, hi) in [("baseline", BASELINE), ("pre_event", PRE), ("post_event", POST)]:
            start = edate + pd.Timedelta(days=lo)
            end = edate + pd.Timedelta(days=hi)
            n_days = hi - lo + 1
            wt = ptrades[(ptrades["trandate"] >= start) & (ptrades["trandate"] <= end)]
            n_trades = len(wt)
            n_buys = (wt["trancode"] == "P").sum()
            n_sells = (wt["trancode"] == "S").sum()

            rows.append({
                "event_id": pair["event_id"],
                "observer_pid": pid,
                "event_date": edate,
                "event_companyid": pair["event_companyid"],
                "window": win_label,
                "n_days": n_days,
                "trades": n_trades,
                "buys": n_buys,
                "sells": n_sells,
                "trades_per_day": n_trades / n_days,
                "buys_per_day": n_buys / n_days,
                "sells_per_day": n_sells / n_days,
            })

    wdf = pd.DataFrame(rows)
    print(f"\n  Trading windows built: {len(wdf):,} rows")
    print(f"  Event-person pairs with trades in any window: {wdf.groupby(['event_id','observer_pid']).ngroups:,}")

    # Pivot to wide for regression (pre-event and post-event dummies, baseline is reference)
    wdf["pre_event"] = (wdf["window"] == "pre_event").astype(int)
    wdf["post_event"] = (wdf["window"] == "post_event").astype(int)

    # Industry coding for subsamples
    wdf["obs_sic2"] = wdf["event_companyid"].map(co_to_sic2)
    wdf_vc = wdf["event_companyid"].map(deal_vc_map)
    wdf["vc_sic2"] = wdf_vc.map(vc_modal_sic)
    wdf["same_ind"] = (wdf["obs_sic2"] == wdf["vc_sic2"]).astype(int)

    for dv_label, dv_col in [("trades_per_day", "trades_per_day"),
                              ("buys_per_day", "buys_per_day"),
                              ("sells_per_day", "sells_per_day")]:
        print(f"\n--- DV: {dv_label} ---")

        for sub_label, sub_mask_fn in [
            ("Overall", lambda d: pd.Series(True, index=d.index)),
            ("Same-Ind", lambda d: d["same_ind"] == 1),
            ("Diff-Ind", lambda d: (d["same_ind"] == 0) & d["obs_sic2"].notna() & d["vc_sic2"].notna()),
        ]:
            sub = wdf[sub_mask_fn(wdf)].copy()

            if len(sub) < 30:
                print(f"  [{sub_label}] N={len(sub)} -- too small, skipping")
                continue

            y = sub[dv_col].astype(float)

            # Means
            base_mean = sub.loc[sub["window"] == "baseline", dv_col].mean()
            pre_mean  = sub.loc[sub["window"] == "pre_event", dv_col].mean()
            post_mean = sub.loc[sub["window"] == "post_event", dv_col].mean()
            print(f"  [{sub_label}] N={len(sub):,}  baseline={base_mean:.4f}  pre={pre_mean:.4f}  post={post_mean:.4f}")

            for spec_label, cov, cov_kw, fe_col in [
                ("HC1", "HC1", {}, None),
                ("Event-cl", "cluster", {"groups": sub["event_id"]}, None),
                ("Person-cl", "cluster", {"groups": sub["observer_pid"]}, None),
                ("PersonFE+HC1", "HC1", {}, "observer_pid"),
            ]:
                yy = y.copy()
                if fe_col:
                    gm = sub.groupby(fe_col)[dv_col].transform("mean")
                    yy = y - gm

                X = sub[["pre_event", "post_event"]].copy().astype(float)
                X = sm.add_constant(X)

                m = run_ols(yy, X, cov_type=cov, cov_kwds=cov_kw)
                if m:
                    bp = m.params.get("pre_event", np.nan)
                    pp = m.pvalues.get("pre_event", np.nan)
                    bpo = m.params.get("post_event", np.nan)
                    ppo = m.pvalues.get("post_event", np.nan)
                    print(f"    {spec_label:<14} b(pre)={bp:>8.5f}{stars(pp):<3} p={pp:.3f}  b(post)={bpo:>8.5f}{stars(ppo):<3} p={ppo:.3f}")
                else:
                    print(f"    {spec_label:<14} -- failed --")
else:
    print("  No event-observer pairs found. Skipping Test B.")


# ##################################################################
#                        TEST C
#       PREQIN FUND PERFORMANCE (dated links only)
# ##################################################################
print("\n" + "=" * 90)
print("TEST C: PREQIN FUND PERFORMANCE — DATED NETWORK")
print("(Event counts at observed companies after investment_date)")
print("=" * 90)

# Match dated network VCs to Preqin
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
ciq_to_preqin = dict(zip(
    xwalk["ciq_vc_companyid"].astype(str).str.replace(".0", "", regex=False),
    xwalk["preqin_firm_id"].astype(int)
))

dated["preqin_firm_id"] = dated["vc_companyid"].map(ciq_to_preqin)
dated_preqin = dated.dropna(subset=["preqin_firm_id"]).copy()
dated_preqin["preqin_firm_id"] = dated_preqin["preqin_firm_id"].astype(int)
print(f"\nDated links with Preqin match: {len(dated_preqin):,}")
print(f"  Unique Preqin firms: {dated_preqin['preqin_firm_id'].nunique():,}")

# Load fund details + performance
funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
matched_firm_ids = set(dated_preqin["preqin_firm_id"])
funds_matched = funds[funds["firm_id"].isin(matched_firm_ids)].copy()
vc_funds = funds_matched[funds_matched["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)].copy()
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))
print(f"  VC funds at matched firms: {len(vc_funds):,}")

perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)
perf = perf[perf["fund_id"].isin(vc_fund_ids)].copy()
perf["date_reported"] = pd.to_datetime(perf["date_reported"], errors="coerce")
perf = perf.dropna(subset=["date_reported"]).sort_values(["fund_id", "date_reported"])
perf["multiple_num"] = pd.to_numeric(perf["multiple"], errors="coerce")
perf["dpi_num"] = pd.to_numeric(perf["distr_dpi_pcent"], errors="coerce")
perf["d_tvpi"] = perf.groupby("fund_id")["multiple_num"].diff()
perf["d_dpi"] = perf.groupby("fund_id")["dpi_num"].diff()
perf = perf.merge(vc_funds[["fund_id", "firm_id", "vintage", "final_size_usd"]],
                   on="fund_id", how="left", suffixes=("", "_fund"))
perf["quarter"] = perf["date_reported"].dt.to_period("Q")
perf["year"] = perf["date_reported"].dt.year
perf = perf.dropna(subset=["d_tvpi"])  # Need at least diff
print(f"  Performance records (with d_tvpi): {len(perf):,}")
print(f"  Funds: {perf['fund_id'].nunique():,}")

# Build: for each Preqin firm, confirmed observed companies + investment_date
firm_to_cos = {}
for _, r in dated_preqin.iterrows():
    fid = int(r["preqin_firm_id"])
    firm_to_cos.setdefault(fid, []).append(
        (r["observed_companyid"], r["investment_date"])
    )

# Material events at observed companies
mat_ev = events[events["material"]].copy()
mat_ev["quarter"] = mat_ev["announcedate"].dt.to_period("Q")

# For each fund-quarter, count events at observed companies ONLY after investment_date
event_counts = []
for fid, cos_list in firm_to_cos.items():
    all_cids = set(c[0] for c in cos_list)
    fe = mat_ev[mat_ev["companyid"].isin(all_cids)]

    # Build quarter-level counts (time-bounded)
    q_counts = {}
    for _, evt in fe.iterrows():
        ecid = evt["companyid"]
        edate = evt["announcedate"]
        eq = evt["quarter"]

        # Only count if event is after the investment date for this company
        for obs_cid, inv_date in cos_list:
            if obs_cid == ecid and pd.notna(inv_date) and edate >= inv_date:
                q_counts[eq] = q_counts.get(eq, 0) + 1
                break

    for q, cnt in q_counts.items():
        event_counts.append({"preqin_firm_id": fid, "quarter": q, "n_events": cnt})

ecdf = pd.DataFrame(event_counts)
if len(ecdf) > 0:
    ecdf["quarter"] = ecdf["quarter"].astype("period[Q]")
    print(f"\n  Firm-quarter event counts: {len(ecdf):,}")

    # perf has firm_id (from fund_details); ecdf has preqin_firm_id
    # Rename perf's firm_id to preqin_firm_id for consistency
    if "firm_id" in perf.columns and "preqin_firm_id" not in perf.columns:
        perf = perf.rename(columns={"firm_id": "preqin_firm_id"})

    perf = perf.merge(ecdf, on=["preqin_firm_id", "quarter"], how="left")

    perf["n_events"] = perf["n_events"].fillna(0)
    perf["has_events"] = (perf["n_events"] > 0).astype(int)
    perf["ln_events"] = np.log(perf["n_events"] + 1)

    # NVCA DiD
    perf["post_2020"] = (perf["year"] >= 2020).astype(int)
    perf["events_x_post2020"] = perf["has_events"] * perf["post_2020"]

    # Industry subsamples for Test C
    # Build VC firm -> modal SIC
    vc_firm_sic = {}
    for fid, cos_list in firm_to_cos.items():
        sics = [co_to_sic2.get(c[0]) for c in cos_list if co_to_sic2.get(c[0])]
        if sics:
            vc_firm_sic[fid] = max(set(sics), key=sics.count)
    perf["vc_sic2"] = perf["preqin_firm_id"].map(vc_firm_sic)

    # For same-industry: compare fund's VC SIC to the SIC of observed companies that had events
    # Simplify: flag if VC has ANY same-industry observed company
    firm_has_same_ind = {}
    for fid, cos_list in firm_to_cos.items():
        vc_s = vc_firm_sic.get(fid)
        if vc_s:
            firm_has_same_ind[fid] = any(co_to_sic2.get(c[0]) == vc_s for c in cos_list)
        else:
            firm_has_same_ind[fid] = None
    perf["has_same_ind"] = perf["preqin_firm_id"].map(firm_has_same_ind)

    for dv_label, dv_col in [("delta_TVPI", "d_tvpi"), ("delta_DPI", "d_dpi")]:
        print(f"\n--- DV: {dv_label} ---")

        sub = perf[perf[dv_col].notna()].copy()
        if len(sub) < 30:
            print(f"  N={len(sub)} -- too small")
            continue

        for sub_label, mask_fn in [
            ("Overall", lambda d: pd.Series(True, index=d.index)),
            ("Same-Ind", lambda d: d["has_same_ind"] == True),
            ("Diff-Ind", lambda d: d["has_same_ind"] == False),
        ]:
            ss = sub[mask_fn(sub)].copy()
            if len(ss) < 30:
                print(f"  [{sub_label}] N={len(ss)} -- too small")
                continue

            y = ss[dv_col].astype(float)
            print(f"  [{sub_label}] N={len(y):,}  mean={y.mean():.4f}  events>0: {ss['has_events'].sum():,}")

            for spec_label, xvars, cov, cov_kw, fe_col in [
                ("HC1",       ["has_events", "ln_events"], "HC1", {}, None),
                ("Firm-cl",   ["has_events", "ln_events"], "cluster", {"groups": ss["preqin_firm_id"]}, None),
                ("FundFE+HC1",["has_events", "ln_events"], "HC1", {}, "fund_id"),
                ("FirmFE+HC1",["has_events", "ln_events"], "HC1", {}, "preqin_firm_id"),
            ]:
                yy = y.copy()
                if fe_col:
                    gm = ss.groupby(fe_col)[dv_col].transform("mean")
                    yy = y - gm

                X = ss[xvars].copy().astype(float)
                X = sm.add_constant(X)

                m = run_ols(yy, X, cov_type=cov, cov_kwds=cov_kw)
                if m:
                    bh = m.params.get("has_events", np.nan)
                    ph = m.pvalues.get("has_events", np.nan)
                    bl = m.params.get("ln_events", np.nan)
                    pl = m.pvalues.get("ln_events", np.nan)
                    print(f"    {spec_label:<14} b(has_ev)={bh:>8.4f}{stars(ph):<3} p={ph:.3f}  b(ln_ev)={bl:>8.4f}{stars(pl):<3} p={pl:.3f}")
                else:
                    print(f"    {spec_label:<14} -- failed --")

        # NVCA DiD
        print(f"\n  --- NVCA 2020 DiD: {dv_label} ---")
        ss = sub.copy()
        y = ss[dv_col].astype(float)
        for spec_label, xvars, cov, cov_kw in [
            ("HC1", ["has_events", "post_2020", "events_x_post2020"], "HC1", {}),
            ("Firm-cl", ["has_events", "post_2020", "events_x_post2020"], "cluster", {"groups": ss["preqin_firm_id"]}),
        ]:
            X = ss[xvars].copy().astype(float)
            X = sm.add_constant(X)
            m = run_ols(y, X, cov_type=cov, cov_kwds=cov_kw)
            if m:
                bh = m.params.get("has_events", np.nan)
                ph = m.pvalues.get("has_events", np.nan)
                bd = m.params.get("events_x_post2020", np.nan)
                pd_ = m.pvalues.get("events_x_post2020", np.nan)
                print(f"    {spec_label:<12} b(has_ev)={bh:>7.4f}{stars(ph):<3} p={ph:.3f}  b(ev x 2020)={bd:>7.4f}{stars(pd_):<3} p={pd_:.3f}  N={int(m.nobs):,}")
            else:
                print(f"    {spec_label:<12} -- failed --")
else:
    print("  No event counts built -- skipping Test C regressions.")


# ##################################################################
#                        SUMMARY
# ##################################################################
print("\n" + "=" * 90)
print("SUMMARY")
print("=" * 90)
print(f"\nDated network: {len(dated):,} rows, {dated['observed_companyid'].nunique():,} companies, {dated['vc_companyid'].nunique():,} VCs")
print(f"Test A: {md['ciq_cid'].nunique():,} companies matched to Preqin, {len(md[md['has_size']]):,} sized deals")
if len(pairs_df) > 0:
    print(f"Test B: {pairs_df['observer_pid'].nunique():,} observers, {len(pairs_df):,} event-observer pairs")
if len(ecdf) > 0:
    print(f"Test C: {perf['preqin_firm_id'].nunique():,} Preqin firms, {perf['fund_id'].nunique():,} funds, {len(perf):,} fund-quarters")
print("\nDone.")
