"""
Company outcomes v3: Additional FE/clustering + confirmed investment subsample.

Adds: founding year FE, state-clustered, double FE, funding controls,
      confirmed-investment subsample from ciqcompanyrel.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os, re
import statsmodels.api as sm
import statsmodels.formula.api as smf

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

print("=" * 90)
print("COMPANY OUTCOMES v3: Full Specs + Confirmed Investment Subsample")
print("=" * 90)

# === Load and match data (same as v2) ===
print("\n--- Loading data ---")

co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
co["companyid"] = co["companyid"].astype(str).str.replace(".0", "", regex=False)
us_priv = co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")].copy()

obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)
obs_us = obs[obs["companyid"].isin(set(us_priv["companyid"]))]
obs_count = obs_us.groupby("companyid")["personid"].nunique().reset_index()
obs_count.columns = ["companyid", "n_observers"]

deals = pd.read_csv(os.path.join(preqin_dir, "vc_deals_full.csv"), low_memory=False)
deals["deal_date"] = pd.to_datetime(deals["deal_date"], errors="coerce")
us_deals = deals[deals["portfolio_company_country"].fillna("").str.contains("US|United States", case=False)].copy()

def clean_name(s):
    if not isinstance(s, str): return ""
    s = s.lower().strip()
    s = re.sub(r'[,.\-\'\"&()!]', ' ', s)
    for suf in [" inc", " llc", " corp", " ltd", " co", " lp"]:
        s = s.replace(suf, "")
    return re.sub(r'\s+', ' ', s).strip()

us_priv["name_clean"] = us_priv["companyname"].apply(clean_name)
us_deals["name_clean"] = us_deals["portfolio_company_name"].fillna("").apply(clean_name)

# Match
ciq_names = dict(zip(us_priv["name_clean"], us_priv["companyid"]))
us_deals["ciq_companyid"] = us_deals["name_clean"].map(ciq_names)
us_deals["has_observer"] = us_deals["ciq_companyid"].notna().astype(int)

# Merge observer count
us_deals = us_deals.merge(obs_count, left_on="ciq_companyid", right_on="companyid", how="left")
us_deals["n_observers"] = us_deals["n_observers"].fillna(0).astype(int)

# Company-level aggregation
co_agg = us_deals.groupby("name_clean").agg(
    total_funding=("deal_financing_size_usd", "sum"),
    n_rounds=("deal_date", "count"),
    first_deal_date=("deal_date", "min"),
    first_deal_size=("deal_financing_size_usd", "first"),
    has_observer=("has_observer", "max"),
    n_observers=("n_observers", "max"),
    industry=("industry_classification", "first"),
    state=("portfolio_company_state", "first"),
    year_established=("year_established", lambda x: x.dropna().iloc[0] if x.notna().any() else np.nan),
    investment_status=("investment_status", "first"),
    highest_stage=("stage", lambda x: x.dropna().iloc[-1] if x.notna().any() else np.nan),
).reset_index()

co_agg["ln_funding"] = np.log(co_agg["total_funding"].clip(lower=0.01))
co_agg["first_deal_year"] = co_agg["first_deal_date"].dt.year
co_agg["founding_year"] = co_agg["year_established"].astype(float)
co_agg["is_realized"] = (co_agg["investment_status"] == "Realized").astype(int)
co_agg = co_agg[co_agg["total_funding"] > 0].copy()

# Industry FE
co_agg["ind_fe"] = co_agg["industry"].fillna("Other").astype("category").cat.codes

# Stage ordinal
stage_map = {"Seed": 1, "Angel": 1, "Series A": 2, "Series B": 3, "Series C": 4,
             "Series D": 5, "Series E": 5, "Growth": 5, "Late Stage": 5}
co_agg["stage_ord"] = co_agg["highest_stage"].map(
    lambda x: max([v for k, v in stage_map.items() if k.lower() in str(x).lower()], default=0) if pd.notna(x) else 0)

print(f"  Companies with funding: {len(co_agg):,}")
print(f"  With observers: {(co_agg['has_observer']==1).sum():,}")
print(f"  Without observers: {(co_agg['has_observer']==0).sum():,}")

# === CEM Matching ===
print("\n--- CEM Matching ---")
co_agg["ind_bin"] = co_agg["ind_fe"]
co_agg["fy_bin"] = (co_agg["founding_year"] // 2 * 2).fillna(-1).astype(int)
co_agg["fdy_bin"] = (co_agg["first_deal_year"] // 2 * 2).fillna(-1).astype(int)
co_agg["fds_bin"] = pd.qcut(co_agg["first_deal_size"].clip(lower=0), q=4, labels=False, duplicates="drop").fillna(-1).astype(int)
co_agg["strata"] = co_agg["ind_bin"].astype(str) + "_" + co_agg["fy_bin"].astype(str) + "_" + co_agg["fdy_bin"].astype(str) + "_" + co_agg["fds_bin"].astype(str)

treated_strata = set(co_agg[co_agg["has_observer"] == 1]["strata"])
co_agg["in_common_strata"] = co_agg["strata"].isin(treated_strata).astype(int)

cem = co_agg[co_agg["in_common_strata"] == 1].copy()
# Limit controls to 3:1
cem_treat = cem[cem["has_observer"] == 1]
cem_ctrl_list = []
for strata in cem_treat["strata"].unique():
    treat_n = (cem_treat["strata"] == strata).sum()
    ctrl_pool = cem[(cem["strata"] == strata) & (cem["has_observer"] == 0)]
    cem_ctrl_list.append(ctrl_pool.head(treat_n * 3))
cem_ctrl = pd.concat(cem_ctrl_list)
cem_matched = pd.concat([cem_treat, cem_ctrl])

print(f"  CEM sample: {len(cem_matched):,} ({(cem_matched['has_observer']==1).sum():,} treated, {(cem_matched['has_observer']==0).sum():,} controls)")

# === Load confirmed investments ===
print("\n--- Loading confirmed investments ---")
vc_inv = pd.read_csv(os.path.join(ciq_dir, "09_vc_portfolio_investments.csv"))
vc_inv["portfolio_companyid"] = vc_inv["portfolio_companyid"].astype(str).str.replace(".0", "", regex=False)
confirmed_cos = set(vc_inv["portfolio_companyid"])

co_agg["confirmed_investment"] = co_agg.apply(
    lambda r: 1 if pd.notna(r.get("ciq_companyid")) and str(r.get("ciq_companyid", "")).replace(".0", "") in confirmed_cos else 0, axis=1)

# Need ciq_companyid in co_agg
co_agg_ciq = us_deals[["name_clean", "ciq_companyid"]].dropna().drop_duplicates("name_clean")
co_agg = co_agg.merge(co_agg_ciq, on="name_clean", how="left")
co_agg["confirmed_investment"] = co_agg["ciq_companyid"].fillna("").astype(str).str.replace(".0", "", regex=False).isin(confirmed_cos).astype(int)

cem_matched = cem_matched.merge(co_agg[["name_clean", "confirmed_investment"]].drop_duplicates(), on="name_clean", how="left")
cem_matched["confirmed_investment"] = cem_matched["confirmed_investment"].fillna(0).astype(int)

n_conf_treat = ((cem_matched["has_observer"] == 1) & (cem_matched["confirmed_investment"] == 1)).sum()
print(f"  CEM treated with confirmed investment: {n_conf_treat:,} of {(cem_matched['has_observer']==1).sum():,}")

# === Regressions ===
print(f"\n\n{'=' * 90}")
print("RESULTS: Full Specification Battery on CEM Matched Sample")
print(f"{'=' * 90}")

yr_dum = pd.get_dummies(cem_matched["fy_bin"], prefix="fy", drop_first=True).astype(float)
ind_dum = pd.get_dummies(cem_matched["ind_fe"], prefix="ind", drop_first=True).astype(float)

def run_all_specs(df, dv, treat, label):
    y = df[dv].dropna()
    if len(y) < 100:
        print(f"    {label}: too few obs ({len(y)})")
        return
    idx = y.index
    X_base = df.loc[idx, [treat]].copy()

    specs = [
        ("HC1", y, sm.add_constant(X_base), "HC1", {}),
        ("Ind-cl", y, sm.add_constant(X_base), "cluster", {"groups": df.loc[idx, "ind_fe"]}),
        ("IndFE+HC1", y, sm.add_constant(pd.concat([X_base, ind_dum.loc[idx]], axis=1)), "HC1", {}),
        ("FyFE+HC1", y, sm.add_constant(pd.concat([X_base, yr_dum.loc[idx]], axis=1)), "HC1", {}),
        ("FyFE+Ind-cl", y, sm.add_constant(pd.concat([X_base, yr_dum.loc[idx]], axis=1)), "cluster", {"groups": df.loc[idx, "ind_fe"]}),
        ("IndFE+FyFE", y, sm.add_constant(pd.concat([X_base, ind_dum.loc[idx], yr_dum.loc[idx]], axis=1)), "HC1", {}),
    ]

    # State clustering if available
    if "state" in df.columns and df.loc[idx, "state"].notna().sum() > len(idx) * 0.5:
        state_codes = df.loc[idx, "state"].fillna("Unknown").astype("category").cat.codes
        specs.append(("State-cl", y, sm.add_constant(pd.concat([X_base, ind_dum.loc[idx]], axis=1)),
                       "cluster", {"groups": state_codes}))

    print(f"\n  {label} (N={len(y):,})")
    for sname, dep, xmat, cov, kwds in specs:
        try:
            m = sm.OLS(dep, xmat).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            b = m.params.get(treat, np.nan)
            p = m.pvalues.get(treat, np.nan)
            s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {sname:<18} b({treat})={b:>8.4f}{s:<3} p={p:.3f}")
        except Exception as e:
            print(f"    {sname:<18} Error: {str(e)[:50]}")

# Main outcomes
for dv, dv_name in [("ln_funding", "ln(Total Funding)"),
                     ("n_rounds", "Number of Rounds"),
                     ("stage_ord", "Highest Stage (ordinal)")]:
    print(f"\n{'─' * 90}")
    print(f"  DV: {dv_name}")
    print(f"{'─' * 90}")

    run_all_specs(cem_matched, dv, "has_observer", f"CEM: has_observer -> {dv_name}")
    run_all_specs(cem_matched, dv, "n_observers", f"CEM: n_observers -> {dv_name}")

    # Confirmed subsample
    cem_conf = cem_matched[(cem_matched["confirmed_investment"] == 1) | (cem_matched["has_observer"] == 0)]
    if (cem_conf["has_observer"] == 1).sum() >= 50:
        run_all_specs(cem_conf, dv, "has_observer", f"CONFIRMED: has_observer -> {dv_name}")

# With funding control for stage
print(f"\n{'─' * 90}")
print("  DV: Highest Stage (controlling for total funding)")
print(f"{'─' * 90}")
cem_matched["ln_fund_ctrl"] = cem_matched["ln_funding"]
y = cem_matched["stage_ord"].dropna()
idx = y.index
X = cem_matched.loc[idx, ["has_observer", "ln_fund_ctrl"]].copy()
X = pd.concat([X, ind_dum.loc[idx], yr_dum.loc[idx]], axis=1)
X = sm.add_constant(X)
try:
    m = sm.OLS(y, X).fit(cov_type="HC1")
    b = m.params.get("has_observer", np.nan)
    p = m.pvalues.get("has_observer", np.nan)
    bf = m.params.get("ln_fund_ctrl", np.nan)
    pf = m.pvalues.get("ln_fund_ctrl", np.nan)
    s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"  IndFE+FyFE+FundCtrl: b(has_obs)={b:.4f}{s} p={p:.3f}  b(ln_fund)={bf:.4f} p={pf:.3f}")
except Exception as e:
    print(f"  Error: {str(e)[:50]}")

# Dose-response with full specs
print(f"\n{'─' * 90}")
print("  DOSE-RESPONSE: 1 vs 2 vs 3+ observers")
print(f"{'─' * 90}")
cem_matched["obs_1"] = (cem_matched["n_observers"] == 1).astype(int)
cem_matched["obs_2"] = (cem_matched["n_observers"] == 2).astype(int)
cem_matched["obs_3plus"] = (cem_matched["n_observers"] >= 3).astype(int)

for dv, name in [("ln_funding", "ln(Funding)"), ("n_rounds", "Rounds")]:
    y = cem_matched[dv].dropna()
    idx = y.index
    X = cem_matched.loc[idx, ["obs_1", "obs_2", "obs_3plus"]].copy()
    X = pd.concat([X, ind_dum.loc[idx], yr_dum.loc[idx]], axis=1)
    X = sm.add_constant(X)
    try:
        m = sm.OLS(y, X).fit(cov_type="HC1")
        b1 = m.params.get("obs_1", np.nan)
        b2 = m.params.get("obs_2", np.nan)
        b3 = m.params.get("obs_3plus", np.nan)
        p1 = m.pvalues.get("obs_1", np.nan)
        p2 = m.pvalues.get("obs_2", np.nan)
        p3 = m.pvalues.get("obs_3plus", np.nan)
        s1 = "***" if p1 < 0.01 else "**" if p1 < 0.05 else "*" if p1 < 0.10 else ""
        s2 = "***" if p2 < 0.01 else "**" if p2 < 0.05 else "*" if p2 < 0.10 else ""
        s3 = "***" if p3 < 0.01 else "**" if p3 < 0.05 else "*" if p3 < 0.10 else ""
        print(f"  {name}: 1obs={b1:.3f}{s1} p={p1:.3f}  2obs={b2:.3f}{s2} p={p2:.3f}  3+obs={b3:.3f}{s3} p={p3:.3f}")
    except Exception as e:
        print(f"  {name}: Error: {str(e)[:50]}")

print("\n\nDone.")
