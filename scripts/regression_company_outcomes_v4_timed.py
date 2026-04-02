"""
Company outcomes v4: Apply position dates to test BEFORE vs AFTER observer arrival.

Key test: do companies raise more AFTER the observer is placed, compared to before?
Uses ciqprotoprofunction start dates (22.8% coverage) and ciqtransaction deal dates
as proxies for when the observer arrived.

Design:
  For each company with an observer start date:
    - Count funding rounds BEFORE observer arrived
    - Count funding rounds AFTER observer arrived
    - Compare round sizes, timing, stage progression before vs after

  Also: within-company comparison (company FE):
    - Each company serves as its own control
    - Does round size increase after observer placement?
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os, re
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

print("=" * 90)
print("COMPANY OUTCOMES v4: Before vs After Observer Arrival")
print("=" * 90)

# === Load observer position dates ===
print("\n--- Loading position dates ---")
func = pd.read_csv(os.path.join(ciq_dir, "11_observer_position_dates.csv"))
proid_map = pd.read_csv(os.path.join(ciq_dir, "11b_observer_proid_mapping.csv"))
proid_map["proid"] = proid_map["proid"].astype(str).str.replace(".0", "", regex=False)
proid_map["personid"] = proid_map["personid"].astype(str).str.replace(".0", "", regex=False)
proid_map["companyid"] = proid_map["companyid"].astype(str).str.replace(".0", "", regex=False)
func["proid"] = func["proid"].astype(str).str.replace(".0", "", regex=False)
func = func.merge(proid_map[["proid", "personid", "companyid"]], on="proid", how="left")

# Observer start date at each company
func["start_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['startyear'])}-{int(r['startmonth']):02d}-01"
               if pd.notna(r.get("startyear")) and pd.notna(r.get("startmonth"))
               else (f"{int(r['startyear'])}-01-01" if pd.notna(r.get("startyear")) else None), axis=1),
    errors="coerce")

# Earliest observer start per company
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_us = obs[obs["companyid"].isin(us_priv)]

# For each observed company, get the earliest observer start date
obs_pids = set(obs_us["personid"])
obs_func = func[func["personid"].isin(obs_pids) & func["companyid"].isin(us_priv)]
company_obs_start = obs_func.groupby("companyid")["start_date"].min().reset_index()
company_obs_start.columns = ["companyid", "observer_start_date"]
company_obs_start = company_obs_start.dropna(subset=["observer_start_date"])

print(f"  Companies with observer start date: {len(company_obs_start):,}")
print(f"  Date range: {company_obs_start['observer_start_date'].min().date()} to {company_obs_start['observer_start_date'].max().date()}")

# Also use transaction dates as fallback
trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))
trans["companyid"] = trans["companyid"].astype(str).str.replace(".0", "", regex=False)
trans["close_date"] = pd.to_datetime(
    trans.apply(lambda r: f"{int(r['closingyear'])}-{int(r['closingmonth']):02d}-{int(r['closingday']):02d}"
                if pd.notna(r.get("closingyear")) and pd.notna(r.get("closingmonth")) and pd.notna(r.get("closingday"))
                else None, axis=1), errors="coerce")
first_trans = trans.groupby("companyid")["close_date"].min().reset_index()
first_trans.columns = ["companyid", "first_deal_date"]

# Merge: prefer position date, fallback to deal date
company_timing = company_obs_start.merge(first_trans, on="companyid", how="outer")
company_timing["obs_arrival"] = company_timing["observer_start_date"].fillna(company_timing["first_deal_date"])
company_timing = company_timing.dropna(subset=["obs_arrival"])
print(f"  Companies with any timing estimate: {len(company_timing):,}")
print(f"    From position dates: {company_timing['observer_start_date'].notna().sum():,}")
print(f"    From deal dates (fallback): {(company_timing['observer_start_date'].isna() & company_timing['first_deal_date'].notna()).sum():,}")

# === Load Preqin deals ===
print("\n--- Loading Preqin deals ---")
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

# Match CIQ companies to Preqin deals
co_det = co[co["companyid"].astype(str).str.replace(".0", "", regex=False).isin(us_priv)].copy()
co_det["companyid"] = co_det["companyid"].astype(str).str.replace(".0", "", regex=False)
co_det["name_clean"] = co_det["companyname"].apply(clean_name)
us_deals["name_clean"] = us_deals["portfolio_company_name"].fillna("").apply(clean_name)

ciq_names = dict(zip(co_det["name_clean"], co_det["companyid"]))
us_deals["ciq_companyid"] = us_deals["name_clean"].map(ciq_names)

# Keep only deals at companies with timing info
timing_cids = set(company_timing["companyid"])
matched_deals = us_deals[us_deals["ciq_companyid"].isin(timing_cids)].copy()
matched_deals = matched_deals.merge(company_timing[["companyid", "obs_arrival"]],
                                     left_on="ciq_companyid", right_on="companyid", how="left")
matched_deals = matched_deals.dropna(subset=["deal_date", "obs_arrival"])

# Flag before vs after observer
matched_deals["post_observer"] = (matched_deals["deal_date"] >= matched_deals["obs_arrival"]).astype(int)

print(f"  Deals at companies with timing: {len(matched_deals):,}")
print(f"  Unique companies: {matched_deals['ciq_companyid'].nunique():,}")
print(f"  Deals before observer: {(matched_deals['post_observer']==0).sum():,}")
print(f"  Deals after observer: {(matched_deals['post_observer']==1).sum():,}")

# === Deal-level analysis ===
print(f"\n\n{'=' * 90}")
print("RESULTS")
print(f"{'=' * 90}")

matched_deals["ln_deal_size"] = np.log(matched_deals["deal_financing_size_usd"].clip(lower=0.01))
matched_deals["has_size"] = matched_deals["deal_financing_size_usd"].notna() & (matched_deals["deal_financing_size_usd"] > 0)
matched_deals["deal_year"] = matched_deals["deal_date"].dt.year

# Stage ordinal
stage_map = {"Seed": 1, "Angel": 1, "Pre-Seed": 0.5, "Series A": 2, "Series B": 3,
             "Series C": 4, "Series D": 5, "Series E": 6, "Growth": 5, "Late Stage": 5,
             "Venture Debt": 2.5}
matched_deals["stage_ord"] = matched_deals["stage"].fillna("").apply(
    lambda x: max([v for k, v in stage_map.items() if k.lower() in x.lower()], default=np.nan) if x else np.nan)

# Time to next round
matched_deals = matched_deals.sort_values(["ciq_companyid", "deal_date"])
matched_deals["next_deal_date"] = matched_deals.groupby("ciq_companyid")["deal_date"].shift(-1)
matched_deals["days_to_next"] = (matched_deals["next_deal_date"] - matched_deals["deal_date"]).dt.days

# --- Test 1: Deal size before vs after observer ---
print(f"\n--- Test 1: Deal size before vs after observer ---")

sized_deals = matched_deals[matched_deals["has_size"]].copy()
print(f"  Deals with size: {len(sized_deals):,}")
print(f"  Before observer: {(sized_deals['post_observer']==0).sum():,}, mean=${sized_deals[sized_deals['post_observer']==0]['deal_financing_size_usd'].mean():.1f}M")
print(f"  After observer: {(sized_deals['post_observer']==1).sum():,}, mean=${sized_deals[sized_deals['post_observer']==1]['deal_financing_size_usd'].mean():.1f}M")

y = sized_deals["ln_deal_size"].dropna()
idx = y.index

# Spec battery
yr_dum = pd.get_dummies(sized_deals.loc[idx, "deal_year"], prefix="dy", drop_first=True).astype(float)

for label, X_extra, cov, kwds in [
    ("HC1", None, "HC1", {}),
    ("Year FE + HC1", yr_dum, "HC1", {}),
    ("Company-cl", None, "cluster", {"groups": sized_deals.loc[idx, "ciq_companyid"]}),
    ("Year FE + Company-cl", yr_dum, "cluster", {"groups": sized_deals.loc[idx, "ciq_companyid"]}),
]:
    X = sized_deals.loc[idx, ["post_observer"]].copy()
    if X_extra is not None:
        X = pd.concat([X, X_extra.loc[idx]], axis=1)
    X = sm.add_constant(X)
    try:
        m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
        b = m.params.get("post_observer", np.nan)
        p = m.pvalues.get("post_observer", np.nan)
        s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {label:<25} b(post_observer)={b:>8.4f}{s:<3} p={p:.3f}")
    except Exception as e:
        print(f"  {label:<25} Error: {str(e)[:50]}")

# Company FE (within-company variation)
n_firms = sized_deals.loc[idx, "ciq_companyid"].nunique()
if n_firms >= 20:
    co_mean = sized_deals.loc[idx].groupby("ciq_companyid")["ln_deal_size"].transform("mean")
    y_fe = y - co_mean.loc[idx]
    X_fe = sized_deals.loc[idx, ["post_observer"]].copy()
    X_fe = sm.add_constant(X_fe)
    try:
        m = sm.OLS(y_fe, X_fe).fit(cov_type="HC1")
        b = m.params.get("post_observer", np.nan)
        p = m.pvalues.get("post_observer", np.nan)
        s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {'Company FE + HC1':<25} b(post_observer)={b:>8.4f}{s:<3} p={p:.3f}  ({n_firms} companies)")
    except Exception as e:
        print(f"  Company FE: Error: {str(e)[:50]}")

# --- Test 2: Time to next round before vs after ---
print(f"\n--- Test 2: Days to next round before vs after observer ---")

timed = matched_deals.dropna(subset=["days_to_next"])
timed = timed[(timed["days_to_next"] > 0) & (timed["days_to_next"] < 3650)]  # cap at 10 years
print(f"  Deals with next-round timing: {len(timed):,}")
print(f"  Before observer: mean {timed[timed['post_observer']==0]['days_to_next'].mean():.0f} days")
print(f"  After observer: mean {timed[timed['post_observer']==1]['days_to_next'].mean():.0f} days")

y = timed["days_to_next"]
idx = y.index
yr_dum_t = pd.get_dummies(timed.loc[idx, "deal_year"], prefix="dy", drop_first=True).astype(float)

for label, X_extra, cov, kwds in [
    ("HC1", None, "HC1", {}),
    ("Year FE + HC1", yr_dum_t, "HC1", {}),
    ("Company-cl", None, "cluster", {"groups": timed.loc[idx, "ciq_companyid"]}),
    ("Year FE + Company-cl", yr_dum_t, "cluster", {"groups": timed.loc[idx, "ciq_companyid"]}),
]:
    X = timed.loc[idx, ["post_observer"]].copy()
    if X_extra is not None:
        X = pd.concat([X, X_extra.loc[idx]], axis=1)
    X = sm.add_constant(X)
    try:
        m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
        b = m.params.get("post_observer", np.nan)
        p = m.pvalues.get("post_observer", np.nan)
        s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {label:<25} b(post_observer)={b:>8.1f}{s:<3} p={p:.3f}")
    except Exception as e:
        print(f"  {label:<25} Error: {str(e)[:50]}")

# Company FE
if timed["ciq_companyid"].nunique() >= 20:
    co_mean = timed.groupby("ciq_companyid")["days_to_next"].transform("mean")
    y_fe = y - co_mean
    X_fe = timed[["post_observer"]].copy()
    X_fe = sm.add_constant(X_fe)
    try:
        m = sm.OLS(y_fe, X_fe).fit(cov_type="HC1")
        b = m.params.get("post_observer", np.nan)
        p = m.pvalues.get("post_observer", np.nan)
        s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {'Company FE + HC1':<25} b(post_observer)={b:>8.1f}{s:<3} p={p:.3f}")
    except Exception as e:
        print(f"  Company FE: Error: {str(e)[:50]}")

# --- Test 3: Stage progression before vs after ---
print(f"\n--- Test 3: Stage before vs after observer ---")

staged = matched_deals.dropna(subset=["stage_ord"])
print(f"  Deals with stage: {len(staged):,}")
print(f"  Before: mean stage {staged[staged['post_observer']==0]['stage_ord'].mean():.2f}")
print(f"  After: mean stage {staged[staged['post_observer']==1]['stage_ord'].mean():.2f}")

y = staged["stage_ord"]
idx = y.index
for label, cov, kwds in [
    ("HC1", "HC1", {}),
    ("Company-cl", "cluster", {"groups": staged.loc[idx, "ciq_companyid"]}),
]:
    X = staged.loc[idx, ["post_observer"]].copy()
    X = sm.add_constant(X)
    try:
        m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
        b = m.params.get("post_observer", np.nan)
        p = m.pvalues.get("post_observer", np.nan)
        s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {label:<25} b(post_observer)={b:>8.4f}{s:<3} p={p:.3f}")
    except Exception as e:
        print(f"  {label:<25} Error: {str(e)[:50]}")

# Company FE
if staged["ciq_companyid"].nunique() >= 20:
    co_mean = staged.groupby("ciq_companyid")["stage_ord"].transform("mean")
    y_fe = y - co_mean
    X_fe = staged[["post_observer"]].copy()
    X_fe = sm.add_constant(X_fe)
    try:
        m = sm.OLS(y_fe, X_fe).fit(cov_type="HC1")
        b = m.params.get("post_observer", np.nan)
        p = m.pvalues.get("post_observer", np.nan)
        s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {'Company FE + HC1':<25} b(post_observer)={b:>8.4f}{s:<3} p={p:.3f}")
    except Exception as e:
        print(f"  Company FE: Error: {str(e)[:50]}")

# --- Summary ---
print(f"\n\n{'=' * 90}")
print("SUMMARY: Before vs After Observer Arrival")
print(f"{'=' * 90}")

for col, name in [("deal_financing_size_usd", "Deal size ($M)"),
                   ("days_to_next", "Days to next round"),
                   ("stage_ord", "Stage (ordinal)")]:
    sub = matched_deals.dropna(subset=[col])
    if col == "days_to_next":
        sub = sub[(sub[col] > 0) & (sub[col] < 3650)]
    before = sub[sub["post_observer"] == 0][col]
    after = sub[sub["post_observer"] == 1][col]
    print(f"  {name:<25} Before: {before.mean():>8.1f} (N={len(before):,})  After: {after.mean():>8.1f} (N={len(after):,})  Diff: {after.mean()-before.mean():>+8.1f}")

print("\n\nDone.")
