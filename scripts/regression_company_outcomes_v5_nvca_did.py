"""Company outcomes: NVCA 2020 and Clayton Act 2025 DiD on deal sizes."""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, numpy as np, os, re
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

# Load timing (same as v4)
func = pd.read_csv(os.path.join(ciq_dir, "11_observer_position_dates.csv"))
proid_map = pd.read_csv(os.path.join(ciq_dir, "11b_observer_proid_mapping.csv"))
proid_map["proid"] = proid_map["proid"].astype(str).str.replace(".0", "", regex=False)
proid_map["personid"] = proid_map["personid"].astype(str).str.replace(".0", "", regex=False)
proid_map["companyid"] = proid_map["companyid"].astype(str).str.replace(".0", "", regex=False)
func["proid"] = func["proid"].astype(str).str.replace(".0", "", regex=False)
func = func.merge(proid_map[["proid", "personid", "companyid"]], on="proid", how="left")
func["start_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['startyear'])}-01-01" if pd.notna(r.get("startyear")) else None, axis=1),
    errors="coerce")

obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_us = obs[obs["companyid"].isin(us_priv)]

obs_func = func[func["personid"].isin(set(obs_us["personid"])) & func["companyid"].isin(us_priv)]
co_start = obs_func.groupby("companyid")["start_date"].min().reset_index()
co_start.columns = ["companyid", "obs_arrival"]
co_start = co_start.dropna()

trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))
trans["companyid"] = trans["companyid"].astype(str).str.replace(".0", "", regex=False)
trans["close_date"] = pd.to_datetime(
    trans.apply(lambda r: f"{int(r['closingyear'])}-{int(r['closingmonth']):02d}-{int(r['closingday']):02d}"
                if pd.notna(r.get("closingyear")) and pd.notna(r.get("closingmonth")) and pd.notna(r.get("closingday"))
                else None, axis=1), errors="coerce")
first_trans = trans.groupby("companyid")["close_date"].min().reset_index()
first_trans.columns = ["companyid", "first_deal"]

timing = co_start.merge(first_trans, on="companyid", how="outer")
timing["obs_arrival"] = timing["obs_arrival"].fillna(timing["first_deal"])
timing = timing.dropna(subset=["obs_arrival"])

# Preqin deals
deals = pd.read_csv(os.path.join(preqin_dir, "vc_deals_full.csv"), low_memory=False)
deals["deal_date"] = pd.to_datetime(deals["deal_date"], errors="coerce")
us_deals = deals[deals["portfolio_company_country"].fillna("").str.contains("US|United States", case=False)].copy()

def clean_name(s):
    if not isinstance(s, str): return ""
    s = re.sub(r'[,.\-\'\"&()!]', ' ', s.lower().strip())
    for suf in [" inc", " llc", " corp", " ltd"]: s = s.replace(suf, "")
    return re.sub(r'\s+', ' ', s).strip()

co_det = co[co["companyid"].astype(str).str.replace(".0","",regex=False).isin(us_priv)].copy()
co_det["companyid"] = co_det["companyid"].astype(str).str.replace(".0", "", regex=False)
co_det["nc"] = co_det["companyname"].apply(clean_name)
us_deals["nc"] = us_deals["portfolio_company_name"].fillna("").apply(clean_name)
ciq_names = dict(zip(co_det["nc"], co_det["companyid"]))
us_deals["ciq_cid"] = us_deals["nc"].map(ciq_names)

md = us_deals[us_deals["ciq_cid"].isin(set(timing["companyid"]))].copy()
md["obs_arrival"] = md["ciq_cid"].map(dict(zip(timing["companyid"], timing["obs_arrival"])))
md = md.dropna(subset=["deal_date", "obs_arrival"])
md["post_observer"] = (md["deal_date"] >= md["obs_arrival"]).astype(int)
md["ln_deal"] = np.log(md["deal_financing_size_usd"].clip(lower=0.01))
md["has_size"] = md["deal_financing_size_usd"].notna() & (md["deal_financing_size_usd"] > 0)
md["deal_year"] = md["deal_date"].dt.year
md["post_2020"] = (md["deal_year"] >= 2020).astype(int)
md["post_jan2025"] = (md["deal_date"] >= pd.Timestamp("2025-01-01")).astype(int)
md["postobs_x_post2020"] = md["post_observer"] * md["post_2020"]
md["postobs_x_post2025"] = md["post_observer"] * md["post_jan2025"]

sized = md[md["has_size"]].copy()

print("=" * 90)
print("REGULATORY SHOCK DiD ON COMPANY OUTCOMES")
print("=" * 90)
print(f"\nSample: {len(sized):,} deals, {sized['ciq_cid'].nunique():,} companies")
print(f"  Pre-2020: {(sized['deal_year']<2020).sum():,}")
print(f"  2020-2024: {((sized['deal_year']>=2020)&(sized['deal_year']<2025)).sum():,}")
print(f"  2025+: {(sized['deal_year']>=2025).sum():,}")

# NVCA 2020 DiD
print("\n--- NVCA 2020 DiD: post_observer x post_2020 ---")
y = sized["ln_deal"].dropna()
idx = y.index
yr = pd.get_dummies(sized.loc[idx, "deal_year"], prefix="dy", drop_first=True).astype(float)

for label, xv, Xe, cov, kw in [
    ("HC1", ["post_observer", "post_2020", "postobs_x_post2020"], None, "HC1", {}),
    ("YrFE+HC1", ["post_observer", "postobs_x_post2020"], yr, "HC1", {}),
    ("Co-cl", ["post_observer", "post_2020", "postobs_x_post2020"], None, "cluster", {"groups": sized.loc[idx, "ciq_cid"]}),
    ("YrFE+Co-cl", ["post_observer", "postobs_x_post2020"], yr, "cluster", {"groups": sized.loc[idx, "ciq_cid"]}),
]:
    X = sized.loc[idx, xv].copy()
    if Xe is not None: X = pd.concat([X, Xe.loc[idx]], axis=1)
    X = sm.add_constant(X)
    try:
        m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kw if kw else {})
        bo = m.params.get("post_observer", np.nan); po = m.pvalues.get("post_observer", np.nan)
        bd = m.params.get("postobs_x_post2020", np.nan); pd_ = m.pvalues.get("postobs_x_post2020", np.nan)
        so = "***" if po<0.01 else "**" if po<0.05 else "*" if po<0.10 else ""
        sd = "***" if pd_<0.01 else "**" if pd_<0.05 else "*" if pd_<0.10 else ""
        print(f"  {label:<15} b(post_obs)={bo:>7.3f}{so:<3} p={po:.3f}  b(obs x post2020)={bd:>7.3f}{sd:<3} p={pd_:.3f}")
    except Exception as e:
        print(f"  {label:<15} Error: {str(e)[:50]}")

# Company FE
cm = sized.loc[idx].groupby("ciq_cid")["ln_deal"].transform("mean")
yfe = y - cm.loc[idx]
Xfe = sized.loc[idx, ["post_observer", "post_2020", "postobs_x_post2020"]].copy()
Xfe = sm.add_constant(Xfe)
try:
    m = sm.OLS(yfe, Xfe).fit(cov_type="HC1")
    bo = m.params.get("post_observer", np.nan); po = m.pvalues.get("post_observer", np.nan)
    bd = m.params.get("postobs_x_post2020", np.nan); pd_ = m.pvalues.get("postobs_x_post2020", np.nan)
    so = "***" if po<0.01 else "**" if po<0.05 else "*" if po<0.10 else ""
    sd = "***" if pd_<0.01 else "**" if pd_<0.05 else "*" if pd_<0.10 else ""
    print(f"  {'CoFE+HC1':<15} b(post_obs)={bo:>7.3f}{so:<3} p={po:.3f}  b(obs x post2020)={bd:>7.3f}{sd:<3} p={pd_:.3f}")
except Exception as e:
    print(f"  CoFE: Error: {str(e)[:50]}")

# Means
print("\n  Deal size means:")
for period, mask in [("Pre-2020", sized["deal_year"]<2020), ("Post-2020", sized["deal_year"]>=2020)]:
    sub = sized[mask]
    bef = sub[sub["post_observer"]==0]["deal_financing_size_usd"]
    aft = sub[sub["post_observer"]==1]["deal_financing_size_usd"]
    if len(bef)>0 and len(aft)>0:
        print(f"    {period}: Before=${bef.mean():.1f}M (N={len(bef):,})  After=${aft.mean():.1f}M (N={len(aft):,})  Diff=${aft.mean()-bef.mean():+.1f}M")

# Clayton Act 2025
print("\n--- Clayton Act 2025 DiD (post-2020 subsample) ---")
p20 = sized[sized["deal_year"] >= 2020].copy()
y2 = p20["ln_deal"].dropna()
i2 = y2.index
for label, xv, cov, kw in [
    ("HC1", ["post_observer","post_jan2025","postobs_x_post2025"], "HC1", {}),
    ("Co-cl", ["post_observer","post_jan2025","postobs_x_post2025"], "cluster", {"groups": p20.loc[i2,"ciq_cid"]}),
]:
    X = p20.loc[i2, xv].copy()
    X = sm.add_constant(X)
    try:
        m = sm.OLS(y2, X).fit(cov_type=cov, cov_kwds=kw if kw else {})
        bo = m.params.get("post_observer", np.nan); po = m.pvalues.get("post_observer", np.nan)
        bd = m.params.get("postobs_x_post2025", np.nan); pd_ = m.pvalues.get("postobs_x_post2025", np.nan)
        so = "***" if po<0.01 else "**" if po<0.05 else "*" if po<0.10 else ""
        sd = "***" if pd_<0.01 else "**" if pd_<0.05 else "*" if pd_<0.10 else ""
        print(f"  {label:<15} b(post_obs)={bo:>7.3f}{so:<3} p={po:.3f}  b(obs x post2025)={bd:>7.3f}{sd:<3} p={pd_:.3f}")
    except Exception as e:
        print(f"  {label:<15} Error: {str(e)[:50]}")

# Means
for period, mask in [("2020-2024", (p20["deal_year"]>=2020)&(p20["deal_year"]<2025)), ("2025+", p20["deal_year"]>=2025)]:
    sub = p20[mask]
    bef = sub[sub["post_observer"]==0]["deal_financing_size_usd"]
    aft = sub[sub["post_observer"]==1]["deal_financing_size_usd"]
    if len(bef)>0 and len(aft)>0:
        print(f"    {period}: Before=${bef.mean():.1f}M (N={len(bef):,})  After=${aft.mean():.1f}M (N={len(aft):,})")

print("\nDone.")
