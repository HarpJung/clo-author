"""
Test whether VC fund performance responds to events at observed portfolio companies.

Design:
  - Unit of observation: fund-quarter
  - DV: Change in fund multiple (TVPI) or IRR from quarter t-1 to t
  - Treatment: Material events at observed companies in quarter t
  - Controls: Fund vintage, fund size, benchmark performance
  - DiD: Event intensity × post-2020 (NVCA fiduciary language removal)

Uses only high+medium quality Preqin matches (drops substring "low" quality).
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

print("=" * 80)
print("VC FUND PERFORMANCE TEST")
print("=" * 80)

# =====================================================================
# STEP 1: Load Preqin crosswalk (high + medium quality only)
# =====================================================================
print("\n--- Step 1: Load validated crosswalk ---")
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
print(f"  All matches: {len(xwalk):,}")
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
print(f"  High+Medium only: {len(xwalk):,}")
print(f"  Unique CIQ VCs: {xwalk['ciq_vc_companyid'].nunique():,}")
print(f"  Unique Preqin firms: {xwalk['preqin_firm_id'].nunique():,}")

# =====================================================================
# STEP 2: Load fund details and performance
# =====================================================================
print("\n--- Step 2: Load fund details + performance ---")
funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)

# Filter to matched firms
matched_firm_ids = set(xwalk["preqin_firm_id"].dropna().astype(int))
funds = funds[funds["firm_id"].isin(matched_firm_ids)].copy()
print(f"  Funds at matched firms: {len(funds):,}")

# Filter to VC/Seed/Early stage funds
vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)].copy()
print(f"  VC/Seed/Early funds: {len(vc_funds):,}")

# Get performance for these funds
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))
perf = perf[perf["fund_id"].isin(vc_fund_ids)].copy()
perf["date_reported"] = pd.to_datetime(perf["date_reported"], errors="coerce")
perf["multiple_num"] = pd.to_numeric(perf["multiple"], errors="coerce")
perf["irr_num"] = pd.to_numeric(perf["net_irr_pcent"], errors="coerce")
perf["called_num"] = pd.to_numeric(perf["called_pcent"], errors="coerce")
perf["dpi_num"] = pd.to_numeric(perf["distr_dpi_pcent"], errors="coerce")
perf = perf.dropna(subset=["date_reported"])
perf = perf.sort_values(["fund_id", "date_reported"])

print(f"  Performance records: {len(perf):,}")
print(f"  Funds with performance: {perf['fund_id'].nunique():,}")
print(f"  With multiple: {perf['multiple_num'].notna().sum():,}")
print(f"  With IRR: {perf['irr_num'].notna().sum():,}")
print(f"  Date range: {perf['date_reported'].min().date()} to {perf['date_reported'].max().date()}")

# Compute quarter-over-quarter changes in multiple and IRR
perf["prev_multiple"] = perf.groupby("fund_id")["multiple_num"].shift(1)
perf["prev_irr"] = perf.groupby("fund_id")["irr_num"].shift(1)
perf["delta_multiple"] = perf["multiple_num"] - perf["prev_multiple"]
perf["delta_irr"] = perf["irr_num"] - perf["prev_irr"]
perf["quarter"] = perf["date_reported"].dt.to_period("Q")
perf["year"] = perf["date_reported"].dt.year

# Merge fund details
perf = perf.merge(vc_funds[["fund_id", "firm_id", "fund_name", "firm_name",
                             "vintage", "fund_type", "final_size_usd", "industry"]],
                   on="fund_id", how="left")

# Merge CIQ VC companyid via crosswalk
firm_to_ciq = xwalk.drop_duplicates("preqin_firm_id").set_index("preqin_firm_id")["ciq_vc_companyid"].to_dict()
perf["ciq_vc_companyid"] = perf["firm_id"].map(firm_to_ciq)

print(f"\n  Fund-quarters with delta_multiple: {perf['delta_multiple'].notna().sum():,}")
print(f"  Fund-quarters with delta_irr: {perf['delta_irr'].notna().sum():,}")

# =====================================================================
# STEP 3: Load events at observed companies
# =====================================================================
print("\n--- Step 3: Load events ---")

# Observer network: VC -> observed companies
tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)
tb["observed_companyid"] = tb["observed_companyid"].astype(str).str.replace(".0", "", regex=False)

# Build VC -> set of observed companies
vc_to_obs_cos = {}
for _, r in tb.iterrows():
    vc = r["vc_firm_companyid"]
    if vc not in vc_to_obs_cos:
        vc_to_obs_cos[vc] = set()
    vc_to_obs_cos[vc].add(r["observed_companyid"])

# Events
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])

# US private filter
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_private = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_private)]

# Drop noise
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
events["quarter"] = events["announcedate"].dt.to_period("Q")

print(f"  Events (US private, filtered): {len(events):,}")

# Classify material events
def is_material(et, role):
    et = str(et)
    role = str(role)
    if "M&A" in et:
        return True
    if "Bankruptcy" in et:
        return True
    if et == "Executive/Board Changes - Other":
        return True
    if "Restructuring" in et or "Downsizing" in et:
        return True
    return False

events["material"] = events.apply(lambda r: is_material(r["eventtype"], r.get("objectroletype", "")), axis=1)
material_events = events[events["material"]]
print(f"  Material events: {len(material_events):,}")

# =====================================================================
# STEP 4: Count events per VC per quarter
# =====================================================================
print("\n--- Step 4: Count events per VC-quarter ---")

# For each VC, for each quarter, count events at observed companies
vc_quarter_events = []

# Get unique CIQ VC IDs that are in our matched Preqin set
matched_ciq_vcs = set(xwalk["ciq_vc_companyid"].astype(str))

for vc_cid in matched_ciq_vcs:
    obs_cos = vc_to_obs_cos.get(vc_cid, set())
    if not obs_cos:
        continue

    # Events at this VC's observed companies
    vc_events = material_events[material_events["companyid"].isin(obs_cos)]
    if len(vc_events) == 0:
        continue

    # Count by quarter
    qcounts = vc_events.groupby("quarter").size().reset_index(name="n_events")

    # Also count by type
    ma_events = vc_events[vc_events["eventtype"].str.contains("M&A", na=False)]
    ma_counts = ma_events.groupby("quarter").size().reset_index(name="n_ma_events")

    qcounts = qcounts.merge(ma_counts, on="quarter", how="left")
    qcounts["n_ma_events"] = qcounts["n_ma_events"].fillna(0).astype(int)
    qcounts["ciq_vc_companyid"] = vc_cid

    vc_quarter_events.append(qcounts)

if vc_quarter_events:
    event_counts = pd.concat(vc_quarter_events, ignore_index=True)
    print(f"  VC-quarter event counts: {len(event_counts):,}")
    print(f"  Unique VCs with events: {event_counts['ciq_vc_companyid'].nunique():,}")
    print(f"  Quarters with events: {event_counts['quarter'].nunique():,}")
else:
    print("  No events found!")
    event_counts = pd.DataFrame()

# =====================================================================
# STEP 5: Merge events with fund performance
# =====================================================================
print("\n--- Step 5: Merge events with fund performance ---")

perf["ciq_vc_companyid"] = perf["ciq_vc_companyid"].astype(str)
perf["quarter"] = perf["date_reported"].dt.to_period("Q")

# Merge: for each fund-quarter, attach the event count for that VC in that quarter
panel = perf.merge(event_counts, on=["ciq_vc_companyid", "quarter"], how="left")
panel["n_events"] = panel["n_events"].fillna(0).astype(int)
panel["n_ma_events"] = panel["n_ma_events"].fillna(0).astype(int)
panel["has_event"] = (panel["n_events"] > 0).astype(int)
panel["has_ma_event"] = (panel["n_ma_events"] > 0).astype(int)
panel["post_2020"] = (panel["year"] >= 2020).astype(int)
panel["event_x_post2020"] = panel["has_event"] * panel["post_2020"]
panel["ma_x_post2020"] = panel["has_ma_event"] * panel["post_2020"]

# Log fund size
panel["ln_size"] = np.log(panel["final_size_usd"].clip(lower=1))

print(f"  Fund-quarter panel: {len(panel):,}")
print(f"  With delta_multiple: {panel['delta_multiple'].notna().sum():,}")
print(f"  With delta_irr: {panel['delta_irr'].notna().sum():,}")
print(f"  Quarters with event: {panel['has_event'].sum():,} ({panel['has_event'].mean()*100:.1f}%)")
print(f"  Quarters with M&A event: {panel['has_ma_event'].sum():,}")

# =====================================================================
# STEP 6: Regressions
# =====================================================================
print(f"\n\n{'=' * 80}")
print("REGRESSION RESULTS")
print(f"{'=' * 80}")

# Winsorize DVs
for col in ["delta_multiple", "delta_irr"]:
    valid = panel[col].dropna()
    if len(valid) > 100:
        lo, hi = valid.quantile([0.01, 0.99])
        panel[col] = panel[col].clip(lo, hi)

# Year dummies
year_dummies = pd.get_dummies(panel["year"], prefix="yr", drop_first=True).astype(float)

dvs = [("delta_multiple", "Change in Multiple (TVPI)"),
       ("delta_irr", "Change in IRR (pp)")]

treatments = [
    ("has_event", "Any Material Event"),
    ("n_events", "Number of Events"),
    ("has_ma_event", "M&A Event"),
]

for dv_col, dv_name in dvs:
    print(f"\n{'─' * 80}")
    print(f"  DV: {dv_name}")
    print(f"{'─' * 80}")

    for treat_col, treat_name in treatments:
        print(f"\n  Treatment: {treat_name}")

        # (A) Baseline: has_event -> delta_multiple
        y = panel[dv_col].dropna()
        base_x = panel.loc[y.index, [treat_col, "post_2020"]].copy()
        base_x["treat_x_post2020"] = panel.loc[y.index, treat_col] * panel.loc[y.index, "post_2020"]

        # Add controls
        controls = panel.loc[y.index, ["ln_size"]].copy()
        # vintage comes from either the perf table or fund details merge
        if "vintage_y" in panel.columns:
            controls["vintage"] = panel.loc[y.index, "vintage_y"].fillna(0)
        elif "vintage_x" in panel.columns:
            controls["vintage"] = panel.loc[y.index, "vintage_x"].fillna(0)
        elif "vintage" in panel.columns:
            controls["vintage"] = panel.loc[y.index, "vintage"].fillna(0)
        else:
            controls["vintage"] = 0
        controls["called_num"] = panel.loc[y.index, "called_num"]
        controls = controls.fillna(0)

        X = pd.concat([base_x, controls, year_dummies.loc[y.index]], axis=1)
        X = sm.add_constant(X)

        if len(y) < 100:
            print(f"    Too few obs ({len(y)})")
            continue

        # Spec 1: HC1
        try:
            m = sm.OLS(y, X).fit(cov_type="HC1")
            bt = m.params.get(treat_col, np.nan)
            pt = m.pvalues.get(treat_col, np.nan)
            bi = m.params.get("treat_x_post2020", np.nan)
            pi = m.pvalues.get("treat_x_post2020", np.nan)
            st = "***" if pt < 0.01 else "**" if pt < 0.05 else "*" if pt < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            print(f"    HC1 (N={len(y):,}):  b({treat_col})={bt:.4f}{st} (p={pt:.3f})  b(treat x post2020)={bi:.4f}{si} (p={pi:.3f})")
        except Exception as e:
            print(f"    HC1 Error: {str(e)[:60]}")

        # Spec 2: Fund-clustered
        try:
            m = sm.OLS(y, X).fit(cov_type="cluster",
                                  cov_kwds={"groups": panel.loc[y.index, "fund_id"]})
            bt = m.params.get(treat_col, np.nan)
            pt = m.pvalues.get(treat_col, np.nan)
            bi = m.params.get("treat_x_post2020", np.nan)
            pi = m.pvalues.get("treat_x_post2020", np.nan)
            st = "***" if pt < 0.01 else "**" if pt < 0.05 else "*" if pt < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            n_clusters = panel.loc[y.index, "fund_id"].nunique()
            print(f"    Fund-cl (N={len(y):,}, {n_clusters} clusters):  b({treat_col})={bt:.4f}{st} (p={pt:.3f})  b(treat x post2020)={bi:.4f}{si} (p={pi:.3f})")
        except Exception as e:
            print(f"    Fund-cl Error: {str(e)[:60]}")

        # Spec 3: Firm-clustered (VC firm level)
        try:
            m = sm.OLS(y, X).fit(cov_type="cluster",
                                  cov_kwds={"groups": panel.loc[y.index, "firm_id"]})
            bt = m.params.get(treat_col, np.nan)
            pt = m.pvalues.get(treat_col, np.nan)
            bi = m.params.get("treat_x_post2020", np.nan)
            pi = m.pvalues.get("treat_x_post2020", np.nan)
            st = "***" if pt < 0.01 else "**" if pt < 0.05 else "*" if pt < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            n_clusters = panel.loc[y.index, "firm_id"].nunique()
            print(f"    Firm-cl (N={len(y):,}, {n_clusters} clusters):  b({treat_col})={bt:.4f}{st} (p={pt:.3f})  b(treat x post2020)={bi:.4f}{si} (p={pi:.3f})")
        except Exception as e:
            print(f"    Firm-cl Error: {str(e)[:60]}")

# =====================================================================
# STEP 7: Summary statistics
# =====================================================================
print(f"\n\n{'=' * 80}")
print("PANEL SUMMARY STATISTICS")
print(f"{'=' * 80}")

print(f"\n  Fund-quarter observations: {len(panel):,}")
print(f"  Unique funds: {panel['fund_id'].nunique():,}")
print(f"  Unique firms: {panel['firm_id'].nunique():,}")
print(f"  Year range: {panel['year'].min()} to {panel['year'].max()}")

print(f"\n  Treatment intensity:")
print(f"    Quarters with any event:    {panel['has_event'].sum():>6,} ({panel['has_event'].mean()*100:.1f}%)")
print(f"    Quarters with M&A event:    {panel['has_ma_event'].sum():>6,} ({panel['has_ma_event'].mean()*100:.1f}%)")
print(f"    Mean events per quarter:    {panel['n_events'].mean():.2f}")
print(f"    Max events in a quarter:    {panel['n_events'].max()}")

print(f"\n  Outcome variables:")
for col in ["delta_multiple", "delta_irr"]:
    v = panel[col].dropna()
    print(f"    {col}: N={len(v):,}, mean={v.mean():.4f}, std={v.std():.4f}, median={v.median():.4f}")

# Pre vs post 2020
print(f"\n  Pre-2020 vs Post-2020:")
for col in ["delta_multiple", "delta_irr"]:
    pre = panel[(panel["year"] < 2020) & panel[col].notna()][col]
    post = panel[(panel["year"] >= 2020) & panel[col].notna()][col]
    print(f"    {col}: pre={pre.mean():.4f} (N={len(pre):,}), post={post.mean():.4f} (N={len(post):,})")

# Event quarters vs non-event quarters
print(f"\n  Event quarters vs non-event:")
for col in ["delta_multiple", "delta_irr"]:
    evt = panel[(panel["has_event"] == 1) & panel[col].notna()][col]
    noevt = panel[(panel["has_event"] == 0) & panel[col].notna()][col]
    if len(evt) > 0 and len(noevt) > 0:
        print(f"    {col}: event={evt.mean():.4f} (N={len(evt):,}), no-event={noevt.mean():.4f} (N={len(noevt):,}), diff={evt.mean()-noevt.mean():.4f}")

print("\n\nDone.")
