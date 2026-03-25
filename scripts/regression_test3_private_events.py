"""Test 3 Revised: Private information spillover — events at PRIVATE observed companies only.
Only uses events where the observed company is private (no public announcement channel).
Any spillover to connected public portfolio companies MUST reflect private information flow.
"""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats
import csv

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 70)
print("TEST 3 REVISED: Private Information Spillover")
print("Events at PRIVATE observed companies only")
print("=" * 70)

# =====================================================================
# STEP 1: Load data
# =====================================================================
print("\n--- Step 1: Loading data ---")

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

# Portfolio PERMNO
port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
port_xwalk_dedup = port_xwalk.drop_duplicates("cik_int", keep="first")
edges = edges.merge(
    port_xwalk_dedup[["cik_int", "permno"]].rename(columns={"cik_int": "portfolio_cik_int"}),
    on="portfolio_cik_int", how="inner"
)
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")

# Classify connection type
edges["is_director_at_portfolio"] = edges["portfolio_title"].str.contains(
    "Director|Chairman|Board Member", case=False, na=False
).astype(int)

# Industry codes
industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")
companyid_to_cik = dict(zip(ciq_xwalk["companyid_str"], ciq_xwalk["cik_int"]))

edges["observed_cik"] = edges["observed_companyid"].map(companyid_to_cik)
edges["observed_sic2"] = edges["observed_cik"].map(cik_to_sic2)
edges["portfolio_sic2"] = edges["portfolio_cik_int"].map(cik_to_sic2)
edges["same_industry"] = (
    edges["observed_sic2"].notna() & edges["portfolio_sic2"].notna() &
    (edges["observed_sic2"] == edges["portfolio_sic2"])
).astype(int)

print(f"  Edges with PERMNO: {len(edges):,}")

# =====================================================================
# STEP 2: Load events and classify by company type
# =====================================================================
print("\n--- Step 2: Loading events and classifying by company type ---")

events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)

# Load company types
cos_types = {}
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r["companyid"]).strip()
        if cid.endswith(".0"):
            cid = cid[:-2]
        cos_types[cid] = r["companytypename"]

events["company_type"] = events["companyid"].map(cos_types)

# Split events
private_events = events[events["company_type"] == "Private Company"].copy()
public_events = events[events["company_type"] == "Public Company"].copy()

print(f"  All events: {len(events):,}")
print(f"  Private company events: {len(private_events):,}")
print(f"  Public company events: {len(public_events):,}")

print(f"\n  Private events by type:")
for et, cnt in private_events["keydeveventtypename"].value_counts().items():
    print(f"    {et:45} {cnt:>6,}")

print(f"\n  Public events by type:")
for et, cnt in public_events["keydeveventtypename"].value_counts().items():
    print(f"    {et:45} {cnt:>6,}")

# =====================================================================
# STEP 3: Load daily returns
# =====================================================================
print("\n--- Step 3: Loading portfolio daily returns ---")

daily_cache = os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv")
port_daily = pd.read_csv(daily_cache)
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")

# Market return
mkt_daily = port_daily.groupby("date")["ret"].mean().reset_index()
mkt_daily.columns = ["date", "mkt_ret"]
port_daily = port_daily.merge(mkt_daily, on="date")
port_daily["abnormal_ret"] = port_daily["ret"] - port_daily["mkt_ret"]

print(f"  Daily returns: {len(port_daily):,} rows | {port_daily['permno'].nunique():,} securities")

# =====================================================================
# STEP 4: Compute CARs for PRIVATE company events only
# =====================================================================
print("\n--- Step 4: Computing CARs for private company events ---")

# Use all private company event types (not just earnings)
private_selected = private_events[private_events["keydeveventtypename"].isin([
    "Executive/Board Changes - Other",
    "Strategic Alliances",
    "Seeking Financing/Partners",
    "Lawsuits & Legal Issues",
    "Bankruptcy - Other",
    "Discontinued Operations/Downsizings",
    "Announcements of Earnings",  # the ~8K private firm earnings (press releases)
])].copy()
private_selected = private_selected.dropna(subset=["announcedate"])
private_selected = private_selected[["companyid", "announcedate", "keydeveventtypename"]].drop_duplicates()

print(f"  Private events selected: {len(private_selected):,}")

# Merge with edges
edges_str = edges.copy()
private_selected_str = private_selected.copy()

event_edges = edges_str.merge(
    private_selected_str,
    left_on="observed_companyid",
    right_on="companyid",
    how="inner"
)
print(f"  Event-edge pairs (private only): {len(event_edges):,}")

# Compute CARs
print("  Computing CARs (pre-event [-10,-1] and post-event [0,+5] separately)...")

car_results = []
window_pre = 10
window_post = 5

chunk_size = 10000
total_chunks = (len(event_edges) + chunk_size - 1) // chunk_size

for chunk_idx in range(total_chunks):
    start = chunk_idx * chunk_size
    end = min(start + chunk_size, len(event_edges))
    chunk = event_edges.iloc[start:end]

    for _, row in chunk.iterrows():
        permno = int(row["permno"])
        event_date = row["announcedate"]

        date_lo = event_date - pd.Timedelta(days=20)
        date_hi = event_date + pd.Timedelta(days=12)

        firm_rets = port_daily[
            (port_daily["permno"] == permno) &
            (port_daily["date"] >= date_lo) &
            (port_daily["date"] <= date_hi)
        ].sort_values("date")

        if len(firm_rets) < 5:
            continue

        # Pre-event window [-10, -1]
        pre = firm_rets[firm_rets["date"] < event_date].tail(window_pre)
        # Post-event window [0, +5]
        post = firm_rets[firm_rets["date"] >= event_date].head(window_post + 1)
        # Full window
        full = pd.concat([pre, post])

        if len(pre) < 3:
            continue

        car_pre = pre["abnormal_ret"].sum()
        car_post = post["abnormal_ret"].sum() if len(post) > 0 else np.nan
        car_full = car_pre + (car_post if pd.notna(car_post) else 0)

        car_results.append({
            "observer_personid": row["observer_personid"],
            "observed_companyid": row["observed_companyid"],
            "vc_firm_companyid": row["vc_firm_companyid"],
            "portfolio_permno": permno,
            "event_date": event_date,
            "event_type": row["keydeveventtypename"],
            "car_pre": car_pre,
            "car_post": car_post,
            "car_full": car_full,
            "n_pre_days": len(pre),
            "n_post_days": len(post),
            "is_director_at_portfolio": row["is_director_at_portfolio"],
            "same_industry": row["same_industry"],
        })

    if (chunk_idx + 1) % 5 == 0 or chunk_idx == total_chunks - 1:
        print(f"    Chunk {chunk_idx+1}/{total_chunks}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
print(f"\n  Total CARs (private events only): {len(car_df):,}")

if len(car_df) == 0:
    print("  ERROR: No CARs computed")
    exit()

# Save
car_df.to_csv(os.path.join(panel_c_dir, "08_private_event_car_results.csv"), index=False)

# =====================================================================
# STEP 5: Results
# =====================================================================
print(f"\n{'='*70}")
print("RESULTS: Private Information Spillover")
print(f"{'='*70}")

print(f"\n  Sample: {len(car_df):,} CARs from PRIVATE company events")
print(f"  Unique observers: {car_df['observer_personid'].nunique():,}")
print(f"  Same-industry pairs: {car_df['same_industry'].sum():,}")

# --- Pre-event CAR[-10,-1] ---
print(f"\n  === PRE-EVENT CAR[-10,-1] (private info leakage test) ===")

t, p = stats.ttest_1samp(car_df["car_pre"].dropna(), 0)
sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
print(f"  Overall: mean={car_df['car_pre'].mean():.6f}  t={t:.4f}  p={p:.4f} {sig}")

# By same industry
for ind, label in [(1, "Same industry"), (0, "Different industry")]:
    sub = car_df[car_df["same_industry"] == ind]["car_pre"].dropna()
    if len(sub) > 10:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {label:25} mean={sub.mean():.6f}  N={len(sub):>6,}  t={t:.4f}  p={p:.4f} {sig}")

if car_df["same_industry"].sum() > 10:
    same = car_df[car_df["same_industry"] == 1]["car_pre"].dropna()
    diff = car_df[car_df["same_industry"] == 0]["car_pre"].dropna()
    t, p = stats.ttest_ind(same, diff, equal_var=False)
    sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"    {'Same vs Different':25} diff={same.mean()-diff.mean():.6f}  t={t:.4f}  p={p:.4f} {sig}")

# By director vs non-director
print(f"\n  By connection type (pre-event):")
for role, label in [(1, "Director at portfolio"), (0, "Non-director")]:
    sub = car_df[car_df["is_director_at_portfolio"] == role]["car_pre"].dropna()
    if len(sub) > 10:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {label:25} mean={sub.mean():.6f}  N={len(sub):>6,}  t={t:.4f}  p={p:.4f} {sig}")

# --- Post-event CAR[0,+5] ---
print(f"\n  === POST-EVENT CAR[0,+5] (public processing test) ===")

t, p = stats.ttest_1samp(car_df["car_post"].dropna(), 0)
sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
print(f"  Overall: mean={car_df['car_post'].mean():.6f}  t={t:.4f}  p={p:.4f} {sig}")

for ind, label in [(1, "Same industry"), (0, "Different industry")]:
    sub = car_df[car_df["same_industry"] == ind]["car_post"].dropna()
    if len(sub) > 10:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {label:25} mean={sub.mean():.6f}  N={len(sub):>6,}  t={t:.4f}  p={p:.4f} {sig}")

# --- Full window ---
print(f"\n  === FULL WINDOW CAR[-10,+5] ===")

t, p = stats.ttest_1samp(car_df["car_full"].dropna(), 0)
sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
print(f"  Overall: mean={car_df['car_full'].mean():.6f}  t={t:.4f}  p={p:.4f} {sig}")

# --- By event type ---
print(f"\n  === BY EVENT TYPE (pre-event CAR) ===")
for et in car_df["event_type"].unique():
    sub = car_df[car_df["event_type"] == et]["car_pre"].dropna()
    if len(sub) > 30:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {et:45} mean={sub.mean():.6f}  N={len(sub):>6,}  p={p:.4f} {sig}")

# =====================================================================
# STEP 6: Regressions with VC-firm clustering
# =====================================================================
print(f"\n\n{'='*70}")
print("REGRESSIONS (clustered by VC firm)")
print(f"{'='*70}")

reg = car_df.dropna(subset=["car_pre", "car_post", "same_industry", "is_director_at_portfolio"]).reset_index(drop=True)
n_vc = reg["vc_firm_companyid"].nunique()
print(f"\n  N: {len(reg):,} | VC clusters: {n_vc:,}")

# Pre-event regressions
print(f"\n  --- Pre-event CAR[-10,-1] ---")

m1 = smf.ols("car_pre ~ 1", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
print(f"  M1 Intercept: {m1.params['Intercept']:.6f} (t={m1.tvalues['Intercept']:.2f}, p={m1.pvalues['Intercept']:.4f})")

if reg["same_industry"].sum() > 0:
    m2 = smf.ols("car_pre ~ same_industry", data=reg).fit(
        cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
    print(f"  M2 same_industry: {m2.params['same_industry']:.6f} (t={m2.tvalues['same_industry']:.2f}, p={m2.pvalues['same_industry']:.4f})")

m3 = smf.ols("car_pre ~ is_director_at_portfolio", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
print(f"  M3 is_director: {m3.params['is_director_at_portfolio']:.6f} (t={m3.tvalues['is_director_at_portfolio']:.2f}, p={m3.pvalues['is_director_at_portfolio']:.4f})")

if reg["same_industry"].sum() > 0:
    m4 = smf.ols("car_pre ~ same_industry + is_director_at_portfolio", data=reg).fit(
        cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
    print(f"  M4 same_industry: {m4.params['same_industry']:.6f} (t={m4.tvalues['same_industry']:.2f}, p={m4.pvalues['same_industry']:.4f})")
    print(f"     is_director:   {m4.params['is_director_at_portfolio']:.6f} (t={m4.tvalues['is_director_at_portfolio']:.2f}, p={m4.pvalues['is_director_at_portfolio']:.4f})")

# Post-event regressions
print(f"\n  --- Post-event CAR[0,+5] ---")

m5 = smf.ols("car_post ~ 1", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
print(f"  M5 Intercept: {m5.params['Intercept']:.6f} (t={m5.tvalues['Intercept']:.2f}, p={m5.pvalues['Intercept']:.4f})")

if reg["same_industry"].sum() > 0:
    m6 = smf.ols("car_post ~ same_industry + is_director_at_portfolio", data=reg).fit(
        cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
    print(f"  M6 same_industry: {m6.params['same_industry']:.6f} (t={m6.tvalues['same_industry']:.2f}, p={m6.pvalues['same_industry']:.4f})")
    print(f"     is_director:   {m6.params['is_director_at_portfolio']:.6f} (t={m6.tvalues['is_director_at_portfolio']:.2f}, p={m6.pvalues['is_director_at_portfolio']:.4f})")

# =====================================================================
# COMPARISON: Private vs Public events
# =====================================================================
print(f"\n\n{'='*70}")
print("COMPARISON: Also compute for PUBLIC company events")
print(f"{'='*70}")

# Quickly compute for public events too (just the summary stats, not full regression)
public_selected = public_events[public_events["keydeveventtypename"].isin([
    "Announcements of Earnings",
    "Executive/Board Changes - Other",
])].copy()
public_selected = public_selected.dropna(subset=["announcedate"])
public_selected = public_selected[["companyid", "announcedate", "keydeveventtypename"]].drop_duplicates()

pub_event_edges = edges_str.merge(
    public_selected, left_on="observed_companyid", right_on="companyid", how="inner"
)
print(f"  Public event-edge pairs: {len(pub_event_edges):,}")

pub_cars = []
# Sample for speed
if len(pub_event_edges) > 50000:
    pub_event_edges = pub_event_edges.sample(50000, random_state=42)

for _, row in pub_event_edges.iterrows():
    permno = int(row["permno"])
    event_date = row["announcedate"]
    date_lo = event_date - pd.Timedelta(days=20)
    date_hi = event_date + pd.Timedelta(days=12)
    firm_rets = port_daily[
        (port_daily["permno"] == permno) &
        (port_daily["date"] >= date_lo) &
        (port_daily["date"] <= date_hi)
    ].sort_values("date")
    if len(firm_rets) < 5:
        continue
    pre = firm_rets[firm_rets["date"] < event_date].tail(10)
    post = firm_rets[firm_rets["date"] >= event_date].head(6)
    if len(pre) < 3:
        continue
    pub_cars.append({
        "car_pre": pre["abnormal_ret"].sum(),
        "car_post": post["abnormal_ret"].sum() if len(post) > 0 else np.nan,
        "same_industry": row["same_industry"],
    })

pub_car_df = pd.DataFrame(pub_cars)
print(f"  Public CARs computed: {len(pub_car_df):,}")

if len(pub_car_df) > 0:
    print(f"\n  Public events pre-event CAR[-10,-1]:")
    t, p = stats.ttest_1samp(pub_car_df["car_pre"].dropna(), 0)
    sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"    Overall: mean={pub_car_df['car_pre'].mean():.6f}  t={t:.4f}  p={p:.4f} {sig}")

    if pub_car_df["same_industry"].sum() > 10:
        same = pub_car_df[pub_car_df["same_industry"] == 1]["car_pre"].dropna()
        diff = pub_car_df[pub_car_df["same_industry"] == 0]["car_pre"].dropna()
        print(f"    Same industry:    mean={same.mean():.6f}  N={len(same):,}")
        print(f"    Different:        mean={diff.mean():.6f}  N={len(diff):,}")

print(f"\n  Private events pre-event CAR[-10,-1] (for comparison):")
print(f"    Overall: mean={car_df['car_pre'].mean():.6f}  N={len(car_df):,}")

print(f"\n{'='*70}")
print("INTERPRETATION")
print(f"{'='*70}")
print("""
  If pre-event CAR is significant for PRIVATE company events:
    -> Information is flowing BEFORE any public disclosure
    -> The observer is the most likely channel (they sit in the boardroom)
    -> This is the strongest evidence for private information spillover

  If pre-event CAR is significant for PUBLIC but not PRIVATE:
    -> The spillover is driven by anticipation of public announcements
    -> Less about private information, more about faster processing

  If same-industry is stronger in private events:
    -> Industry-specific private information is the channel
    -> Supports the DOJ/FTC competitive intelligence concern
""")
