"""Test 3 with Daily Returns: Event study with [-5,+5] window and VC-firm clustering.
Power improvements:
  1. Daily returns instead of monthly (tighter window = less noise)
  2. Cluster at VC-firm level (4,664 firms) instead of observer level (185)
  3. Include both earnings AND executive change events
"""

import os
import numpy as np
import pandas as pd
import psycopg2
import statsmodels.formula.api as smf
from scipy import stats
import time

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 70)
print("TEST 3 DAILY: Event Study [-5,+5] with VC-Firm Clustering")
print("=" * 70)

# =====================================================================
# STEP 1: Pull daily returns for portfolio companies from WRDS
# =====================================================================
print("\n--- Step 1: Pulling daily CRSP returns for portfolio companies ---")

port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_permnos = sorted(port_xwalk["permno"].dropna().unique().astype(int).tolist())
print(f"  Portfolio PERMNOs: {len(port_permnos):,}")

# Check if we already have this data cached
daily_cache = os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv")

if os.path.exists(daily_cache):
    print(f"  Loading from cache: {daily_cache}")
    port_daily = pd.read_csv(daily_cache)
    port_daily["date"] = pd.to_datetime(port_daily["date"])
else:
    print(f"  Pulling from WRDS (batched, 5-sec delays)...")
    conn = psycopg2.connect(
        host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
        user="harperjung", password="Wwjksnm9087yu!"
    )
    cur = conn.cursor()

    all_rows = []
    batch_size = 200
    for i in range(0, len(port_permnos), batch_size):
        batch = port_permnos[i:i + batch_size]
        batch_str = ", ".join(str(p) for p in batch)
        cur.execute(f"""
            SELECT permno, date, ret, prc, vol
            FROM crsp_a_stock.dsf
            WHERE permno IN ({batch_str})
            AND date >= '2015-01-01'
            ORDER BY permno, date
        """)
        all_rows.extend(cur.fetchall())
        batch_num = i // batch_size + 1
        total_batches = (len(port_permnos) + batch_size - 1) // batch_size
        print(f"    Batch {batch_num}/{total_batches}: {len(all_rows):,} rows")
        time.sleep(5)

    cur.close()
    conn.close()

    port_daily = pd.DataFrame(all_rows, columns=["permno", "date", "ret", "prc", "vol"])
    port_daily.to_csv(daily_cache, index=False)
    print(f"  Saved cache: {len(port_daily):,} rows")

port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily["date"] = pd.to_datetime(port_daily["date"])
print(f"  Daily returns: {len(port_daily):,} rows | {port_daily['permno'].nunique():,} securities")

# Market return (equal-weighted average)
mkt_daily = port_daily.groupby("date")["ret"].mean().reset_index()
mkt_daily.columns = ["date", "mkt_ret"]
port_daily = port_daily.merge(mkt_daily, on="date")
port_daily["abnormal_ret"] = port_daily["ret"] - port_daily["mkt_ret"]

# =====================================================================
# STEP 2: Load network and events
# =====================================================================
print("\n--- Step 2: Loading network and events ---")

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

# Add PERMNO to edges
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
print(f"  Same industry pairs: {edges['same_industry'].sum():,}")

# Events
events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)

# Use BOTH earnings and exec changes
event_types = ["Announcements of Earnings", "Executive/Board Changes - Other"]
selected_events = events[events["keydeveventtypename"].isin(event_types)].copy()
selected_events = selected_events.dropna(subset=["announcedate"])
selected_events = selected_events[["companyid", "announcedate", "keydeveventtypename"]].drop_duplicates()
print(f"  Events: {len(selected_events):,} ({selected_events['keydeveventtypename'].value_counts().to_dict()})")

# =====================================================================
# STEP 3: Build event study CARs [-5, +5]
# =====================================================================
print("\n--- Step 3: Building CARs [-5,+5] around events ---")

# For each event at an observed company, for each connected portfolio company:
# 1. Find the event date
# 2. Get portfolio company daily returns in [-5, +5] window
# 3. Compute CAR = sum of abnormal returns in the window

# Merge events with edges
event_edges = edges.merge(
    selected_events,
    left_on="observed_companyid",
    right_on="companyid",
    how="inner"
)
print(f"  Event-edge pairs: {len(event_edges):,}")

# For efficiency, build a lookup: permno -> date -> abnormal_ret
print("  Building return lookup...")
port_daily_indexed = port_daily.set_index(["permno", "date"])["abnormal_ret"]

# Compute CARs in batches
print("  Computing CARs (this may take a few minutes)...")
car_results = []
window = 5

# Process in chunks
chunk_size = 10000
total_chunks = (len(event_edges) + chunk_size - 1) // chunk_size

for chunk_idx in range(total_chunks):
    start = chunk_idx * chunk_size
    end = min(start + chunk_size, len(event_edges))
    chunk = event_edges.iloc[start:end]

    for _, row in chunk.iterrows():
        permno = int(row["permno"])
        event_date = row["announcedate"]

        # Get trading days around event
        # Find dates within [-10, +10] calendar days to capture 5 trading days each side
        date_lo = event_date - pd.Timedelta(days=10)
        date_hi = event_date + pd.Timedelta(days=10)

        try:
            firm_rets = port_daily[
                (port_daily["permno"] == permno) &
                (port_daily["date"] >= date_lo) &
                (port_daily["date"] <= date_hi)
            ].sort_values("date")

            if len(firm_rets) < 5:
                continue

            # Find the event date position
            # Get trading days before and after
            pre = firm_rets[firm_rets["date"] < event_date].tail(window)
            post = firm_rets[firm_rets["date"] >= event_date].head(window + 1)
            event_window = pd.concat([pre, post])

            if len(event_window) < 5:
                continue

            car = event_window["abnormal_ret"].sum()
            car_pre = pre["abnormal_ret"].sum() if len(pre) > 0 else np.nan
            car_post = post["abnormal_ret"].sum() if len(post) > 0 else np.nan

            car_results.append({
                "observer_personid": row["observer_personid"],
                "observer_name": row["observer_name"],
                "observed_companyid": row["observed_companyid"],
                "vc_firm_companyid": row["vc_firm_companyid"],
                "portfolio_permno": permno,
                "event_date": event_date,
                "event_type": row["keydeveventtypename"],
                "car_full": car,
                "car_pre": car_pre,
                "car_post": car_post,
                "n_days": len(event_window),
                "is_director_at_portfolio": row["is_director_at_portfolio"],
                "same_industry": row["same_industry"],
            })
        except Exception:
            continue

    if (chunk_idx + 1) % 5 == 0 or chunk_idx == total_chunks - 1:
        print(f"    Chunk {chunk_idx + 1}/{total_chunks}: {len(car_results):,} CARs computed")

car_df = pd.DataFrame(car_results)
print(f"\n  Total CARs computed: {len(car_df):,}")

if len(car_df) == 0:
    print("  ERROR: No CARs computed. Check data alignment.")
    exit()

# Save CARs
car_df.to_csv(os.path.join(panel_c_dir, "07_daily_car_results.csv"), index=False)

# =====================================================================
# STEP 4: Summary and regressions
# =====================================================================
print(f"\n--- Step 4: Results ---")

print(f"\n  Sample: {len(car_df):,} event-portfolio CARs")
print(f"  Unique observers: {car_df['observer_personid'].nunique():,}")
print(f"  Unique VC firms: {car_df['vc_firm_companyid'].nunique():,}")
print(f"  Unique portfolio companies: {car_df['portfolio_permno'].nunique():,}")
print(f"  Same-industry pairs: {car_df['same_industry'].sum():,}")

print(f"\n  CAR[-5,+5] stats:")
print(f"    Mean:   {car_df['car_full'].mean():.6f}")
print(f"    Median: {car_df['car_full'].median():.6f}")
print(f"    Std:    {car_df['car_full'].std():.6f}")

# Overall test
t, p = stats.ttest_1samp(car_df["car_full"].dropna(), 0)
sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
print(f"\n  H0: CAR = 0 -> t={t:.4f}, p={p:.6f} {sig}")

# By event type
print(f"\n  --- By event type ---")
for etype in car_df["event_type"].unique():
    sub = car_df[car_df["event_type"] == etype]["car_full"].dropna()
    if len(sub) > 10:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {etype:45} mean={sub.mean():>9.6f}  N={len(sub):>6,}  t={t:>6.2f}  p={p:.4f} {sig}")

# By connection type
print(f"\n  --- By connection type ---")
for role, label in [(1, "Director at portfolio"), (0, "Non-director at portfolio")]:
    sub = car_df[car_df["is_director_at_portfolio"] == role]["car_full"].dropna()
    if len(sub) > 10:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {label:45} mean={sub.mean():>9.6f}  N={len(sub):>6,}  t={t:>6.2f}  p={p:.4f} {sig}")

# By industry
print(f"\n  --- By industry overlap ---")
for ind, label in [(1, "Same industry (SIC2)"), (0, "Different industry")]:
    sub = car_df[car_df["same_industry"] == ind]["car_full"].dropna()
    if len(sub) > 10:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {label:45} mean={sub.mean():>9.6f}  N={len(sub):>6,}  t={t:>6.2f}  p={p:.4f} {sig}")

if car_df["same_industry"].sum() > 10:
    same = car_df[car_df["same_industry"] == 1]["car_full"].dropna()
    diff = car_df[car_df["same_industry"] == 0]["car_full"].dropna()
    t, p = stats.ttest_ind(same, diff, equal_var=False)
    sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"    {'Same vs Different':45} diff={same.mean()-diff.mean():>9.6f}  t={t:>6.2f}  p={p:.4f} {sig}")

# =====================================================================
# STEP 5: Regressions with VC-firm clustering
# =====================================================================
print(f"\n\n--- Step 5: Regressions (clustered by VC firm) ---")

reg = car_df.dropna(subset=["car_full", "same_industry", "is_director_at_portfolio"]).reset_index(drop=True)
n_vc_clusters = reg["vc_firm_companyid"].nunique()
n_obs_clusters = reg["observer_personid"].nunique()
print(f"  N: {len(reg):,} | VC clusters: {n_vc_clusters:,} | Observer clusters: {n_obs_clusters:,}")

# Model 1: Intercept only, VC-firm clustering
m1 = smf.ols("car_full ~ 1", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
print(f"\n  Model 1: Intercept (clustered by VC firm, {n_vc_clusters} clusters)")
print(f"    Intercept: {m1.params['Intercept']:.6f} (t={m1.tvalues['Intercept']:.2f}, p={m1.pvalues['Intercept']:.4f})")

# Model 1b: Same but clustered by observer (for comparison)
m1b = smf.ols("car_full ~ 1", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["observer_personid"]})
print(f"  Model 1b: Intercept (clustered by observer, {n_obs_clusters} clusters)")
print(f"    Intercept: {m1b.params['Intercept']:.6f} (t={m1b.tvalues['Intercept']:.2f}, p={m1b.pvalues['Intercept']:.4f})")

# Model 2: Same industry
if reg["same_industry"].sum() > 0:
    m2 = smf.ols("car_full ~ same_industry", data=reg).fit(
        cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
    print(f"\n  Model 2: Same industry (VC-firm clustered)")
    print(f"    same_industry: {m2.params['same_industry']:.6f} "
          f"(t={m2.tvalues['same_industry']:.2f}, p={m2.pvalues['same_industry']:.4f})")

# Model 3: Director connection
m3 = smf.ols("car_full ~ is_director_at_portfolio", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
print(f"\n  Model 3: Director connection (VC-firm clustered)")
print(f"    is_director: {m3.params['is_director_at_portfolio']:.6f} "
      f"(t={m3.tvalues['is_director_at_portfolio']:.2f}, p={m3.pvalues['is_director_at_portfolio']:.4f})")

# Model 4: Both + interaction
if reg["same_industry"].sum() > 0:
    m4 = smf.ols("car_full ~ is_director_at_portfolio * same_industry", data=reg).fit(
        cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
    print(f"\n  Model 4: Full model (VC-firm clustered)")
    for param in m4.params.index:
        if param != "Intercept":
            sig = "***" if m4.pvalues[param] < 0.01 else "**" if m4.pvalues[param] < 0.05 else "*" if m4.pvalues[param] < 0.10 else ""
            print(f"    {param:45} {m4.params[param]:>10.6f} (t={m4.tvalues[param]:>6.2f}, p={m4.pvalues[param]:.4f}) {sig}")

# Model 5: By event type
print(f"\n  Model 5: By event type (VC-firm clustered)")
reg["is_earnings"] = (reg["event_type"] == "Announcements of Earnings").astype(int)
m5 = smf.ols("car_full ~ is_earnings + same_industry + is_director_at_portfolio", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
for param in m5.params.index:
    if param != "Intercept":
        sig = "***" if m5.pvalues[param] < 0.01 else "**" if m5.pvalues[param] < 0.05 else "*" if m5.pvalues[param] < 0.10 else ""
        print(f"    {param:45} {m5.params[param]:>10.6f} (t={m5.tvalues[param]:>6.2f}, p={m5.pvalues[param]:.4f}) {sig}")

print(f"\n  R-squared: M1={m1.rsquared:.6f}, M3={m3.rsquared:.6f}")
