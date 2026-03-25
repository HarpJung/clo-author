"""Test 4: Pre-Announcement Price Drift at Public Firms With vs Without Observers
Tests whether observer access to MNPI shows up in pre-announcement trading patterns.

Design:
- Treatment: Material events at public firms WITH board observers
- Control: Material events at public firms WITHOUT board observers (matched)
- Outcome: CAR[-10,-1] — abnormal returns in the 10 days BEFORE announcement
- If observer firms show larger pre-announcement drift, that's evidence of
  information leakage (someone is trading on the observer's private info)
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

print("=" * 70)
print("TEST 4: Pre-Announcement Drift at Firms With vs Without Observers")
print("=" * 70)

# =====================================================================
# STEP 1: Identify public firms WITH and WITHOUT observers
# =====================================================================
print("\n--- Step 1: Identifying observer vs non-observer public firms ---")

# Observer firms: CIQ companies with observer data AND CIK
master = pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))
master_public = master[master["companytypename"] == "Public Company"].copy()
master_public["cik_clean"] = master_public["cik"].astype(str).str.strip()
master_public = master_public[master_public["cik_clean"] != ""].copy()
master_public["cik_int"] = pd.to_numeric(master_public["cik_clean"].str.lstrip("0"), errors="coerce")

observer_ciks = set(master_public["cik_int"].dropna().astype(int))
print(f"  Public firms with observers (CIQ): {len(observer_ciks):,}")

# All public firms with CRSP data (from Test 1 control group)
# Use the Test 1 crosswalk which has all S-1 filers mapped to PERMNO
test1_xwalk = pd.read_csv(os.path.join(data_dir, "Test1_Observer_vs_NoObserver", "01_identifier_crosswalk.csv"))
test1_xwalk["cik_int"] = pd.to_numeric(test1_xwalk["cik"], errors="coerce")
all_crsp_ciks = set(test1_xwalk["cik_int"].dropna().astype(int))

# Non-observer public firms = in CRSP but not in CIQ observer set
non_observer_ciks = all_crsp_ciks - observer_ciks
both_ciks = all_crsp_ciks & observer_ciks

print(f"  All CRSP-matched firms: {len(all_crsp_ciks):,}")
print(f"  Observer firms in CRSP: {len(both_ciks):,}")
print(f"  Non-observer firms in CRSP: {len(non_observer_ciks):,}")

# Build CIK -> PERMNO map
cik_to_permno = {}
for _, r in test1_xwalk.iterrows():
    cik = int(r["cik_int"]) if pd.notna(r["cik_int"]) else None
    permno = int(r["permno"]) if pd.notna(r["permno"]) else None
    if cik and permno:
        cik_to_permno[cik] = permno

# =====================================================================
# STEP 2: Get material events for these firms from CIQ Key Dev
# =====================================================================
print("\n--- Step 2: Loading material events ---")

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
    user="harperjung", password="Wwjksnm9087yu!"
)
cur = conn.cursor()

# Get CIQ companyid -> CIK mapping for public firms
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")
cik_to_companyid = dict(zip(ciq_xwalk["cik_int"], ciq_xwalk["companyid"]))

# Pull key dev events for ALL public firms (observer + non-observer)
# Focus on earnings announcements and M&A (most material)
all_cik_list = sorted(all_crsp_ciks)
all_cik_str = ", ".join(str(c) for c in all_cik_list)

print("  Pulling events from CIQ Key Dev...")
time.sleep(3)

cur.execute(f"""
    SELECT kd.companyid, c.cik, kd.announcedate, kd.keydeveventtypeid,
           kd.eventtype, kd.headline
    FROM ciq_keydev.wrds_keydev kd
    JOIN comp.company c ON kd.gvkey = c.gvkey
    WHERE CAST(c.cik AS BIGINT) IN ({all_cik_str})
    AND kd.keydeveventtypeid IN (28, 16)
    AND kd.announcedate >= '2017-01-01'
    ORDER BY c.cik, kd.announcedate
""")

event_rows = cur.fetchall()
events = pd.DataFrame(event_rows, columns=["companyid", "cik", "announcedate", "eventtypeid", "eventtype", "headline"])
events["cik_int"] = pd.to_numeric(events["cik"], errors="coerce").astype("Int64")
events["announcedate"] = pd.to_datetime(events["announcedate"])
events["has_observer"] = events["cik_int"].isin(observer_ciks).astype(int)

print(f"  Total events: {len(events):,}")
print(f"  Events at observer firms: {events['has_observer'].sum():,}")
print(f"  Events at non-observer firms: {(events['has_observer']==0).sum():,}")
print(f"  Event types: {events['eventtype'].value_counts().to_dict()}")

# Map to PERMNO
events["permno"] = events["cik_int"].map(cik_to_permno)
events = events.dropna(subset=["permno", "announcedate"])
events["permno"] = events["permno"].astype(int)

print(f"  Events with PERMNO: {len(events):,}")

# =====================================================================
# STEP 3: Pull daily CRSP returns for these firms
# =====================================================================
print("\n--- Step 3: Loading daily returns ---")

# Check if Test 1 daily returns cover our firms
test1_daily_path = os.path.join(data_dir, "Test1_Observer_vs_NoObserver", "03_crsp_daily_returns.csv")
print(f"  Loading Test 1 daily returns (may take a moment)...")

daily = pd.read_csv(test1_daily_path)
daily["date"] = pd.to_datetime(daily["date"])
daily["ret"] = pd.to_numeric(daily["ret"], errors="coerce")
daily["permno"] = pd.to_numeric(daily["permno"], errors="coerce").astype("Int64")

# Market return
mkt = daily.groupby("date")["ret"].mean().reset_index()
mkt.columns = ["date", "mkt_ret"]
daily = daily.merge(mkt, on="date")
daily["abnormal_ret"] = daily["ret"] - daily["mkt_ret"]

event_permnos = set(events["permno"].unique())
daily_permnos = set(daily["permno"].dropna().unique())
covered = event_permnos & daily_permnos
print(f"  Event PERMNOs: {len(event_permnos):,}")
print(f"  Daily return PERMNOs: {len(daily_permnos):,}")
print(f"  Covered: {len(covered):,}")

# Filter events to covered firms
events = events[events["permno"].isin(covered)].copy()
print(f"  Events after coverage filter: {len(events):,}")

cur.close()
conn.close()

# =====================================================================
# STEP 4: Compute pre-announcement CARs
# =====================================================================
print("\n--- Step 4: Computing pre-announcement CARs ---")

# For each event, compute:
# CAR[-10,-1]: abnormal returns in 10 trading days BEFORE announcement
# CAR[0,+1]: announcement period return (for comparison)
# CAR[-10,+1]: full window

car_results = []
n_events = len(events)

# Sample events if too many (for speed)
if n_events > 50000:
    events_sample = events.sample(50000, random_state=42)
    print(f"  Sampled {len(events_sample):,} events from {n_events:,}")
else:
    events_sample = events
    print(f"  Processing all {len(events_sample):,} events")

for idx, (_, row) in enumerate(events_sample.iterrows()):
    permno = int(row["permno"])
    event_date = row["announcedate"]

    # Get returns around event
    date_lo = event_date - pd.Timedelta(days=20)
    date_hi = event_date + pd.Timedelta(days=5)

    firm_rets = daily[
        (daily["permno"] == permno) &
        (daily["date"] >= date_lo) &
        (daily["date"] <= date_hi)
    ].sort_values("date")

    if len(firm_rets) < 5:
        continue

    # Pre-announcement window: trading days before event date
    pre = firm_rets[firm_rets["date"] < event_date].tail(10)
    # Announcement window: event date + 1 day
    post = firm_rets[firm_rets["date"] >= event_date].head(2)

    if len(pre) < 3:
        continue

    car_pre = pre["abnormal_ret"].sum()
    car_ann = post["abnormal_ret"].sum() if len(post) > 0 else np.nan
    car_full = car_pre + (car_ann if pd.notna(car_ann) else 0)

    # Pre-announcement volume (abnormal)
    avg_vol = firm_rets["vol"].mean() if "vol" in firm_rets.columns else np.nan
    pre_vol = pre["vol"].mean() if "vol" in pre.columns and len(pre) > 0 else np.nan
    abnormal_vol = (pre_vol / avg_vol - 1) if pd.notna(avg_vol) and avg_vol > 0 else np.nan

    car_results.append({
        "permno": permno,
        "cik_int": int(row["cik_int"]),
        "event_date": event_date,
        "event_type": row["eventtype"],
        "has_observer": row["has_observer"],
        "car_pre_10_1": car_pre,
        "car_ann_0_1": car_ann,
        "car_full": car_full,
        "n_pre_days": len(pre),
        "abnormal_volume": abnormal_vol,
    })

    if (idx + 1) % 10000 == 0:
        print(f"    Processed {idx+1:,} / {len(events_sample):,} events, {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
print(f"\n  CARs computed: {len(car_df):,}")
car_df.to_csv(os.path.join(data_dir, "test4_preannouncement_cars.csv"), index=False)

# =====================================================================
# STEP 5: Results
# =====================================================================
print(f"\n\n{'='*70}")
print("TEST 4 RESULTS: Pre-Announcement Drift")
print(f"{'='*70}")

print(f"\n  Sample: {len(car_df):,} event-CARs")
print(f"  Observer events: {car_df['has_observer'].sum():,}")
print(f"  Non-observer events: {(car_df['has_observer']==0).sum():,}")

# Summary by group
print(f"\n  --- CAR[-10,-1] by observer status ---")
for obs, label in [(1, "OBSERVER firms"), (0, "NON-OBSERVER firms")]:
    sub = car_df[car_df["has_observer"] == obs]["car_pre_10_1"].dropna()
    if len(sub) > 10:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {label:25} mean={sub.mean():>9.6f}  median={sub.median():>9.6f}  N={len(sub):>6,}  t={t:>6.2f}  p={p:.4f} {sig}")

# Difference
obs_car = car_df.loc[car_df["has_observer"] == 1, "car_pre_10_1"].dropna()
non_car = car_df.loc[car_df["has_observer"] == 0, "car_pre_10_1"].dropna()
if len(obs_car) > 10 and len(non_car) > 10:
    t, p = stats.ttest_ind(obs_car, non_car, equal_var=False)
    sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"    {'DIFFERENCE':25} diff={obs_car.mean() - non_car.mean():>9.6f}  t={t:>6.2f}  p={p:.4f} {sig}")

# Absolute CAR (measures magnitude regardless of direction)
print(f"\n  --- |CAR[-10,-1]| (absolute drift) by observer status ---")
car_df["abs_car_pre"] = car_df["car_pre_10_1"].abs()
for obs, label in [(1, "OBSERVER firms"), (0, "NON-OBSERVER firms")]:
    sub = car_df[car_df["has_observer"] == obs]["abs_car_pre"].dropna()
    if len(sub) > 10:
        print(f"    {label:25} mean={sub.mean():>9.6f}  median={sub.median():>9.6f}  N={len(sub):>6,}")

obs_abs = car_df.loc[car_df["has_observer"] == 1, "abs_car_pre"].dropna()
non_abs = car_df.loc[car_df["has_observer"] == 0, "abs_car_pre"].dropna()
if len(obs_abs) > 10 and len(non_abs) > 10:
    t, p = stats.ttest_ind(obs_abs, non_abs, equal_var=False)
    sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"    {'DIFFERENCE':25} diff={obs_abs.mean() - non_abs.mean():>9.6f}  t={t:>6.2f}  p={p:.4f} {sig}")

# Abnormal volume
print(f"\n  --- Abnormal Volume by observer status ---")
for obs, label in [(1, "OBSERVER firms"), (0, "NON-OBSERVER firms")]:
    sub = car_df[car_df["has_observer"] == obs]["abnormal_volume"].dropna()
    if len(sub) > 10:
        print(f"    {label:25} mean={sub.mean():>9.6f}  N={len(sub):>6,}")

# By event type
print(f"\n  --- CAR[-10,-1] by event type × observer status ---")
for etype in car_df["event_type"].unique():
    for obs, label in [(1, "Observer"), (0, "Non-observer")]:
        sub = car_df[(car_df["event_type"] == etype) & (car_df["has_observer"] == obs)]["car_pre_10_1"].dropna()
        if len(sub) > 10:
            t, p = stats.ttest_1samp(sub, 0)
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {etype[:35]:35} {label:15} mean={sub.mean():>9.6f}  N={len(sub):>6,}  p={p:.4f} {sig}")

# =====================================================================
# STEP 6: Regression
# =====================================================================
print(f"\n\n--- Step 6: Regressions ---")

# Get industry codes for controls
industry = pd.read_csv(os.path.join(data_dir, "Test1_Observer_vs_NoObserver", "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic = dict(zip(industry["cik_int"], industry["sic2"]))

car_df["sic2"] = car_df["cik_int"].map(cik_to_sic)
car_df["event_year"] = pd.to_datetime(car_df["event_date"]).dt.year.astype(str)

reg = car_df.dropna(subset=["car_pre_10_1", "has_observer", "sic2"]).reset_index(drop=True)
print(f"  Regression sample: {len(reg):,}")

# Model 1: No controls
m1 = smf.ols("car_pre_10_1 ~ has_observer", data=reg).fit(cov_type="HC1")
print(f"\n  Model 1: No controls")
print(f"    has_observer: {m1.params['has_observer']:.6f} (t={m1.tvalues['has_observer']:.2f}, p={m1.pvalues['has_observer']:.4f})")

# Model 2: Year FE
m2 = smf.ols("car_pre_10_1 ~ has_observer + C(event_year)", data=reg).fit(cov_type="HC1")
print(f"\n  Model 2: Year FE")
print(f"    has_observer: {m2.params['has_observer']:.6f} (t={m2.tvalues['has_observer']:.2f}, p={m2.pvalues['has_observer']:.4f})")

# Model 3: Year FE + Industry FE
m3 = smf.ols("car_pre_10_1 ~ has_observer + C(event_year) + C(sic2)", data=reg).fit(cov_type="HC1")
print(f"\n  Model 3: Year FE + Industry FE")
print(f"    has_observer: {m3.params['has_observer']:.6f} (t={m3.tvalues['has_observer']:.2f}, p={m3.pvalues['has_observer']:.4f})")

# Model 4: Absolute CAR
reg["abs_car"] = reg["car_pre_10_1"].abs()
m4 = smf.ols("abs_car ~ has_observer + C(event_year) + C(sic2)", data=reg).fit(cov_type="HC1")
print(f"\n  Model 4: |CAR[-10,-1]| ~ has_observer + Year FE + Industry FE")
print(f"    has_observer: {m4.params['has_observer']:.6f} (t={m4.tvalues['has_observer']:.2f}, p={m4.pvalues['has_observer']:.4f})")

# Model 5: Firm-clustered
m5 = smf.ols("car_pre_10_1 ~ has_observer + C(event_year)", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["permno"]})
print(f"\n  Model 5: Firm-clustered SEs")
print(f"    has_observer: {m5.params['has_observer']:.6f} (t={m5.tvalues['has_observer']:.2f}, p={m5.pvalues['has_observer']:.4f})")
print(f"    Clusters (firms): {reg['permno'].nunique():,}")

print(f"\n  R-squared: M1={m1.rsquared:.6f}, M2={m2.rsquared:.6f}, M3={m3.rsquared:.6f}")
print(f"  N: {int(m1.nobs):,}")
