"""
Event study on VC fund cashflows around events at observed companies.

Key timing insight:
  CIQ event date = PUBLIC DISCLOSURE date (press release, SEC filing)
  Board meeting = days/weeks/months BEFORE the public date
  Observer learns at the board meeting, not at disclosure

  If observer information matters, VC fund activity should increase
  BEFORE the public disclosure date (the VC acts on private info).
  Activity AFTER disclosure is just public information reaction.

Design:
  For each event at an observed company, look at cashflows from the
  VC fund in windows around the event date:
    [-180, -91]  Far pre-event (baseline)
    [-90, -31]   Pre-event (board likely discussed)
    [-30, -1]    Immediate pre-event (close to decision)
    [0, +30]     Post-announcement
    [+31, +90]   Post-event (public info reaction)

  Compare cashflow activity (calls, distributions) in each window
  to the fund's baseline cashflow rate.
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

print("=" * 90)
print("EVENT STUDY ON VC FUND CASHFLOWS")
print("=" * 90)

# === Load crosswalk ===
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
firm_to_ciq = xwalk.drop_duplicates("preqin_firm_id").set_index("preqin_firm_id")["ciq_vc_companyid"].to_dict()
ciq_to_firm = {str(v): int(k) for k, v in firm_to_ciq.items()}

# === Load funds ===
funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
matched_firm_ids = set(xwalk["preqin_firm_id"].dropna().astype(int))
funds = funds[funds["firm_id"].isin(matched_firm_ids)]
vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)].copy()
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))
fund_to_firm = dict(zip(vc_funds["fund_id"].astype(int), vc_funds["firm_id"].astype(int)))

# === Load cashflows with EXACT DATES ===
print("\n--- Loading cashflows ---")
cf = pd.read_csv(os.path.join(preqin_dir, "cashflows_full.csv"))
cf = cf[cf["fund_id"].isin(vc_fund_ids)].copy()
cf["transaction_date"] = pd.to_datetime(cf["transaction_date"], errors="coerce")
cf = cf.dropna(subset=["transaction_date"])
cf["fund_id"] = cf["fund_id"].astype(int)
cf["firm_id"] = cf["fund_id"].map(fund_to_firm)
cf["ciq_vc"] = cf["firm_id"].map(lambda x: {v: k for k, v in ciq_to_firm.items()}.get(x, ""))
cf["is_call"] = (cf["transaction_type"] == "Capital Call").astype(int)
cf["is_dist"] = (cf["transaction_type"] == "Distribution").astype(int)

print(f"  Cashflows: {len(cf):,}")
print(f"  Calls: {cf['is_call'].sum():,}")
print(f"  Distributions: {cf['is_dist'].sum():,}")
print(f"  Funds: {cf['fund_id'].nunique():,}")
print(f"  Date range: {cf['transaction_date'].min().date()} to {cf['transaction_date'].max().date()}")

# === Load events ===
print("\n--- Loading events ---")
tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)
tb["observed_companyid"] = tb["observed_companyid"].astype(str).str.replace(".0", "", regex=False)

vc_to_obs = {}
for _, r in tb.iterrows():
    vc = r["vc_firm_companyid"]
    if vc not in vc_to_obs:
        vc_to_obs[vc] = set()
    vc_to_obs[vc].add(r["observed_companyid"])

events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])

co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_priv)]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]

# Material events only
events["material"] = events["eventtype"].apply(
    lambda x: "M&A" in str(x) or "Bankruptcy" in str(x) or "Executive/Board" in str(x) or "Restructuring" in str(x))
mat_events = events[events["material"]].copy()
print(f"  Material events (US private): {len(mat_events):,}")

# === Build event-VC pairs ===
# For each material event, find which matched VCs have observers at that company
print("\n--- Building event-VC pairs ---")

event_vc_pairs = []
matched_ciq_vcs = set(xwalk["ciq_vc_companyid"].astype(str))

for _, evt in mat_events.iterrows():
    oc = evt["companyid"]
    edate = evt["announcedate"]

    # Find VCs with observers at this company
    for vc_cid in matched_ciq_vcs:
        if oc in vc_to_obs.get(vc_cid, set()):
            preqin_fid = ciq_to_firm.get(vc_cid)
            if preqin_fid:
                event_vc_pairs.append({
                    "event_companyid": oc,
                    "event_date": edate,
                    "event_type": evt["eventtype"],
                    "ciq_vc": vc_cid,
                    "preqin_firm_id": preqin_fid,
                })

pairs = pd.DataFrame(event_vc_pairs)
print(f"  Event-VC pairs: {len(pairs):,}")
print(f"  Unique events: {pairs['event_date'].nunique():,}")
print(f"  Unique VCs: {pairs['ciq_vc'].nunique():,}")

# === Event study: count cashflows in windows around each event ===
print("\n--- Computing cashflow event study ---")

windows = [
    ("[-180,-91]", -180, -91),
    ("[-90,-31]", -90, -31),
    ("[-30,-1]", -30, -1),
    ("[0,+30]", 0, 30),
    ("[+31,+90]", 31, 90),
    ("[+91,+180]", 91, 180),
]

# Build firm-level cashflow lookup: firm_id -> list of (date, is_call, is_dist, amount)
firm_cashflows = {}
for _, r in cf.iterrows():
    fid = r["firm_id"]
    if pd.isna(fid):
        continue
    fid = int(fid)
    if fid not in firm_cashflows:
        firm_cashflows[fid] = []
    firm_cashflows[fid].append((r["transaction_date"], r["is_call"], r["is_dist"], r["transaction_amount"]))

# Convert to sorted arrays for faster lookup
for fid in firm_cashflows:
    firm_cashflows[fid] = sorted(firm_cashflows[fid], key=lambda x: x[0])

print(f"  Firms with cashflow data: {len(firm_cashflows):,}")

# For each event-VC pair, count cashflows in each window
results = []
for idx, (_, pair) in enumerate(pairs.iterrows()):
    edate = pair["event_date"]
    fid = pair["preqin_firm_id"]

    cf_list = firm_cashflows.get(fid, [])
    if not cf_list:
        continue

    row = {
        "event_date": edate,
        "event_type": pair["event_type"],
        "ciq_vc": pair["ciq_vc"],
        "preqin_firm_id": fid,
        "year": edate.year,
        "post_2020": 1 if edate.year >= 2020 else 0,
    }

    for wname, wstart, wend in windows:
        d_start = edate + pd.Timedelta(days=wstart)
        d_end = edate + pd.Timedelta(days=wend)

        n_calls = 0
        n_dists = 0
        amt_calls = 0
        amt_dists = 0

        for dt, is_c, is_d, amt in cf_list:
            if d_start <= dt <= d_end:
                n_calls += is_c
                n_dists += is_d
                if is_c:
                    amt_calls += abs(amt) if pd.notna(amt) else 0
                if is_d:
                    amt_dists += abs(amt) if pd.notna(amt) else 0

        row[f"n_calls_{wname}"] = n_calls
        row[f"n_dists_{wname}"] = n_dists
        row[f"amt_calls_{wname}"] = amt_calls
        row[f"amt_dists_{wname}"] = amt_dists
        row[f"has_dist_{wname}"] = 1 if n_dists > 0 else 0
        row[f"has_call_{wname}"] = 1 if n_calls > 0 else 0

    results.append(row)

    if (idx + 1) % 5000 == 0:
        print(f"    {idx + 1}/{len(pairs)} pairs processed")

res = pd.DataFrame(results)
print(f"\n  Event-study observations: {len(res):,}")

# === Results ===
print(f"\n\n{'=' * 90}")
print("RESULTS: Cashflow Activity Around Events")
print(f"{'=' * 90}")

# Distribution probability in each window
print(f"\n  PROBABILITY OF DISTRIBUTION BY WINDOW")
print(f"  (% of event-VC pairs where the VC's fund makes a distribution)")
print(f"  {'Window':<15} {'Has Dist':>10} {'N Dists':>10} {'Has Call':>10}")
print(f"  {'-' * 50}")
for wname, _, _ in windows:
    hd = res[f"has_dist_{wname}"].mean() * 100
    nd = res[f"n_dists_{wname}"].mean()
    hc = res[f"has_call_{wname}"].mean() * 100
    print(f"  {wname:<15} {hd:>9.2f}% {nd:>10.3f} {hc:>9.2f}%")

# Compare pre-event to post-event
print(f"\n  PRE vs POST COMPARISON")
pre_dist = res["has_dist_[-30,-1]"].mean() * 100
post_dist = res["has_dist_[0,+30]"].mean() * 100
far_pre = res["has_dist_[-180,-91]"].mean() * 100
print(f"  Baseline [-180,-91]:    {far_pre:.2f}%")
print(f"  Pre-event [-30,-1]:     {pre_dist:.2f}%")
print(f"  Post-event [0,+30]:     {post_dist:.2f}%")
print(f"  Pre - Baseline:         {pre_dist - far_pre:+.2f} pp")
print(f"  Post - Baseline:        {post_dist - far_pre:+.2f} pp")

# Same for calls
print(f"\n  CAPITAL CALLS")
pre_call = res["has_call_[-30,-1]"].mean() * 100
post_call = res["has_call_[0,+30]"].mean() * 100
far_pre_call = res["has_call_[-180,-91]"].mean() * 100
print(f"  Baseline [-180,-91]:    {far_pre_call:.2f}%")
print(f"  Pre-event [-30,-1]:     {pre_call:.2f}%")
print(f"  Post-event [0,+30]:     {post_call:.2f}%")

# By event type
print(f"\n\n  DISTRIBUTION PROBABILITY BY EVENT TYPE")
print(f"  {'Event Type':<35} {'Baseline':>10} {'Pre[-30]':>10} {'Post[0+30]':>10} {'N':>6}")
print(f"  {'-' * 75}")
for et in ["M&A Transaction Announcements", "M&A Transaction Closings",
           "Executive/Board Changes - Other", "Bankruptcy"]:
    sub = res[res["event_type"].str.contains(et.split()[0], na=False)]
    if len(sub) < 50:
        sub = res[res["event_type"] == et]
    if len(sub) < 20:
        continue
    bl = sub["has_dist_[-180,-91]"].mean() * 100
    pr = sub["has_dist_[-30,-1]"].mean() * 100
    po = sub["has_dist_[0,+30]"].mean() * 100
    print(f"  {et[:35]:<35} {bl:>9.2f}% {pr:>9.2f}% {po:>9.2f}% {len(sub):>5}")

# Pre-2020 vs Post-2020
print(f"\n\n  PRE-2020 vs POST-2020")
print(f"  {'Window':<15} {'Pre-2020 Dist%':>15} {'Post-2020 Dist%':>16} {'Diff':>8}")
print(f"  {'-' * 60}")
for wname, _, _ in windows:
    pre20 = res[res["post_2020"] == 0][f"has_dist_{wname}"].mean() * 100
    post20 = res[res["post_2020"] == 1][f"has_dist_{wname}"].mean() * 100
    print(f"  {wname:<15} {pre20:>14.2f}% {post20:>15.2f}% {post20-pre20:>+7.2f}")

# Regression: within-event-pair, does the pre-event window have more activity than baseline?
print(f"\n\n  REGRESSION: Distribution probability by window (stacked panel)")
# Stack windows into a long panel
rows_long = []
for _, r in res.iterrows():
    for wname, ws, we in windows:
        rows_long.append({
            "event_id": f"{r['event_date']}_{r['ciq_vc']}",
            "ciq_vc": r["ciq_vc"],
            "year": r["year"],
            "post_2020": r["post_2020"],
            "window": wname,
            "has_dist": r[f"has_dist_{wname}"],
            "has_call": r[f"has_call_{wname}"],
            "n_dists": r[f"n_dists_{wname}"],
        })

long = pd.DataFrame(rows_long)
long["pre_30"] = (long["window"] == "[-30,-1]").astype(int)
long["pre_90"] = (long["window"] == "[-90,-31]").astype(int)
long["post_30"] = (long["window"] == "[0,+30]").astype(int)
long["post_90"] = (long["window"] == "[+31,+90]").astype(int)

y = long["has_dist"]
X = long[["pre_90", "pre_30", "post_30", "post_90"]].copy()
X = sm.add_constant(X)

print(f"  Baseline: [-180,-91] window")
print(f"  N = {len(long):,} (event-VC-window observations)")

for cov_label, cov_type, kwds in [("HC1", "HC1", {}),
                                    ("Event-cl", "cluster", {"groups": long["event_id"]})]:
    try:
        m = sm.OLS(y, X).fit(cov_type=cov_type, cov_kwds=kwds if kwds else {})
        print(f"\n  {cov_label}:")
        for var in ["pre_90", "pre_30", "post_30", "post_90"]:
            b = m.params.get(var, np.nan)
            p = m.pvalues.get(var, np.nan)
            s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {var:<10} b={b:>8.4f}{s:<3} p={p:.3f}  (vs baseline [-180,-91])")
        print(f"    Baseline (const): {m.params.get('const', np.nan):.4f}")
    except Exception as e:
        print(f"  {cov_label}: Error {str(e)[:50]}")

print("\n\nDone.")
