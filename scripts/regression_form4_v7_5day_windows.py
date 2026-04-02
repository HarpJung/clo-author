"""
Form 4 v7: 5-day windows for fine-grained event study.

Windows (all 5 trading days):
  [-30,-26], [-25,-21], [-20,-16], [-15,-11], [-10,-6], [-5,-1]
  [0,+4], [+5,+9], [+10,+14], [+15,+19], [+20,+24], [+25,+29]

Baseline: average of [-60,-56], [-55,-51], [-50,-46], [-45,-41], [-40,-36], [-35,-31]

DV: trades per day, sells per day, buys per day
Samples: Full, Loose time-matched, Strict time-matched
Specs: HC1, Event-cl, Person-cl, PersonFE+HC1
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

print("=" * 90)
print("FORM 4 v7: 5-Day Windows")
print("=" * 90)

# === Load all data (same as v6) ===
trades = pd.read_csv(os.path.join(data_dir, "Form4/observer_form4_trades.csv"))
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce", format="mixed")
trades = trades.dropna(subset=["trandate"])
trades = trades[trades["trancode"].isin(["P", "S"])]
tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
trades["ciq_pid"] = trades["personid"].map(dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"])))
trades = trades.dropna(subset=["ciq_pid"])

obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_us = obs[obs["companyid"].isin(us_priv)]
obs_to_cos = {}
for _, r in obs_us.iterrows():
    pid = r["personid"]
    if pid not in obs_to_cos:
        obs_to_cos[pid] = set()
    obs_to_cos[pid].add(r["companyid"])

events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events = events[events["companyid"].isin(us_priv)]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
events["material"] = events["eventtype"].apply(
    lambda x: "M&A" in str(x) or "Bankruptcy" in str(x) or "Executive/Board" in str(x) or "Restructuring" in str(x))
mat_events = events[events["material"]]

# Position dates
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
func["end_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['endyear'])}-12-31" if pd.notna(r.get("endyear")) else None, axis=1),
    errors="coerce")
pc = func.groupby(["personid", "companyid"]).agg(
    obs_start=("start_date", "min"), obs_end=("end_date", "max"), is_current=("currentflag", "max")).reset_index()
pc.loc[pc["is_current"] == 1, "obs_end"] = pc.loc[pc["is_current"] == 1, "obs_end"].fillna(pd.Timestamp("2026-12-31"))

trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))
trans["companyid"] = trans["companyid"].astype(str).str.replace(".0", "", regex=False)
trans["close_date"] = pd.to_datetime(
    trans.apply(lambda r: f"{int(r['closingyear'])}-{int(r['closingmonth']):02d}-{int(r['closingday']):02d}"
                if pd.notna(r.get("closingyear")) and pd.notna(r.get("closingmonth")) and pd.notna(r.get("closingday"))
                else None, axis=1), errors="coerce")
first_trans = dict(trans.groupby("companyid")["close_date"].min())

strict_lookup = {}
loose_lookup = {}
for _, r in pc.iterrows():
    key = (r["personid"], r["companyid"])
    if pd.notna(r["obs_start"]):
        end = r["obs_end"] if pd.notna(r["obs_end"]) else pd.Timestamp("2026-12-31")
        strict_lookup[key] = (r["obs_start"], end)
    start = r["obs_start"]
    if pd.isna(start):
        start = first_trans.get(r["companyid"])
    if pd.isna(start):
        start = pd.Timestamp("2005-01-01")
    end = r["obs_end"]
    if pd.isna(end):
        end = pd.Timestamp("2026-12-31") if r["is_current"] == 1 else pd.Timestamp("2020-12-31")
    loose_lookup[key] = (start, end)

print(f"  Trades: {len(trades):,}, Events: {len(mat_events):,}")

# === 5-day windows ===
# Baseline: [-60,-31] in 5-day chunks
# Pre-event: [-30,-1] in 5-day chunks
# Post-event: [0,+29] in 5-day chunks
baseline_windows = [(f"B{i}", -60 + i * 5, -60 + i * 5 + 4) for i in range(6)]  # [-60,-56] to [-35,-31]
pre_windows = [(f"P{i}", -30 + i * 5, -30 + i * 5 + 4) for i in range(6)]  # [-30,-26] to [-5,-1]
post_windows = [(f"A{i}", i * 5, i * 5 + 4) for i in range(6)]  # [0,+4] to [+25,+29]
all_windows = baseline_windows + pre_windows + post_windows

print(f"\n  Windows (all 5 days each):")
print(f"    Baseline: {', '.join(f'[{w[1]},{w[2]}]' for w in baseline_windows)}")
print(f"    Pre-event: {', '.join(f'[{w[1]},{w[2]}]' for w in pre_windows)}")
print(f"    Post-event: {', '.join(f'[{w[1]},{w[2]}]' for w in post_windows)}")

# Build panel
print("\n--- Building 5-day windows ---")
rows = []
for ciq_pid in set(trades["ciq_pid"]):
    observed_cos = obs_to_cos.get(ciq_pid, set())
    if not observed_cos:
        continue
    ptrades = trades[trades["ciq_pid"] == ciq_pid]
    pevents = mat_events[mat_events["companyid"].isin(observed_cos)]

    for _, evt in pevents.iterrows():
        edate = evt["announcedate"]
        ecid = evt["companyid"]
        sk = (ciq_pid, ecid)
        is_strict = sk in strict_lookup and strict_lookup[sk][0] <= edate <= strict_lookup[sk][1]
        is_loose = sk in loose_lookup and loose_lookup[sk][0] <= edate <= loose_lookup[sk][1]

        for wname, ws, we in all_windows:
            d_start = edate + pd.Timedelta(days=ws)
            d_end = edate + pd.Timedelta(days=we)
            wt = ptrades[(ptrades["trandate"] >= d_start) & (ptrades["trandate"] <= d_end)]
            nd = we - ws + 1

            rows.append({
                "eid": f"{edate.date()}_{ecid}_{ciq_pid}",
                "pid": ciq_pid, "ecid": ecid, "edate": edate,
                "year": edate.year, "window": wname,
                "ws": ws, "we": we,
                "tpd": len(wt) / nd,
                "spd": (wt["trancode"] == "S").sum() / nd,
                "bpd": (wt["trancode"] == "P").sum() / nd,
                "post2020": 1 if edate.year >= 2020 else 0,
                "strict": int(is_strict), "loose": int(is_loose),
                "is_baseline": 1 if wname.startswith("B") else 0,
                "is_pre": 1 if wname.startswith("P") else 0,
                "is_post": 1 if wname.startswith("A") else 0,
            })

df = pd.DataFrame(rows)
df_loose = df[df["loose"] == 1].copy()
df_strict = df[df["strict"] == 1].copy()

print(f"  Full: {len(df):,} ({df['eid'].nunique():,} event-pairs)")
print(f"  Loose: {len(df_loose):,} ({len(df_loose)/len(df)*100:.1f}%)")
print(f"  Strict: {len(df_strict):,} ({len(df_strict)/len(df)*100:.1f}%)")

# === Mean trading rates by 5-day window ===
print(f"\n\n{'=' * 90}")
print("MEAN TRADING RATES BY 5-DAY WINDOW")
print(f"{'=' * 90}")

for sample, label in [(df, "FULL"), (df_loose, "LOOSE"), (df_strict, "STRICT")]:
    if len(sample) < 100:
        continue
    print(f"\n  {label} (N={len(sample):,})")
    print(f"  {'Window':<12} {'Days':<12} {'Trades/day':>12} {'Sells/day':>12} {'Buys/day':>12}")
    print(f"  {'-' * 60}")
    for wname, ws, we in all_windows:
        sub = sample[sample["window"] == wname]
        marker = ""
        if wname == "P5":
            marker = " <-- [-5,-1]"
        elif wname == "P4":
            marker = " <-- [-10,-6]"
        elif wname == "A0":
            marker = " <-- [0,+4]"
        print(f"  {wname:<5} [{ws:>+4},{we:>+4}]  {sub['tpd'].mean():>12.6f} {sub['spd'].mean():>12.6f} {sub['bpd'].mean():>12.6f}{marker}")

# === Regression: pre-event dummies vs baseline ===
print(f"\n\n{'=' * 90}")
print("REGRESSION: Pre/Post 5-day dummies vs Baseline")
print(f"{'=' * 90}")

# Create dummies for each pre and post window
for d in [df, df_loose, df_strict]:
    for wname, _, _ in pre_windows + post_windows:
        d[f"d_{wname}"] = (d["window"] == wname).astype(int)

pre_post_vars = [f"d_{w[0]}" for w in pre_windows + post_windows]

for sample, label in [(df, "FULL"), (df_loose, "LOOSE")]:
    if len(sample) < 1000:
        continue
    y = sample["tpd"]
    X = sample[pre_post_vars].copy()
    X = sm.add_constant(X)
    idx = y.index

    print(f"\n  {label} — DV: Trades/day (N={len(y):,})")
    for sname, cov, kwds in [
        ("HC1", "HC1", {}),
        ("Event-cl", "cluster", {"groups": sample.loc[idx, "eid"]}),
        ("Person-cl", "cluster", {"groups": sample.loc[idx, "pid"]}),
    ]:
        try:
            m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            base = m.params.get("const", np.nan)
            print(f"\n    {sname} (baseline={base:.6f}/day)")
            for wname, ws, we in pre_windows + post_windows:
                b = m.params.get(f"d_{wname}", np.nan)
                p = m.pvalues.get(f"d_{wname}", np.nan)
                s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                pct = b / base * 100 if base > 0 else 0
                marker = " <---" if wname in ["P4", "P5", "A0"] else ""
                print(f"      [{ws:>+4},{we:>+4}] b={b:>10.6f}{s:<3} p={p:.3f}  ({pct:>+6.1f}% vs base){marker}")
        except Exception as e:
            print(f"    {sname}: Error {str(e)[:50]}")

# Sells specifically
print(f"\n\n{'=' * 90}")
print("SELLS/DAY by 5-day window")
print(f"{'=' * 90}")

for sample, label in [(df_loose, "LOOSE")]:
    y = sample["spd"]
    X = sample[pre_post_vars].copy()
    X = sm.add_constant(X)
    idx = y.index
    for sname, cov, kwds in [("HC1", "HC1", {}), ("Event-cl", "cluster", {"groups": sample.loc[idx, "eid"]})]:
        try:
            m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            base = m.params.get("const", np.nan)
            print(f"\n  {label} {sname} (baseline={base:.6f}/day)")
            for wname, ws, we in pre_windows + post_windows:
                b = m.params.get(f"d_{wname}", np.nan)
                p = m.pvalues.get(f"d_{wname}", np.nan)
                s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                print(f"    [{ws:>+4},{we:>+4}] b={b:>10.6f}{s:<3} p={p:.3f}")
        except Exception as e:
            print(f"  {sname}: Error {str(e)[:50]}")

# Buys
print(f"\n\n{'=' * 90}")
print("BUYS/DAY by 5-day window")
print(f"{'=' * 90}")

for sample, label in [(df_loose, "LOOSE")]:
    y = sample["bpd"]
    X = sample[pre_post_vars].copy()
    X = sm.add_constant(X)
    idx = y.index
    for sname, cov, kwds in [("HC1", "HC1", {}), ("Event-cl", "cluster", {"groups": sample.loc[idx, "eid"]})]:
        try:
            m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            base = m.params.get("const", np.nan)
            print(f"\n  {label} {sname} (baseline={base:.6f}/day)")
            for wname, ws, we in pre_windows + post_windows:
                b = m.params.get(f"d_{wname}", np.nan)
                p = m.pvalues.get(f"d_{wname}", np.nan)
                s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                print(f"    [{ws:>+4},{we:>+4}] b={b:>10.6f}{s:<3} p={p:.3f}")
        except Exception as e:
            print(f"  {sname}: Error {str(e)[:50]}")

print("\n\nDone.")
