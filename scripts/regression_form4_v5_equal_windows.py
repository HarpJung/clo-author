"""
Form 4 v5: Equal-length windows to fix the window-length confound.

All windows are exactly 30 days:
  [-180,-151]  Far baseline
  [-150,-121]
  [-120,-91]
  [-90,-61]    Mid pre-event
  [-60,-31]    Near pre-event
  [-30,-1]     Immediate pre-event (KEY window)
  [0,+29]      Post-announcement
  [+30,+59]
  [+60,+89]    Far post

DV: trades per day in each window (continuous, not binary)
This avoids the mechanical window-length effect.
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
print("FORM 4 v5: Equal 30-Day Windows")
print("=" * 90)

# === Load data ===
trades = pd.read_csv(os.path.join(data_dir, "Form4/observer_form4_trades.csv"))
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce", format="mixed")
trades = trades.dropna(subset=["trandate"])
trades = trades[trades["trancode"].isin(["P", "S"])]

tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
trades["ciq_personid"] = trades["personid"].map(dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"])))
trades = trades.dropna(subset=["ciq_personid"])

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

# Position dates for active-only filtering
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
pc_dates = func.groupby(["personid", "companyid"]).agg(
    obs_start=("start_date", "min"), obs_end=("end_date", "max"), is_current=("currentflag", "max")).reset_index()
pc_dates["obs_start"] = pc_dates["obs_start"].fillna(pd.Timestamp("2005-01-01"))
pc_dates.loc[pc_dates["is_current"] == 1, "obs_end"] = pc_dates.loc[pc_dates["is_current"] == 1, "obs_end"].fillna(pd.Timestamp("2026-12-31"))
pc_dates["obs_end"] = pc_dates["obs_end"].fillna(pd.Timestamp("2020-12-31"))
date_lookup = {(r["personid"], r["companyid"]): (r["obs_start"], r["obs_end"]) for _, r in pc_dates.iterrows()}

print(f"  Trades: {len(trades):,}, Material events: {len(mat_events):,}")

# === Equal 30-day windows ===
windows = [
    ("[-180,-151]", -180, -151),
    ("[-150,-121]", -150, -121),
    ("[-120,-91]", -120, -91),
    ("[-90,-61]", -90, -61),
    ("[-60,-31]", -60, -31),
    ("[-30,-1]", -30, -1),
    ("[0,+29]", 0, 29),
    ("[+30,+59]", 30, 59),
    ("[+60,+89]", 60, 89),
]

print("\n--- Building equal 30-day windows ---")
rows = []

for ciq_pid in set(trades["ciq_personid"]):
    observed_cos = obs_to_cos.get(ciq_pid, set())
    if not observed_cos:
        continue
    person_trades = trades[trades["ciq_personid"] == ciq_pid]
    person_events = mat_events[mat_events["companyid"].isin(observed_cos)]

    for _, evt in person_events.iterrows():
        edate = evt["announcedate"]
        ecid = evt["companyid"]
        obs_start, obs_end = date_lookup.get((ciq_pid, ecid), (pd.Timestamp("2005-01-01"), pd.Timestamp("2026-12-31")))
        is_active = obs_start <= edate <= obs_end

        for wname, ws, we in windows:
            d_start = edate + pd.Timedelta(days=ws)
            d_end = edate + pd.Timedelta(days=we)
            w_trades = person_trades[(person_trades["trandate"] >= d_start) & (person_trades["trandate"] <= d_end)]
            n_days = we - ws + 1  # should be 30 for all windows

            rows.append({
                "event_id": f"{edate}_{ecid}_{ciq_pid}",
                "ciq_personid": ciq_pid,
                "event_companyid": ecid,
                "event_date": edate,
                "year": edate.year,
                "window": wname,
                "n_trades": len(w_trades),
                "trades_per_day": len(w_trades) / n_days,
                "has_trade": 1 if len(w_trades) > 0 else 0,
                "n_sells": (w_trades["trancode"] == "S").sum(),
                "sells_per_day": (w_trades["trancode"] == "S").sum() / n_days,
                "n_buys": (w_trades["trancode"] == "P").sum(),
                "buys_per_day": (w_trades["trancode"] == "P").sum() / n_days,
                "post_2020": 1 if edate.year >= 2020 else 0,
                "is_active": int(is_active),
            })

df = pd.DataFrame(rows)
df_active = df[df["is_active"] == 1].copy()

print(f"  Full: {len(df):,} window-obs, {df['event_id'].nunique():,} event-pairs")
print(f"  Active: {len(df_active):,} window-obs ({len(df_active)/len(df)*100:.1f}%)")

# Window dummies (baseline = average of [-180,-151], [-150,-121], [-120,-91])
for d in df, df_active:
    d["is_baseline"] = d["window"].isin(["[-180,-151]", "[-150,-121]", "[-120,-91]"]).astype(int)
    d["pre_90_60"] = (d["window"] == "[-90,-61]").astype(int)
    d["pre_60_30"] = (d["window"] == "[-60,-31]").astype(int)
    d["pre_30_0"] = (d["window"] == "[-30,-1]").astype(int)
    d["post_0_30"] = (d["window"] == "[0,+29]").astype(int)
    d["post_30_60"] = (d["window"] == "[+30,+59]").astype(int)
    d["post_60_90"] = (d["window"] == "[+60,+89]").astype(int)
    d["pre30_x_post2020"] = d["pre_30_0"] * d["post_2020"]

# === Results ===
print(f"\n\n{'=' * 90}")
print("RESULTS: Equal 30-Day Windows")
print(f"{'=' * 90}")

# Means by window
print(f"\n  TRADES PER DAY BY WINDOW:")
print(f"  {'Window':<15} {'Full: tpd':>12} {'has_trade':>10}  {'Active: tpd':>12} {'has_trade':>10}")
print(f"  {'-' * 65}")
for wname, _, _ in windows:
    f_sub = df[df["window"] == wname]
    a_sub = df_active[df_active["window"] == wname]
    print(f"  {wname:<15} {f_sub['trades_per_day'].mean():>12.5f} {f_sub['has_trade'].mean()*100:>9.2f}%  "
          f"{a_sub['trades_per_day'].mean():>12.5f} {a_sub['has_trade'].mean()*100:>9.2f}%")

# Regression
xvars = ["pre_90_60", "pre_60_30", "pre_30_0", "post_0_30", "post_30_60", "post_60_90"]

def run_specs(data, dv, label, xv=xvars):
    y = data[dv]
    X = data[xv].copy()
    X = sm.add_constant(X)
    idx = y.index

    print(f"\n  {label} — DV: {dv} (N={len(y):,})")
    for sname, cov, kwds in [
        ("HC1", "HC1", {}),
        ("Event-cl", "cluster", {"groups": data.loc[idx, "event_id"]}),
        ("Person-cl", "cluster", {"groups": data.loc[idx, "ciq_personid"]}),
        ("PersonFE+HC1", None, None),  # special handling
    ]:
        if sname == "PersonFE+HC1":
            pmean = data.loc[idx].groupby("ciq_personid")[dv].transform("mean")
            y_fe = y - pmean
            X_fe = data.loc[idx, xv].copy()
            X_fe = sm.add_constant(X_fe)
            try:
                m = sm.OLS(y_fe, X_fe).fit(cov_type="HC1")
                b = m.params.get("pre_30_0", np.nan)
                p = m.pvalues.get("pre_30_0", np.nan)
                s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                print(f"    {sname:<15} pre_30_0={b:>10.6f}{s:<3} p={p:.3f}")
            except Exception as e:
                print(f"    {sname:<15} Error: {str(e)[:50]}")
        else:
            try:
                m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
                b = m.params.get("pre_30_0", np.nan)
                p = m.pvalues.get("pre_30_0", np.nan)
                s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                base = m.params.get("const", np.nan)
                print(f"    {sname:<15} pre_30_0={b:>10.6f}{s:<3} p={p:.3f}  (baseline={base:.6f})")
            except Exception as e:
                print(f"    {sname:<15} Error: {str(e)[:50]}")

# Test A: Trading rate per day
for sample, label in [(df, "FULL"), (df_active, "ACTIVE-ONLY")]:
    run_specs(sample, "trades_per_day", f"{label}: Trades per day")
    run_specs(sample, "sells_per_day", f"{label}: Sells per day")
    run_specs(sample, "buys_per_day", f"{label}: Buys per day")

# Test B: NVCA DiD
print(f"\n{'─' * 90}")
print("  NVCA 2020 DiD (pre_30_0 x post_2020)")
print(f"{'─' * 90}")

xvars_did = xvars + ["post_2020", "pre30_x_post2020"]
for sample, label in [(df, "FULL"), (df_active, "ACTIVE-ONLY")]:
    y = sample["trades_per_day"]
    X = sample[xvars_did].copy()
    X = sm.add_constant(X)
    idx = y.index

    print(f"\n  {label} (N={len(y):,})")
    for sname, cov, kwds in [
        ("HC1", "HC1", {}),
        ("Event-cl", "cluster", {"groups": sample.loc[idx, "event_id"]}),
        ("Person-cl", "cluster", {"groups": sample.loc[idx, "ciq_personid"]}),
    ]:
        try:
            m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            bp = m.params.get("pre_30_0", np.nan)
            pp = m.pvalues.get("pre_30_0", np.nan)
            bd = m.params.get("pre30_x_post2020", np.nan)
            pd_val = m.pvalues.get("pre30_x_post2020", np.nan)
            sp = "***" if pp < 0.01 else "**" if pp < 0.05 else "*" if pp < 0.10 else ""
            sd = "***" if pd_val < 0.01 else "**" if pd_val < 0.05 else "*" if pd_val < 0.10 else ""
            print(f"    {sname:<15} pre_30={bp:>10.6f}{sp:<3} p={pp:.3f}  DiD={bd:>10.6f}{sd:<3} p={pd_val:.3f}")
        except Exception as e:
            print(f"    {sname:<15} Error: {str(e)[:50]}")

    # Pre vs post split
    for period, mask in [("Pre-2020", sample["post_2020"] == 0), ("Post-2020", sample["post_2020"] == 1)]:
        sub = sample[mask]
        if len(sub) < 500:
            continue
        y_s = sub["trades_per_day"]
        X_s = sub[xvars].copy()
        X_s = sm.add_constant(X_s)
        try:
            m = sm.OLS(y_s, X_s).fit(cov_type="cluster", cov_kwds={"groups": sub["event_id"]})
            b = m.params.get("pre_30_0", np.nan)
            p = m.pvalues.get("pre_30_0", np.nan)
            s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {period:<15} pre_30={b:>10.6f}{s:<3} p={p:.3f}  (N={len(y_s):,})")
        except Exception as e:
            print(f"    {period:<15} Error: {str(e)[:50]}")

print("\n\nDone.")
