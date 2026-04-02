"""
Form 4 v6: Equal 30-day windows + time-matched + full FE/clustering battery.

Combines:
  - Equal 30-day windows (fixes window-length artifact)
  - Position-date filtering (only events where observer verifiably active)
  - Per-day trading rate as DV
  - Full spec battery: HC1, event-cl, person-cl, company-cl, year FE, person FE, quarter FE
  - NVCA 2020 DiD with all specs
  - Three samples: Full, Active-only (position dates), Strict (protoprofunction dates only)
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
print("FORM 4 v6: Time-Matched + Equal Windows + Full Specs")
print("=" * 90)

# === Load all data ===
print("\n--- Loading ---")
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

# === Position dates ===
print("  Loading position dates...")
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
    obs_start=("start_date", "min"), obs_end=("end_date", "max"), is_current=("currentflag", "max")
).reset_index()
pc_dates.loc[pc_dates["is_current"] == 1, "obs_end"] = pc_dates.loc[
    pc_dates["is_current"] == 1, "obs_end"].fillna(pd.Timestamp("2026-12-31"))

# Strict dates: only positions with actual protoprofunction start dates
strict_lookup = {}
for _, r in pc_dates.dropna(subset=["obs_start"]).iterrows():
    key = (r["personid"], r["companyid"])
    end = r["obs_end"] if pd.notna(r["obs_end"]) else pd.Timestamp("2026-12-31")
    strict_lookup[key] = (r["obs_start"], end)

# Loose dates: protoprofunction + CIQ transaction fallback + current/former
trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))
trans["companyid"] = trans["companyid"].astype(str).str.replace(".0", "", regex=False)
trans["close_date"] = pd.to_datetime(
    trans.apply(lambda r: f"{int(r['closingyear'])}-{int(r['closingmonth']):02d}-{int(r['closingday']):02d}"
                if pd.notna(r.get("closingyear")) and pd.notna(r.get("closingmonth")) and pd.notna(r.get("closingday"))
                else None, axis=1), errors="coerce")
first_trans = dict(trans.groupby("companyid")["close_date"].min())

loose_lookup = {}
for _, r in pc_dates.iterrows():
    key = (r["personid"], r["companyid"])
    start = r["obs_start"]
    if pd.isna(start):
        start = first_trans.get(r["companyid"])
    if pd.isna(start):
        start = pd.Timestamp("2005-01-01")
    end = r["obs_end"]
    if pd.isna(end):
        if r["is_current"] == 1:
            end = pd.Timestamp("2026-12-31")
        else:
            end = pd.Timestamp("2020-12-31")
    loose_lookup[key] = (start, end)

print(f"  Strict date pairs (protoprofunction): {len(strict_lookup):,}")
print(f"  Loose date pairs (all sources): {len(loose_lookup):,}")

print(f"\n  Trades: {len(trades):,}, Events: {len(mat_events):,}")

# === Build equal 30-day windows with three samples ===
print("\n--- Building windows ---")

windows = [
    ("[-180,-151]", -180, -151), ("[-150,-121]", -150, -121), ("[-120,-91]", -120, -91),
    ("[-90,-61]", -90, -61), ("[-60,-31]", -60, -31), ("[-30,-1]", -30, -1),
    ("[0,+29]", 0, 29), ("[+30,+59]", 30, 59), ("[+60,+89]", 60, 89),
]

rows = []
for ciq_pid in set(trades["ciq_pid"]):
    observed_cos = obs_to_cos.get(ciq_pid, set())
    if not observed_cos:
        continue
    person_trades = trades[trades["ciq_pid"] == ciq_pid]
    person_events = mat_events[mat_events["companyid"].isin(observed_cos)]

    for _, evt in person_events.iterrows():
        edate = evt["announcedate"]
        ecid = evt["companyid"]

        # Check timing
        strict_key = (ciq_pid, ecid)
        is_strict = False
        if strict_key in strict_lookup:
            s, e = strict_lookup[strict_key]
            if s <= edate <= e:
                is_strict = True

        loose_key = (ciq_pid, ecid)
        is_loose = False
        if loose_key in loose_lookup:
            s, e = loose_lookup[loose_key]
            if s <= edate <= e:
                is_loose = True

        for wname, ws, we in windows:
            d_start = edate + pd.Timedelta(days=ws)
            d_end = edate + pd.Timedelta(days=we)
            w_trades = person_trades[(person_trades["trandate"] >= d_start) & (person_trades["trandate"] <= d_end)]
            n_days = we - ws + 1

            # Get public company name for clustering
            pub_co = w_trades["cname"].iloc[0] if len(w_trades) > 0 else ""

            rows.append({
                "event_id": f"{edate.date()}_{ecid}_{ciq_pid}",
                "ciq_pid": ciq_pid,
                "event_cid": ecid,
                "pub_company": pub_co,
                "event_date": edate,
                "year": edate.year,
                "yq": f"{edate.year}Q{(edate.month-1)//3+1}",
                "window": wname,
                "trades_per_day": len(w_trades) / n_days,
                "sells_per_day": (w_trades["trancode"] == "S").sum() / n_days,
                "buys_per_day": (w_trades["trancode"] == "P").sum() / n_days,
                "has_trade": 1 if len(w_trades) > 0 else 0,
                "post_2020": 1 if edate.year >= 2020 else 0,
                "is_strict": int(is_strict),
                "is_loose": int(is_loose),
            })

df = pd.DataFrame(rows)
df_loose = df[df["is_loose"] == 1].copy()
df_strict = df[df["is_strict"] == 1].copy()

print(f"  Full sample: {len(df):,} ({df['event_id'].nunique():,} event-pairs)")
print(f"  Loose time-matched: {len(df_loose):,} ({len(df_loose)/len(df)*100:.1f}%)")
print(f"  Strict time-matched: {len(df_strict):,} ({len(df_strict)/len(df)*100:.1f}%)")

# Window dummies
for d in [df, df_loose, df_strict]:
    d["pre_90"] = (d["window"] == "[-90,-61]").astype(int)
    d["pre_60"] = (d["window"] == "[-60,-31]").astype(int)
    d["pre_30"] = (d["window"] == "[-30,-1]").astype(int)
    d["post_0"] = (d["window"] == "[0,+29]").astype(int)
    d["post_30"] = (d["window"] == "[+30,+59]").astype(int)
    d["post_60"] = (d["window"] == "[+60,+89]").astype(int)
    d["pre30_x_post2020"] = d["pre_30"] * d["post_2020"]

# === Regressions ===
print(f"\n\n{'=' * 90}")
print("RESULTS")
print(f"{'=' * 90}")

xvars = ["pre_90", "pre_60", "pre_30", "post_0", "post_30", "post_60"]

def run_all_specs(data, dv, label):
    y = data[dv]
    X = data[xvars].copy()
    X = sm.add_constant(X)
    idx = y.index

    if len(y) < 500:
        print(f"\n  {label}: N={len(y)}, too few")
        return

    # Year and quarter dummies
    yr = pd.get_dummies(data.loc[idx, "year"], prefix="yr", drop_first=True).astype(float)
    qr = pd.get_dummies(data.loc[idx, "yq"], prefix="yq", drop_first=True).astype(float)
    X_yr = pd.concat([data.loc[idx, xvars], yr], axis=1)
    X_yr = sm.add_constant(X_yr)
    X_qr = pd.concat([data.loc[idx, xvars], qr], axis=1)
    X_qr = sm.add_constant(X_qr)

    # Person FE
    pmean = data.loc[idx].groupby("ciq_pid")[dv].transform("mean")
    y_pfe = y - pmean

    print(f"\n  {label} (N={len(y):,})")

    specs = [
        ("HC1", y, X, "HC1", {}),
        ("Event-cl", y, X, "cluster", {"groups": data.loc[idx, "event_id"]}),
        ("Person-cl", y, X, "cluster", {"groups": data.loc[idx, "ciq_pid"]}),
        ("YrFE+Evt-cl", y, X_yr, "cluster", {"groups": data.loc[idx, "event_id"]}),
        ("YrFE+Per-cl", y, X_yr, "cluster", {"groups": data.loc[idx, "ciq_pid"]}),
        ("QtrFE+Evt-cl", y, X_qr, "cluster", {"groups": data.loc[idx, "event_id"]}),
        ("PersonFE+HC1", y_pfe, X, "HC1", {}),
    ]

    for sname, dep, xmat, cov, kwds in specs:
        try:
            m = sm.OLS(dep, xmat).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            b30 = m.params.get("pre_30", np.nan)
            p30 = m.pvalues.get("pre_30", np.nan)
            b60 = m.params.get("pre_60", np.nan)
            p60 = m.pvalues.get("pre_60", np.nan)
            s30 = "***" if p30 < 0.01 else "**" if p30 < 0.05 else "*" if p30 < 0.10 else ""
            s60 = "***" if p60 < 0.01 else "**" if p60 < 0.05 else "*" if p60 < 0.10 else ""
            base = m.params.get("const", np.nan)
            print(f"    {sname:<16} pre_60={b60:>10.6f}{s60:<3} pre_30={b30:>10.6f}{s30:<3} (base={base:.6f})")
        except Exception as e:
            print(f"    {sname:<16} Error: {str(e)[:50]}")

# --- Trades per day ---
print(f"\n{'=' * 90}")
print("  DV: Trades per day")
print(f"{'=' * 90}")

for sample, label in [(df, "FULL"), (df_loose, "LOOSE TIME-MATCHED"), (df_strict, "STRICT TIME-MATCHED")]:
    run_all_specs(sample, "trades_per_day", label)

# --- Sells per day ---
print(f"\n{'=' * 90}")
print("  DV: Sells per day")
print(f"{'=' * 90}")

for sample, label in [(df, "FULL"), (df_loose, "LOOSE TIME-MATCHED")]:
    run_all_specs(sample, "sells_per_day", label)

# --- Buys per day ---
print(f"\n{'=' * 90}")
print("  DV: Buys per day")
print(f"{'=' * 90}")

for sample, label in [(df, "FULL"), (df_loose, "LOOSE TIME-MATCHED")]:
    run_all_specs(sample, "buys_per_day", label)

# --- NVCA DiD ---
print(f"\n{'=' * 90}")
print("  NVCA 2020 DiD (pre_30 x post_2020)")
print(f"{'=' * 90}")

xvars_did = xvars + ["post_2020", "pre30_x_post2020"]

for sample, label in [(df, "FULL"), (df_loose, "LOOSE TIME-MATCHED"), (df_strict, "STRICT TIME-MATCHED")]:
    if len(sample) < 500:
        print(f"\n  {label}: too few")
        continue
    y = sample["trades_per_day"]
    X = sample[xvars_did].copy()
    X = sm.add_constant(X)
    idx = y.index

    print(f"\n  {label} (N={len(y):,})")
    for sname, cov, kwds in [
        ("HC1", "HC1", {}),
        ("Event-cl", "cluster", {"groups": sample.loc[idx, "event_id"]}),
        ("Person-cl", "cluster", {"groups": sample.loc[idx, "ciq_pid"]}),
    ]:
        try:
            m = sm.OLS(y, X).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            bp = m.params.get("pre_30", np.nan); pp = m.pvalues.get("pre_30", np.nan)
            bd = m.params.get("pre30_x_post2020", np.nan); pd_ = m.pvalues.get("pre30_x_post2020", np.nan)
            sp = "***" if pp < 0.01 else "**" if pp < 0.05 else "*" if pp < 0.10 else ""
            sd = "***" if pd_ < 0.01 else "**" if pd_ < 0.05 else "*" if pd_ < 0.10 else ""
            print(f"    {sname:<12} pre_30={bp:>10.6f}{sp:<3} p={pp:.3f}  DiD={bd:>10.6f}{sd:<3} p={pd_:.3f}")
        except Exception as e:
            print(f"    {sname:<12} Error: {str(e)[:50]}")

    # Split sample
    for period, mask in [("Pre-2020", sample["post_2020"] == 0), ("Post-2020", sample["post_2020"] == 1)]:
        sub = sample[mask]
        if len(sub) < 300:
            continue
        y_s = sub["trades_per_day"]
        X_s = sub[xvars].copy()
        X_s = sm.add_constant(X_s)
        try:
            m = sm.OLS(y_s, X_s).fit(cov_type="cluster", cov_kwds={"groups": sub["event_id"]})
            b = m.params.get("pre_30", np.nan); p = m.pvalues.get("pre_30", np.nan)
            s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {period:<12} pre_30={b:>10.6f}{s:<3} p={p:.3f}  (N={len(y_s):,})")
        except Exception as e:
            print(f"    {period:<12} Error: {str(e)[:50]}")

# --- Means table ---
print(f"\n\n{'=' * 90}")
print("  MEAN TRADING RATES BY WINDOW")
print(f"{'=' * 90}")
print(f"\n  {'Window':<15} {'Full: tpd':>12} {'Loose: tpd':>12} {'Strict: tpd':>12}")
print(f"  {'-' * 55}")
for wname, _, _ in windows:
    f_mean = df[df["window"] == wname]["trades_per_day"].mean()
    l_mean = df_loose[df_loose["window"] == wname]["trades_per_day"].mean() if len(df_loose) > 0 else np.nan
    s_mean = df_strict[df_strict["window"] == wname]["trades_per_day"].mean() if len(df_strict) > 0 else np.nan
    marker = " <--" if wname == "[-30,-1]" else ""
    print(f"  {wname:<15} {f_mean:>12.6f} {l_mean:>12.6f} {s_mean:>12.6f}{marker}")

print("\n\nDone.")
