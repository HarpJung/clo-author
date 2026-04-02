"""
Form 4 v3: Full spec battery + position date filtering + confirmed subsample.

Combines:
  - 10 FE/clustering specs
  - Active-only subsample using ciqprotoprofunction dates
  - Form D confirmed subsample
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
print("FORM 4 v3: Full Specs + Position Dates + Confirmed Subsample")
print("=" * 90)

# === Load data (same as v2) ===
print("\n--- Loading data ---")
trades = pd.read_csv(os.path.join(data_dir, "Form4/observer_form4_trades.csv"))
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce", format="mixed")
trades = trades.dropna(subset=["trandate"])
trades = trades[trades["trancode"].isin(["P", "S"])]

tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
tr_to_ciq = dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
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

print(f"  Trades: {len(trades):,}")
print(f"  Material events: {len(mat_events):,}")

# === Load position dates ===
print("\n--- Loading position dates ---")
func = pd.read_csv(os.path.join(ciq_dir, "11_observer_position_dates.csv"))
proid_map = pd.read_csv(os.path.join(ciq_dir, "11b_observer_proid_mapping.csv"))
proid_map["proid"] = proid_map["proid"].astype(str).str.replace(".0", "", regex=False)
proid_map["personid"] = proid_map["personid"].astype(str).str.replace(".0", "", regex=False)
proid_map["companyid"] = proid_map["companyid"].astype(str).str.replace(".0", "", regex=False)
func["proid"] = func["proid"].astype(str).str.replace(".0", "", regex=False)
func = func.merge(proid_map[["proid", "personid", "companyid"]], on="proid", how="left")

func["start_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['startyear'])}-{int(r['startmonth']):02d}-{int(r['startday']):02d}"
               if pd.notna(r.get("startyear")) and pd.notna(r.get("startmonth")) and pd.notna(r.get("startday"))
               else (f"{int(r['startyear'])}-01-01" if pd.notna(r.get("startyear")) else None), axis=1),
    errors="coerce")
func["end_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['endyear'])}-{int(r['endmonth']):02d}-{int(r['endday']):02d}"
               if pd.notna(r.get("endyear")) and pd.notna(r.get("endmonth")) and pd.notna(r.get("endday"))
               else (f"{int(r['endyear'])}-12-31" if pd.notna(r.get("endyear")) else None), axis=1),
    errors="coerce")

person_co_dates = func.groupby(["personid", "companyid"]).agg(
    obs_start=("start_date", "min"),
    obs_end=("end_date", "max"),
    is_current=("currentflag", "max"),
).reset_index()

# Fill missing: current = active through 2026, former without dates = active 2005-2020
person_co_dates["obs_start"] = person_co_dates["obs_start"].fillna(pd.Timestamp("2005-01-01"))
person_co_dates.loc[person_co_dates["is_current"] == 1, "obs_end"] = person_co_dates.loc[
    person_co_dates["is_current"] == 1, "obs_end"].fillna(pd.Timestamp("2026-12-31"))
person_co_dates["obs_end"] = person_co_dates["obs_end"].fillna(pd.Timestamp("2020-12-31"))

date_lookup = {}
for _, r in person_co_dates.iterrows():
    key = (r["personid"], r["companyid"])
    date_lookup[key] = (r["obs_start"], r["obs_end"])

print(f"  Person-company date pairs: {len(date_lookup):,}")

# === Load Form D confirmed links ===
formd_confirmed = set()
formd_file = os.path.join(data_dir, "FormD_Parsed/confirmed_observer_company_links.csv")
if os.path.exists(formd_file):
    fd = pd.read_csv(formd_file)
    fd["observer_personid"] = fd["observer_personid"].astype(str).str.replace(".0", "", regex=False)
    fd["companyid"] = fd["companyid"].astype(str).str.replace(".0", "", regex=False)
    formd_confirmed = set(zip(fd["observer_personid"], fd["companyid"]))
    print(f"  Form D confirmed pairs: {len(formd_confirmed):,}")

# === Build event-trade windows ===
print("\n--- Building event-trade windows ---")

windows = [("[-180,-91]", -180, -91), ("[-90,-31]", -90, -31),
           ("[-30,-1]", -30, -1), ("[0,+30]", 0, 30), ("[+31,+90]", 31, 90)]

rows_full = []
rows_active = []
rows_confirmed = []

n_pairs_full = 0
n_pairs_active = 0
n_pairs_confirmed = 0

for ciq_pid in set(trades["ciq_personid"]):
    observed_cos = obs_to_cos.get(ciq_pid, set())
    if not observed_cos:
        continue
    person_trades = trades[trades["ciq_personid"] == ciq_pid]
    person_events = mat_events[mat_events["companyid"].isin(observed_cos)]

    for _, evt in person_events.iterrows():
        edate = evt["announcedate"]
        ecid = evt["companyid"]

        # Check if active at event date
        obs_start, obs_end = date_lookup.get((ciq_pid, ecid), (pd.Timestamp("2005-01-01"), pd.Timestamp("2026-12-31")))
        is_active = obs_start <= edate <= obs_end
        is_formd = (ciq_pid, ecid) in formd_confirmed

        for wname, ws, we in windows:
            d_start = edate + pd.Timedelta(days=ws)
            d_end = edate + pd.Timedelta(days=we)
            w_trades = person_trades[(person_trades["trandate"] >= d_start) &
                                     (person_trades["trandate"] <= d_end)]

            row = {
                "event_id": f"{edate}_{ecid}_{ciq_pid}",
                "ciq_personid": ciq_pid,
                "event_companyid": ecid,
                "event_date": edate,
                "year": edate.year,
                "window": wname,
                "n_trades": len(w_trades),
                "trade_ind": 1 if len(w_trades) > 0 else 0,
                "n_buys": (w_trades["trancode"] == "P").sum(),
                "n_sells": (w_trades["trancode"] == "S").sum(),
                "post_2020": 1 if edate.year >= 2020 else 0,
                "public_company": w_trades["cname"].iloc[0] if len(w_trades) > 0 else "",
            }

            rows_full.append(row)
            n_pairs_full += 1
            if is_active:
                rows_active.append(row)
                n_pairs_active += 1
            if is_formd:
                rows_confirmed.append(row)
                n_pairs_confirmed += 1

full = pd.DataFrame(rows_full)
active = pd.DataFrame(rows_active)
confirmed = pd.DataFrame(rows_confirmed)

print(f"  Full sample: {len(full):,} window-obs ({n_pairs_full//5:,} event-observer pairs)")
print(f"  Active-only: {len(active):,} window-obs ({n_pairs_active//5:,} pairs, {n_pairs_active/max(n_pairs_full,1)*100:.1f}% retained)")
print(f"  Form D confirmed: {len(confirmed):,} window-obs ({n_pairs_confirmed//5:,} pairs)")

# === Create window dummies ===
for df in [full, active, confirmed]:
    df["pre_90"] = (df["window"] == "[-90,-31]").astype(int)
    df["pre_30"] = (df["window"] == "[-30,-1]").astype(int)
    df["post_30"] = (df["window"] == "[0,+30]").astype(int)
    df["post_90"] = (df["window"] == "[+31,+90]").astype(int)
    df["pre30_x_post2020"] = df["pre_30"] * df["post_2020"]

# === Regressions ===
print(f"\n\n{'=' * 90}")
print("RESULTS")
print(f"{'=' * 90}")

def run_specs(df, dv, label, xvars=["pre_90", "pre_30", "post_30", "post_90"]):
    y = df[dv]
    X = df[xvars].copy()
    X = sm.add_constant(X)
    idx = y.index

    specs = [
        ("HC1", "HC1", {}),
        ("Event-cl", "cluster", {"groups": df.loc[idx, "event_id"]}),
        ("Person-cl", "cluster", {"groups": df.loc[idx, "ciq_personid"]}),
    ]

    # Year FE
    yr = pd.get_dummies(df.loc[idx, "year"], prefix="yr", drop_first=True).astype(float)
    X_yr = pd.concat([X, yr], axis=1)
    specs.append(("YrFE+Evt-cl", "cluster", {"groups": df.loc[idx, "event_id"]}))

    # Person FE (within)
    person_mean = df.loc[idx].groupby("ciq_personid")[dv].transform("mean")
    y_pfe = y - person_mean

    # Quarter FE
    df["yq"] = df["year"].astype(str) + "Q" + ((df["event_date"].dt.month - 1) // 3 + 1).astype(str)
    qr = pd.get_dummies(df.loc[idx, "yq"], prefix="yq", drop_first=True).astype(float)
    X_qr = pd.concat([df.loc[idx, xvars].copy(), qr], axis=1)
    X_qr = sm.add_constant(X_qr)

    print(f"\n  {label} (N={len(y):,})")

    all_specs = [
        ("(1) HC1", y, X, "HC1", {}),
        ("(2) Event-cl", y, X, "cluster", {"groups": df.loc[idx, "event_id"]}),
        ("(3) Person-cl", y, X, "cluster", {"groups": df.loc[idx, "ciq_personid"]}),
        ("(4) YrFE+Evt-cl", y, X_yr, "cluster", {"groups": df.loc[idx, "event_id"]}),
        ("(5) PersonFE+HC1", y_pfe, X, "HC1", {}),
        ("(6) QtrFE+Evt-cl", y, X_qr, "cluster", {"groups": df.loc[idx, "event_id"]}),
    ]

    for sname, dep, xmat, cov, kwds in all_specs:
        try:
            m = sm.OLS(dep, xmat).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            b30 = m.params.get("pre_30", np.nan)
            p30 = m.pvalues.get("pre_30", np.nan)
            s30 = "***" if p30 < 0.01 else "**" if p30 < 0.05 else "*" if p30 < 0.10 else ""
            # DiD if present
            bdi = m.params.get("pre30_x_post2020", np.nan)
            pdi = m.pvalues.get("pre30_x_post2020", np.nan)
            if pd.notna(bdi):
                sdi = "***" if pdi < 0.01 else "**" if pdi < 0.05 else "*" if pdi < 0.10 else ""
                print(f"    {sname:<20} pre_30={b30:>8.4f}{s30:<3} p={p30:.3f}  DiD={bdi:>8.4f}{sdi:<3} p={pdi:.3f}")
            else:
                print(f"    {sname:<20} pre_30={b30:>8.4f}{s30:<3} p={p30:.3f}")
        except Exception as e:
            print(f"    {sname:<20} Error: {str(e)[:50]}")

# --- Test A: Pre-event trading spike ---
print(f"\n{'─' * 90}")
print("  TEST A: Pre-event trading spike (trade_ind ~ window dummies)")
print(f"{'─' * 90}")

run_specs(full, "trade_ind", "FULL SAMPLE")
run_specs(active, "trade_ind", "ACTIVE-ONLY (position dates)")
if len(confirmed) > 100:
    run_specs(confirmed, "trade_ind", "FORM D CONFIRMED")
else:
    print(f"\n  FORM D CONFIRMED: too few obs ({len(confirmed)})")

# --- Test B: NVCA DiD ---
print(f"\n{'─' * 90}")
print("  TEST B: NVCA 2020 DiD (pre_30 x post_2020)")
print(f"{'─' * 90}")

for sample, label in [(full, "FULL"), (active, "ACTIVE-ONLY")]:
    run_specs(sample, "trade_ind", label,
              xvars=["pre_90", "pre_30", "post_30", "post_90", "post_2020", "pre30_x_post2020"])

# --- Test C: Sells specifically ---
print(f"\n{'─' * 90}")
print("  TEST C: Sell indicator")
print(f"{'─' * 90}")

full["sell_ind"] = (full["n_sells"] > 0).astype(int)
active["sell_ind"] = (active["n_sells"] > 0).astype(int)
run_specs(full, "sell_ind", "FULL - SELLS")
run_specs(active, "sell_ind", "ACTIVE - SELLS")

print("\n\nDone.")
