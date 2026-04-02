"""
Pooled all-events regression comparing ALWAYS-ON vs ACTIVE-ONLY network.
Pools all event types into one regression (no event-type splits).
Runs both network definitions side by side.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, numpy as np, os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c = os.path.join(data_dir, "Panel_C_Network")

print("=" * 80)
print("POOLED ALL-EVENTS: Always-On vs Active-Only Network")
print("=" * 80)

# --- Load network edges ---
edges = pd.read_csv(os.path.join(panel_c, "02b_supplemented_network_edges_us.csv"))
edges["observer_personid"] = edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

pxw = pd.read_csv(os.path.join(panel_c, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
pxw["permno"] = pd.to_numeric(pxw["permno"], errors="coerce")
pxw = pxw.drop_duplicates("cik_int", keep="first")
cik_to_permno = dict(zip(pxw["cik_int"], pxw["permno"]))
edges["permno"] = edges["portfolio_cik"].map(cik_to_permno)
edges = edges.dropna(subset=["permno"])
edges["permno"] = edges["permno"].astype(int)

# --- Industry ---
industry = pd.read_csv(os.path.join(panel_c, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))
permno_to_cik = {int(v): int(k) for k, v in cik_to_permno.items() if pd.notna(k) and pd.notna(v)}
pm_sic2 = {pm: cik_to_sic2.get(cik, "") for pm, cik in permno_to_cik.items()}

# --- BoardEx dates ---
bd_pos = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_positions.csv"))
bd_pos["datestartrole"] = pd.to_datetime(bd_pos["datestartrole"], errors="coerce")
bd_pos["dateendrole"] = pd.to_datetime(bd_pos["dateendrole"], errors="coerce")
bd_pos["dateendrole"] = bd_pos["dateendrole"].fillna(pd.Timestamp("2026-12-31"))
bd_xwalk = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_crosswalk.csv"))
bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
bd_did_to_ciq = dict(zip(bd_xwalk["directorid"], bd_xwalk["ciq_personid"]))
bd_pos["ciq_personid"] = bd_pos["directorid"].map(bd_did_to_ciq)
bd_co = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_companies.csv"))
bd_co_cik = dict(zip(bd_co["companyid"], pd.to_numeric(bd_co["cikcode"], errors="coerce")))
bd_pos["portfolio_cik"] = bd_pos["companyid"].map(bd_co_cik)

bd_pub = bd_pos[bd_pos["orgtype"].isin(["Quoted", "Listed"])].dropna(subset=["ciq_personid", "portfolio_cik"])
bd_dates = {}
for _, r in bd_pub.iterrows():
    key = (r["ciq_personid"], int(r["portfolio_cik"]))
    if key not in bd_dates:
        bd_dates[key] = []
    bd_dates[key].append((r["datestartrole"], r["dateendrole"]))

# --- CIQ current flag ---
ciq_pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
ciq_pos["personid"] = ciq_pos["personid"].astype(str).str.replace(".0", "", regex=False)
ciq_pos["companyid"] = ciq_pos["companyid"].astype(str).str.replace(".0", "", regex=False)
pub_cik_lk = pd.read_csv(os.path.join(panel_c, "01_public_portfolio_companies.csv"))
pub_cik_lk["companyid"] = pub_cik_lk["companyid"].astype(str).str.replace(".0", "", regex=False)
pub_cik_lk["cik"] = pd.to_numeric(pub_cik_lk["cik"], errors="coerce")
pubcid_to_cik = dict(zip(pub_cik_lk["companyid"], pub_cik_lk["cik"]))

ciq_current = {}
for _, r in ciq_pos[ciq_pos["companytypename"] == "Public Company"].iterrows():
    pid = r["personid"]
    cik = pubcid_to_cik.get(r["companyid"])
    if cik and not pd.isna(cik):
        key = (pid, int(cik))
        is_cur = str(r.get("currentproflag", "")) == "1.0"
        if key not in ciq_current or is_cur:
            ciq_current[key] = is_cur

def is_active(obs_pid, port_cik, edate):
    key = (obs_pid, port_cik)
    intervals = bd_dates.get(key, [])
    if intervals:
        for s, e in intervals:
            if pd.notna(s) and pd.notna(e) and s <= edate <= e:
                return True
        return False
    cur = ciq_current.get(key)
    if cur is True:
        return True
    return False

# --- Returns ---
ret = pd.read_csv(os.path.join(panel_c, "06_portfolio_crsp_daily.csv"))
try:
    ret25 = pd.read_csv(os.path.join(panel_c, "06b_portfolio_crsp_daily_2025.csv"))
    ret = pd.concat([ret, ret25], ignore_index=True)
except:
    pass
ret["date"] = pd.to_datetime(ret["date"], errors="coerce")
ret["permno"] = pd.to_numeric(ret["permno"], errors="coerce")
ret["ret"] = pd.to_numeric(ret["ret"], errors="coerce")
ret = ret.dropna(subset=["date", "permno", "ret"])

mkt_ret = ret.groupby("date")["ret"].mean().to_dict()
trading_dates = sorted(ret["date"].unique())
date_to_idx = {d: i for i, d in enumerate(trading_dates)}
stock_returns = {}
for pm, grp in ret.groupby("permno"):
    stock_returns[int(pm)] = dict(zip(grp["date"], grp["ret"]))
all_permnos = sorted(stock_returns.keys())

def compute_car(permno, edate, ws, we):
    series = stock_returns.get(permno, {})
    if not series:
        return np.nan
    idx = date_to_idx.get(edate)
    if idx is None:
        for d in trading_dates:
            if d >= edate:
                idx = date_to_idx[d]
                break
    if idx is None:
        return np.nan
    car, count = 0.0, 0
    for off in range(ws, we + 1):
        di = idx + off
        if 0 <= di < len(trading_dates):
            td = trading_dates[di]
            r = series.get(td)
            m = mkt_ret.get(td, 0)
            if r is not None and not np.isnan(r):
                car += (r - m)
                count += 1
    return car if count >= max(1, abs(we - ws) * 0.5) else np.nan

# --- Events ---
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])

co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_private = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_private)]
events = events[events["announcedate"] >= "2015-01-01"]

noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date",
         "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]

# Only events at networked companies
edge_cos = set(edges["observed_companyid"])
events = events[events["companyid"].isin(edge_cos)]
print(f"Events at networked companies: {len(events):,}")

# Sample 800 for speed (pooling all types)
events_sample = events.sample(min(800, len(events)), random_state=42)
print(f"Sampled: {len(events_sample)} events (all types pooled)")

# Edge lookup
edge_lookup = {}
for _, r in edges.iterrows():
    oc = r["observed_companyid"]
    if oc not in edge_lookup:
        edge_lookup[oc] = []
    edge_lookup[oc].append((r["observer_personid"], r["permno"], int(r["portfolio_cik"])))

windows = {"CAR[-30,-1]": (-30, -1), "CAR[-10,-1]": (-10, -1),
           "CAR[-5,-1]": (-5, -1), "CAR[-1,0]": (-1, 0),
           "CAR[0,+5]": (0, 5), "CAR[0,+10]": (0, 10), "CAR[0,+30]": (0, 30)}

# --- Run for both network definitions ---
for net_label, use_time_match in [("ALWAYS-ON", False), ("ACTIVE-ONLY", True)]:
    print(f"\n{'=' * 80}")
    print(f"  NETWORK: {net_label} (all events pooled)")
    print(f"{'=' * 80}")

    rows = []
    for evt_i, (_, evt) in enumerate(events_sample.iterrows()):
        oc = evt["companyid"]
        edate = evt["announcedate"]
        event_cik = cid_to_cik.get(oc)
        event_sic2 = cik_to_sic2.get(event_cik, "") if event_cik else ""

        np.random.seed(evt_i)
        sample_mask = np.random.random(len(all_permnos)) < 0.10

        connected_pms = set()
        for obs_pid, permno, port_cik in edge_lookup.get(oc, []):
            if use_time_match:
                if is_active(obs_pid, port_cik, edate):
                    connected_pms.add(permno)
            else:
                connected_pms.add(permno)

        for i, pm in enumerate(all_permnos):
            conn = 1 if pm in connected_pms else 0
            if conn == 0 and not sample_mask[i]:
                continue
            si = 1 if (pm_sic2.get(pm, "") and event_sic2 and pm_sic2[pm] == event_sic2) else 0
            cars = {}
            for wn, (ws, we) in windows.items():
                cars[wn] = compute_car(pm, edate, ws, we)
            rows.append({"permno": pm, "event_cid": oc, "announcedate": edate,
                         "connected": conn, "same_industry": si,
                         "conn_x_sameind": conn * si, **cars})

        if (evt_i + 1) % 100 == 0:
            print(f"    {evt_i + 1}/{len(events_sample)} events")

    df = pd.DataFrame(rows)
    for w in windows:
        lo, hi = df[w].quantile([0.01, 0.99])
        df[w] = df[w].clip(lo, hi)

    n_conn = int(df["connected"].sum())
    n_cxsi = int(df["conn_x_sameind"].sum())
    print(f"\n  Total obs: {len(df):,}")
    print(f"  Connected: {n_conn:,}")
    print(f"  Conn x SameInd: {n_cxsi:,}")

    # --- Overall (all events) ---
    print(f"\n  --- ALL EVENTS POOLED ---")
    print(f"  {'Window':<15} {'b(Conn)':>10} {'p':>7}  {'b(SI)':>10} {'p':>7}  {'b(CxSI)':>10} {'p':>7}")
    print(f"  {'-' * 70}")
    for wn in windows:
        y = df[wn].dropna()
        X = df.loc[y.index, ["connected", "same_industry", "conn_x_sameind"]]
        X = sm.add_constant(X)
        if len(y) < 50:
            continue
        try:
            m = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": df.loc[y.index, "event_cid"]})
            bc = m.params.get("connected", np.nan) * 100
            pc = m.pvalues.get("connected", np.nan)
            bs = m.params.get("same_industry", np.nan) * 100
            ps = m.pvalues.get("same_industry", np.nan)
            bi = m.params.get("conn_x_sameind", np.nan) * 100
            pi = m.pvalues.get("conn_x_sameind", np.nan)
            sc = "***" if pc < 0.01 else "**" if pc < 0.05 else "*" if pc < 0.10 else ""
            si_star = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            print(f"  {wn:<15} {bc:>9.2f}%{sc:<3} {pc:>6.3f}  {bs:>9.2f}% {ps:>6.3f}  {bi:>9.2f}%{si_star:<3} {pi:>6.3f}")
        except Exception as e:
            print(f"  {wn:<15} Error: {e}")

    # --- Also show: overall, same-industry, diff-industry subsample means ---
    print(f"\n  --- CONNECTED SUBSAMPLE MEANS ---")
    conn_df = df[df["connected"] == 1]
    si_df = conn_df[conn_df["same_industry"] == 1]
    di_df = conn_df[conn_df["same_industry"] == 0]
    print(f"  {'Window':<15} {'Overall':>10} {'N':>6}  {'Same-Ind':>10} {'N':>6}  {'Diff-Ind':>10} {'N':>6}")
    print(f"  {'-' * 70}")
    for wn in windows:
        ov = conn_df[wn].mean() * 100
        on = conn_df[wn].notna().sum()
        sm_val = si_df[wn].mean() * 100 if len(si_df) > 0 else np.nan
        sn = si_df[wn].notna().sum()
        dm = di_df[wn].mean() * 100 if len(di_df) > 0 else np.nan
        dn = di_df[wn].notna().sum()
        print(f"  {wn:<15} {ov:>9.2f}% {on:>5}  {sm_val:>9.2f}% {sn:>5}  {dm:>9.2f}% {dn:>5}")

print("\n\nDone.")
