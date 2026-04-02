"""
Pooled all-events regression with full specification battery.
Always-On vs Active-Only, with multiple FE and clustering combinations.

Specs:
  (1) HC1 robust SE
  (2) Event-clustered SE
  (3) Stock-clustered SE
  (4) Year FE + Event-clustered SE
  (5) Year FE + Stock-clustered SE
  (6) Stock FE + Event-clustered SE (within-transformation)
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, numpy as np, os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c = os.path.join(data_dir, "Panel_C_Network")

print("=" * 100)
print("POOLED ALL-EVENTS: Full Specification Battery")
print("=" * 100)

# --- Load all data (same as before) ---
print("\n--- Loading data ---")
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

# BoardEx dates
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

# CIQ current
ciq_pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
ciq_pos["personid"] = ciq_pos["personid"].astype(str).str.replace(".0", "", regex=False)
pub_cik_lk = pd.read_csv(os.path.join(panel_c, "01_public_portfolio_companies.csv"))
pub_cik_lk["companyid"] = pub_cik_lk["companyid"].astype(str).str.replace(".0", "", regex=False)
pub_cik_lk["cik"] = pd.to_numeric(pub_cik_lk["cik"], errors="coerce")
pubcid_to_cik = dict(zip(pub_cik_lk["companyid"], pub_cik_lk["cik"]))
ciq_current = {}
for _, r in ciq_pos[ciq_pos["companytypename"] == "Public Company"].iterrows():
    pid = r["personid"]
    cik = pubcid_to_cik.get(str(r["companyid"]).replace(".0", ""))
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
    return cur is True

# Returns
ret = pd.read_csv(os.path.join(panel_c, "06_portfolio_crsp_daily.csv"))
try:
    ret25 = pd.read_csv(os.path.join(panel_c, "06b_portfolio_crsp_daily_2025.csv"))
    ret = pd.concat([ret, ret25], ignore_index=True)
except: pass
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
    if not series: return np.nan
    idx = date_to_idx.get(edate)
    if idx is None:
        for d in trading_dates:
            if d >= edate:
                idx = date_to_idx[d]; break
    if idx is None: return np.nan
    car, count = 0.0, 0
    for off in range(ws, we + 1):
        di = idx + off
        if 0 <= di < len(trading_dates):
            td = trading_dates[di]
            r = series.get(td)
            m = mkt_ret.get(td, 0)
            if r is not None and not np.isnan(r):
                car += (r - m); count += 1
    return car if count >= max(1, abs(we - ws) * 0.5) else np.nan

# Events
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_private = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_private)]
events = events[events["announcedate"] >= "2015-01-01"]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
edge_cos = set(edges["observed_companyid"])
events = events[events["companyid"].isin(edge_cos)]

events_sample = events.sample(min(800, len(events)), random_state=42)
print(f"  Events sampled: {len(events_sample)}")

edge_lookup = {}
for _, r in edges.iterrows():
    oc = r["observed_companyid"]
    if oc not in edge_lookup:
        edge_lookup[oc] = []
    edge_lookup[oc].append((r["observer_personid"], r["permno"], int(r["portfolio_cik"])))

# --- Build VC-level network lookups ---
print("  Building VC-level network...")
obs_rec = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_rec["personid"] = obs_rec["personid"].astype(str).str.replace(".0", "", regex=False)
obs_rec["companyid"] = obs_rec["companyid"].astype(str).str.replace(".0", "", regex=False)
obs_rec = obs_rec[obs_rec["companyid"].isin(us_private)]

# observer -> observed companies
obs_person_to_cos = {}
for _, r in obs_rec.iterrows():
    pid = r["personid"]
    if pid not in obs_person_to_cos:
        obs_person_to_cos[pid] = set()
    obs_person_to_cos[pid].add(r["companyid"])

# observer -> VC firms
vc_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}
all_pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
all_pos["personid"] = all_pos["personid"].astype(str).str.replace(".0", "", regex=False)
all_pos["companyid"] = all_pos["companyid"].astype(str).str.replace(".0", "", regex=False)
vc_pos = all_pos[all_pos["companytypename"].isin(vc_types)]
pub_pos = all_pos[all_pos["companytypename"] == "Public Company"]

obs_to_vcs = {}
for _, r in vc_pos.iterrows():
    pid = r["personid"]
    if pid in obs_person_to_cos:
        if pid not in obs_to_vcs:
            obs_to_vcs[pid] = set()
        obs_to_vcs[pid].add(r["companyid"])

# VC firm -> all people
vc_to_people = {}
for _, r in vc_pos.iterrows():
    vc = r["companyid"]
    if vc not in vc_to_people:
        vc_to_people[vc] = set()
    vc_to_people[vc].add(r["personid"])

# person -> dated public company positions (same as before, reuse bd_dates + ciq_current)
person_pub_dated = {}
for _, r in bd_pub.iterrows():
    pid = r["ciq_personid"]
    cik = r["portfolio_cik"]
    if pd.isna(pid) or pd.isna(cik):
        continue
    pm = cik_to_permno.get(int(cik))
    if pm and not pd.isna(pm):
        if pid not in person_pub_dated:
            person_pub_dated[pid] = []
        person_pub_dated[pid].append((int(pm), r["datestartrole"], r["dateendrole"]))

# CIQ fallback for people not in BoardEx
all_vc_people = set()
for ppl in vc_to_people.values():
    all_vc_people.update(ppl)

for _, r in pub_pos.iterrows():
    pid = r["personid"]
    if pid in person_pub_dated or pid not in all_vc_people:
        continue
    title = str(r.get("title", "")).lower()
    bflag = str(r.get("boardflag", "")) == "1.0" or str(r.get("currentboardflag", "")) == "1.0"
    if not bflag and "director" not in title and "chairman" not in title:
        continue
    cik = pubcid_to_cik.get(r["companyid"])
    if cik and not pd.isna(cik):
        pm = cik_to_permno.get(cik)
        if pm and not pd.isna(pm):
            if pid not in person_pub_dated:
                person_pub_dated[pid] = []
            is_cur = str(r.get("currentproflag", "")) == "1.0"
            end = pd.Timestamp("2026-12-31") if is_cur else pd.Timestamp("2020-12-31")
            person_pub_dated[pid].append((int(pm), pd.Timestamp("2010-01-01"), end))

# Pre-build: observed_cid -> observer list
obs_cos_to_persons = {}
for pid, cos in obs_person_to_cos.items():
    for oc in cos:
        if oc not in obs_cos_to_persons:
            obs_cos_to_persons[oc] = []
        obs_cos_to_persons[oc].append(pid)

def get_vc_connections_at_date(observed_cid, event_date):
    """VC-level: all public cos where ANY partner at observer's VC is active director."""
    connected_pms = set()
    for obs_pid in obs_cos_to_persons.get(observed_cid, []):
        for vc_cid in obs_to_vcs.get(obs_pid, set()):
            for vp in vc_to_people.get(vc_cid, set()):
                for pm, start, end in person_pub_dated.get(vp, []):
                    if start <= event_date <= end:
                        connected_pms.add(pm)
    return connected_pms

print(f"  VC firms: {len(vc_to_people):,}, VC people: {len(all_vc_people):,}")

windows = {"CAR[-30,-1]": (-30, -1), "CAR[-10,-1]": (-10, -1),
           "CAR[-5,-1]": (-5, -1), "CAR[-1,0]": (-1, 0),
           "CAR[0,+5]": (0, 5), "CAR[0,+10]": (0, 10)}

# --- Build datasets for both networks ---
for net_label, use_time_match, use_vc in [("ALWAYS-ON (Person)", False, False),
                                          ("ACTIVE-ONLY (Person)", True, False),
                                          ("ACTIVE-ONLY (VC-Level)", True, True)]:
    print(f"\n{'=' * 100}")
    print(f"  NETWORK: {net_label}")
    print(f"{'=' * 100}")

    rows = []
    for evt_i, (_, evt) in enumerate(events_sample.iterrows()):
        oc = evt["companyid"]
        edate = evt["announcedate"]
        event_cik = cid_to_cik.get(oc)
        event_sic2 = cik_to_sic2.get(event_cik, "") if event_cik else ""

        np.random.seed(evt_i)
        sample_mask = np.random.random(len(all_permnos)) < 0.10

        if use_vc:
            # VC-level: connected through any partner at the observer's VC
            connected_pms = get_vc_connections_at_date(oc, edate)
        else:
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
            rows.append({
                "permno": pm, "event_cid": oc, "announcedate": edate,
                "year": edate.year,
                "connected": conn, "same_industry": si,
                "conn_x_sameind": conn * si, **cars
            })
        if (evt_i + 1) % 200 == 0:
            print(f"    {evt_i + 1}/{len(events_sample)} events")

    df = pd.DataFrame(rows)
    for w in windows:
        lo, hi = df[w].quantile([0.01, 0.99])
        df[w] = df[w].clip(lo, hi)

    n_conn = int(df["connected"].sum())
    n_cxsi = int(df["conn_x_sameind"].sum())
    print(f"\n  Obs: {len(df):,}  Connected: {n_conn:,}  CxSI: {n_cxsi:,}")

    # Year dummies
    df["year"] = df["announcedate"].dt.year
    year_dummies = pd.get_dummies(df["year"], prefix="yr", drop_first=True).astype(float)

    # Stock FE: within-transformation (demean by stock)
    df_demeaned = df.copy()
    for w in windows:
        stock_means = df.groupby("permno")[w].transform("mean")
        df_demeaned[w] = df[w] - stock_means

    # --- Run 6 specifications for each window ---
    xvars = ["connected", "same_industry", "conn_x_sameind"]

    print(f"\n  {'Window':<15} {'Spec':<25} {'b(Conn)':>9} {'p':>6} {'b(CxSI)':>9} {'p':>6}  {'N':>8}")
    print(f"  {'-' * 80}")

    for wn in windows:
        specs = [
            ("(1) HC1",               df, xvars, "HC1",     None),
            ("(2) Event-cluster",     df, xvars, "cluster", "event_cid"),
            ("(3) Stock-cluster",     df, xvars, "cluster", "permno"),
            ("(4) YrFE + Event-cl",   df, xvars + list(year_dummies.columns), "cluster", "event_cid"),
            ("(5) YrFE + Stock-cl",   df, xvars + list(year_dummies.columns), "cluster", "permno"),
            ("(6) StockFE + Evt-cl",  df_demeaned, xvars, "cluster", "event_cid"),
        ]

        for spec_name, data, xv, cov_type, cluster_col in specs:
            y = data[wn].dropna()
            if cov_type == "cluster":
                X_cols = [c for c in xv if c in data.columns]
            else:
                X_cols = xv

            # For year FE specs, add year dummies
            if "yr_" in str(xv):
                X = pd.concat([data.loc[y.index, ["connected", "same_industry", "conn_x_sameind"]],
                               year_dummies.loc[y.index]], axis=1)
            else:
                X = data.loc[y.index, ["connected", "same_industry", "conn_x_sameind"]]

            X = sm.add_constant(X)
            if len(y) < 50:
                continue

            try:
                if cov_type == "HC1":
                    m = sm.OLS(y, X).fit(cov_type="HC1")
                else:
                    groups = data.loc[y.index, cluster_col] if cluster_col else None
                    m = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": groups})

                bc = m.params.get("connected", np.nan) * 100
                pc = m.pvalues.get("connected", np.nan)
                bi = m.params.get("conn_x_sameind", np.nan) * 100
                pi = m.pvalues.get("conn_x_sameind", np.nan)
                sc = "***" if pc < 0.01 else "**" if pc < 0.05 else "*" if pc < 0.10 else ""
                si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
                print(f"  {wn:<15} {spec_name:<25} {bc:>8.2f}%{sc:<3} {pc:>5.3f} {bi:>8.2f}%{si:<3} {pi:>5.3f}  {len(y):>8,}")
            except Exception as e:
                print(f"  {wn:<15} {spec_name:<25} Error: {str(e)[:40]}")

        print()  # blank line between windows

print("\nDone.")
