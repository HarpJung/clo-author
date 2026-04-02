"""
Pooled all-events regression WITH regulatory shock DiD interactions.
Tests all 3 networks x 3 period specifications:

Period specs:
  (A) Full sample, no period split (baseline)
  (B) NVCA 2020 DiD: SameInd x Post2020 on connected subsample
  (C) Clayton Act 2025 DiD: SameInd x PostJan2025 on connected post-2020 subsample

Networks:
  1. Always-On (Person)
  2. Active-Only (Person)
  3. Active-Only (VC-Level)

All with event-clustered SE + Year FE.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, numpy as np, os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c = os.path.join(data_dir, "Panel_C_Network")

print("=" * 100)
print("POOLED ALL-EVENTS WITH REGULATORY SHOCK DiD")
print("=" * 100)

# === Load all data (same setup as before) ===
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
    return ciq_current.get(key) is True

# VC-level network
obs_rec = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_rec["personid"] = obs_rec["personid"].astype(str).str.replace(".0", "", regex=False)
obs_rec["companyid"] = obs_rec["companyid"].astype(str).str.replace(".0", "", regex=False)
co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_private = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_rec = obs_rec[obs_rec["companyid"].isin(us_private)]

obs_person_to_cos = {}
for _, r in obs_rec.iterrows():
    pid = r["personid"]
    if pid not in obs_person_to_cos:
        obs_person_to_cos[pid] = set()
    obs_person_to_cos[pid].add(r["companyid"])

vc_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}
all_pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
all_pos["personid"] = all_pos["personid"].astype(str).str.replace(".0", "", regex=False)
all_pos["companyid"] = all_pos["companyid"].astype(str).str.replace(".0", "", regex=False)
vc_pos_df = all_pos[all_pos["companytypename"].isin(vc_types)]
pub_pos_df = all_pos[all_pos["companytypename"] == "Public Company"]

obs_to_vcs = {}
for _, r in vc_pos_df.iterrows():
    pid = r["personid"]
    if pid in obs_person_to_cos:
        if pid not in obs_to_vcs:
            obs_to_vcs[pid] = set()
        obs_to_vcs[pid].add(r["companyid"])

vc_to_people = {}
for _, r in vc_pos_df.iterrows():
    vc = r["companyid"]
    if vc not in vc_to_people:
        vc_to_people[vc] = set()
    vc_to_people[vc].add(r["personid"])

all_vc_people = set()
for ppl in vc_to_people.values():
    all_vc_people.update(ppl)

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

for _, r in pub_pos_df.iterrows():
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

obs_cos_to_persons = {}
for pid, cos in obs_person_to_cos.items():
    for oc in cos:
        if oc not in obs_cos_to_persons:
            obs_cos_to_persons[oc] = []
        obs_cos_to_persons[oc].append(pid)

def get_vc_connections_at_date(observed_cid, event_date):
    connected_pms = set()
    for obs_pid in obs_cos_to_persons.get(observed_cid, []):
        for vc_cid in obs_to_vcs.get(obs_pid, set()):
            for vp in vc_to_people.get(vc_cid, set()):
                for pm, start, end in person_pub_dated.get(vp, []):
                    if start <= event_date <= end:
                        connected_pms.add(pm)
    return connected_pms

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
events = events[events["companyid"].isin(us_private)]
events = events[events["announcedate"] >= "2015-01-01"]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
edge_cos = set(edges["observed_companyid"])
events = events[events["companyid"].isin(edge_cos)]
events_sample = events.sample(min(1000, len(events)), random_state=42)
print(f"  Events sampled: {len(events_sample)}")

edge_lookup = {}
for _, r in edges.iterrows():
    oc = r["observed_companyid"]
    if oc not in edge_lookup:
        edge_lookup[oc] = []
    edge_lookup[oc].append((r["observer_personid"], r["permno"], int(r["portfolio_cik"])))

windows = {
    "CAR[-30,-1]": (-30, -1),
    "CAR[-20,-1]": (-20, -1),
    "CAR[-10,-1]": (-10, -1),
    "CAR[-5,-1]": (-5, -1),
    "CAR[-3,-1]": (-3, -1),
    "CAR[-1,0]": (-1, 0),
    "CAR[0,+3]": (0, 3),
    "CAR[0,+5]": (0, 5),
    "CAR[0,+10]": (0, 10),
    "CAR[0,+20]": (0, 20),
    "CAR[0,+30]": (0, 30),
}

# === Build datasets for all 3 networks ===
network_defs = [
    ("Always-On (Person)", False, False),
    ("Active-Only (Person)", True, False),
    ("Active-Only (VC-Level)", True, True),
]

for net_label, use_time_match, use_vc in network_defs:
    print(f"\n\n{'=' * 100}")
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
                "conn_x_sameind": conn * si,
                "post_2020": 1 if edate.year >= 2020 else 0,
                "post_jan2025": 1 if edate >= pd.Timestamp("2025-01-01") else 0,
                **cars
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
    year_dummies = pd.get_dummies(df["year"], prefix="yr", drop_first=True).astype(float)

    # =================================================================
    # (A) BASELINE: Full sample, Connected x SameInd (Year FE, event-cluster)
    # =================================================================
    print(f"\n  --- (A) BASELINE: Full sample ---")
    print(f"  {'Window':<15} {'b(Conn)':>9} {'p':>6}  {'b(CxSI)':>9} {'p':>6}  {'N':>8}")
    print(f"  {'-' * 60}")
    for wn in windows:
        y = df[wn].dropna()
        X = pd.concat([df.loc[y.index, ["connected", "same_industry", "conn_x_sameind"]],
                        year_dummies.loc[y.index]], axis=1)
        X = sm.add_constant(X)
        try:
            m = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": df.loc[y.index, "event_cid"]})
            bc = m.params.get("connected", np.nan) * 100
            pc = m.pvalues.get("connected", np.nan)
            bi = m.params.get("conn_x_sameind", np.nan) * 100
            pi = m.pvalues.get("conn_x_sameind", np.nan)
            sc = "***" if pc < 0.01 else "**" if pc < 0.05 else "*" if pc < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            print(f"  {wn:<15} {bc:>8.2f}%{sc:<3} {pc:>5.3f}  {bi:>8.2f}%{si:<3} {pi:>5.3f}  {len(y):>8,}")
        except Exception as e:
            print(f"  {wn:<15} Error: {str(e)[:50]}")

    # =================================================================
    # (B) NVCA 2020 DiD: Connected subsample, SameInd x Post2020
    # =================================================================
    conn_df = df[df["connected"] == 1].copy()
    conn_df["si_x_post2020"] = conn_df["same_industry"] * conn_df["post_2020"]

    if len(conn_df) > 30:
        conn_yr = pd.get_dummies(conn_df["year"], prefix="yr", drop_first=True).astype(float)

        print(f"\n  --- (B) NVCA 2020 DiD: Connected subsample (N={len(conn_df):,}) ---")
        print(f"  {'Window':<15} {'b(SI)':>9} {'p':>6}  {'b(SI x Post2020)':>17} {'p':>6}")
        print(f"  {'-' * 60}")
        for wn in windows:
            y = conn_df[wn].dropna()
            X = pd.concat([conn_df.loc[y.index, ["same_industry", "si_x_post2020"]],
                            conn_yr.loc[y.index]], axis=1)
            X = sm.add_constant(X)
            try:
                m = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": conn_df.loc[y.index, "event_cid"]})
                bs = m.params.get("same_industry", np.nan) * 100
                ps = m.pvalues.get("same_industry", np.nan)
                bi = m.params.get("si_x_post2020", np.nan) * 100
                pi = m.pvalues.get("si_x_post2020", np.nan)
                ss = "***" if ps < 0.01 else "**" if ps < 0.05 else "*" if ps < 0.10 else ""
                si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
                print(f"  {wn:<15} {bs:>8.2f}%{ss:<3} {ps:>5.3f}  {bi:>16.2f}%{si:<3} {pi:>5.3f}")
            except Exception as e:
                print(f"  {wn:<15} Error: {str(e)[:50]}")

        # Period means for connected same-industry
        conn_si = conn_df[conn_df["same_industry"] == 1]
        pre = conn_si[conn_si["post_2020"] == 0]
        post = conn_si[conn_si["post_2020"] == 1]
        print(f"\n  Connected same-industry means:")
        print(f"  {'Window':<15} {'Pre-2020':>10} {'N':>5}  {'Post-2020':>10} {'N':>5}")
        print(f"  {'-' * 50}")
        for wn in windows:
            pre_m = pre[wn].mean() * 100 if len(pre) > 0 else np.nan
            post_m = post[wn].mean() * 100 if len(post) > 0 else np.nan
            print(f"  {wn:<15} {pre_m:>9.2f}% {len(pre):>4}  {post_m:>9.2f}% {len(post):>4}")

    # =================================================================
    # (C) Clayton Act 2025 DiD: Connected post-2020 subsample
    # =================================================================
    post20 = conn_df[conn_df["post_2020"] == 1].copy()
    post20["si_x_postjan2025"] = post20["same_industry"] * post20["post_jan2025"]

    if len(post20) > 30:
        post20_yr = pd.get_dummies(post20["year"], prefix="yr", drop_first=True).astype(float)

        print(f"\n  --- (C) Clayton Act 2025 DiD: Post-2020 connected (N={len(post20):,}) ---")
        print(f"  {'Window':<15} {'b(SI)':>9} {'p':>6}  {'b(SI x PostJan25)':>18} {'p':>6}")
        print(f"  {'-' * 60}")
        for wn in windows:
            y = post20[wn].dropna()
            X = pd.concat([post20.loc[y.index, ["same_industry", "si_x_postjan2025"]],
                            post20_yr.loc[y.index]], axis=1)
            X = sm.add_constant(X)
            try:
                m = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": post20.loc[y.index, "event_cid"]})
                bs = m.params.get("same_industry", np.nan) * 100
                ps = m.pvalues.get("same_industry", np.nan)
                bi = m.params.get("si_x_postjan2025", np.nan) * 100
                pi = m.pvalues.get("si_x_postjan2025", np.nan)
                ss = "***" if ps < 0.01 else "**" if ps < 0.05 else "*" if ps < 0.10 else ""
                si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
                print(f"  {wn:<15} {bs:>8.2f}%{ss:<3} {ps:>5.3f}  {bi:>17.2f}%{si:<3} {pi:>5.3f}")
            except Exception as e:
                print(f"  {wn:<15} Error: {str(e)[:50]}")

        # Period means
        pre25 = post20[(post20["same_industry"] == 1) & (post20["post_jan2025"] == 0)]
        post25 = post20[(post20["same_industry"] == 1) & (post20["post_jan2025"] == 1)]
        print(f"\n  Connected same-industry means (post-2020 sample):")
        print(f"  {'Window':<15} {'2020-2024':>10} {'N':>5}  {'Post-Jan25':>10} {'N':>5}")
        print(f"  {'-' * 50}")
        for wn in windows:
            m1 = pre25[wn].mean() * 100 if len(pre25) > 0 else np.nan
            m2 = post25[wn].mean() * 100 if len(post25) > 0 else np.nan
            print(f"  {wn:<15} {m1:>9.2f}% {len(pre25):>4}  {m2:>9.2f}% {len(post25):>4}")

print("\n\nDone.")
