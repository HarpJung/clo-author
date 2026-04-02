"""
Re-run the DiD regressions with EXPANDED SAMPLE:
  - Extended period: 2010-2025 (was 2015-2025)
  - Added ISS director positions (84 new observers with dated positions)
  - Combined BoardEx + ISS for time-matching

Tests the NVCA 2020 DiD with more pre-period data.
Active-Only Person network with all available date sources.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, numpy as np, os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c = os.path.join(data_dir, "Panel_C_Network")

print("=" * 90)
print("EXPANDED SAMPLE: 2010-2025 + ISS Directors")
print("=" * 90)

# === Load network edges ===
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

# Industry
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

# === Build COMBINED date lookup: BoardEx + ISS + CIQ fallback ===
print("\n--- Building combined date lookup (BoardEx + ISS + CIQ) ---")

# Source 1: BoardEx
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

# Build: (ciq_personid, portfolio_cik) -> [(start, end), ...]
person_dates = {}  # Combined from all sources

for _, r in bd_pub.iterrows():
    pid = r["ciq_personid"]
    cik = int(r["portfolio_cik"])
    pm = cik_to_permno.get(cik)
    if pm and not pd.isna(pm):
        key = (pid, int(pm))
        if key not in person_dates:
            person_dates[key] = []
        person_dates[key].append((r["datestartrole"], r["dateendrole"]))

bd_count = len(person_dates)
print(f"  BoardEx: {bd_count:,} (person, stock) pairs with dates")

# Source 2: ISS
iss_pos = pd.read_csv(os.path.join(data_dir, "ISS/observer_iss_positions.csv"))
iss_xwalk = pd.read_csv(os.path.join(data_dir, "ISS/observer_iss_crosswalk.csv"))

iss_added = 0
for _, r in iss_pos.iterrows():
    pid = str(r.get("ciq_personid", "")).replace(".0", "")
    cik = r.get("cik")
    if pd.isna(cik) or not pid:
        continue
    pm = cik_to_permno.get(int(cik))
    if pm and not pd.isna(pm):
        key = (pid, int(pm))
        dirsince = r.get("dirsince")
        yte = r.get("year_term_ends")
        if pd.notna(dirsince):
            start = pd.Timestamp(f"{int(dirsince)}-01-01")
            end = pd.Timestamp(f"{int(yte)}-12-31") if pd.notna(yte) else pd.Timestamp("2026-12-31")
            if key not in person_dates:
                person_dates[key] = []
                iss_added += 1
            person_dates[key].append((start, end))

print(f"  ISS added: {iss_added:,} new (person, stock) pairs")
print(f"  Combined total: {len(person_dates):,} (person, stock) pairs with dates")

# Source 3: CIQ fallback for remaining
ciq_pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
ciq_pos["personid"] = ciq_pos["personid"].astype(str).str.replace(".0", "", regex=False)
pub_cik_lk = pd.read_csv(os.path.join(panel_c, "01_public_portfolio_companies.csv"))
pub_cik_lk["companyid"] = pub_cik_lk["companyid"].astype(str).str.replace(".0", "", regex=False)
pub_cik_lk["cik"] = pd.to_numeric(pub_cik_lk["cik"], errors="coerce")
pubcid_to_cik = dict(zip(pub_cik_lk["companyid"], pub_cik_lk["cik"]))

ciq_added = 0
for _, r in ciq_pos[ciq_pos["companytypename"] == "Public Company"].iterrows():
    pid = r["personid"]
    cik = pubcid_to_cik.get(str(r["companyid"]).replace(".0", ""))
    if cik and not pd.isna(cik):
        pm = cik_to_permno.get(int(cik))
        if pm and not pd.isna(pm):
            key = (pid, int(pm))
            if key not in person_dates:
                is_cur = str(r.get("currentproflag", "")) == "1.0"
                start = pd.Timestamp("2005-01-01")
                end = pd.Timestamp("2026-12-31") if is_cur else pd.Timestamp("2020-12-31")
                person_dates[key] = [(start, end)]
                ciq_added += 1

print(f"  CIQ fallback added: {ciq_added:,} (person, stock) pairs")
print(f"  FINAL total: {len(person_dates):,} (person, stock) pairs")

def is_active(obs_pid, permno, edate):
    key = (obs_pid, permno)
    for s, e in person_dates.get(key, []):
        if pd.notna(s) and pd.notna(e) and s <= edate <= e:
            return True
    return False

# === Load EXPANDED returns (2010-2025) ===
print("\n--- Loading returns (2010-2025) ---")
ret_parts = []
for fname in ["06c_portfolio_crsp_daily_2010_2014.csv", "06_portfolio_crsp_daily.csv",
              "06b_portfolio_crsp_daily_2025.csv"]:
    fp = os.path.join(panel_c, fname)
    if os.path.exists(fp):
        df = pd.read_csv(fp)
        ret_parts.append(df)
        print(f"  {fname}: {len(df):,} rows")
ret = pd.concat(ret_parts, ignore_index=True)
ret["date"] = pd.to_datetime(ret["date"], errors="coerce")
ret["permno"] = pd.to_numeric(ret["permno"], errors="coerce")
ret["ret"] = pd.to_numeric(ret["ret"], errors="coerce")
ret = ret.dropna(subset=["date", "permno", "ret"])
ret = ret.drop_duplicates(subset=["permno", "date"])
print(f"  Total: {len(ret):,} rows, {ret['permno'].nunique():,} stocks, {ret['date'].min().date()} to {ret['date'].max().date()}")

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

# === Load events (2010-2025) ===
print("\n--- Loading events (2010-2025) ---")
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])

co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_private = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_private)]
events = events[events["announcedate"] >= "2010-01-01"]  # EXTENDED from 2015
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
edge_cos = set(edges["observed_companyid"])
events = events[events["companyid"].isin(edge_cos)]
print(f"  Events (2010-2025, US private, filtered): {len(events):,}")
print(f"  Pre-2015: {(events['announcedate'] < '2015-01-01').sum():,}")
print(f"  2015-2019: {((events['announcedate'] >= '2015-01-01') & (events['announcedate'] < '2020-01-01')).sum():,}")
print(f"  2020-2025: {(events['announcedate'] >= '2020-01-01').sum():,}")

# Edge lookup
edge_lookup = {}
for _, r in edges.iterrows():
    oc = r["observed_companyid"]
    if oc not in edge_lookup:
        edge_lookup[oc] = []
    edge_lookup[oc].append((r["observer_personid"], r["permno"], int(r["portfolio_cik"])))

# Sample events
events_sample = events.sample(min(1500, len(events)), random_state=42)
print(f"  Sampled: {len(events_sample)}")

windows = {
    "CAR[-30,-1]": (-30, -1), "CAR[-20,-1]": (-20, -1),
    "CAR[-10,-1]": (-10, -1), "CAR[-5,-1]": (-5, -1),
    "CAR[-1,0]": (-1, 0),
    "CAR[0,+5]": (0, 5), "CAR[0,+10]": (0, 10),
}

# === Build dataset with time-matched connections ===
print("\n--- Computing CARs with time-matched connections ---")
rows = []
for evt_i, (_, evt) in enumerate(events_sample.iterrows()):
    oc = evt["companyid"]
    edate = evt["announcedate"]
    event_cik = cid_to_cik.get(oc)
    event_sic2 = cik_to_sic2.get(event_cik, "") if event_cik else ""

    np.random.seed(evt_i)
    sample_mask = np.random.random(len(all_permnos)) < 0.10

    # Time-matched connected set
    connected_pms = set()
    for obs_pid, permno, port_cik in edge_lookup.get(oc, []):
        if is_active(obs_pid, permno, edate):
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
    if (evt_i + 1) % 300 == 0:
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
print(f"  Pre-2020 connected same-ind: {int(df[(df['connected']==1)&(df['same_industry']==1)&(df['post_2020']==0)].shape[0]):,}")
print(f"  Post-2020 connected same-ind: {int(df[(df['connected']==1)&(df['same_industry']==1)&(df['post_2020']==1)].shape[0]):,}")

year_dummies = pd.get_dummies(df["year"], prefix="yr", drop_first=True).astype(float)

# === (A) BASELINE ===
print(f"\n{'=' * 90}")
print(f"  (A) BASELINE: Full sample (Year FE, event-clustered)")
print(f"{'=' * 90}")
print(f"  {'Window':<15} {'b(Conn)':>9} {'p':>6}  {'b(CxSI)':>9} {'p':>6}")
print(f"  {'-' * 50}")
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
        print(f"  {wn:<15} {bc:>8.2f}%{sc:<3} {pc:>5.3f}  {bi:>8.2f}%{si:<3} {pi:>5.3f}")
    except Exception as e:
        print(f"  {wn:<15} Error: {str(e)[:50]}")

# === (B) NVCA 2020 DiD ===
conn_df = df[df["connected"] == 1].copy()
conn_df["si_x_post2020"] = conn_df["same_industry"] * conn_df["post_2020"]
conn_yr = pd.get_dummies(conn_df["year"], prefix="yr", drop_first=True).astype(float)

print(f"\n{'=' * 90}")
print(f"  (B) NVCA 2020 DiD: Connected subsample (N={len(conn_df):,})")
print(f"{'=' * 90}")
print(f"  {'Window':<15} {'b(SI)':>9} {'p':>6}  {'b(SI x Post2020)':>17} {'p':>6}")
print(f"  {'-' * 55}")
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

# Period means
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

# === (C) Clayton Act 2025 DiD ===
post20 = conn_df[conn_df["post_2020"] == 1].copy()
post20["si_x_postjan2025"] = post20["same_industry"] * post20["post_jan2025"]
post20_yr = pd.get_dummies(post20["year"], prefix="yr", drop_first=True).astype(float)

if len(post20) > 30:
    print(f"\n{'=' * 90}")
    print(f"  (C) Clayton Act 2025 DiD: Post-2020 connected (N={len(post20):,})")
    print(f"{'=' * 90}")
    print(f"  {'Window':<15} {'b(SI)':>9} {'p':>6}  {'b(SI x PostJan25)':>18} {'p':>6}")
    print(f"  {'-' * 55}")
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

    si_post20 = post20[post20["same_industry"] == 1]
    pre25 = si_post20[si_post20["post_jan2025"] == 0]
    post25 = si_post20[si_post20["post_jan2025"] == 1]
    print(f"\n  Connected same-industry means (post-2020):")
    print(f"  {'Window':<15} {'2020-2024':>10} {'N':>5}  {'Post-Jan25':>10} {'N':>5}")
    print(f"  {'-' * 50}")
    for wn in windows:
        m1 = pre25[wn].mean() * 100 if len(pre25) > 0 else np.nan
        m2 = post25[wn].mean() * 100 if len(post25) > 0 else np.nan
        print(f"  {wn:<15} {m1:>9.2f}% {len(pre25):>4}  {m2:>9.2f}% {len(post25):>4}")

print("\n\nDone.")


# === (D) FULL SPECIFICATION BATTERY ===
print(f"\n\n{'=' * 90}")
print(f"  (D) FULL SPECIFICATION BATTERY: Baseline + NVCA DiD")
print(f"{'=' * 90}")

# Stock FE: within-transformation
df_demeaned = df.copy()
for w in windows:
    df_demeaned[w] = df[w] - df.groupby("permno")[w].transform("mean")

conn_demeaned = conn_df.copy()
for w in windows:
    conn_demeaned[w] = conn_df[w] - conn_df.groupby("permno")[w].transform("mean")

# --- Baseline: 6 specs ---
print(f"\n  BASELINE (full sample, N={len(df):,})")
print(f"  {'Window':<15} {'Spec':<25} {'b(Conn)':>9} {'p':>6} {'b(CxSI)':>9} {'p':>6}")
print(f"  {'-' * 75}")

for wn in windows:
    specs = [
        ("(1) HC1",             df, "HC1",     None),
        ("(2) Event-cluster",   df, "cluster", "event_cid"),
        ("(3) Stock-cluster",   df, "cluster", "permno"),
        ("(4) YrFE+Event-cl",   df, "cluster", "event_cid"),
        ("(5) YrFE+Stock-cl",   df, "cluster", "permno"),
        ("(6) StockFE+Evt-cl",  df_demeaned, "cluster", "event_cid"),
    ]
    for spec_name, data, cov_type, cluster_col in specs:
        y = data[wn].dropna()
        use_yr = "YrFE" in spec_name
        if use_yr:
            X = pd.concat([data.loc[y.index, ["connected", "same_industry", "conn_x_sameind"]],
                           year_dummies.loc[y.index]], axis=1)
        else:
            X = data.loc[y.index, ["connected", "same_industry", "conn_x_sameind"]]
        X = sm.add_constant(X)
        if len(y) < 50: continue
        try:
            if cov_type == "HC1":
                m = sm.OLS(y, X).fit(cov_type="HC1")
            else:
                m = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": data.loc[y.index, cluster_col]})
            bc = m.params.get("connected", np.nan) * 100
            pc = m.pvalues.get("connected", np.nan)
            bi = m.params.get("conn_x_sameind", np.nan) * 100
            pi = m.pvalues.get("conn_x_sameind", np.nan)
            sc = "***" if pc < 0.01 else "**" if pc < 0.05 else "*" if pc < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            print(f"  {wn:<15} {spec_name:<25} {bc:>8.2f}%{sc:<3} {pc:>5.3f} {bi:>8.2f}%{si:<3} {pi:>5.3f}")
        except Exception as e:
            print(f"  {wn:<15} {spec_name:<25} Error: {str(e)[:40]}")
    print()

# --- NVCA DiD: 6 specs ---
conn_yr = pd.get_dummies(conn_df["year"], prefix="yr", drop_first=True).astype(float)
conn_dm_yr = pd.get_dummies(conn_demeaned["year"], prefix="yr", drop_first=True).astype(float)

print(f"\n  NVCA 2020 DiD (connected subsample, N={len(conn_df):,})")
print(f"  {'Window':<15} {'Spec':<25} {'b(SI)':>9} {'p':>6} {'b(SIxPost20)':>13} {'p':>6}")
print(f"  {'-' * 75}")

for wn in windows:
    specs = [
        ("(1) HC1",             conn_df, "HC1",     None),
        ("(2) Event-cluster",   conn_df, "cluster", "event_cid"),
        ("(3) Stock-cluster",   conn_df, "cluster", "permno"),
        ("(4) YrFE+Event-cl",   conn_df, "cluster", "event_cid"),
        ("(5) YrFE+Stock-cl",   conn_df, "cluster", "permno"),
        ("(6) StockFE+Evt-cl",  conn_demeaned, "cluster", "event_cid"),
    ]
    for spec_name, data, cov_type, cluster_col in specs:
        y = data[wn].dropna()
        use_yr = "YrFE" in spec_name
        if use_yr:
            yr_d = conn_yr if "demeaned" not in str(id(data)) else conn_dm_yr
            X = pd.concat([data.loc[y.index, ["same_industry", "si_x_post2020"]],
                           conn_yr.loc[y.index]], axis=1)
        else:
            X = data.loc[y.index, ["same_industry", "si_x_post2020"]]
        X = sm.add_constant(X)
        if len(y) < 30: continue
        try:
            if cov_type == "HC1":
                m = sm.OLS(y, X).fit(cov_type="HC1")
            else:
                m = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": data.loc[y.index, cluster_col]})
            bs = m.params.get("same_industry", np.nan) * 100
            ps = m.pvalues.get("same_industry", np.nan)
            bi = m.params.get("si_x_post2020", np.nan) * 100
            pi = m.pvalues.get("si_x_post2020", np.nan)
            ss = "***" if ps < 0.01 else "**" if ps < 0.05 else "*" if ps < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            print(f"  {wn:<15} {spec_name:<25} {bs:>8.2f}%{ss:<3} {ps:>5.3f} {bi:>12.2f}%{si:<3} {pi:>5.3f}")
        except Exception as e:
            print(f"  {wn:<15} {spec_name:<25} Error: {str(e)[:40]}")
    print()

print("\nDone with full specs.")
