"""Prepare all analysis-ready datasets for R and Stata.
Computes CARs, assigns flags, saves as CSVs.
Both original CIQ network and supplemented network.

Output datasets:
  1. control_group_{network}_{eventtype}.csv — event × stock level CARs
  2. connected_shocks_{network}.csv — connected CARs for NVCA/Clayton tests
  3. form4_trades.csv — insider trading analysis dataset
  4. volume_{network}_{eventtype}.csv — abnormal volume data
  5. formd_control_group_{network}.csv — Form D events
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv, time

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")
out_dir = os.path.join(data_dir, "Analysis_Ready")
os.makedirs(out_dir, exist_ok=True)

print("=" * 100)
print("PREPARE ANALYSIS-READY DATASETS")
print("=" * 100)

# =====================================================================
# COMMON DATA LOADING
# =====================================================================
print("\n--- Loading common data ---")

# Industry
industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
industry["sic3"] = industry["sic"].astype(str).str[:3]
industry = industry.drop_duplicates("cik_int", keep="first")
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
cik_to_sic3 = dict(zip(industry["cik_int"], industry["sic3"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

# PERMNO crosswalk
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
pxw["permno"] = pd.to_numeric(pxw["permno"], errors="coerce")
pxw = pxw.drop_duplicates("cik_int", keep="first")
cik_to_permno = dict(zip(pxw["cik_int"], pxw["permno"]))
pmcik = dict(zip(pxw["permno"].dropna().astype(int), pxw["cik_int"].dropna().astype(int)))
pm_sic2 = {int(p): cik_to_sic2.get(c, "") for p, c in pmcik.items() if pd.notna(p) and pd.notna(c)}
pm_sic3 = {int(p): cik_to_sic3.get(c, "") for p, c in pmcik.items() if pd.notna(p) and pd.notna(c)}

# Two networks
def load_network(filepath):
    edges = pd.read_csv(filepath)
    if "observer_personid" in edges.columns:
        edges["observer_personid"] = edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
    edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
    edges["portfolio_cik"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
    if "permno" not in edges.columns:
        edges["permno"] = edges["portfolio_cik"].map(cik_to_permno)
    else:
        edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")
    edges = edges.dropna(subset=["permno"])
    edges["permno"] = edges["permno"].astype(int)
    edges["same_industry"] = (
        edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
        edges["portfolio_cik"].map(cik_to_sic2)
    ).astype(int)
    connected = set()
    for _, row in edges.iterrows():
        connected.add((row["observed_companyid"], row["permno"]))
    return edges, connected

print("  Loading original network...")
orig_edges, orig_connected = load_network(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
print(f"  Original: {len(orig_edges):,} edges, {len(orig_connected):,} connected pairs")

print("  Loading supplemented network...")
supp_edges, supp_connected = load_network(os.path.join(panel_c_dir, "02b_supplemented_network_edges.csv"))
print(f"  Supplemented: {len(supp_edges):,} edges, {len(supp_connected):,} connected pairs")

# Events (filtered)
print("  Loading events...")
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events["event_year"] = events["announcedate"].dt.year
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2025)]

pub_cids = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub_cids.add(cid)
events = events[~events["companyid"].isin(pub_cids)]
noise_types = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
               "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)",
               "Annual General Meeting", "Special/Extraordinary Shareholders Meeting",
               "Shareholder/Analyst Calls", "Special Calls", "Ex-Div Date (Regular)", "Ex-Div Date (Special)"]
events = events[~events["eventtype"].isin(noise_types)]

# CRSP filter
panel_b_xwalk = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(panel_b_xwalk["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
listing = panel_b_xwalk.groupby("cik_int").agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max")).reset_index()
events["cik_int"] = events["companyid"].map(cid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (events["announcedate"] >= events["first_listed"]) & (events["announcedate"] <= events["last_listed"])
events = events[~events["was_public"].fillna(False)]
print(f"  Events: {len(events):,}")

# Returns + market adjustment
print("  Loading returns...")
pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily["vol"] = pd.to_numeric(pd_daily["vol"], errors="coerce")
pd_daily["prc"] = pd.to_numeric(pd_daily["prc"], errors="coerce").abs()
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
mkt_ret = pd_daily.groupby("date")["ret"].mean().rename("mkt_ret")
pd_daily = pd_daily.merge(mkt_ret, on="date", how="left")
pd_daily["aret"] = pd_daily["ret"] - pd_daily["mkt_ret"]
all_permnos = sorted(pd_daily["permno"].unique())

avg_price = pd_daily.groupby("permno")["prc"].mean()
penny_stocks = set(avg_price[avg_price < 5].index)

pmdata = {}
for p, g in pd_daily.groupby("permno"):
    pmdata[p] = (g["date"].values, g["ret"].values, g["aret"].values, g["vol"].values)
print(f"  Stocks: {len(pmdata):,}, Penny stocks: {len(penny_stocks):,}")

# CAR windows
car_windows = [
    ("car_30", -30, -1), ("car_20", -20, -1), ("car_15", -15, -1),
    ("car_10", -10, -1), ("car_5", -5, -1), ("car_3", -3, -1),
    ("car_2", -2, -1), ("car_1", -1, 0),
    ("car_post3", 0, 3), ("car_post5", 0, 5),
]

def calc_all(permno, event_np):
    """Compute raw CARs, market-adj CARs, BHARs, and abnormal volume."""
    if permno not in pmdata:
        return None
    dates, rets, arets, vols = pmdata[permno]
    if len(dates) < 60:
        return None
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)

    result = {}
    for wn, d0, d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        war = arets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            result[wn] = float(np.sum(wr))                    # raw CAR
            result[f"{wn}_adj"] = float(np.sum(war))           # market-adjusted CAR
            result[f"{wn}_bhar"] = float(np.prod(1 + war) - 1) # BHAR

    # Abnormal volume
    baseline_mask = (diffs >= -120) & (diffs <= -31)
    bvol = vols[baseline_mask]
    if len(bvol) >= 30 and np.nanmean(bvol) > 0:
        bmean = np.nanmean(bvol)
        for wn, d0, d1 in [("avol_30", -30, -1), ("avol_10", -10, -1), ("avol_5", -5, -1)]:
            mask = (diffs >= d0) & (diffs <= d1)
            wv = vols[mask]
            if len(wv) >= max(2, abs(d1 - d0) * 0.3):
                emean = np.nanmean(wv)
                if not np.isnan(emean) and bmean > 0:
                    result[wn] = float(emean / bmean)

    return result if result else None


# Event groups
bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t)]
event_groups = {
    "ma_buyer": lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")],
    "ma_target": lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Target")],
    "bankruptcy": lambda df: df[df["eventtype"].isin(bankruptcy_types)],
    "exec_board": lambda df: df[df["eventtype"] == "Executive/Board Changes - Other"],
    "ceo_cfo": lambda df: df[df["eventtype"].isin(["Executive Changes - CEO", "Executive Changes - CFO"])],
    "all_events": lambda df: df,
}

jan2025 = pd.Timestamp("2025-01-01")

# =====================================================================
# BUILD DATASETS
# =====================================================================

for net_name, connected_set, obs_edges in [("original", orig_connected, orig_edges),
                                             ("supplemented", supp_connected, supp_edges)]:
    obs_with_edges = set(obs_edges["observed_companyid"])

    for grp_name, grp_fn in event_groups.items():
        grp = grp_fn(events)
        grp = grp[grp["companyid"].isin(obs_with_edges)]
        grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(
            subset=["companyid", "announcedate"])

        if len(grp_df) < 20:
            print(f"\n  {net_name}/{grp_name}: {len(grp_df)} events — SKIP")
            continue

        print(f"\n  {net_name}/{grp_name}: {len(grp_df):,} events")
        t0 = time.time()

        all_obs = []
        event_id = 0
        evl = grp_df.to_dict("records")

        for ei, ev in enumerate(evl):
            enp = np.datetime64(ev["announcedate"])
            ecid = ev["companyid"]
            eyr = ev["event_year"]
            edate = ev["announcedate"]
            obs_cik = cid_to_cik.get(ecid)
            obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""
            obs_sic3 = cik_to_sic3.get(obs_cik, "") if obs_cik else ""

            for pmi, pm in enumerate(all_permnos):
                is_conn = 1 if (ecid, pm) in connected_set else 0
                if not is_conn and pmi % 10 != 0:
                    continue

                result = calc_all(pm, enp)
                if not result:
                    continue

                psic2 = pm_sic2.get(pm, "")
                psic3 = pm_sic3.get(pm, "")
                si2 = 1 if (obs_sic2 and psic2 and obs_sic2 == psic2) else 0
                si3 = 1 if (obs_sic3 and psic3 and obs_sic3 == psic3) else 0

                all_obs.append({
                    "event_id": event_id,
                    "permno": pm,
                    "event_year": eyr,
                    "event_date": str(edate.date()) if hasattr(edate, "date") else str(edate),
                    "connected": is_conn,
                    "same_ind_sic2": si2,
                    "same_ind_sic3": si3,
                    "is_penny": 1 if pm in penny_stocks else 0,
                    "post_2020": 1 if eyr >= 2020 else 0,
                    "post_jan2025": 1 if pd.Timestamp(edate) >= jan2025 else 0,
                    **result,
                })
            event_id += 1

            if (ei + 1) % 500 == 0:
                elapsed = time.time() - t0
                remaining = (len(evl) - ei - 1) / (ei + 1) * elapsed / 60
                print(f"    Event {ei+1:,}/{len(evl):,} | {len(all_obs):,} obs | ~{remaining:.0f}min")

        if not all_obs:
            continue

        df = pd.DataFrame(all_obs)
        df["conn_x_same2"] = df["connected"] * df["same_ind_sic2"]
        df["conn_x_same3"] = df["connected"] * df["same_ind_sic3"]

        outpath = os.path.join(out_dir, f"control_group_{net_name}_{grp_name}.csv")
        df.to_csv(outpath, index=False)
        n_conn = df["connected"].sum()
        n_cx = df["conn_x_same2"].sum()
        print(f"    Saved: {outpath}")
        print(f"    N={len(df):,}, conn={n_conn:,}, conn_x_same={n_cx:,}")

        # Also save connected-only for shock tests
        conn_only = df[df["connected"] == 1].copy()
        conn_path = os.path.join(out_dir, f"connected_{net_name}_{grp_name}.csv")
        conn_only.to_csv(conn_path, index=False)
        print(f"    Connected-only: {len(conn_only):,} -> {conn_path}")


# =====================================================================
# FORM 4 DATASET
# =====================================================================
print(f"\n\n--- Form 4 Dataset ---")

trades = pd.read_csv(os.path.join(data_dir, "Form4", "observer_form4_trades.csv"))
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce")
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades = trades.dropna(subset=["trandate"])
trades = trades[trades["trancode"].isin(["P", "S"])].copy()
trades["is_buy"] = (trades["trancode"] == "P").astype(int)

tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
tr_to_ciq = dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
trades = trades.dropna(subset=["ciq_personid"])

# CUSIP -> SIC
import psycopg2
conn_db = psycopg2.connect(host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
                            user="harperjung", password="Wwjksnm9087yu!")
cur = conn_db.cursor()
time.sleep(3)
cur.execute("SELECT DISTINCT ncusip, siccd FROM crsp.stocknames WHERE ncusip IS NOT NULL AND siccd IS NOT NULL AND siccd > 0")
cusip_to_sic = {r[0]: str(int(r[1]))[:2] for r in cur.fetchall()}
conn_db.close()

trades["cusip8"] = trades["cusip6"].astype(str).str.strip() + trades["cusip2"].astype(str).str.strip()
trades["trade_sic2"] = trades["cusip8"].map(cusip_to_sic)

# Match to events
obs_records = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_records["personid"] = obs_records["personid"].astype(str).str.replace(".0", "", regex=False)
obs_records["companyid"] = obs_records["companyid"].astype(str).str.replace(".0", "", regex=False)
observer_to_companies = obs_records.groupby("personid")["companyid"].apply(set).to_dict()

company_events = {}
for _, ev in events.iterrows():
    cid = ev["companyid"]
    if cid not in company_events:
        company_events[cid] = []
    company_events[cid].append(ev["announcedate"])

trade_rows = []
for _, tr in trades.iterrows():
    obs_pid = tr["ciq_personid"]
    trade_date = tr["trandate"]
    trade_sic2 = tr.get("trade_sic2", "")

    observed_companies = observer_to_companies.get(obs_pid, set())
    best_match = None

    for ocid in observed_companies:
        ev_dates = company_events.get(ocid, [])
        ecik = cid_to_cik.get(ocid)
        esic2 = cik_to_sic2.get(ecik, "") if ecik else ""
        for edate in ev_dates:
            days = (trade_date - edate).days
            if -30 <= days <= -1:
                si = 1 if (trade_sic2 and esic2 and trade_sic2 == esic2) else 0
                best_match = {"pre_event": 1, "same_industry": si}
                break
        if best_match:
            break

    if best_match is None:
        any_same = 0
        for ocid in observed_companies:
            ecik = cid_to_cik.get(ocid)
            esic2 = cik_to_sic2.get(ecik, "") if ecik else ""
            if trade_sic2 and esic2 and trade_sic2 == esic2:
                any_same = 1
                break
        best_match = {"pre_event": 0, "same_industry": any_same}

    trade_rows.append({
        "ciq_personid": obs_pid,
        "is_buy": tr["is_buy"],
        "pre_event": best_match["pre_event"],
        "same_industry": best_match["same_industry"],
        "trade_year": trade_date.year,
    })

f4_df = pd.DataFrame(trade_rows)
f4_df["pre_x_same"] = f4_df["pre_event"] * f4_df["same_industry"]
f4_path = os.path.join(out_dir, "form4_trades.csv")
f4_df.to_csv(f4_path, index=False)
print(f"  Form 4: {len(f4_df):,} trades -> {f4_path}")
print(f"  Pre-event: {f4_df['pre_event'].sum():,}, Same-ind: {f4_df['same_industry'].sum():,}, Pre x Same: {f4_df['pre_x_same'].sum():,}")


print(f"\n\n{'='*100}")
print("ALL DATASETS SAVED")
print(f"{'='*100}")

# List all output files
for f in sorted(os.listdir(out_dir)):
    fpath = os.path.join(out_dir, f)
    size_mb = os.path.getsize(fpath) / 1024 / 1024
    print(f"  {f:<60} {size_mb:>8.1f} MB")

print("\nDone.")
