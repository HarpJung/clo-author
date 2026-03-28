"""Show ALL coefficients for supplemented network regressions.
connected, same_industry, conn x same, intercept for each spec."""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 110)
print("SUPPLEMENTED NETWORK: FULL COEFFICIENTS")
print("=" * 110)

# Load (abbreviated — same pipeline)
edges = pd.read_csv(os.path.join(panel_c_dir, "02b_supplemented_network_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
pxw["permno"] = pd.to_numeric(pxw["permno"], errors="coerce")
pxw = pxw.drop_duplicates("cik_int", keep="first")
edges["permno"] = edges["portfolio_cik"].map(dict(zip(pxw["cik_int"], pxw["permno"])))
edges = edges.dropna(subset=["permno"])
edges["permno"] = edges["permno"].astype(int)

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
industry = industry.drop_duplicates("cik_int", keep="first")
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

edges["same_industry"] = (
    edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
    edges["portfolio_cik"].map(cik_to_sic2)
).astype(int)

connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], row["permno"]))

pmcik = dict(zip(pxw["permno"].dropna().astype(int), pxw["cik_int"].dropna().astype(int)))
pm_sic2 = {int(p): cik_to_sic2.get(c, "") for p, c in pmcik.items() if pd.notna(p) and pd.notna(c)}
obs_with_edges = set(edges["observed_companyid"])

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
panel_b_xwalk = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(panel_b_xwalk["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
listing = panel_b_xwalk.groupby("cik_int").agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max")).reset_index()
events["cik_int"] = events["companyid"].map(cid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (events["announcedate"] >= events["first_listed"]) & (events["announcedate"] <= events["last_listed"])
events = events[~events["was_public"].fillna(False)]

pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
mkt_ret = pd_daily.groupby("date")["ret"].mean().rename("mkt_ret")
pd_daily = pd_daily.merge(mkt_ret, on="date", how="left")
pd_daily["aret"] = pd_daily["ret"] - pd_daily["mkt_ret"]
all_permnos = sorted(pd_daily["permno"].unique())
pmdata = {}
for p, g in pd_daily.groupby("permno"):
    pmdata[p] = (g["date"].values, g["aret"].values)

car_windows = [("car_30", -30, -1), ("car_10", -10, -1), ("car_5", -5, -1), ("car_1", -1, 0)]

def calc_cars(permno, event_np):
    if permno not in pmdata: return None
    dates, rets = pmdata[permno]
    if len(dates) < 30: return None
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    cars = {}
    for wn, d0, d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))
    return cars if cars else None

def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "

wins = [("car_30", "[-30,-1]"), ("car_10", "[-10,-1]"), ("car_5", "[-5,-1]"), ("car_1", "[-1,0]")]
bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t)]

event_groups = [
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("Bankruptcy", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
    ("Exec/Board", lambda df: df[df["eventtype"] == "Executive/Board Changes - Other"]),
]

for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp = grp[grp["companyid"].isin(obs_with_edges)]
    grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(subset=["companyid", "announcedate"])
    if len(grp_df) < 20:
        continue

    print(f"\n{'=' * 110}")
    print(f"{group_name} ({len(grp_df)} events)")
    print(f"{'=' * 110}")

    all_obs = []
    event_id = 0
    for ev in grp_df.to_dict("records"):
        enp = np.datetime64(ev["announcedate"])
        ecid = ev["companyid"]
        obs_cik = cid_to_cik.get(ecid)
        obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""
        for pmi, pm in enumerate(all_permnos):
            is_conn = 1 if (ecid, pm) in connected_set else 0
            if not is_conn and pmi % 10 != 0:
                continue
            cars = calc_cars(pm, enp)
            if not cars:
                continue
            psic = pm_sic2.get(pm, "")
            si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0
            all_obs.append({"event_id": event_id, "permno": pm, "event_year": ev["event_year"],
                            "connected": is_conn, "same_industry": si, **cars})
        event_id += 1

    df = pd.DataFrame(all_obs)
    df["cx"] = df["connected"] * df["same_industry"]
    df["eid_str"] = df["event_id"].astype(str)

    for var, label in wins:
        s = df.dropna(subset=[var]).copy().reset_index(drop=True)
        if len(s) < 200:
            continue

        for sn, fe_type, cl_type in [("HC1", None, "HC1"),
                                      ("Event-cl", None, "event"),
                                      ("YrFE+Evt-cl", "yr", "event")]:
            try:
                dm = s[[var, "connected", "same_industry", "cx"]].copy()
                if fe_type == "yr":
                    gm = dm.groupby(s["event_year"]).transform("mean")
                    dm = dm - gm
                    formula = f"{var} ~ connected + same_industry + cx - 1"
                else:
                    formula = f"{var} ~ connected + same_industry + cx"

                if cl_type == "HC1":
                    m = smf.ols(formula, data=dm).fit(cov_type="HC1")
                else:
                    m = smf.ols(formula, data=dm).fit(
                        cov_type="cluster", cov_kwds={"groups": s["eid_str"]})

                print(f"\n  {label} | {sn} | N={len(s):,}")
                for param in ["Intercept", "connected", "same_industry", "cx"]:
                    if param in m.params:
                        c = m.params[param]
                        p = m.pvalues[param]
                        print(f"    {param:<20} coef={c:>+10.5f}  p={p:.4f}{sig(p)}")
            except Exception as e:
                print(f"  {label} | {sn} | ERROR: {str(e)[:50]}")

print("\n\nDone.")
