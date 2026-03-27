"""Test 3: Form 4 trades by same vs diff industry — V2.
Uses CUSIP matching to CRSP stocknames for SIC codes.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import psycopg2, time
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 110)
print("TEST 3: FORM 4 TRADES BY INDUSTRY — V2 (CUSIP-to-CRSP SIC)")
print("=" * 110)

# =====================================================================
# STEP 1: Build CUSIP -> SIC2 mapping from CRSP
# =====================================================================
print("\n--- Step 1: Pull CUSIP -> SIC from CRSP ---")

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
    user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()

time.sleep(3)
cur.execute("""
    SELECT DISTINCT ncusip, siccd
    FROM crsp.stocknames
    WHERE ncusip IS NOT NULL AND siccd IS NOT NULL AND siccd > 0
""")
crsp_cusip_sic = cur.fetchall()
conn.close()

cusip_to_sic = {}
for ncusip, sic in crsp_cusip_sic:
    cusip_to_sic[ncusip] = str(int(sic))[:2]  # SIC2
print(f"  CRSP CUSIP->SIC mappings: {len(cusip_to_sic):,}")

# =====================================================================
# STEP 2: Load Form 4 trades and assign SIC2
# =====================================================================
print("\n--- Step 2: Match trades to SIC ---")

trades = pd.read_csv(os.path.join(data_dir, "Form4", "observer_form4_trades.csv"))
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce")
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades = trades.dropna(subset=["trandate"])

# Build CUSIP8 from cusip6 + cusip2
trades["cusip8"] = trades["cusip6"].astype(str).str.strip() + trades["cusip2"].astype(str).str.strip()
trades["trade_sic2"] = trades["cusip8"].map(cusip_to_sic)

n_matched = trades["trade_sic2"].notna().sum()
print(f"  Trades: {len(trades):,}")
print(f"  With SIC2 via CUSIP: {n_matched:,} ({n_matched/len(trades)*100:.1f}%)")
print(f"  Without SIC2: {len(trades) - n_matched:,}")

# CIQ-TR crosswalk
xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
xwalk["tr_personid"] = xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
xwalk["ciq_personid"] = xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
tr_to_ciq = dict(zip(xwalk["tr_personid"], xwalk["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
trades = trades.dropna(subset=["ciq_personid"])

# =====================================================================
# STEP 3: Load network, events, observer records (same as before)
# =====================================================================
print("\n--- Step 3: Load events and network ---")

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

obs_records = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_records["personid"] = obs_records["personid"].astype(str).str.replace(".0", "", regex=False)
obs_records["companyid"] = obs_records["companyid"].astype(str).str.replace(".0", "", regex=False)
company_to_observers = obs_records.groupby("companyid")["personid"].apply(set).to_dict()

observer_trades = {}
for ciq_pid, grp in trades.groupby("ciq_personid"):
    observer_trades[ciq_pid] = grp

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


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


# =====================================================================
# STEP 4: Run event study by industry
# =====================================================================
print(f"\n--- Step 4: Event study ---")
print(f"  Observers with Form 4 data: {len(observer_trades):,}")

bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t)]

event_groups = [
    ("All Events", lambda df: df),
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("M&A Target", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Target")]),
    ("Bankruptcy", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
    ("Exec/Board", lambda df: df[df["eventtype"] == "Executive/Board Changes - Other"]),
]

for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(subset=["companyid", "announcedate"])

    print(f"\n\n{'='*110}")
    print(f"{group_name} ({len(grp_df):,} events)")
    print(f"{'='*110}")

    results = {"pre": {"same": [], "diff": [], "unk": []},
               "base": {"same": [], "diff": [], "unk": []},
               "post": {"same": [], "diff": [], "unk": []}}
    n_pairs = 0

    for _, ev in grp_df.iterrows():
        ecid = ev["companyid"]
        edate = ev["announcedate"]
        ecik = cid_to_cik.get(ecid)
        esic2 = cik_to_sic2.get(ecik, "") if ecik else ""

        observers = company_to_observers.get(ecid, set())
        for obs_pid in observers:
            if obs_pid not in observer_trades:
                continue
            n_pairs += 1
            obs_tr = observer_trades[obs_pid]
            days_diff = (obs_tr["trandate"] - edate).dt.days

            for window_name, mask in [("pre", (days_diff >= -30) & (days_diff <= -1)),
                                       ("base", (days_diff >= -120) & (days_diff <= -31)),
                                       ("post", (days_diff >= 0) & (days_diff <= 10))]:
                window_trades = obs_tr[mask]
                for _, tr in window_trades.iterrows():
                    tsic2 = tr.get("trade_sic2", "")
                    if pd.isna(tsic2) or tsic2 == "" or esic2 == "":
                        results[window_name]["unk"].append(tr.to_dict())
                    elif tsic2 == esic2:
                        results[window_name]["same"].append(tr.to_dict())
                    else:
                        results[window_name]["diff"].append(tr.to_dict())

    print(f"  Event-observer pairs: {n_pairs:,}")

    for window_name, window_label in [("pre", "Pre-event [-30,-1]"),
                                        ("base", "Baseline [-120,-31]"),
                                        ("post", "Post-event [0,+10]")]:
        ns = len(results[window_name]["same"])
        nd = len(results[window_name]["diff"])
        nu = len(results[window_name]["unk"])
        total = ns + nd + nu
        print(f"\n  {window_label}:")
        print(f"    Same-industry:  {ns:>6,}")
        print(f"    Diff-industry:  {nd:>6,}")
        print(f"    Unknown:        {nu:>6,}")
        if ns + nd > 0:
            print(f"    Same-ind share (of classified): {ns/(ns+nd)*100:.1f}%")

    # Trading rates
    if n_pairs > 0:
        print(f"\n  --- Trading Rates (per event-observer pair, normalized to 30 days) ---")
        for ind_label, ind_key in [("Same-industry", "same"), ("Diff-industry", "diff"), ("All classified", None)]:
            if ind_key:
                pre_n = len(results["pre"][ind_key])
                base_n = len(results["base"][ind_key])
            else:
                pre_n = len(results["pre"]["same"]) + len(results["pre"]["diff"])
                base_n = len(results["base"]["same"]) + len(results["base"]["diff"])

            pre_rate = pre_n / n_pairs
            base_rate = (base_n / n_pairs) * (30 / 90)
            ratio = pre_rate / base_rate if base_rate > 0 else float("inf")
            print(f"    {ind_label:<20} pre={pre_rate:.4f}  base={base_rate:.4f}  ratio={ratio:.2f}x")

    # Buy/Sell breakdown by industry
    for ind_label, ind_key in [("Same-industry", "same"), ("Diff-industry", "diff")]:
        pre_trades = results["pre"][ind_key]
        base_trades = results["base"][ind_key]

        if len(pre_trades) >= 3:
            pre_df = pd.DataFrame(pre_trades)
            buys = (pre_df["trancode"] == "P").sum()
            sells = (pre_df["trancode"] == "S").sum()
            awards = (pre_df["trancode"] == "A").sum()
            other = len(pre_df) - buys - sells - awards

            print(f"\n  --- {ind_label} Pre-Event Trades ---")
            print(f"    Total: {len(pre_df)}, Purchases: {buys}, Sales: {sells}, Awards: {awards}, Other: {other}")
            if buys + sells > 0:
                print(f"    Buy share: {buys/(buys+sells)*100:.1f}%")

            if len(base_trades) >= 3:
                base_df = pd.DataFrame(base_trades)
                b_buys = (base_df["trancode"] == "P").sum()
                b_sells = (base_df["trancode"] == "S").sum()
                print(f"    Baseline: {len(base_df)} total, Purchases: {b_buys}, Sales: {b_sells}")
                if b_buys + b_sells > 0:
                    print(f"    Baseline buy share: {b_buys/(b_buys+b_sells)*100:.1f}%")

                if buys + sells >= 3 and b_buys + b_sells >= 3:
                    from scipy.stats import fisher_exact
                    table = [[buys, sells], [b_buys, b_sells]]
                    or_val, p_val = fisher_exact(table)
                    print(f"    Fisher exact: OR={or_val:.3f}, p={p_val:.4f}{sig(p_val)}")

    # Sample trades
    for ind_label, ind_key in [("Same-industry", "same"), ("Diff-industry", "diff")]:
        pre_trades = results["pre"][ind_key]
        if pre_trades:
            print(f"\n  --- Sample {ind_label} Pre-Event Trades ---")
            for tr in pre_trades[:5]:
                days = (pd.Timestamp(tr["trandate"]) - pd.Timestamp(tr.get("event_date", tr["trandate"]))).days if "event_date" in tr else "?"
                print(f"    {tr['trancode']} {tr.get('shares','?')} shares of {str(tr.get('cname','?'))[:30]} | SIC2={tr.get('trade_sic2','?')}")


print("\n\nDone.")
