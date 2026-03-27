"""Test 3: Form 4 Regression — is_buy ~ pre_event x same_industry.
Proper regression with clustering by observer and event company.

For each Form 4 trade by an observer, classify:
- is_buy = 1 if purchase, 0 if sale
- pre_event = 1 if trade is in [-30,-1] of any event at observed company
- same_industry = 1 if trade company SIC2 matches event company SIC2
- interaction: pre_event x same_industry

Also: trade_frequency regression (does observer trade MORE in pre-event window?)
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import psycopg2, time
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 110)
print("TEST 3: FORM 4 REGRESSION")
print("=" * 110)

# =====================================================================
# STEP 1: Build CUSIP -> SIC2
# =====================================================================
print("\n--- Loading ---")
conn = psycopg2.connect(host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
                         user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()
time.sleep(3)
cur.execute("SELECT DISTINCT ncusip, siccd FROM crsp.stocknames WHERE ncusip IS NOT NULL AND siccd IS NOT NULL AND siccd > 0")
cusip_to_sic = {r[0]: str(int(r[1]))[:2] for r in cur.fetchall()}
conn.close()

# Load trades
trades = pd.read_csv(os.path.join(data_dir, "Form4", "observer_form4_trades.csv"))
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce")
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades = trades.dropna(subset=["trandate"])
trades["cusip8"] = trades["cusip6"].astype(str).str.strip() + trades["cusip2"].astype(str).str.strip()
trades["trade_sic2"] = trades["cusip8"].map(cusip_to_sic)

xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
xwalk["tr_personid"] = xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
xwalk["ciq_personid"] = xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
tr_to_ciq = dict(zip(xwalk["tr_personid"], xwalk["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
trades = trades.dropna(subset=["ciq_personid"])

# Only keep buys and sells
trades = trades[trades["trancode"].isin(["P", "S"])].copy()
trades["is_buy"] = (trades["trancode"] == "P").astype(int)
print(f"  Trades (P+S only): {len(trades):,} ({trades['is_buy'].sum():,} buys, {(~trades['is_buy'].astype(bool)).sum():,} sells)")

# Load network, events
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
observer_to_companies = obs_records.groupby("personid")["companyid"].apply(set).to_dict()

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

# =====================================================================
# STEP 2: For each trade, determine if it's in a pre-event window
# and whether it's same-industry as the event company
# =====================================================================
print("\n--- Matching trades to events ---")

# Build event lookup: for each observer's observed company, get event dates
company_events = {}
for _, ev in events.iterrows():
    cid = ev["companyid"]
    if cid not in company_events:
        company_events[cid] = []
    company_events[cid].append(ev["announcedate"])

# For each trade, check all observed companies of that observer
trade_rows = []
for _, tr in trades.iterrows():
    obs_pid = tr["ciq_personid"]
    trade_date = tr["trandate"]
    trade_sic2 = tr.get("trade_sic2", "")

    observed_companies = observer_to_companies.get(obs_pid, set())
    best_match = None
    min_days = 999

    for ocid in observed_companies:
        ev_dates = company_events.get(ocid, [])
        ecik = cid_to_cik.get(ocid)
        esic2 = cik_to_sic2.get(ecik, "") if ecik else ""

        for edate in ev_dates:
            days = (trade_date - edate).days
            if -30 <= days <= -1:
                if abs(days) < abs(min_days):
                    min_days = days
                    si = 1 if (trade_sic2 and esic2 and trade_sic2 == esic2) else 0
                    best_match = {"pre_event": 1, "same_industry": si, "event_company": ocid, "days_before": days}

    if best_match is None:
        # Not in any pre-event window — this is a baseline trade
        # Check if same industry as ANY observed company
        any_same = 0
        for ocid in observed_companies:
            ecik = cid_to_cik.get(ocid)
            esic2 = cik_to_sic2.get(ecik, "") if ecik else ""
            if trade_sic2 and esic2 and trade_sic2 == esic2:
                any_same = 1
                break
        best_match = {"pre_event": 0, "same_industry": any_same, "event_company": "", "days_before": 0}

    trade_rows.append({
        "ciq_personid": obs_pid,
        "is_buy": tr["is_buy"],
        "pre_event": best_match["pre_event"],
        "same_industry": best_match["same_industry"],
        "trade_date": trade_date,
        "cname": tr["cname"],
    })

df = pd.DataFrame(trade_rows)
df["pre_x_same"] = df["pre_event"] * df["same_industry"]
print(f"  Total classified trades: {len(df):,}")
print(f"  Pre-event: {df['pre_event'].sum():,}, Baseline: {(df['pre_event']==0).sum():,}")
print(f"  Same-industry: {df['same_industry'].sum():,}")
print(f"  Pre-event x Same-industry: {df['pre_x_same'].sum():,}")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


# =====================================================================
# STEP 3: Regressions
# =====================================================================
print("\n" + "=" * 100)
print("REGRESSION: is_buy ~ pre_event + same_industry + pre_event x same_industry")
print("=" * 100)

specs = [
    ("OLS + HC1", None, "HC1"),
    ("Observer-cluster", None, "observer"),
    ("Year FE + HC1", "yr", "HC1"),
    ("Year FE + Observer-cl", "yr", "observer"),
]

df["year"] = df["trade_date"].dt.year

print(f"\n  {'Spec':<25} {'pre_event':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'pre x same':>12} {'p':>8} {'N':>8}")
print(f"  {'-'*92}")

for spec_name, fe_type, cl_type in specs:
    dm = df[["is_buy", "pre_event", "same_industry", "pre_x_same"]].copy()
    if fe_type == "yr":
        gm = dm.groupby(df["year"]).transform("mean")
        dm = dm - gm
        formula = "is_buy ~ pre_event + same_industry + pre_x_same - 1"
    else:
        formula = "is_buy ~ pre_event + same_industry + pre_x_same"

    try:
        if cl_type == "HC1":
            m = smf.ols(formula, data=dm).fit(cov_type="HC1")
        elif cl_type == "observer":
            m = smf.ols(formula, data=dm).fit(
                cov_type="cluster", cov_kwds={"groups": df["ciq_personid"]})

        c1 = m.params["pre_event"]; p1 = m.pvalues["pre_event"]
        c2 = m.params["same_industry"]; p2 = m.pvalues["same_industry"]
        c3 = m.params["pre_x_same"]; p3 = m.pvalues["pre_x_same"]
        print(f"  {spec_name:<25} {c1:>+10.4f}{sig(p1)} {p1:>8.4f} {c2:>+10.4f}{sig(p2)} {p2:>8.4f} {c3:>+10.4f}{sig(p3)} {p3:>8.4f} {len(dm):>8,}")
    except Exception as e:
        print(f"  {spec_name:<25} ERROR: {str(e)[:60]}")

# =====================================================================
# Also run as logistic regression
# =====================================================================
print(f"\n\n{'='*100}")
print("LOGISTIC REGRESSION: P(buy) ~ pre_event + same_industry + pre x same")
print("=" * 100)

import statsmodels.api as sm

for spec_name, cl_type in [("Logit + HC1", "HC1"), ("Logit + Observer-cl", "observer")]:
    try:
        X = df[["pre_event", "same_industry", "pre_x_same"]].copy()
        X = sm.add_constant(X)
        y = df["is_buy"]

        if cl_type == "HC1":
            m = sm.Logit(y, X).fit(cov_type="HC1", disp=0)
        elif cl_type == "observer":
            m = sm.Logit(y, X).fit(cov_type="cluster", cov_kwds={"groups": df["ciq_personid"]}, disp=0)

        print(f"\n  {spec_name}:")
        print(f"  {'Variable':<20} {'Coef':>10} {'Odds Ratio':>12} {'p':>10}")
        print(f"  {'-'*52}")
        for var in ["pre_event", "same_industry", "pre_x_same"]:
            coef = m.params[var]
            p = m.pvalues[var]
            or_val = np.exp(coef)
            print(f"  {var:<20} {coef:>+8.4f}   {or_val:>10.3f}    {p:>8.4f}{sig(p)}")
        print(f"  N={len(df):,}, Pseudo R2={m.prsquared:.4f}")
    except Exception as e:
        print(f"  {spec_name}: ERROR: {str(e)[:80]}")

# =====================================================================
# Summary statistics
# =====================================================================
print(f"\n\n{'='*100}")
print("SUMMARY: Buy rates by group")
print("=" * 100)

for name, mask in [
    ("Baseline + Diff-ind", (df["pre_event"] == 0) & (df["same_industry"] == 0)),
    ("Baseline + Same-ind", (df["pre_event"] == 0) & (df["same_industry"] == 1)),
    ("Pre-event + Diff-ind", (df["pre_event"] == 1) & (df["same_industry"] == 0)),
    ("Pre-event + Same-ind", (df["pre_event"] == 1) & (df["same_industry"] == 1)),
]:
    sub = df[mask]
    if len(sub) > 0:
        buy_rate = sub["is_buy"].mean()
        print(f"  {name:<25} N={len(sub):>6,}  buy_rate={buy_rate:.3f} ({sub['is_buy'].sum():,} buys / {len(sub):,})")


print("\n\nDone.")
