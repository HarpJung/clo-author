"""Pull remaining Preqin tables (5-13) that failed on first run."""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import psycopg2
import pandas as pd
import os, time

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
preqin_dir = os.path.join(data_dir, "Preqin")

conn = psycopg2.connect(host="wrds-pgdata.wharton.upenn.edu", port=9737,
                         dbname="wrds", user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()

def pull_and_save(query, filename, description):
    print(f"\n--- {description} ---")
    time.sleep(3)
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(os.path.join(preqin_dir, filename), index=False)
    print(f"  Saved: {len(df):,} rows, {len(cols)} columns -> {filename}")
    return df

# 5. PME benchmarks - use SELECT * to avoid column name issues
pull_and_save("SELECT * FROM preqin.preqinbenchmarkspc ORDER BY benchmark_id, benchmark_vintage",
              "benchmarks_pme.csv", "5. Benchmark PMEs")

# 6. Fund terms
pull_and_save("""SELECT * FROM preqin.preqinfundterms ORDER BY fund_id""",
              "fund_terms.csv", "6. Fund Terms")

# 7. Manager details (full)
pull_and_save("""SELECT * FROM preqin.managerdetails ORDER BY firm_id""",
              "manager_details_full.csv", "7. Manager Details")

# 8. Manager investment types
pull_and_save("""SELECT * FROM preqin.managerinvestmenttypes ORDER BY firm_id""",
              "manager_investment_types.csv", "8. Manager Investment Types")

# 9. Investor portfolio
pull_and_save("""SELECT * FROM preqin.investorportfolio ORDER BY firm_id, fund_id""",
              "investor_portfolio.csv", "9. Investor Portfolio")

# 10. Investor details
pull_and_save("""SELECT * FROM preqin.investordetails ORDER BY firm_id""",
              "investor_details.csv", "10. Investor Details")

# 11. Cashflows
pull_and_save("""SELECT * FROM preqin.cashflow ORDER BY fund_id, transaction_date""",
              "cashflows_full.csv", "11. Cashflows")

# 12. VC deals
pull_and_save("""SELECT * FROM preqin.preqindealsvc ORDER BY deal_date""",
              "vc_deals_full.csv", "12. VC Deals")

# 13. Buyout deals
pull_and_save("""SELECT * FROM preqin.preqindealsbuyout ORDER BY deal_date""",
              "buyout_deals_full.csv", "13. Buyout Deals")

conn.close()

# Summary
print(f"\n\n{'=' * 80}")
print("ALL PREQIN FILES")
print(f"{'=' * 80}")
for fname in sorted(os.listdir(preqin_dir)):
    if fname.endswith(".csv"):
        fp = os.path.join(preqin_dir, fname)
        size_mb = os.path.getsize(fp) / (1024 * 1024)
        with open(fp, "r", encoding="utf-8") as fh:
            n = sum(1 for _ in fh) - 1
        print(f"  {fname:<50} {n:>8,} rows  ({size_mb:.1f} MB)")

print("\nDone.")
