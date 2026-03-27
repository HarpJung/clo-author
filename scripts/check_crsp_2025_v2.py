"""Check newer CRSP tables for 2025 data."""
import psycopg2, time

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()

# Check crsp.daily_returns
for tbl in ["crsp.daily_returns", "crsp.dsf_v2", "crsp_a_stock.dsf_v2"]:
    time.sleep(1)
    try:
        cur.execute(f"SELECT * FROM {tbl} LIMIT 1")
        cols = [desc[0] for desc in cur.description]
        print(f"\n{tbl} columns: {cols}")
        # Find the date column
        date_col = None
        for c in cols:
            if 'date' in c.lower() or 'dt' in c.lower():
                date_col = c
                break
        if date_col:
            cur.execute(f"SELECT MAX({date_col}) FROM {tbl}")
            row = cur.fetchone()
            print(f"  Max {date_col}: {row[0]}")
        else:
            print(f"  No obvious date column found")
    except Exception as e:
        conn.rollback()
        print(f"{tbl}: error - {str(e)[:100]}")

conn.close()
