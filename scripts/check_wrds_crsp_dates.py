"""Check latest available CRSP date on WRDS."""
import wrds
db = wrds.Connection(wrds_username='hjung1')
result = db.raw_sql("SELECT MAX(date) as max_date, MIN(date) as min_date FROM crsp.dsf")
print(f"CRSP dsf date range: {result.iloc[0]['min_date']} to {result.iloc[0]['max_date']}")
db.close()
