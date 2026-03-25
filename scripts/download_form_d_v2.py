"""Download SEC Form D structured data sets — correct URL pattern."""

import requests, os, time, zipfile, io

headers = {"User-Agent": "HarpJung research@harvard.edu"}
out_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/FormD"
base_url = "https://www.sec.gov/files/structureddata/data/form-d-data-sets"

years_quarters = []
for year in range(2017, 2027):
    for qtr in range(1, 5):
        if year == 2026 and qtr > 1:
            continue
        years_quarters.append((year, qtr))

print(f"Downloading {len(years_quarters)} quarterly Form D files")
print(f"URL pattern: {base_url}/{{year}}q{{qtr}}_d.zip")
print(f"Rate: 10 seconds between downloads")
print()

success = 0
failed = 0

for year, qtr in years_quarters:
    url = f"{base_url}/{year}q{qtr}_d.zip"
    qtr_dir = os.path.join(out_dir, f"{year}_Q{qtr}")

    if os.path.exists(qtr_dir) and len(os.listdir(qtr_dir)) > 0:
        print(f"  {year} Q{qtr}: already exists, skipping")
        success += 1
        continue

    try:
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code == 200 and len(r.content) > 1000:
            os.makedirs(qtr_dir, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                file_list = zf.namelist()
                zf.extractall(qtr_dir)
            print(f"  {year} Q{qtr}: OK ({len(r.content)//1024} KB, {len(file_list)} files)")
            success += 1
        else:
            print(f"  {year} Q{qtr}: HTTP {r.status_code}")
            failed += 1
    except Exception as e:
        print(f"  {year} Q{qtr}: ERROR - {str(e)[:60]}")
        failed += 1

    time.sleep(10)

print(f"\nDone: {success} OK, {failed} failed")

# Inspect the first successful download
for d in sorted(os.listdir(out_dir)):
    dp = os.path.join(out_dir, d)
    if os.path.isdir(dp) and os.listdir(dp):
        files = os.listdir(dp)
        print(f"\nSample directory ({d}): {files}")

        # Check RELATEDPERSONS file
        for f in files:
            if "RELATED" in f.upper() or "PERSON" in f.upper():
                fp = os.path.join(dp, f)
                with open(fp, "r", encoding="utf-8", errors="replace") as fh:
                    lines = fh.readlines()
                print(f"\n{f}: {len(lines):,} rows")
                print(f"Header: {lines[0].strip()}")
                if len(lines) > 1:
                    print(f"Sample row: {lines[1].strip()[:200]}")
                    print(f"Sample row: {lines[2].strip()[:200]}")
        break
