"""Download SEC Form D structured data sets from DERA Data Library."""

import requests, os, time, zipfile, io

headers = {"User-Agent": "HarpJung research@harvard.edu"}
out_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/FormD"

# Form D data sets are available quarterly from SEC
# URL pattern: https://www.sec.gov/files/dera/data/form-d/formd{YYYYQQ}.zip
# Available from 2009 onward

# We want 2017-2026 to match our study period
years_quarters = []
for year in range(2017, 2027):
    for qtr in range(1, 5):
        if year == 2026 and qtr > 1:
            continue
        years_quarters.append((year, qtr))

print(f"Form D quarterly files to download: {len(years_quarters)}")
print(f"Rate: 10 seconds between downloads")
print(f"Estimated time: ~{len(years_quarters) * 10 / 60:.0f} minutes")
print()

success = 0
failed = 0
total_files = 0

for year, qtr in years_quarters:
    # SEC uses format: formdYYYYQQ.zip (e.g., formd202301.zip)
    # or formd_YYYYQQ.zip -- need to try both
    qtr_str = f"{year}q{qtr}"

    urls_to_try = [
        f"https://www.sec.gov/files/dera/data/form-d/formd{qtr_str}.zip",
        f"https://www.sec.gov/files/dera/data/form-d/formd_{qtr_str}.zip",
        f"https://www.sec.gov/data-research/sec-markets-data/form-d-data-sets/formd{qtr_str}.zip",
    ]

    downloaded = False
    for url in urls_to_try:
        try:
            r = requests.get(url, headers=headers, timeout=60)
            if r.status_code == 200 and len(r.content) > 1000:
                # It's a zip file — extract it
                qtr_dir = os.path.join(out_dir, f"{year}_Q{qtr}")
                os.makedirs(qtr_dir, exist_ok=True)

                with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                    file_list = zf.namelist()
                    zf.extractall(qtr_dir)

                print(f"  {year} Q{qtr}: OK ({len(r.content)//1024} KB, {len(file_list)} files: {', '.join(file_list[:5])})")
                success += 1
                total_files += len(file_list)
                downloaded = True
                break
            elif r.status_code == 404:
                continue  # try next URL pattern
            else:
                continue
        except Exception as e:
            continue

    if not downloaded:
        # Try alternate patterns
        alt_urls = [
            f"https://efts.sec.gov/LATEST/data-catalog/form-d/{year}q{qtr}",
            f"https://www.sec.gov/dera/data/form-d-data-sets/{year}q{qtr}.zip",
        ]
        for url in alt_urls:
            try:
                r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
                if r.status_code == 200 and len(r.content) > 1000:
                    content_type = r.headers.get("content-type", "")
                    if "zip" in content_type or r.content[:4] == b"PK\x03\x04":
                        qtr_dir = os.path.join(out_dir, f"{year}_Q{qtr}")
                        os.makedirs(qtr_dir, exist_ok=True)
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                            file_list = zf.namelist()
                            zf.extractall(qtr_dir)
                        print(f"  {year} Q{qtr}: OK (alt URL, {len(file_list)} files)")
                        success += 1
                        total_files += len(file_list)
                        downloaded = True
                        break
            except:
                continue

    if not downloaded:
        print(f"  {year} Q{qtr}: FAILED (tried all URL patterns)")
        failed += 1

    time.sleep(10)

print(f"\n{'='*60}")
print(f"FORM D DOWNLOAD COMPLETE")
print(f"{'='*60}")
print(f"  Successful quarters: {success}")
print(f"  Failed quarters: {failed}")
print(f"  Total files extracted: {total_files}")

# List what we got
print(f"\n  Downloaded quarters:")
for d in sorted(os.listdir(out_dir)):
    dp = os.path.join(out_dir, d)
    if os.path.isdir(dp):
        files = os.listdir(dp)
        print(f"    {d}: {len(files)} files ({', '.join(files[:3])}{'...' if len(files) > 3 else ''})")
