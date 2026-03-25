"""Fetch IRA exhibits for S-1 companies missing IRA data (Round 2)."""

import csv, os, requests, re, time

headers_http = {"User-Agent": "HarpJung research@harvard.edu"}
edgar_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/EDGAR_Extract"
exhibitdir = os.path.join(edgar_dir, "exhibits")

# Load missing companies
with open(os.path.join(edgar_dir, "missing_ira_companies.csv"), "r", encoding="utf-8") as f:
    companies = list(csv.DictReader(f))

print(f"Looking up IRA exhibits for {len(companies)} companies")
print(f"10 sec delay between each request")
print(f"Estimated time: ~{len(companies) * 20 / 60:.0f} minutes")
print()

results = []
success = 0
failed = 0
no_ira_found = 0

for i, co in enumerate(companies):
    cik = co["cik"]
    adsh = co["adsh"]
    adsh_nodash = adsh.replace("-", "")
    company_name = co["display_names"].split("|")[0] if co["display_names"] else ""

    # Step 1: Fetch the filing index JSON
    json_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh_nodash}/index.json"

    try:
        r = requests.get(json_url, headers=headers_http, timeout=60)
        time.sleep(10)

        if r.status_code == 403:
            print(f"  [{i+1}] 403 BLOCKED on index - pausing 60 sec...")
            failed += 1
            results.append({
                "cik": cik, "company": company_name, "adsh": adsh,
                "file_date": co["file_date"], "exhibit_url": "",
                "exhibit_type": "", "fetch_status": "blocked_index",
                "file_size": 0, "observer_mentions": 0,
                "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
            })
            time.sleep(60)
            continue

        if r.status_code != 200:
            # Try HTML index instead
            html_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh_nodash}/"
            r = requests.get(html_url, headers=headers_http, timeout=60)
            time.sleep(10)

            if r.status_code != 200:
                failed += 1
                results.append({
                    "cik": cik, "company": company_name, "adsh": adsh,
                    "file_date": co["file_date"], "exhibit_url": "",
                    "exhibit_type": "", "fetch_status": f"index_http_{r.status_code}",
                    "file_size": 0, "observer_mentions": 0,
                    "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
                })
                continue

        # Parse the response to find exhibit files
        exhibit_files = []

        if r.headers.get("content-type", "").startswith("application/json"):
            # JSON index
            try:
                idx_data = r.json()
                items = idx_data.get("directory", {}).get("item", [])
                for item in items:
                    name = item.get("name", "")
                    name_lower = name.lower()
                    if re.search(r"ex[- _]?(4|10)", name_lower):
                        score = 0
                        if "investor" in name_lower or "rights" in name_lower:
                            score += 10
                        if "stockholder" in name_lower or "shareholder" in name_lower:
                            score += 8
                        if "observer" in name_lower:
                            score += 15
                        if "ex4" in name_lower or "ex-4" in name_lower or "ex_4" in name_lower:
                            score += 5
                        if "ex10" in name_lower or "ex-10" in name_lower or "ex_10" in name_lower:
                            score += 3
                        if name_lower.endswith(".htm") or name_lower.endswith(".html"):
                            score += 1
                        exhibit_files.append((name, score))
            except:
                pass
        else:
            # HTML index - parse links
            links = re.findall(r'href="([^"]+)"', r.text)
            for link in links:
                link_lower = link.lower()
                if re.search(r"ex[- _]?(4|10)", link_lower):
                    score = 0
                    if "investor" in link_lower or "rights" in link_lower:
                        score += 10
                    if "stockholder" in link_lower or "shareholder" in link_lower:
                        score += 8
                    if "observer" in link_lower:
                        score += 15
                    if "ex4" in link_lower or "ex-4" in link_lower:
                        score += 5
                    if "ex10" in link_lower or "ex-10" in link_lower:
                        score += 3
                    if link_lower.endswith(".htm") or link_lower.endswith(".html"):
                        score += 1
                    exhibit_files.append((link, score))

        if not exhibit_files:
            no_ira_found += 1
            results.append({
                "cik": cik, "company": company_name, "adsh": adsh,
                "file_date": co["file_date"], "exhibit_url": json_url,
                "exhibit_type": "", "fetch_status": "no_ira_exhibit_found",
                "file_size": 0, "observer_mentions": 0,
                "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
            })
            if (i + 1) % 20 == 0:
                print(f"  [{i+1}/{len(companies)}] {success} OK, {failed} fail, {no_ira_found} no IRA | {company_name[:40]}")
            continue

        # Pick the best exhibit by score
        exhibit_files.sort(key=lambda x: x[1], reverse=True)
        best_file = exhibit_files[0][0]

        # Build full URL
        if best_file.startswith("/"):
            exhibit_url = f"https://www.sec.gov{best_file}"
        elif best_file.startswith("http"):
            exhibit_url = best_file
        else:
            exhibit_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh_nodash}/{best_file}"

        # Step 2: Fetch the exhibit
        time.sleep(10)
        r2 = requests.get(exhibit_url, headers=headers_http, timeout=60)

        if r2.status_code == 200 and len(r2.content) > 500:
            # Save raw file
            safe_name = f"{cik}_{adsh}_IRA_{co['file_date']}.txt"
            local_path = os.path.join(exhibitdir, safe_name)
            with open(local_path, "w", encoding="utf-8", errors="replace") as f:
                f.write(r2.text)

            # Analyze
            text_clean = re.sub(r"<[^>]+>", " ", r2.text)
            text_clean = re.sub(r"\s+", " ", text_clean)

            has_fiduciary = bool(re.search(r"(?i)fiduciary\s+manner", text_clean))
            has_no_fiduciary = bool(re.search(r"(?i)(?:shall\s+not|no)\s+.*?fiduciary\s+(?:dut|obligation)", text_clean))
            observer_count = len(re.findall(r"(?i)\bobserver\b", text_clean))

            results.append({
                "cik": cik, "company": company_name, "adsh": adsh,
                "file_date": co["file_date"], "exhibit_url": exhibit_url,
                "exhibit_type": "IRA_lookup", "fetch_status": "ok",
                "file_size": len(r2.text), "observer_mentions": observer_count,
                "has_fiduciary_manner": has_fiduciary, "has_no_fiduciary_duty": has_no_fiduciary,
            })
            success += 1

        elif r2.status_code == 403:
            print(f"  [{i+1}] 403 BLOCKED on exhibit - pausing 60 sec...")
            results.append({
                "cik": cik, "company": company_name, "adsh": adsh,
                "file_date": co["file_date"], "exhibit_url": exhibit_url,
                "exhibit_type": "", "fetch_status": "blocked_exhibit",
                "file_size": 0, "observer_mentions": 0,
                "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
            })
            failed += 1
            time.sleep(60)
            continue
        else:
            results.append({
                "cik": cik, "company": company_name, "adsh": adsh,
                "file_date": co["file_date"], "exhibit_url": exhibit_url,
                "exhibit_type": "", "fetch_status": f"exhibit_http_{r2.status_code}",
                "file_size": 0, "observer_mentions": 0,
                "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
            })
            failed += 1

    except Exception as e:
        results.append({
            "cik": cik, "company": company_name, "adsh": adsh,
            "file_date": co["file_date"], "exhibit_url": "",
            "exhibit_type": "", "fetch_status": f"error: {str(e)[:50]}",
            "file_size": 0, "observer_mentions": 0,
            "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
        })
        failed += 1

    if (i + 1) % 20 == 0:
        print(f"  [{i+1}/{len(companies)}] {success} OK, {failed} fail, {no_ira_found} no IRA | {company_name[:40]}")

    # Save intermediate results every 50 companies
    if (i + 1) % 50 == 0:
        outfile = os.path.join(edgar_dir, "exhibit_analysis_results_round2.csv")
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            if results:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
        print(f"  [Saved intermediate results: {len(results)} rows]")

# Final save
outfile = os.path.join(edgar_dir, "exhibit_analysis_results_round2.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    if results:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

# Summary
fid_yes = sum(1 for r in results if r["has_fiduciary_manner"] in (True, "True"))
fid_no = sum(1 for r in results if r["has_no_fiduciary_duty"] in (True, "True"))

print(f"\n{'='*60}")
print(f"ROUND 2 EXHIBIT FETCHING COMPLETE")
print(f"{'='*60}")
print(f"Companies attempted: {len(companies)}")
print(f"IRA exhibits found & fetched: {success}")
print(f"No IRA exhibit found: {no_ira_found}")
print(f"Failed: {failed}")
print(f"")
print(f"=== FIDUCIARY LANGUAGE (Round 2) ===")
print(f"Fiduciary manner: {fid_yes}")
print(f"No fiduciary duty: {fid_no}")
print(f"Saved to: {outfile}")
