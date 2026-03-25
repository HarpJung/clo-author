"""Fetch S-1 body files for non-SPAC companies and extract observer+fiduciary passages."""

import csv, os, requests, re, time

headers_http = {"User-Agent": "HarpJung research@harvard.edu"}
edgar_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/EDGAR_Extract"

# Load missing companies, filter to non-SPACs
with open(os.path.join(edgar_dir, "missing_ira_companies.csv"), "r", encoding="utf-8") as f:
    all_companies = list(csv.DictReader(f))

spac_keywords = ["acquisition corp", "spac", "blank check", "capital corp",
                 "merger corp", "growth corp"]
companies = []
for co in all_companies:
    name = (co["display_names"].split("|")[0] if co["display_names"] else "").lower()
    is_spac = any(kw in name for kw in spac_keywords)
    if not is_spac:
        companies.append(co)

print(f"Non-SPAC companies to process: {len(companies)}")
print(f"10 sec delay between requests")
print(f"Estimated time: ~{len(companies) * 10 / 60:.0f} minutes")
print()

# Load EFTS hits to find the S-1 body file URLs
with open(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"), "r", encoding="utf-8") as f:
    efts_hits = list(csv.DictReader(f))

# Build CIK -> S-1 body file mapping (most recent per CIK)
cik_to_s1body = {}
for h in efts_hits:
    if h["file_type"] in ("S-1", "S-1/A"):
        cik = h["ciks"].split("|")[0] if h["ciks"] else ""
        date = h["file_date"]
        if cik and (cik not in cik_to_s1body or date > cik_to_s1body[cik]["file_date"]):
            cik_to_s1body[cik] = h

results = []
success = 0
failed = 0
no_url = 0

for i, co in enumerate(companies):
    cik = co["cik"]
    company_name = co["display_names"].split("|")[0] if co["display_names"] else ""

    if cik not in cik_to_s1body:
        no_url += 1
        results.append({
            "cik": cik, "company": company_name, "file_date": co["file_date"],
            "fetch_status": "no_s1_body_url", "observer_mentions": 0,
            "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
            "observer_passages": "",
        })
        continue

    hit = cik_to_s1body[cik]
    file_id = hit["file_id"]
    adsh = hit["adsh"]
    adsh_nodash = adsh.replace("-", "")

    # Extract filename from file_id
    if ":" in file_id:
        filename = file_id.split(":")[1]
    else:
        filename = file_id

    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh_nodash}/{filename}"

    try:
        r = requests.get(url, headers=headers_http, timeout=120)

        if r.status_code == 200 and len(r.content) > 1000:
            # Strip HTML
            text_clean = re.sub(r"<[^>]+>", " ", r.text)
            text_clean = re.sub(r"&nbsp;", " ", text_clean)
            text_clean = re.sub(r"&[a-z]+;", " ", text_clean)
            text_clean = re.sub(r"\s+", " ", text_clean)

            # Find all observer mentions with surrounding context (500 chars each side)
            observer_passages = []
            for m in re.finditer(r"(?i)\bobserver\b", text_clean):
                start = max(0, m.start() - 500)
                end = min(len(text_clean), m.end() + 500)
                passage = text_clean[start:end].strip()
                observer_passages.append(passage)

            # Deduplicate overlapping passages
            unique_passages = []
            seen_starts = set()
            for p in observer_passages:
                p_key = p[:100]
                if p_key not in seen_starts:
                    seen_starts.add(p_key)
                    unique_passages.append(p)

            # Check for fiduciary language
            full_observer_text = " ".join(unique_passages)
            has_fiduciary = bool(re.search(r"(?i)fiduciary\s+manner", full_observer_text))
            has_no_fiduciary = bool(re.search(
                r"(?i)(?:shall\s+not|no|not)\s+.*?fiduciary\s+(?:dut|obligation)",
                full_observer_text
            ))

            # Also check the full document for fiduciary+observer proximity
            if not has_fiduciary and not has_no_fiduciary:
                # Broader search in full text
                has_fiduciary = bool(re.search(r"(?i)fiduciary\s+manner", text_clean))
                has_no_fiduciary = bool(re.search(
                    r"(?i)observer.{0,500}(?:shall\s+not|no|not).{0,100}fiduciary",
                    text_clean
                ))

            observer_count = len(re.findall(r"(?i)\bobserver\b", text_clean))

            results.append({
                "cik": cik, "company": company_name, "file_date": hit["file_date"],
                "fetch_status": "ok", "observer_mentions": observer_count,
                "has_fiduciary_manner": has_fiduciary,
                "has_no_fiduciary_duty": has_no_fiduciary,
                "observer_passages": " ||| ".join(unique_passages[:5]),  # save first 5 passages
            })
            success += 1

        elif r.status_code == 403:
            print(f"  [{i+1}] 403 BLOCKED - pausing 60 sec...")
            results.append({
                "cik": cik, "company": company_name, "file_date": hit["file_date"],
                "fetch_status": "blocked", "observer_mentions": 0,
                "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
                "observer_passages": "",
            })
            failed += 1
            time.sleep(60)
            continue
        else:
            results.append({
                "cik": cik, "company": company_name, "file_date": hit["file_date"],
                "fetch_status": f"http_{r.status_code}", "observer_mentions": 0,
                "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
                "observer_passages": "",
            })
            failed += 1

    except Exception as e:
        results.append({
            "cik": cik, "company": company_name, "file_date": co["file_date"],
            "fetch_status": f"error: {str(e)[:50]}", "observer_mentions": 0,
            "has_fiduciary_manner": False, "has_no_fiduciary_duty": False,
            "observer_passages": "",
        })
        failed += 1

    if (i + 1) % 20 == 0:
        print(f"  [{i+1}/{len(companies)}] {success} OK, {failed} fail, {no_url} no URL | {company_name[:40]}")

    # Save intermediate every 50
    if (i + 1) % 50 == 0:
        outfile = os.path.join(edgar_dir, "s1_body_observer_analysis.csv")
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            if results:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
        print(f"  [Saved intermediate: {len(results)} rows]")

    time.sleep(10)

# Final save
outfile = os.path.join(edgar_dir, "s1_body_observer_analysis.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    if results:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

fid_yes = sum(1 for r in results if r["has_fiduciary_manner"] in (True, "True"))
fid_no = sum(1 for r in results if r["has_no_fiduciary_duty"] in (True, "True"))
ok_count = sum(1 for r in results if r["fetch_status"] == "ok")
avg_mentions = sum(int(r["observer_mentions"]) for r in results if r["fetch_status"] == "ok") / max(ok_count, 1)

print(f"\n{'='*60}")
print(f"S-1 BODY ANALYSIS COMPLETE")
print(f"{'='*60}")
print(f"Companies attempted: {len(companies)}")
print(f"Successfully fetched: {success}")
print(f"No S-1 body URL: {no_url}")
print(f"Failed: {failed}")
print(f"Avg observer mentions per S-1: {avg_mentions:.1f}")
print(f"")
print(f"=== FIDUCIARY LANGUAGE ===")
print(f"Fiduciary manner: {fid_yes}")
print(f"No fiduciary duty: {fid_no}")
print(f"Neither: {len(results) - fid_yes - fid_no}")
print(f"Saved to: {outfile}")
