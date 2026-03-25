"""Expand EFTS search with broader observer terms to find more treatment firms."""

import requests, time, json, csv, os

headers_http = {"User-Agent": "HarpJung research@harvard.edu"}
edgar_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/EDGAR_Extract"

search_terms = [
    '"nonvoting observer"',
    '"non-voting observer"',
    '"observer rights"',
    '"nonvoting capacity"',
    '"non-voting capacity"',
    '"board attendee"',
    '"nonvoting representative"',
    '"observer to the board"',
]

all_ciks = set()
term_results = {}

for term in search_terms:
    print(f"\nSearching: {term}")
    url = (
        f"https://efts.sec.gov/LATEST/search-index?"
        f"q={requests.utils.quote(term)}&forms=S-1"
        f"&dateRange=custom&startdt=2017-01-01&enddt=2026-12-31"
        f"&from=0&size=100"
    )

    try:
        r = requests.get(url, headers=headers_http, timeout=60)
        if r.status_code == 200:
            data = r.json()
            total = data["hits"]["total"]["value"]
            hits = data["hits"]["hits"]

            ciks_for_term = set()
            for hit in hits:
                src = hit["_source"]
                for cik in src.get("ciks", []):
                    ciks_for_term.add(cik.strip())
                    all_ciks.add(cik.strip())

            term_results[term] = {"total_files": total, "unique_ciks_page1": len(ciks_for_term)}
            print(f"  Total files: {total}, Unique CIKs (page 1): {len(ciks_for_term)}")

            # If many results, page through
            if total > 100:
                pages = min((total + 99) // 100, 20)  # cap at 20 pages
                for page in range(1, pages):
                    time.sleep(10)
                    url_page = (
                        f"https://efts.sec.gov/LATEST/search-index?"
                        f"q={requests.utils.quote(term)}&forms=S-1"
                        f"&dateRange=custom&startdt=2017-01-01&enddt=2026-12-31"
                        f"&from={page * 100}&size=100"
                    )
                    rp = requests.get(url_page, headers=headers_http, timeout=60)
                    if rp.status_code == 200:
                        dp = rp.json()
                        for hit in dp["hits"]["hits"]:
                            for cik in hit["_source"].get("ciks", []):
                                all_ciks.add(cik.strip())
                        print(f"    Page {page+1}: total CIKs so far = {len(all_ciks)}")
        else:
            print(f"  HTTP {r.status_code}")
            term_results[term] = {"total_files": 0, "unique_ciks_page1": 0}

    except Exception as e:
        print(f"  Error: {e}")
        term_results[term] = {"total_files": 0, "unique_ciks_page1": 0}

    time.sleep(10)

# Load existing "board observer" CIKs
with open(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"), "r") as f:
    existing = list(csv.DictReader(f))
existing_ciks = set()
for h in existing:
    if h.get("ciks"):
        for cik in h["ciks"].split("|"):
            existing_ciks.add(cik.strip())

# New CIKs not in original search
new_ciks = all_ciks - existing_ciks
combined_ciks = all_ciks | existing_ciks

print(f"\n{'='*60}")
print(f"EXPANDED SEARCH RESULTS")
print(f"{'='*60}")
print(f"  Original 'board observer' CIKs: {len(existing_ciks):,}")
print(f"  New CIKs from expanded search: {len(new_ciks):,}")
print(f"  Combined unique CIKs: {len(combined_ciks):,}")

print(f"\n  Per-term results:")
for term, res in term_results.items():
    print(f"    {term:35} files={res['total_files']:>5}  CIKs(p1)={res['unique_ciks_page1']:>4}")

# Save expanded CIK list
outfile = os.path.join(edgar_dir, "efts_expanded_observer_ciks.csv")
with open(outfile, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["cik", "source"])
    for cik in sorted(existing_ciks):
        writer.writerow([cik, "board_observer"])
    for cik in sorted(new_ciks):
        writer.writerow([cik, "expanded_search"])

print(f"\n  Saved: {outfile}")

# Also update the treatment/control assignment
all_s1_file = os.path.join(edgar_dir, "all_s1_filings_2017_2026.csv")
with open(all_s1_file, "r") as f:
    all_s1 = list(csv.DictReader(f))
all_s1_ciks = set(str(int(r["cik"])) for r in all_s1 if r["cik"] and r["cik"].strip().isdigit())

# Normalize combined CIKs
combined_normalized = set()
for cik in combined_ciks:
    try:
        combined_normalized.add(str(int(cik)))
    except:
        pass

treatment_expanded = all_s1_ciks & combined_normalized
control_expanded = all_s1_ciks - combined_normalized

print(f"\n  Updated treatment/control (expanded):")
print(f"    Treatment: {len(treatment_expanded):,}")
print(f"    Control: {len(control_expanded):,}")

# Save updated assignment
outfile2 = os.path.join(
    "C:/Users/hjung/Documents/Claude/CorpAcct/Data/Test1_Observer_vs_NoObserver",
    "00_treatment_control_expanded.csv"
)
with open(outfile2, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["cik", "group", "has_observer_mention"])
    for cik in sorted(treatment_expanded, key=int):
        writer.writerow([cik, "treatment", 1])
    for cik in sorted(control_expanded, key=int):
        writer.writerow([cik, "control", 0])

print(f"  Saved: {outfile2}")
