#!/usr/bin/env python3
"""
Maya Index Refresh — Updates the company index CSV with new companies and seed report IDs.

Run this monthly (or whenever needed) to keep the index current.

Usage:
  python3 maya_refresh_index.py                        # Update in place
  python3 maya_refresh_index.py --output new_index.csv # Save to different file
  python3 maya_refresh_index.py --dry-run              # Show what would change, don't save
  python3 maya_refresh_index.py --fill-seeds-only      # Only find missing seed IDs

What it does:
  1. Loads the existing maya_company_index.csv
  2. Scans Maya API for companies not in the index (ID range is configurable, best-effort)
  3. Finds seed report IDs for companies that don't have one (exact name match only)
  4. Reports suspected fuzzy matches to stdout but NEVER assigns them automatically
  5. Saves the updated CSV with a SeedSource column (exact / manual / empty)

Safety:
  - Seeds are only assigned when the API's companyName matches NameHE exactly.
  - Fuzzy/substring matches are printed as suggestions for manual review.
  - The SeedSource column tracks provenance: 'exact' for auto-found, 'manual' for hand-set.

Typical runtime: 3-8 minutes (depending on how many gaps need filling).
"""

import json
import subprocess
import sys
import os
import csv
import argparse
from datetime import datetime

BASE_URL = "https://maya.tase.co.il/api/v1"
CURL_HEADERS = [
    '-H', 'Accept: application/json',
    '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
]

# Company ID scan range
SCAN_MIN_ID = 800
SCAN_MAX_ID = 2600

# Report ID scan range for finding seeds
SEED_SCAN_START = 1730000
SEED_SCAN_END = 1100000
SEED_SCAN_STEP_COARSE = 200
SEED_SCAN_STEP_FINE = 50

# ─── HTTP ─────────────────────────────────────────────────────────────────────

def fetch(url, timeout_sec=5):
    """Fetch JSON from URL. Returns None on failure (non-throwing for batch use)."""
    try:
        r = subprocess.run(
            ['curl', '-s', '--fail', '--max-time', str(timeout_sec)] + CURL_HEADERS + [url],
            capture_output=True, text=True, timeout=timeout_sec + 5
        )
        if r.returncode != 0:
            return None
        return json.loads(r.stdout)
    except (json.JSONDecodeError, subprocess.TimeoutExpired):
        return None

# ─── CSV ──────────────────────────────────────────────────────────────────────

def find_index_file():
    """Find the company index CSV file."""
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "maya_company_index.csv"),
        os.path.join(os.getcwd(), "maya_company_index.csv"),
        "/mnt/user-data/uploads/maya_company_index.csv",
        "/mnt/user-data/outputs/maya_company_index.csv",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None

def load_index(path):
    """Load existing CSV index into a dict keyed by CompanyId."""
    companies = {}
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cid = int(row["CompanyId"])
                seed = row.get("SeedReportId", "").strip()
                source = row.get("SeedSource", "").strip()
                if seed and not source:
                    source = "legacy"
                companies[cid] = {
                    "CompanyId": cid,
                    "NameEN": row.get("NameEN", "").strip(),
                    "NameHE": row.get("NameHE", "").strip(),
                    "SubId": row.get("SubId", "").strip(),
                    "SeedReportId": seed,
                    "SeedSource": source,
                }
            except (KeyError, ValueError):
                continue
    return companies

def save_index(companies, path):
    """Save company dict to CSV, sorted alphabetically by English name."""
    rows = sorted(companies.values(), key=lambda c: (c.get("NameEN") or c.get("NameHE") or "").upper())
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["CompanyId", "NameEN", "NameHE", "SubId", "SeedReportId", "SeedSource"])
        writer.writeheader()
        writer.writerows(rows)

# ─── Step 1: Scan for new companies ──────────────────────────────────────────

def scan_for_new_companies(existing_ids, scan_min=SCAN_MIN_ID, scan_max=SCAN_MAX_ID):
    """Scan Maya API for company IDs not in the existing index."""
    new_companies = []
    total = scan_max - scan_min + 1

    for i, cid in enumerate(range(scan_min, scan_max + 1)):
        if (i + 1) % 100 == 0:
            print(f"  Scanning IDs... {i + 1}/{total} checked, {len(new_companies)} new found")

        if cid in existing_ids:
            continue

        data = fetch(f"{BASE_URL}/companies/{cid}/details", timeout_sec=3)
        if data and data.get("name"):
            new_companies.append({
                "CompanyId": cid,
                "NameEN": "",
                "NameHE": data.get("name", ""),
                "SubId": str(data.get("mainSecurityId", "")),
                "SeedReportId": "",
                "SeedSource": "",
            })

    return new_companies

# ─── Step 2: Find missing seed report IDs ────────────────────────────────────

def find_missing_seeds(companies):
    """
    For companies without a SeedReportId, scan Maya reports to find one.
    Uses /reports/{id}/reduce which returns companyName quickly.

    Only assigns seeds on EXACT name match (companyName == NameHE).
    Fuzzy matches are reported to stdout but never saved automatically.
    """
    # Build lookup: Hebrew name → company ID (only for those missing seeds)
    need_seed = {}
    for cid, c in companies.items():
        if not c.get("SeedReportId"):
            name_he = c["NameHE"].strip()
            if name_he:
                need_seed[name_he] = cid

    if not need_seed:
        return 0, []

    print(f"  Need seeds for {len(need_seed)} companies...")
    found = 0
    checked = 0

    # Coarse scan: every 200 report IDs
    for rid in range(SEED_SCAN_START, SEED_SCAN_END, -SEED_SCAN_STEP_COARSE):
        if not need_seed:
            break

        data = fetch(f"{BASE_URL}/reports/{rid}/reduce", timeout_sec=3)
        if data and "companyName" in data:
            cname = data["companyName"]
            if cname in need_seed:
                cid = need_seed.pop(cname)
                companies[cid]["SeedReportId"] = str(rid)
                companies[cid]["SeedSource"] = "exact"
                found += 1

        checked += 1
        if checked % 200 == 0:
            print(f"    Checked {checked} reports, found {found} seeds, {len(need_seed)} remaining")

    # Fine scan for stragglers: every 50 report IDs
    if need_seed:
        print(f"  Fine scan for {len(need_seed)} remaining...")
        for rid in range(SEED_SCAN_START, SEED_SCAN_END, -SEED_SCAN_STEP_FINE):
            if not need_seed:
                break

            data = fetch(f"{BASE_URL}/reports/{rid}/reduce", timeout_sec=3)
            if data and "companyName" in data:
                cname = data["companyName"]
                if cname in need_seed:
                    cid = need_seed.pop(cname)
                    companies[cid]["SeedReportId"] = str(rid)
                    companies[cid]["SeedSource"] = "exact"
                    found += 1

            checked += 1
            if checked % 500 == 0:
                print(f"    Checked {checked} reports, found {found} seeds, {len(need_seed)} remaining")

    # Fuzzy: report suspected matches but NEVER assign automatically
    suspected = []
    if need_seed:
        # Collect all known name→seed mappings (list per name to handle duplicates)
        from collections import defaultdict
        name_to_reports = defaultdict(list)
        for cid, c in companies.items():
            if c.get("SeedReportId"):
                name_to_reports[c["NameHE"]].append((c["SeedReportId"], cid))

        for name_he, cid in need_seed.items():
            for known_name, entries in name_to_reports.items():
                if name_he in known_name or known_name in name_he:
                    for seed, source_cid in entries:
                        suspected.append({
                            "company_id": cid,
                            "company_name": name_he,
                            "matched_name": known_name,
                            "matched_id": source_cid,
                            "suggested_seed": seed,
                        })
                    break

    return found, suspected

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Maya Index Refresh — Update company index CSV")
    parser.add_argument("--output", default=None, help="Save to this path (default: overwrite original)")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without saving")
    parser.add_argument("--fill-seeds-only", action="store_true", help="Only find missing seed IDs")
    parser.add_argument("--scan-range", nargs=2, type=int, metavar=("MIN", "MAX"),
                        help=f"Company ID scan range (default: {SCAN_MIN_ID} {SCAN_MAX_ID})")

    args = parser.parse_args()

    # Load existing index
    index_path = find_index_file()
    if not index_path:
        print("❌ Cannot find maya_company_index.csv")
        print("   Place it next to this script or in the current directory.")
        sys.exit(1)

    print(f"Loading: {index_path}")
    companies = load_index(index_path)
    original_count = len(companies)
    original_seeds = sum(1 for c in companies.values() if c.get("SeedReportId"))
    print(f"  {original_count} companies, {original_seeds} with seed IDs ({original_seeds/original_count*100:.0f}%)")

    # Step 1: New companies
    if not args.fill_seeds_only:
        scan_min = args.scan_range[0] if args.scan_range else SCAN_MIN_ID
        scan_max = args.scan_range[1] if args.scan_range else SCAN_MAX_ID
        print(f"\nStep 1: Scanning for new companies (IDs {scan_min}-{scan_max})...")
        new_companies = scan_for_new_companies(set(companies.keys()), scan_min, scan_max)

        if new_companies:
            print(f"\n  ✓ Found {len(new_companies)} new companies:")
            for c in new_companies[:15]:
                print(f"    {c['CompanyId']}: {c['NameHE']}")
            if len(new_companies) > 15:
                print(f"    ... and {len(new_companies) - 15} more")
            for c in new_companies:
                companies[c["CompanyId"]] = c
        else:
            print("  ✓ No new companies found.")
    else:
        print("\nStep 1: Skipped (--fill-seeds-only)")

    # Step 2: Seed IDs
    missing = sum(1 for c in companies.values() if not c.get("SeedReportId"))
    print(f"\nStep 2: Finding seed report IDs ({missing} missing)...")

    seed_found = 0
    suspected = []
    still_missing = 0

    if missing > 0:
        seed_found, suspected = find_missing_seeds(companies)
        still_missing = sum(1 for c in companies.values() if not c.get("SeedReportId"))
        print(f"\n  ✓ Found {seed_found} exact seeds. Still missing: {still_missing}")

        if suspected:
            print(f"\n  ⚠ Suspected fuzzy matches (NOT auto-assigned — review manually):")
            for s in suspected:
                print(f"    ID {s['company_id']} \"{s['company_name']}\"")
                print(f"      → might match ID {s['matched_id']} \"{s['matched_name']}\" (seed {s['suggested_seed']})")

        if still_missing > 0 and still_missing <= 30:
            print(f"\n  Companies still without seeds:")
            for c in companies.values():
                if not c.get("SeedReportId"):
                    print(f"    {c['CompanyId']}: {c['NameHE']} / {c['NameEN']}")
    else:
        print("  ✓ All companies have seed IDs.")

    # Summary
    final_count = len(companies)
    final_seeds = sum(1 for c in companies.values() if c.get("SeedReportId"))

    print(f"\n{'═' * 60}")
    print(f"  Results")
    print(f"{'═' * 60}")
    print(f"  Companies:  {original_count} → {final_count} (+{final_count - original_count})")
    print(f"  With seeds: {original_seeds} → {final_seeds} (+{final_seeds - original_seeds})")
    print(f"  Exact found:{seed_found:>4}")
    print(f"  Suspected:  {len(suspected):>4}")
    print(f"  Missing:    {still_missing:>4}")
    print(f"  Coverage:   {final_seeds}/{final_count} ({final_seeds/final_count*100:.0f}%)")

    # Save
    if args.dry_run:
        print(f"\n  [DRY RUN] No changes saved.")
    else:
        output_path = args.output or index_path
        save_index(companies, output_path)
        print(f"\n  ✓ Saved to: {output_path}")
        print(f"    {datetime.now().strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()
