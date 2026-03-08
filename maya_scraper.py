#!/usr/bin/env python3
"""
Maya TASE Scraper — Retrieves financial data and reports for Israeli public companies.

Usage:
  python3 maya_scraper.py אורון                   # Search by Hebrew name
  python3 maya_scraper.py "oron group"            # Search by English name
  python3 maya_scraper.py 1644                    # By company ID
  python3 maya_scraper.py אורון --json            # Output as JSON
  python3 maya_scraper.py אורון --download        # Download report PDFs
  python3 maya_scraper.py אורון --years 3         # Only last 3 years
  python3 maya_scraper.py אורון --financials-only # Just the financial table
  python3 maya_scraper.py --search רכבת           # Search without running

API Endpoints used (no authentication required):
  GET  /api/v1/companies/{id}/details     — Company profile
  GET  /api/v1/companies/{id}/financials  — Structured financial data
  GET  /api/v1/reports/{id}               — Report details + PDF links
  GET  /api/v1/reports/{id}/siblings      — Paginated report listing (max 30 per page)
"""

import json
import subprocess
import sys
import os
import csv
import argparse
from datetime import datetime, timedelta, timezone

BASE_URL = "https://maya.tase.co.il/api/v1"
FILES_URL = "https://mayafiles.tase.co.il"
CURL_HEADERS = [
    '-H', 'Accept: application/json',
    '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
]
MAX_SIBLINGS_PER_PAGE = 30
MAX_SIBLINGS_PAGES = 30

# ─── Company Index ────────────────────────────────────────────────────────────

def _find_index_file():
    """Find the company index CSV file."""
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "maya_company_index.csv"),
        os.path.join(os.getcwd(), "maya_company_index.csv"),
        "/mnt/user-data/uploads/maya_company_index.csv",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None

def load_company_index():
    """Load the company index CSV into a list of dicts. Skips malformed rows."""
    path = _find_index_file()
    if not path:
        return []
    companies = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                seed = row.get("SeedReportId", "").strip()
                companies.append({
                    "id": int(row["CompanyId"]),
                    "nameEN": row.get("NameEN", "").strip(),
                    "nameHE": row.get("NameHE", "").strip(),
                    "securityId": row.get("SubId", "").strip(),
                    "seedReportId": int(seed) if seed else None,
                })
            except (KeyError, ValueError):
                continue
    return companies

def search_company(query, companies):
    """Search for a company by name (Hebrew or English). Returns list of matches."""
    q = query.strip()
    q_lower = q.lower()
    
    exact = [c for c in companies if 
             c["nameHE"] == q or c["nameEN"].lower() == q_lower]
    if exact:
        return exact
    
    partial = [c for c in companies if 
               q in c["nameHE"] or q_lower in c["nameEN"].lower()]
    return partial

def resolve_company(query):
    """
    Resolve a company query (name or ID) to (company_id, seed_report_id).
    Returns tuple (int, int|None) or (None, None).
    """
    try:
        cid = int(query)
        companies = load_company_index()
        match = [c for c in companies if c["id"] == cid]
        seed = match[0]["seedReportId"] if match else None
        return cid, seed
    except ValueError:
        pass
    
    companies = load_company_index()
    if not companies:
        print("⚠ Company index file (maya_company_index.csv) not found.", file=sys.stderr)
        print("  Use a numeric company ID instead.", file=sys.stderr)
        return None, None
    
    matches = search_company(query, companies)
    
    if len(matches) == 0:
        print(f"❌ No company found matching '{query}'", file=sys.stderr)
        return None, None
    
    if len(matches) == 1:
        m = matches[0]
        print(f"✓ Found: {m['nameHE']} / {m['nameEN']} (ID: {m['id']})")
        return m["id"], m["seedReportId"]
    
    print(f"Found {len(matches)} matches for '{query}':")
    for i, m in enumerate(matches[:15]):
        marker = "→" if i == 0 else " "
        print(f"  {marker} {m['id']}: {m['nameHE']} / {m['nameEN']}")
    if len(matches) > 15:
        print(f"  ... and {len(matches) - 15} more")
    
    print(f"\nUsing first match: {matches[0]['nameHE']} (ID: {matches[0]['id']})")
    return matches[0]["id"], matches[0]["seedReportId"]

# ─── HTTP ─────────────────────────────────────────────────────────────────────

def fetch(url):
    """Fetch JSON from URL using curl (most reliable for Maya API)."""
    r = subprocess.run(
        ['curl', '-s', '--fail', '--max-time', '15'] + CURL_HEADERS + [url],
        capture_output=True, text=True, timeout=20
    )
    if r.returncode != 0:
        err = r.stderr.strip()
        raise ConnectionError(
            f"HTTP request failed for {url} (curl exit code {r.returncode})"
            + (f": {err}" if err else "")
        )
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON from {url}: {r.stdout[:200]}")

def download_file(url, dest_path):
    """Download a file using curl. Returns (success, error_message)."""
    r = subprocess.run(
        ['curl', '-s', '--fail', '-L', '--max-time', '60',
         '-H', 'User-Agent: Mozilla/5.0',
         '-o', dest_path, url],
        capture_output=True, text=True, timeout=90
    )
    if r.returncode != 0:
        return False, r.stderr
    return True, None

# ─── API FUNCTIONS ────────────────────────────────────────────────────────────

def get_company_details(company_id):
    """Get company profile: name, sector, address, market cap, etc."""
    d = fetch(f"{BASE_URL}/companies/{company_id}/details")
    
    address_parts = [d.get("address", "").strip(), d.get("city", "").strip()]
    address = ", ".join([p for p in address_parts if p])
    
    return {
        "id": d.get("companyId"),
        "name": d.get("name", ""),
        "longName": d.get("longName", ""),
        "corporateNo": d.get("corporateNo", ""),
        "sector": d.get("sector", ""),
        "branch": d.get("branch", ""),
        "subBranch": (d.get("subBranch") or "").strip(),
        "address": address,
        "website": d.get("site", ""),
        "email": d.get("email", ""),
        "phone": d.get("phone", ""),
        "marketValue": d.get("marketValue"),
        "description": d.get("companyActivites", ""),
        "indices": d.get("indices", []),
        "mainSecurityId": d.get("mainSecurityId"),
        "isDual": d.get("isDual", False),
    }

def _parse_financial_value(v):
    """Parse a financial value string into a number. Returns None for missing data."""
    if not v or v == '---':
        return None
    try:
        return float(v.replace('%', '').replace(',', '').strip())
    except (ValueError, AttributeError):
        return v

def _safe_int(v, default=0):
    """Safely convert a value to int. Returns default if conversion fails."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return default

def get_financials(company_id):
    """Get structured financial data (balance sheet + P&L summary)."""
    data = fetch(f"{BASE_URL}/companies/{company_id}/financials")
    
    h = data.get("headline")
    if not h:
        raise ValueError("Unexpected financials schema from Maya API: missing 'headline'")
    
    items = []
    for f in data.get("financials", []):
        items.append({
            "code": f.get("fieldCode", ""),
            "name": f.get("fieldName", ""),
            "current": _parse_financial_value(f.get("valueCurrPeriod")),
            "previous": _parse_financial_value(f.get("valuePrevPeriod")),
            "previousYear": _parse_financial_value(f.get("valuePrevYear")),
            "grid": _safe_int(f.get("gridNumber"), 0)
        })
    
    return {
        "company": h.get("companyShortName", ""),
        "currency": h.get("currencyName", ""),
        "periods": {
            "current": h.get("currPeriodHeadLine", ""),
            "previous": h.get("prevPeriodHeadLine", ""),
            "previousYear": h.get("prevYearHeadLine", "")
        },
        "items": items
    }

def get_all_reports(company_id, seed_report_id=None):
    """
    Get all reports for a company via the siblings endpoint.
    Requires a seed_report_id (any known report for this company).
    If not provided, tries a best-effort scan (slow, unreliable).
    """
    if not seed_report_id:
        seed_report_id = _find_seed_report(company_id)
        if not seed_report_id:
            raise LookupError(
                f"No seed report ID for company {company_id}. "
                f"Provide --seed-report-id or update the CSV index."
            )
    
    all_reports = []
    seen_ids = set()
    
    for offset in range(0, MAX_SIBLINGS_PAGES * MAX_SIBLINGS_PER_PAGE, MAX_SIBLINGS_PER_PAGE):
        batch = fetch(f"{BASE_URL}/reports/{seed_report_id}/siblings?offset={offset}&limit={MAX_SIBLINGS_PER_PAGE}")
        if isinstance(batch, list):
            for r in batch:
                rid = r.get("id")
                if rid and rid not in seen_ids:
                    # Include if company matches OR if company field is missing (permissive)
                    company_info = r.get("company")
                    if company_info is None or company_info.get("companyId") == int(company_id):
                        seen_ids.add(rid)
                        all_reports.append(r)
            if len(batch) < MAX_SIBLINGS_PER_PAGE:
                break
        elif isinstance(batch, dict) and 'errors' in batch:
            break
        else:
            break
    
    return all_reports

def _find_seed_report(company_id):
    """
    Best-effort fallback: scan recent report IDs to find one belonging to this company.
    This is slow and unreliable — prefer using SeedReportId from the CSV index.
    """
    company_id = int(company_id)
    for start in range(1726000, 1700000, -100):
        try:
            data = fetch(f"{BASE_URL}/reports/{start}")
            if data.get("reporterId") == company_id or \
               any(c.get("companyId") == company_id for c in data.get("companies", [])):
                return start
        except (ConnectionError, ValueError, KeyError):
            continue
    
    for start in range(1700000, 1600000, -500):
        try:
            data = fetch(f"{BASE_URL}/reports/{start}")
            if data.get("reporterId") == company_id or \
               any(c.get("companyId") == company_id for c in data.get("companies", [])):
                return start
        except (ConnectionError, ValueError, KeyError):
            continue
    
    return None

def _parse_report_date(pub_date):
    """Parse a Maya report date string into a timezone-aware UTC datetime."""
    if not pub_date:
        return None
    try:
        clean = pub_date.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None

def get_financial_reports(company_id, years=5, seed_report_id=None):
    """Get financial reports (annual + quarterly) with PDF links."""
    all_reports = get_all_reports(company_id, seed_report_id)
    
    fin_keywords = ['דוח שנתי', 'דוח רבעון', 'דוח תקופתי', 'מצגת שוק הון']
    cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365)
    
    financial = []
    for r in all_reports:
        title = r.get("title", "")
        if not any(kw in title for kw in fin_keywords):
            continue
        dt = _parse_report_date(r.get("publishDate", ""))
        if dt is None or dt >= cutoff:
            financial.append(r)
    
    detailed = []
    for r in financial:
        try:
            full = fetch(f"{BASE_URL}/reports/{r['id']}")
            attachments = []
            for a in full.get("attachments", []):
                attachments.append({
                    "type": a.get("fileType", "unknown"),
                    "fileName": a.get("fileName"),
                    "fileSize": a.get("fileSize", 0),
                    "url": f"{FILES_URL}/{a.get('url', '')}"
                })
            detailed.append({
                "id": full.get("id", r["id"]),
                "title": full.get("title", r.get("title", "")),
                "publishDate": full.get("publishDate", ""),
                "formId": full.get("formId", ""),
                "attachments": attachments
            })
        except (ConnectionError, ValueError) as e:
            detailed.append({"id": r["id"], "title": r.get("title", ""), "error": str(e)})
    
    return detailed

def download_from_reports(reports, output_dir):
    """Download PDFs from an already-fetched reports list. No duplicate API calls."""
    os.makedirs(output_dir, exist_ok=True)
    
    downloaded = []
    for r in reports:
        for a in r.get("attachments", []):
            if "pdf" not in a.get("type", ""):
                continue
            
            original_name = a.get("fileName") or "report.pdf"
            safe_name = f"{r['id']}_{original_name}"
            safe_name = "".join(c if c.isalnum() or c in '._-' else '_' for c in safe_name)
            dest = os.path.join(output_dir, safe_name)
            
            success, err = download_file(a["url"], dest)
            if success:
                downloaded.append({"report": r.get("title", ""), "file": dest, "size": a.get("fileSize", 0)})
                print(f"  ✓ {safe_name} ({a.get('fileSize', 0)} KB)")
            else:
                print(f"  ✗ Failed: {safe_name} — {err or 'unknown error'}")
    
    return downloaded

# ─── FORMATTING ───────────────────────────────────────────────────────────────

def print_details(details):
    print(f"\n{'═' * 80}")
    print(f"  {details.get('longName') or details.get('name', '')}")
    print(f"{'═' * 80}")
    for key, label in [
        ("id", "Company ID"), ("name", "Short Name"), ("corporateNo", "Corp. No"),
        ("sector", "Sector"), ("branch", "Branch"), ("address", "Address"),
        ("website", "Website"), ("phone", "Phone"), ("email", "Email"),
    ]:
        print(f"  {label:<16} {details.get(key, 'N/A')}")
    mv = details.get("marketValue")
    print(f"  {'Market Value':<16} {mv:,} K NIS" if mv else f"  {'Market Value':<16} N/A")
    print(f"  {'Dual Listed':<16} {'Yes' if details.get('isDual') else 'No'}")
    indices = details.get('indices', [])
    print(f"  {'Indices':<16} {', '.join(indices) if indices else 'N/A'}")
    print(f"  {'Description':<16} {details.get('description', 'N/A')}")

def print_financials(financials):
    print(f"\n{'═' * 80}")
    print(f"  {financials['company']} — Financial Summary ({financials['currency']})")
    print(f"{'═' * 80}")
    
    p = financials['periods']
    c1, c2 = 35, 15
    print(f"\n  {'Field':<{c1}} {p['current']:>{c2}} {p['previous']:>{c2}} {p['previousYear']:>{c2}}")
    print(f"  {'─' * (c1 + c2 * 3)}")
    
    last_grid = 0
    for item in financials['items']:
        if item['grid'] != last_grid:
            print()
            last_grid = item['grid']
        
        def fmt(v):
            if v is None: return f"{'—':>{c2}}"
            if isinstance(v, str): return f"{v:>{c2}}"
            return f"{v:>{c2},.0f}"
        
        print(f"  {item['name']:<{c1}} {fmt(item['current'])} {fmt(item['previous'])} {fmt(item['previousYear'])}")

def print_reports(reports, company_name=""):
    print(f"\n{'═' * 80}")
    print(f"  {company_name} — Financial Reports ({len(reports)} found)")
    print(f"{'═' * 80}")
    
    for r in reports:
        date = r.get("publishDate", "")[:10]
        print(f"\n  [{r['id']}] {date} — {r['title']}")
        if r.get("formId"):
            print(f"  Form: {r['formId']}")
        if r.get("error"):
            print(f"  ⚠ Error: {r['error']}")
        for a in r.get("attachments", []):
            icon = "📄" if "pdf" in a.get("type", "") else "📎"
            name = a.get("fileName")
            size = a.get("fileSize", 0)
            size_str = f"{size:,} KB" if size < 1024 else f"{size/1024:.1f} MB"
            if name:
                print(f"    {icon} {a['type']}: {name}")
                print(f"       {a['url']} ({size_str})")
            else:
                print(f"    {icon} {a['type']}: {a['url']} ({size_str})")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Maya TASE Scraper — Israeli public company data")
    parser.add_argument("company", help="Company name (Hebrew/English) or Maya company ID")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--download", action="store_true", help="Download report PDFs")
    parser.add_argument("--years", type=int, default=5, help="Number of years to look back (default: 5)")
    parser.add_argument("--financials-only", action="store_true", help="Only show financials table")
    parser.add_argument("--reports-only", action="store_true", help="Only show reports list")
    parser.add_argument("--details-only", action="store_true", help="Only show company details")
    parser.add_argument("--output", default=None, help="Directory for downloaded PDFs")
    parser.add_argument("--seed-report-id", type=int, default=None, help="Known report ID for this company")
    parser.add_argument("--search", action="store_true", help="Just search for company, don't fetch data")
    
    args = parser.parse_args()
    
    # Search-only mode
    if args.search:
        companies = load_company_index()
        if not companies:
            print("⚠ Company index file not found. Cannot search by name.")
            return
        matches = search_company(args.company, companies)
        if not matches:
            print(f"No matches for '{args.company}'")
        else:
            print(f"Found {len(matches)} match(es):")
            for m in matches:
                seed = f", Seed: {m['seedReportId']}" if m.get('seedReportId') else ""
                print(f"  {m['id']}: {m['nameHE']} / {m['nameEN']}{seed}")
        return
    
    # Resolve company name to ID
    cid, csv_seed = resolve_company(args.company)
    if cid is None:
        sys.exit(1)
    
    seed_report_id = args.seed_report_id or csv_seed
    output = {}
    reports = None  # Shared between reports listing and download — no double fetch
    
    show_all = not (args.financials_only or args.reports_only or args.details_only)
    
    try:
        if show_all or args.details_only:
            details = get_company_details(cid)
            output["details"] = details
            if not args.json:
                print_details(details)
        
        if show_all or args.financials_only:
            financials = get_financials(cid)
            output["financials"] = financials
            if not args.json:
                print_financials(financials)
        
        if show_all or args.reports_only or args.download:
            if not args.json:
                print(f"\nFetching reports (last {args.years} years)...")
            reports = get_financial_reports(cid, args.years, seed_report_id)
            output["reports"] = reports
            if not args.json:
                company_name = output.get("details", {}).get("name", "")
                print_reports(reports, company_name)
        
        if args.download and reports is not None:
            out_dir = args.output or f"maya_reports_{cid}"
            if not args.json:
                print(f"\nDownloading PDFs to {out_dir}/...")
            downloaded = download_from_reports(reports, out_dir)
            output["downloaded"] = downloaded
            if not args.json:
                print(f"\n✓ Downloaded {len(downloaded)} files to {out_dir}/")
        
        if args.json:
            print(json.dumps(output, ensure_ascii=False, indent=2))
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
