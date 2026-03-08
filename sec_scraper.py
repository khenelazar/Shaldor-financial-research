#!/usr/bin/env python3
"""
SEC EDGAR Scraper — Retrieves financial data and filings for US public companies.

Usage:
  python3 sec_scraper.py AAPL                      # Full report by ticker
  python3 sec_scraper.py "Apple Inc"                # Search by company name
  python3 sec_scraper.py 320193                     # By CIK number
  python3 sec_scraper.py AAPL --json                # Output as JSON
  python3 sec_scraper.py AAPL --download            # Download filing documents
  python3 sec_scraper.py AAPL --years 3             # Only last 3 years
  python3 sec_scraper.py AAPL --financials-only     # Just the financial tables
  python3 sec_scraper.py AAPL --filings-only        # Just the filing list
  python3 sec_scraper.py --search biotech           # Search without running

Data sources (no authentication required):
  SEC EDGAR APIs:
    GET  https://www.sec.gov/files/company_tickers.json       — Ticker→CIK lookup
    GET  https://data.sec.gov/submissions/CIK{cik}.json       — Company profile + filings
    GET  https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json — XBRL financial data

  yfinance (optional, if installed):
    Used as fallback/supplement for financial data if XBRL coverage is sparse.

SEC EDGAR requires a User-Agent header with name and email. Set SEC_USER_AGENT env var
or it defaults to "CompanyResearch research@example.com".

Rate limit: EDGAR allows ~10 requests/second. Built-in 0.12s delay between requests.
"""

from __future__ import annotations

import json
import subprocess
import sys
import os
import random
import time
import argparse
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

# ─── Configuration ────────────────────────────────────────────────────────────

SEC_BASE = "https://data.sec.gov"
EDGAR_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "CompanyResearch research@example.com"
)

CURL_HEADERS = [
    '-H', 'Accept: application/json',
    '-H', f'User-Agent: {USER_AGENT}',
]

# Rate limiter: EDGAR allows ~10 req/s — we stay well under
_last_request_time = 0.0
REQUEST_DELAY = 0.12  # seconds between requests

# Retry configuration
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0   # seconds — doubles each retry
RETRY_JITTER = 0.3        # random ±30% on delay
RETRYABLE_CURL_CODES = {
    7,   # Failed to connect
    18,  # Partial file
    28,  # Timeout
    52,  # Empty reply
    56,  # Recv failure
}

# Filings pagination safety
MAX_FILING_PAGES = 10

# ─── XBRL Tag Definitions ────────────────────────────────────────────────────
# Organized by financial statement.  Each entry: (tag_list, display_name, is_per_share)
# The scraper tries tags in order and uses the first one with data.

INCOME_STATEMENT_TAGS = [
    (["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues",
      "SalesRevenueNet", "RevenueFromContractWithCustomerIncludingAssessedTax"],
     "Revenue", False),
    (["CostOfGoodsAndServicesSold", "CostOfRevenue", "CostOfGoodsSold"],
     "Cost of Revenue", False),
    (["GrossProfit"], "Gross Profit", False),
    (["ResearchAndDevelopmentExpense"], "R&D Expense", False),
    (["SellingGeneralAndAdministrativeExpense"], "SG&A Expense", False),
    (["OperatingExpenses"], "Operating Expenses", False),
    (["OperatingIncomeLoss"], "Operating Income", False),
    (["InterestExpense", "InterestExpenseDebt"], "Interest Expense", False),
    (["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
      "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"],
     "Pre-tax Income", False),
    (["IncomeTaxExpenseBenefit"], "Income Tax", False),
    (["NetIncomeLoss", "ProfitLoss"], "Net Income", False),
    (["EarningsPerShareBasic"], "EPS (Basic)", True),
    (["EarningsPerShareDiluted"], "EPS (Diluted)", True),
]

BALANCE_SHEET_TAGS = [
    (["CashAndCashEquivalentsAtCarryingValue"], "Cash & Equivalents", False),
    (["ShortTermInvestments", "MarketableSecuritiesCurrent"], "Short-term Investments", False),
    (["AccountsReceivableNetCurrent"], "Accounts Receivable", False),
    (["InventoryNet", "Inventories"], "Inventories", False),
    (["AssetsCurrent"], "Total Current Assets", False),
    (["PropertyPlantAndEquipmentNet"], "PP&E (Net)", False),
    (["Goodwill"], "Goodwill", False),
    (["Assets"], "Total Assets", False),
    (["AccountsPayableCurrent"], "Accounts Payable", False),
    (["LongTermDebtCurrent", "LongTermDebtAndCapitalLeaseObligationsCurrent"],
     "Current Portion of LT Debt", False),
    (["LiabilitiesCurrent"], "Total Current Liabilities", False),
    (["LongTermDebtNoncurrent", "LongTermDebt"], "Long-term Debt", False),
    (["Liabilities"], "Total Liabilities", False),
    (["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
     "Total Equity", False),
    (["LiabilitiesAndStockholdersEquity"], "Total Liabilities & Equity", False),
]

CASH_FLOW_TAGS = [
    (["NetCashProvidedByUsedInOperatingActivities",
      "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"],
     "Operating Cash Flow", False),
    (["PaymentsToAcquirePropertyPlantAndEquipment"], "Capital Expenditures", False),
    (["NetCashProvidedByUsedInInvestingActivities",
      "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations"],
     "Investing Cash Flow", False),
    (["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"], "Dividends Paid", False),
    (["PaymentsForRepurchaseOfCommonStock"], "Share Repurchases", False),
    (["NetCashProvidedByUsedInFinancingActivities",
      "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations"],
     "Financing Cash Flow", False),
]

# Filing types we care about
FINANCIAL_FORMS = {"10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A", "20-F", "20-F/A"}

# ─── HTTP ─────────────────────────────────────────────────────────────────────

def _rate_limit() -> None:
    """Enforce rate limiting between requests."""
    global _last_request_time
    now = time.monotonic()
    elapsed = now - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.monotonic()


def _retry_delay(attempt: int) -> float:
    """Calculate retry delay with exponential backoff + jitter."""
    base = RETRY_BASE_DELAY * (2 ** attempt)
    jitter = base * RETRY_JITTER * (2 * random.random() - 1)
    return max(0.1, base + jitter)


def fetch(url: str) -> Optional[dict]:
    """
    Fetch JSON from URL using curl with retry + exponential backoff.
    Returns parsed JSON or None on failure.

    Note: curl is invoked WITHOUT --fail so we can read the HTTP status code
    via -w and handle 429 vs 404 differently for retry logic.  This is
    intentional — --fail would mask the status code distinction.

    Retries on: timeouts, connection failures, HTTP 429/5xx.
    Does NOT retry on: 4xx (except 429), JSON parse errors.
    """
    last_error = ""
    for attempt in range(MAX_RETRIES):
        try:
            _rate_limit()
            r = subprocess.run(
                ['curl', '-s', '-w', '\n%{http_code}', '--max-time', '20']
                + CURL_HEADERS + [url],
                capture_output=True, text=True, timeout=25
            )
            # Separate body from HTTP status code (written by -w)
            parts = r.stdout.rsplit('\n', 1)
            body = parts[0] if len(parts) == 2 else r.stdout
            status_str = parts[1].strip() if len(parts) == 2 else ""
            http_code = int(status_str) if status_str.isdigit() else 0

            # ── Retryable HTTP errors ──
            if http_code == 429 or http_code >= 500:
                last_error = f"HTTP {http_code}"
                if attempt < MAX_RETRIES - 1:
                    delay = _retry_delay(attempt)
                    if http_code == 429:
                        delay = max(delay, 2.0)  # Extra patience for rate limits
                    time.sleep(delay)
                    continue
                print(f"  ⚠ {last_error} for {url} after {MAX_RETRIES} tries", file=sys.stderr)
                return None

            # ── Retryable curl-level errors ──
            if r.returncode in RETRYABLE_CURL_CODES:
                last_error = f"curl code {r.returncode}"
                if attempt < MAX_RETRIES - 1:
                    time.sleep(_retry_delay(attempt))
                    continue
                print(f"  ⚠ {last_error} for {url} after {MAX_RETRIES} tries", file=sys.stderr)
                return None

            # ── Non-retryable errors ──
            if r.returncode != 0:
                print(f"  ⚠ curl error {r.returncode} for {url}", file=sys.stderr)
                return None
            if http_code and (400 <= http_code < 500):
                print(f"  ⚠ HTTP {http_code} for {url}", file=sys.stderr)
                return None

            # ── Success — parse JSON ──
            if not body.strip():
                print(f"  ⚠ Empty response from {url}", file=sys.stderr)
                return None
            return json.loads(body)

        except subprocess.TimeoutExpired:
            last_error = "timeout"
            if attempt < MAX_RETRIES - 1:
                time.sleep(_retry_delay(attempt))
                continue
            print(f"  ⚠ Timeout for {url} after {MAX_RETRIES} tries", file=sys.stderr)
            return None
        except json.JSONDecodeError:
            # JSON errors are not retryable (the server returned garbage)
            print(f"  ⚠ Invalid JSON from {url}", file=sys.stderr)
            return None

    return None


def download_file(url: str, dest_path: str) -> tuple[bool, Optional[str]]:
    """Download a file using curl with retry + backoff. Returns (success, error)."""
    last_error = ""
    for attempt in range(MAX_RETRIES):
        try:
            _rate_limit()
            r = subprocess.run(
                ['curl', '-s', '--fail', '-L', '--max-time', '60',
                 '-H', f'User-Agent: {USER_AGENT}',
                 '-o', dest_path, url],
                capture_output=True, text=True, timeout=90
            )
            if r.returncode == 0:
                return True, None

            last_error = r.stderr.strip() or f"curl code {r.returncode}"
            if r.returncode in RETRYABLE_CURL_CODES and attempt < MAX_RETRIES - 1:
                time.sleep(_retry_delay(attempt))
                continue
            return False, last_error

        except subprocess.TimeoutExpired:
            last_error = "timeout"
            if attempt < MAX_RETRIES - 1:
                time.sleep(_retry_delay(attempt))
                continue
            return False, f"timeout after {MAX_RETRIES} attempts"

    return False, last_error


# ─── Company Resolution ──────────────────────────────────────────────────────

_ticker_cache: Optional[list[dict[str, Any]]] = None


def _load_tickers() -> list[dict[str, Any]]:
    """Load SEC ticker→CIK mapping. Cached after first call."""
    global _ticker_cache
    if _ticker_cache is not None:
        return _ticker_cache

    data = fetch(TICKERS_URL)
    if not data:
        print("⚠ Could not load SEC ticker index.", file=sys.stderr)
        _ticker_cache = []
        return _ticker_cache

    entries: list[dict[str, Any]] = []
    for v in data.values():
        entries.append({
            "cik": int(v.get("cik_str", 0)),
            "ticker": v.get("ticker", "").upper(),
            "name": v.get("title", ""),
        })
    _ticker_cache = entries
    return _ticker_cache


def search_company(query: str) -> list[dict[str, Any]]:
    """
    Search for a company by ticker, name, or CIK.
    Returns list of matches sorted by relevance:
      1. Exact CIK  2. Exact ticker  3. Exact name  4. Partial name/ticker
    """
    tickers = _load_tickers()
    q = query.strip()
    q_upper = q.upper()
    q_lower = q.lower()

    # Try CIK number
    try:
        cik = int(q)
        return [t for t in tickers if t["cik"] == cik]
    except ValueError:
        pass

    # Exact ticker match
    exact_ticker = [t for t in tickers if t["ticker"] == q_upper]
    if exact_ticker:
        return exact_ticker

    # Exact name match (case-insensitive)
    exact_name = [t for t in tickers if t["name"].lower() == q_lower]
    if exact_name:
        return exact_name

    # Partial name match
    partial = [t for t in tickers if q_lower in t["name"].lower()]

    # Also try partial ticker match if query is short
    if len(q) <= 5:
        ticker_partial = [t for t in tickers if q_upper in t["ticker"]]
        seen = {t["cik"] for t in partial}
        for t in ticker_partial:
            if t["cik"] not in seen:
                partial.append(t)
                seen.add(t["cik"])

    return partial


def resolve_company(query: str) -> tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Resolve a query to a CIK number.
    Returns (cik, ticker, name) or (None, None, None).
    """
    matches = search_company(query)

    if len(matches) == 0:
        print(f"❌ No company found matching '{query}'", file=sys.stderr)
        return None, None, None

    if len(matches) == 1:
        m = matches[0]
        print(f"✓ Found: {m['name']} ({m['ticker']}) — CIK {m['cik']}", file=sys.stderr)
        return m["cik"], m["ticker"], m["name"]

    # Multiple matches
    print(f"Found {len(matches)} matches for '{query}':", file=sys.stderr)
    for i, m in enumerate(matches[:15]):
        marker = "→" if i == 0 else " "
        ticker_str = f" ({m['ticker']})" if m['ticker'] else ""
        print(f"  {marker} CIK {m['cik']}: {m['name']}{ticker_str}", file=sys.stderr)
    if len(matches) > 15:
        print(f"  ... and {len(matches) - 15} more", file=sys.stderr)

    best = matches[0]
    print(f"\nUsing first match: {best['name']} (CIK: {best['cik']})", file=sys.stderr)
    return best["cik"], best["ticker"], best["name"]


def _pad_cik(cik: int) -> str:
    """Pad CIK to 10 digits with leading zeros (EDGAR format)."""
    return str(cik).zfill(10)


# ─── Company Details ──────────────────────────────────────────────────────────

def get_company_details(cik: int) -> Optional[dict[str, Any]]:
    """Get company profile from the submissions endpoint."""
    data = fetch(f"{SEC_BASE}/submissions/CIK{_pad_cik(cik)}.json")
    if not data:
        return None

    # Build address — normalize None/null fields
    biz = data.get("addresses", {}).get("business", {}) or {}
    address_parts = [
        (biz.get("street1") or "").strip(),
        (biz.get("street2") or "").strip(),
        (biz.get("city") or "").strip(),
        (biz.get("stateOrCountry") or "").strip(),
        (biz.get("zipCode") or "").strip(),
    ]
    address = ", ".join([p for p in address_parts if p])

    return {
        "cik": data.get("cik", ""),
        "name": data.get("name", ""),
        "entityType": data.get("entityType", ""),
        "tickers": data.get("tickers", []),
        "exchanges": data.get("exchanges", []),
        "sic": data.get("sic", ""),
        "sicDescription": data.get("sicDescription", ""),
        "category": data.get("category", ""),
        "fiscalYearEnd": data.get("fiscalYearEnd", ""),
        "stateOfIncorporation": data.get("stateOfIncorporation", ""),
        "ein": data.get("ein", ""),
        "phone": (data.get("phone") or "").strip(),
        "address": address,
        "website": (data.get("website") or "").strip(),
        "formerNames": data.get("formerNames", []),
        "_submissions": data,
    }


# ─── Financial Data (XBRL) ───────────────────────────────────────────────────

def _extract_facts(
    facts_data: dict[str, Any],
    tag_list: list[str],
    unit: str = "USD",
) -> list[dict[str, Any]]:
    """
    Given a taxonomy facts dict (us-gaap or ifrs-full), try each tag in
    tag_list (first match wins). Returns deduplicated fact entries.

    Deduplication:
      1. Only entries with 'frame' attribute (SEC's canonical dedup).
      2. For duplicate frames: keep the most recently filed entry (handles amendments).
    """
    if not facts_data:
        return []

    for tag in tag_list:
        if tag not in facts_data:
            continue

        units = facts_data[tag].get("units", {})
        entries = units.get(unit, [])

        # Try alternative unit spellings (NOT cross-currency — that would
        # silently mix currencies without telling the user)
        if not entries:
            if unit == "USD/shares":
                for alt in ["USD/shares", "USD / shares"]:
                    if alt in units:
                        entries = units[alt]
                        break
            # No cross-currency fallback: showing EUR values labeled as USD
            # would be silently wrong.  If the requested unit isn't available,
            # this tag simply has no data for this company.

        # Filter to entries with frame attribute
        framed = [e for e in entries if e.get("frame")]
        if not framed:
            continue

        # Deduplicate per frame: keep the entry with the latest 'filed' date
        # (handles amended filings replacing earlier values)
        by_frame: dict[str, dict] = {}
        for e in framed:
            frame = e["frame"]
            existing = by_frame.get(frame)
            if existing is None:
                by_frame[frame] = e
            elif (e.get("filed", "") or "") > (existing.get("filed", "") or ""):
                by_frame[frame] = e

        result = sorted(by_frame.values(), key=lambda e: e.get("frame", ""))
        if result:
            return result

    return []


def _classify_frame(frame: str) -> str:
    """
    Classify a frame string into period type.

    Frame formats from SEC:
      CY2023       → annual (full calendar year, flow)
      CY2023Q2     → quarterly (single quarter, flow)
      CY2023Q2I    → quarterly instant (point-in-time, balance sheet)
    """
    if not frame or not frame.startswith("CY"):
        return "unknown"
    suffix = frame[6:]
    if not suffix:
        return "annual"
    if suffix.endswith("I"):
        return "instant"
    if suffix.startswith("Q"):
        return "quarterly"
    return "unknown"


def _split_annual_quarterly(
    entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split fact entries into annual and quarterly (incl. instant) buckets."""
    annual: list[dict] = []
    quarterly: list[dict] = []
    for e in entries:
        cls = _classify_frame(e.get("frame", ""))
        if cls == "annual":
            annual.append(e)
        elif cls in ("quarterly", "instant"):
            quarterly.append(e)
    return annual, quarterly


def _frame_year(frame: str) -> int:
    """Extract year from frame string like 'CY2023' or 'CY2023Q2I'."""
    try:
        return int(frame[2:6])
    except (ValueError, IndexError):
        return 0


def get_financials(cik: int, years: int = 5) -> Optional[dict[str, Any]]:
    """
    Get structured financial data from XBRL Company Facts.
    Supports both US-GAAP and IFRS taxonomies (us-gaap preferred).
    Returns dict with income_statement, balance_sheet, cash_flow or None.
    """
    data = fetch(f"{SEC_BASE}/api/xbrl/companyfacts/CIK{_pad_cik(cik)}.json")
    if not data:
        return None

    facts = data.get("facts", {})
    taxonomy_data = facts.get("us-gaap", {})
    taxonomy_name = "us-gaap"

    # IFRS fallback for foreign filers (20-F filers, ADRs, etc.)
    if not taxonomy_data:
        taxonomy_data = facts.get("ifrs-full", {})
        taxonomy_name = "ifrs-full"
    if not taxonomy_data:
        return None

    # Cutoff using precise date arithmetic (not calendar year approximation)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=years * 365.25)
    cutoff_year = cutoff_date.year

    def build_table(
        tag_defs: list[tuple[list[str], str, bool]],
        unit: str = "USD",
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for tags, label, is_per_share in tag_defs:
            u = "USD/shares" if is_per_share else unit
            entries = _extract_facts(taxonomy_data, tags, u)
            annual, quarterly = _split_annual_quarterly(entries)

            annual = [e for e in annual if _frame_year(e.get("frame", "")) >= cutoff_year]
            quarterly = [e for e in quarterly if _frame_year(e.get("frame", "")) >= cutoff_year]

            if annual or quarterly:
                rows.append({
                    "label": label,
                    "annual": {e["frame"]: e["val"] for e in annual},
                    "quarterly": {e["frame"]: e["val"] for e in quarterly},
                    "is_per_share": is_per_share,
                })
        return rows

    return {
        "entityName": data.get("entityName", ""),
        "taxonomy": taxonomy_name,
        "income_statement": build_table(INCOME_STATEMENT_TAGS),
        "balance_sheet": build_table(BALANCE_SHEET_TAGS),
        "cash_flow": build_table(CASH_FLOW_TAGS),
    }


# ─── yfinance Fallback ───────────────────────────────────────────────────────

def _try_yfinance(ticker: str, years: int = 5) -> Optional[dict[str, Any]]:
    """Attempt to get financial data via yfinance if installed."""
    try:
        import yfinance as yf
    except ImportError:
        return None

    try:
        t = yf.Ticker(ticker)

        def df_to_dict(df: Any) -> dict[str, dict[str, float]]:
            if df is None or df.empty:
                return {}
            result: dict[str, dict[str, float]] = {}
            for col in df.columns:
                period = col.strftime("%Y-%m-%d") if hasattr(col, "strftime") else str(col)
                for idx in df.index:
                    label = str(idx)
                    if label not in result:
                        result[label] = {}
                    val = df.loc[idx, col]
                    if val is not None and str(val) != "nan":
                        result[label][period] = float(val)
            return result

        income = df_to_dict(t.income_stmt)
        balance = df_to_dict(t.balance_sheet)
        cashflow = df_to_dict(t.cashflow)

        if not income and not balance and not cashflow:
            return None

        return {
            "source": "yfinance",
            "income_statement_raw": income,
            "balance_sheet_raw": balance,
            "cash_flow_raw": cashflow,
        }
    except Exception:
        return None


# ─── Filings ─────────────────────────────────────────────────────────────────

def get_filings(
    cik: int,
    years: int = 5,
    forms: Optional[set[str]] = None,
    submissions_data: Optional[dict] = None,
) -> list[dict[str, Any]]:
    """
    Get filing list from the submissions endpoint.
    Respects MAX_FILING_PAGES to prevent runaway pagination.
    """
    if forms is None:
        forms = FINANCIAL_FORMS

    if submissions_data is None:
        data = fetch(f"{SEC_BASE}/submissions/CIK{_pad_cik(cik)}.json")
        if not data:
            return []
    else:
        data = submissions_data

    cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365.25)
    cik_str = _pad_cik(cik)

    # Recent filings (first ~1000 inline in submissions response)
    recent = data.get("filings", {}).get("recent", {})
    filings = _parse_filing_batch(recent, cik_str, cutoff, forms)

    # Older filings (paginated JSON files) — with page limit guard
    older_files = data.get("filings", {}).get("files", [])
    pages_fetched = 0
    for f in older_files:
        if pages_fetched >= MAX_FILING_PAGES:
            break
        fname = f.get("name", "")
        if not fname:
            continue

        batch_data = fetch(f"{SEC_BASE}/submissions/{fname}")
        pages_fetched += 1
        if not batch_data:
            continue

        batch = _parse_filing_batch(batch_data, cik_str, cutoff, forms)
        filings.extend(batch)

        # Early termination: check if the ENTIRE page (not just filtered batch)
        # is before cutoff.  We check raw dates because an empty `batch` might
        # just mean no matching form types on this page, not that we're past cutoff.
        raw_dates = batch_data.get("filingDate", [])
        if raw_dates:
            # If ALL dates on this page are before cutoff, no point fetching more
            all_before = all(
                (_parse_date(d) or datetime.max.replace(tzinfo=timezone.utc)) < cutoff
                for d in raw_dates
            )
            if all_before:
                break

    filings.sort(key=lambda x: x.get("filingDate", ""), reverse=True)
    return filings


def _parse_filing_batch(
    batch: dict[str, Any],
    cik_str: str,
    cutoff: datetime,
    forms: set[str],
) -> list[dict[str, Any]]:
    """Parse a batch of filings from the submissions columnar format."""
    filings: list[dict[str, Any]] = []
    accessions = batch.get("accessionNumber", [])
    n = len(accessions)

    form_col = batch.get("form", [])
    date_col = batch.get("filingDate", [])
    report_col = batch.get("reportDate", [])
    doc_col = batch.get("primaryDocument", [])
    desc_col = batch.get("primaryDocDescription", [])
    items_col = batch.get("items", [])

    for i in range(n):
        form = form_col[i] if i < len(form_col) else ""
        if form not in forms:
            continue

        filing_date = date_col[i] if i < len(date_col) else ""
        dt = _parse_date(filing_date)
        if dt and dt < cutoff:
            continue

        accession = accessions[i].replace("-", "")
        accession_dashed = accessions[i]
        primary_doc = doc_col[i] if i < len(doc_col) else ""
        description = desc_col[i] if i < len(desc_col) else ""
        report_date = report_col[i] if i < len(report_col) else ""

        base_url = f"{EDGAR_ARCHIVES}/{cik_str}/{accession}"
        filing_index = f"{base_url}/{accession_dashed}-index.htm"
        primary_url = f"{base_url}/{primary_doc}" if primary_doc else ""

        # Detect document type from extension
        doc_ext = os.path.splitext(primary_doc)[1].lower() if primary_doc else ""
        doc_type = {
            ".htm": "HTML", ".html": "HTML",
            ".xml": "XML/XBRL", ".xsd": "XSD",
            ".txt": "Text",
        }.get(doc_ext, doc_ext.lstrip('.').upper() or "Unknown")

        filings.append({
            "form": form,
            "filingDate": filing_date,
            "reportDate": report_date,
            "accessionNumber": accessions[i],
            "primaryDocument": primary_doc,
            "primaryDocDescription": description,
            "documentType": doc_type,
            "indexUrl": filing_index,
            "documentUrl": primary_url,
            "items": items_col[i] if i < len(items_col) else "",
        })

    return filings


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse YYYY-MM-DD date string to timezone-aware datetime."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def download_filings(filings: list[dict], output_dir: str) -> list[dict[str, Any]]:
    """Download filing documents. Returns list of downloaded file info."""
    os.makedirs(output_dir, exist_ok=True)
    downloaded: list[dict[str, Any]] = []

    for f in filings:
        url = f.get("documentUrl", "")
        if not url:
            continue

        doc_name = f.get("primaryDocument", "filing")
        safe_name = f"{f['accessionNumber']}_{doc_name}"
        safe_name = "".join(c if c.isalnum() or c in '._-' else '_' for c in safe_name)
        dest = os.path.join(output_dir, safe_name)

        doc_type = f.get("documentType", "")
        success, err = download_file(url, dest)
        if success:
            size_bytes = os.path.getsize(dest)
            # Integrity check: reject empty files and tiny HTML error pages
            if size_bytes == 0:
                os.remove(dest)
                print(f"  ✗ Empty file: {safe_name} — skipped")
                continue
            if size_bytes < 500 and doc_type == "HTML":
                # Might be an SEC error page; warn but keep
                with open(dest, 'r', errors='ignore') as fh:
                    head = fh.read(200).lower()
                if 'error' in head or 'not found' in head:
                    os.remove(dest)
                    print(f"  ✗ Error page: {safe_name} — skipped")
                    continue

            size_kb = size_bytes // 1024
            downloaded.append({
                "form": f.get("form", ""),
                "filingDate": f.get("filingDate", ""),
                "documentType": doc_type,
                "file": dest,
                "size_kb": size_kb,
            })
            print(f"  ✓ {safe_name} [{doc_type}] ({size_kb} KB)")
        else:
            print(f"  ✗ Failed: {safe_name} — {err or 'unknown error'}")

    return downloaded


# ─── Formatting ───────────────────────────────────────────────────────────────

def print_details(details: dict[str, Any]) -> None:
    """Print company profile in formatted layout."""
    if not details:
        print("  No company details available.")
        return

    print(f"\n{'═' * 90}")
    print(f"  {details.get('name', 'Unknown')}")
    print(f"{'═' * 90}")

    tickers = ", ".join(details.get("tickers", [])) or "N/A"
    exchanges = ", ".join(details.get("exchanges", [])) or "N/A"

    for key, label in [
        ("cik", "CIK"),
        (None, "Ticker(s)"),
        (None, "Exchange(s)"),
        ("sic", "SIC Code"),
        ("sicDescription", "Industry"),
        ("category", "Filer Category"),
        ("entityType", "Entity Type"),
        ("stateOfIncorporation", "State of Inc."),
        ("address", "Address"),
        ("phone", "Phone"),
        ("ein", "EIN"),
        ("fiscalYearEnd", "Fiscal Year End"),
    ]:
        if label == "Ticker(s)":
            val = tickers
        elif label == "Exchange(s)":
            val = exchanges
        elif label == "Fiscal Year End":
            raw = details.get("fiscalYearEnd", "")
            if raw and len(raw) == 4:
                val = f"Month {int(raw[:2])}, Day {int(raw[2:])}"
            else:
                val = raw or "N/A"
        else:
            val = details.get(key, "") or "N/A"
        print(f"  {label:<18} {val}")

    former = details.get("formerNames", [])
    if former:
        names = [f"{fn.get('name','')} (until {fn.get('to','')})" for fn in former[:3]]
        print(f"  {'Former Names':<18} {'; '.join(names)}")


def _fmt_val(val: Any, is_per_share: bool = False) -> str:
    """Format a financial value for display."""
    if val is None:
        return "—"
    if is_per_share:
        return f"{val:,.2f}"
    abs_val = abs(val)
    if abs_val >= 1_000_000_000:
        return f"{val / 1_000_000_000:,.1f}B"
    if abs_val >= 1_000_000:
        return f"{val / 1_000_000:,.0f}M"
    elif abs_val >= 1_000:
        return f"{val / 1_000:,.0f}K"
    else:
        return f"{val:,.0f}"


def print_financials(financials: dict[str, Any]) -> None:
    """Print financial statements in aligned table format."""
    if not financials:
        print("  No financial data available.")
        return

    taxonomy = financials.get("taxonomy", "us-gaap")
    taxonomy_label = taxonomy.upper().replace("-", " ")

    print(f"\n{'═' * 90}")
    print(f"  {financials['entityName']} — Financial Data ({taxonomy_label})")
    print(f"{'═' * 90}")

    for section_name, section_key in [
        ("INCOME STATEMENT", "income_statement"),
        ("BALANCE SHEET", "balance_sheet"),
        ("CASH FLOW STATEMENT", "cash_flow"),
    ]:
        rows = financials.get(section_key, [])
        if not rows:
            continue

        annual_periods: set[str] = set()
        for r in rows:
            annual_periods.update(r.get("annual", {}).keys())
        sorted_annual = sorted(annual_periods)[-6:]

        quarterly_periods: set[str] = set()
        for r in rows:
            quarterly_periods.update(r.get("quarterly", {}).keys())
        sorted_quarterly = sorted(quarterly_periods)[-4:]

        print(f"\n  ── {section_name} {'─' * (85 - len(section_name))}")

        if sorted_annual:
            col_w = 14
            label_w = 28
            header = f"  {'Annual':<{label_w}}"
            for p in sorted_annual:
                header += f" {p:>{col_w}}"
            print(header)
            print(f"  {'─' * (label_w + col_w * len(sorted_annual))}")

            for r in rows:
                line = f"  {r['label']:<{label_w}}"
                for p in sorted_annual:
                    val = r["annual"].get(p)
                    line += f" {_fmt_val(val, r['is_per_share']):>{col_w}}"
                print(line)

        if sorted_quarterly:
            print()
            col_w = 14
            label_w = 28
            # Balance sheet items are point-in-time snapshots, not quarterly flows
            sub_header = "Period-End" if section_key == "balance_sheet" else "Quarterly"
            header = f"  {sub_header:<{label_w}}"
            for p in sorted_quarterly:
                header += f" {p:>{col_w}}"
            print(header)
            print(f"  {'─' * (label_w + col_w * len(sorted_quarterly))}")

            for r in rows:
                vals = {p: r["quarterly"].get(p) for p in sorted_quarterly}
                if any(v is not None for v in vals.values()):
                    line = f"  {r['label']:<{label_w}}"
                    for p in sorted_quarterly:
                        line += f" {_fmt_val(vals[p], r['is_per_share']):>{col_w}}"
                    print(line)


def print_filings(filings: list[dict], company_name: str = "") -> None:
    """Print filing list in formatted layout."""
    if not filings:
        print("  No filings found.")
        return

    counts: dict[str, int] = {}
    for f in filings:
        counts[f["form"]] = counts.get(f["form"], 0) + 1
    summary = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))

    print(f"\n{'═' * 90}")
    print(f"  {company_name} — SEC Filings ({len(filings)} found: {summary})")
    print(f"{'═' * 90}")

    for f in filings:
        items_str = ""
        if f.get("items"):
            items_str = f"  Items: {f['items']}"
        doc_type = f.get("documentType", "")
        type_tag = f" [{doc_type}]" if doc_type else ""
        print(f"\n  [{f['form']:<8}] {f['filingDate']}  Period: {f.get('reportDate', 'N/A')}{type_tag}")
        print(f"  {f.get('primaryDocDescription', '')}{items_str}")
        if f.get("documentUrl"):
            print(f"    📄 {f['documentUrl']}")
        print(f"    📋 {f['indexUrl']}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="SEC EDGAR Scraper — US public company financial data & filings"
    )
    parser.add_argument("company", nargs="?", help="Ticker (AAPL), company name, or CIK number")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--download", action="store_true", help="Download filing documents")
    parser.add_argument("--years", type=int, default=5, help="Years to look back (default: 5)")
    parser.add_argument("--financials-only", action="store_true", help="Only show financials")
    parser.add_argument("--filings-only", action="store_true", help="Only show filings list")
    parser.add_argument("--details-only", action="store_true", help="Only show company details")
    parser.add_argument("--output", default=None, help="Directory for downloaded files")
    parser.add_argument("--search", action="store_true", help="Search for company, don't fetch data")
    parser.add_argument("--forms", default=None,
                        help="Comma-separated form types to include (default: 10-K,10-Q,8-K,20-F)")

    args = parser.parse_args()

    if not args.company:
        parser.print_help()
        sys.exit(1)

    form_filter: Optional[set[str]] = None
    if args.forms:
        form_filter = {f.strip() for f in args.forms.split(",")}

    # ── Search-only mode ──
    if args.search:
        matches = search_company(args.company)
        if not matches:
            print(f"No matches for '{args.company}'")
        else:
            print(f"Found {len(matches)} match(es):")
            for m in matches[:25]:
                ticker_str = f" ({m['ticker']})" if m['ticker'] else ""
                print(f"  CIK {m['cik']:>10}: {m['name']}{ticker_str}")
            if len(matches) > 25:
                print(f"  ... and {len(matches) - 25} more")
        return

    # ── Resolve company ──
    cik, ticker, company_name = resolve_company(args.company)
    if cik is None:
        sys.exit(1)

    output: dict[str, Any] = {}
    show_all = not (args.financials_only or args.filings_only or args.details_only)

    try:
        # ── Details ──
        details: Optional[dict] = None
        if show_all or args.details_only:
            details = get_company_details(cik)
            if details:
                output["details"] = {k: v for k, v in details.items() if k != "_submissions"}
                if not args.json:
                    print_details(details)

        # ── Financials ──
        if show_all or args.financials_only:
            if not args.json:
                print(f"\nFetching XBRL financial data...")
            financials = get_financials(cik, args.years)

            if financials:
                output["financials"] = financials
                if not args.json:
                    print_financials(financials)
            else:
                if not args.json:
                    print("  ⚠ No XBRL data found on EDGAR.")
                if ticker:
                    if not args.json:
                        print("  Trying yfinance fallback...")
                    yf_data = _try_yfinance(ticker, args.years)
                    if yf_data:
                        output["financials_yfinance"] = yf_data
                        if not args.json:
                            print("  ✓ yfinance data loaded (raw format in JSON output)")
                    elif not args.json:
                        print("  ⚠ yfinance not available or no data.")

        # ── Filings ──
        filings: Optional[list] = None
        if show_all or args.filings_only or args.download:
            if not args.json:
                print(f"\nFetching filings (last {args.years} years)...")

            subs = details.get("_submissions") if details else None
            filings = get_filings(cik, args.years, form_filter, subs)
            output["filings"] = filings
            if not args.json:
                print_filings(filings, company_name or "")

        # ── Download ──
        if args.download and filings:
            out_dir = args.output or f"sec_filings_{cik}"
            if not args.json:
                print(f"\nDownloading to {out_dir}/...")
            downloaded = download_filings(filings, out_dir)
            output["downloaded"] = downloaded
            if not args.json:
                print(f"\n✓ Downloaded {len(downloaded)} files to {out_dir}/")

        # ── JSON output ──
        if args.json:
            if "details" in output and "_submissions" in output.get("details", {}):
                del output["details"]["_submissions"]
            print(json.dumps(output, ensure_ascii=False, indent=2, default=str))

    except KeyboardInterrupt:
        print("\n\nInterrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        if os.environ.get("DEBUG"):
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
