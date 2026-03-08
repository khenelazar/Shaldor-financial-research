#!/usr/bin/env python3
"""
Shaldor Research Orchestrator v2
────────────────────────────────
Unified interface for SEC EDGAR, Maya TASE, and Yahoo Finance scrapers.
Produces consolidated research packages for strategic analysis.

v2 improvements over v1:
  - Source hierarchy: TASE = Maya + Yahoo primary; US = SEC + Yahoo supplement
  - Parallel execution: up to MAX_WORKERS (4) threads across companies
  - Yahoo ticker column support in Maya CSV (YahooTicker field)
  - Simple file-based cache with configurable TTL
  - Up to 12 peers supported (13 companies total including primary)

Usage (CLI):
  python3 shaldor_research_orchestrator.py AAPL --peers MSFT GOOGL AMZN
  python3 shaldor_research_orchestrator.py "אורון" --peers "שיכון ובינוי" "אלקטרה"
  python3 shaldor_research_orchestrator.py AAPL --peers MSFT GOOGL --years 3 --json
  python3 shaldor_research_orchestrator.py ORON.TA --yahoo-ticker "אורון" ORON.TA

Usage (as module):
  from shaldor_research_orchestrator import run_research
  result = run_research("Apple", peers=["Microsoft", "Google"], years=5)

Source hierarchy:
  TASE companies:
    1. Maya TASE → company profile, filings/reports, basic financial summary
    2. Yahoo Finance → full financials (income/BS/CF), ratios, TTM, market data
    3. Maya financials → backup if Yahoo fails

  US companies:
    1. SEC EDGAR → company profile, XBRL financials (canonical), filings list
    2. Yahoo Finance → ratios, TTM, market data, gap-fill when XBRL is sparse
"""

from __future__ import annotations

import json
import sys
import os
import hashlib
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Optional

# ─── Import scrapers ─────────────────────────────────────────────────────────

_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

import sec_scraper
import maya_scraper
import shaldor_financials

# ─── Thread Safety ───────────────────────────────────────────────────────────
# SEC scraper's rate limiter uses global state — patch it to be thread-safe

import threading

_sec_rate_lock = threading.Lock()
_original_sec_rate_limit = sec_scraper._rate_limit

def _thread_safe_rate_limit():
    with _sec_rate_lock:
        _original_sec_rate_limit()

sec_scraper._rate_limit = _thread_safe_rate_limit

# ─── Configuration ───────────────────────────────────────────────────────────

MAX_PEERS = 12
MAX_WORKERS = 4  # Parallel threads — kept moderate to avoid SEC rate limit collisions
STAGGER_DELAY = 0.5  # Seconds between launching each company thread

# Cache settings
CACHE_DIR = os.path.join(_script_dir, ".research_cache")
CACHE_TTL_HOURS = 24  # Cache entries expire after this many hours
CACHE_VERSION = "v3"  # Bump when scraper logic changes to invalidate old cache


# ─── Cache ───────────────────────────────────────────────────────────────────

def _cache_key(source: str, identifier: str, years: int) -> str:
    """Generate a filesystem-safe cache key."""
    raw = f"{CACHE_VERSION}:{source}:{identifier}:{years}"
    h = hashlib.md5(raw.encode()).hexdigest()[:12]
    safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in identifier)[:40]
    return f"{source}_{safe_id}_{h}"


def cache_get(source: str, identifier: str, years: int, ttl_hours: float = CACHE_TTL_HOURS) -> Optional[dict]:
    """Retrieve cached data if it exists and hasn't expired."""
    if ttl_hours <= 0:
        return None
    if not os.path.exists(CACHE_DIR):
        return None
    key = _cache_key(source, identifier, years)
    path = os.path.join(CACHE_DIR, f"{key}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            cached = json.load(f)
        # Check version
        if cached.get("_version") != CACHE_VERSION:
            return None
        # Check expiry
        ts = cached.get("_cached_at", "")
        if ts:
            cached_time = datetime.fromisoformat(ts)
            age_hours = (datetime.now(timezone.utc) - cached_time).total_seconds() / 3600
            if age_hours > ttl_hours:
                return None
        # Don't return cached errors (transient failures shouldn't stick)
        if cached.get("_status") == "error":
            return None
        return cached.get("data")
    except (json.JSONDecodeError, ValueError, KeyError):
        return None


def cache_set(source: str, identifier: str, years: int, data: Any, status: str = "ok") -> None:
    """Store data in cache. Status can be 'ok', 'partial', or 'error'."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    key = _cache_key(source, identifier, years)
    path = os.path.join(CACHE_DIR, f"{key}.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "_cached_at": datetime.now(timezone.utc).isoformat(),
                "_version": CACHE_VERSION,
                "_status": status,
                "data": data,
            }, f, ensure_ascii=False, default=str)
    except Exception:
        pass  # Cache write failures are non-critical


# ─── Yahoo Ticker Resolution ────────────────────────────────────────────────

def _load_maya_index_with_yahoo() -> list[dict]:
    """Load Maya company index, including YahooTicker column if present."""
    companies = maya_scraper.load_company_index()
    import csv
    path = maya_scraper._find_index_file()
    if not path:
        return companies

    yahoo_map = {}
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cid = int(row["CompanyId"])
                yt = row.get("YahooTicker", "").strip()
                if yt:
                    yahoo_map[cid] = yt
            except (KeyError, ValueError):
                continue

    for c in companies:
        c["yahooTicker"] = yahoo_map.get(c["id"], "")

    return companies


def resolve_yahoo_ticker(
    query: str,
    exchange: str,
    resolved_data: dict,
    maya_companies: Optional[list] = None,
) -> tuple[Optional[str], bool]:
    """
    Determine the Yahoo Finance ticker for a company.
    Returns (ticker, is_confirmed) — is_confirmed=False means it's a guess.

    Priority:
      1. Already a .TA ticker → confirmed
      2. SEC-resolved ticker (US companies) → confirmed
      3. YahooTicker column from Maya CSV → confirmed
      4. TASE without explicit ticker → None (don't guess, flag as gap)
      5. Query as-is for non-TASE → unconfirmed
    """
    q = query.strip()

    # Explicit .TA ticker
    if q.upper().endswith(".TA"):
        return q.upper(), True

    # US: use SEC ticker directly (reliable)
    if exchange == "US" and resolved_data.get("sec_ticker"):
        return resolved_data["sec_ticker"], True

    # TASE: only use explicit YahooTicker from CSV
    if exchange == "TASE" and maya_companies:
        matches = maya_scraper.search_company(q, maya_companies)
        if matches:
            yt = matches[0].get("yahooTicker", "")
            if yt:
                return yt, True
            # No YahooTicker in CSV — don't guess, return None
            return None, False

    # UNKNOWN or fallback: try query as ticker (unconfirmed)
    return q, False


# ─── Exchange Detection ──────────────────────────────────────────────────────

def detect_exchange(query: str, maya_companies: Optional[list] = None) -> str:
    """
    Detect whether a company is TASE-listed or US-listed.
    Returns 'TASE', 'US', or 'UNKNOWN'.
    """
    q = query.strip()

    if q.upper().endswith(".TA"):
        return "TASE"

    if maya_companies is None:
        maya_companies = maya_scraper.load_company_index()
    if maya_companies:
        maya_matches = maya_scraper.search_company(q, maya_companies)
        if maya_matches:
            return "TASE"

    try:
        sec_matches = sec_scraper.search_company(q)
        if sec_matches:
            return "US"
    except Exception:
        pass

    return "UNKNOWN"


# ─── Source-Specific Scrapers ────────────────────────────────────────────────

def _scrape_sec(query: str, years: int, cache_ttl: float = CACHE_TTL_HOURS) -> dict[str, Any]:
    """Run SEC EDGAR scraper. Returns dict with details, financials, filings."""
    result = {"source": "sec", "data": None, "gaps": [], "errors": [], "time": 0}
    t0 = time.time()

    cached = cache_get("sec", query, years, cache_ttl)
    if cached:
        result["data"] = cached
        result["time"] = round(time.time() - t0, 1)
        return result

    try:
        # Note: we let scraper print to stderr (thread-safe enough for status msgs)
        # Instead of redirect_stdout which is NOT thread-safe
        cik, ticker, name = sec_scraper.resolve_company(query)

        if not cik:
            result["gaps"].append("SEC: company not found")
            result["time"] = round(time.time() - t0, 1)
            return result

        sec_data = {"cik": cik, "ticker": ticker, "name": name}

        details = sec_scraper.get_company_details(cik)
        if details:
            sec_data["details"] = {k: v for k, v in details.items() if k != "_submissions"}
            sec_data["_submissions"] = details.get("_submissions")
        else:
            result["gaps"].append("SEC: company details")

        financials = sec_scraper.get_financials(cik, years)
        if financials:
            sec_data["financials"] = financials
        else:
            result["gaps"].append("SEC: XBRL financials")

        subs = sec_data.get("_submissions")
        filings = sec_scraper.get_filings(cik, years, submissions_data=subs)
        if filings:
            sec_data["filings"] = filings
        else:
            result["gaps"].append("SEC: filings list")

        sec_data.pop("_submissions", None)
        result["data"] = sec_data
        cache_set("sec", query, years, sec_data)

    except Exception as e:
        result["errors"].append(f"SEC EDGAR: {e}")

    result["time"] = round(time.time() - t0, 1)
    return result


def _scrape_maya(query: str, years: int, maya_companies: Optional[list] = None, cache_ttl: float = CACHE_TTL_HOURS) -> dict[str, Any]:
    """Run Maya TASE scraper. Returns dict with details, financials, reports."""
    result = {"source": "maya", "data": None, "gaps": [], "errors": [], "time": 0}
    t0 = time.time()

    cached = cache_get("maya", query, years, cache_ttl)
    if cached:
        result["data"] = cached
        result["time"] = round(time.time() - t0, 1)
        return result

    try:
        cid, seed = maya_scraper.resolve_company(query)

        if not cid:
            result["gaps"].append("Maya: company not found in index")
            result["time"] = round(time.time() - t0, 1)
            return result

        maya_data = {"company_id": cid, "seed_report_id": seed}

        try:
            details = maya_scraper.get_company_details(cid)
            if details:
                maya_data["details"] = details
                maya_data["name"] = details.get("longName") or details.get("name")
            else:
                result["gaps"].append("Maya: company details")
        except Exception as e:
            result["errors"].append(f"Maya details: {e}")

        try:
            financials = maya_scraper.get_financials(cid)
            if financials:
                maya_data["financials"] = financials
            else:
                result["gaps"].append("Maya: financials")
        except Exception as e:
            result["errors"].append(f"Maya financials: {e}")

        try:
            reports = maya_scraper.get_financial_reports(cid, years, seed)
            if reports:
                maya_data["reports"] = reports
            else:
                result["gaps"].append("Maya: financial reports")
        except Exception as e:
            result["errors"].append(f"Maya reports: {e}")

        result["data"] = maya_data
        cache_set("maya", query, years, maya_data)

    except Exception as e:
        result["errors"].append(f"Maya TASE: {e}")

    result["time"] = round(time.time() - t0, 1)
    return result


def _scrape_yahoo(ticker: str, years: int, cache_ttl: float = CACHE_TTL_HOURS) -> dict[str, Any]:
    """
    Run Yahoo Finance scraper.
    Returns both the text report AND structured financial data.
    The structured data is what the normalizer consumes (not the text).
    """
    result = {"source": "yahoo", "data": None, "gaps": [], "errors": [], "time": 0}
    t0 = time.time()

    if not ticker:
        result["gaps"].append("Yahoo Finance: no ticker determined")
        return result

    cached = cache_get("yahoo", ticker, years, cache_ttl)
    if cached:
        result["data"] = cached
        result["time"] = round(time.time() - t0, 1)
        return result

    try:
        import yfinance as yf
        import pandas as pd

        t = yf.Ticker(ticker)
        info = t.info or {}

        # Validate ticker
        has_info = bool(info.get('shortName') or info.get('longName') or info.get('symbol'))
        if not has_info:
            result["gaps"].append(f"Yahoo Finance ({ticker}): ticker not found")
            result["time"] = round(time.time() - t0, 1)
            return result

        # Get text report (for markdown export / Phase B)
        text_report = shaldor_financials.get_financials(ticker)
        if text_report and text_report.startswith("[ERROR]"):
            text_report = None

        # Get structured data (for normalizer / Phase A)
        def _df_to_dict(df):
            """Convert yfinance DataFrame to serializable dict: {period_end: {row: val}}"""
            if df is None or df.empty:
                return {}
            out = {}
            for col in df.columns:
                period_key = col.strftime("%Y-%m-%d") if hasattr(col, 'strftime') else str(col)
                col_data = {}
                for idx in df.index:
                    try:
                        val = df.loc[idx, col]
                        if val is not None and not (isinstance(val, float) and pd.isna(val)):
                            col_data[str(idx)] = float(val)
                    except (TypeError, ValueError):
                        continue  # Skip non-numeric values
                out[period_key] = col_data
            return out

        structured = {}
        for attr, key in [
            ("financials", "income_statement"),
            ("balance_sheet", "balance_sheet"),
            ("cashflow", "cash_flow"),
            ("quarterly_financials", "quarterly_income"),
            ("quarterly_balance_sheet", "quarterly_balance_sheet"),
            ("quarterly_cashflow", "quarterly_cash_flow"),
        ]:
            try:
                df = getattr(t, attr)
                structured[key] = _df_to_dict(df)
            except Exception:
                structured[key] = {}

        # Extract key info fields for normalizer
        info_fields = {}
        for k in [
            "marketCap", "enterpriseValue", "trailingPE", "forwardPE",
            "priceToBook", "enterpriseToRevenue", "enterpriseToEbitda",
            "grossMargins", "operatingMargins", "profitMargins",
            "returnOnEquity", "returnOnAssets", "debtToEquity",
            "revenueGrowth", "earningsGrowth", "financialCurrency",
            "currency", "sector", "industry", "fullTimeEmployees",
            "shortName", "longName", "exchange",
        ]:
            if k in info and info[k] is not None:
                info_fields[k] = info[k]

        yahoo_data = {
            "ticker": ticker,
            "report": text_report,           # Text for markdown export
            "structured": structured,         # DataFrames as dicts for normalizer
            "info": info_fields,              # Key company info for normalizer
        }
        result["data"] = yahoo_data
        cache_set("yahoo", ticker, years, yahoo_data)

    except ImportError:
        # yfinance not installed — fall back to text-only
        try:
            output = shaldor_financials.get_financials(ticker)
            if output and not output.startswith("[ERROR]"):
                yahoo_data = {"ticker": ticker, "report": output, "structured": {}, "info": {}}
                result["data"] = yahoo_data
                cache_set("yahoo", ticker, years, yahoo_data)
            else:
                result["gaps"].append(f"Yahoo Finance ({ticker}): {output or 'no data'}")
        except Exception as e:
            result["errors"].append(f"Yahoo Finance ({ticker}): {e}")
    except Exception as e:
        result["errors"].append(f"Yahoo Finance ({ticker}): {e}")

    result["time"] = round(time.time() - t0, 1)
    return result


# ─── Single Company Research ─────────────────────────────────────────────────

def research_company(
    query: str,
    years: int = 5,
    yahoo_ticker_override: Optional[str] = None,
    maya_companies: Optional[list] = None,
    progress_callback: Optional[callable] = None,
    cache_ttl: float = CACHE_TTL_HOURS,
) -> dict[str, Any]:
    """
    Run all applicable scrapers for a single company, applying source hierarchy.

    TASE: Maya (profile+reports) → Yahoo (financials) primary
    US: SEC (profile+financials+filings) → Yahoo (supplement) primary
    """
    if maya_companies is None:
        maya_companies = _load_maya_index_with_yahoo()

    result = {
        "query": query,
        "exchange": None,
        "company_name": None,
        "sec": None,
        "maya": None,
        "yahoo": None,
        "data_gaps": [],
        "errors": [],
        "timings": {},
        "resolved": {},
    }

    def _progress(msg):
        if progress_callback:
            progress_callback(msg)

    # Detect exchange
    exchange = detect_exchange(query, maya_companies)
    result["exchange"] = exchange
    # Track how we detected the exchange
    if query.strip().upper().endswith(".TA"):
        result["exchange_confidence"] = "explicit_suffix"
    elif exchange == "TASE":
        result["exchange_confidence"] = "maya_index_match"
    elif exchange == "US":
        result["exchange_confidence"] = "sec_ticker_match"
    else:
        result["exchange_confidence"] = "unknown"
    _progress(f"  {query} → {exchange}")

    resolved = {"sec_ticker": None, "sec_cik": None, "maya_id": None}

    # Run scrapers SEQUENTIALLY within a single company
    # (parallelism happens at the multi-company level in run_research)
    # This avoids thread-safety issues with SEC rate limiter and global state

    def _collect(key, scrape_result):
        result["timings"][key] = scrape_result["time"]
        result["data_gaps"].extend(scrape_result["gaps"])
        result["errors"].extend(scrape_result["errors"])
        return scrape_result

    # SEC (US companies)
    if exchange in ("US", "UNKNOWN"):
        sec_result = _collect("sec", _scrape_sec(query, years, cache_ttl))
        if sec_result["data"]:
            result["sec"] = sec_result["data"]
            resolved["sec_cik"] = sec_result["data"].get("cik")
            resolved["sec_ticker"] = sec_result["data"].get("ticker")
            if not result["company_name"]:
                result["company_name"] = sec_result["data"].get("name")

    # Maya (TASE companies)
    if exchange in ("TASE", "UNKNOWN"):
        maya_result = _collect("maya", _scrape_maya(query, years, maya_companies, cache_ttl))
        if maya_result["data"]:
            result["maya"] = maya_result["data"]
            resolved["maya_id"] = maya_result["data"].get("company_id")
            if not result["company_name"]:
                result["company_name"] = maya_result["data"].get("name")

    # Yahoo (all companies — needs SEC/Maya resolved first for ticker)
    yahoo_ticker = yahoo_ticker_override
    yahoo_confirmed = bool(yahoo_ticker_override)
    if not yahoo_ticker:
        yahoo_ticker, yahoo_confirmed = resolve_yahoo_ticker(query, exchange, resolved, maya_companies)

    if yahoo_ticker:
        _progress(f"  Yahoo: {yahoo_ticker} ({'confirmed' if yahoo_confirmed else 'unconfirmed'})")
        yahoo_result = _collect("yahoo", _scrape_yahoo(yahoo_ticker, years, cache_ttl))
        if yahoo_result["data"]:
            result["yahoo"] = yahoo_result["data"]
            result["yahoo"]["ticker_confirmed"] = yahoo_confirmed
    else:
        result["data_gaps"].append("Yahoo Finance: no ticker available (add YahooTicker to Maya CSV or use --yahoo-ticker)")

    result["resolved"] = resolved
    return result


# ─── Multi-Company Parallel Research ─────────────────────────────────────────

def run_research(
    primary: str,
    peers: Optional[list[str]] = None,
    years: int = 5,
    yahoo_tickers: Optional[dict[str, str]] = None,
    progress_callback: Optional[callable] = None,
    use_cache: bool = True,
) -> dict[str, Any]:
    """
    Run research for a primary company and up to 12 peers, in parallel.
    """
    yahoo_tickers = yahoo_tickers or {}
    peers = (peers or [])[:MAX_PEERS]

    # Cache TTL: 0 disables cache without mutating global state
    cache_ttl = CACHE_TTL_HOURS if use_cache else 0

    def _progress(msg):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg, file=sys.stderr)

    t_start = time.time()

    # Load Maya index once (shared across all companies)
    maya_companies = _load_maya_index_with_yahoo()

    all_queries = [primary] + list(peers)
    _progress(f"Researching {len(all_queries)} companies ({1} primary + {len(peers)} peers)...")

    # Pre-load SEC ticker cache (thread safety: avoid concurrent first-load)
    try:
        sec_scraper._load_tickers()
    except Exception:
        pass

    results_map = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_query = {}
        for i, q in enumerate(all_queries):
            # Stagger submissions to avoid SEC rate limit collisions
            if i > 0:
                time.sleep(STAGGER_DELAY)
            future = executor.submit(
                research_company,
                query=q,
                years=years,
                yahoo_ticker_override=yahoo_tickers.get(q),
                maya_companies=maya_companies,
                progress_callback=progress_callback,
                cache_ttl=cache_ttl,
            )
            future_to_query[future] = q

        for future in as_completed(future_to_query):
            q = future_to_query[future]
            try:
                company_result = future.result()
                results_map[q] = company_result
                name = company_result.get("company_name") or q
                gaps = len(company_result.get("data_gaps", []))
                _progress(f"  ✓ {name} ({company_result['exchange']}, {gaps} gaps)")
            except Exception as e:
                _progress(f"  ✗ {q}: {e}")
                results_map[q] = {
                    "query": q, "exchange": None, "exchange_confidence": "error",
                    "company_name": q,
                    "sec": None, "maya": None, "yahoo": None,
                    "data_gaps": [f"Fatal error: {e}"], "errors": [str(e)],
                    "timings": {}, "resolved": {},
                }

    primary_data = results_map.get(primary, results_map.get(all_queries[0]))
    peers_data = [results_map[p] for p in peers if p in results_map]

    duration = round(time.time() - t_start, 1)

    report = build_markdown_report(primary_data, peers_data, years)

    return {
        "primary": primary_data,
        "peers": peers_data,
        "report": report,
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": duration,
            "years_requested": years,
            "primary_query": primary,
            "peer_queries": peers,
            "companies_total": len(all_queries),
        },
    }


# ─── Markdown Report Builder ────────────────────────────────────────────────

def _fmt_val(v: Any, is_per_share: bool = False) -> str:
    """Format a financial value for markdown tables."""
    if v is None:
        return "—"
    if is_per_share:
        return f"{v:,.2f}"
    abs_v = abs(v)
    if abs_v >= 1_000_000_000:
        return f"{v / 1_000_000_000:,.1f}B"
    elif abs_v >= 1_000_000:
        return f"{v / 1_000_000:,.0f}M"
    elif abs_v >= 1_000:
        return f"{v / 1_000:,.0f}K"
    else:
        return f"{v:,.0f}"


def _build_sec_financials(fin: dict) -> list[str]:
    """Build markdown tables for SEC XBRL financial data."""
    lines = []
    taxonomy = fin.get("taxonomy", "us-gaap").upper().replace("-", " ")
    lines.append(f"### Financial Data (SEC EDGAR XBRL, {taxonomy})")
    lines.append("")

    for section_name, section_key in [
        ("Income Statement", "income_statement"),
        ("Balance Sheet", "balance_sheet"),
        ("Cash Flow Statement", "cash_flow"),
    ]:
        rows = fin.get(section_key, [])
        if not rows:
            lines.append(f"**{section_name}:** [DATA GAP]")
            lines.append("")
            continue

        lines.append(f"**{section_name}**")
        lines.append("")

        annual_periods = set()
        for r in rows:
            annual_periods.update(r.get("annual", {}).keys())
        sorted_annual = sorted(annual_periods)[-6:]

        if sorted_annual:
            header = f"| {'Metric':<28} |" + "|".join(f" {p:>12} " for p in sorted_annual) + "|"
            sep = f"|{'─' * 30}|" + "|".join("─" * 14 for _ in sorted_annual) + "|"
            lines.append(header)
            lines.append(sep)

            for r in rows:
                vals = [_fmt_val(r["annual"].get(p), r.get("is_per_share")) for p in sorted_annual]
                line = f"| {r['label']:<28} |" + "|".join(f" {v:>12} " for v in vals) + "|"
                lines.append(line)
            lines.append("")
    return lines


def _build_maya_financials(fin: dict) -> list[str]:
    """Build markdown table for Maya financial summary."""
    lines = []
    lines.append(f"### Financial Summary (Maya TASE, {fin.get('currency', 'N/A')})")
    lines.append("")

    p = fin.get("periods", {})
    header = f"| {'Field':<35} | {p.get('current', ''):>15} | {p.get('previous', ''):>15} | {p.get('previousYear', ''):>15} |"
    sep = f"|{'─' * 37}|{'─' * 17}|{'─' * 17}|{'─' * 17}|"
    lines.append(header)
    lines.append(sep)

    for item in fin.get("items", []):
        def _fmv(v):
            if v is None: return "—"
            if isinstance(v, str): return v
            return f"{v:,.0f}"
        line = f"| {item['name']:<35} | {_fmv(item.get('current')):>15} | {_fmv(item.get('previous')):>15} | {_fmv(item.get('previousYear')):>15} |"
        lines.append(line)
    lines.append("")
    return lines


def _build_company_section(data: dict, is_primary: bool = True) -> list[str]:
    """Build the full markdown section for one company, respecting source hierarchy."""
    lines = []
    name = data.get("company_name") or data.get("query", "Unknown")
    exchange = data.get("exchange", "N/A")

    if is_primary:
        lines.append(f"## PRIMARY COMPANY: {name}")
    else:
        lines.append(f"### {name}")
    lines.append("")
    lines.append(f"Exchange: {exchange}")
    lines.append("")

    # ── Company Profile ──
    if data.get("sec") and data["sec"].get("details"):
        d = data["sec"]["details"]
        lines.append("**Company Profile (SEC EDGAR)**")
        lines.append("")
        tickers = ", ".join(d.get("tickers", [])) or "N/A"
        exchanges = ", ".join(d.get("exchanges", [])) or "N/A"
        lines.append(f"- Name: {d.get('name', 'N/A')}")
        lines.append(f"- Ticker(s): {tickers} | Exchange(s): {exchanges}")
        lines.append(f"- SIC: {d.get('sic', 'N/A')} — {d.get('sicDescription', 'N/A')}")
        lines.append(f"- Entity Type: {d.get('entityType', 'N/A')} | Category: {d.get('category', 'N/A')}")
        lines.append(f"- Address: {d.get('address', 'N/A')}")
        lines.append(f"- State of Inc.: {d.get('stateOfIncorporation', 'N/A')}")
        lines.append("")

    if data.get("maya") and data["maya"].get("details"):
        d = data["maya"]["details"]
        lines.append("**Company Profile (Maya TASE)**")
        lines.append("")
        lines.append(f"- Name: {d.get('longName') or d.get('name', 'N/A')}")
        lines.append(f"- Sector: {d.get('sector', 'N/A')} | Branch: {d.get('branch', 'N/A')}")
        if d.get("subBranch"):
            lines.append(f"- Sub-Branch: {d['subBranch']}")
        mv = d.get("marketValue")
        lines.append(f"- Market Value: {mv:,} K NIS" if mv else "- Market Value: N/A")
        lines.append(f"- Dual Listed: {'Yes' if d.get('isDual') else 'No'}")
        indices = d.get("indices", [])
        lines.append(f"- Indices: {', '.join(indices) if indices else 'N/A'}")
        desc = d.get("description", "")
        if desc:
            lines.append(f"- Description: {desc}")
        lines.append("")

    # ── Financial Data (source hierarchy) ──
    if exchange == "TASE":
        # TASE: Yahoo primary → Maya backup
        if data.get("yahoo") and data["yahoo"].get("report"):
            lines.append("### Financial Data (Yahoo Finance — Primary)")
            lines.append("")
            lines.append(f"Ticker: {data['yahoo'].get('ticker', 'N/A')}")
            lines.append("")
            lines.append("```")
            lines.append(data["yahoo"]["report"])
            lines.append("```")
            lines.append("")
            if data.get("maya") and data["maya"].get("financials"):
                lines.append("### Financial Summary (Maya TASE — Supplementary)")
                lines.append("")
                lines.extend(_build_maya_financials(data["maya"]["financials"]))
        else:
            if data.get("maya") and data["maya"].get("financials"):
                lines.append("### Financial Summary (Maya TASE — Primary, Yahoo unavailable)")
                lines.append("")
                lines.extend(_build_maya_financials(data["maya"]["financials"]))
            else:
                lines.append("### Financial Data: [DATA GAP] — Neither Yahoo nor Maya returned financials")
                lines.append("")

    elif exchange == "US":
        # US: SEC primary → Yahoo supplement
        if data.get("sec") and data["sec"].get("financials"):
            lines.extend(_build_sec_financials(data["sec"]["financials"]))
            if data.get("yahoo") and data["yahoo"].get("report"):
                lines.append("### Market Data & Ratios (Yahoo Finance — Supplement)")
                lines.append("")
                lines.append(f"Ticker: {data['yahoo'].get('ticker', 'N/A')}")
                lines.append("")
                lines.append("```")
                lines.append(data["yahoo"]["report"])
                lines.append("```")
                lines.append("")
        else:
            if data.get("yahoo") and data["yahoo"].get("report"):
                lines.append("### Financial Data (Yahoo Finance — Primary, SEC XBRL unavailable)")
                lines.append("")
                lines.append("```")
                lines.append(data["yahoo"]["report"])
                lines.append("```")
                lines.append("")
            else:
                lines.append("### Financial Data: [DATA GAP] — Neither SEC nor Yahoo returned financials")
                lines.append("")
    else:
        # UNKNOWN — dump everything
        if data.get("sec") and data["sec"].get("financials"):
            lines.extend(_build_sec_financials(data["sec"]["financials"]))
        if data.get("maya") and data["maya"].get("financials"):
            lines.extend(_build_maya_financials(data["maya"]["financials"]))
        if data.get("yahoo") and data["yahoo"].get("report"):
            lines.append("### Financial Data (Yahoo Finance)")
            lines.append("")
            lines.append("```")
            lines.append(data["yahoo"]["report"])
            lines.append("```")
            lines.append("")

    # ── Filings & Reports (primary only, to keep report manageable) ──
    if is_primary:
        if data.get("sec") and data["sec"].get("filings"):
            filings = data["sec"]["filings"]
            lines.append(f"### Recent SEC Filings ({len(filings)} found)")
            lines.append("")
            for f in filings[:20]:
                items_str = f" — Items: {f['items']}" if f.get("items") else ""
                lines.append(f"- **{f['form']}** ({f['filingDate']}) Period: {f.get('reportDate', 'N/A')}{items_str}")
                if f.get("documentUrl"):
                    lines.append(f"  {f['documentUrl']}")
            if len(filings) > 20:
                lines.append(f"- ... and {len(filings) - 20} more filings")
            lines.append("")

        if data.get("maya") and data["maya"].get("reports"):
            reports = data["maya"]["reports"]
            lines.append(f"### Maya TASE Reports ({len(reports)} found)")
            lines.append("")
            for r in reports[:15]:
                date = r.get("publishDate", "")[:10]
                lines.append(f"- [{date}] {r.get('title', 'Untitled')}")
                for a in r.get("attachments", []):
                    if "pdf" in a.get("type", ""):
                        lines.append(f"  PDF: {a.get('url', '')}")
            if len(reports) > 15:
                lines.append(f"- ... and {len(reports) - 15} more reports")
            lines.append("")

    return lines


def build_markdown_report(
    primary: dict[str, Any],
    peers: list[dict[str, Any]],
    years: int,
) -> str:
    """Build the full markdown research package."""
    lines = []
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    company_name = primary.get("company_name") or primary.get("query", "Unknown")

    # Header
    lines.append(f"# RESEARCH PACKAGE: {company_name}")
    lines.append("")
    lines.append(f"Generated: {ts}")
    lines.append(f"Data period: Last {years} years")
    lines.append(f"Exchange: {primary.get('exchange', 'N/A')}")
    if peers:
        peer_names = [p.get("company_name") or p.get("query") for p in peers]
        lines.append(f"Peers ({len(peers)}): {', '.join(peer_names)}")
    lines.append("")
    hierarchy = "Maya + Yahoo Finance" if primary.get("exchange") == "TASE" else "SEC EDGAR + Yahoo Finance"
    lines.append(f"Source hierarchy: {hierarchy}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Primary
    lines.extend(_build_company_section(primary, is_primary=True))

    # Peers
    if peers:
        lines.append("---")
        lines.append("")
        lines.append(f"## PEER COMPANIES ({len(peers)})")
        lines.append("")
        for peer in peers:
            lines.extend(_build_company_section(peer, is_primary=False))

    # Data quality report
    lines.append("---")
    lines.append("")
    lines.append("## DATA QUALITY REPORT")
    lines.append("")

    all_results = [("Primary", primary)] + [(f"Peer", p) for p in peers]
    has_issues = False

    for label, data in all_results:
        comp_name = data.get("company_name") or data.get("query", "")
        gaps = data.get("data_gaps", [])
        errors = data.get("errors", [])
        timings = data.get("timings", {})

        if gaps or errors:
            has_issues = True
            lines.append(f"**{label}: {comp_name}**")
            for g in gaps:
                lines.append(f"- [DATA GAP] {g}")
            for e in errors:
                lines.append(f"- [ERROR] {e}")
            lines.append("")

        if timings:
            timing_str = ", ".join(f"{k}: {v}s" for k, v in timings.items())
            lines.append(f"Timing ({comp_name}): {timing_str}")

    if not has_issues:
        lines.append("All data sources returned successfully. No gaps detected.")

    lines.append("")
    lines.append("---")
    lines.append("*End of research package*")

    return "\n".join(lines)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Shaldor Research Orchestrator v2 — Parallel company research",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s AAPL                                          # US company
  %(prog)s "אורון" --yahoo-ticker "אורון" ORON.TA        # TASE + Yahoo override
  %(prog)s AAPL --peers MSFT GOOGL AMZN META             # With 4 peers
  %(prog)s "OPC Energy" --peers "Enlight" "Nofar" --years 3
  %(prog)s AAPL --no-cache                                # Skip cache
  %(prog)s AAPL --clear-cache                             # Clear all cached data
        """
    )
    parser.add_argument("company", nargs="?", help="Primary company: ticker, name, CIK, or Maya ID")
    parser.add_argument("--peers", nargs="+", default=[], help=f"Peer companies (max {MAX_PEERS})")
    parser.add_argument("--years", type=int, default=5, help="Years of data (default: 5)")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    parser.add_argument("--output", default=None, help="Directory to save output files")
    parser.add_argument("--yahoo-ticker", nargs=2, action="append", default=[],
                        metavar=("COMPANY", "TICKER"),
                        help="Override Yahoo ticker (repeatable)")
    parser.add_argument("--no-cache", action="store_true", help="Disable cache for this run")
    parser.add_argument("--clear-cache", action="store_true", help="Clear cache and exit")

    args = parser.parse_args()

    if args.clear_cache:
        if os.path.exists(CACHE_DIR):
            import shutil
            shutil.rmtree(CACHE_DIR)
            print(f"✓ Cache cleared: {CACHE_DIR}")
        else:
            print("Cache directory doesn't exist.")
        return

    if not args.company:
        parser.print_help()
        sys.exit(1)

    if len(args.peers) > MAX_PEERS:
        print(f"⚠ Maximum {MAX_PEERS} peers allowed. Using first {MAX_PEERS}.", file=sys.stderr)
        args.peers = args.peers[:MAX_PEERS]

    yahoo_tickers = {company: ticker for company, ticker in args.yahoo_ticker}

    result = run_research(
        primary=args.company,
        peers=args.peers,
        years=args.years,
        yahoo_tickers=yahoo_tickers,
        use_cache=not args.no_cache,
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(result["report"])

    if args.output:
        os.makedirs(args.output, exist_ok=True)
        report_path = os.path.join(args.output, "research_package.md")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(result["report"])
        json_path = os.path.join(args.output, "research_data.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n✓ Saved: {report_path}", file=sys.stderr)
        print(f"✓ Saved: {json_path}", file=sys.stderr)

    meta = result["metadata"]
    total_gaps = sum(len(r.get("data_gaps", [])) for r in [result["primary"]] + result["peers"])
    total_errors = sum(len(r.get("errors", [])) for r in [result["primary"]] + result["peers"])
    print(f"\n{'═' * 60}", file=sys.stderr)
    print(f"  {meta['companies_total']} companies in {meta['duration_seconds']}s | {total_gaps} gaps | {total_errors} errors", file=sys.stderr)
    print(f"{'═' * 60}", file=sys.stderr)


if __name__ == "__main__":
    main()
