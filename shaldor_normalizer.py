#!/usr/bin/env python3
"""
Shaldor Financial Normalizer
─────────────────────────────
Phase A data processing layer: takes raw scraper output and produces a unified,
comparable dataset with computed metrics, growth rates, and data quality flags.

This module sits between the scrapers and the UI/export layers:
  Scrapers → Normalizer → Streamlit / Markdown export

Design principles:
  - Every calculation is defensive (checks for None/zero before computing)
  - No strategic analysis — only objective, technical, data-based outputs
  - All companies get the same schema, enabling peer comparison
  - Currency is preserved as-reported, never converted (conversion = Phase B)
  - Missing data is explicitly flagged, never silently dropped

Usage:
  from shaldor_normalizer import normalize_company, build_peer_table

  # From orchestrator result:
  normalized = normalize_company(company_result)
  peer_table = build_peer_table([primary_normalized] + [normalize_company(p) for p in peers])
"""

from __future__ import annotations

import math
from typing import Any, Optional


# ─── Canonical Metric Definitions ────────────────────────────────────────────
# Single source of truth for all metrics across the system.
# compute_metrics, completeness scoring, and peer table all reference these.

PROFITABILITY_METRICS = [
    "gross_margin", "ebitda_margin", "operating_margin", "net_margin", "roe",
]

EFFICIENCY_METRICS = [
    "roa", "asset_turnover", "operating_leverage",
]

CASH_FLOW_METRICS = [
    "fcf_margin", "cash_conversion",
]

LEVERAGE_METRICS = [
    "debt_to_equity", "debt_to_ebitda", "net_debt_to_ebitda",
]

CAPITAL_STRUCTURE_METRICS = [
    "equity_ratio",
]

VALUATION_METRICS = [
    "pe_trailing", "pe_forward", "ev_to_ebitda", "ev_to_revenue", "price_to_sales", "fcf_yield",
]

GROWTH_METRICS = [
    "revenue", "ebitda", "net_income", "operating_income", "free_cash_flow",
]

# The full canonical set (used for completeness scoring)
CANONICAL_METRICS = (
    PROFITABILITY_METRICS + EFFICIENCY_METRICS + CASH_FLOW_METRICS +
    LEVERAGE_METRICS + CAPITAL_STRUCTURE_METRICS + VALUATION_METRICS
)

# Metrics shown in peer comparison table (includes YoY growth variants)
PEER_TABLE_METRICS = [
    "revenue_yoy", "ebitda_yoy", "net_income_yoy",
] + PROFITABILITY_METRICS + EFFICIENCY_METRICS + CASH_FLOW_METRICS + LEVERAGE_METRICS + CAPITAL_STRUCTURE_METRICS + VALUATION_METRICS

# Absolute value metrics (NOT comparable across currencies)
ABSOLUTE_METRICS = {"free_cash_flow"}

# ─── Unified Schema ──────────────────────────────────────────────────────────
# Every company gets mapped to this structure, regardless of source.

def empty_financials_row() -> dict[str, Any]:
    """Template for a single year's financial data."""
    return {
        # Period info
        "period": None,           # e.g. "2024", "FY2024", "CY2024Q2"
        "period_end": None,       # date string: "2024-09-30"
        "fiscal_year": None,      # int: 2024
        "is_annual": True,

        # Income statement (in reporting currency)
        "revenue": None,
        "cost_of_revenue": None,
        "gross_profit": None,
        "rd_expense": None,
        "sga_expense": None,
        "operating_income": None,
        "ebitda": None,
        "interest_expense": None,
        "pretax_income": None,
        "income_tax": None,
        "net_income": None,
        "eps_basic": None,
        "eps_diluted": None,

        # Balance sheet
        "total_assets": None,
        "current_assets": None,
        "cash_and_equivalents": None,
        "total_liabilities": None,
        "current_liabilities": None,
        "long_term_debt": None,
        "total_debt": None,
        "net_debt": None,
        "total_equity": None,
        "shares_outstanding": None,

        # Cash flow
        "operating_cash_flow": None,
        "capital_expenditure": None,
        "free_cash_flow": None,
        "investing_cash_flow": None,
        "financing_cash_flow": None,
        "dividends_paid": None,
        "depreciation_amortization": None,
    }


def empty_normalized() -> dict[str, Any]:
    """Template for a fully normalized company dataset."""
    return {
        # Identity
        "company_name": None,
        "exchange": None,         # "TASE" or "US"
        "ticker": None,
        "currency": None,         # Reporting currency: "USD", "ILS", etc.
        "sector": None,
        "industry": None,
        "description": None,
        "fiscal_year_end_month": None,  # int: 9 for September, 12 for December

        # Profile data
        "market_cap": None,
        "enterprise_value": None,
        "employees": None,

        # Annual financials (list of dicts, sorted chronologically)
        "annual": [],             # list of financials rows

        # Computed metrics (latest available)
        "metrics": {},            # dict of metric_name → value

        # Growth rates
        "growth": {},             # dict of metric_name → {yoy: [...], cagr_3y: x, cagr_5y: x}

        # Trend signals (mechanical only, no interpretation)
        "trends": [],             # list of {"metric": str, "direction": "UP"/"DOWN"/"FLAT", "detail": str}

        # Data quality
        "data_quality": {
            "sources_used": [],   # e.g. ["SEC", "Yahoo"]
            "years_available": 0,
            "gaps": [],           # list of str descriptions
            "warnings": [],       # list of str descriptions
            "completeness": 0.0,  # 0-1 score
        },
    }


# ─── Safe Math Helpers ───────────────────────────────────────────────────────

def _safe_div(numerator: Any, denominator: Any) -> Optional[float]:
    """Divide only if both values exist and denominator != 0."""
    if numerator is None or denominator is None:
        return None
    try:
        n = float(numerator)
        d = float(denominator)
        if d == 0:
            return None
        return n / d
    except (TypeError, ValueError):
        return None


def _safe_pct(numerator: Any, denominator: Any) -> Optional[float]:
    """Compute percentage (0-1 scale). Returns None if inputs are invalid."""
    return _safe_div(numerator, denominator)


def _safe_subtract(a: Any, b: Any) -> Optional[float]:
    """Subtract only if both values exist."""
    if a is None or b is None:
        return None
    try:
        return float(a) - float(b)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float, returning None for any failure."""
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _yoy_growth(current: Any, previous: Any) -> Optional[float]:
    """Compute year-over-year growth rate. Returns decimal (0.15 = 15%)."""
    c = _safe_float(current)
    p = _safe_float(previous)
    if c is None or p is None or p == 0:
        return None
    return (c - p) / abs(p)


def _cagr(start_val: Any, end_val: Any, years: int) -> Optional[float]:
    """
    Compute CAGR. Returns decimal (0.12 = 12%).
    Only valid if both values are positive and years > 0.
    """
    s = _safe_float(start_val)
    e = _safe_float(end_val)
    if s is None or e is None or s <= 0 or e <= 0 or years <= 0:
        return None
    try:
        return (e / s) ** (1.0 / years) - 1
    except (ValueError, ZeroDivisionError, OverflowError):
        return None


# ─── Source-Specific Extractors ──────────────────────────────────────────────

def _extract_yahoo_annual(yahoo_data: dict) -> tuple[list[dict], dict[str, Any]]:
    """
    Extract structured annual financial data from the orchestrator's Yahoo output.
    Reads from yahoo_data["structured"] and yahoo_data["info"] — NO network calls.

    Args:
        yahoo_data: The orchestrator's yahoo result dict containing:
            - "structured": {income_statement: {period: {row: val}}, ...}
            - "info": {marketCap: ..., sector: ..., etc.}

    Returns (list_of_annual_rows, info_dict).
    """
    info = yahoo_data.get("info", {})
    structured = yahoo_data.get("structured", {})

    if not structured:
        return [], info

    inc = structured.get("income_statement", {})
    bs = structured.get("balance_sheet", {})
    cf = structured.get("cash_flow", {})

    # Collect all available periods across all statements
    all_periods = sorted(set(list(inc.keys()) + list(bs.keys()) + list(cf.keys())))

    if not all_periods:
        return [], info

    def _val(statement: dict, row_name: str, period: str) -> Optional[float]:
        """Safely extract a value from structured data."""
        period_data = statement.get(period, {})
        v = period_data.get(row_name)
        return _safe_float(v)

    rows = []
    for period_end in all_periods:
        # Parse fiscal year from period string (e.g. "2024-09-30" → 2024)
        fy = None
        try:
            fy = int(period_end[:4])
        except (ValueError, IndexError):
            pass

        row = empty_financials_row()
        row["period"] = str(fy) if fy else period_end
        row["period_end"] = period_end
        row["fiscal_year"] = fy
        row["is_annual"] = True

        # Income statement mapping
        row["revenue"] = _val(inc, "Total Revenue", period_end)
        row["cost_of_revenue"] = _val(inc, "Cost Of Revenue", period_end)
        row["gross_profit"] = _val(inc, "Gross Profit", period_end)
        row["rd_expense"] = _val(inc, "Research And Development", period_end)
        row["sga_expense"] = _val(inc, "Selling General And Administration", period_end)
        row["operating_income"] = _val(inc, "Operating Income", period_end)
        row["ebitda"] = _val(inc, "EBITDA", period_end)
        row["interest_expense"] = _val(inc, "Interest Expense", period_end)
        row["pretax_income"] = _val(inc, "Pretax Income", period_end)
        row["income_tax"] = _val(inc, "Tax Provision", period_end)
        row["net_income"] = _val(inc, "Net Income", period_end)
        row["eps_basic"] = _val(inc, "Basic EPS", period_end)
        row["eps_diluted"] = _val(inc, "Diluted EPS", period_end)

        # Balance sheet mapping
        row["total_assets"] = _val(bs, "Total Assets", period_end)
        row["current_assets"] = _val(bs, "Current Assets", period_end)
        row["cash_and_equivalents"] = _val(bs, "Cash And Cash Equivalents", period_end)
        row["total_liabilities"] = _val(bs, "Total Liabilities Net Minority Interest", period_end)
        row["current_liabilities"] = _val(bs, "Current Liabilities", period_end)
        row["long_term_debt"] = _val(bs, "Long Term Debt", period_end)
        row["total_debt"] = _val(bs, "Total Debt", period_end)
        row["net_debt"] = _val(bs, "Net Debt", period_end)
        row["total_equity"] = _val(bs, "Stockholders Equity", period_end)
        row["shares_outstanding"] = _val(bs, "Share Issued", period_end)

        # Cash flow mapping
        row["operating_cash_flow"] = _val(cf, "Operating Cash Flow", period_end)
        row["capital_expenditure"] = _val(cf, "Capital Expenditure", period_end)
        row["free_cash_flow"] = _val(cf, "Free Cash Flow", period_end)
        row["investing_cash_flow"] = _val(cf, "Investing Cash Flow", period_end)
        row["financing_cash_flow"] = _val(cf, "Financing Cash Flow", period_end)
        row["dividends_paid"] = _val(cf, "Cash Dividends Paid", period_end)
        row["depreciation_amortization"] = _val(cf, "Depreciation And Amortization", period_end)

        # Compute derived fields
        if row["gross_profit"] is None and row["revenue"] is not None and row["cost_of_revenue"] is not None:
            row["gross_profit"] = row["revenue"] - row["cost_of_revenue"]
        if row["ebitda"] is None and row["operating_income"] is not None and row["depreciation_amortization"] is not None:
            row["ebitda"] = row["operating_income"] + abs(row["depreciation_amortization"])
        if row["free_cash_flow"] is None and row["operating_cash_flow"] is not None and row["capital_expenditure"] is not None:
            row["free_cash_flow"] = row["operating_cash_flow"] + row["capital_expenditure"]  # capex is negative

        rows.append(row)

    return rows, info


def _extract_sec_annual(sec_data: dict) -> list[dict]:
    """
    Extract annual financial data from SEC XBRL financials.
    Maps SEC frame-based data into the unified schema.
    """
    financials = sec_data.get("financials")
    if not financials:
        return []

    # SEC field mapping: label → unified field name
    sec_map = {
        "Revenue": "revenue",
        "Cost of Revenue": "cost_of_revenue",
        "Gross Profit": "gross_profit",
        "R&D Expense": "rd_expense",
        "SG&A Expense": "sga_expense",
        "Operating Income": "operating_income",
        "Interest Expense": "interest_expense",
        "Pre-tax Income": "pretax_income",
        "Income Tax": "income_tax",
        "Net Income": "net_income",
        "EPS (Basic)": "eps_basic",
        "EPS (Diluted)": "eps_diluted",
        "Cash & Equivalents": "cash_and_equivalents",
        "Total Current Assets": "current_assets",
        "Total Assets": "total_assets",
        "Total Current Liabilities": "current_liabilities",
        "Long-term Debt": "long_term_debt",
        "Total Liabilities": "total_liabilities",
        "Total Equity": "total_equity",
        "Operating Cash Flow": "operating_cash_flow",
        "Capital Expenditures": "capital_expenditure",
        "Investing Cash Flow": "investing_cash_flow",
        "Dividends Paid": "dividends_paid",
        "Financing Cash Flow": "financing_cash_flow",
    }

    # Collect all annual periods across all statements
    period_data = {}  # frame → row dict

    for section_key in ["income_statement", "balance_sheet", "cash_flow"]:
        for item in financials.get(section_key, []):
            label = item.get("label", "")
            field = sec_map.get(label)
            if not field:
                continue

            for frame, val in item.get("annual", {}).items():
                if frame not in period_data:
                    row = empty_financials_row()
                    # Parse frame: "CY2024" → fiscal_year 2024
                    try:
                        fy = int(frame[2:6])
                        row["period"] = frame
                        row["fiscal_year"] = fy
                        row["is_annual"] = True
                    except (ValueError, IndexError):
                        row["period"] = frame
                    period_data[frame] = row

                period_data[frame][field] = _safe_float(val)

    # Sort chronologically
    rows = sorted(period_data.values(), key=lambda r: r.get("fiscal_year") or 0)

    # Compute derived fields
    for row in rows:
        if row["gross_profit"] is None:
            row["gross_profit"] = _safe_subtract(row["revenue"], row["cost_of_revenue"])
        if row["free_cash_flow"] is None and row["operating_cash_flow"] is not None and row["capital_expenditure"] is not None:
            row["free_cash_flow"] = row["operating_cash_flow"] - abs(row["capital_expenditure"])

    return rows


def _extract_maya_financials(maya_data: dict) -> tuple[list[dict], list[str]]:
    """
    Extract financial data from Maya's financial summary.
    Maya gives only 3 periods with limited line items.
    
    Returns (rows, warnings) — warnings flag non-annual periods.
    """
    financials = maya_data.get("financials")
    if not financials:
        return [], []

    # Maya field mapping (Hebrew field names → unified)
    maya_field_map = {
        "הכנסות": "revenue",
        "סה\"כ הכנסות": "revenue",
        "רווח גולמי": "gross_profit",
        "רווח תפעולי": "operating_income",
        "רווח נקי": "net_income",
        "רווח נקי המיוחס לבעלי מניות": "net_income",
        "סך מאזן": "total_assets",
        "סה\"כ נכסים": "total_assets",
        "הון עצמי": "total_equity",
        "הון עצמי המיוחס לבעלי מניות": "total_equity",
        "תזרים מזומנים מפעילות שוטפת": "operating_cash_flow",
    }

    periods = financials.get("periods", {})
    period_keys = ["current", "previous", "previousYear"]
    period_labels = [
        periods.get("current", ""),
        periods.get("previous", ""),
        periods.get("previousYear", ""),
    ]

    # Check if periods look annual (Hebrew period labels often contain month ranges)
    maya_warnings = []
    non_annual_keywords = ["רבעון", "חצי", "Q1", "Q2", "Q3", "Q4", "6 חודשים", "3 חודשים"]
    for pl in period_labels:
        if any(kw in pl for kw in non_annual_keywords):
            maya_warnings.append(f"Maya period '{pl}' may not be annual — growth/trend calculations may be inaccurate")
            break

    rows = []
    for i, (pk, pl) in enumerate(zip(period_keys, period_labels)):
        row = empty_financials_row()
        row["period"] = pl
        row["is_annual"] = not any(any(kw in pl for kw in non_annual_keywords) for _ in [1])

        for item in financials.get("items", []):
            name = item.get("name", "").strip()
            field = maya_field_map.get(name)
            if field:
                val = item.get(pk)
                row[field] = _safe_float(val) if val is not None else None

        rows.append(row)

    return rows, maya_warnings


# ─── Metric Computation ─────────────────────────────────────────────────────

def compute_metrics(annual: list[dict], info: dict = None) -> dict[str, Optional[float]]:
    """
    Compute the 25 key metrics from the latest available annual data.
    Returns dict of metric_name → value (None if not computable).
    """
    info = info or {}
    metrics = {}

    if not annual:
        return metrics

    latest = annual[-1]

    # ── Profitability margins ──
    rev = latest.get("revenue")
    metrics["gross_margin"] = _safe_pct(latest.get("gross_profit"), rev)
    metrics["ebitda_margin"] = _safe_pct(latest.get("ebitda"), rev)
    metrics["operating_margin"] = _safe_pct(latest.get("operating_income"), rev)
    metrics["net_margin"] = _safe_pct(latest.get("net_income"), rev)

    # ── Return metrics ──
    metrics["roe"] = _safe_pct(latest.get("net_income"), latest.get("total_equity"))
    metrics["roa"] = _safe_pct(latest.get("net_income"), latest.get("total_assets"))

    # ── Efficiency ──
    metrics["asset_turnover"] = _safe_div(rev, latest.get("total_assets"))

    # Operating leverage (% change in operating income / % change in revenue)
    if len(annual) >= 2:
        prev = annual[-2]
        rev_growth = _yoy_growth(rev, prev.get("revenue"))
        oi_growth = _yoy_growth(latest.get("operating_income"), prev.get("operating_income"))
        metrics["operating_leverage"] = _safe_div(oi_growth, rev_growth)
    else:
        metrics["operating_leverage"] = None

    # ── Cash flow ──
    metrics["free_cash_flow"] = _safe_float(latest.get("free_cash_flow"))
    metrics["fcf_margin"] = _safe_pct(latest.get("free_cash_flow"), rev)
    # Cash conversion: FCF / Net Income
    metrics["cash_conversion"] = _safe_div(latest.get("free_cash_flow"), latest.get("net_income"))

    # ── Leverage ──
    metrics["debt_to_equity"] = _safe_div(latest.get("total_debt"), latest.get("total_equity"))
    ebitda = latest.get("ebitda")
    metrics["debt_to_ebitda"] = _safe_div(latest.get("total_debt"), ebitda)
    metrics["net_debt_to_ebitda"] = _safe_div(latest.get("net_debt"), ebitda)

    # ── Capital structure ──
    metrics["equity_ratio"] = _safe_div(latest.get("total_equity"), latest.get("total_assets"))

    # ── Valuation (from Yahoo info if available) ──
    metrics["pe_trailing"] = _safe_float(info.get("trailingPE"))
    metrics["pe_forward"] = _safe_float(info.get("forwardPE"))
    metrics["ev_to_ebitda"] = _safe_float(info.get("enterpriseToEbitda"))
    metrics["ev_to_revenue"] = _safe_float(info.get("enterpriseToRevenue"))

    # Price/Sales: Market Cap / Revenue (distinct from EV/Revenue which uses enterprise value)
    market_cap = _safe_float(info.get("marketCap"))
    metrics["price_to_sales"] = _safe_div(market_cap, rev)
    metrics["fcf_yield"] = _safe_div(latest.get("free_cash_flow"), market_cap)

    return metrics


def compute_growth(annual: list[dict]) -> dict[str, dict]:
    """
    Compute growth rates for key metrics.
    Returns dict of metric_name → {yoy: [list], cagr_3y: float, cagr_5y: float}
    """
    growth = {}

    for field in GROWTH_METRICS:
        values = [(r.get("fiscal_year"), r.get(field)) for r in annual if r.get(field) is not None]
        values = [(fy, v) for fy, v in values if fy is not None]

        if len(values) < 2:
            growth[field] = {"yoy": [], "cagr_3y": None, "cagr_5y": None}
            continue

        # YoY growth rates
        yoy = []
        for i in range(1, len(values)):
            fy = values[i][0]
            g = _yoy_growth(values[i][1], values[i - 1][1])
            yoy.append({"year": fy, "growth": g})

        # CAGR
        cagr_3y = None
        cagr_5y = None
        if len(values) >= 4:  # need 4 data points for 3-year CAGR
            cagr_3y = _cagr(values[-4][1], values[-1][1], 3)
        if len(values) >= 6:  # need 6 data points for 5-year CAGR
            cagr_5y = _cagr(values[-6][1], values[-1][1], 5)

        growth[field] = {"yoy": yoy, "cagr_3y": cagr_3y, "cagr_5y": cagr_5y}

    return growth


def detect_trends(annual: list[dict], metrics: dict) -> list[dict]:
    """
    Detect basic mechanical trends. No interpretation — just direction.
    Returns list of {"metric", "direction", "detail"} dicts.
    """
    trends = []

    if len(annual) < 3:
        return trends

    def _trend_direction(vals):
        """Determine UP/DOWN/FLAT from 3+ data points."""
        if len(vals) < 3:
            return None, None
        recent = vals[-3:]
        ups = sum(1 for i in range(1, len(recent)) if recent[i] is not None and recent[i - 1] is not None and recent[i] > recent[i - 1])
        downs = sum(1 for i in range(1, len(recent)) if recent[i] is not None and recent[i - 1] is not None and recent[i] < recent[i - 1])
        if ups >= 2:
            return "UP", f"Increased in {ups} of last {len(recent)-1} periods"
        elif downs >= 2:
            return "DOWN", f"Decreased in {downs} of last {len(recent)-1} periods"
        else:
            return "FLAT", "No clear direction"

    # Revenue trend
    rev_vals = [r.get("revenue") for r in annual]
    direction, detail = _trend_direction(rev_vals)
    if direction:
        trends.append({"metric": "revenue", "direction": direction, "detail": detail})

    # Margin trends (compute margins per year)
    for margin_name, num_field in [
        ("gross_margin", "gross_profit"),
        ("operating_margin", "operating_income"),
        ("net_margin", "net_income"),
    ]:
        margin_vals = [_safe_pct(r.get(num_field), r.get("revenue")) for r in annual]
        direction, detail = _trend_direction(margin_vals)
        if direction:
            trends.append({"metric": margin_name, "direction": direction, "detail": detail})

    # Debt trend
    debt_vals = [r.get("total_debt") or r.get("long_term_debt") for r in annual]
    direction, detail = _trend_direction(debt_vals)
    if direction:
        trends.append({"metric": "total_debt", "direction": direction, "detail": detail})

    return trends


# ─── Main Normalization Function ─────────────────────────────────────────────

def normalize_company(company_result: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a single company's research result into the unified schema.

    Takes the output of research_company() from the orchestrator.
    Returns the unified normalized dataset.

    Source priority for financial data:
      US companies:  SEC XBRL → Yahoo Finance (supplement/gap-fill)
      TASE companies: Yahoo Finance → Maya (backup)
    """
    norm = empty_normalized()
    exchange = company_result.get("exchange", "UNKNOWN")
    norm["exchange"] = exchange
    norm["company_name"] = company_result.get("company_name")

    sources_used = []
    gaps = []
    warnings = []

    # ── Extract identity from best available source ──
    yahoo_data = company_result.get("yahoo")
    sec_data = company_result.get("sec")
    maya_data = company_result.get("maya")

    # Yahoo ticker
    if yahoo_data:
        norm["ticker"] = yahoo_data.get("ticker")
    elif sec_data:
        norm["ticker"] = sec_data.get("ticker")

    # Extract Yahoo structured data (from orchestrator, NO network calls)
    yahoo_info = {}
    yahoo_annual = []

    if yahoo_data and yahoo_data.get("structured"):
        try:
            yahoo_annual, yahoo_info = _extract_yahoo_annual(yahoo_data)
            if yahoo_annual:
                sources_used.append("Yahoo Finance")
        except Exception as e:
            warnings.append(f"Yahoo extraction error: {e}")
    elif yahoo_data and yahoo_data.get("info"):
        # Structured data missing but info available (legacy cache)
        yahoo_info = yahoo_data.get("info", {})

    # Fill profile fields
    norm["currency"] = yahoo_info.get("financialCurrency") or yahoo_info.get("currency")
    norm["sector"] = yahoo_info.get("sector")
    norm["industry"] = yahoo_info.get("industry")
    norm["market_cap"] = _safe_float(yahoo_info.get("marketCap"))
    norm["enterprise_value"] = _safe_float(yahoo_info.get("enterpriseValue"))
    norm["employees"] = yahoo_info.get("fullTimeEmployees")

    # Fiscal year end detection
    if yahoo_annual:
        last_period_end = yahoo_annual[-1].get("period_end", "")
        if last_period_end and len(last_period_end) >= 7:
            try:
                norm["fiscal_year_end_month"] = int(last_period_end[5:7])
            except ValueError:
                pass

    # SEC profile (US companies)
    if sec_data and sec_data.get("details"):
        d = sec_data["details"]
        if not norm["company_name"]:
            norm["company_name"] = d.get("name")
        if not norm["sector"]:
            norm["sector"] = d.get("sicDescription")
        if not norm["description"]:
            norm["description"] = d.get("sicDescription")
        if d.get("fiscalYearEnd") and len(d["fiscalYearEnd"]) >= 2:
            try:
                norm["fiscal_year_end_month"] = int(d["fiscalYearEnd"][:2])
            except ValueError:
                pass
        sources_used.append("SEC EDGAR")

    # Maya profile (TASE companies)
    if maya_data and maya_data.get("details"):
        d = maya_data["details"]
        if not norm["company_name"]:
            norm["company_name"] = d.get("longName") or d.get("name")
        if not norm["sector"]:
            norm["sector"] = d.get("sector")
        if not norm["description"]:
            norm["description"] = d.get("description")
        # Maya market cap is in K NIS
        if not norm["market_cap"] and d.get("marketValue"):
            norm["market_cap"] = d["marketValue"] * 1000  # Convert K NIS to NIS
            if not norm["currency"]:
                norm["currency"] = "ILS"
        sources_used.append("Maya TASE")

    # ── Select best financial data based on hierarchy ──
    sec_annual = []
    maya_annual = []

    if sec_data:
        try:
            sec_annual = _extract_sec_annual(sec_data)
        except Exception as e:
            warnings.append(f"SEC extraction error: {e}")

    if maya_data:
        try:
            maya_annual, maya_warnings = _extract_maya_financials(maya_data)
            warnings.extend(maya_warnings)
        except Exception as e:
            warnings.append(f"Maya extraction error: {e}")

    # Determine primary annual data
    if exchange == "US":
        # US: SEC primary, Yahoo gap-fill
        if sec_annual:
            primary_annual = sec_annual
            # Gap-fill from Yahoo: for each year, fill None fields from Yahoo
            if yahoo_annual:
                _merge_annual(primary_annual, yahoo_annual)
        elif yahoo_annual:
            primary_annual = yahoo_annual
            warnings.append("SEC XBRL unavailable — using Yahoo Finance as primary")
        else:
            primary_annual = []
            gaps.append("No annual financial data from SEC or Yahoo")

    elif exchange == "TASE":
        # TASE: Yahoo primary, Maya backup
        if yahoo_annual:
            primary_annual = yahoo_annual
        elif maya_annual:
            primary_annual = maya_annual
            warnings.append("Yahoo Finance unavailable — using Maya as primary (limited data)")
        else:
            primary_annual = []
            gaps.append("No annual financial data from Yahoo or Maya")

    else:
        # UNKNOWN: use best available
        primary_annual = yahoo_annual or sec_annual or maya_annual
        if not primary_annual:
            gaps.append("No annual financial data from any source")

    norm["annual"] = primary_annual
    norm["data_quality"]["years_available"] = len(primary_annual)

    # ── Compute metrics ──
    norm["metrics"] = compute_metrics(primary_annual, yahoo_info)

    # ── Compute growth ──
    norm["growth"] = compute_growth(primary_annual)

    # ── Detect trends ──
    norm["trends"] = detect_trends(primary_annual, norm["metrics"])

    # ── Data quality assessment ──
    norm["data_quality"]["sources_used"] = list(dict.fromkeys(sources_used))  # dedup, preserve order
    norm["data_quality"]["gaps"] = gaps + company_result.get("data_gaps", [])
    norm["data_quality"]["warnings"] = warnings

    # Check for mixed currencies in case this is used for peer comparison later
    if norm["currency"] and norm["currency"] not in ("USD", "ILS"):
        warnings.append(f"Unusual reporting currency: {norm['currency']}")

    # Completeness score: fraction of CANONICAL metrics that are non-None
    filled = sum(1 for k in CANONICAL_METRICS if norm["metrics"].get(k) is not None)
    norm["data_quality"]["completeness"] = round(filled / len(CANONICAL_METRICS), 2)

    return norm


def _merge_annual(primary: list[dict], secondary: list[dict]) -> None:
    """
    Merge secondary annual data into primary, filling None fields only.
    Matches by fiscal_year. Modifies primary in place.
    """
    secondary_by_fy = {}
    for row in secondary:
        fy = row.get("fiscal_year")
        if fy:
            secondary_by_fy[fy] = row

    for row in primary:
        fy = row.get("fiscal_year")
        if fy and fy in secondary_by_fy:
            sec_row = secondary_by_fy[fy]
            for key, val in sec_row.items():
                if key in ("period", "period_end", "fiscal_year", "is_annual"):
                    continue
                if row.get(key) is None and val is not None:
                    row[key] = val


# ─── Peer Comparison Table ───────────────────────────────────────────────────

def build_peer_table(normalized_companies: list[dict]) -> dict[str, Any]:
    """
    Build a side-by-side peer comparison table from normalized company data.

    Returns dict with:
      - "companies": list of company names
      - "metrics": dict of metric_name → list of values (one per company)
      - "growth": dict of metric_name → list of CAGR values
    """
    if not normalized_companies:
        return {"companies": [], "metrics": {}, "growth": {}}

    names = [c.get("company_name", "Unknown") for c in normalized_companies]

    metrics_table = {}
    for key in PEER_TABLE_METRICS:
        vals = []
        for c in normalized_companies:
            if key.endswith("_yoy"):
                base_key = key.replace("_yoy", "")
                growth_data = c.get("growth", {}).get(base_key, {})
                yoy_list = growth_data.get("yoy", [])
                vals.append(yoy_list[-1]["growth"] if yoy_list else None)
            else:
                vals.append(c.get("metrics", {}).get(key))
        metrics_table[key] = vals

    # CAGR comparison
    cagr_table = {}
    for metric in ["revenue", "ebitda", "net_income"]:
        for period in ["cagr_3y", "cagr_5y"]:
            key = f"{metric}_{period}"
            vals = []
            for c in normalized_companies:
                growth_data = c.get("growth", {}).get(metric, {})
                vals.append(growth_data.get(period))
            cagr_table[key] = vals

    # Currency mismatch warning
    currencies = [c.get("currency", "N/A") for c in normalized_companies]
    unique_currencies = set(c for c in currencies if c and c != "N/A")
    currency_warning = None
    if len(unique_currencies) > 1:
        currency_warning = (
            f"Mixed currencies detected: {', '.join(sorted(unique_currencies))}. "
            f"Ratio metrics (margins, multiples) are comparable. "
            f"Absolute values (FCF, revenue) are NOT directly comparable."
        )

    return {
        "companies": names,
        "exchanges": [c.get("exchange", "N/A") for c in normalized_companies],
        "currencies": currencies,
        "sectors": [c.get("sector", "N/A") for c in normalized_companies],
        "metrics": metrics_table,
        "cagr": cagr_table,
        "completeness": [c.get("data_quality", {}).get("completeness", 0) for c in normalized_companies],
        "currency_warning": currency_warning,
        "absolute_metrics": list(ABSOLUTE_METRICS),
    }


# ─── Format Helpers ──────────────────────────────────────────────────────────

def fmt_pct(v: Optional[float], decimals: int = 1) -> str:
    """Format a decimal ratio as percentage string."""
    if v is None:
        return "—"
    return f"{v * 100:.{decimals}f}%"


def fmt_millions(v: Optional[float], currency: str = "") -> str:
    """Format a value in millions."""
    if v is None:
        return "—"
    prefix = f"{currency} " if currency else ""
    abs_v = abs(v)
    if abs_v >= 1_000_000_000:
        return f"{prefix}{v / 1_000_000_000:,.1f}B"
    elif abs_v >= 1_000_000:
        return f"{prefix}{v / 1_000_000:,.0f}M"
    elif abs_v >= 1_000:
        return f"{prefix}{v / 1_000:,.0f}K"
    else:
        return f"{prefix}{v:,.0f}"


def fmt_ratio(v: Optional[float], decimals: int = 2) -> str:
    """Format a ratio."""
    if v is None:
        return "—"
    return f"{v:.{decimals}f}"
