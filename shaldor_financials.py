"""
Shaldor Financial Data Scraper
Pulls financials from Yahoo Finance via yfinance and outputs structured text for LLM consumption.
Line items shown are subject to data availability per ticker/market.
Usage: python shaldor_financials.py OPCE.TA
"""

import yfinance as yf
import pandas as pd
import sys
from collections import defaultdict
from datetime import datetime, timezone


def get_financials(ticker_str: str) -> str:
    """Fetch financials for a ticker and return formatted text block.
    
    Returns a structured text string on success, or an error message on failure.
    """
    
    warnings = []  # Collected separately, rendered at end of report
    
    # --- Safe fetch of ticker and info ---
    try:
        t = yf.Ticker(ticker_str)
    except Exception as e:
        return f"[ERROR] Failed to create ticker object for '{ticker_str}': {e}"
    
    try:
        info = t.info or {}
    except Exception as e:
        warnings.append(f"Could not fetch company info: {e}")
        info = {}
    
    # --- Ticker validation ---
    # Don't rely solely on info — also check if financial data exists.
    # Some real tickers have sparse info but valid financials.
    has_info = bool(info.get('shortName') or info.get('longName') or info.get('symbol'))
    
    if not has_info:
        # Info is empty/useless — try financial statements as fallback check
        has_financials = False
        for fetch_attr in ('financials', 'balance_sheet', 'cashflow'):
            try:
                test_df = getattr(t, fetch_attr)
                if test_df is not None and not test_df.empty:
                    has_financials = True
                    break
            except Exception:
                continue
        
        if not has_financials:
            return f"[ERROR] Ticker '{ticker_str}' not found or returned no data from Yahoo Finance."
    
    # --- Detect currency and company info ---
    fin_currency = info.get('financialCurrency', info.get('currency', 'N/A'))
    price_currency = info.get('currency', 'N/A')
    exchange = info.get('exchange', 'N/A')
    name = info.get('longName', info.get('shortName', ticker_str))
    sector = info.get('sector', 'N/A')
    industry = info.get('industry', 'N/A')
    
    # --- Formatting helpers ---
    
    def _isna(val):
        """Check if value is missing (None or NaN)."""
        if val is None:
            return True
        try:
            return pd.isna(val)
        except (TypeError, ValueError):
            return False
    
    def fmt(val):
        """Format number to millions with appropriate precision."""
        if _isna(val):
            return "N/A"
        v = val / 1_000_000
        if abs(v) >= 100:
            return f"{v:,.0f}"
        elif abs(v) >= 1:
            return f"{v:,.1f}"
        else:
            return f"{v:,.2f}"
    
    def fmt_eps(val):
        """Format per-share values (not in millions). 2 decimal places."""
        if _isna(val):
            return "N/A"
        return f"{val:,.2f}"
    
    def fmt_count(val):
        """Format count values like shares (in millions, 1 decimal)."""
        if _isna(val):
            return "N/A"
        v = val / 1_000_000
        if abs(v) >= 100:
            return f"{v:,.0f}"
        else:
            return f"{v:,.1f}"
    
    def fmt_pct(val):
        """Format as percentage."""
        if _isna(val):
            return "N/A"
        return f"{val*100:.1f}%"
    
    def fmt_ratio(val):
        """Format ratio with 2 decimal places."""
        if _isna(val):
            return "N/A"
        return f"{val:.2f}"
    
    # --- Build output ---
    lines = []
    lines.append(f"{'='*70}")
    lines.append(f"FINANCIAL DATA: {name}")
    lines.append(f"Ticker: {ticker_str} | Exchange: {exchange} | Currency: {fin_currency}")
    lines.append(f"Sector: {sector} | Industry: {industry}")
    lines.append(f"Data pulled: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"All figures in millions of {fin_currency} unless noted otherwise.")
    lines.append(f"EPS is per-share in {fin_currency}. Share counts are in millions.")
    if price_currency != fin_currency:
        lines.append(f"Note: Stock price is denominated in {price_currency}.")
    lines.append(f"{'='*70}")
    
    # =========================================================
    # KEY RATIOS & MARKET DATA
    # =========================================================
    lines.append(f"\n## KEY RATIOS & MARKET DATA (TTM/Current)")
    lines.append(f"")
    
    market_cap = info.get('marketCap')
    ev = info.get('enterpriseValue')
    
    ratio_rows = [
        ("Market Cap", fmt(market_cap) if market_cap is not None else "N/A"),
        ("Enterprise Value", fmt(ev) if ev is not None else "N/A"),
        ("Trailing P/E", fmt_ratio(info.get('trailingPE'))),
        ("Forward P/E", fmt_ratio(info.get('forwardPE'))),
        ("P/B", fmt_ratio(info.get('priceToBook'))),
        ("EV/Revenue", fmt_ratio(info.get('enterpriseToRevenue'))),
        ("EV/EBITDA", fmt_ratio(info.get('enterpriseToEbitda'))),
        ("", ""),
        ("Gross Margin", fmt_pct(info.get('grossMargins'))),
        ("Operating Margin", fmt_pct(info.get('operatingMargins'))),
        ("Net Margin", fmt_pct(info.get('profitMargins'))),
        ("", ""),
        ("ROE", fmt_pct(info.get('returnOnEquity'))),
        ("ROA", fmt_pct(info.get('returnOnAssets'))),
        ("", ""),
        ("Debt/Equity", fmt_ratio(info.get('debtToEquity'))),
        ("Current Ratio", fmt_ratio(info.get('currentRatio'))),
        ("Quick Ratio", fmt_ratio(info.get('quickRatio'))),
        ("", ""),
        ("Revenue Growth (YoY)", fmt_pct(info.get('revenueGrowth'))),
        ("Earnings Growth (YoY)", fmt_pct(info.get('earningsGrowth'))),
        ("", ""),
        ("Beta", fmt_ratio(info.get('beta'))),
        ("52W High", fmt_ratio(info.get('fiftyTwoWeekHigh'))),
        ("52W Low", fmt_ratio(info.get('fiftyTwoWeekLow'))),
        ("Dividend Yield", fmt_pct(info.get('dividendYield'))),
        ("Payout Ratio", fmt_pct(info.get('payoutRatio'))),
    ]
    
    for label, val in ratio_rows:
        if label == "":
            continue
        lines.append(f"  {label:<25} {val}")
    
    # =========================================================
    # HELPER: Sort columns defensively
    # =========================================================
    def sort_date_columns(df):
        """Sort DataFrame columns chronologically. Defensive against non-date columns."""
        try:
            date_cols = [c for c in df.columns if hasattr(c, 'date')]
            non_date_cols = [c for c in df.columns if not hasattr(c, 'date')]
            return df[sorted(date_cols) + non_date_cols]
        except Exception:
            return df
    
    # =========================================================
    # HELPER: Format a DataFrame as a table
    # =========================================================
    def df_to_table(df, selected_items=None, item_rename=None, eps_items=None, count_items=None):
        """Convert a yfinance DataFrame to a readable text table."""
        if df is None or df.empty:
            return ["  No data available."]
        
        if item_rename is None:
            item_rename = {}
        if eps_items is None:
            eps_items = set()
        if count_items is None:
            count_items = set()
        
        df = sort_date_columns(df)
        
        years = []
        for c in df.columns:
            if hasattr(c, 'date'):
                years.append(str(c.date().year))
            else:
                years.append(str(c))
        
        tlines = []
        header = f"  {'Line Item':<45}" + "".join(f"{y:>14}" for y in years)
        tlines.append(header)
        tlines.append("  " + "-" * (45 + 14 * len(years)))
        
        items = selected_items if selected_items else df.index.tolist()
        
        for item in items:
            if item not in df.index:
                continue
            display_name = item_rename.get(item, item)
            if item in eps_items:
                vals = "".join(f"{fmt_eps(df.loc[item, c]):>14}" for c in df.columns)
            elif item in count_items:
                vals = "".join(f"{fmt_count(df.loc[item, c]):>14}" for c in df.columns)
            else:
                vals = "".join(f"{fmt(df.loc[item, c]):>14}" for c in df.columns)
            tlines.append(f"  {display_name:<45}{vals}")
        
        return tlines
    
    # =========================================================
    # Safe data fetching — warnings collected, not inlined
    # =========================================================
    def safe_fetch(fetch_fn, label):
        """Safely fetch a DataFrame from yfinance."""
        try:
            result = fetch_fn()
            if result is None or (hasattr(result, 'empty') and result.empty):
                return None
            return result
        except Exception as e:
            warnings.append(f"Could not fetch {label}: {e}")
            return None
    
    # =========================================================
    # INCOME STATEMENT
    # =========================================================
    lines.append(f"\n## INCOME STATEMENT (Annual)")
    
    inc_items = [
        'Total Revenue',
        'Cost Of Revenue',
        'Gross Profit',
        'Research And Development',
        'Selling General And Administration',
        'Operating Expense',
        'Operating Income',
        'Total Operating Income As Reported',
        'Net Non Operating Interest Income Expense',
        'Interest Expense',
        'Interest Income',
        'Other Non Operating Income Expenses',
        'Pretax Income',
        'Tax Provision',
        'Net Income Including Noncontrolling Interests',
        'Minority Interests',
        'Net Income',
        'EBITDA',
        'Normalized EBITDA',
        'Reconciled Depreciation',
        'Basic EPS',
        'Diluted EPS',
        'Basic Average Shares',
        'Diluted Average Shares',
    ]
    
    inc_rename = {
        'Net Non Operating Interest Income Expense': 'Net Interest Income/Expense',
        'Other Non Operating Income Expenses': 'Other Non-Operating Items',
        'Net Income Including Noncontrolling Interests': 'Net Income (incl. minorities)',
        'Minority Interests': 'Minority Interest',
        'Net Income': 'Net Income (to parent)',
        'Reconciled Depreciation': 'D&A',
        'Selling General And Administration': 'SG&A',
        'Research And Development': 'R&D',
        'Total Operating Income As Reported': 'Operating Income (as reported)',
    }
    
    inc = safe_fetch(lambda: t.financials, "Income Statement")
    lines.extend(df_to_table(
        inc, inc_items, inc_rename,
        eps_items={'Basic EPS', 'Diluted EPS'},
        count_items={'Basic Average Shares', 'Diluted Average Shares'},
    ))
    
    # =========================================================
    # BALANCE SHEET
    # =========================================================
    lines.append(f"\n## BALANCE SHEET (Annual)")
    
    bs_items = [
        'Total Assets',
        'Current Assets',
        'Cash And Cash Equivalents',
        'Other Short Term Investments',
        'Receivables',
        'Inventory',
        'Total Non Current Assets',
        'Net PPE',
        'Goodwill And Other Intangible Assets',
        'Long Term Equity Investment',
        'Total Liabilities Net Minority Interest',
        'Current Liabilities',
        'Current Debt',
        'Accounts Payable',
        'Total Non Current Liabilities Net Minority Interest',
        'Long Term Debt',
        'Total Equity Gross Minority Interest',
        'Stockholders Equity',
        'Common Stock',
        'Additional Paid In Capital',
        'Retained Earnings',
        'Minority Interest',
        'Total Debt',
        'Net Debt',
        'Working Capital',
        'Invested Capital',
        'Tangible Book Value',
        'Share Issued',
    ]
    
    bs_rename = {
        'Total Liabilities Net Minority Interest': 'Total Liabilities',
        'Total Non Current Liabilities Net Minority Interest': 'Non-Current Liabilities',
        'Total Equity Gross Minority Interest': 'Total Equity (incl. minorities)',
        'Stockholders Equity': 'Equity (to parent)',
        'Cash And Cash Equivalents': 'Cash & Equivalents',
        'Other Short Term Investments': 'Short-Term Investments',
        'Goodwill And Other Intangible Assets': 'Goodwill & Intangibles',
        'Long Term Equity Investment': 'LT Equity Investments',
        'Net PPE': 'PP&E (net)',
        'Share Issued': 'Shares Outstanding',
        'Additional Paid In Capital': 'Additional Paid-In Capital',
    }
    
    bs = safe_fetch(lambda: t.balance_sheet, "Balance Sheet")
    lines.extend(df_to_table(
        bs, bs_items, bs_rename,
        count_items={'Share Issued'},
    ))
    
    # =========================================================
    # CASH FLOW
    # =========================================================
    lines.append(f"\n## CASH FLOW STATEMENT (Annual)")
    
    cf_items = [
        'Operating Cash Flow',
        'Capital Expenditure',
        'Free Cash Flow',
        'Investing Cash Flow',
        'Financing Cash Flow',
        'Issuance Of Capital Stock',
        'Issuance Of Debt',
        'Repayment Of Debt',
        'Net Issuance Payments Of Debt',
        'Net Common Stock Issuance',
        'Interest Paid Cff',
        'Changes In Cash',
        'End Cash Position',
        'Beginning Cash Position',
        'Effect Of Exchange Rate Changes',
        'Depreciation And Amortization',
        'Change In Working Capital',
        'Stock Based Compensation',
    ]
    
    cf_rename = {
        'Operating Cash Flow': 'Cash from Operations',
        'Capital Expenditure': 'CapEx',
        'Investing Cash Flow': 'Cash from Investing',
        'Financing Cash Flow': 'Cash from Financing',
        'Issuance Of Capital Stock': 'Equity Issuance',
        'Issuance Of Debt': 'Debt Issuance',
        'Repayment Of Debt': 'Debt Repayment',
        'Net Issuance Payments Of Debt': 'Net Debt Issuance',
        'Net Common Stock Issuance': 'Net Equity Issuance',
        'Interest Paid Cff': 'Interest Paid',
        'Changes In Cash': 'Net Change in Cash',
        'Effect Of Exchange Rate Changes': 'FX Effect',
        'Depreciation And Amortization': 'D&A',
        'Change In Working Capital': 'Working Capital Changes',
        'Stock Based Compensation': 'Stock-Based Comp',
    }
    
    cf = safe_fetch(lambda: t.cashflow, "Cash Flow")
    lines.extend(df_to_table(cf, cf_items, cf_rename))
    
    # =========================================================
    # PARTIAL-YEAR QUARTERLY DATA
    # =========================================================
    # Show quarterly breakdown for fiscal years not yet in the annual data,
    # with a summary column (e.g., "9M", "6M", "3M").
    
    qi = safe_fetch(lambda: t.quarterly_financials, "Quarterly Income")
    qcf = safe_fetch(lambda: t.quarterly_cashflow, "Quarterly Cash Flow")
    
    # Determine which years are already covered by annual statements
    annual_years = set()
    for src in (inc, bs, cf):
        if src is not None and not src.empty:
            for c in src.columns:
                if hasattr(c, 'year'):
                    annual_years.add(c.year)
    
    def get_partial_year_cols(qdf):
        """Return {year: [cols]} for years NOT in annual data."""
        if qdf is None or qdf.empty:
            return {}
        qdf = sort_date_columns(qdf)
        by_year = defaultdict(list)
        for c in qdf.columns:
            if hasattr(c, 'year') and c.year not in annual_years:
                by_year[c.year].append(c)
        return dict(by_year)
    
    def quarterly_table_with_sum(qdf, year_cols, selected_items, item_rename=None,
                                  eps_items=None, count_items=None):
        """Build a table: Q1 | Q2 | Q3 | ... | NM summary column."""
        if item_rename is None:
            item_rename = {}
        if eps_items is None:
            eps_items = set()
        if count_items is None:
            count_items = set()
        
        cols = sorted(year_cols)
        n = len(cols)
        q_labels = [f"Q{(c.month-1)//3+1}" for c in cols]
        sum_label = f"{n*3}M"
        
        tlines = []
        header = f"  {'Line Item':<45}" + "".join(f"{q:>14}" for q in q_labels) + f"{sum_label:>14}"
        tlines.append(header)
        tlines.append("  " + "-" * (45 + 14 * (n + 1)))
        
        for item in selected_items:
            if item not in qdf.index:
                continue
            display_name = item_rename.get(item, item)
            
            vals_raw = [qdf.loc[item, c] for c in cols]
            valid_vals = [v for v in vals_raw if not _isna(v)]
            
            if item in eps_items:
                val_strs = [f"{fmt_eps(v):>14}" for v in vals_raw]
                # EPS sum doesn't make sense — show N/A for sum
                sum_str = f"{'N/A':>14}"
            elif item in count_items:
                val_strs = [f"{fmt_count(v):>14}" for v in vals_raw]
                # Share count sum doesn't make sense — show latest
                latest = vals_raw[-1] if vals_raw else None
                sum_str = f"{fmt_count(latest):>14}"
            else:
                val_strs = [f"{fmt(v):>14}" for v in vals_raw]
                if len(valid_vals) == n:
                    sum_str = f"{fmt(sum(valid_vals)):>14}"
                else:
                    sum_str = f"{'N/A':>14}"
            
            tlines.append(f"  {display_name:<45}" + "".join(val_strs) + sum_str)
        
        return tlines
    
    # Build partial-year tables for income statement items
    qi_partial = get_partial_year_cols(qi)
    qcf_partial = get_partial_year_cols(qcf)
    
    # Merge years from both sources
    partial_years = sorted(set(list(qi_partial.keys()) + list(qcf_partial.keys())))
    
    if partial_years:
        for year in partial_years:
            qi_cols = qi_partial.get(year, [])
            qcf_cols = qcf_partial.get(year, [])
            n_q = max(len(qi_cols), len(qcf_cols))
            
            lines.append(f"\n## QUARTERLY DATA — {year} ({n_q*3}M, {n_q} quarter{'s' if n_q != 1 else ''})")
            
            # Income statement quarterly
            if qi_cols and qi is not None:
                if len(qi_cols) != n_q:
                    lines.append(f"\n  Income Statement ({len(qi_cols)} quarters available):")
                else:
                    lines.append(f"\n  Income Statement:")
                q_inc_items = [
                    'Total Revenue', 'Cost Of Revenue', 'Gross Profit',
                    'Operating Expense', 'Operating Income',
                    'Interest Expense', 'Interest Income',
                    'Pretax Income', 'Tax Provision',
                    'Net Income Including Noncontrolling Interests',
                    'Net Income', 'EBITDA',
                    'Basic EPS', 'Diluted EPS',
                ]
                lines.extend(quarterly_table_with_sum(
                    qi, qi_cols, q_inc_items, inc_rename,
                    eps_items={'Basic EPS', 'Diluted EPS'},
                ))
            
            # Cash flow quarterly
            if qcf_cols and qcf is not None:
                if len(qcf_cols) != n_q:
                    lines.append(f"\n  Cash Flow ({len(qcf_cols)} quarters available):")
                else:
                    lines.append(f"\n  Cash Flow:")
                q_cf_items = [
                    'Operating Cash Flow', 'Capital Expenditure', 'Free Cash Flow',
                    'Investing Cash Flow', 'Financing Cash Flow',
                    'Depreciation And Amortization',
                ]
                lines.extend(quarterly_table_with_sum(
                    qcf, qcf_cols, q_cf_items, cf_rename,
                ))
    else:
        lines.append(f"\n## QUARTERLY DATA")
        lines.append(f"  No partial-year quarterly data available (all quarters covered by annual statements).")
    
    # =========================================================
    # TTM (Trailing Twelve Months)
    # =========================================================
    # Computed by summing the last 4 available quarters.
    # Complements the partial-year view with a rolling 12-month perspective.
    
    lines.append(f"\n## TTM ESTIMATES (Trailing Twelve Months)")
    
    def calc_ttm(qdf, item):
        """Sum the last 4 quarters for a given line item. Returns value or None."""
        if qdf is None or qdf.empty or item not in qdf.index:
            return None
        qdf_sorted = sort_date_columns(qdf)
        if len(qdf_sorted.columns) < 4:
            return None
        last4 = qdf_sorted.columns[-4:]
        vals = [qdf_sorted.loc[item, c] for c in last4]
        if any(_isna(v) for v in vals):
            return None
        return sum(vals)
    
    if qi is not None and not qi.empty:
        qi_sorted = sort_date_columns(qi)
        n_quarters = len(qi_sorted.columns)
        
        if n_quarters >= 4:
            last4_cols = qi_sorted.columns[-4:]
            last4_dates = [str(c.date()) for c in last4_cols if hasattr(c, 'date')]
            lines.append(f"  Based on latest 4 quarters: {', '.join(last4_dates)}")
        else:
            lines.append(f"  Only {n_quarters} quarter(s) available — need 4 for TTM.")
            warnings.append(f"TTM: only {n_quarters} quarterly period(s) available, need 4.")
        
        lines.append(f"")
        
        ttm_items = [
            ('Total Revenue', qi),
            ('Gross Profit', qi),
            ('Operating Income', qi),
            ('EBITDA', qi),
            ('Net Income', qi),
            ('Operating Cash Flow', qcf),
            ('Capital Expenditure', qcf),
            ('Free Cash Flow', qcf),
        ]
        
        ttm_rename = {
            'Operating Cash Flow': 'Cash from Operations',
            'Capital Expenditure': 'CapEx',
        }
        
        for item, source_df in ttm_items:
            ttm_val = calc_ttm(source_df, item)
            display_name = ttm_rename.get(item, item)
            if ttm_val is not None:
                lines.append(f"  {display_name:<35} {fmt(ttm_val):>14}")
            else:
                lines.append(f"  {display_name:<35} {'N/A':>14}")
        
        # TTM margins
        ttm_rev = calc_ttm(qi, 'Total Revenue')
        ttm_gp = calc_ttm(qi, 'Gross Profit')
        ttm_op = calc_ttm(qi, 'Operating Income')
        ttm_ni = calc_ttm(qi, 'Net Income')
        ttm_ebitda = calc_ttm(qi, 'EBITDA')
        
        lines.append(f"")
        lines.append(f"  TTM Margins:")
        if ttm_rev is not None and ttm_rev != 0:
            if ttm_gp is not None:
                lines.append(f"    Gross Margin:      {ttm_gp/ttm_rev*100:.1f}%")
            if ttm_op is not None:
                lines.append(f"    Operating Margin:  {ttm_op/ttm_rev*100:.1f}%")
            if ttm_ni is not None:
                lines.append(f"    Net Margin:        {ttm_ni/ttm_rev*100:.1f}%")
            if ttm_ebitda is not None:
                lines.append(f"    EBITDA Margin:     {ttm_ebitda/ttm_rev*100:.1f}%")
        elif ttm_rev is not None and ttm_rev == 0:
            lines.append(f"    Revenue is zero — margins not calculable.")
        else:
            lines.append(f"    Revenue data unavailable for margin calculation.")
    else:
        lines.append(f"  Quarterly data not available for TTM calculation.")
    
    # =========================================================
    # WARNINGS (if any)
    # =========================================================
    if warnings:
        lines.append(f"\n## DATA WARNINGS")
        for w in dict.fromkeys(warnings):  # dedupe, preserve order
            lines.append(f"  - {w}")
    
    lines.append(f"\n{'='*70}")
    lines.append(f"END OF FINANCIAL DATA: {name}")
    lines.append(f"{'='*70}")
    
    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python shaldor_financials.py <TICKER>")
        print("Examples:")
        print("  python shaldor_financials.py OPCE.TA     # OPC Energy (TASE)")
        print("  python shaldor_financials.py ORON.TA     # Oron Group (TASE)")
        print("  python shaldor_financials.py AAPL        # Apple (NASDAQ)")
        sys.exit(1)
    
    ticker = sys.argv[1]
    output = get_financials(ticker)
    print(output)
