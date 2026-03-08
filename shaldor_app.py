"""
Shaldor Research Dashboard v2
─────────────────────────────
Streamlit UI for Phase A: collect, normalize, explore company research data.

Principles:
  1. UI does NO business logic — calls orchestrator + normalizer, displays results
  2. All display driven by normalized objects and canonical metric definitions
  3. Raw + normalized inspectors for debugging

Layout:
  1. Sidebar: Input / Controls
  2. Company Profile
  3. Financial Summary (driven by CANONICAL_METRICS)
  4. Metric Explorer (interactive: select metric, companies, chart type)
  5. Growth & Trends
  6. Peer Comparison (table + focused single-metric comparison)
  7. Data Quality + Inspectors (raw + normalized)
"""

import streamlit as st
import pandas as pd
import sys
import os
import json
import time

_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

# CRITICAL: Patch scrapers to use requests instead of curl BEFORE importing orchestrator
# This ensures all fetch functions use requests from the very first call
import sec_scraper
import maya_scraper
import shaldor_http
shaldor_http.patch_scrapers()

import shaldor_research_orchestrator as orch
from shaldor_normalizer import (
    normalize_company, build_peer_table, fmt_pct, fmt_millions, fmt_ratio,
    CANONICAL_METRICS, PEER_TABLE_METRICS, GROWTH_METRICS, ABSOLUTE_METRICS,
    PROFITABILITY_METRICS, EFFICIENCY_METRICS, CASH_FLOW_METRICS,
    LEVERAGE_METRICS, CAPITAL_STRUCTURE_METRICS, VALUATION_METRICS,
)

# ─── Display name mapping (canonical key → human label) ─────────────────────

METRIC_DISPLAY_NAMES = {
    # Profitability
    "gross_margin": "Gross Margin", "ebitda_margin": "EBITDA Margin",
    "operating_margin": "Operating Margin", "net_margin": "Net Margin", "roe": "ROE",
    # Efficiency
    "roa": "ROA", "asset_turnover": "Asset Turnover", "operating_leverage": "Op. Leverage",
    # Cash flow
    "fcf_margin": "FCF Margin", "cash_conversion": "Cash Conversion",
    # Leverage
    "debt_to_equity": "Debt/Equity", "debt_to_ebitda": "Debt/EBITDA",
    "net_debt_to_ebitda": "Net Debt/EBITDA",
    # Capital structure
    "equity_ratio": "Equity Ratio",
    # Valuation
    "pe_trailing": "P/E (Trailing)", "pe_forward": "P/E (Forward)",
    "ev_to_ebitda": "EV/EBITDA", "ev_to_revenue": "EV/Revenue",
    "price_to_sales": "Price/Sales", "fcf_yield": "FCF Yield",
    # Growth (YoY)
    "revenue_yoy": "Revenue YoY", "ebitda_yoy": "EBITDA YoY",
    "net_income_yoy": "Net Income YoY",
}

# Annual financials fields available for historical charting
ANNUAL_FIELDS = {
    "revenue": "Revenue", "gross_profit": "Gross Profit",
    "operating_income": "Operating Income", "ebitda": "EBITDA",
    "net_income": "Net Income", "total_assets": "Total Assets",
    "total_equity": "Total Equity", "total_debt": "Total Debt",
    "operating_cash_flow": "Op. Cash Flow", "free_cash_flow": "Free Cash Flow",
}

# Metric type classification
PCT_METRICS = (
    set(PROFITABILITY_METRICS) | set(CASH_FLOW_METRICS) |
    {"equity_ratio", "fcf_yield", "revenue_yoy", "ebitda_yoy", "net_income_yoy"}
)
RATIO_METRICS = set(EFFICIENCY_METRICS) | set(LEVERAGE_METRICS) | set(VALUATION_METRICS) - {"fcf_yield"}


def _fmt_metric(key: str, val) -> str:
    """Format a metric value based on its type."""
    if val is None:
        return "—"
    if key in PCT_METRICS:
        return fmt_pct(val)
    if key in RATIO_METRICS:
        return fmt_ratio(val)
    if key in ABSOLUTE_METRICS:
        return fmt_millions(val)
    return fmt_ratio(val)


# ─── Page Config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Shaldor Research",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Sidebar ─────────────────────────────────────────────────────────────────

st.sidebar.title("📊 Shaldor Research")
st.sidebar.markdown("---")

primary_input = st.sidebar.text_input(
    "Primary Company",
    placeholder="AAPL, אורון, OPC Energy...",
    help="Ticker, company name (Hebrew/English), CIK, or Maya ID",
)

peers_input = st.sidebar.text_area(
    "Peer Companies (one per line, max 12)",
    placeholder="MSFT\nGOOGL\nAMZN",
    height=120,
)

with st.sidebar.expander("Yahoo Ticker Overrides"):
    st.caption("For TASE companies without auto-detected Yahoo tickers")
    yahoo_overrides_input = st.sidebar.text_area(
        "company=TICKER (one per line)",
        placeholder="אורון=ORON.TA\nשיכון ובינוי=SKBN.TA",
        height=80,
        key="yahoo_overrides",
    )

with st.sidebar.expander("Settings"):
    years = st.slider("Years of data", 1, 10, 5)
    use_cache = st.checkbox("Use cache", True)

run_button = st.sidebar.button("🚀 Run Research", type="primary", use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.caption("Shaldor Strategy Consulting · From Insight to Impact")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def parse_peers(text: str) -> list[str]:
    if not text.strip():
        return []
    return [l.strip() for l in text.strip().split("\n") if l.strip()][:12]


def parse_yahoo_overrides(text: str) -> dict[str, str]:
    overrides = {}
    if not text.strip():
        return overrides
    for line in text.strip().split("\n"):
        if "=" in line:
            k, v = line.split("=", 1)
            overrides[k.strip()] = v.strip()
    return overrides


# ─── Run Research ────────────────────────────────────────────────────────────

def run_and_display():
    peers = parse_peers(peers_input)
    yahoo_tickers = parse_yahoo_overrides(yahoo_overrides_input)

    status = st.status(f"Researching {1 + len(peers)} companies...", expanded=True)

    def progress_cb(msg):
        status.update(label=msg)

    t0 = time.time()
    result = orch.run_research(
        primary=primary_input.strip(),
        peers=peers,
        years=years,
        yahoo_tickers=yahoo_tickers,
        progress_callback=progress_cb,
        use_cache=use_cache,
    )
    duration = round(time.time() - t0, 1)

    status.update(label="Normalizing data...")
    primary_norm = normalize_company(result["primary"])
    peers_norm = [normalize_company(p) for p in result["peers"]]
    all_norm = [primary_norm] + peers_norm
    peer_table = build_peer_table(all_norm) if peers_norm else None

    total_gaps = sum(len(n.get("data_quality", {}).get("gaps", [])) for n in all_norm)
    status.update(
        label=f"Done in {duration}s — {len(all_norm)} companies, {total_gaps} data gaps",
        state="complete", expanded=False,
    )

    st.session_state["result"] = result
    st.session_state["primary_norm"] = primary_norm
    st.session_state["peers_norm"] = peers_norm
    st.session_state["all_norm"] = all_norm
    st.session_state["peer_table"] = peer_table
    st.session_state["duration"] = duration


# ─── Area 2: Company Profile ────────────────────────────────────────────────

def display_profile(norm: dict):
    st.header(norm.get("company_name", "Unknown"))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Exchange", norm.get("exchange", "N/A"))
    c2.metric("Sector", norm.get("sector") or "N/A")
    c3.metric("Currency", norm.get("currency") or "N/A")
    c4.metric("FY End", f"Month {norm.get('fiscal_year_end_month', '?')}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Market Cap", fmt_millions(norm.get("market_cap"), norm.get("currency", "")))
    c2.metric("Enterprise Value", fmt_millions(norm.get("enterprise_value"), norm.get("currency", "")))
    c3.metric("Industry", norm.get("industry") or "N/A")
    c4.metric("Data Years", str(norm.get("data_quality", {}).get("years_available", 0)))

    if norm.get("description"):
        st.caption(norm["description"])


# ─── Area 3: Financial Summary (driven by canonical metrics, compact layout) ──

MAX_COLS_PER_ROW = 5  # Cap columns to avoid crowding on small screens

def display_financials(norm: dict):
    st.subheader("Key Metrics")
    metrics = norm.get("metrics", {})

    metric_groups = [
        ("Profitability", PROFITABILITY_METRICS),
        ("Efficiency & Returns", EFFICIENCY_METRICS),
        ("Cash Flow", CASH_FLOW_METRICS),
        ("Leverage", LEVERAGE_METRICS + CAPITAL_STRUCTURE_METRICS),
        ("Valuation", VALUATION_METRICS),
    ]

    for group_name, group_keys in metric_groups:
        st.markdown(f"**{group_name}**")
        # Split into rows of MAX_COLS_PER_ROW
        for row_start in range(0, len(group_keys), MAX_COLS_PER_ROW):
            row_keys = group_keys[row_start:row_start + MAX_COLS_PER_ROW]
            cols = st.columns(MAX_COLS_PER_ROW)
            for i, key in enumerate(row_keys):
                label = METRIC_DISPLAY_NAMES.get(key, key)
                cols[i].metric(label, _fmt_metric(key, metrics.get(key)))

    # Annual financials table
    annual = norm.get("annual", [])
    if annual:
        st.subheader("Annual Financials")
        periods = [r.get("period", "?") for r in annual]

        table_data = {}
        for field, label in ANNUAL_FIELDS.items():
            row = {}
            for j, r in enumerate(annual):
                v = r.get(field)
                row[periods[j]] = fmt_millions(v) if v is not None else "—"
            table_data[label] = row

        df = pd.DataFrame(table_data).T
        st.dataframe(df, use_container_width=True)


# ─── Area 4: Metric Explorer (interactive, with growth series) ───────────────

# Growth series display names
GROWTH_DISPLAY_NAMES = {
    "revenue": "Revenue YoY Growth",
    "ebitda": "EBITDA YoY Growth",
    "net_income": "Net Income YoY Growth",
    "operating_income": "Operating Income YoY Growth",
    "free_cash_flow": "FCF YoY Growth",
}

def display_metric_explorer(all_norm: list[dict]):
    st.subheader("📊 Metric Explorer")

    if len(all_norm) < 1:
        st.info("Run research first.")
        return

    company_names = [n.get("company_name", "Unknown") for n in all_norm]

    # Controls row
    col_ctrl1, col_ctrl2 = st.columns([3, 2])

    with col_ctrl1:
        # Build grouped options: Annual → Growth → Ratios
        explorer_options = {}

        explorer_options["── Historical Financials ──"] = None
        for key, label in ANNUAL_FIELDS.items():
            explorer_options[f"📈 {label}"] = ("annual", key)

        explorer_options["── Growth Over Time ──"] = None
        for key in GROWTH_METRICS:
            label = GROWTH_DISPLAY_NAMES.get(key, key)
            explorer_options[f"📊 {label}"] = ("growth", key)

        explorer_options["── Ratios & Metrics ──"] = None
        for key in CANONICAL_METRICS:
            label = METRIC_DISPLAY_NAMES.get(key, key)
            explorer_options[f"📐 {label}"] = ("metric", key)

        selected_label = st.selectbox(
            "Select metric",
            list(explorer_options.keys()),
            index=1,
        )
        selected = explorer_options.get(selected_label)

    with col_ctrl2:
        selected_companies = st.multiselect(
            "Companies",
            company_names,
            default=company_names,
        )

    if not selected or not selected_companies:
        st.caption("Select a metric and at least one company.")
        return

    metric_type, metric_key = selected

    # Context-aware chart type options
    if metric_type == "metric":
        # Single latest value → bar or table only (line chart makes no sense)
        chart_type = st.radio("View", ["Bar Chart", "Table"], index=0, horizontal=True)
    else:
        # Time series → all options
        chart_type = st.radio("View", ["Line Chart", "Bar Chart", "Table"], index=0, horizontal=True)

    # Currency warning: annual series are always absolute values, growth is always ratios
    is_absolute = (metric_type == "annual") or (metric_key in ABSOLUTE_METRICS)
    if is_absolute:
        currencies = set()
        for n in all_norm:
            if n.get("company_name") in selected_companies:
                c = n.get("currency")
                if c:
                    currencies.add(c)
        if len(currencies) > 1:
            st.warning(f"⚠️ Mixed currencies ({', '.join(currencies)}). Absolute values are NOT directly comparable.")

    # ── Render based on metric type ──

    if metric_type == "annual":
        # Historical time series from normalized annual data
        chart_data = {}
        for n in all_norm:
            name = n.get("company_name", "?")
            if name not in selected_companies:
                continue
            series = {}
            for r in n.get("annual", []):
                period = r.get("period", "?")
                val = r.get(metric_key)
                if val is not None:
                    series[str(period)] = val
            if series:
                chart_data[name] = series

        if not chart_data:
            st.caption("No data available for this metric.")
            return

        df = pd.DataFrame(chart_data)
        df.index.name = "Period"

        if chart_type == "Line Chart":
            st.line_chart(df)
        elif chart_type == "Bar Chart":
            st.bar_chart(df)
        else:
            st.dataframe(df.applymap(lambda v: fmt_millions(v) if pd.notnull(v) else "—"),
                         use_container_width=True)

    elif metric_type == "growth":
        # Growth YoY time series from normalized growth data
        chart_data = {}
        for n in all_norm:
            name = n.get("company_name", "?")
            if name not in selected_companies:
                continue
            growth_data = n.get("growth", {}).get(metric_key, {})
            yoy_list = growth_data.get("yoy", [])
            series = {}
            for entry in yoy_list:
                yr = entry.get("year")
                g = entry.get("growth")
                if yr is not None and g is not None:
                    series[str(yr)] = g
            if series:
                chart_data[name] = series

        if not chart_data:
            st.caption("No growth data available for this metric.")
            return

        df = pd.DataFrame(chart_data)
        df.index.name = "Year"

        if chart_type == "Line Chart":
            st.line_chart(df)
        elif chart_type == "Bar Chart":
            st.bar_chart(df)
        else:
            st.dataframe(df.applymap(lambda v: fmt_pct(v) if pd.notnull(v) else "—"),
                         use_container_width=True)

        # CAGR comparison below the chart
        cagr_rows = {}
        for n in all_norm:
            name = n.get("company_name", "?")
            if name not in selected_companies:
                continue
            gd = n.get("growth", {}).get(metric_key, {})
            cagr_rows[name] = {
                "CAGR 3Y": fmt_pct(gd.get("cagr_3y")),
                "CAGR 5Y": fmt_pct(gd.get("cagr_5y")),
            }
        if cagr_rows:
            st.caption("Compound Annual Growth Rates:")
            st.dataframe(pd.DataFrame(cagr_rows).T, use_container_width=True)

    elif metric_type == "metric":
        # Single latest-value comparison
        values = {}
        for n in all_norm:
            name = n.get("company_name", "?")
            if name not in selected_companies:
                continue
            val = n.get("metrics", {}).get(metric_key)
            if val is not None:
                values[name] = val

        if not values:
            st.caption("No data available for this metric.")
            return

        df = pd.DataFrame({"Company": list(values.keys()), "Value": list(values.values())})
        df = df.set_index("Company").sort_values("Value", ascending=False)

        if chart_type == "Bar Chart":
            st.bar_chart(df)
        else:
            df["Formatted"] = [_fmt_metric(metric_key, v) for v in df["Value"]]
            st.dataframe(df[["Formatted"]], use_container_width=True)

        # Ranking + caption
        st.caption("*Showing latest available normalized value.* Ranking: " + " > ".join(
            f"**{name}** ({_fmt_metric(metric_key, val)})"
            for name, val in sorted(values.items(), key=lambda x: x[1] or 0, reverse=True)
        ))


# ─── Area 5: Growth & Trends ────────────────────────────────────────────────

def display_growth_trends(norm: dict):
    st.subheader("Growth Rates")
    growth = norm.get("growth", {})

    if growth:
        cols = st.columns(min(len(growth), 5))
        for i, (metric, data) in enumerate(growth.items()):
            with cols[i % len(cols)]:
                label = GROWTH_DISPLAY_NAMES.get(metric, metric.replace("_", " ").title())
                st.markdown(f"**{label}**")
                yoy = data.get("yoy", [])
                if yoy:
                    latest = yoy[-1]
                    st.metric(f"YoY ({latest['year']})", fmt_pct(latest["growth"]))
                c3 = data.get("cagr_3y")
                c5 = data.get("cagr_5y")
                if c3 is not None:
                    st.caption(f"3Y CAGR: {fmt_pct(c3)}")
                if c5 is not None:
                    st.caption(f"5Y CAGR: {fmt_pct(c5)}")

    trends = norm.get("trends", [])
    if trends:
        st.subheader("Trend Signals")
        for t in trends:
            icon = "📈" if t["direction"] == "UP" else "📉" if t["direction"] == "DOWN" else "➡️"
            name = METRIC_DISPLAY_NAMES.get(t["metric"], t["metric"].replace("_", " ").title())
            st.markdown(f"{icon} **{name}**: {t['direction']} — {t['detail']}")


# ─── Area 6: Peer Comparison ────────────────────────────────────────────────

def display_peer_comparison(peer_table: dict, all_norm: list[dict]):
    st.subheader("Peer Comparison")

    if peer_table.get("currency_warning"):
        st.warning(peer_table["currency_warning"])

    companies = peer_table.get("companies", [])
    if not companies:
        st.info("No peers to compare.")
        return

    # Focused single-metric comparison
    st.markdown("**Quick Compare**")
    col1, col2 = st.columns([2, 3])
    with col1:
        compare_options = {METRIC_DISPLAY_NAMES.get(k, k): k for k in PEER_TABLE_METRICS}
        selected_label = st.selectbox("Select metric", list(compare_options.keys()), key="peer_quick")
        selected_key = compare_options[selected_label]

    with col2:
        vals = peer_table["metrics"].get(selected_key, [])
        if vals:
            compare_df = pd.DataFrame({
                "Company": companies,
                "Value": [_fmt_metric(selected_key, v) for v in vals],
                "_sort": [v if v is not None else float("-inf") for v in vals],
            }).sort_values("_sort", ascending=False).drop("_sort", axis=1).set_index("Company")
            st.dataframe(compare_df, use_container_width=True)

    # Full peer table
    with st.expander("Full Peer Comparison Table", expanded=False):
        metrics_data = peer_table.get("metrics", {})
        cagr_data = peer_table.get("cagr", {})

        rows = {}
        for key, vals in metrics_data.items():
            label = METRIC_DISPLAY_NAMES.get(key, key)
            rows[label] = [_fmt_metric(key, v) for v in vals]

        cagr_names = {
            "revenue_cagr_3y": "Revenue CAGR 3Y", "ebitda_cagr_3y": "EBITDA CAGR 3Y",
            "net_income_cagr_3y": "Net Income CAGR 3Y",
            "revenue_cagr_5y": "Revenue CAGR 5Y", "ebitda_cagr_5y": "EBITDA CAGR 5Y",
            "net_income_cagr_5y": "Net Income CAGR 5Y",
        }
        for key, vals in cagr_data.items():
            label = cagr_names.get(key, key)
            rows[label] = [fmt_pct(v) for v in vals]

        if rows:
            df = pd.DataFrame(rows, index=companies).T
            st.dataframe(df, use_container_width=True, height=700)

    st.caption("Completeness: " + " | ".join(
        f"{n}: {fmt_pct(c)}" for n, c in zip(companies, peer_table.get("completeness", []))
    ))


# ─── Area 7: Data Quality + Inspectors ──────────────────────────────────────

def display_data_quality(norm: dict, result_data: dict):
    dq = norm.get("data_quality", {})

    c1, c2, c3 = st.columns(3)
    c1.metric("Completeness", fmt_pct(dq.get("completeness", 0)))
    c2.metric("Years", str(dq.get("years_available", 0)))
    c3.metric("Sources", ", ".join(dq.get("sources_used", [])) or "None")

    gaps = dq.get("gaps", [])
    warnings_list = dq.get("warnings", [])

    if gaps:
        for g in gaps:
            st.markdown(f"🔴 {g}")
    if warnings_list:
        for w in warnings_list:
            st.markdown(f"🟡 {w}")
    if not gaps and not warnings_list:
        st.success("All data sources OK. No gaps or warnings.")

    # Inspectors
    col_raw, col_norm = st.columns(2)

    with col_raw:
        st.markdown("**Raw Data Inspector**")
        if result_data.get("sec"):
            with st.expander("🇺🇸 SEC EDGAR"):
                sec_d = {k: v for k, v in result_data["sec"].items() if k != "_submissions"}
                st.json(sec_d)
        if result_data.get("maya"):
            with st.expander("🇮🇱 Maya TASE"):
                st.json(result_data["maya"])
        if result_data.get("yahoo"):
            with st.expander("📈 Yahoo Finance"):
                yd = dict(result_data["yahoo"])
                if yd.get("report") and len(yd["report"]) > 1500:
                    yd["report"] = yd["report"][:1500] + "\n...[truncated]"
                st.json(yd)

    with col_norm:
        st.markdown("**Normalized Data Inspector**")
        with st.expander("📋 Normalized Profile + Metrics"):
            inspect = {
                "company_name": norm.get("company_name"),
                "exchange": norm.get("exchange"),
                "currency": norm.get("currency"),
                "sector": norm.get("sector"),
                "industry": norm.get("industry"),
                "fiscal_year_end_month": norm.get("fiscal_year_end_month"),
                "metrics": norm.get("metrics"),
                "data_quality": norm.get("data_quality"),
            }
            st.json(inspect)
        with st.expander("📋 Normalized Growth"):
            st.json(norm.get("growth", {}))
        with st.expander("📋 Normalized Trends"):
            st.json(norm.get("trends", []))
        with st.expander("📋 Normalized Annual Data"):
            annual = norm.get("annual", [])
            if annual:
                st.json(annual[-1])  # Show latest year as sample
                st.caption(f"Showing latest year. {len(annual)} years total.")


# ─── Main Layout ─────────────────────────────────────────────────────────────

if not primary_input.strip():
    st.title("📊 Shaldor Research Dashboard")
    st.markdown("Enter a company in the sidebar and click **Run Research**.")
    st.markdown("---")
    st.markdown("""
    **Supported:** US (SEC + Yahoo) · TASE (Maya + Yahoo) · Up to 12 peers
    
    **Output:** 20+ normalized metrics · Growth rates · Trends · Peer comparison · Interactive explorer
    """)

elif run_button or "result" in st.session_state:
    if run_button:
        run_and_display()

    if "primary_norm" in st.session_state:
        primary_norm = st.session_state["primary_norm"]
        peers_norm = st.session_state["peers_norm"]
        all_norm = st.session_state["all_norm"]
        peer_table = st.session_state["peer_table"]
        result = st.session_state["result"]

        # Area 2: Profile
        display_profile(primary_norm)
        st.markdown("---")

        # Area 3: Financial Summary
        display_financials(primary_norm)
        st.markdown("---")

        # Area 4: Metric Explorer
        display_metric_explorer(all_norm)
        st.markdown("---")

        # Area 5: Growth & Trends
        display_growth_trends(primary_norm)
        st.markdown("---")

        # Area 6: Peer Comparison
        if peer_table:
            display_peer_comparison(peer_table, all_norm)
            st.markdown("---")

        # Area 7: Data Quality + Inspectors
        st.subheader("Data Quality & Inspectors")
        tabs = st.tabs(
            [f"Primary: {primary_norm.get('company_name', '?')}"] +
            [f"Peer: {p.get('company_name', '?')}" for p in peers_norm]
        )
        with tabs[0]:
            display_data_quality(primary_norm, result["primary"])
        for i in range(len(peers_norm)):
            with tabs[i + 1]:
                display_data_quality(peers_norm[i], result["peers"][i])
