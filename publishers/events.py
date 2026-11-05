"""
Event detectors — read curated Parquets, compute deltas, decide if an event
warrants an alert, format the Telegram message.

Each detector:
  - Takes (current_df, previous_df) or just (current_df)
  - Returns EventResult | None
    EventResult: dict with dedup_key, html_body
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

CURATED_DIR = Path("data/curated")


def detect_storage_print() -> dict[str, str] | None:
    """
    Fires on new EIA weekly storage print. Computes:
      - Net withdrawal/injection (latest - prior week) per region
      - Total NA number (Lower 48 / NA region row in EIA response)
      - vs same-week-prior-year delta
      - vs 5-yr avg (if enough history)
    """
    path = CURATED_DIR / "eia_storage.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        logger.error(f"Failed to read {path}: {exc}")
        return None

    if df.empty:
        return None

    # Sort chronologically
    df = df.sort_values("period")
    latest_period = df["period"].max()

    # Pick the Lower 48 or NA total series — schema-dependent
    # Try common region names from EIA
    region_priority = ["Lower 48", "NA", "Lower48", "U.S."]
    total_df = pd.DataFrame()
    for region in region_priority:
        total_df = df[df["region"] == region]
        if not total_df.empty:
            break
    if total_df.empty:
        # Fallback: sum all regions
        total_df = df.groupby("period", as_index=False)["value"].sum()
        total_df["region"] = "NA (summed)"

    total_df = total_df.sort_values("period")
    latest_row = total_df[total_df["period"] == latest_period].iloc[-1]
    latest_value = float(latest_row["value"])

    # Week-over-week delta
    prior_rows = total_df[total_df["period"] < latest_period]
    if prior_rows.empty:
        return None
    prior_value = float(prior_rows.iloc[-1]["value"])
    delta = latest_value - prior_value
    direction = "injection" if delta > 0 else "withdrawal"

    # Year-ago comparison
    year_ago_period = pd.to_datetime(latest_period) - pd.Timedelta(days=364)
    # Handle both string and datetime 'period'
    period_dt = pd.to_datetime(total_df["period"])
    year_ago_match_idx = (period_dt - year_ago_period).abs().argsort()[:1]
    year_ago_match = total_df.iloc[year_ago_match_idx]
    year_ago_value = float(year_ago_match["value"].iloc[0]) if not year_ago_match.empty else None

    # 5-year average for same week-of-year
    wk = pd.to_datetime(latest_period).isocalendar().week
    same_week = total_df[period_dt.dt.isocalendar().week == wk]
    same_week = same_week[same_week["period"] < latest_period].tail(5)
    five_yr_avg = float(same_week["value"].mean()) if len(same_week) >= 3 else None

    def fmt(n: float) -> str:
        return f"{n:+,.0f}" if n else "0"

    body_lines = [
        f"🟢 <b>EIA Storage Print — {latest_period}</b>",
        "",
        f"Net {direction}: <code>{fmt(delta)} Bcf</code>",
        f"Total: <code>{latest_value:,.0f} Bcf</code>",
    ]
    if year_ago_value is not None:
        yoy = latest_value - year_ago_value
        body_lines.append(f"vs year ago: <code>{fmt(yoy)} Bcf</code>")
    if five_yr_avg is not None:
        vs_5y = latest_value - five_yr_avg
        pct = (vs_5y / five_yr_avg) * 100 if five_yr_avg else 0
        body_lines.append(f"vs 5-yr avg: <code>{fmt(vs_5y)} Bcf ({pct:+.1f}%)</code>")

    # South Central Salt breakout & Texas 527 Bcf Benchmark
    if "region" in df.columns:
        salt_df = df[df["region"] == "South Central Salt"].sort_values("period")
        if not salt_df.empty:
            salt_latest = salt_df[salt_df["period"] == latest_period]
            if not salt_latest.empty:
                salt_val = float(salt_latest.iloc[-1]["value"])
                salt_prior = salt_df[salt_df["period"] < latest_period]
                salt_delta = (salt_val - float(salt_prior.iloc[-1]["value"])) if not salt_prior.empty else None
                salt_delta_str = f" ({fmt(salt_delta)} Bcf WoW)" if salt_delta is not None else ""
                texas_pct = (salt_val / 527.0) * 100.0
                body_lines.append(f"SC Salt Caverns: <code>{salt_val:,.0f} Bcf{salt_delta_str}</code> (<code>{texas_pct:.1f}%</code> of TX 527 Bcf Nameplate)")

    return {
        "dedup_key": f"storage_print_{latest_period}",
        "html_body": "\n".join(body_lines),
    }


def detect_eu_storage_print() -> dict[str, str] | None:
    """
    Fires daily when new GIE AGSI+ data arrives. Computes:
      - Total European stock (TWh) and aggregate % full
      - Daily net flow (GWh/d reported injection - withdrawal)
      - Key country balances (Germany, France, Italy, Netherlands)
    """
    path = CURATED_DIR / "gie_agsi.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        logger.error(f"Failed to read {path}: {exc}")
        return None

    if df.empty:
        return None

    latest_period = str(df["period"].max())
    day_df = df[df["period"] == latest_period]
    if day_df.empty:
        return None

    stock_df = day_df[day_df["series_id"].str.endswith("_gas_in_storage")]
    inj_df = day_df[day_df["series_id"].str.endswith("_injection")]
    wdr_df = day_df[day_df["series_id"].str.endswith("_withdrawal")]
    pct_df = day_df[day_df["series_id"].str.endswith("_full_pct")]

    if stock_df.empty:
        return None

    total_stock = float(stock_df["value"].sum())
    total_inj = float(inj_df["value"].sum()) if not inj_df.empty else 0.0
    total_wdr = float(wdr_df["value"].sum()) if not wdr_df.empty else 0.0
    net_flow = total_inj - total_wdr
    flow_regime = "Injection" if net_flow > 100 else ("Withdrawal" if net_flow < -100 else "Neutral")

    # Estimate capacity from each country's stock and pct
    caps: list[float] = []
    for _, srow in stock_df.iterrows():
        sid = str(srow["series_id"])
        parts = sid.split("_")
        if len(parts) >= 3:
            cc = parts[2]
            prow = pct_df[pct_df["series_id"] == f"gie_storage_{cc}_full_pct"]
            if not prow.empty and float(prow.iloc[0]["value"]) > 0:
                c = float(srow["value"]) / (float(prow.iloc[0]["value"]) / 100.0)
                caps.append(c)
    total_cap = sum(caps) if caps else 1010.1
    agg_pct = (total_stock / total_cap) * 100.0 if total_cap > 0 else 0.0

    body_lines = [
        f"🇪🇺 <b>GIE AGSI+ European Gas Storage — {latest_period}</b>",
        "",
        f"Aggregate: <code>{agg_pct:.1f}%</code> (<code>{total_stock:.1f} TWh</code> / <code>{total_cap:.1f} TWh</code>)",
        f"Daily Flow: <code>{net_flow:+,.0f} GWh/d</code> ({flow_regime})",
    ]

    key_ccs = [("de", "Germany"), ("it", "Italy"), ("fr", "France"), ("nl", "Netherlands")]
    country_lines = []
    for cc, cname in key_ccs:
        cpct_row = pct_df[pct_df["series_id"] == f"gie_storage_{cc}_full_pct"]
        cstock_row = stock_df[stock_df["series_id"] == f"gie_storage_{cc}_gas_in_storage"]
        if not cpct_row.empty and not cstock_row.empty:
            cp = float(cpct_row.iloc[0]["value"])
            cs = float(cstock_row.iloc[0]["value"])
            country_lines.append(f"  • {cname}: <code>{cp:.1f}%</code> (<code>{cs:.1f} TWh</code>)")
    if country_lines:
        body_lines.append("")
        body_lines.append("<b>Key Member States:</b>")
        body_lines.extend(country_lines)

    return {
        "dedup_key": f"eu_storage_print_{latest_period}",
        "html_body": "\n".join(body_lines),
    }


def detect_rig_reversal(threshold_rigs: int = 20) -> dict[str, str] | None:
    """
    Fires when US total rig count changes by >= threshold_rigs week-over-week
    (in either direction — both add/drop are signal).
    """
    path = CURATED_DIR / "baker_hughes_weekly.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        logger.error(f"Failed to read {path}: {exc}")
        return None

    df_total = df[df["series_id"] == "bh_rollup_us_total"].sort_values("period")
    if len(df_total) < 2:
        return None

    latest = df_total.iloc[-1]
    prior = df_total.iloc[-2]
    delta = int(latest["value"]) - int(prior["value"])
    if abs(delta) < threshold_rigs:
        return None

    direction_emoji = "🟢 ↗" if delta > 0 else "🔴 ↘"
    body_lines = [
        f"{direction_emoji} <b>Rig Count Reversal — Week of {latest['period']}</b>",
        "",
        f"US Total: <code>{int(latest['value'])} rigs</code> " f"(<code>{delta:+d}</code> WoW)",
        f"Prior week: <code>{int(prior['value'])} rigs</code>",
    ]

    # Add basin breakdown for largest movers
    basin_now = df[
        (df["series_id"].str.startswith("bh_rollup_basin_")) & (df["period"] == latest["period"])
    ].set_index("series_id")["value"]
    basin_prev = df[
        (df["series_id"].str.startswith("bh_rollup_basin_")) & (df["period"] == prior["period"])
    ].set_index("series_id")["value"]
    basin_delta = (basin_now - basin_prev).dropna().sort_values()

    if not basin_delta.empty:
        body_lines.append("")
        body_lines.append("<b>Top basin moves:</b>")
        # Largest drops and largest gains
        top_moves = pd.concat([basin_delta.head(3), basin_delta.tail(3)]).drop_duplicates()
        for sid, d in top_moves.items():
            if abs(d) < 1:
                continue
            basin_name = sid.replace("bh_rollup_basin_", "").replace("_", " ").title()
            body_lines.append(f"  • {basin_name}: <code>{int(d):+d}</code>")

    return {
        "dedup_key": f"rig_reversal_{latest['period']}",
        "html_body": "\n".join(body_lines),
    }


def detect_feedgas_downtime() -> list[dict[str, str]]:
    """Detect active feedgas outages, sustained depression, or acute drops across LNG terminals."""
    from publishers.alerts import send_feedgas_alerts_if_needed

    raw_alerts = send_feedgas_alerts_if_needed(dry_run=True)
    return [{"dedup_key": a["dedup_key"], "html_body": a["html_body"]} for a in raw_alerts]


def run_all_detectors() -> list[dict[str, str]]:
    """Run all detectors, return list of events that fired."""
    # Explicit Callable annotation: without it mypy infers the list element
    # type as plain `object` (mixed inferred callable types) and flags the
    # call site with "Cannot call function of unknown type" under --strict.
    detectors: list[Callable[[], dict[str, str] | None]] = [
        detect_storage_print,
        detect_eu_storage_print,
        detect_rig_reversal,
    ]
    events: list[dict[str, str]] = []
    for detector in detectors:
        try:
            result = detector()
            if result:
                events.append(result)
        except Exception as exc:
            logger.exception(f"Detector {detector.__name__} failed: {exc}")

    try:
        feedgas_events = detect_feedgas_downtime()
        events.extend(feedgas_events)
    except Exception as exc:
        logger.exception(f"Detector detect_feedgas_downtime failed: {exc}")

    return events

