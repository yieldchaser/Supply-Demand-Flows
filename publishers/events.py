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

    return {
        "dedup_key": f"storage_print_{latest_period}",
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


def run_all_detectors() -> list[dict[str, str]]:
    """Run all detectors, return list of events that fired."""
    events: list[dict[str, str]] = []
    for detector in [detect_storage_print, detect_rig_reversal]:
        try:
            result = detector()
            if result:
                events.append(result)
        except Exception as exc:
            logger.exception(f"Detector {detector.__name__} failed: {exc}")
    return events
