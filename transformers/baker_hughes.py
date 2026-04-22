"""
Baker Hughes NA Rig Count weekly .xlsx → curated long-format Parquet.

Strategy: the .xlsx has a "US Oil & Gas Split" sheet with:
  columns: Date | Oil | Gas | Miscellaneous | Total | ...
Extract rows where Date is a valid date cell. Produce long-format output
with series_id in {"us_oil", "us_gas", "us_misc", "us_total"}.

For Canada, parse the "Canada Oil & Gas Split" sheet similarly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from scrapers.base.safe_writer import safe_write_parquet
from transformers.errors import TransformError


def transform(raw_xlsx_path: Path, curated_parquet_path: Path) -> dict:
    """
    Transform Baker Hughes Excel file to curated long-format Parquet.

    Raises TransformError if expected sheets or columns are missing.
    """
    try:
        wb = load_workbook(raw_xlsx_path, data_only=True)
    except Exception as exc:
        raise TransformError(f"Failed to load Excel workbook: {exc}") from exc

    rows = []
    ingested_at = datetime.now(UTC).isoformat()

    sheets_to_process = {
        "US Oil & Gas Split": "us",
        "Canada Oil & Gas Split": "canada",
    }

    for sheet_name, region_prefix in sheets_to_process.items():
        if sheet_name not in wb.sheetnames:
            raise TransformError(
                f"Missing expected sheet: '{sheet_name}'. Available: {wb.sheetnames}"
            )

        ws = wb[sheet_name]

        # Identify column headers in the first few rows
        # Usually row 1 or 2. We'll look for "Date" and "Oil"
        header_row = None
        for row in ws.iter_rows(min_row=1, max_row=5):
            values = [cell.value for cell in row]
            if "Date" in values and "Oil" in values:
                header_row = row
                break

        if not header_row:
            raise TransformError(f"Could not find header row in sheet '{sheet_name}'")

        col_map = {}
        for idx, cell in enumerate(header_row):
            if cell.value:
                col_map[cell.value] = idx

        required_cols = ["Date", "Oil", "Gas", "Total"]
        for col in required_cols:
            if col not in col_map:
                raise TransformError(f"Missing required column '{col}' in sheet '{sheet_name}'")

        # Iterate data rows
        for row in ws.iter_rows(min_row=header_row[0].row + 1):
            date_val = row[col_map["Date"]].value

            # Skip if not a valid date
            if not isinstance(date_val, datetime):
                continue

            period = date_val.strftime("%Y-%m-%d")

            for col_name in ["Oil", "Gas", "Miscellaneous", "Total"]:
                if col_name not in col_map:
                    continue

                value = row[col_map[col_name]].value
                if value is None:
                    continue

                series_id = f"{region_prefix}_{col_name.lower().replace('miscellaneous', 'misc')}"
                series_name = f"Baker Hughes Rig Count - {region_prefix.upper()} {col_name}"

                rows.append(
                    {
                        "source": "baker_hughes",
                        "series_id": series_id,
                        "series_name": series_name,
                        "period": period,
                        "value": float(value),
                        "unit": "rigs",
                        "region": region_prefix.upper(),
                        "ingested_at": ingested_at,
                    }
                )

    if not rows:
        return {"rows": 0, "period_range": (None, None), "sheets": []}

    df = pd.DataFrame(rows)
    safe_write_parquet(curated_parquet_path, df)

    return {
        "rows": len(df),
        "period_range": (df["period"].min(), df["period"].max()),
        "regions": sorted(df["region"].unique().tolist()),
    }
