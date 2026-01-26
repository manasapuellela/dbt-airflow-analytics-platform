#!/usr/bin/env python3
"""Export daily metrics for BI tools into /exports."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

EXPORT_QUERY = """
SELECT
  pickup_date,
  COUNT(*) AS trip_count,
  ROUND(AVG(total_amount)::numeric, 2) AS avg_fare,
  ROUND(SUM(total_amount)::numeric, 2) AS total_revenue
FROM marts.fact_trips
GROUP BY pickup_date
ORDER BY pickup_date
"""


def main() -> None:
    load_dotenv()
    warehouse_uri = os.environ.get(
        "WAREHOUSE_DB_URI",
        "postgresql+psycopg2://analytics:analytics@localhost:5434/analytics",
    )
    engine = create_engine(warehouse_uri)
    output_dir = Path(os.environ.get("EXPORT_DIR", "exports"))
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_sql(EXPORT_QUERY, engine)
    output_path = output_dir / "daily_metrics.csv"
    df.to_csv(output_path, index=False)
    print(f"Exported {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
