"""
Verification script: compares the local PySpark output (spark_transform.py)
against the equivalent BigQuery tables, to check functional equivalence
between the two transformation paths.

No pytest in this repo yet, so this runs as a standalone script and exits
with a non-zero status if any check fails (usable as a CI/manual gate).

Requires the same BigQuery access as ingestion/load_to_bigquery.py
(GCP_PROJECT_ID, ADC credentials) plus the PySpark output already written
by spark_transform.py.
"""
import os
import sys
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SPARK_OUTPUT_DIR = DATA_DIR / "spark_output"

# Relative tolerance for float comparisons (rounding differences between
# BigQuery's and Spark's floating point / ROUND implementations).
TOLERANCE_PCT = 0.5


def read_spark_parquet_dir(path: Path):
    """Reads a Spark output directory (possibly several part files) as one pyarrow Table."""
    return pq.read_table(str(path))


def spark_trips_clean_row_count() -> int:
    table = read_spark_parquet_dir(SPARK_OUTPUT_DIR / "trips_clean.parquet")
    return table.num_rows


def spark_daily_kpis_aggregates() -> dict:
    table = read_spark_parquet_dir(SPARK_OUTPUT_DIR / "daily_kpis.parquet").to_pandas()
    return {
        "day_count": len(table),
        "total_trip_count": int(table["trip_count"].sum()),
        "total_revenue_usd": float(table["revenue_usd"].sum()),
        "avg_tip_pct": float(table["avg_tip_pct"].mean()),
    }


def bigquery_trips_clean_row_count(client: bigquery.Client, project_id: str) -> int:
    query = f"SELECT COUNT(*) AS row_count FROM `{project_id}.nyc_taxi.trips_clean`"
    return int(next(iter(client.query(query).result()))["row_count"])


def bigquery_daily_kpis_aggregates(client: bigquery.Client, project_id: str) -> dict:
    query = f"""
        SELECT
          COUNT(*) AS day_count,
          SUM(trip_count) AS total_trip_count,
          ROUND(SUM(revenue_usd), 2) AS total_revenue_usd,
          ROUND(AVG(avg_tip_pct), 1) AS avg_tip_pct
        FROM `{project_id}.nyc_taxi.daily_kpis`
    """
    row = next(iter(client.query(query).result()))
    return {
        "day_count": int(row["day_count"]),
        "total_trip_count": int(row["total_trip_count"]),
        "total_revenue_usd": float(row["total_revenue_usd"]),
        "avg_tip_pct": float(row["avg_tip_pct"]),
    }


def within_tolerance(spark_value: float, bq_value: float, tolerance_pct: float = TOLERANCE_PCT) -> bool:
    if bq_value == 0:
        return spark_value == 0
    return abs(spark_value - bq_value) / abs(bq_value) * 100 <= tolerance_pct


def compare(label: str, spark_value: float, bq_value: float) -> bool:
    ok = within_tolerance(spark_value, bq_value)
    status = "OK" if ok else "MISMATCH"
    print(f"[{status}] {label}: spark={spark_value} bigquery={bq_value}")
    return ok


def main() -> int:
    project_id = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project_id)

    print("[compare] trips_clean row count")
    spark_rows = spark_trips_clean_row_count()
    bq_rows = bigquery_trips_clean_row_count(client, project_id)
    results = [compare("trips_clean.row_count", spark_rows, bq_rows)]

    print("[compare] daily_kpis aggregates")
    spark_agg = spark_daily_kpis_aggregates()
    bq_agg = bigquery_daily_kpis_aggregates(client, project_id)
    for key in ("day_count", "total_trip_count", "total_revenue_usd", "avg_tip_pct"):
        results.append(compare(f"daily_kpis.{key}", spark_agg[key], bq_agg[key]))

    if all(results):
        print("[done] PySpark output is functionally equivalent to BigQuery, within tolerance.")
        return 0
    print("[error] one or more checks failed, see MISMATCH lines above.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
