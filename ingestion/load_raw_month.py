"""
Downloads one month of NYC Yellow Taxi trips and loads it, as-is, into its own
BigQuery table raw_yellow_tripdata_YYYYMM (one table per month).

Why one table per month: a Parquet load cannot add a partition column, and the
BigQuery Sandbox refuses DML (no DELETE + INSERT). WRITE_TRUNCATE on a monthly
table makes a re-run replace that month instead of appending to it. dbt reads
all months through the wildcard raw_yellow_tripdata_*.

The legacy single table yellow_tripdata_raw (used by sql/01-03) is not touched.
"""
import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_taxi_data import download_month  # noqa: E402
from load_to_bigquery import load_file  # noqa: E402

RAW_TABLE_PREFIX = "raw_yellow_tripdata_"


def raw_table_name(year: int, month: int) -> str:
    if not 1 <= month <= 12:
        raise ValueError(f"month must be in 1..12, got {month}")
    return f"{RAW_TABLE_PREFIX}{year:04d}{month:02d}"


def load_raw_month(year: int, month: int, project_id: str, dataset: str) -> str:
    table = raw_table_name(year, month)
    local_path = download_month(year, month)
    load_file(local_path, project_id, dataset, table)
    return table


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Load one month into raw_yellow_tripdata_YYYYMM")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    load_raw_month(
        args.year,
        args.month,
        os.environ["GCP_PROJECT_ID"],
        os.environ.get("BQ_DATASET", "nyc_taxi"),
    )
