"""
Loads a local NYC Yellow Taxi Parquet file into BigQuery, using an
explicit schema (no blind autodetect) so column types are controlled
and predictable for downstream SQL.
"""
import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Explicit schema matching the official NYC TLC Yellow Taxi trip record layout.
SCHEMA = [
    bigquery.SchemaField("VendorID", "INTEGER"),
    bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("tpep_dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("passenger_count", "INTEGER"),
    bigquery.SchemaField("trip_distance", "FLOAT"),
    bigquery.SchemaField("RatecodeID", "INTEGER"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("PULocationID", "INTEGER"),
    bigquery.SchemaField("DOLocationID", "INTEGER"),
    bigquery.SchemaField("payment_type", "INTEGER"),
    bigquery.SchemaField("fare_amount", "FLOAT"),
    bigquery.SchemaField("extra", "FLOAT"),
    bigquery.SchemaField("mta_tax", "FLOAT"),
    bigquery.SchemaField("tip_amount", "FLOAT"),
    bigquery.SchemaField("tolls_amount", "FLOAT"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
    bigquery.SchemaField("congestion_surcharge", "FLOAT"),
    bigquery.SchemaField("airport_fee", "FLOAT"),
]


def load_file(local_path: Path, project_id: str, dataset: str, table: str) -> None:
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"[load] {local_path} -> {table_ref}")
    with open(local_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()  # wait for completion

    table_obj = client.get_table(table_ref)
    print(f"[done] loaded {table_obj.num_rows:,} rows into {table_ref} "
          f"({table_obj.num_bytes / 1e6:.1f} MB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load a local Parquet taxi file into BigQuery")
    parser.add_argument("--file", required=True, help="Path to local parquet file")
    args = parser.parse_args()

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get("BQ_DATASET", "nyc_taxi")
    table = os.environ.get("BQ_TABLE", "yellow_tripdata_raw")

    load_file(Path(args.file), project_id, dataset, table)
