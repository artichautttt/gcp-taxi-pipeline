"""
Bronze layer: lands each source in the bronze dataset as received, plus four
ingestion columns on every row:

    _source       source system (see SOURCE_* below)
    _source_file  name of the file the rows come from
    _loaded_at    load time (UTC), the same for every row of one load
    _run_id       pipeline run (Airflow run_id, or manual__<UTC time>)

One table per source (and per month for monthly sources), written with
WRITE_TRUNCATE: re-running a month replaces its table, never appends (the
Sandbox refuses DML). After each load the table row count is compared with
the source row count, and the load is appended to <bronze>.ingestion_log
(load job WRITE_APPEND, accepted by the Sandbox: checked on a throwaway table
on 2026-10-30).

    python ingestion/bronze.py --year 2025 --month 1          # trips + zones + TLC XML
"""
import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_taxi_data import DATA_DIR, download_month  # noqa: E402

SOURCE_TRIPS = "nyc_tlc_trip_records"
SOURCE_ZONES = "nyc_tlc_taxi_zone_lookup"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{name}"

# Columns of the TLC Yellow Taxi file loaded in bronze, with their BigQuery
# types (same contract as ingestion/load_to_bigquery.py). Columns of the file
# not listed here (cbd_congestion_fee, added in 2025) are not loaded.
TRIP_COLUMNS = [
    ("VendorID", "INTEGER"),
    ("tpep_pickup_datetime", "TIMESTAMP"),
    ("tpep_dropoff_datetime", "TIMESTAMP"),
    ("passenger_count", "INTEGER"),
    ("trip_distance", "FLOAT"),
    ("RatecodeID", "INTEGER"),
    ("store_and_fwd_flag", "STRING"),
    ("PULocationID", "INTEGER"),
    ("DOLocationID", "INTEGER"),
    ("payment_type", "INTEGER"),
    ("fare_amount", "FLOAT"),
    ("extra", "FLOAT"),
    ("mta_tax", "FLOAT"),
    ("tip_amount", "FLOAT"),
    ("tolls_amount", "FLOAT"),
    ("improvement_surcharge", "FLOAT"),
    ("total_amount", "FLOAT"),
    ("congestion_surcharge", "FLOAT"),
    ("airport_fee", "FLOAT"),
]
METADATA_COLUMNS = [
    ("_source", "STRING"),
    ("_source_file", "STRING"),
    ("_loaded_at", "TIMESTAMP"),
    ("_run_id", "STRING"),
]
LOG_COLUMNS = [
    ("run_id", "STRING"),
    ("source", "STRING"),
    ("source_file", "STRING"),
    ("source_uri", "STRING"),
    ("file_sha256", "STRING"),
    ("file_bytes", "INTEGER"),
    ("source_rows", "INTEGER"),
    ("loaded_rows", "INTEGER"),
    ("target_table", "STRING"),
    ("loaded_at", "TIMESTAMP"),
    ("status", "STRING"),
]


class BronzeCountError(Exception):
    """The bronze table does not hold exactly the rows of its source."""


def datasets() -> dict:
    return {
        "bronze": os.environ.get("BQ_BRONZE_DATASET", "nyc_taxi_bronze"),
        "silver": os.environ.get("BQ_SILVER_DATASET", "nyc_taxi_silver"),
        "gold": os.environ.get("BQ_GOLD_DATASET", "nyc_taxi_gold"),
    }


def new_run_id(now: datetime | None = None) -> str:
    """Run id for a run started outside Airflow, in Airflow's manual__ format."""
    now = now or datetime.now(timezone.utc)
    return f"manual__{now.astimezone(timezone.utc).isoformat(timespec='seconds')}"


def trips_table_name(year: int, month: int) -> str:
    if not 1 <= month <= 12:
        raise ValueError(f"month must be in 1..12, got {month}")
    return f"yellow_tripdata_{year:04d}{month:02d}"


def bq_schema(columns):
    from google.cloud import bigquery

    return [bigquery.SchemaField(name, kind) for name, kind in columns]


def select_trip_columns(table: pa.Table) -> pa.Table:
    """Keeps the TRIP_COLUMNS of the file, in order, matched case-insensitively
    (the 2025 files spell Airport_fee with a capital A)."""
    by_lower = {name.lower(): name for name in table.column_names}
    missing = [name for name, _ in TRIP_COLUMNS if name.lower() not in by_lower]
    if missing:
        raise ValueError(f"columns missing from the source file: {missing}")
    picked = table.select([by_lower[name.lower()] for name, _ in TRIP_COLUMNS])
    return picked.rename_columns([name for name, _ in TRIP_COLUMNS])


def with_ingestion_metadata(table: pa.Table, source: str, source_file: str,
                            loaded_at: datetime, run_id: str) -> pa.Table:
    n = table.num_rows
    values = [
        pa.repeat(pa.scalar(source, pa.string()), n),
        pa.repeat(pa.scalar(source_file, pa.string()), n),
        pa.repeat(pa.scalar(loaded_at, pa.timestamp("us", tz="UTC")), n),
        pa.repeat(pa.scalar(run_id, pa.string()), n),
    ]
    for (name, _), column in zip(METADATA_COLUMNS, values):
        table = table.append_column(name, column)
    return table


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_arrow(client, table: pa.Table, table_id: str, columns) -> int:
    """Loads an Arrow table (WRITE_TRUNCATE) through a temporary Parquet file;
    returns the row count of the BigQuery table after the load."""
    from google.cloud import bigquery

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=bq_schema(columns),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "bronze.parquet"
        pq.write_table(table, path)
        with open(path, "rb") as f:
            client.load_table_from_file(f, table_id, job_config=config).result()
    return client.get_table(table_id).num_rows


def append_rows(client, table_id: str, columns, rows: list) -> None:
    """Appends rows with a load job (WRITE_APPEND): no DML, no streaming insert."""
    from google.cloud import bigquery

    config = bigquery.LoadJobConfig(
        schema=bq_schema(columns),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    client.load_table_from_json(rows, table_id, job_config=config).result()


def land(client, project_id: str, table: pa.Table, target: str, columns, log: dict) -> dict:
    """Loads `table` into bronze.<target>, logs the load, raises if the counts differ."""
    bronze = datasets()["bronze"]
    table_id = f"{project_id}.{bronze}.{target}"
    print(f"[bronze] {log['source_file']} -> {table_id} ({table.num_rows:,} rows)")
    loaded = load_arrow(client, table, table_id, columns)
    record = {
        **log,
        "source_rows": table.num_rows,
        "loaded_rows": loaded,
        "target_table": table_id,
        "status": "ok" if loaded == table.num_rows else "count_mismatch",
    }
    append_rows(client, f"{project_id}.{bronze}.ingestion_log", LOG_COLUMNS, [record])
    if loaded != table.num_rows:
        raise BronzeCountError(f"{table_id}: {loaded} rows loaded, source has {table.num_rows}")
    print(f"[bronze] ok, {loaded:,} rows = source")
    return record


def _client(client, project_id: str):
    if client is not None:
        return client
    from google.cloud import bigquery

    return bigquery.Client(project=project_id)


def load_trips_month(year: int, month: int, run_id: str, project_id: str,
                     client=None, data_dir: Path = DATA_DIR) -> dict:
    """Downloads (skipped if local) and lands one month of trips in bronze."""
    path = download_month(year, month, data_dir)
    source_rows = pq.ParquetFile(path).metadata.num_rows
    loaded_at = datetime.now(timezone.utc)
    table = with_ingestion_metadata(
        select_trip_columns(pq.read_table(path)), SOURCE_TRIPS, path.name, loaded_at, run_id
    )
    if table.num_rows != source_rows:
        raise BronzeCountError(f"{path.name}: read {table.num_rows} rows, metadata says {source_rows}")
    return land(_client(client, project_id), project_id, table, trips_table_name(year, month),
                TRIP_COLUMNS + METADATA_COLUMNS, {
                    "run_id": run_id,
                    "source": SOURCE_TRIPS,
                    "source_file": path.name,
                    "source_uri": TRIPS_URL.format(name=path.name),
                    "file_sha256": file_sha256(path),
                    "file_bytes": path.stat().st_size,
                    "loaded_at": loaded_at.isoformat(),
                })


def read_csv_as_strings(path: Path) -> pa.Table:
    """Reads a CSV as-is: every column a string, empty cells kept as ''."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = [row for row in reader if row]
    widths = {len(row) for row in rows}
    if widths - {len(header)}:
        raise ValueError(f"{path.name}: rows with {sorted(widths)} fields, header has {len(header)}")
    return pa.table({name: pa.array([row[i] for row in rows], pa.string()) for i, name in enumerate(header)})


def download_zone_lookup(data_dir: Path = DATA_DIR, session=None) -> Path:
    """Official TLC zone lookup CSV, downloaded once (same .part pattern as the trips)."""
    dest = data_dir / "taxi_zone_lookup.csv"
    if dest.exists():
        return dest
    import requests

    session = session or requests
    data_dir.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(".csv.part")
    try:
        resp = session.get(ZONES_URL, timeout=60)
        resp.raise_for_status()
        part.write_bytes(resp.content)
        part.replace(dest)
    finally:
        part.unlink(missing_ok=True)
    return dest


def load_zone_lookup(run_id: str, project_id: str, client=None, data_dir: Path = DATA_DIR) -> dict:
    """Lands the raw TLC zone lookup CSV (all columns as strings) in bronze."""
    path = download_zone_lookup(data_dir)
    raw = read_csv_as_strings(path)
    columns = [(name, "STRING") for name in raw.column_names] + METADATA_COLUMNS
    loaded_at = datetime.now(timezone.utc)
    table = with_ingestion_metadata(raw, SOURCE_ZONES, path.name, loaded_at, run_id)
    return land(_client(client, project_id), project_id, table, "taxi_zone_lookup", columns, {
        "run_id": run_id,
        "source": SOURCE_ZONES,
        "source_file": path.name,
        "source_uri": ZONES_URL,
        "file_sha256": file_sha256(path),
        "file_bytes": path.stat().st_size,
        "loaded_at": loaded_at.isoformat(),
    })


if __name__ == "__main__":
    from dotenv import load_dotenv

    from tlc_zone_trips_xml import load_tlc_zone_trips

    load_dotenv()
    parser = argparse.ArgumentParser(description="Land one month of every source in bronze")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--run-id", default=None, help="default: manual__<UTC time>")
    parser.add_argument("--skip-xml", action="store_true", help="do not load the TLC XML control source")
    args = parser.parse_args()

    project = os.environ["GCP_PROJECT_ID"]
    run = args.run_id or new_run_id()
    bq = _client(None, project)
    summary = {
        "run_id": run,
        "trips": load_trips_month(args.year, args.month, run, project, bq),
        "zones": load_zone_lookup(run, project, bq),
    }
    if not args.skip_xml:
        summary["tlc_zone_trips"] = load_tlc_zone_trips(args.year, args.month, run, project, bq)
    print(json.dumps(summary, indent=2, default=str))
