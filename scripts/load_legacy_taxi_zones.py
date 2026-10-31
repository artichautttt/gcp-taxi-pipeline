"""
Reloads the legacy table nyc_taxi.taxi_zones (README, Excel VBA section) from
the versioned seed dbt/seeds/taxi_zone_lookup.csv, the output of
vba/TaxiZoneCleaner.bas. Same schema and result as the documented
`bq load --replace`, without the bq CLI.

    python scripts/load_legacy_taxi_zones.py
"""
import os
from pathlib import Path

from google.cloud import bigquery

SEED = Path(__file__).resolve().parent.parent / "dbt" / "seeds" / "taxi_zone_lookup.csv"
SCHEMA = [
    bigquery.SchemaField("location_id", "INTEGER"),
    bigquery.SchemaField("borough", "STRING"),
    bigquery.SchemaField("zone", "STRING"),
    bigquery.SchemaField("service_zone", "STRING"),
    bigquery.SchemaField("is_airport", "BOOLEAN"),
    bigquery.SchemaField("is_unknown", "BOOLEAN"),
]

if __name__ == "__main__":
    project = os.environ["GCP_PROJECT_ID"]
    table_id = f"{project}.{os.environ.get('BQ_DATASET', 'nyc_taxi')}.taxi_zones"
    client = bigquery.Client(project=project)
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(SEED, "rb") as f:
        client.load_table_from_file(f, table_id, job_config=config).result()
    print(f"[done] {client.get_table(table_id).num_rows} zones in {table_id}")
