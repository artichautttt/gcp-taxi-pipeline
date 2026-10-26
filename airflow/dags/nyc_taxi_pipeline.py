"""
Orchestrates the NYC Taxi pipeline end to end:

    check_source_available -> ingest -> sql_quality_checks -> sql_trips_clean
        -> sql_daily_kpis -> dbt_test

- check_source_available: HTTP HEAD on the public NYC TLC file (CloudFront),
  fails fast if the month is not published yet.
- ingest: reuses ingestion/download_taxi_data.py and ingestion/load_to_bigquery.py.
- sql_*: runs sql/01-03 as-is on BigQuery (same files as the manual `bq query` path).
- dbt_test: runs the dbt source tests in dbt/ (data-quality gate).

Authentication: ADC only (the gcloud config directory is mounted read-only by
docker-compose.yml); no service account key.
"""
import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

PIPELINE_DIR = Path(os.environ.get("PIPELINE_DIR", "/opt/pipeline"))
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _run_sql_file(filename: str) -> None:
    from google.cloud import bigquery

    sql = (PIPELINE_DIR / "sql" / filename).read_text(encoding="utf-8")
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    job = client.query(sql)
    job.result()
    # A multi-statement script returns only the last result; log every child
    # job so the quality-check numbers of sql/01 show up in the task log.
    for child in client.list_jobs(parent_job=job.job_id):
        if child.statement_type == "SELECT":
            for row in child.result():
                print(f"[{filename}] {dict(row)}")
    print(f"[{filename}] done, {job.total_bytes_processed or 0:,} bytes processed")


@dag(
    dag_id="nyc_taxi_pipeline",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "year": Param(2025, type="integer"),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
    tags=["nyc_taxi", "bigquery", "dbt"],
)
def nyc_taxi_pipeline():
    @task
    def check_source_available(params=None) -> str:
        import requests

        filename = f"yellow_tripdata_{params['year']}-{params['month']:02d}.parquet"
        url = f"{TLC_BASE_URL}/{filename}"
        resp = requests.head(url, timeout=30, allow_redirects=True)
        print(f"[check] HEAD {url} -> {resp.status_code}, "
              f"content-length={resp.headers.get('content-length')}, "
              f"last-modified={resp.headers.get('last-modified')}")
        resp.raise_for_status()
        return filename

    @task
    def ingest(filename: str) -> None:
        sys.path.insert(0, str(PIPELINE_DIR / "ingestion"))
        from download_taxi_data import download_month
        from load_to_bigquery import load_file

        year, month = filename.removeprefix("yellow_tripdata_").removesuffix(".parquet").split("-")
        local_path = download_month(int(year), int(month))
        load_file(
            local_path,
            os.environ["GCP_PROJECT_ID"],
            os.environ.get("BQ_DATASET", "nyc_taxi"),
            os.environ.get("BQ_TABLE", "yellow_tripdata_raw"),
        )

    @task
    def sql_quality_checks() -> None:
        _run_sql_file("01_data_quality_checks.sql")

    @task
    def sql_trips_clean() -> None:
        _run_sql_file("02_transform_trips_clean.sql")

    @task
    def sql_daily_kpis() -> None:
        _run_sql_file("03_analytics_daily_kpis.sql")

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            # Only the tests on the sql/01-03 tables: the star-schema models
            # are built and tested separately (dbt run/test with pipeline_month).
            f"{DBT_BIN} test --select source:nyc_taxi "
            f"--project-dir {PIPELINE_DIR}/dbt --profiles-dir {PIPELINE_DIR}/dbt "
            "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs --no-use-colors"
        ),
    )

    (
        ingest(check_source_available())
        >> sql_quality_checks()
        >> sql_trips_clean()
        >> sql_daily_kpis()
        >> dbt_test
    )


nyc_taxi_pipeline()
