"""
ELT pipeline for one month of NYC Yellow Taxi trips, into the dbt star schema:

    check_source -> download -> load_raw -> dbt_seed -> dbt_run -> dbt_test -> quality_gate

- check_source: HTTP HEAD on the public NYC TLC file, fails fast if the month
  is not published.
- download: ingestion/download_taxi_data.py (skipped if the file is local).
- load_raw: one table per month, raw_yellow_tripdata_YYYYMM, WRITE_TRUNCATE.
- dbt_seed / dbt_run: lookups, then staging -> intermediate -> marts for the
  month (fct_trips and audit_trip_rows replace their YYYYMM partition).
- dbt_test: tests of the star schema (the legacy source tests belong to the
  nyc_taxi_pipeline DAG).
- quality_gate: quality/quality_gate.py, fails on a failed dbt test or when
  raw rows loaded != rows audited, or kept rows != rows in fct_trips.

Idempotent: re-running a month replaces its raw table and its partitions, it
never appends. Authentication: ADC only (gcloud config mounted read-only), no
service account key.
"""
import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, get_current_context, task

PIPELINE_DIR = Path(os.environ.get("PIPELINE_DIR", "/opt/pipeline"))
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
# One dbt target directory per run, so the quality gate reads the results of
# its own dbt test (and parallel runs cannot overwrite each other).
DBT_TARGET = "/tmp/dbt_elt/{{ run_id | replace(':', '_') | replace('+', '_') }}"
PIPELINE_MONTH = "{{ params.year * 100 + params.month }}"


def _dbt(command: str) -> str:
    return (
        f"{DBT_BIN} {command} --vars '{{pipeline_month: {PIPELINE_MONTH}}}' "
        f"--project-dir {PIPELINE_DIR}/dbt --profiles-dir {PIPELINE_DIR}/dbt "
        f"--target-path {DBT_TARGET} --log-path {DBT_TARGET}/logs --no-use-colors"
    )


@dag(
    dag_id="nyc_taxi_elt",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        # Same range as dim_date (vars dim_date_start/dim_date_end in
        # dbt_project.yml): a month outside it would fail the relationships
        # test fct_trips.pickup_date -> dim_date. Widen both together.
        "year": Param(2025, type="integer", minimum=2024, maximum=2026),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
    tags=["nyc_taxi", "bigquery", "dbt", "elt"],
)
def nyc_taxi_elt():
    @task
    def check_source(params=None) -> dict:
        import requests

        year, month = params["year"], params["month"]
        url = f"{TLC_BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"
        resp = requests.head(url, timeout=30, allow_redirects=True)
        print(f"[check] HEAD {url} -> {resp.status_code}, "
              f"content-length={resp.headers.get('content-length')}")
        resp.raise_for_status()
        return {"year": year, "month": month, "pipeline_month": year * 100 + month}

    @task
    def download(month_info: dict) -> str:
        sys.path.insert(0, str(PIPELINE_DIR / "ingestion"))
        from download_taxi_data import download_month

        return str(download_month(month_info["year"], month_info["month"]))

    @task
    def load_raw(month_info: dict, local_path: str) -> int:
        sys.path.insert(0, str(PIPELINE_DIR / "ingestion"))
        from load_raw_month import raw_table_name
        from load_to_bigquery import load_file

        return load_file(
            Path(local_path),
            os.environ["GCP_PROJECT_ID"],
            os.environ.get("BQ_DATASET", "nyc_taxi"),
            raw_table_name(month_info["year"], month_info["month"]),
        )

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=_dbt("seed"))
    dbt_run = BashOperator(task_id="dbt_run", bash_command=_dbt("run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=_dbt("test --exclude source:nyc_taxi"))

    @task
    def quality_gate(month_info: dict, raw_rows_loaded: int) -> dict:
        sys.path.insert(0, str(PIPELINE_DIR / "quality"))
        from quality_gate import run_gate

        run_id = get_current_context()["run_id"].replace(":", "_").replace("+", "_")
        return run_gate(
            Path("/tmp/dbt_elt") / run_id / "run_results.json",
            raw_rows_loaded,
            month_info["pipeline_month"],
            os.environ["GCP_PROJECT_ID"],
            os.environ.get("BQ_DATASET", "nyc_taxi"),
        )

    month_info = check_source()
    raw_rows = load_raw(month_info, download(month_info))
    gate = quality_gate(month_info, raw_rows)
    raw_rows >> dbt_seed >> dbt_run >> dbt_test >> gate


nyc_taxi_elt()
