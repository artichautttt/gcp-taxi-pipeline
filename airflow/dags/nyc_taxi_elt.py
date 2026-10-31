"""
Medallion ELT pipeline for one month of NYC Yellow Taxi trips:

    check_source -> download -> load_bronze ----------+
    check_source -> load_bronze_zones ----------------+-> dbt_seed -> dbt_run -> dbt_test -> quality_gate
    check_source -> load_bronze_tlc_xml --------------+

- check_source: HTTP HEAD on the public NYC TLC file, fails fast if the month
  is not published.
- download: ingestion/download_taxi_data.py (skipped if the file is local).
- load_bronze*: ingestion/bronze.py, every source as received plus _source,
  _source_file, _loaded_at, _run_id, one table per month (WRITE_TRUNCATE);
  fails if the bronze table does not hold exactly the rows of its source.
  Bronze tables: yellow_tripdata_YYYYMM (Parquet), taxi_zone_lookup (CSV),
  tlc_zone_trips_YYYYMM (XML, NYC Open Data c5iv-bn4s, informative control).
- dbt_seed / dbt_run: referentials, then silver (trips, trips_quarantine,
  audit_trip_rows) and gold (star schema, aggregates, TLC reconciliation) for
  the month; incremental models replace their YYYYMM partition only.
- dbt_test: tests of the medallion models (the legacy source tests belong to
  the nyc_taxi_pipeline DAG).
- quality_gate: quality/quality_gate.py, appends the run to
  silver.quality_report, then fails on a failed dbt test or when
  source != bronze, bronze != silver + quarantine, or the quarantine was not
  rebuilt by this run.

The Airflow run_id is the run id of every layer: bronze _run_id, quarantine
run_id, quality_report run_id. Idempotent: re-running a month replaces its
bronze tables and its partitions, it never appends (except quality_report,
one entry per run by design). Authentication: ADC only (gcloud config
mounted read-only), no service account key.
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
RUN_ID = "{{ run_id }}"


def _dbt(command: str) -> str:
    return (
        f"{DBT_BIN} {command} --vars '{{pipeline_month: {PIPELINE_MONTH}, run_id: \"{RUN_ID}\"}}' "
        f"--project-dir {PIPELINE_DIR}/dbt --profiles-dir {PIPELINE_DIR}/dbt "
        f"--target-path {DBT_TARGET} --log-path {DBT_TARGET}/logs --no-use-colors"
    )


def _ingestion() -> None:
    sys.path.insert(0, str(PIPELINE_DIR / "ingestion"))


def _run_id() -> str:
    return get_current_context()["run_id"]


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
    tags=["nyc_taxi", "bigquery", "dbt", "elt", "medallion"],
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
        _ingestion()
        from download_taxi_data import download_month

        return str(download_month(month_info["year"], month_info["month"]))

    @task
    def load_bronze(month_info: dict, local_path: str) -> dict:
        _ingestion()
        from bronze import load_trips_month

        # local_path orders this task after download; load_trips_month finds the file again
        print(f"[bronze] {local_path}")
        return load_trips_month(month_info["year"], month_info["month"], _run_id(),
                                os.environ["GCP_PROJECT_ID"])

    @task
    def load_bronze_zones(month_info: dict) -> dict:
        _ingestion()
        from bronze import load_zone_lookup

        return load_zone_lookup(_run_id(), os.environ["GCP_PROJECT_ID"])

    @task
    def load_bronze_tlc_xml(month_info: dict) -> dict:
        _ingestion()
        from tlc_zone_trips_xml import load_tlc_zone_trips

        return load_tlc_zone_trips(month_info["year"], month_info["month"], _run_id(),
                                   os.environ["GCP_PROJECT_ID"])

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=_dbt("seed"))
    dbt_run = BashOperator(task_id="dbt_run", bash_command=_dbt("run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=_dbt("test --exclude source:nyc_taxi"))

    @task
    def quality_gate(month_info: dict, bronze: dict) -> dict:
        sys.path.insert(0, str(PIPELINE_DIR / "quality"))
        from quality_gate import run_gate

        run_id = _run_id()
        target = run_id.replace(":", "_").replace("+", "_")
        return run_gate(
            Path("/tmp/dbt_elt") / target / "run_results.json",
            bronze["source_rows"],
            month_info["pipeline_month"],
            run_id,
            os.environ["GCP_PROJECT_ID"],
        )

    month_info = check_source()
    bronze = load_bronze(month_info, download(month_info))
    zones = load_bronze_zones(month_info)
    tlc_xml = load_bronze_tlc_xml(month_info)
    gate = quality_gate(month_info, bronze)
    [bronze, zones, tlc_xml] >> dbt_seed >> dbt_run >> dbt_test >> gate


nyc_taxi_elt()
