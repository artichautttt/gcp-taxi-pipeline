"""
Imports the real DAG files with Airflow's DagBag (the same parser as the
scheduler) and checks their structure. Skipped when Airflow is not installed
(Airflow does not run natively on Windows), except when REQUIRE_AIRFLOW=1,
as in CI, where a missing Airflow is a failure, not a skip.
"""
import os
from pathlib import Path

import pytest

if os.environ.get("REQUIRE_AIRFLOW") == "1":
    import airflow  # noqa: F401
else:
    pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parent.parent / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_both_dags_loaded(dagbag):
    assert {"nyc_taxi_pipeline", "nyc_taxi_elt"} <= set(dagbag.dag_ids)


def test_elt_chain(dagbag):
    dag = dagbag.dags["nyc_taxi_elt"]
    expected = [
        "check_source", "download", "load_bronze", "dbt_seed", "dbt_run", "dbt_test", "quality_gate",
    ]
    loaders = ["load_bronze_zones", "load_bronze_tlc_xml"]
    assert sorted(dag.task_ids) == sorted(expected + loaders)
    for upstream, downstream in zip(expected, expected[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)
    # every bronze load runs before dbt, after the source check
    for loader in loaders:
        assert "check_source" in dag.get_task(loader).upstream_task_ids
        assert "dbt_seed" in dag.get_task(loader).downstream_task_ids
    # the gate also receives the source row count straight from load_bronze
    assert "load_bronze" in dag.get_task("quality_gate").upstream_task_ids


def test_elt_dbt_commands_pass_the_month_and_run_id(dagbag):
    dag = dagbag.dags["nyc_taxi_elt"]
    for task_id in ("dbt_seed", "dbt_run", "dbt_test"):
        command = dag.get_task(task_id).bash_command
        assert "pipeline_month: {{ params.year * 100 + params.month }}" in command
        assert 'run_id: "{{ run_id }}"' in command
        assert "/tmp/dbt_elt/{{ run_id" in command
    assert "--exclude source:nyc_taxi" in dag.get_task("dbt_test").bash_command


def test_legacy_dag_runs_only_its_source_tests(dagbag):
    dag = dagbag.dags["nyc_taxi_pipeline"]
    assert "--select source:nyc_taxi" in dag.get_task("dbt_test").bash_command


@pytest.mark.parametrize(
    "name,value",
    [("month", 13), ("month", 0), ("year", 2023), ("year", 2027), ("year", "2025")],
)
def test_elt_params_reject_bad_values(dagbag, name, value):
    dag = dagbag.dags["nyc_taxi_elt"]
    with pytest.raises(Exception):
        dag.params.get_param(name).resolve(value)


def test_elt_params_accept_default(dagbag):
    dag = dagbag.dags["nyc_taxi_elt"]
    assert dag.params.get_param("month").resolve(2) == 2
    assert dag.params.get_param("year").resolve(2025) == 2025
    assert dag.params.get_param("year").resolve(2026) == 2026


def test_elt_year_range_matches_dim_date(dagbag):
    """The DAG must only accept years that dim_date covers (dbt_project.yml vars)."""
    import yaml

    project = yaml.safe_load((DAGS_DIR.parent.parent / "dbt" / "dbt_project.yml").read_text(encoding="utf-8"))
    start, end = project["vars"]["dim_date_start"], project["vars"]["dim_date_end"]
    assert start.endswith("-01-01") and end.endswith("-12-31"), (start, end)
    schema = dagbag.dags["nyc_taxi_elt"].params.get_param("year").schema
    assert (schema["minimum"], schema["maximum"]) == (int(start[:4]), int(end[:4]))
