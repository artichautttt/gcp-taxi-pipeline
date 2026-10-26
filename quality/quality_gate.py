"""
Quality gate run at the end of the nyc_taxi_elt DAG, after dbt test.

It fails the run explicitly when:
  1. dbt test produced no result (empty selection, wrong target path), or a
     test ended in fail / error (warnings are reported, not blocking);
  2. the rows of the month do not reconcile: rows loaded by the raw load job
     must equal the rows accounted for in audit_trip_rows, and the rows of the
     month in fct_trips must equal the 'kept' count.

Check 2 is independent from dbt: the raw count comes from the load job, not
from a dbt query.
"""
import json
from collections import Counter
from pathlib import Path

BLOCKING_STATUSES = {"fail", "error", "runtime error"}


class QualityGateError(Exception):
    pass


def summarize_dbt_results(run_results_path: Path) -> dict:
    """Counts dbt test results by status; raises if empty or if a test failed."""
    path = Path(run_results_path)
    if not path.exists():
        raise QualityGateError(f"dbt run_results.json not found: {path}")
    results = json.loads(path.read_text(encoding="utf-8")).get("results", [])
    if not results:
        raise QualityGateError(f"no dbt test result in {path}")
    statuses = Counter(str(r.get("status")).lower() for r in results)
    blocking = sorted(
        r.get("unique_id", "?") for r in results if str(r.get("status")).lower() in BLOCKING_STATUSES
    )
    if blocking:
        raise QualityGateError(f"{len(blocking)} dbt test(s) not passing: {blocking}")
    return {"tests": len(results), **dict(statuses)}


def check_reconciliation(raw_rows_loaded: int, audit_rows: dict, fct_rows: int) -> list:
    """Returns the list of problems (empty when the month reconciles)."""
    problems = []
    if not audit_rows:
        return [f"no row in audit_trip_rows for this month (raw rows loaded: {raw_rows_loaded})"]
    audited = sum(audit_rows.values())
    kept = audit_rows.get("kept", 0)
    if audited != raw_rows_loaded:
        problems.append(f"raw rows loaded {raw_rows_loaded} != rows in audit_trip_rows {audited}")
    if kept != fct_rows:
        problems.append(f"kept rows in audit_trip_rows {kept} != rows in fct_trips {fct_rows}")
    return problems


def fetch_month_counts(client, project_id: str, dataset: str, pipeline_month: int):
    """Reads audit_trip_rows and the fct_trips count for one month (partition-pruned)."""
    from google.cloud import bigquery

    params = [bigquery.ScalarQueryParameter("m", "INT64", pipeline_month)]
    config = bigquery.QueryJobConfig(query_parameters=params)
    audit_sql = (
        f"select row_outcome, row_count from `{project_id}.{dataset}.audit_trip_rows` "
        "where source_month = @m"
    )
    fct_sql = f"select count(*) as n from `{project_id}.{dataset}.fct_trips` where pickup_month = @m"
    audit = {r["row_outcome"]: r["row_count"] for r in client.query(audit_sql, job_config=config).result()}
    fct_rows = next(iter(client.query(fct_sql, job_config=config).result()))["n"]
    return audit, fct_rows


def run_gate(run_results_path, raw_rows_loaded: int, pipeline_month: int,
             project_id: str, dataset: str, client=None) -> dict:
    tests = summarize_dbt_results(run_results_path)
    if client is None:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
    audit, fct_rows = fetch_month_counts(client, project_id, dataset, pipeline_month)
    problems = check_reconciliation(raw_rows_loaded, audit, fct_rows)
    summary = {
        "pipeline_month": pipeline_month,
        "dbt_tests": tests,
        "raw_rows_loaded": raw_rows_loaded,
        "fct_rows": fct_rows,
        "excluded_rows": {k: v for k, v in sorted(audit.items()) if k != "kept"},
    }
    print(f"[quality_gate] {json.dumps(summary)}")
    if problems:
        raise QualityGateError("; ".join(problems))
    return summary
