"""
Quality gate run at the end of the nyc_taxi_elt DAG, after dbt test, and the
quality report it writes for every run.

The gate fails the run explicitly when:
  1. dbt test produced no result (empty selection, wrong target path), or a
     test ended in fail / error (warnings are reported, not blocking);
  2. the rows of the month do not reconcile, layer by layer:
       source file rows = bronze rows                (ingestion)
       bronze rows      = rows in audit_trip_rows    (every row classified)
       audit 'kept'     = silver.trips rows of the month
       audit per rule   = quarantine rows per rule
       silver + quarantine = bronze
       quarantine rows carry this run's run_id       (rebuilt by this run)

The source row count comes from the Parquet file (bronze ingestion), not from
a dbt query.

Whatever the outcome, one row per (run, outcome) is appended to
<silver>.quality_report: rows read, rows valid, rows rejected per rule, rows
flagged per warning, TLC zones in alert, dbt test counts and the gate status.
Appended with a load job (WRITE_APPEND): the Sandbox refuses DML.
"""
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

BLOCKING_STATUSES = {"fail", "error", "runtime error"}
WARNING_FLAGS = ("flag_unknown_zone", "flag_passenger_count", "flag_short_trip")
REPORT_COLUMNS = [
    ("run_id", "STRING"),
    ("pipeline_month", "INTEGER"),
    ("reported_at", "TIMESTAMP"),
    ("gate_status", "STRING"),
    ("outcome_kind", "STRING"),
    ("outcome", "STRING"),
    ("row_count", "INTEGER"),
    ("dbt_tests", "INTEGER"),
    ("dbt_tests_not_passing", "INTEGER"),
    ("dbt_warnings", "INTEGER"),
    ("problems", "STRING"),
]


class QualityGateError(Exception):
    pass


def datasets() -> dict:
    return {
        "bronze": os.environ.get("BQ_BRONZE_DATASET", "nyc_taxi_bronze"),
        "silver": os.environ.get("BQ_SILVER_DATASET", "nyc_taxi_silver"),
        "gold": os.environ.get("BQ_GOLD_DATASET", "nyc_taxi_gold"),
    }


def read_dbt_results(run_results_path: Path) -> dict:
    """Counts dbt test results by status. Never raises: the gate decides."""
    path = Path(run_results_path)
    if not path.exists():
        return {"tests": 0, "not_passing": [], "warnings": 0, "statuses": {},
                "problem": f"dbt run_results.json not found: {path}"}
    results = json.loads(path.read_text(encoding="utf-8")).get("results", [])
    statuses = Counter(str(r.get("status")).lower() for r in results)
    not_passing = sorted(
        r.get("unique_id", "?") for r in results if str(r.get("status")).lower() in BLOCKING_STATUSES
    )
    problem = None
    if not results:
        problem = f"no dbt test result in {path}"
    elif not_passing:
        problem = f"{len(not_passing)} dbt test(s) not passing: {not_passing}"
    return {"tests": len(results), "not_passing": not_passing, "warnings": statuses.get("warn", 0),
            "statuses": dict(statuses), "problem": problem}


def summarize_dbt_results(run_results_path: Path) -> dict:
    """Counts dbt test results by status; raises if empty or if a test failed."""
    result = read_dbt_results(run_results_path)
    if result["problem"]:
        raise QualityGateError(result["problem"])
    return {"tests": result["tests"], **result["statuses"]}


def check_reconciliation(source_rows: int, counts: dict, run_id: str | None = None) -> list:
    """Returns the list of problems (empty when the month reconciles).

    counts: bronze_rows, audit {outcome: rows}, silver_rows,
            quarantine {rule: rows}, quarantine_run_ids [ids]
    """
    problems = []
    bronze, audit = counts["bronze_rows"], counts["audit"]
    silver, quarantine = counts["silver_rows"], counts["quarantine"]
    if source_rows != bronze:
        problems.append(f"source rows {source_rows} != bronze rows {bronze}")
    if not audit:
        return problems + [f"no row in audit_trip_rows for this month (bronze rows: {bronze})"]
    if sum(audit.values()) != bronze:
        problems.append(f"bronze rows {bronze} != rows in audit_trip_rows {sum(audit.values())}")
    if audit.get("kept", 0) != silver:
        problems.append(f"kept rows in audit_trip_rows {audit.get('kept', 0)} != rows in silver.trips {silver}")
    rejected = {k: v for k, v in audit.items() if k != "kept"}
    if rejected != quarantine:
        problems.append(f"rejected rows per rule in audit_trip_rows {rejected} != quarantine {quarantine}")
    if silver + sum(quarantine.values()) != bronze:
        problems.append(f"silver {silver} + quarantine {sum(quarantine.values())} != bronze {bronze}")
    run_ids = set(counts.get("quarantine_run_ids", []))
    if run_id is not None and quarantine and run_ids != {run_id}:
        problems.append(f"quarantine run_id {sorted(run_ids)} != this run {run_id}")
    return problems


def fetch_month_counts(client, project_id: str, ds: dict, pipeline_month: int) -> dict:
    """Reads the counts of one month in every layer (partition-pruned queries)."""
    from google.cloud import bigquery

    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("m", "INT64", pipeline_month)]
    )

    def rows(sql):
        return list(client.query(sql, job_config=config).result())

    silver = f"{project_id}.{ds['silver']}"
    bronze_table = f"{project_id}.{ds['bronze']}.yellow_tripdata_{pipeline_month}"
    flags = ", ".join(f"countif({f}) as {f}" for f in WARNING_FLAGS)
    trips = rows(f"select count(*) as n, {flags} from `{silver}.trips` where pickup_month = @m")[0]
    quarantine = rows(
        f"select rejection_reason, count(*) as n, array_agg(distinct run_id) as run_ids "
        f"from `{silver}.trips_quarantine` where source_month = @m group by rejection_reason"
    )
    alerts = rows(
        f"select countif(is_alert) as n from `{project_id}.{ds['gold']}.tlc_zone_reconciliation` "
        "where month = @m"
    )
    return {
        "bronze_rows": rows(f"select count(*) as n from `{bronze_table}`")[0]["n"],
        "audit": {r["row_outcome"]: r["row_count"] for r in rows(
            f"select row_outcome, row_count from `{silver}.audit_trip_rows` where source_month = @m"
        )},
        "silver_rows": trips["n"],
        "flags": {f: trips[f] for f in WARNING_FLAGS},
        "quarantine": {r["rejection_reason"]: r["n"] for r in quarantine},
        "quarantine_run_ids": sorted({i for r in quarantine for i in r["run_ids"]}),
        "tlc_alert_zones": alerts[0]["n"] if alerts else 0,
    }


def report_rows(run_id: str, pipeline_month: int, source_rows: int, counts: dict,
                dbt: dict, problems: list, reported_at: datetime | None = None) -> list:
    """One row per outcome of the run: read, valid, rejected per rule, warnings."""
    reported_at = (reported_at or datetime.now(timezone.utc)).isoformat()
    base = {
        "run_id": run_id,
        "pipeline_month": pipeline_month,
        "reported_at": reported_at,
        "gate_status": "failed" if problems else "passed",
        "dbt_tests": dbt["tests"],
        "dbt_tests_not_passing": len(dbt["not_passing"]),
        "dbt_warnings": dbt["warnings"],
        "problems": "; ".join(problems) or None,
    }
    outcomes = [
        ("read", "source_rows", source_rows),
        ("read", "bronze_rows", counts.get("bronze_rows")),
        ("valid", "kept", counts.get("silver_rows")),
    ]
    outcomes += [("rejected", rule, n) for rule, n in sorted(counts.get("quarantine", {}).items())]
    outcomes += [("warning", flag, n) for flag, n in sorted(counts.get("flags", {}).items())]
    outcomes.append(("tlc_control", "tlc_alert_zones", counts.get("tlc_alert_zones")))
    return [{**base, "outcome_kind": kind, "outcome": name, "row_count": n} for kind, name, n in outcomes]


def write_report(client, table_id: str, rows: list) -> None:
    """Appends the rows with a load job (WRITE_APPEND), creating the table if needed."""
    from google.cloud import bigquery

    config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(name, kind) for name, kind in REPORT_COLUMNS],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    client.load_table_from_json(rows, table_id, job_config=config).result()


def run_gate(run_results_path, source_rows: int, pipeline_month: int, run_id: str,
             project_id: str, client=None, ds: dict | None = None) -> dict:
    ds = ds or datasets()
    if client is None:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
    dbt = read_dbt_results(run_results_path)
    counts = fetch_month_counts(client, project_id, ds, pipeline_month)
    problems = ([dbt["problem"]] if dbt["problem"] else []) + check_reconciliation(source_rows, counts, run_id)
    rows = report_rows(run_id, pipeline_month, source_rows, counts, dbt, problems)
    write_report(client, f"{project_id}.{ds['silver']}.quality_report", rows)
    summary = {
        "run_id": run_id,
        "pipeline_month": pipeline_month,
        "gate_status": rows[0]["gate_status"],
        "dbt_tests": dbt["tests"],
        "dbt_warnings": dbt["warnings"],
        "source_rows": source_rows,
        **{k: counts[k] for k in ("bronze_rows", "silver_rows", "quarantine", "flags", "tlc_alert_zones")},
    }
    print(f"[quality_gate] {json.dumps(summary)}")
    if problems:
        raise QualityGateError("; ".join(problems))
    return summary


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser(description="Quality gate + report for one month, outside Airflow")
    parser.add_argument("--month", type=int, required=True, help="YYYYMM")
    parser.add_argument("--source-rows", type=int, required=True, help="rows of the source Parquet file")
    parser.add_argument("--run-id", required=True, help="run id given to bronze.py and dbt")
    parser.add_argument("--run-results", default="dbt/target/run_results.json")
    args = parser.parse_args()
    try:
        run_gate(Path(args.run_results), args.source_rows, args.month, args.run_id,
                 os.environ["GCP_PROJECT_ID"])
    except QualityGateError as e:
        print(f"[quality_gate] FAILED: {e}", file=sys.stderr)
        sys.exit(1)
