import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "quality"))
from quality_gate import (  # noqa: E402
    QualityGateError,
    check_reconciliation,
    run_gate,
    summarize_dbt_results,
)


def _run_results(tmp_path, statuses):
    path = tmp_path / "run_results.json"
    results = [{"unique_id": f"test.nyc_taxi.t{i}", "status": s} for i, s in enumerate(statuses)]
    path.write_text(json.dumps({"results": results}), encoding="utf-8")
    return path


def test_summary_counts_statuses(tmp_path):
    summary = summarize_dbt_results(_run_results(tmp_path, ["pass", "pass", "warn"]))
    assert summary == {"tests": 3, "pass": 2, "warn": 1}


@pytest.mark.parametrize("bad", ["fail", "error"])
def test_failed_or_errored_test_blocks(tmp_path, bad):
    with pytest.raises(QualityGateError, match="t1"):
        summarize_dbt_results(_run_results(tmp_path, ["pass", bad]))


def test_empty_or_missing_results_block(tmp_path):
    with pytest.raises(QualityGateError, match="no dbt test result"):
        summarize_dbt_results(_run_results(tmp_path, []))
    with pytest.raises(QualityGateError, match="not found"):
        summarize_dbt_results(tmp_path / "absent.json")


def test_reconciliation_ok():
    assert check_reconciliation(10, {"kept": 7, "invalid_fare": 3}, 7) == []


def test_reconciliation_detects_lost_rows_and_fct_mismatch():
    problems = check_reconciliation(11, {"kept": 7, "invalid_fare": 3}, 6)
    assert len(problems) == 2
    assert "raw rows loaded 11 != rows in audit_trip_rows 10" in problems[0]
    assert "kept rows in audit_trip_rows 7 != rows in fct_trips 6" in problems[1]


def test_reconciliation_month_not_built():
    assert "no row in audit_trip_rows" in check_reconciliation(10, {}, 0)[0]


class _FakeClient:
    """Answers the two gate queries with fixed rows, no BigQuery call."""

    def __init__(self, audit, fct_rows):
        self.audit, self.fct_rows = audit, fct_rows

    def query(self, sql, job_config=None):
        rows = (
            [{"row_outcome": k, "row_count": v} for k, v in self.audit.items()]
            if "audit_trip_rows" in sql
            else [{"n": self.fct_rows}]
        )
        return type("Job", (), {"result": lambda _self: iter(rows)})()


def test_run_gate_end_to_end_with_fake_client(tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    path = _run_results(tmp_path, ["pass"] * 3)
    summary = run_gate(path, 10, 202501, "p", "d", client=_FakeClient({"kept": 8, "duplicate": 2}, 8))
    assert summary["fct_rows"] == 8
    assert summary["excluded_rows"] == {"duplicate": 2}

    with pytest.raises(QualityGateError):
        run_gate(path, 10, 202501, "p", "d", client=_FakeClient({"kept": 8, "duplicate": 2}, 7))
