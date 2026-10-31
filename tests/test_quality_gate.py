import json
from datetime import datetime, timezone

import pytest

from conftest import FakeBigQuery
from quality_gate import (
    QualityGateError,
    check_reconciliation,
    read_dbt_results,
    report_rows,
    run_gate,
    summarize_dbt_results,
)

DS = {"bronze": "b", "silver": "s", "gold": "g"}


def _run_results(tmp_path, statuses):
    path = tmp_path / "run_results.json"
    results = [{"unique_id": f"test.nyc_taxi.t{i}", "status": s} for i, s in enumerate(statuses)]
    path.write_text(json.dumps({"results": results}), encoding="utf-8")
    return path


def _counts(bronze=10, silver=7, quarantine=None, audit=None, run_ids=("run-1",)):
    quarantine = {"invalid_fare": 3} if quarantine is None else quarantine
    audit = {"kept": silver, **quarantine} if audit is None else audit
    return {
        "bronze_rows": bronze,
        "audit": audit,
        "silver_rows": silver,
        "quarantine": quarantine,
        "quarantine_run_ids": list(run_ids),
        "flags": {"flag_short_trip": 2, "flag_unknown_zone": 1, "flag_passenger_count": 0},
        "tlc_alert_zones": 4,
    }


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


def test_warnings_are_counted_not_blocking(tmp_path):
    result = read_dbt_results(_run_results(tmp_path, ["pass", "warn"]))
    assert result["warnings"] == 1 and result["problem"] is None


def test_reconciliation_ok():
    assert check_reconciliation(10, _counts(), "run-1") == []


def test_source_and_bronze_must_match():
    assert "source rows 11 != bronze rows 10" in check_reconciliation(11, _counts())[0]


def test_silver_plus_quarantine_must_equal_bronze():
    problems = check_reconciliation(10, _counts(silver=6, audit={"kept": 7, "invalid_fare": 3}))
    assert any("kept rows in audit_trip_rows 7 != rows in silver.trips 6" in p for p in problems)
    assert any("silver 6 + quarantine 3 != bronze 10" in p for p in problems)


def test_quarantine_must_match_the_audit_rule_by_rule():
    counts = _counts(quarantine={"invalid_fare": 2, "duplicate": 1}, audit={"kept": 7, "invalid_fare": 3})
    problems = check_reconciliation(10, counts)
    assert len(problems) == 1 and "per rule" in problems[0]


def test_quarantine_must_come_from_this_run():
    problems = check_reconciliation(10, _counts(run_ids=("run-0",)), "run-1")
    assert problems == ["quarantine run_id ['run-0'] != this run run-1"]


def test_month_not_built():
    assert "no row in audit_trip_rows" in check_reconciliation(10, _counts(audit={}))[0]


def test_report_has_read_valid_rejected_and_warning_rows():
    dbt = {"tests": 61, "not_passing": [], "warnings": 1}
    when = datetime(2026, 10, 30, tzinfo=timezone.utc)
    rows = report_rows("run-1", 202501, 10, _counts(), dbt, [], when)
    by_outcome = {(r["outcome_kind"], r["outcome"]): r["row_count"] for r in rows}
    assert by_outcome == {
        ("read", "source_rows"): 10,
        ("read", "bronze_rows"): 10,
        ("valid", "kept"): 7,
        ("rejected", "invalid_fare"): 3,
        ("warning", "flag_passenger_count"): 0,
        ("warning", "flag_short_trip"): 2,
        ("warning", "flag_unknown_zone"): 1,
        ("tlc_control", "tlc_alert_zones"): 4,
    }
    assert {r["gate_status"] for r in rows} == {"passed"}
    assert {(r["run_id"], r["dbt_tests"], r["dbt_warnings"]) for r in rows} == {("run-1", 61, 1)}


class _GateClient(FakeBigQuery):
    """Answers the gate queries with fixed counts, records the report load."""

    def __init__(self, counts):
        super().__init__()
        self.counts = counts

    def query(self, sql, job_config=None):
        c = self.counts
        if "audit_trip_rows" in sql:
            rows = [{"row_outcome": k, "row_count": v} for k, v in c["audit"].items()]
        elif "trips_quarantine" in sql:
            rows = [{"rejection_reason": k, "n": v, "run_ids": c["quarantine_run_ids"]}
                    for k, v in c["quarantine"].items()]
        elif "tlc_zone_reconciliation" in sql:
            rows = [{"n": c["tlc_alert_zones"]}]
        elif ".trips`" in sql:
            rows = [{"n": c["silver_rows"], **c["flags"]}]
        else:  # bronze table
            rows = [{"n": c["bronze_rows"]}]
        return type("Job", (), {"result": lambda _self: iter(rows)})()


def test_run_gate_writes_the_report_and_passes(tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    client = _GateClient(_counts())
    summary = run_gate(_run_results(tmp_path, ["pass"] * 3), 10, 202501, "run-1", "p", client, DS)

    assert summary["gate_status"] == "passed" and summary["silver_rows"] == 7
    report = client.appended["p.s.quality_report"]
    assert len(report) == 8 and {r["gate_status"] for r in report} == {"passed"}
    (table_id, config), = client.load_configs
    assert (table_id, config.write_disposition) == ("p.s.quality_report", "WRITE_APPEND")


def test_run_gate_writes_the_report_then_fails(tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    client = _GateClient(_counts(silver=6, audit={"kept": 7, "invalid_fare": 3}))
    with pytest.raises(QualityGateError, match="silver 6"):
        run_gate(_run_results(tmp_path, ["pass", "fail"]), 10, 202501, "run-1", "p", client, DS)
    report = client.appended["p.s.quality_report"]
    assert {r["gate_status"] for r in report} == {"failed"}
    assert "dbt test(s) not passing" in report[0]["problems"]
