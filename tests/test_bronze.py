"""Bronze ingestion, without network: fake BigQuery client, local files."""
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import bronze
from conftest import FakeBigQuery

LOADED_AT = datetime(2026, 10, 30, 10, 0, tzinfo=timezone.utc)


def _trip_file(path, rows=3, extra_column=True):
    """A Parquet file with the 2025 TLC layout (Airport_fee, cbd_congestion_fee)."""
    columns = {}
    for name, kind in bronze.TRIP_COLUMNS:
        source_name = "Airport_fee" if name == "airport_fee" else name
        if kind == "TIMESTAMP":
            columns[source_name] = pa.array([datetime(2025, 1, 1, 8, i) for i in range(rows)], pa.timestamp("us"))
        elif kind == "STRING":
            columns[source_name] = pa.array(["N"] * rows)
        elif kind == "INTEGER":
            columns[source_name] = pa.array(list(range(1, rows + 1)), pa.int64())
        else:
            columns[source_name] = pa.array([1.5] * rows)
    if extra_column:
        columns["cbd_congestion_fee"] = pa.array([0.75] * rows)
    pq.write_table(pa.table(columns), path)
    return path


def test_table_names_and_run_id():
    assert bronze.trips_table_name(2025, 1) == "yellow_tripdata_202501"
    with pytest.raises(ValueError):
        bronze.trips_table_name(2025, 13)
    assert bronze.new_run_id(LOADED_AT) == "manual__2026-10-30T10:00:00+00:00"


def test_select_trip_columns_matches_case_and_drops_unlisted(tmp_path):
    table = bronze.select_trip_columns(pq.read_table(_trip_file(tmp_path / "f.parquet")))
    assert table.column_names == [name for name, _ in bronze.TRIP_COLUMNS]
    assert "cbd_congestion_fee" not in table.column_names


def test_select_trip_columns_rejects_a_missing_column(tmp_path):
    table = pq.read_table(_trip_file(tmp_path / "f.parquet")).drop_columns(["fare_amount"])
    with pytest.raises(ValueError, match="fare_amount"):
        bronze.select_trip_columns(table)


def test_ingestion_metadata_is_the_same_on_every_row():
    table = bronze.with_ingestion_metadata(pa.table({"a": [1, 2, 3]}), "src", "f.csv", LOADED_AT, "run-1")
    assert table.column_names == ["a", "_source", "_source_file", "_loaded_at", "_run_id"]
    assert set(table["_run_id"].to_pylist()) == {"run-1"}
    assert set(table["_source_file"].to_pylist()) == {"f.csv"}
    # compared as epoch microseconds: no time zone database needed
    assert table.schema.field("_loaded_at").type == pa.timestamp("us", tz="UTC")
    assert set(table["_loaded_at"].cast(pa.int64()).to_pylist()) == {int(LOADED_AT.timestamp() * 1_000_000)}


def test_load_trips_month_lands_every_row_and_logs_it(tmp_path, monkeypatch):
    pytest.importorskip("google.cloud.bigquery")
    _trip_file(tmp_path / "yellow_tripdata_2025-01.parquet", rows=5)
    client = FakeBigQuery()
    record = bronze.load_trips_month(2025, 1, "run-1", "p", client, data_dir=tmp_path)

    assert record["source_rows"] == record["loaded_rows"] == 5
    assert record["status"] == "ok"
    assert client.tables["p.nyc_taxi_bronze.yellow_tripdata_202501"] == 5
    (log,) = client.appended["p.nyc_taxi_bronze.ingestion_log"]
    assert log["run_id"] == "run-1" and log["source_file"] == "yellow_tripdata_2025-01.parquet"
    assert len(log["file_sha256"]) == 64
    dispositions = {t: c.write_disposition for t, c in client.load_configs}
    assert dispositions["p.nyc_taxi_bronze.yellow_tripdata_202501"] == "WRITE_TRUNCATE"
    assert dispositions["p.nyc_taxi_bronze.ingestion_log"] == "WRITE_APPEND"


def test_rerun_replaces_the_month_instead_of_appending(tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    _trip_file(tmp_path / "yellow_tripdata_2025-01.parquet", rows=4)
    client = FakeBigQuery()
    for run in ("run-1", "run-2"):
        bronze.load_trips_month(2025, 1, run, "p", client, data_dir=tmp_path)
    assert client.tables["p.nyc_taxi_bronze.yellow_tripdata_202501"] == 4
    assert [r["run_id"] for r in client.appended["p.nyc_taxi_bronze.ingestion_log"]] == ["run-1", "run-2"]


def test_count_mismatch_is_logged_then_raised(tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    _trip_file(tmp_path / "yellow_tripdata_2025-01.parquet", rows=5)
    client = FakeBigQuery(lose_rows={"p.nyc_taxi_bronze.yellow_tripdata_202501": 1})
    with pytest.raises(bronze.BronzeCountError, match="4 rows loaded, source has 5"):
        bronze.load_trips_month(2025, 1, "run-1", "p", client, data_dir=tmp_path)
    (log,) = client.appended["p.nyc_taxi_bronze.ingestion_log"]
    assert log["status"] == "count_mismatch"


def test_zone_lookup_is_loaded_as_strings(tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    (tmp_path / "taxi_zone_lookup.csv").write_text(
        '﻿"LocationID","Borough","Zone","service_zone"\n1,"EWR","Newark Airport","EWR"\n'
        '264,"Unknown","N/A","N/A"\n265,"N/A","Outside of NYC","N/A"\n',
        encoding="utf-8",
    )
    table = bronze.read_csv_as_strings(tmp_path / "taxi_zone_lookup.csv")
    assert table.column_names == ["LocationID", "Borough", "Zone", "service_zone"]
    assert table["LocationID"].to_pylist() == ["1", "264", "265"]

    client = FakeBigQuery()
    record = bronze.load_zone_lookup("run-1", "p", client, data_dir=tmp_path)
    assert record["loaded_rows"] == 3 and client.tables["p.nyc_taxi_bronze.taxi_zone_lookup"] == 3


def test_csv_with_a_ragged_row_is_refused(tmp_path):
    path = tmp_path / "z.csv"
    path.write_text("a,b\n1,2\n3\n", encoding="utf-8")
    with pytest.raises(ValueError, match="fields"):
        bronze.read_csv_as_strings(path)


def test_datasets_follow_the_environment(monkeypatch):
    monkeypatch.setenv("BQ_BRONZE_DATASET", "b")
    assert bronze.datasets()["bronze"] == "b"
