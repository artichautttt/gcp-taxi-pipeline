"""XML source (NYC Open Data c5iv-bn4s), without network: fake HTTP session."""
from types import SimpleNamespace

import pytest

import tlc_zone_trips_xml as tlc
from conftest import FakeBigQuery


def _row(zone, direction="Pick-up", month="2025-01-01", count=10):
    return (
        f'<row _id="r{zone}{direction[0]}"><metric_month>{month}</metric_month>'
        f"<industry>Yellow Taxi</industry><pickup_dropoff>{direction}</pickup_dropoff>"
        f"<locationid>{zone}</locationid><borough>Queens</borough><zone>Z{zone}</zone>"
        f"<trip_count>{count}</trip_count></row>"
    )


def _xml(rows, wrapper="rows"):
    """Socrata wraps rows in <response><rows> (/resource) or <response><row> (export)."""
    return f"<response><{wrapper}>{''.join(rows)}</{wrapper}></response>".encode()


@pytest.mark.parametrize("wrapper", ["rows", "row"])
def test_parse_rows_reads_both_socrata_layouts(wrapper):
    rows = tlc.parse_rows(_xml([_row(1), _row(1, "Drop-off")], wrapper))
    assert [(r["locationid"], r["pickup_dropoff"], r["trip_count"]) for r in rows] == [
        ("1", "Pick-up", "10"), ("1", "Drop-off", "10"),
    ]


def test_dtd_is_refused():
    content = b'<?xml version="1.0"?><!DOCTYPE r [<!ENTITY a "x">]><response></response>'
    with pytest.raises(tlc.TlcXmlError, match="DTD"):
        tlc.parse_rows(content)


def test_missing_field_and_invalid_xml_are_refused():
    with pytest.raises(tlc.TlcXmlError, match="missing fields"):
        tlc.parse_rows(b"<response><rows><row><metric_month>2025-01-01</metric_month></row></rows></response>")
    with pytest.raises(tlc.TlcXmlError, match="invalid XML"):
        tlc.parse_rows(b"<response><rows>")


def test_check_rows():
    rows = tlc.parse_rows(_xml([_row(1), _row(2)]))
    tlc.check_rows(rows, 2025, 1, expected=2)
    with pytest.raises(tlc.TlcXmlError, match="API reports 3"):
        tlc.check_rows(rows, 2025, 1, expected=3)
    with pytest.raises(tlc.TlcXmlError, match="outside 2025-02-01"):
        tlc.check_rows(rows, 2025, 2, expected=2)


def test_check_rows_refuses_a_full_page(monkeypatch):
    monkeypatch.setattr(tlc, "MAX_ROWS", 2)
    with pytest.raises(tlc.TlcXmlError, match="truncated"):
        tlc.check_rows(tlc.parse_rows(_xml([_row(1), _row(2)])), 2025, 1, expected=2)


class FakeSession:
    def __init__(self, xml: bytes, count: int):
        self.xml, self.count, self.calls = xml, count, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        if url == tlc.COUNT_URL:
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: [{"n": str(self.count)}])
        return SimpleNamespace(raise_for_status=lambda: None, content=self.xml)


def test_load_lands_the_month_with_the_api_count(tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    session = FakeSession(_xml([_row(1), _row(1, "Drop-off"), _row(2)]), count=3)
    client = FakeBigQuery()
    record = tlc.load_tlc_zone_trips(2025, 1, "run-1", "p", client, data_dir=tmp_path, session=session)

    assert record["loaded_rows"] == 3 and record["status"] == "ok"
    assert client.tables["p.nyc_taxi_bronze.tlc_zone_trips_202501"] == 3
    assert (tmp_path / "tlc_zone_trips_2025-01.xml").exists()
    where = session.calls[0][1]["$where"]
    assert where == "industry='Yellow Taxi' AND metric_month='2025-01-01'"


def test_unpublished_month_loads_nothing(tmp_path):
    session = FakeSession(_xml([]), count=0)
    client = FakeBigQuery()
    record = tlc.load_tlc_zone_trips(2026, 12, "run-1", "p", client, data_dir=tmp_path, session=session)
    assert record["status"] == "not_published"
    assert client.tables == {}
