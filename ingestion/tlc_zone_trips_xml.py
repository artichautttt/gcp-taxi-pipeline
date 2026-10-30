"""
XML source: monthly Yellow Taxi pickups and drop-offs per taxi zone, published
by the TLC on NYC Open Data, dataset c5iv-bn4s ("Pickups and Drop-offs by Taxi
Zone and Industry"), through the Socrata API in XML:

    https://data.cityofnewyork.us/resource/c5iv-bn4s.xml?$where=...

Used as an external, informative control: gold.tlc_zone_reconciliation
compares these published counts with the trips of the TLC file, per zone. The
TLC filters its own counts, so the two never match to the trip.

Landed in bronze.tlc_zone_trips_YYYYMM as received: every field a string,
plus the ingestion columns. The row count is checked against the count the
API reports for the same filter. A month the TLC has not published yet
returns no row: nothing is loaded, the run goes on.
"""
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bronze import METADATA_COLUMNS, _client, file_sha256, land, with_ingestion_metadata  # noqa: E402
from download_taxi_data import DATA_DIR  # noqa: E402

SOURCE = "nyc_open_data_c5iv-bn4s"
XML_URL = "https://data.cityofnewyork.us/resource/c5iv-bn4s.xml"
COUNT_URL = "https://data.cityofnewyork.us/resource/c5iv-bn4s.json"
FIELDS = ("metric_month", "industry", "pickup_dropoff", "locationid", "borough", "zone", "trip_count")
INDUSTRY = "Yellow Taxi"
# About 520 rows per month (2 directions x ~260 zones); a page this size never
# truncates a month, and check_rows fails if it ever would.
MAX_ROWS = 5000


class TlcXmlError(Exception):
    pass


def where_clause(year: int, month: int) -> str:
    return f"industry='{INDUSTRY}' AND metric_month='{year:04d}-{month:02d}-01'"


def parse_rows(content: bytes) -> list[dict]:
    """Parses the Socrata XML answer into dicts of strings. Refuses any DTD
    (no entity expansion on a downloaded file)."""
    if b"<!DOCTYPE" in content or b"<!ENTITY" in content:
        raise TlcXmlError("XML with a DTD or entity declaration refused")
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        raise TlcXmlError(f"invalid XML: {e}") from e
    rows = []
    for row in root.iter("row"):
        if row.find("metric_month") is None:  # <response><rows><row> wrapper levels
            continue
        values = {child.tag: (child.text or "") for child in row}
        missing = [f for f in FIELDS if f not in values]
        if missing:
            raise TlcXmlError(f"row {row.get('_id')}: missing fields {missing}")
        rows.append({f: values[f] for f in FIELDS})
    return rows


def check_rows(rows: list[dict], year: int, month: int, expected: int) -> None:
    month_value = f"{year:04d}-{month:02d}-01"
    wrong = {(r["metric_month"][:10], r["industry"]) for r in rows} - {(month_value, INDUSTRY)}
    if wrong:
        raise TlcXmlError(f"rows outside {month_value} / {INDUSTRY}: {sorted(wrong)[:5]}")
    if len(rows) >= MAX_ROWS:
        raise TlcXmlError(f"{len(rows)} rows: page limit {MAX_ROWS} reached, the month may be truncated")
    if len(rows) != expected:
        raise TlcXmlError(f"{len(rows)} rows in the XML, the API reports {expected}")


def download(year: int, month: int, data_dir: Path = DATA_DIR, session=None) -> tuple[Path, int]:
    """Writes the month's XML to data/ and returns (path, row count the API reports).
    Always re-downloaded: the TLC revises this dataset."""
    import requests

    session = session or requests
    where = where_clause(year, month)
    count = session.get(COUNT_URL, params={"$select": "count(*) as n", "$where": where}, timeout=60)
    count.raise_for_status()
    expected = int(count.json()[0]["n"])

    data_dir.mkdir(parents=True, exist_ok=True)
    dest = data_dir / f"tlc_zone_trips_{year:04d}-{month:02d}.xml"
    part = dest.with_suffix(".xml.part")
    try:
        resp = session.get(XML_URL, params={"$where": where, "$limit": str(MAX_ROWS), "$order": ":id"},
                           timeout=120)
        resp.raise_for_status()
        part.write_bytes(resp.content)
        part.replace(dest)
    finally:
        part.unlink(missing_ok=True)
    return dest, expected


def load_tlc_zone_trips(year: int, month: int, run_id: str, project_id: str,
                        client=None, data_dir: Path = DATA_DIR, session=None) -> dict:
    path, expected = download(year, month, data_dir, session)
    rows = parse_rows(path.read_bytes())
    check_rows(rows, year, month, expected)
    if not rows:
        print(f"[bronze] {path.name}: no TLC row for {year}-{month:02d} yet, nothing loaded")
        return {"source_file": path.name, "source_rows": 0, "loaded_rows": 0, "status": "not_published"}
    raw = pa.table({f: pa.array([r[f] for r in rows], pa.string()) for f in FIELDS})
    loaded_at = datetime.now(timezone.utc)
    table = with_ingestion_metadata(raw, SOURCE, path.name, loaded_at, run_id)
    return land(_client(client, project_id), project_id, table, f"tlc_zone_trips_{year:04d}{month:02d}",
                [(f, "STRING") for f in FIELDS] + METADATA_COLUMNS, {
                    "run_id": run_id,
                    "source": SOURCE,
                    "source_file": path.name,
                    "source_uri": f"{XML_URL}?$where={where_clause(year, month)}",
                    "file_sha256": file_sha256(path),
                    "file_bytes": path.stat().st_size,
                    "loaded_at": loaded_at.isoformat(),
                })
