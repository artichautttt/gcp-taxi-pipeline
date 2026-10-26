import sys
from pathlib import Path

import pytest

pytest.importorskip("google.cloud.bigquery")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
from load_raw_month import raw_table_name  # noqa: E402


def test_raw_table_name_is_zero_padded():
    assert raw_table_name(2025, 1) == "raw_yellow_tripdata_202501"
    assert raw_table_name(2025, 12) == "raw_yellow_tripdata_202512"


@pytest.mark.parametrize("month", [0, 13])
def test_raw_table_name_rejects_bad_month(month):
    with pytest.raises(ValueError):
        raw_table_name(2025, month)
