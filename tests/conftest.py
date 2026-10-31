"""Shared test doubles: a fake BigQuery client that never touches the network."""
import io
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parent.parent
for folder in ("ingestion", "quality", "iam"):
    sys.path.insert(0, str(REPO / folder))


class FakeBigQuery:
    """Records load jobs and answers get_table with the rows it loaded.

    lose_rows: table_id -> rows to drop from the reported count (simulates a
    load that does not land every row).
    """

    def __init__(self, lose_rows=None):
        self.tables = {}          # table_id -> row count
        self.appended = {}        # table_id -> list of JSON rows (WRITE_APPEND loads)
        self.load_configs = []    # (table_id, job_config)
        self.lose_rows = lose_rows or {}

    def load_table_from_file(self, f, table_id, job_config=None):
        import pyarrow.parquet as pq

        rows = pq.read_table(io.BytesIO(f.read())).num_rows
        self.tables[table_id] = rows - self.lose_rows.get(table_id, 0)
        self.load_configs.append((table_id, job_config))
        return SimpleNamespace(result=lambda: None)

    def load_table_from_json(self, rows, table_id, job_config=None):
        self.appended.setdefault(table_id, []).extend(rows)
        self.load_configs.append((table_id, job_config))
        return SimpleNamespace(result=lambda: None)

    def get_table(self, table_id):
        return SimpleNamespace(num_rows=self.tables[table_id])


@pytest.fixture
def fake_bq():
    pytest.importorskip("google.cloud.bigquery")
    return FakeBigQuery()
