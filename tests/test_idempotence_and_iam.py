"""quality/idempotence.py comparison and iam/iam_setup.py dataset grants (no network)."""
from types import SimpleNamespace

import pytest

from idempotence import compare


def test_identical_snapshots_have_no_difference():
    snap = {"202501": {"bronze_rows": 10, "silver_rows": 7, "quarantine_rows": {"invalid_fare": 3}}}
    assert compare(snap, snap) == []


def test_changed_count_month_or_fingerprint_is_reported():
    before = {"202501": {"silver_rows": 7, "silver_fingerprint": 1}, "202502": {"silver_rows": 5}}
    after = {"202501": {"silver_rows": 7, "silver_fingerprint": 2}, "202503": {"silver_rows": 5}}
    assert compare(before, after) == [
        "202501 silver_fingerprint: 1 -> 2",
        "202502: present before only",
        "202503: present after only",
    ]


class _DatasetClient:
    def __init__(self, names):
        self.datasets = {n: SimpleNamespace(access_entries=[]) for n in names}
        self.updates = []

    def get_dataset(self, name):
        return self.datasets[name]

    def update_dataset(self, dataset, fields):
        self.updates.append(fields)


def test_sa_gets_writer_on_the_four_pipeline_datasets_only_once():
    pytest.importorskip("google.cloud.bigquery")
    import iam_setup

    assert iam_setup.PIPELINE_DATASETS == ["nyc_taxi", "nyc_taxi_bronze", "nyc_taxi_silver", "nyc_taxi_gold"]
    client = _DatasetClient(iam_setup.PIPELINE_DATASETS + ["telco_churn"])
    assert iam_setup.grant_dataset_writer(client) == iam_setup.PIPELINE_DATASETS
    for name in iam_setup.PIPELINE_DATASETS:
        (entry,) = client.datasets[name].access_entries
        assert (entry.role, entry.entity_id) == ("WRITER", iam_setup.SA_EMAIL)
    assert client.datasets["telco_churn"].access_entries == []
    # second run: nothing left to change
    assert iam_setup.grant_dataset_writer(client) == []
