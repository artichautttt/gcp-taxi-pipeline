import sys
from pathlib import Path

import pytest

pytest.importorskip("requests")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
import download_taxi_data  # noqa: E402
from download_taxi_data import IncompleteDownloadError, download_month  # noqa: E402


class _FakeResponse:
    """Streams the given chunks; content_length can lie to simulate a cut connection."""

    def __init__(self, chunks, content_length=None, fail_after=None):
        self.chunks = chunks
        self.fail_after = fail_after
        length = sum(len(c) for c in chunks) if content_length is None else content_length
        self.headers = {"content-length": str(length)}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        for i, chunk in enumerate(self.chunks):
            if self.fail_after is not None and i == self.fail_after:
                raise ConnectionError("connection reset")
            yield chunk


def _patch_get(monkeypatch, response):
    monkeypatch.setattr(download_taxi_data.requests, "get", lambda *a, **k: response)


def test_complete_download_is_renamed(tmp_path, monkeypatch):
    _patch_get(monkeypatch, _FakeResponse([b"abc", b"def"]))
    path = download_month(2025, 3, data_dir=tmp_path)
    assert path.read_bytes() == b"abcdef"
    assert [p.name for p in tmp_path.iterdir()] == ["yellow_tripdata_2025-03.parquet"]


def test_short_download_leaves_no_file(tmp_path, monkeypatch):
    _patch_get(monkeypatch, _FakeResponse([b"abc"], content_length=10))
    with pytest.raises(IncompleteDownloadError, match="got 3 bytes"):
        download_month(2025, 3, data_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_interrupted_download_leaves_no_file(tmp_path, monkeypatch):
    _patch_get(monkeypatch, _FakeResponse([b"abc", b"def"], fail_after=1))
    with pytest.raises(ConnectionError):
        download_month(2025, 3, data_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_existing_complete_file_is_reused(tmp_path, monkeypatch):
    (tmp_path / "yellow_tripdata_2025-03.parquet").write_bytes(b"done")

    def _no_network(*a, **k):
        raise AssertionError("must not download again")

    monkeypatch.setattr(download_taxi_data.requests, "get", _no_network)
    assert download_month(2025, 3, data_dir=tmp_path).read_bytes() == b"done"
