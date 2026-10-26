"""
Downloads a monthly NYC Yellow Taxi trip data file (Parquet) from the
official NYC TLC public dataset and saves it locally in ./data/.

Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""
import argparse
import sys
from pathlib import Path

import requests

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"


class IncompleteDownloadError(Exception):
    pass


def download_month(year: int, month: int, data_dir: Path = DATA_DIR) -> Path:
    """Downloads one month. The file is written to <name>.part, its size is
    checked against Content-Length, then it is renamed: an interrupted
    download never leaves a truncated .parquet that a re-run would reuse."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    data_dir.mkdir(parents=True, exist_ok=True)
    dest = data_dir / filename
    part = data_dir / f"{filename}.part"

    if dest.exists():
        print(f"[skip] {dest} already exists ({dest.stat().st_size / 1e6:.1f} MB)")
        return dest

    print(f"[download] {url}")
    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            written = 0
            with open(part, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    written += len(chunk)
                    if total:
                        pct = written / total * 100
                        print(f"\r  {written / 1e6:.1f} / {total / 1e6:.1f} MB ({pct:.0f}%)", end="")
        size = part.stat().st_size
        if total and size != total:
            raise IncompleteDownloadError(f"{url}: got {size} bytes, Content-Length is {total}")
        if size == 0:
            raise IncompleteDownloadError(f"{url}: empty file")
        part.replace(dest)
    finally:
        part.unlink(missing_ok=True)
    print(f"\n[done] saved to {dest}")
    return dest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download NYC Yellow Taxi trip data (Parquet)")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=1)
    args = parser.parse_args()

    try:
        download_month(args.year, args.month)
    except requests.HTTPError as e:
        print(f"[error] download failed: {e}", file=sys.stderr)
        sys.exit(1)
