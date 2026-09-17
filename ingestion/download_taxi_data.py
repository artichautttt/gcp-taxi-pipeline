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


def download_month(year: int, month: int) -> Path:
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATA_DIR / filename

    if dest.exists():
        print(f"[skip] {dest} already exists ({dest.stat().st_size / 1e6:.1f} MB)")
        return dest

    print(f"[download] {url}")
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        written = 0
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                written += len(chunk)
                if total:
                    pct = written / total * 100
                    print(f"\r  {written / 1e6:.1f} / {total / 1e6:.1f} MB ({pct:.0f}%)", end="")
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
