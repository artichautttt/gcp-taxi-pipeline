"""
Checks that re-running a month changes nothing: same rows in every layer, for
the re-run month and for every other month.

    python quality/idempotence.py snapshot > before.json
    ... re-run one month (DAG or bronze.py + dbt + quality_gate.py) ...
    python quality/idempotence.py compare before.json     # exit 1 on any difference

The snapshot holds, per month: bronze rows, silver rows, quarantine rows per
rule, and a BIT_XOR of the trip_id of silver.trips and of the quarantine (a
content fingerprint: the same counts with different rows would change it).
Run ids and load times are expected to change and are not compared.
"""
import json
import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from quality_gate import datasets  # noqa: E402


def snapshot(client, project_id: str, ds: dict) -> dict:
    p = project_id
    queries = {
        "bronze_rows": (
            f"select cast(_table_suffix as int64) as m, count(*) as v "
            f"from `{p}.{ds['bronze']}.yellow_tripdata_*` group by m"
        ),
        "silver_rows": f"select pickup_month as m, count(*) as v from `{p}.{ds['silver']}.trips` group by m",
        "silver_fingerprint": (
            f"select pickup_month as m, bit_xor(trip_id) as v from `{p}.{ds['silver']}.trips` group by m"
        ),
        "quarantine_fingerprint": (
            f"select source_month as m, bit_xor(trip_id) as v "
            f"from `{p}.{ds['silver']}.trips_quarantine` group by m"
        ),
    }
    out: dict = {}
    for name, sql in queries.items():
        for row in client.query(sql).result():
            out.setdefault(str(row["m"]), {})[name] = row["v"]
    for row in client.query(
        f"select source_month as m, rejection_reason as r, count(*) as v "
        f"from `{p}.{ds['silver']}.trips_quarantine` group by m, r"
    ).result():
        out.setdefault(str(row["m"]), {}).setdefault("quarantine_rows", {})[row["r"]] = row["v"]
    return dict(sorted(out.items()))


def compare(before: dict, after: dict) -> list:
    """Returns one line per difference (empty when nothing changed)."""
    diffs = []
    for month in sorted(set(before) | set(after)):
        b, a = before.get(month), after.get(month)
        if b is None or a is None:
            diffs.append(f"{month}: present {'after' if b is None else 'before'} only")
            continue
        for key in sorted(set(b) | set(a)):
            if b.get(key) != a.get(key):
                diffs.append(f"{month} {key}: {b.get(key)} -> {a.get(key)}")
    return diffs


if __name__ == "__main__":
    import argparse

    from google.cloud import bigquery

    load_dotenv()
    parser = argparse.ArgumentParser(description="Snapshot / compare the medallion layers")
    parser.add_argument("action", choices=["snapshot", "compare"])
    parser.add_argument("before", nargs="?", help="snapshot file (compare)")
    args = parser.parse_args()

    project = os.environ["GCP_PROJECT_ID"]
    current = snapshot(bigquery.Client(project=project), project, datasets())
    if args.action == "snapshot":
        print(json.dumps(current, indent=2))
        sys.exit(0)
    with open(args.before, encoding="utf-8") as f:
        differences = compare(json.load(f), current)
    for line in differences:
        print(f"[idempotence] DIFF {line}")
    print(f"[idempotence] {'OK: no difference' if not differences else f'{len(differences)} difference(s)'}"
          f" over {len(current)} month(s)")
    sys.exit(1 if differences else 0)
