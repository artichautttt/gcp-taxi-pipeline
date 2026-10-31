#!/usr/bin/env bash
# Rebuilds every BigQuery table of the project after the Sandbox expiry
# (tables expire 60 days after their last write; the datasets themselves stay).
# Everything comes back from the public sources and the versioned seeds:
#
#   1. legacy chain   nyc_taxi_pipeline (January 2025): yellow_tripdata_raw,
#                     trips_clean, daily_kpis (+ its 20 dbt tests)
#   2. taxi_zones     legacy zone table, from dbt/seeds/taxi_zone_lookup.csv
#                     (the VBA output, versioned)
#   3. medallion      nyc_taxi_elt, one run per month: bronze, silver, gold,
#                     quality gate and quality report
#
# Usage (repo root, gcloud ADC configured, Docker running):
#   scripts/rebuild_after_expiry.sh                    # 2025-01 2025-02 2025-03
#   scripts/rebuild_after_expiry.sh 2025-01 2025-04    # chosen months
#
# Cost: 0 (Sandbox quotas). Stops at the first failing step.
#   scripts/rebuild_after_expiry.sh --dry-run          # print the container command only
set -euo pipefail
cd "$(dirname "$0")/.."

dry_run=0
if [ "${1:-}" = "--dry-run" ]; then
    dry_run=1
    shift
fi
if [ $# -eq 0 ]; then
    set -- 2025-01 2025-02 2025-03
fi

steps="airflow db migrate > /dev/null"
steps+=" && echo '[rebuild] legacy chain' && airflow dags test nyc_taxi_pipeline 2025-01-01"
steps+=" && echo '[rebuild] taxi_zones' && python /opt/pipeline/scripts/load_legacy_taxi_zones.py"
for month in "$@"; do
    year=${month%-*}
    mm=$((10#${month#*-}))
    steps+=" && echo '[rebuild] nyc_taxi_elt ${month}'"
    steps+=" && airflow dags test nyc_taxi_elt --conf '{\"year\": ${year}, \"month\": ${mm}}'"
done

if [ "$dry_run" = 1 ]; then
    printf '%s\n' "$steps"
    exit 0
fi

# MSYS_NO_PATHCONV: keeps Git Bash on Windows from rewriting the container paths
MSYS_NO_PATHCONV=1 docker compose -f airflow/docker-compose.yml run --rm airflow bash -c "${steps}"
echo "[rebuild] done: $*"
