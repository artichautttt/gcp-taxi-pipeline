{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=monthly_partition('source_month'),
    on_schema_change='fail'
) }}

-- Row accounting per bronze file month: rows kept in silver.trips ('kept')
-- and rows sent to quarantine, per rule. For each month,
-- sum(row_count) = rows of bronze yellow_tripdata_YYYYMM (tested).

select
    source_month,
    coalesce(exclusion_reason, 'kept') as row_outcome,
    count(*) as row_count,
    '{{ current_run_id() }}' as run_id
from {{ ref('int_trips_classified') }}
group by source_month, row_outcome
