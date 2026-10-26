{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=monthly_partition('source_month'),
    on_schema_change='fail'
) }}

-- Row accounting per raw file month: how many rows went to fct_trips ('kept')
-- and how many were excluded, per reason. For each month,
-- sum(row_count) = rows of raw_yellow_tripdata_YYYYMM (tested).

select
    source_month,
    coalesce(exclusion_reason, 'kept') as row_outcome,
    count(*) as row_count
from {{ ref('int_trips_classified') }}
group by source_month, row_outcome
