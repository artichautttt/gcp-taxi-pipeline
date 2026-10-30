{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=monthly_partition('source_month'),
    on_schema_change='fail'
) }}

-- Quarantine: every bronze row of the month that breaks a rejection rule, as
-- received (bronze columns), with the rule, the run that rejected it and when.
-- Replaced month by month like silver.trips, so re-running a month replaces
-- its quarantine instead of stacking a second copy.

select
    source_month,
    exclusion_reason as rejection_reason,
    '{{ current_run_id() }}' as run_id,
    current_timestamp() as quarantined_at,
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pickup_zone_id,
    dropoff_zone_id,
    payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    source_file,
    bronze_loaded_at,
    ingestion_run_id
from {{ ref('int_trips_classified') }}
where exclusion_reason is not null
