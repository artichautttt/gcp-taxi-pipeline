{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=monthly_partition('pickup_month'),
    cluster_by=['pickup_zone_id'],
    on_schema_change='fail'
) }}

-- Silver trips: one row per bronze row that passes every rejection rule,
-- typed and renamed, with the three warning flags. Rebuilt one month at a
-- time: a run replaces the pickup_month partition of --vars pipeline_month and
-- leaves every other month untouched (copy job, no DML).

select
    trip_id,
    pickup_month,
    date(pickup_datetime) as pickup_date,
    pickup_datetime,
    dropoff_datetime,
    pickup_zone_id,
    dropoff_zone_id,
    payment_type_id,
    -- rate code is null on Flex Fare rows; the TLC dictionary code 99 means Null/unknown
    coalesce(rate_code_id, 99) as rate_code_id,
    vendor_id,
    passenger_count,
    trip_distance as trip_distance_mi,
    round(duration_s / 60.0, 2) as trip_duration_min,
    safe_divide(trip_distance, duration_s / 3600.0) as avg_speed_mph,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    flag_unknown_zone,
    flag_passenger_count,
    flag_short_trip
from {{ ref('int_trips_classified') }}
where exclusion_reason is null
