{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=monthly_partition('pickup_month'),
    on_schema_change='fail'
) }}

-- Daily indicators, one row per pickup day of the month being built.
select
    pickup_month,
    pickup_date,
    count(*) as trip_count,
    round(sum(total_amount), 2) as revenue_usd,
    round(avg(trip_distance_mi), 2) as avg_distance_mi,
    round(avg(trip_duration_min), 2) as avg_duration_min,
    round(avg(total_amount), 2) as avg_total_amount_usd,
    round(avg(safe_divide(tip_amount, nullif(fare_amount, 0))) * 100, 1) as avg_tip_pct,
    countif(flag_unknown_zone) as trips_unknown_zone
from {{ ref('fct_trips') }}
where pickup_month = {{ pipeline_month() }}
group by pickup_month, pickup_date
