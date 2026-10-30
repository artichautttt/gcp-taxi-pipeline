{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=monthly_partition('pickup_month'),
    on_schema_change='fail'
) }}

-- Pickups and drop-offs per taxi zone for the month being built.
with month_trips as (
    select
        pickup_month,
        pickup_zone_id,
        dropoff_zone_id
    from {{ ref('fct_trips') }}
    where pickup_month = {{ pipeline_month() }}
),

pickups as (
    select
        pickup_month,
        pickup_zone_id as zone_id,
        count(*) as pickups
    from month_trips
    group by pickup_month, zone_id
),

dropoffs as (
    select
        pickup_month,
        dropoff_zone_id as zone_id,
        count(*) as dropoffs
    from month_trips
    group by pickup_month, zone_id
)

select
    coalesce(p.pickup_month, d.pickup_month) as pickup_month,
    coalesce(p.zone_id, d.zone_id) as zone_id,
    coalesce(p.pickups, 0) as pickups,
    coalesce(d.dropoffs, 0) as dropoffs
from pickups as p
full outer join dropoffs as d
    on p.pickup_month = d.pickup_month and p.zone_id = d.zone_id
