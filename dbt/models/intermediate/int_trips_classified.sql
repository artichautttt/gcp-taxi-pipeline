-- Every raw row of the month being built, with the first exclusion rule it
-- breaks (null when the row is kept). Rules are checked in this order, so each
-- excluded row is counted once, under one reason:
--   missing_timestamp, pickup_outside_file_month, dropoff_before_pickup,
--   duration_over_6h, invalid_distance (null, <= 0 or > 200 mi),
--   invalid_fare (null, < 0 or > 1000 USD), invalid_total (null, or below
--   fare + tip), duplicate (exact copy of a kept row).
-- pickup_outside_file_month keeps fct_trips partitions disjoint: the January
-- file also holds a few December trips, which must not overwrite December.

with month_rows as (
    select *
    from {{ ref('stg_yellow_trips') }}
    where source_month = {{ pipeline_month() }}
),

fingerprinted as (
    select
        t.*,
        farm_fingerprint(to_json_string(t)) as trip_id
    from month_rows as t
),

ranked as (
    select
        *,
        row_number() over (partition by trip_id order by pickup_datetime) as copy_rank,
        cast(format_timestamp('%Y%m', pickup_datetime) as int64) as pickup_month,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as duration_s
    from fingerprinted
)

select
    *,
    case
        when pickup_datetime is null or dropoff_datetime is null then 'missing_timestamp'
        when pickup_month != source_month then 'pickup_outside_file_month'
        when dropoff_datetime < pickup_datetime then 'dropoff_before_pickup'
        when duration_s > 6 * 3600 then 'duration_over_6h'
        when trip_distance is null or trip_distance <= 0 or trip_distance > 200 then 'invalid_distance'
        when fare_amount is null or fare_amount < 0 or fare_amount > 1000 then 'invalid_fare'
        when total_amount is null or total_amount < fare_amount + coalesce(tip_amount, 0)
            then 'invalid_total'
        when copy_rank > 1 then 'duplicate'
    end as exclusion_reason
from ranked
