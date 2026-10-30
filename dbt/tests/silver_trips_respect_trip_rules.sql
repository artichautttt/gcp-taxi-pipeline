-- Every silver trip ends after it starts (strictly), lasts at most 6 hours,
-- sits in the partition of its own pickup month, stays at or under 100 mph,
-- and has no negative fee, tax, tip or toll.
select
    trip_id,
    pickup_month,
    pickup_datetime,
    dropoff_datetime,
    avg_speed_mph
from {{ ref('trips') }}
where
    dropoff_datetime <= pickup_datetime
    or timestamp_diff(dropoff_datetime, pickup_datetime, second) > 6 * 3600
    or cast(format_timestamp('%Y%m', pickup_datetime) as int64) != pickup_month
    or avg_speed_mph > 100
    or least(
        coalesce(extra, 0), coalesce(mta_tax, 0), coalesce(tip_amount, 0),
        coalesce(tolls_amount, 0), coalesce(improvement_surcharge, 0),
        coalesce(congestion_surcharge, 0), coalesce(airport_fee, 0)
    ) < 0
