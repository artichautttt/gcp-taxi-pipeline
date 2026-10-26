-- Business rule: every trip in the fact ends after it starts, lasts at most
-- 6 hours, and sits in the partition of its own pickup month.
select
    trip_id,
    pickup_month,
    pickup_datetime,
    dropoff_datetime
from {{ ref('fct_trips') }}
where
    dropoff_datetime < pickup_datetime
    or timestamp_diff(dropoff_datetime, pickup_datetime, second) > 6 * 3600
    or cast(format_timestamp('%Y%m', pickup_datetime) as int64) != pickup_month
