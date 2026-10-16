-- Business rule from sql/02_transform_trips_clean.sql: a trip cannot end
-- before it starts, and lasts at most 6 hours.
select tpep_pickup_datetime, tpep_dropoff_datetime
from {{ source('nyc_taxi', 'trips_clean') }}
where tpep_dropoff_datetime < tpep_pickup_datetime
   or timestamp_diff(tpep_dropoff_datetime, tpep_pickup_datetime, hour) > 6
