{{ config(materialized='view') }}

-- Star-schema fact table, one row per trip of silver.trips. A view, not a
-- copy: the Sandbox caps storage at 10 GB, and the rows are already stored,
-- partitioned and clustered in silver.trips.

select
    trip_id,
    pickup_month,
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    pickup_zone_id,
    dropoff_zone_id,
    payment_type_id,
    rate_code_id,
    vendor_id,
    passenger_count,
    trip_distance_mi,
    trip_duration_min,
    avg_speed_mph,
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
from {{ ref('trips') }}
