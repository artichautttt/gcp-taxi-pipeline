-- One row per bronze trip record, all loaded months, renamed to snake_case.
-- No filtering here: rows are classified (kept / rejected) in int_trips_classified.
select
    cast(_table_suffix as int64) as source_month,
    vendorid as vendor_id,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid as rate_code_id,
    store_and_fwd_flag,
    pulocationid as pickup_zone_id,
    dolocationid as dropoff_zone_id,
    payment_type as payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    _source_file as source_file,
    _loaded_at as bronze_loaded_at,
    _run_id as ingestion_run_id
from {{ source('bronze', 'yellow_tripdata') }}
