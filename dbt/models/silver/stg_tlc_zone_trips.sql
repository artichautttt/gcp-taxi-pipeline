-- TLC published counts per zone and month (XML, NYC Open Data c5iv-bn4s), typed.
select
    cast(_table_suffix as int64) as source_month,
    cast(format_date('%Y%m', safe_cast(substr(metric_month, 1, 10) as date)) as int64) as metric_month,
    industry,
    pickup_dropoff,
    safe_cast(locationid as int64) as zone_id,
    borough,
    zone as zone_name,
    safe_cast(trip_count as int64) as trip_count,
    _source_file as source_file,
    _loaded_at as bronze_loaded_at
from {{ source('bronze', 'tlc_zone_trips') }}
