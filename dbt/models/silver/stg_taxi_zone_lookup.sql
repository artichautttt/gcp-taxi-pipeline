-- Raw TLC zone lookup from bronze, typed. The cleaned referential is the seed
-- taxi_zone_lookup (output of vba/TaxiZoneCleaner.bas); this view lets a test
-- check that both hold the same zone ids.
select
    safe_cast(locationid as int64) as location_id,
    locationid as location_id_raw,
    borough,
    zone,
    service_zone,
    _source_file as source_file,
    _loaded_at as bronze_loaded_at
from {{ source('bronze', 'taxi_zone_lookup') }}
