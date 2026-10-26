-- Taxi zones (265), from the TLC lookup cleaned by vba/TaxiZoneCleaner.bas.
select
    location_id as zone_id,
    borough,
    zone as zone_name,
    service_zone,
    is_airport,
    is_unknown
from {{ ref('taxi_zone_lookup') }}
