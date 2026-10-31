-- The cleaned zone referential (seed taxi_zone_lookup, output of
-- vba/TaxiZoneCleaner.bas) holds exactly the zone ids of the raw TLC CSV in
-- bronze: no zone lost or invented by the cleaning.
with bronze as (
    select location_id from {{ ref('stg_taxi_zone_lookup') }}
),

silver as (
    select location_id from {{ ref('taxi_zone_lookup') }}
)

select
    coalesce(b.location_id, s.location_id) as location_id,
    b.location_id is not null as in_bronze,
    s.location_id is not null as in_silver
from bronze as b
full outer join silver as s on b.location_id = s.location_id
where b.location_id is null or s.location_id is null
