-- Reconciliation: daily_kpis is an aggregate of trips_clean, so the sum of
-- trip_count must equal the number of rows in trips_clean (no lost or
-- duplicated trips in sql/03_analytics_daily_kpis.sql).
with kpis as (
  select sum(trip_count) as n from {{ source('nyc_taxi', 'daily_kpis') }}
),
trips as (
  select count(*) as n from {{ source('nyc_taxi', 'trips_clean') }}
)
select kpis.n as kpis_trip_count, trips.n as trips_clean_rows
from kpis cross join trips
where kpis.n != trips.n
