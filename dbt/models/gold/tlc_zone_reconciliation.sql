-- External, informative control against the TLC published counts (XML,
-- NYC Open Data c5iv-bn4s): pickups per zone and month, for every month that
-- has both a bronze trip file and a TLC publication.
--   bronze_pickups: rows of the TLC trip file picked up in that month
--   kept_pickups:   the same, after the silver rules
--   tlc_pickups:    count published by the TLC (after its own filtering)
-- is_alert: at least tlc_alert_min_trips trips on one side, and a gap above
-- tlc_alert_gap_pct % of the TLC count (or the zone absent from the TLC side).
-- Not blocking: the test on it has severity warn. Threshold: docs/qualite.md.

with tlc as (
    select
        metric_month as month,
        zone_id,
        sum(trip_count) as tlc_pickups
    from {{ ref('stg_tlc_zone_trips') }}
    where industry = 'Yellow Taxi' and pickup_dropoff = 'Pick-up'
    group by month, zone_id
),

bronze as (
    select
        source_month as month,
        pickup_zone_id as zone_id,
        count(*) as bronze_pickups
    from {{ ref('stg_yellow_trips') }}
    where cast(format_timestamp('%Y%m', pickup_datetime) as int64) = source_month
    group by month, zone_id
),

kept as (
    select
        pickup_month as month,
        zone_id,
        pickups as kept_pickups
    from {{ ref('agg_zone_monthly') }}
),

months as (
    select distinct month from tlc
    intersect distinct
    select distinct month from bronze
),

joined as (
    select
        coalesce(b.month, t.month) as month,
        coalesce(b.zone_id, t.zone_id) as zone_id,
        coalesce(t.tlc_pickups, 0) as tlc_pickups,
        coalesce(b.bronze_pickups, 0) as bronze_pickups
    from bronze as b
    full outer join tlc as t on b.month = t.month and b.zone_id = t.zone_id
    where coalesce(b.month, t.month) in (select m.month from months as m)
)

select
    j.month,
    j.zone_id,
    z.zone_name,
    z.borough,
    j.tlc_pickups,
    j.bronze_pickups,
    coalesce(k.kept_pickups, 0) as kept_pickups,
    j.bronze_pickups - j.tlc_pickups as gap_bronze,
    round(safe_divide(j.bronze_pickups - j.tlc_pickups, j.tlc_pickups) * 100, 2) as gap_bronze_pct,
    round(safe_divide(coalesce(k.kept_pickups, 0) - j.tlc_pickups, j.tlc_pickups) * 100, 2) as gap_kept_pct,
    greatest(j.tlc_pickups, j.bronze_pickups) >= {{ var('tlc_alert_min_trips') }}
    and (
        j.tlc_pickups = 0
        or abs(j.bronze_pickups - j.tlc_pickups) > j.tlc_pickups * {{ var('tlc_alert_gap_pct') }} / 100
    ) as is_alert
from joined as j
left join kept as k on j.month = k.month and j.zone_id = k.zone_id
left join {{ ref('dim_zones') }} as z on j.zone_id = z.zone_id
