{{ config(materialized='ephemeral') }}

-- Every bronze row of the month being built, with the first rejection rule it
-- breaks (null when the row is kept) and three warning flags (kept rows only
-- matter). Rules are checked in this order, so each rejected row is counted
-- once, under one reason. 16 rules: the 8 original ones (the first seven
-- below, plus duplicate) and 8 added ones. The added content rules sit after
-- the original content rules, so those counts do not move; the two duplicate
-- rules come last (a duplicate that also breaks another rule is counted under
-- that rule; no exact duplicate existed in January-March 2025).
--   missing_timestamp, pickup_outside_file_month, dropoff_before_pickup,
--   duration_over_6h, invalid_distance (null, <= 0 or > 200 mi),
--   invalid_fare (null, < 0 or > 1000 USD), invalid_total (null, or below
--   fare + tip),
--   unknown_zone (pickup or drop-off zone null or not in the zone referential),
--   unknown_payment_type, unknown_rate_code (not null and not in the referential;
--   null is kept and mapped to 99), unknown_vendor (not 1, 2, 6, 7),
--   zero_duration (drop-off = pickup), implausible_speed (> 100 mph),
--   negative_component (a fee, tax, tip or toll below 0),
--   duplicate (exact copy of another row), duplicate_business_key (same
--   vendor, times, zones, distance and total as another row).
-- Rules and their justification: docs/qualite.md.

with month_rows as (
    select *
    from {{ ref('stg_yellow_trips') }}
    where source_month = {{ pipeline_month() }}
),

fingerprinted as (
    select
        t.*,
        -- same fields and order as before the bronze ingestion columns
        -- existed, so trip_id values are unchanged
        farm_fingerprint(to_json_string(struct(
            t.source_month, t.vendor_id, t.pickup_datetime, t.dropoff_datetime,
            t.passenger_count, t.trip_distance, t.rate_code_id, t.store_and_fwd_flag,
            t.pickup_zone_id, t.dropoff_zone_id, t.payment_type_id, t.fare_amount,
            t.extra, t.mta_tax, t.tip_amount, t.tolls_amount, t.improvement_surcharge,
            t.total_amount, t.congestion_surcharge, t.airport_fee
        ))) as trip_id
    from month_rows as t
),

copies as (
    select
        *,
        row_number() over (partition by trip_id order by pickup_datetime) as copy_rank,
        cast(format_timestamp('%Y%m', pickup_datetime) as int64) as pickup_month,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as duration_s
    from fingerprinted
),

ranked as (
    select
        *,
        -- ordered by (trip_id, copy_rank) so the first exact copy of a row is
        -- also the first of its business key
        -- the key is fingerprinted: BigQuery refuses FLOAT64 columns in a
        -- window PARTITION BY (trip_distance, total_amount)
        row_number() over (
            partition by
                farm_fingerprint(to_json_string(struct(
                    vendor_id, pickup_datetime, dropoff_datetime, pickup_zone_id,
                    dropoff_zone_id, trip_distance, total_amount
                )))
            order by trip_id, copy_rank
        ) as business_key_rank
    from copies
),

zones as (
    select
        location_id,
        is_unknown
    from {{ ref('taxi_zone_lookup') }}
),

payment_types as (
    select payment_type_id from {{ ref('payment_type_lookup') }}
),

rate_codes as (
    select rate_code_id from {{ ref('rate_code_lookup') }}
)

select
    r.*,
    case
        when r.pickup_datetime is null or r.dropoff_datetime is null then 'missing_timestamp'
        when r.pickup_month != r.source_month then 'pickup_outside_file_month'
        when r.dropoff_datetime < r.pickup_datetime then 'dropoff_before_pickup'
        when r.duration_s > 6 * 3600 then 'duration_over_6h'
        when r.trip_distance is null or r.trip_distance <= 0 or r.trip_distance > 200 then 'invalid_distance'
        when r.fare_amount is null or r.fare_amount < 0 or r.fare_amount > 1000 then 'invalid_fare'
        when r.total_amount is null or r.total_amount < r.fare_amount + coalesce(r.tip_amount, 0)
            then 'invalid_total'
        when pu.location_id is null or dz.location_id is null then 'unknown_zone'
        when pt.payment_type_id is null then 'unknown_payment_type'
        when r.rate_code_id is not null and rc.rate_code_id is null then 'unknown_rate_code'
        when r.vendor_id is null or r.vendor_id not in (1, 2, 6, 7) then 'unknown_vendor'
        when r.duration_s = 0 then 'zero_duration'
        when r.trip_distance / (r.duration_s / 3600.0) > 100 then 'implausible_speed'
        when least(
            coalesce(r.extra, 0), coalesce(r.mta_tax, 0), coalesce(r.tip_amount, 0),
            coalesce(r.tolls_amount, 0), coalesce(r.improvement_surcharge, 0),
            coalesce(r.congestion_surcharge, 0), coalesce(r.airport_fee, 0)
        ) < 0 then 'negative_component'
        when r.copy_rank > 1 then 'duplicate'
        when r.business_key_rank > 1 then 'duplicate_business_key'
    end as exclusion_reason,
    -- warnings: the row is kept, the flag is counted in the quality report
    coalesce(pu.is_unknown, false) or coalesce(dz.is_unknown, false) as flag_unknown_zone,
    r.passenger_count is null or r.passenger_count = 0 or r.passenger_count > 6 as flag_passenger_count,
    coalesce(r.duration_s < 60, false) as flag_short_trip
from ranked as r
left join zones as pu on r.pickup_zone_id = pu.location_id
left join zones as dz on r.dropoff_zone_id = dz.location_id
left join payment_types as pt on r.payment_type_id = pt.payment_type_id
left join rate_codes as rc on r.rate_code_id = rc.rate_code_id
