-- No row lost or duplicated between bronze and silver. For every month built:
-- bronze rows = silver.trips rows + quarantine rows, and both match the
-- accounting in audit_trip_rows. Returns one row per broken month.
with audit as (
    select
        source_month,
        sum(row_count) as audited_rows,
        sum(if(row_outcome = 'kept', row_count, 0)) as kept_rows
    from {{ ref('audit_trip_rows') }}
    group by source_month
),

bronze as (
    select
        cast(_table_suffix as int64) as source_month,
        count(*) as bronze_rows
    from {{ source('bronze', 'yellow_tripdata') }}
    group by source_month
),

silver as (
    select
        pickup_month as source_month,
        count(*) as silver_rows
    from {{ ref('trips') }}
    group by source_month
),

quarantine as (
    select
        source_month,
        count(*) as quarantine_rows
    from {{ ref('trips_quarantine') }}
    group by source_month
)

select
    audit.source_month,
    bronze.bronze_rows,
    audit.audited_rows,
    audit.kept_rows,
    silver.silver_rows,
    quarantine.quarantine_rows
from audit
full outer join silver on audit.source_month = silver.source_month
left join quarantine on coalesce(audit.source_month, silver.source_month) = quarantine.source_month
left join bronze on coalesce(audit.source_month, silver.source_month) = bronze.source_month
where
    bronze.bronze_rows is null
    or audit.audited_rows is null
    or silver.silver_rows is null
    or bronze.bronze_rows != audit.audited_rows
    or audit.kept_rows != silver.silver_rows
    or bronze.bronze_rows != silver.silver_rows + coalesce(quarantine.quarantine_rows, 0)
