-- Business rule: no trip is lost or duplicated between raw and the mart.
-- For every month built, raw rows = sum of audit_trip_rows, and
-- fct_trips rows = the kept count. Returns one row per broken month.
with audit as (
    select
        source_month,
        sum(row_count) as audited_rows,
        sum(if(row_outcome = 'kept', row_count, 0)) as kept_rows
    from {{ ref('audit_trip_rows') }}
    group by source_month
),

raw as (
    select
        cast(_table_suffix as int64) as source_month,
        count(*) as raw_rows
    from {{ source('tlc_raw', 'raw_yellow_tripdata') }}
    group by source_month
),

fct as (
    select
        pickup_month as source_month,
        count(*) as fct_rows
    from {{ ref('fct_trips') }}
    group by source_month
)

select
    coalesce(audit.source_month, fct.source_month) as source_month,
    raw.raw_rows,
    audit.audited_rows,
    audit.kept_rows,
    fct.fct_rows
from audit
full outer join fct on audit.source_month = fct.source_month
left join raw on coalesce(audit.source_month, fct.source_month) = raw.source_month
where
    raw.raw_rows is null
    or audit.audited_rows is null
    or fct.fct_rows is null
    or raw.raw_rows != audit.audited_rows
    or audit.kept_rows != fct.fct_rows
