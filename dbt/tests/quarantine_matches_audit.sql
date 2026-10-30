-- The quarantine holds exactly the rejected rows counted in audit_trip_rows,
-- reason by reason, and one run per month (a re-run replaced the month, it
-- did not stack a second copy). Returns one row per broken (month, reason).
with audit as (
    select
        source_month,
        row_outcome as rejection_reason,
        row_count
    from {{ ref('audit_trip_rows') }}
    where row_outcome != 'kept'
),

quarantine as (
    select
        source_month,
        rejection_reason,
        count(*) as quarantine_rows,
        count(distinct run_id) as runs
    from {{ ref('trips_quarantine') }}
    group by source_month, rejection_reason
)

select
    coalesce(a.source_month, q.source_month) as source_month,
    coalesce(a.rejection_reason, q.rejection_reason) as rejection_reason,
    a.row_count as audited_rows,
    q.quarantine_rows,
    q.runs
from audit as a
full outer join quarantine as q
    on a.source_month = q.source_month and a.rejection_reason = q.rejection_reason
where
    a.row_count is null
    or q.quarantine_rows is null
    or a.row_count != q.quarantine_rows
    or q.runs != 1
