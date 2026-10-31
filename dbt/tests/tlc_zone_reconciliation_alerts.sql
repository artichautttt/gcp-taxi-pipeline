{{ config(severity='warn') }}

-- Informative, never blocking: zones whose bronze pickups differ from the
-- TLC published count beyond the threshold (see tlc_zone_reconciliation).
-- dbt reports them as a warning; the quality report counts them.
select
    month,
    zone_id,
    zone_name,
    tlc_pickups,
    bronze_pickups,
    gap_bronze_pct
from {{ ref('tlc_zone_reconciliation') }}
where is_alert
