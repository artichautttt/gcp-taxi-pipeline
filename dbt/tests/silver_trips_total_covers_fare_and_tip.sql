-- Business rule: the amount charged covers at least the fare and the tip.
-- (A stricter "total = sum of every component" rule does not hold on the TLC
-- files: measured on January 2025, about a third of the rows differ by the
-- 2.50 USD congestion surcharge or the 0.75 USD CBD fee, so it is not tested.)
select
    trip_id,
    total_amount,
    fare_amount,
    tip_amount
from {{ ref('trips') }}
where total_amount < fare_amount + coalesce(tip_amount, 0)
