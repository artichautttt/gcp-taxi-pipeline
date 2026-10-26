-- One row per calendar day between vars dim_date_start and dim_date_end.
select
    date_day,
    extract(year from date_day) as calendar_year,
    extract(quarter from date_day) as calendar_quarter,
    extract(month from date_day) as calendar_month,
    extract(year from date_day) * 100 + extract(month from date_day) as year_month,
    extract(day from date_day) as day_of_month,
    format_date('%A', date_day) as weekday_name,
    extract(dayofweek from date_day) in (1, 7) as is_weekend
from unnest(generate_date_array(
    date '{{ var("dim_date_start") }}', date '{{ var("dim_date_end") }}'
)) as date_day
