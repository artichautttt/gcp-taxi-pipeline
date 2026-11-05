{# Doc blocks: business terms and column definitions shared by several models.
   Referenced from the schema yml files with "{{ doc('name') }}".
   Rules and thresholds come from int_trips_classified.sql and docs/qualite.md. #}

{% docs rejection_rules %}
**Rejection rules** (16). Every bronze row of `yellow_tripdata_YYYYMM` ends up in
exactly one place: `silver.trips` if it breaks no rule, otherwise
`silver.trips_quarantine` with one reason, the **first** rule it breaks in this
order (`int_trips_classified.sql`):

| # | Reason | Condition |
|---|---|---|
| 1 | `missing_timestamp` | pickup or drop-off timestamp null |
| 2 | `pickup_outside_file_month` | pickup month differs from the file month |
| 3 | `dropoff_before_pickup` | drop-off before pickup |
| 4 | `duration_over_6h` | duration above 6 hours |
| 5 | `invalid_distance` | distance null, <= 0 or > 200 mi |
| 6 | `invalid_fare` | fare null, < 0 or > 1000 USD |
| 7 | `invalid_total` | total null, or below fare + tip |
| 8 | `unknown_zone` | pickup or drop-off zone null or not in the zone referential |
| 9 | `unknown_payment_type` | payment code not in `payment_type_lookup` |
| 10 | `unknown_rate_code` | rate code not null and not in `rate_code_lookup` (null is kept, mapped to 99) |
| 11 | `unknown_vendor` | vendor null or not 1, 2, 6, 7 |
| 12 | `zero_duration` | drop-off equal to pickup |
| 13 | `implausible_speed` | average speed above 100 mph |
| 14 | `negative_component` | a fee, tax, tip, toll or surcharge below 0 |
| 15 | `duplicate` | exact copy of another row (same `trip_id`); the first copy is kept |
| 16 | `duplicate_business_key` | same vendor, timestamps, zones, distance and total as another row; the first is kept |

Justification of each rule: `docs/qualite.md`.
{% enddocs %}

{% docs rejection_reason %}
Rejection rule that sent the row to quarantine: the first of the 16 rejection
rules the row breaks (see the `rejection_rules` doc block). One reason per row.
{% enddocs %}

{% docs exclusion_reason %}
First rejection rule the row breaks, in the order of the 16 rejection rules;
null when the row is kept. Becomes `rejection_reason` in `trips_quarantine`.
{% enddocs %}

{% docs quarantine %}
**Quarantine**: the bronze rows rejected by a rejection rule, kept as received
(bronze columns renamed to snake_case, values unchanged) with the rule, the run
that rejected them and when. Partition `source_month`, replaced month by month:
re-running a month replaces its quarantine instead of adding a second copy.
Silver rows + quarantine rows = bronze rows, for each month (tested).
{% enddocs %}

{% docs warning_flags %}
**Warnings** (3 flags): the row is kept in silver and the flag is counted in
`silver.quality_report` (`outcome_kind = 'warning'`). Used when the trip took
place and was paid but one secondary field is doubtful.
{% enddocs %}

{% docs flag_unknown_zone %}
Warning, row kept. Pickup or drop-off in zone 264 or 265 (TLC "Unknown").
{% enddocs %}

{% docs flag_passenger_count %}
Warning, row kept. passenger_count null, 0 or above 6.
{% enddocs %}

{% docs flag_short_trip %}
Warning, row kept. Trip under 60 seconds.
{% enddocs %}

{% docs tlc_alert %}
**TLC alert** (`is_alert`): true when both hold for a zone and month:
at least `tlc_alert_min_trips` trips (var, 1000) on one side (TLC or bronze),
and a gap bronze - TLC above `tlc_alert_gap_pct` (var, 10) % of the TLC count,
or the zone absent from the TLC side. Informative, never blocking: the test
`tlc_zone_reconciliation_alerts` has severity warn. Thresholds: `docs/qualite.md`.
{% enddocs %}

{% docs row_outcome %}
`kept`, or the rejection rule that sent the rows to quarantine (one of the 16
rejection rules).
{% enddocs %}

{% docs trip_id %}
FARM_FINGERPRINT of the 20 source fields of the bronze row, file month included.
Exact copies share it; the same value is used in silver.trips and trips_quarantine.
{% enddocs %}

{% docs source_month %}
Month of the bronze file (YYYYMM integer), read from the bronze table suffix.
{% enddocs %}

{% docs pickup_month %}
Pickup month YYYYMM, equal to the bronze file month (rule pickup_outside_file_month).
Integer-range partition key.
{% enddocs %}

{% docs run_id %}
Pipeline run that wrote the row: dbt var `run_id` (Airflow run_id), else dbt's
invocation_id.
{% enddocs %}

{% docs ingestion_source %}
Bronze ingestion column: source system (`nyc_tlc_trip_records`,
`nyc_tlc_taxi_zone_lookup` or `nyc_open_data_c5iv-bn4s`).
{% enddocs %}

{% docs ingestion_source_file %}
Name of the file loaded into bronze.
{% enddocs %}

{% docs ingestion_loaded_at %}
Time of the bronze load (UTC), the same on every row of one load.
{% enddocs %}

{% docs ingestion_run_id %}
Pipeline run that loaded the bronze table (Airflow run_id, or `manual__<UTC time>`).
{% enddocs %}

{% docs tlc_vendor_id %}
TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025.
{% enddocs %}

{% docs tlc_pickup_datetime %}
Date and time the meter was engaged (pickup).
{% enddocs %}

{% docs tlc_dropoff_datetime %}
Date and time the meter was disengaged (drop-off).
{% enddocs %}

{% docs tlc_passenger_count %}
Number of passengers, entered by the driver; often null.
{% enddocs %}

{% docs tlc_trip_distance %}
Trip distance in miles reported by the taximeter.
{% enddocs %}

{% docs tlc_rate_code_id %}
TLC rate code (see `rate_code_lookup`); null on Flex Fare rows.
{% enddocs %}

{% docs tlc_store_and_fwd_flag %}
`Y` when the record was stored in the vehicle then forwarded, `N` otherwise.
{% enddocs %}

{% docs tlc_pickup_zone %}
TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`).
{% enddocs %}

{% docs tlc_dropoff_zone %}
TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`).
{% enddocs %}

{% docs tlc_payment_type %}
Payment type code, 0 to 6 (see `payment_type_lookup`).
{% enddocs %}

{% docs tlc_fare_amount %}
Time-and-distance fare computed by the meter, USD.
{% enddocs %}

{% docs tlc_extra %}
Miscellaneous extras and surcharges, USD.
{% enddocs %}

{% docs tlc_mta_tax %}
MTA tax, USD.
{% enddocs %}

{% docs tlc_tip_amount %}
Tip, USD. Card tips only: cash tips are not recorded.
{% enddocs %}

{% docs tlc_tolls_amount %}
Tolls paid during the trip, USD.
{% enddocs %}

{% docs tlc_improvement_surcharge %}
Improvement surcharge, USD.
{% enddocs %}

{% docs tlc_total_amount %}
Total amount charged to the passenger, USD (cash tips not included).
{% enddocs %}

{% docs tlc_congestion_surcharge %}
New York State congestion surcharge, USD.
{% enddocs %}

{% docs tlc_airport_fee %}
Airport fee (pickups at LaGuardia and JFK), USD.
{% enddocs %}
