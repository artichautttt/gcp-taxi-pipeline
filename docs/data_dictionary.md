# Dictionnaire de données (généré)

Fichier généré par `python scripts/dbt_catalog.py dictionary` à partir du
manifest dbt (`dbt parse`) et du catalog (`dbt docs generate`, colonnes et types réels de BigQuery).
Ne pas modifier à la main : les descriptions vivent dans les `schema.yml`
et les doc blocks de `dbt/models/_docs.md`.

- Objets : 24 (sources, seeds, modèles), 24 décrits
- Colonnes décrites : 293 / 293 (100.0 %)
- Hors catalog dbt (tables wildcard, modèle éphémère) : colonnes lues dans les yml, sans type : `bronze.tlc_zone_trips`, `bronze.yellow_tripdata`, `int_trips_classified`

## Chaîne historique (`nyc_taxi`)

### `nyc_taxi.daily_kpis`

source `nyc_taxi.daily_kpis`, `models/sources.yml`

One row per pickup date (sql/03_analytics_daily_kpis.sql).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `pickup_date` | DATE | Pickup day, one row per day. | not_null, unique |
| `pickup_weekday` | STRING | English weekday name of the pickup day. |  |
| `trip_count` | INT64 | Trips of trips_clean picked up that day. | not_null, positive |
| `avg_distance_mi` | FLOAT64 | Average trip distance in miles, rounded to 2 decimals. |  |
| `avg_duration_min` | FLOAT64 | Average trip duration in minutes, rounded to 2 decimals. |  |
| `avg_total_amount_usd` | FLOAT64 | Average total_amount in USD, rounded to 2 decimals. |  |
| `revenue_usd` | FLOAT64 | Sum of total_amount in USD, rounded to 2 decimals. | not_null, positive |
| `avg_tip_pct` | FLOAT64 | Average of tip_amount / fare_amount per trip, in %, rounded to 1 decimal (trips with a zero fare ignored). |  |

### `nyc_taxi.trips_clean`

source `nyc_taxi.trips_clean`, `models/sources.yml`

Filtered trips with derived columns (sql/02_transform_trips_clean.sql).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `VendorID` | INT64 | TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025. |  |
| `tpep_pickup_datetime` | TIMESTAMP | Date and time the meter was engaged (pickup). | not_null |
| `tpep_dropoff_datetime` | TIMESTAMP | Date and time the meter was disengaged (drop-off). | not_null |
| `trip_duration_min` | FLOAT64 | Drop-off minus pickup, in minutes (not rounded). |  |
| `passenger_count` | INT64 | Number of passengers, entered by the driver; often null. |  |
| `trip_distance` | FLOAT64 | Trip distance in miles reported by the taximeter. | positive, value_between |
| `avg_speed_mph` | FLOAT64 | trip_distance divided by the duration in hours (null when the duration is 0). |  |
| `PULocationID` | INT64 | TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`). | not_null |
| `DOLocationID` | INT64 | TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`). | not_null |
| `payment_type` | INT64 | Payment type code, 0 to 6 (see `payment_type_lookup`). |  |
| `fare_amount` | FLOAT64 | Time-and-distance fare computed by the meter, USD. | value_between |
| `tip_amount` | FLOAT64 | Tip, USD. Card tips only: cash tips are not recorded. |  |
| `tolls_amount` | FLOAT64 | Tolls paid during the trip, USD. |  |
| `total_amount` | FLOAT64 | Total amount charged to the passenger, USD (cash tips not included). |  |
| `pickup_date` | DATE | Pickup day (date of tpep_pickup_datetime). | not_null, value_between |
| `pickup_hour` | INT64 | Pickup hour, 0 to 23. |  |
| `pickup_weekday` | STRING | English weekday name of the pickup day. | accepted_values |

### `nyc_taxi.yellow_tripdata_raw`

source `nyc_taxi.yellow_tripdata_raw`, `models/sources.yml`

Raw NYC TLC Yellow Taxi trips, loaded as-is from Parquet.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `VendorID` | INT64 | TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025. |  |
| `tpep_pickup_datetime` | TIMESTAMP | Date and time the meter was engaged (pickup). | not_null |
| `tpep_dropoff_datetime` | TIMESTAMP | Date and time the meter was disengaged (drop-off). | not_null |
| `passenger_count` | INT64 | Number of passengers, entered by the driver; often null. |  |
| `trip_distance` | FLOAT64 | Trip distance in miles reported by the taximeter. |  |
| `RatecodeID` | INT64 | TLC rate code (see `rate_code_lookup`); null on Flex Fare rows. |  |
| `store_and_fwd_flag` | STRING | `Y` when the record was stored in the vehicle then forwarded, `N` otherwise. |  |
| `PULocationID` | INT64 | TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`). |  |
| `DOLocationID` | INT64 | TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`). |  |
| `payment_type` | INT64 | Payment type code, 0 to 6 (see `payment_type_lookup`). |  |
| `fare_amount` | FLOAT64 | Time-and-distance fare computed by the meter, USD. |  |
| `extra` | FLOAT64 | Miscellaneous extras and surcharges, USD. |  |
| `mta_tax` | FLOAT64 | MTA tax, USD. |  |
| `tip_amount` | FLOAT64 | Tip, USD. Card tips only: cash tips are not recorded. |  |
| `tolls_amount` | FLOAT64 | Tolls paid during the trip, USD. |  |
| `improvement_surcharge` | FLOAT64 | Improvement surcharge, USD. |  |
| `total_amount` | FLOAT64 | Total amount charged to the passenger, USD (cash tips not included). |  |
| `congestion_surcharge` | FLOAT64 | New York State congestion surcharge, USD. |  |
| `airport_fee` | FLOAT64 | Airport fee (pickups at LaGuardia and JFK), USD. |  |

## Bronze (`nyc_taxi_bronze`)

### `nyc_taxi_bronze.taxi_zone_lookup`

source `bronze.taxi_zone_lookup`, `models/silver/_bronze__sources.yml`

Official TLC taxi_zone_lookup.csv, every column as a string.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `LocationID` | STRING | TLC zone id as text (1 to 265). |  |
| `Borough` | STRING | Borough of the zone, raw value (Unknown and N/A included). |  |
| `Zone` | STRING | Zone name, raw value. |  |
| `service_zone` | STRING | TLC service zone, raw value (e.g. Yellow Zone, Boro Zone, Airports, EWR). |  |
| `_source` | STRING | Bronze ingestion column: source system (`nyc_tlc_trip_records`, `nyc_tlc_taxi_zone_lookup` or `nyc_open_data_c5iv-bn4s`). |  |
| `_source_file` | STRING | Name of the file loaded into bronze. |  |
| `_loaded_at` | TIMESTAMP | Time of the bronze load (UTC), the same on every row of one load. |  |
| `_run_id` | STRING | Pipeline run that loaded the bronze table (Airflow run_id, or `manual__<UTC time>`). |  |

### `nyc_taxi_bronze.tlc_zone_trips_*`

source `bronze.tlc_zone_trips`, `models/silver/_bronze__sources.yml`

NYC Open Data c5iv-bn4s (XML), Yellow Taxi pickups and drop-offs per zone and month published by the TLC. Every field as a string.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `metric_month` |  | First day of the month the count refers to, as text (e.g. 2025-01-01). |  |
| `industry` |  | TLC industry; only Yellow Taxi is downloaded. |  |
| `pickup_dropoff` |  | Pick-up or Drop-off. |  |
| `locationid` |  | TLC zone id, as text. |  |
| `borough` |  | Borough of the zone, as published. |  |
| `zone` |  | Zone name, as published. |  |
| `trip_count` |  | Trips published by the TLC for the zone, month and direction, as text. |  |
| `_source` |  | Bronze ingestion column: source system (`nyc_tlc_trip_records`, `nyc_tlc_taxi_zone_lookup` or `nyc_open_data_c5iv-bn4s`). |  |
| `_source_file` |  | Name of the file loaded into bronze. |  |
| `_loaded_at` |  | Time of the bronze load (UTC), the same on every row of one load. |  |
| `_run_id` |  | Pipeline run that loaded the bronze table (Airflow run_id, or `manual__<UTC time>`). |  |

### `nyc_taxi_bronze.yellow_tripdata_*`

source `bronze.yellow_tripdata`, `models/silver/_bronze__sources.yml`

NYC TLC Yellow Taxi trip records (Parquet), all loaded months.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `VendorID` |  | TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025. |  |
| `tpep_pickup_datetime` |  | Date and time the meter was engaged (pickup). |  |
| `tpep_dropoff_datetime` |  | Date and time the meter was disengaged (drop-off). |  |
| `passenger_count` |  | Number of passengers, entered by the driver; often null. |  |
| `trip_distance` |  | Trip distance in miles reported by the taximeter. |  |
| `RatecodeID` |  | TLC rate code (see `rate_code_lookup`); null on Flex Fare rows. |  |
| `store_and_fwd_flag` |  | `Y` when the record was stored in the vehicle then forwarded, `N` otherwise. |  |
| `PULocationID` |  | TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`). |  |
| `DOLocationID` |  | TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`). |  |
| `payment_type` |  | Payment type code, 0 to 6 (see `payment_type_lookup`). |  |
| `fare_amount` |  | Time-and-distance fare computed by the meter, USD. |  |
| `extra` |  | Miscellaneous extras and surcharges, USD. |  |
| `mta_tax` |  | MTA tax, USD. |  |
| `tip_amount` |  | Tip, USD. Card tips only: cash tips are not recorded. |  |
| `tolls_amount` |  | Tolls paid during the trip, USD. |  |
| `improvement_surcharge` |  | Improvement surcharge, USD. |  |
| `total_amount` |  | Total amount charged to the passenger, USD (cash tips not included). |  |
| `congestion_surcharge` |  | New York State congestion surcharge, USD. |  |
| `airport_fee` |  | Airport fee (pickups at LaGuardia and JFK), USD. |  |
| `_source` |  | Bronze ingestion column: source system (`nyc_tlc_trip_records`, `nyc_tlc_taxi_zone_lookup` or `nyc_open_data_c5iv-bn4s`). |  |
| `_source_file` |  | Name of the file loaded into bronze. |  |
| `_loaded_at` |  | Time of the bronze load (UTC), the same on every row of one load. |  |
| `_run_id` |  | Pipeline run that loaded the bronze table (Airflow run_id, or `manual__<UTC time>`). |  |

## Silver (`nyc_taxi_silver`)

### `nyc_taxi_silver.payment_type_lookup`

seed (seed) `payment_type_lookup`, `seeds/payment_type_lookup.csv`

Payment type codes, NYC TLC Yellow Taxi data dictionary.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `payment_type_id` | INT64 | TLC payment type code, 0 to 6. |  |
| `payment_type_name` | STRING | Label of the code. |  |

### `nyc_taxi_silver.rate_code_lookup`

seed (seed) `rate_code_lookup`, `seeds/rate_code_lookup.csv`

Rate code codes, NYC TLC Yellow Taxi data dictionary (99 = Null/unknown).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `rate_code_id` | INT64 | TLC rate code, 1 to 6, or 99 (Null/unknown). |  |
| `rate_code_name` | STRING | Label of the code. |  |

### `nyc_taxi_silver.taxi_zone_lookup`

seed (seed) `taxi_zone_lookup`, `seeds/taxi_zone_lookup.csv`

Output of vba/TaxiZoneCleaner.bas on the official TLC taxi_zone_lookup.csv (https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv), 265 zones.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `location_id` | INT64 | TLC zone id (LocationID), 1 to 265. |  |
| `borough` | STRING | Borough of the zone (Unknown for zones 264 and 265). |  |
| `zone` | STRING | Zone name. |  |
| `service_zone` | STRING | TLC service zone (Boro Zone, Yellow Zone, Airports, EWR, Unknown). |  |
| `is_airport` | BOOL | True for the airport zones (1 EWR, 132 JFK, 138 LaGuardia). |  |
| `is_unknown` | BOOL | True for zones 264 and 265 (TLC Unknown / outside of NYC). |  |

### `nyc_taxi_silver.audit_trip_rows`

model (incremental) `audit_trip_rows`, `models/silver/audit_trip_rows.sql`

Row accounting per bronze file month: rows kept in silver.trips and rows quarantined, per rule (one reason per row, first rule broken).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `source_month` | INT64 | Month of the bronze file (YYYYMM integer), read from the bronze table suffix. Partition key. | not_null |
| `row_outcome` | STRING | `kept`, or the rejection rule that sent the rows to quarantine (one of the 16 rejection rules). | accepted_values, not_null |
| `row_count` | INT64 | Rows of the month with this outcome; per month, the sum equals the bronze rows (tested). | not_null, positive |
| `run_id` | STRING | Pipeline run that wrote the row: dbt var `run_id` (Airflow run_id), else dbt's invocation_id. | not_null |

### `nyc_taxi_silver.int_trips_classified`

model (ephemeral) `int_trips_classified`, `models/silver/int_trips_classified.sql`

Ephemeral (no table): every bronze row of the month being built (var pipeline_month), with its fingerprint, the first rejection rule it breaks and the three warning flags. Feeds trips, trips_quarantine and audit_trip_rows. **Rejection rules** (16). Every bronze row of `yellow_tripdata_YYYYMM` ends up in
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

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `source_month` |  | Month of the bronze file (YYYYMM integer), read from the bronze table suffix. |  |
| `vendor_id` |  | TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025. |  |
| `pickup_datetime` |  | Date and time the meter was engaged (pickup). |  |
| `dropoff_datetime` |  | Date and time the meter was disengaged (drop-off). |  |
| `passenger_count` |  | Number of passengers, entered by the driver; often null. |  |
| `trip_distance` |  | Trip distance in miles reported by the taximeter. |  |
| `rate_code_id` |  | TLC rate code (see `rate_code_lookup`); null on Flex Fare rows. |  |
| `store_and_fwd_flag` |  | `Y` when the record was stored in the vehicle then forwarded, `N` otherwise. |  |
| `pickup_zone_id` |  | TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`). |  |
| `dropoff_zone_id` |  | TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`). |  |
| `payment_type_id` |  | Payment type code, 0 to 6 (see `payment_type_lookup`). |  |
| `fare_amount` |  | Time-and-distance fare computed by the meter, USD. |  |
| `extra` |  | Miscellaneous extras and surcharges, USD. |  |
| `mta_tax` |  | MTA tax, USD. |  |
| `tip_amount` |  | Tip, USD. Card tips only: cash tips are not recorded. |  |
| `tolls_amount` |  | Tolls paid during the trip, USD. |  |
| `improvement_surcharge` |  | Improvement surcharge, USD. |  |
| `total_amount` |  | Total amount charged to the passenger, USD (cash tips not included). |  |
| `congestion_surcharge` |  | New York State congestion surcharge, USD. |  |
| `airport_fee` |  | Airport fee (pickups at LaGuardia and JFK), USD. |  |
| `source_file` |  | Name of the file loaded into bronze. |  |
| `bronze_loaded_at` |  | Time of the bronze load (UTC), the same on every row of one load. |  |
| `ingestion_run_id` |  | Pipeline run that loaded the bronze table (Airflow run_id, or `manual__<UTC time>`). |  |
| `trip_id` |  | FARM_FINGERPRINT of the 20 source fields of the bronze row, file month included. Exact copies share it; the same value is used in silver.trips and trips_quarantine. |  |
| `copy_rank` |  | Rank of the row among its exact copies (same trip_id); above 1 means rule duplicate. |  |
| `pickup_month` |  | Month of pickup_datetime (YYYYMM), compared with source_month. |  |
| `duration_s` |  | Drop-off minus pickup, in seconds. |  |
| `business_key_rank` |  | Rank of the row among the rows with the same vendor, timestamps, zones, distance and total; above 1 means rule duplicate_business_key. |  |
| `exclusion_reason` |  | First rejection rule the row breaks, in the order of the 16 rejection rules; null when the row is kept. Becomes `rejection_reason` in `trips_quarantine`. |  |
| `flag_unknown_zone` |  | Warning, row kept. Pickup or drop-off in zone 264 or 265 (TLC "Unknown"). |  |
| `flag_passenger_count` |  | Warning, row kept. passenger_count null, 0 or above 6. |  |
| `flag_short_trip` |  | Warning, row kept. Trip under 60 seconds. |  |

### `nyc_taxi_silver.stg_taxi_zone_lookup`

model (view) `stg_taxi_zone_lookup`, `models/silver/stg_taxi_zone_lookup.sql`

Raw TLC zone lookup (bronze CSV), LocationID cast to an integer.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `location_id` | INT64 | Null when LocationID is not an integer (the test fails then). | not_null, unique |
| `location_id_raw` | STRING | LocationID as received (text). |  |
| `borough` | STRING | Borough, raw value. |  |
| `zone` | STRING | Zone name, raw value. |  |
| `service_zone` | STRING | TLC service zone, raw value. |  |
| `source_file` | STRING | Name of the file loaded into bronze. |  |
| `bronze_loaded_at` | TIMESTAMP | Time of the bronze load (UTC), the same on every row of one load. |  |

### `nyc_taxi_silver.stg_tlc_zone_trips`

model (view) `stg_tlc_zone_trips`, `models/silver/stg_tlc_zone_trips.sql`

TLC published pickups / drop-offs per zone and month (bronze XML), typed.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `source_month` | INT64 | Month of the bronze table (YYYYMM), from the table suffix. |  |
| `metric_month` | INT64 | Month the TLC count refers to (YYYYMM), parsed from the published date. | not_null |
| `industry` | STRING | TLC industry (Yellow Taxi). |  |
| `pickup_dropoff` | STRING | Pick-up or Drop-off. |  |
| `zone_id` | INT64 | TLC zone id (null when the published value is not an integer). | not_null |
| `borough` | STRING | Borough, as published. |  |
| `zone_name` | STRING | Zone name, as published. |  |
| `trip_count` | INT64 | Trips published by the TLC for the zone, month and direction. | not_null |
| `source_file` | STRING | Name of the file loaded into bronze. |  |
| `bronze_loaded_at` | TIMESTAMP | Time of the bronze load (UTC), the same on every row of one load. |  |

### `nyc_taxi_silver.stg_yellow_trips`

model (view) `stg_yellow_trips`, `models/silver/stg_yellow_trips.sql`

Bronze trip records of every loaded month, renamed to snake_case, not filtered. View: costs nothing to store, rebuilt on each read.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `source_month` | INT64 | Month of the bronze file (YYYYMM), from the table suffix. | not_null |
| `vendor_id` | INT64 | TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025. |  |
| `pickup_datetime` | TIMESTAMP | Date and time the meter was engaged (pickup). |  |
| `dropoff_datetime` | TIMESTAMP | Date and time the meter was disengaged (drop-off). |  |
| `passenger_count` | INT64 | Number of passengers, entered by the driver; often null. |  |
| `trip_distance` | FLOAT64 | Trip distance in miles reported by the taximeter. |  |
| `rate_code_id` | INT64 | TLC rate code (see `rate_code_lookup`); null on Flex Fare rows. |  |
| `store_and_fwd_flag` | STRING | `Y` when the record was stored in the vehicle then forwarded, `N` otherwise. |  |
| `pickup_zone_id` | INT64 | TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`). |  |
| `dropoff_zone_id` | INT64 | TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`). |  |
| `payment_type_id` | INT64 | Payment type code, 0 to 6 (see `payment_type_lookup`). |  |
| `fare_amount` | FLOAT64 | Time-and-distance fare computed by the meter, USD. |  |
| `extra` | FLOAT64 | Miscellaneous extras and surcharges, USD. |  |
| `mta_tax` | FLOAT64 | MTA tax, USD. |  |
| `tip_amount` | FLOAT64 | Tip, USD. Card tips only: cash tips are not recorded. |  |
| `tolls_amount` | FLOAT64 | Tolls paid during the trip, USD. |  |
| `improvement_surcharge` | FLOAT64 | Improvement surcharge, USD. |  |
| `total_amount` | FLOAT64 | Total amount charged to the passenger, USD (cash tips not included). |  |
| `congestion_surcharge` | FLOAT64 | New York State congestion surcharge, USD. |  |
| `airport_fee` | FLOAT64 | Airport fee (pickups at LaGuardia and JFK), USD. |  |
| `source_file` | STRING | Name of the file loaded into bronze. |  |
| `bronze_loaded_at` | TIMESTAMP | Time of the bronze load (UTC), the same on every row of one load. |  |
| `ingestion_run_id` | STRING | Pipeline run that loaded the bronze table (Airflow run_id, or `manual__<UTC time>`). |  |

### `nyc_taxi_silver.trips`

model (incremental) `trips`, `models/silver/trips.sql`

Silver trips: bronze rows that pass every rejection rule, typed, with three warning flags. Incremental by month (insert_overwrite on the integer partition pickup_month, copy jobs, no DML).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `trip_id` | INT64 | FARM_FINGERPRINT of the source fields of the row (exact copies share it). | not_null, unique |
| `pickup_month` | INT64 | Pickup month YYYYMM, equal to the bronze file month. Partition key. | not_null |
| `pickup_date` | DATE | Pickup day (date of pickup_datetime). |  |
| `pickup_datetime` | TIMESTAMP | Date and time the meter was engaged (pickup). |  |
| `dropoff_datetime` | TIMESTAMP | Date and time the meter was disengaged (drop-off). |  |
| `pickup_zone_id` | INT64 | TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`). | not_null |
| `dropoff_zone_id` | INT64 | TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`). | not_null |
| `payment_type_id` | INT64 | Payment type code, 0 to 6 (see `payment_type_lookup`). | not_null |
| `rate_code_id` | INT64 | TLC rate code; null in the source (Flex Fare rows) is mapped to 99 (Null/unknown). | not_null |
| `vendor_id` | INT64 | TLC vendor code; any other code is rejected (unknown_vendor). | accepted_values |
| `passenger_count` | INT64 | Number of passengers, entered by the driver; often null. |  |
| `trip_distance_mi` | FLOAT64 | Trip distance in miles reported by the taximeter. | value_between |
| `trip_duration_min` | FLOAT64 | Drop-off minus pickup, in minutes, rounded to 2 decimals. | value_between |
| `avg_speed_mph` | FLOAT64 | trip_distance_mi divided by the duration in hours. | value_between |
| `fare_amount` | FLOAT64 | Time-and-distance fare computed by the meter, USD. | value_between |
| `extra` | FLOAT64 | Miscellaneous extras and surcharges, USD. |  |
| `mta_tax` | FLOAT64 | MTA tax, USD. |  |
| `tip_amount` | FLOAT64 | Tip, USD. Card tips only: cash tips are not recorded. |  |
| `tolls_amount` | FLOAT64 | Tolls paid during the trip, USD. |  |
| `improvement_surcharge` | FLOAT64 | Improvement surcharge, USD. |  |
| `congestion_surcharge` | FLOAT64 | New York State congestion surcharge, USD. |  |
| `airport_fee` | FLOAT64 | Airport fee (pickups at LaGuardia and JFK), USD. |  |
| `total_amount` | FLOAT64 | Total amount charged to the passenger, USD (cash tips not included). |  |
| `flag_unknown_zone` | BOOL | Warning, row kept. Pickup or drop-off in zone 264 or 265 (TLC "Unknown"). |  |
| `flag_passenger_count` | BOOL | Warning, row kept. passenger_count null, 0 or above 6. |  |
| `flag_short_trip` | BOOL | Warning, row kept. Trip under 60 seconds. |  |

### `nyc_taxi_silver.trips_quarantine`

model (incremental) `trips_quarantine`, `models/silver/trips_quarantine.sql`

Rejected rows of each month, as received in bronze, with the rule (rejection_reason), the run that rejected them (run_id) and when. Replaced month by month: a re-run does not stack a second copy. **Quarantine**: the bronze rows rejected by a rejection rule, kept as received
(bronze columns renamed to snake_case, values unchanged) with the rule, the run
that rejected them and when. Partition `source_month`, replaced month by month:
re-running a month replaces its quarantine instead of adding a second copy.
Silver rows + quarantine rows = bronze rows, for each month (tested).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `source_month` | INT64 | Month of the bronze file (YYYYMM integer), read from the bronze table suffix. Partition key. | not_null |
| `rejection_reason` | STRING | Rejection rule that sent the row to quarantine: the first of the 16 rejection rules the row breaks (see the `rejection_rules` doc block). One reason per row. | accepted_values, not_null |
| `run_id` | STRING | Pipeline run that wrote the row: dbt var `run_id` (Airflow run_id), else dbt's invocation_id. | not_null |
| `quarantined_at` | TIMESTAMP | Time the row was written to quarantine (UTC). |  |
| `trip_id` | INT64 | FARM_FINGERPRINT of the 20 source fields of the bronze row, file month included. Exact copies share it; the same value is used in silver.trips and trips_quarantine. | not_null |
| `vendor_id` | INT64 | TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025. |  |
| `pickup_datetime` | TIMESTAMP | Date and time the meter was engaged (pickup). |  |
| `dropoff_datetime` | TIMESTAMP | Date and time the meter was disengaged (drop-off). |  |
| `passenger_count` | INT64 | Number of passengers, entered by the driver; often null. |  |
| `trip_distance` | FLOAT64 | Trip distance in miles reported by the taximeter. |  |
| `rate_code_id` | INT64 | TLC rate code (see `rate_code_lookup`); null on Flex Fare rows. |  |
| `store_and_fwd_flag` | STRING | `Y` when the record was stored in the vehicle then forwarded, `N` otherwise. |  |
| `pickup_zone_id` | INT64 | TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`). |  |
| `dropoff_zone_id` | INT64 | TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`). |  |
| `payment_type_id` | INT64 | Payment type code, 0 to 6 (see `payment_type_lookup`). |  |
| `fare_amount` | FLOAT64 | Time-and-distance fare computed by the meter, USD. |  |
| `extra` | FLOAT64 | Miscellaneous extras and surcharges, USD. |  |
| `mta_tax` | FLOAT64 | MTA tax, USD. |  |
| `tip_amount` | FLOAT64 | Tip, USD. Card tips only: cash tips are not recorded. |  |
| `tolls_amount` | FLOAT64 | Tolls paid during the trip, USD. |  |
| `improvement_surcharge` | FLOAT64 | Improvement surcharge, USD. |  |
| `total_amount` | FLOAT64 | Total amount charged to the passenger, USD (cash tips not included). |  |
| `congestion_surcharge` | FLOAT64 | New York State congestion surcharge, USD. |  |
| `airport_fee` | FLOAT64 | Airport fee (pickups at LaGuardia and JFK), USD. |  |
| `source_file` | STRING | Name of the file loaded into bronze. |  |
| `bronze_loaded_at` | TIMESTAMP | Time of the bronze load (UTC), the same on every row of one load. |  |
| `ingestion_run_id` | STRING | Pipeline run that loaded the bronze table (Airflow run_id, or `manual__<UTC time>`). |  |

## Gold (`nyc_taxi_gold`)

### `nyc_taxi_gold.agg_daily_kpis`

model (incremental) `agg_daily_kpis`, `models/gold/agg_daily_kpis.sql`

Daily indicators per pickup day (trips, revenue, averages).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `pickup_month` | INT64 | Pickup month YYYYMM, equal to the bronze file month (rule pickup_outside_file_month). Integer-range partition key. |  |
| `pickup_date` | DATE | Pickup day, one row per day. | not_null, unique |
| `trip_count` | INT64 | Trips of fct_trips picked up that day. | not_null, positive |
| `revenue_usd` | FLOAT64 | Sum of total_amount in USD, rounded to 2 decimals. |  |
| `avg_distance_mi` | FLOAT64 | Average trip_distance_mi, rounded to 2 decimals. |  |
| `avg_duration_min` | FLOAT64 | Average trip_duration_min, rounded to 2 decimals. |  |
| `avg_total_amount_usd` | FLOAT64 | Average total_amount in USD, rounded to 2 decimals. |  |
| `avg_tip_pct` | FLOAT64 | Average of tip_amount / fare_amount per trip, in %, rounded to 1 decimal (trips with a zero fare ignored). |  |
| `trips_unknown_zone` | INT64 | Trips of the day with flag_unknown_zone (warning, rows kept). |  |

### `nyc_taxi_gold.agg_zone_monthly`

model (incremental) `agg_zone_monthly`, `models/gold/agg_zone_monthly.sql`

Pickups and drop-offs per zone and month.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `pickup_month` | INT64 | Pickup month YYYYMM, equal to the bronze file month (rule pickup_outside_file_month). Integer-range partition key. |  |
| `zone_id` | INT64 | TLC zone, key to dim_zones. | not_null, relationships |
| `pickups` | INT64 | Trips of the month picked up in the zone (0 when none). |  |
| `dropoffs` | INT64 | Trips of the month dropped off in the zone (0 when none). |  |

### `nyc_taxi_gold.dim_date`

model (table) `dim_date`, `models/gold/dim_date.sql`

Calendar from var dim_date_start to dim_date_end.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `date_day` | DATE | Calendar day. | not_null, unique |
| `calendar_year` | INT64 | Year. |  |
| `calendar_quarter` | INT64 | Quarter, 1 to 4. |  |
| `calendar_month` | INT64 | Month, 1 to 12. |  |
| `year_month` | INT64 | Year * 100 + month (YYYYMM), comparable with pickup_month. |  |
| `day_of_month` | INT64 | Day of the month, 1 to 31. |  |
| `weekday_name` | STRING | English weekday name. | accepted_values |
| `is_weekend` | BOOL | True on Saturday and Sunday. |  |

### `nyc_taxi_gold.dim_payment_type`

model (table) `dim_payment_type`, `models/gold/dim_payment_type.sql`

Payment types, codes from the TLC Yellow Taxi data dictionary (seed payment_type_lookup).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `payment_type_id` | INT64 | TLC payment type code, 0 to 6. | not_null, unique |
| `payment_type_name` | STRING | Label of the code (e.g. Credit card, Cash, Flex Fare trip). | not_null |

### `nyc_taxi_gold.dim_rate_code`

model (table) `dim_rate_code`, `models/gold/dim_rate_code.sql`

Rate codes, codes from the TLC Yellow Taxi data dictionary (seed rate_code_lookup).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `rate_code_id` | INT64 | TLC rate code, 1 to 6, or 99 (Null/unknown). | not_null, unique |
| `rate_code_name` | STRING | Label of the code (e.g. Standard rate, JFK, Negotiated fare). | not_null |

### `nyc_taxi_gold.dim_zones`

model (table) `dim_zones`, `models/gold/dim_zones.sql`

NYC taxi zones (265 rows, 2 unknown, 3 airports).

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `zone_id` | INT64 | TLC zone id (LocationID), 1 to 265. | not_null, unique |
| `borough` | STRING | Borough of the zone (Unknown for zones 264 and 265). | accepted_values, not_null |
| `zone_name` | STRING | Zone name. |  |
| `service_zone` | STRING | TLC service zone (Boro Zone, Yellow Zone, Airports, EWR, Unknown). |  |
| `is_airport` | BOOL | True for the airport zones (1 EWR, 132 JFK, 138 LaGuardia). |  |
| `is_unknown` | BOOL | True for zones 264 and 265 (TLC Unknown / outside of NYC). |  |

### `nyc_taxi_gold.fct_trips`

model (view) `fct_trips`, `models/gold/fct_trips.sql`

Star-schema fact table, one row per trip of silver.trips (view, no copy). Rejected rows are not here: see silver.trips_quarantine.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `trip_id` | INT64 | FARM_FINGERPRINT of the 20 source fields of the bronze row, file month included. Exact copies share it; the same value is used in silver.trips and trips_quarantine. |  |
| `pickup_month` | INT64 | Pickup month YYYYMM, equal to the bronze file month (rule pickup_outside_file_month). Integer-range partition key. |  |
| `pickup_date` | DATE | Pickup day, key to dim_date. | relationships |
| `pickup_datetime` | TIMESTAMP | Date and time the meter was engaged (pickup). |  |
| `dropoff_datetime` | TIMESTAMP | Date and time the meter was disengaged (drop-off). |  |
| `pickup_zone_id` | INT64 | Pickup zone, key to dim_zones. | relationships |
| `dropoff_zone_id` | INT64 | Drop-off zone, key to dim_zones. | relationships |
| `payment_type_id` | INT64 | Payment type, key to dim_payment_type. | relationships |
| `rate_code_id` | INT64 | Rate code (99 when null in the source), key to dim_rate_code. | relationships |
| `vendor_id` | INT64 | TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025. |  |
| `passenger_count` | INT64 | Number of passengers, entered by the driver; often null. |  |
| `trip_distance_mi` | FLOAT64 | Trip distance in miles reported by the taximeter. |  |
| `trip_duration_min` | FLOAT64 | Drop-off minus pickup, in minutes, rounded to 2 decimals. |  |
| `avg_speed_mph` | FLOAT64 | trip_distance_mi divided by the duration in hours. |  |
| `fare_amount` | FLOAT64 | Time-and-distance fare computed by the meter, USD. |  |
| `extra` | FLOAT64 | Miscellaneous extras and surcharges, USD. |  |
| `mta_tax` | FLOAT64 | MTA tax, USD. |  |
| `tip_amount` | FLOAT64 | Tip, USD. Card tips only: cash tips are not recorded. |  |
| `tolls_amount` | FLOAT64 | Tolls paid during the trip, USD. |  |
| `improvement_surcharge` | FLOAT64 | Improvement surcharge, USD. |  |
| `congestion_surcharge` | FLOAT64 | New York State congestion surcharge, USD. |  |
| `airport_fee` | FLOAT64 | Airport fee (pickups at LaGuardia and JFK), USD. |  |
| `total_amount` | FLOAT64 | Total amount charged to the passenger, USD (cash tips not included). |  |
| `flag_unknown_zone` | BOOL | Warning, row kept. Pickup or drop-off in zone 264 or 265 (TLC "Unknown"). |  |
| `flag_passenger_count` | BOOL | Warning, row kept. passenger_count null, 0 or above 6. |  |
| `flag_short_trip` | BOOL | Warning, row kept. Trip under 60 seconds. |  |

### `nyc_taxi_gold.tlc_zone_reconciliation`

model (table) `tlc_zone_reconciliation`, `models/gold/tlc_zone_reconciliation.sql`

Informative control: pickups per zone and month, bronze and silver against the counts the TLC publishes (XML, NYC Open Data c5iv-bn4s). is_alert marks the gaps above the threshold (vars tlc_alert_gap_pct, tlc_alert_min_trips). Never blocking.

| Colonne | Type | Description | Tests |
|---|---|---|---|
| `month` | INT64 | Month YYYYMM; only months with both a bronze trip file and a TLC publication. | not_null |
| `zone_id` | INT64 | TLC zone. | not_null |
| `zone_name` | STRING | Zone name from dim_zones. |  |
| `borough` | STRING | Borough from dim_zones. |  |
| `tlc_pickups` | INT64 | Yellow Taxi pickups published by the TLC (after its own filtering); 0 when the zone is not published. |  |
| `bronze_pickups` | INT64 | Bronze rows of the month picked up in the zone (pickup inside the file month). |  |
| `kept_pickups` | INT64 | The same, after the silver rejection rules (agg_zone_monthly). |  |
| `gap_bronze` | INT64 | bronze_pickups - tlc_pickups. |  |
| `gap_bronze_pct` | FLOAT64 | gap_bronze in % of tlc_pickups, rounded to 2 decimals (null when tlc_pickups is 0). |  |
| `gap_kept_pct` | FLOAT64 | (kept_pickups - tlc_pickups) in % of tlc_pickups, rounded to 2 decimals (null when tlc_pickups is 0). |  |
| `is_alert` | BOOL | **TLC alert** (`is_alert`): true when both hold for a zone and month: at least `tlc_alert_min_trips` trips (var, 1000) on one side (TLC or bronze), and a gap bronze - TLC above `tlc_alert_gap_pct` (var, 10) % of the TLC count, or the zone absent from the TLC side. Informative, never blocking: the test `tlc_zone_reconciliation_alerts` has severity warn. Thresholds: `docs/qualite.md`. |  |

## Doc blocks (`dbt/models/_docs.md`)

### `exclusion_reason`

First rejection rule the row breaks, in the order of the 16 rejection rules;
null when the row is kept. Becomes `rejection_reason` in `trips_quarantine`.

### `flag_passenger_count`

Warning, row kept. passenger_count null, 0 or above 6.

### `flag_short_trip`

Warning, row kept. Trip under 60 seconds.

### `flag_unknown_zone`

Warning, row kept. Pickup or drop-off in zone 264 or 265 (TLC "Unknown").

### `ingestion_loaded_at`

Time of the bronze load (UTC), the same on every row of one load.

### `ingestion_run_id`

Pipeline run that loaded the bronze table (Airflow run_id, or `manual__<UTC time>`).

### `ingestion_source`

Bronze ingestion column: source system (`nyc_tlc_trip_records`,
`nyc_tlc_taxi_zone_lookup` or `nyc_open_data_c5iv-bn4s`).

### `ingestion_source_file`

Name of the file loaded into bronze.

### `pickup_month`

Pickup month YYYYMM, equal to the bronze file month (rule pickup_outside_file_month).
Integer-range partition key.

### `quarantine`

**Quarantine**: the bronze rows rejected by a rejection rule, kept as received
(bronze columns renamed to snake_case, values unchanged) with the rule, the run
that rejected them and when. Partition `source_month`, replaced month by month:
re-running a month replaces its quarantine instead of adding a second copy.
Silver rows + quarantine rows = bronze rows, for each month (tested).

### `rejection_reason`

Rejection rule that sent the row to quarantine: the first of the 16 rejection
rules the row breaks (see the `rejection_rules` doc block). One reason per row.

### `rejection_rules`

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

### `row_outcome`

`kept`, or the rejection rule that sent the rows to quarantine (one of the 16
rejection rules).

### `run_id`

Pipeline run that wrote the row: dbt var `run_id` (Airflow run_id), else dbt's
invocation_id.

### `source_month`

Month of the bronze file (YYYYMM integer), read from the bronze table suffix.

### `tlc_airport_fee`

Airport fee (pickups at LaGuardia and JFK), USD.

### `tlc_alert`

**TLC alert** (`is_alert`): true when both hold for a zone and month:
at least `tlc_alert_min_trips` trips (var, 1000) on one side (TLC or bronze),
and a gap bronze - TLC above `tlc_alert_gap_pct` (var, 10) % of the TLC count,
or the zone absent from the TLC side. Informative, never blocking: the test
`tlc_zone_reconciliation_alerts` has severity warn. Thresholds: `docs/qualite.md`.

### `tlc_congestion_surcharge`

New York State congestion surcharge, USD.

### `tlc_dropoff_datetime`

Date and time the meter was disengaged (drop-off).

### `tlc_dropoff_zone`

TLC taxi zone of the drop-off (1 to 265, see `taxi_zone_lookup`).

### `tlc_extra`

Miscellaneous extras and surcharges, USD.

### `tlc_fare_amount`

Time-and-distance fare computed by the meter, USD.

### `tlc_improvement_surcharge`

Improvement surcharge, USD.

### `tlc_mta_tax`

MTA tax, USD.

### `tlc_passenger_count`

Number of passengers, entered by the driver; often null.

### `tlc_payment_type`

Payment type code, 0 to 6 (see `payment_type_lookup`).

### `tlc_pickup_datetime`

Date and time the meter was engaged (pickup).

### `tlc_pickup_zone`

TLC taxi zone of the pickup (1 to 265, see `taxi_zone_lookup`).

### `tlc_rate_code_id`

TLC rate code (see `rate_code_lookup`); null on Flex Fare rows.

### `tlc_store_and_fwd_flag`

`Y` when the record was stored in the vehicle then forwarded, `N` otherwise.

### `tlc_tip_amount`

Tip, USD. Card tips only: cash tips are not recorded.

### `tlc_tolls_amount`

Tolls paid during the trip, USD.

### `tlc_total_amount`

Total amount charged to the passenger, USD (cash tips not included).

### `tlc_trip_distance`

Trip distance in miles reported by the taximeter.

### `tlc_vendor_id`

TPEP provider code (TLC Yellow Taxi data dictionary); 1, 2, 6 or 7 in 2025.

### `trip_id`

FARM_FINGERPRINT of the 20 source fields of the bronze row, file month included.
Exact copies share it; the same value is used in silver.trips and trips_quarantine.

### `warning_flags`

**Warnings** (3 flags): the row is kept in silver and the flag is counted in
`silver.quality_report` (`outcome_kind = 'warning'`). Used when the trip took
place and was paid but one secondary field is doubtful.
