-- Tests de qualité de données sur la table brute yellow_tripdata_raw.
-- Objectif : quantifier les anomalies avant de construire la table finale,
-- pas les corriger ici (la table de transformation filtre ce qui est
-- exploité en aval, voir 02_transform_trips_clean.sql).

-- 1. Valeurs nulles sur les colonnes critiques
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(tpep_pickup_datetime IS NULL) AS null_pickup_datetime,
  COUNTIF(tpep_dropoff_datetime IS NULL) AS null_dropoff_datetime,
  COUNTIF(trip_distance IS NULL) AS null_trip_distance,
  COUNTIF(fare_amount IS NULL) AS null_fare_amount,
  COUNTIF(PULocationID IS NULL) AS null_pu_location,
  COUNTIF(DOLocationID IS NULL) AS null_do_location
FROM `mini-siem-taxi-pipeline.nyc_taxi.yellow_tripdata_raw`;

-- 2. Doublons exacts (une course ne devrait apparaître qu'une fois)
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT TO_JSON_STRING(t)) AS distinct_rows,
  COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(t)) AS duplicate_rows
FROM `mini-siem-taxi-pipeline.nyc_taxi.yellow_tripdata_raw` AS t;

-- 3. Cohérence métier : valeurs impossibles ou aberrantes
SELECT
  COUNTIF(trip_distance < 0) AS negative_distance,
  COUNTIF(trip_distance > 200) AS distance_over_200mi,
  COUNTIF(fare_amount < 0) AS negative_fare,
  COUNTIF(fare_amount > 1000) AS fare_over_1000usd,
  COUNTIF(passenger_count IS NOT NULL AND passenger_count = 0) AS zero_passengers,
  COUNTIF(tpep_dropoff_datetime < tpep_pickup_datetime) AS dropoff_before_pickup,
  COUNTIF(TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, HOUR) > 6) AS trip_over_6h,
  COUNTIF(EXTRACT(YEAR FROM tpep_pickup_datetime) NOT IN (2024, 2025)) AS pickup_outside_expected_year
FROM `mini-siem-taxi-pipeline.nyc_taxi.yellow_tripdata_raw`;
