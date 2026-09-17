-- Construit la table finale exploitable trips_clean à partir de
-- yellow_tripdata_raw : filtre les lignes invalides identifiées dans
-- 01_data_quality_checks.sql et ajoute des colonnes dérivées utiles
-- pour le dashboard (durée, vitesse, tranche horaire).
--
-- CREATE OR REPLACE TABLE : simple à ré-exécuter (idempotent), suffisant
-- à cette échelle (pas besoin de MERGE incrémental pour un batch mensuel).

CREATE OR REPLACE TABLE `mini-siem-taxi-pipeline.nyc_taxi.trips_clean` AS
SELECT
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) / 60.0 AS trip_duration_min,
  passenger_count,
  trip_distance,
  SAFE_DIVIDE(trip_distance, TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) / 3600.0) AS avg_speed_mph,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  tip_amount,
  tolls_amount,
  total_amount,
  EXTRACT(DATE FROM tpep_pickup_datetime) AS pickup_date,
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
  FORMAT_DATE('%A', DATE(tpep_pickup_datetime)) AS pickup_weekday
FROM `mini-siem-taxi-pipeline.nyc_taxi.yellow_tripdata_raw`
WHERE
  tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime >= tpep_pickup_datetime
  AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, HOUR) <= 6
  AND trip_distance > 0
  AND trip_distance <= 200
  AND fare_amount >= 0
  AND fare_amount <= 1000
  AND EXTRACT(YEAR FROM tpep_pickup_datetime) IN (2024, 2025);
