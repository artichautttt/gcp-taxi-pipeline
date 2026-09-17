-- Table d'agrégats journaliers, utilisée comme source pour le dashboard
-- Looker Studio (moins de lignes à scanner = requêtes de restitution
-- plus rapides et moins coûteuses en volume traité).

CREATE OR REPLACE TABLE `mini-siem-taxi-pipeline.nyc_taxi.daily_kpis` AS
SELECT
  pickup_date,
  pickup_weekday,
  COUNT(*) AS trip_count,
  ROUND(AVG(trip_distance), 2) AS avg_distance_mi,
  ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
  ROUND(AVG(total_amount), 2) AS avg_total_amount_usd,
  ROUND(SUM(total_amount), 2) AS revenue_usd,
  ROUND(AVG(SAFE_DIVIDE(tip_amount, NULLIF(fare_amount, 0))) * 100, 1) AS avg_tip_pct
FROM `mini-siem-taxi-pipeline.nyc_taxi.trips_clean`
GROUP BY pickup_date, pickup_weekday
ORDER BY pickup_date;
