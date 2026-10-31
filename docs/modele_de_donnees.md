# Modèle de données

Dictionnaire des colonnes par couche. Projet `mini-siem-taxi-pipeline`, trois
datasets (noms modifiables par `BQ_BRONZE_DATASET`, `BQ_SILVER_DATASET`,
`BQ_GOLD_DATASET`). La chaîne historique (`nyc_taxi` : `yellow_tripdata_raw`,
`trips_clean`, `daily_kpis`, `taxi_zones`) est décrite dans le README.

Conventions : `AAAAMM` = mois en entier (`202501`) ; « mois du fichier » = mois
du fichier TLC chargé, lu dans le suffixe de la table Bronze.

## Bronze : `nyc_taxi_bronze`

Écrit par `ingestion/bronze.py`, jamais par dbt. Chaque source telle que
reçue, plus quatre colonnes d'ingestion, identiques sur toutes les lignes d'un
même chargement :

| Colonne | Type | Contenu |
|---|---|---|
| `_source` | STRING | système source : `nyc_tlc_trip_records`, `nyc_tlc_taxi_zone_lookup`, `nyc_open_data_c5iv-bn4s` |
| `_source_file` | STRING | nom du fichier chargé |
| `_loaded_at` | TIMESTAMP | heure du chargement (UTC) |
| `_run_id` | STRING | run du pipeline (run_id Airflow, ou `manual__<heure UTC>`) |

### `yellow_tripdata_AAAAMM` (Parquet TLC, une table par mois)

Colonnes du fichier TLC, noms et types d'origine (dictionnaire TLC Yellow Taxi) :

| Colonne | Type | Contenu |
|---|---|---|
| `VendorID` | INTEGER | fournisseur de la technologie de bord (1, 2, 6, 7 en 2025) |
| `tpep_pickup_datetime` / `tpep_dropoff_datetime` | TIMESTAMP | prise en charge / dépose |
| `passenger_count` | INTEGER | passagers (déclaré par le chauffeur, souvent nul) |
| `trip_distance` | FLOAT | distance en miles |
| `RatecodeID` | INTEGER | code tarifaire (nul sur les courses Flex Fare) |
| `store_and_fwd_flag` | STRING | `Y` si l'enregistrement a été stocké puis transmis |
| `PULocationID` / `DOLocationID` | INTEGER | zone TLC de prise en charge / dépose |
| `payment_type` | INTEGER | mode de paiement (0 à 6) |
| `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `total_amount` | FLOAT | montants en USD |

`cbd_congestion_fee` (présent dans les fichiers 2025) n'est pas chargé : même
contrat de schéma que la chaîne historique.

### `taxi_zone_lookup` (CSV TLC)

`LocationID`, `Borough`, `Zone`, `service_zone` : STRING, valeurs brutes du
fichier officiel (y compris `N/A`).

### `tlc_zone_trips_AAAAMM` (XML NYC Open Data `c5iv-bn4s`, une table par mois)

Tout en STRING, tel que renvoyé par l'API Socrata : `metric_month`
(`2025-01-01`), `industry` (`Yellow Taxi` uniquement), `pickup_dropoff`
(`Pick-up` / `Drop-off`), `locationid`, `borough`, `zone`, `trip_count`.

### `ingestion_log` (un enregistrement par chargement, ajout seul)

`run_id`, `source`, `source_file`, `source_uri`, `file_sha256`, `file_bytes`,
`source_rows` (lignes de la source), `loaded_rows` (lignes de la table après
chargement), `target_table`, `loaded_at`, `status` (`ok` ou `count_mismatch`).

## Silver : `nyc_taxi_silver`

Construit par dbt (`dbt/models/silver`, seeds).

### `trips` (incrémental, partition entière `pickup_month`, cluster `pickup_zone_id`)

Une ligne par ligne Bronze qui respecte les 16 règles de rejet
([qualite.md](qualite.md)).

| Colonne | Type | Contenu |
|---|---|---|
| `trip_id` | INT64 | `FARM_FINGERPRINT` des 20 champs source de la ligne (mois du fichier compris) ; les copies exactes le partagent |
| `pickup_month` | INT64 | mois de prise en charge `AAAAMM`, égal au mois du fichier ; clé de partition |
| `pickup_date` | DATE | jour de prise en charge |
| `pickup_datetime`, `dropoff_datetime` | TIMESTAMP | horodatages |
| `pickup_zone_id`, `dropoff_zone_id` | INT64 | zones TLC (1 à 265) |
| `payment_type_id` | INT64 | mode de paiement |
| `rate_code_id` | INT64 | code tarifaire ; nul dans la source → 99 (« Null/unknown » du dictionnaire TLC) |
| `vendor_id` | INT64 | 1, 2, 6 ou 7 |
| `passenger_count` | INT64 | tel que reçu (peut être nul) |
| `trip_distance_mi` | FLOAT64 | distance (0,01 à 200 mi) |
| `trip_duration_min` | FLOAT64 | durée en minutes, arrondie au centième (> 0, ≤ 360) |
| `avg_speed_mph` | FLOAT64 | vitesse moyenne (≤ 100 mph) |
| `fare_amount` … `total_amount` | FLOAT64 | montants en USD, tels que reçus |
| `flag_unknown_zone` | BOOL | alerte : zone 264 ou 265 (« Unknown » TLC) |
| `flag_passenger_count` | BOOL | alerte : passagers nul, 0 ou > 6 |
| `flag_short_trip` | BOOL | alerte : moins de 60 secondes |

### `trips_quarantine` (incrémental, partition entière `source_month`)

Une ligne par ligne Bronze rejetée.

| Colonne | Type | Contenu |
|---|---|---|
| `source_month` | INT64 | mois du fichier ; clé de partition |
| `rejection_reason` | STRING | première règle enfreinte (une des 16) |
| `run_id` | STRING | run qui a rejeté la ligne |
| `quarantined_at` | TIMESTAMP | heure de la mise en quarantaine |
| `trip_id` | INT64 | même empreinte que dans `trips` |
| `vendor_id` … `airport_fee` | | colonnes Bronze renommées en snake_case, valeurs non modifiées |
| `source_file`, `bronze_loaded_at`, `ingestion_run_id` | | métadonnées d'ingestion de la ligne Bronze |

### `audit_trip_rows` (incrémental, partition entière `source_month`)

`source_month`, `row_outcome` (`kept` ou une règle), `row_count`, `run_id`.
Pour chaque mois, la somme de `row_count` = lignes Bronze du mois.

### `quality_report` (ajout seul, une série de lignes par run)

Écrit par `quality/quality_gate.py`, même quand le contrôle échoue.

| Colonne | Contenu |
|---|---|
| `run_id`, `pipeline_month`, `reported_at` | run, mois, heure |
| `gate_status` | `passed` ou `failed` |
| `outcome_kind`, `outcome`, `row_count` | `read` (`source_rows`, `bronze_rows`), `valid` (`kept`), `rejected` (une règle), `warning` (un indicateur), `tlc_control` (`tlc_alert_zones`) |
| `dbt_tests`, `dbt_tests_not_passing`, `dbt_warnings` | résultat de `dbt test` |
| `problems` | écarts trouvés par le contrôle, sinon nul |

### Référentiels (seeds) et vues

- `taxi_zone_lookup` : sortie de `vba/TaxiZoneCleaner.bas` (`location_id`,
  `borough`, `zone`, `service_zone`, `is_airport`, `is_unknown`), 265 zones.
- `payment_type_lookup`, `rate_code_lookup` : codes du dictionnaire TLC.
- Vues `stg_yellow_trips`, `stg_taxi_zone_lookup`, `stg_tlc_zone_trips` :
  Bronze renommé et typé, sans filtre.

## Gold : `nyc_taxi_gold`

| Objet | Grain | Colonnes |
|---|---|---|
| `fct_trips` (vue sur `silver.trips`) | trajet | celles de `silver.trips` |
| `dim_zones` | zone | `zone_id`, `borough`, `zone_name`, `service_zone`, `is_airport`, `is_unknown` |
| `dim_payment_type` | code | `payment_type_id`, `payment_type_name` |
| `dim_rate_code` | code | `rate_code_id`, `rate_code_name` |
| `dim_date` | jour (2024-01-01 → 2026-12-31) | `date_day`, `calendar_year`, `calendar_quarter`, `calendar_month`, `year_month`, `day_of_month`, `weekday_name`, `is_weekend` |
| `agg_daily_kpis` (partition `pickup_month`) | jour | `trip_count`, `revenue_usd`, `avg_distance_mi`, `avg_duration_min`, `avg_total_amount_usd`, `avg_tip_pct`, `trips_unknown_zone` |
| `agg_zone_monthly` (partition `pickup_month`) | zone × mois | `pickups`, `dropoffs` |
| `tlc_zone_reconciliation` | zone × mois | `tlc_pickups`, `bronze_pickups`, `kept_pickups`, `gap_bronze`, `gap_bronze_pct`, `gap_kept_pct`, `is_alert` |
