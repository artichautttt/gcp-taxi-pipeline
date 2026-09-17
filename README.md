# GCP Taxi Pipeline

Pipeline data engineering minimal mais réel : ingestion, chargement, transformation
et restitution de données NYC Yellow Taxi sur Google Cloud Platform, en restant
à **0€ de coût réel** (BigQuery Sandbox, sans compte de facturation lié).

## Pourquoi ce dataset

[NYC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) :
dataset public, mis à jour mensuellement, volumétrie idéale pour un projet gratuit
(~55-60 Mo par mois en Parquet, ~3.5M lignes) — assez réaliste pour manipuler du
vrai volume, assez petit pour tenir large sous les quotas gratuits.

## Architecture

```
NYC TLC (source publique, Parquet)
        │  download_taxi_data.py (requests, streaming)
        ▼
   data/*.parquet (local)
        │  load_to_bigquery.py (google-cloud-bigquery, schéma explicite)
        ▼
 BigQuery Sandbox : nyc_taxi.yellow_tripdata_raw
        │  sql/01_data_quality_checks.sql (contrôle : nulls, doublons, cohérence)
        │  sql/02_transform_trips_clean.sql (filtre + colonnes dérivées)
        ▼
 BigQuery : nyc_taxi.trips_clean
        │  sql/03_analytics_daily_kpis.sql (agrégats journaliers)
        ▼
 BigQuery : nyc_taxi.daily_kpis
        │
        ▼
   Looker Studio (dashboard, connexion directe BigQuery)
```

Pas de Cloud Storage dans ce pipeline : GCS exige un compte de facturation lié
même pour rester dans l'Always Free tier, ce qui contredit la contrainte "0€,
aucune carte bancaire" de ce projet. Le chargement se fait donc directement
depuis un fichier Parquet local vers BigQuery Sandbox.

## Pourquoi BigQuery Sandbox (et pas GCS + billing)

BigQuery propose un mode "Sandbox" utilisable **sans compte de facturation**,
avec ces limites (acceptables ici) :
- les datasets expirent automatiquement après 60 jours (`defaultTableExpirationMs`)
- pas de streaming insert, pas de requêtes planifiées (scheduled queries)
- 10 Go de stockage et 1 To de requêtes traitées par mois inclus

## Coût réel

**0 €.** Aucune carte bancaire liée au projet. Volumétrie utilisée très en
dessous des quotas gratuits :

| Ressource                          | Utilisé      | Quota gratuit Sandbox |
|-------------------------------------|-------------|------------------------|
| Stockage BigQuery (3 tables)         | ~935 Mo     | 10 Go                  |
| Volume de requêtes (chargement + SQL)| < 1 Go traité| 1 To/mois             |

## Volumétrie traitée

- Source : `yellow_tripdata_2025-01.parquet`, ~59 Mo, 3 475 226 lignes brutes
- Après nettoyage (`trips_clean`) : 3 253 304 lignes (94% conservées)
- Anomalies détectées et exclues : montants négatifs (144 118), trajets
  dropoff < pickup (124), distances > 200 mi (122), durées > 6h (1 167)

## Qualité de données

`sql/01_data_quality_checks.sql` vérifie avant transformation :
1. Valeurs nulles sur les colonnes critiques (dates, distance, tarif, zones)
2. Doublons exacts (comparaison ligne à ligne via `TO_JSON_STRING`)
3. Cohérence métier : distances/tarifs négatifs ou aberrants, dropoff avant
   pickup, trajets de plus de 6h, années hors plage attendue

Résultat sur le mois de janvier 2025 : 0 valeur nulle, 0 doublon, anomalies
métier réelles mais minoritaires (~6% des lignes), filtrées dans `trips_clean`.

## Reproduction

Prérequis : un projet GCP, `gcloud` CLI authentifié, Python 3.11-3.13
(pyarrow n'a pas encore de wheel pour 3.14 au moment de l'écriture).

```bash
# 1. Setup GCP (une fois)
gcloud services enable bigquery.googleapis.com --project=<PROJECT_ID>
bq mk --dataset --location=US <PROJECT_ID>:nyc_taxi
gcloud auth application-default login   # credentials pour le SDK Python

# 2. Environnement Python
python -m venv venv
venv\Scripts\activate      # (Windows) — source venv/bin/activate sous Linux/Mac
pip install -r requirements.txt

# 3. Ingestion + chargement
python ingestion/download_taxi_data.py --year 2025 --month 1
set GCP_PROJECT_ID=<PROJECT_ID>
python ingestion/load_to_bigquery.py --file data/yellow_tripdata_2025-01.parquet

# 4. Transformation (bq CLI ou console BigQuery)
bq query --use_legacy_sql=false < sql/01_data_quality_checks.sql
bq query --use_legacy_sql=false < sql/02_transform_trips_clean.sql
bq query --use_legacy_sql=false < sql/03_analytics_daily_kpis.sql
```

Configuration : copier `.env.example` vers `.env` et ajuster `GCP_PROJECT_ID`.
Le fichier `.env` et toute clé de service ne sont jamais commités (`.gitignore`).

## Dashboard Looker Studio

1. https://lookerstudio.google.com → "Créer" → "Source de données" → BigQuery
2. Sélectionner le projet → dataset `nyc_taxi` → table `daily_kpis`
3. Construire le rapport avec au minimum :
   - Série temporelle : `trip_count` par `pickup_date`
   - Indicateur : `revenue_usd` total sur la période
   - Graphique : `avg_tip_pct` par `pickup_weekday`

Looker Studio est gratuit sans limite liée à ce projet (facturé uniquement le
volume de requêtes BigQuery sous-jacent, déjà comptabilisé ci-dessus).

## Nettoyage (important)

Pour éviter tout coût résiduel une fois le projet terminé :

```bash
bq rm -r -f <PROJECT_ID>:nyc_taxi   # supprime le dataset et toutes ses tables
```

Le dataset Sandbox expire de toute façon automatiquement après 60 jours
d'inactivité, mais autant le supprimer explicitement dès que le portfolio
n'en a plus besoin.
