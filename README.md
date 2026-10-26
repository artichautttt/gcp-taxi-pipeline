# GCP Taxi Pipeline

> Pipeline data engineering NYC Yellow Taxi sur GCP à 0 € : ingestion Python,
> transformations SQL BigQuery en 3 étages, tests dbt, orchestration Airflow et
> comparaison PySpark vs BigQuery.

Pipeline data engineering minimal mais réel : ingestion, chargement, transformation
et restitution de données NYC Yellow Taxi sur Google Cloud Platform, en restant
à **0€ de coût réel** (BigQuery Sandbox, sans compte de facturation lié).

## Problème

Les données brutes NYC TLC sont publiées chaque mois en Parquet, avec des
lignes inexploitables (montants négatifs, trajets qui finissent avant de
commencer, durées ou distances aberrantes). Le pipeline doit :
1. vérifier que le fichier du mois est publié, puis le charger dans BigQuery ;
2. mesurer les anomalies et produire une table propre (`trips_clean`) et des
   agrégats journaliers (`daily_kpis`) pour un dashboard ;
3. garantir la qualité du résultat par des tests automatisés (dbt) et
   enchaîner le tout de façon reproductible (Airflow) ;
4. rester à 0 € et sans clé de service account.

## Résultats mesurés

Mois traité : janvier 2025 (`yellow_tripdata_2025-01.parquet`). Chiffres
relevés le 2026-10-16 lors d'une exécution complète du DAG Airflow, puis de
`spark/spark_transform.py` et `spark/compare_with_bigquery.py` :

| Mesure | Valeur |
|---|---|
| Lignes brutes chargées (`yellow_tripdata_raw`) | 3 475 226 |
| Lignes conservées (`trips_clean`) | 3 253 304 |
| Lignes écartées comme anomalies | 221 922 (6,4 % des lignes brutes) |
| Jours dans `daily_kpis` | 33 (31/12/2024 → 01/02/2025, horodatages de bord de mois) |
| `trips_clean` BigQuery vs PySpark | 3 253 304 lignes de part et d'autre (identique) |
| `daily_kpis` BigQuery vs PySpark | 5/5 contrôles OK, tolérance 0,5 % (revenu total 87 219 707,12 $ des deux côtés) |
| Tests dbt | 20/20 PASS |
| DAG Airflow `nyc_taxi_pipeline` (`airflow dags test`) | 6/6 tâches en succès |

## Pourquoi ce dataset

[NYC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) :
dataset public, mis à jour mensuellement, volumétrie idéale pour un projet gratuit
(~55-60 Mo par mois en Parquet, ~3.5M lignes) — assez réaliste pour manipuler du
vrai volume, assez petit pour tenir large sous les quotas gratuits.

## Architecture

```
            ┌──────────── Airflow DAG nyc_taxi_pipeline (airflow/) ────────────┐
NYC TLC (source publique, Parquet)
        │  check_source_available : HTTP HEAD sur le fichier du mois (CloudFront)
        │  download_taxi_data.py (requests, streaming)              ┐
        ▼                                                           │ ingest
   data/*.parquet (local) ──────────────► spark/ (PySpark local,    │
        │  load_to_bigquery.py             mêmes 3 étages,          ┘
        ▼                                  comparé à BigQuery)
 BigQuery Sandbox : nyc_taxi.yellow_tripdata_raw          (staging)
        │  sql/01_data_quality_checks.sql (contrôle : nulls, doublons, cohérence)
        │  sql/02_transform_trips_clean.sql (filtre + colonnes dérivées)
        ▼
 BigQuery : nyc_taxi.trips_clean                          (nettoyée)
        │  sql/03_analytics_daily_kpis.sql (agrégats journaliers)
        ▼
 BigQuery : nyc_taxi.daily_kpis                           (agrégats)
        │  dbt test (dbt/) : 20 tests sur les 3 tables
            └──────────────────────────────────────────────────────────────────┘
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
| Volume de requêtes SQL (`sql/01-03`, une exécution) | ~1,3 Go traité (793 + 329 + 186 Mo) | 1 To/mois |

Le chargement (load job) n'est pas facturé en volume de requêtes.

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
Pour exécuter le pipeline en moindre privilège (service account dédié, sans
clé), voir [Sécurité & gouvernance des accès](#sécurité--gouvernance-des-accès).
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

## PySpark local (voie alternative)

Le pipeline BigQuery ci-dessus reste la voie principale (SQL managé, dashboard
Looker Studio). `spark/` ajoute une **voie alternative en parallèle**, sur les
mêmes données Parquet locales : elle reproduit les 3 mêmes étages (contrôle
qualité → `trips_clean` → `daily_kpis`) avec la DataFrame API PySpark plutôt
qu'en SQL BigQuery, en local (`local[*]`, sans cluster ni service Spark
managé). Objectif : démontrer la même logique de traitement de données avec un
moteur de traitement distribué (Spark), en complément du SQL managé — utile
pour comparer les deux approches ou repartir sur un moteur non-SQL si le
volume dépassait un jour les quotas Sandbox.

Prérequis : Java 17+ (requis par Spark), en plus de l'environnement Python
déjà utilisé pour l'ingestion (`pip install -r requirements.txt` installe
aussi `pyspark`).

```bash
# Transformation (lit data/yellow_tripdata_2025-01.parquet par défaut)
python spark/spark_transform.py --file data/yellow_tripdata_2025-01.parquet

# Equivalent avec spark-submit
spark-submit spark/spark_transform.py --file data/yellow_tripdata_2025-01.parquet
```

Écrit `data/spark_output/trips_clean.parquet` et `data/spark_output/daily_kpis.parquet`
(non commités, voir `.gitignore`).

### Vérification d'équivalence avec BigQuery

`spark/compare_with_bigquery.py` compare le nombre de lignes de `trips_clean`
et les agrégats clés de `daily_kpis` (nombre de jours, volume total de
courses, revenu total, tip % moyen) entre la sortie PySpark locale et les
tables BigQuery correspondantes, avec une tolérance de 0.5 % (arrondis
différents entre les deux moteurs). Ce script s'exécute en standalone, hors
pytest (il a besoin de BigQuery et de la sortie Spark), et retourne un code de
sortie non nul si un écart dépasse la tolérance :

```bash
# Nécessite le pipeline BigQuery déjà exécuté (étape 4) et spark_transform.py
# déjà exécuté (étape ci-dessus), avec le même GCP_PROJECT_ID que .env
python spark/compare_with_bigquery.py
```

## Tests de qualité : dbt

`dbt/` est un projet dbt (`dbt-bigquery`) en deux parties. Cette section décrit
la première : il déclare les 3 tables produites par `sql/01-03` comme
**sources** et les teste (dbt sert ici de contrôle qualité en aval de `sql/`).
La seconde, le schéma en étoile, est décrite dans
[Schéma en étoile dbt et DAG `nyc_taxi_elt`](#schéma-en-étoile-dbt-et-dag-nyc_taxi_elt).

| Table | Tests |
|---|---|
| `yellow_tripdata_raw` | `not_null` sur pickup/dropoff |
| `trips_clean` | `not_null` (dates, `pickup_date`, zones) ; `trip_distance` > 0 et ≤ 200 ; `fare_amount` entre 0 et 1000 ; `pickup_date` entre 2024-01-01 et 2025-12-31 ; `pickup_weekday` ∈ jours de la semaine ; dropoff ≥ pickup et durée ≤ 6 h |
| `daily_kpis` | `unique` + `not_null` sur `pickup_date` (clé) ; `trip_count` et `revenue_usd` non nuls et > 0 ; réconciliation `SUM(trip_count)` = `COUNT(*)` de `trips_clean` |

Les seuils reprennent les règles de filtrage de `sql/02_transform_trips_clean.sql`.
`trips_clean` n'a pas de clé naturelle (aucun identifiant de course dans la
source TLC) : pas de test `unique` sur cette table.

Authentification : `method: oauth` dans `dbt/profiles.yml`, c'est-à-dire ADC
(le même `gcloud auth application-default login` que le reste du pipeline),
sans clé JSON.

```bash
# dbt dans un venv séparé (évite les conflits de dépendances)
py -3.11 -m venv .venv-dbt
.venv-dbt\Scripts\pip install dbt-core==1.12.6 dbt-bigquery==1.12.1

set GCP_PROJECT_ID=<PROJECT_ID>
.venv-dbt\Scripts\dbt test --select source:nyc_taxi --project-dir dbt --profiles-dir dbt
# Résultat obtenu le 2026-10-16 : Done. PASS=20 WARN=0 ERROR=0 SKIP=0 TOTAL=20
# (relancé le 2026-10-26 avec --select source:nyc_taxi : PASS=20)
```

## Orchestration : Airflow

`airflow/dags/nyc_taxi_pipeline.py` enchaîne tout le pipeline :

```
check_source_available → ingest → sql_quality_checks → sql_trips_clean → sql_daily_kpis → dbt_test
```

- `check_source_available` : requête HTTP `HEAD` sur le fichier public NYC TLC
  du mois demandé ; la tâche échoue si le fichier n'est pas encore publié.
- `ingest` : réutilise `download_taxi_data.py` et `load_to_bigquery.py`.
- `sql_*` : exécute `sql/01`, `02`, `03` tels quels via le client BigQuery
  (les résultats des contrôles qualité de `sql/01` sont écrits dans le log).
- `dbt_test` : lance `dbt test --select source:nyc_taxi` (les 20 tests des
  tables `sql/01-03` ; le schéma en étoile est testé par le DAG `nyc_taxi_elt`).

Paramètres `year` / `month` (défaut 2025 / 1), déclenchement manuel
(`schedule=None`).

Airflow ne s'exécute pas nativement sous Windows. `airflow/docker-compose.yml`
fournit un environnement minimal : un seul conteneur `apache/airflow:3.3.2`,
base SQLite, dbt installé dans un venv séparé, repo monté sur `/opt/pipeline`,
et répertoire gcloud de l'hôte monté **en lecture seule** pour ADC (aucune clé).

```bash
docker compose -f airflow/docker-compose.yml build

# Validation + exécution complète d'un run, sans scheduler
docker compose -f airflow/docker-compose.yml run --rm airflow bash -c \
  "airflow db migrate && airflow dags reserialize && airflow dags list && airflow dags test nyc_taxi_pipeline 2025-01-01"

# UI (dev) sur http://localhost:8080
docker compose -f airflow/docker-compose.yml up
```

Sous Linux/macOS, définir `GCLOUD_CONFIG_DIR=~/.config/gcloud` avant `docker compose`.

Exécuté le 2026-10-16 (`airflow dags test nyc_taxi_pipeline 2025-01-01`) :
les 6 tâches sont en succès, avec le HEAD TLC → 200 (59 158 238 octets),
3 475 226 lignes chargées, `sql/01-03` OK et `dbt test` PASS=20.

## Schéma en étoile dbt et DAG `nyc_taxi_elt`

Deuxième chaîne, indépendante de la première (qui reste inchangée) : un mois
à la fois, du fichier TLC brut jusqu'à un schéma en étoile construit et testé
par dbt.

```
check_source → download → load_raw → dbt_seed → dbt_run → dbt_test → quality_gate
```

- `load_raw` : `ingestion/load_raw_month.py`, une table par mois
  `raw_yellow_tripdata_AAAAMM`, en écrasement (`WRITE_TRUNCATE`).
- `dbt_run` : `stg_yellow_trips` (vue sur `raw_yellow_tripdata_*`) →
  `int_trips_classified` (chaque ligne reçoit la première règle d'exclusion
  qu'elle enfreint) → `fct_trips` (un trajet par ligne) et `audit_trip_rows`
  (comptes gardés / écartés par règle), plus `dim_zones`, `dim_date`,
  `dim_payment_type`, `dim_rate_code`.
- `dbt_test` : 41 tests (unique, not_null, relationships, accepted_values,
  bornes, et 3 tests métier : réconciliation brut = gardées + écartées,
  règles de trajet, total ≥ tarif + pourboire).
- `quality_gate` : `quality/quality_gate.py` échoue si un test dbt n'est pas
  passé, ou si les lignes chargées par le load job ≠ lignes de
  `audit_trip_rows`, ou si les lignes gardées ≠ lignes du mois dans `fct_trips`.

**Incrémental sans DML.** La Sandbox refuse `INSERT`, `DELETE` et `MERGE`
(erreur 403 mesurée le 2026-10-26). `fct_trips` et `audit_trip_rows` sont donc
partitionnées par un entier `AAAAMM` et reconstruites en `insert_overwrite`
avec `copy_partitions` : relancer un mois remplace sa partition par une copie
de table, sans toucher aux autres mois. Pas de partition par date : le
dataset expire les partitions de plus de 60 jours, ce qui effacerait des
données de 2025 dès leur écriture.

**Règles d'exclusion** (dans cet ordre, une seule raison par ligne) :
horodatage manquant, pickup hors du mois du fichier, dropoff avant pickup,
durée > 6 h, distance nulle, ≤ 0 ou > 200 mi, tarif nul, < 0 ou > 1000 $,
total inférieur à tarif + pourboire, doublon exact. La règle « pickup hors du
mois » garde les partitions disjointes : le fichier de janvier contient
quelques trajets de décembre, qui ne doivent pas écraser décembre.

**Test remplacé.** Un premier test exigeait `total_amount` = somme des
composants (tarif, extra, taxe MTA, pourboire, péages, surcharges). Il
échouait sur 2 395 452 trajets de janvier 2025. Mesure sur le fichier brut
(3 475 226 lignes) : 2 564 195 lignes s'écartent de plus d'un centime, et
encore 1 111 807 après avoir ajouté la taxe CBD (`cbd_congestion_fee`, non
chargée par ce projet). Les écarts les plus fréquents valent +2,50 $
(396 358 lignes), −3,25 $ (391 172) et −2,50 $ (176 749), soit la surtaxe de
congestion et la taxe CBD comptées en double ou absentes selon les
enregistrements. C'est la façon dont le fichier TLC est rempli, pas une erreur
du pipeline : ce test n'est pas gardé. Il est remplacé par la règle
« total ≥ tarif + pourboire », enfreinte par 225 trajets sur 3 253 246 en
janvier, désormais écartés (`invalid_total`) et vérifiés par un test bloquant.

**Mesuré le 2026-10-26** (Sandbox BigQuery, exécution locale sous Docker) :

| Mois | Construit par | Lignes brutes | Dans `fct_trips` | Écartées |
|---|---|---|---|---|
| 2025-01 | `dbt run` (sans Airflow) | 3 475 226 | 3 253 021 | 222 205 |
| 2025-02 | `dbt run` (sans Airflow) | 3 577 543 | 3 310 026 | 267 517 |
| 2025-03 | DAG Airflow, de bout en bout | 4 145 257 | 3 847 686 | 297 571 |

- **Seul mars 2025 a tourné de bout en bout par Airflow** (`airflow dags test`,
  téléchargement compris) : 211 s, 7 tâches en succès, 41/41 tests, contrôle
  qualité vert. Janvier et février ont été chargés par
  `ingestion/load_raw_month.py` puis construits par `dbt run` directement.
- Relance du même mois par le DAG : 206 s (fichier déjà présent, téléchargement
  sauté), mêmes comptes à la ligne près, janvier et février inchangés.
- Une seule mesure par cas, sur une machine : les durées sont un ordre de
  grandeur, pas un benchmark.

**Versions réellement testées** : Python 3.11 (image
`apache/airflow:3.3.2-python3.11` pour le DAG, venv dbt local, CI) avec
Airflow 3.3.2, dbt-core 1.12.6 et dbt-bigquery 1.12.1. Airflow 3.3.2 supporte
officiellement Python 3.10 à 3.14, mais **Python 3.13 n'a pas été testé** pour
ce pipeline.

Le téléchargement écrit dans `<fichier>.part`, vérifie la taille reçue contre
`Content-Length`, puis renomme : une coupure ne laisse jamais de Parquet
tronqué qu'une relance réutiliserait (`tests/test_download_taxi_data.py`).

```bash
# un mois, de bout en bout (Docker)
docker compose -f airflow/docker-compose.yml run --rm airflow bash -c \
  'airflow db migrate && airflow dags test nyc_taxi_elt --conf "{\"year\": 2025, \"month\": 3}"'

# sans Airflow
python ingestion/load_raw_month.py --year 2025 --month 3
.venv-dbt\Scripts\dbt seed --project-dir dbt --profiles-dir dbt
.venv-dbt\Scripts\dbt run  --project-dir dbt --profiles-dir dbt --vars "{pipeline_month: 202503}"
.venv-dbt\Scripts\dbt test --project-dir dbt --profiles-dir dbt --exclude source:nyc_taxi
```

### Ce que vérifie la CI (et ce qu'elle ne vérifie pas)

GitHub Actions (`.github/workflows/ci.yml`) tourne sans aucun secret ni accès
à BigQuery. Elle vérifie seulement :
- le lint SQL (`sqlfluff lint dbt/models dbt/tests`) ;
- `dbt parse` et `dbt compile` (avec un faux fichier ADC sans valeur réelle et
  `--no-populate-cache --no-introspect`, pour qu'aucune requête ne parte) ;
- pytest, dont l'import réel des deux DAG par le `DagBag` d'Airflow 3.3.2.

**Les 41 tests dbt du schéma en étoile ne tournent pas en CI** : ils tournent
en local contre BigQuery (`dbt test` ou tâche `dbt_test` du DAG), comme les
exécutions du DAG. Un modèle qui compile en CI peut donc encore échouer sur
BigQuery.

### Expiration Sandbox et rechargement

Toute table du dataset expire 60 jours après sa dernière création ou son
dernier remplacement (réglage par défaut de la Sandbox). Dates relevées le
2026-10-26 :

| Table(s) | Expire le | Lue par |
|---|---|---|
| `yellow_tripdata_raw` | 16/11/2026 | `sql/01-02`, test dbt `source:nyc_taxi` |
| `taxi_zones` | 22/12/2026 | exemple de jointure ci-dessous |
| `trips_clean`, `daily_kpis` | 23/12/2026 | dashboard Looker Studio, `spark/compare_with_bigquery.py` |
| tables du schéma en étoile (`raw_yellow_tripdata_*`, `fct_trips`, `dim_*`, seeds…) | 25/12/2026 | DAG `nyc_taxi_elt` |

Conséquences si rien n'est rechargé :
- après le 16/11, `sql/01-03` et le DAG `nyc_taxi_pipeline` ne peuvent plus
  reconstruire `trips_clean` ni `daily_kpis`, et le test dbt sur
  `yellow_tripdata_raw` échoue ;
- après le 23/12, le dashboard Looker Studio et
  `spark/compare_with_bigquery.py` cessent de fonctionner (tables absentes).

Rechargement :
- chaîne historique : relancer le DAG `nyc_taxi_pipeline` (ou
  `load_to_bigquery.py` puis `sql/01-03`), ce qui recrée les trois tables avec
  60 nouveaux jours ;
- `taxi_zones` : relancer le `bq load` de la section VBA ;
- schéma en étoile : relancer `nyc_taxi_elt` **pour chaque mois** voulu. Si
  `fct_trips` a expiré, le premier run la recrée avec ce seul mois.

## Référentiel des zones : nettoyage Excel VBA

Les trajets ne contiennent que des identifiants (`PULocationID`, `DOLocationID`).
Pour les traduire en quartiers, la TLC publie une table de référence,
[`taxi_zone_lookup.csv`](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)
(265 zones). Elle n'est pas homogène : les zones inconnues 264 et 265 utilisent
tantôt `Unknown`, tantôt `N/A`, et les exports retouchés à la main ajoutent des
espaces, des casses différentes ou des doublons.

Le module [`vba/TaxiZoneCleaner.bas`](vba/TaxiZoneCleaner.bas) (macro
`CleanTaxiZones`) automatise ce nettoyage en local, dans Excel :

| Règle | Exemple |
|---|---|
| Espaces : trim, espaces doubles, espaces insécables, tabulations | `" queens "` → `Queens` |
| Borough ramené à une orthographe unique | `BRONX`, `the bronx` → `Bronx` |
| `service_zone` ramené à une orthographe unique | `yellow zone` → `Yellow Zone`, `airport` → `Airports` |
| Valeurs inconnues harmonisées | `N/A`, vide, `null` → `Unknown` |
| Séparateurs dans les noms de zone | `Allerton / Pelham Gardens` → `Allerton/Pelham Gardens` |
| Casse corrigée seulement si tout en majuscules ou tout en minuscules | `ALPHABET CITY` → `Alphabet City` (`JFK Airport` reste intact) |
| `LocationID` non numérique ou en double : ligne rejetée | `abc`, 2ᵉ ligne `4` |
| Signalés sans être modifiés : IDs manquants, borough inconnu, même nom de zone sur plusieurs IDs | `Corona` = 56 et 57 (légitime dans le fichier TLC) |

Sorties :
- feuille `zones_clean` : `location_id, borough, zone, service_zone, is_airport, is_unknown` ;
- feuille `zones_log` : chaque modification, rejet ou alerte (avant / après / règle) ;
- fichier `data/taxi_zone_lookup_clean.csv` (UTF-8 sans BOM), prêt pour BigQuery.

Utilisation :

```bash
curl -o data/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

1. Créer un classeur Excel prenant en charge les macros (`.xlsm`) dans `vba/`
   (la macro cherche alors `../data/taxi_zone_lookup.csv`, sinon il ouvre un sélecteur de fichier).
2. `Alt+F11` → Fichier → Importer un fichier → `vba/TaxiZoneCleaner.bas`.
3. `Alt+F8` → `CleanTaxiZones` → Exécuter.

Pour essayer les règles, `vba/sample/taxi_zone_lookup_dirty.csv` contient
des défauts volontaires (choisir ce fichier dans le sélecteur).

Chargement dans BigQuery et jointure avec les trajets :

```bash
bq load --source_format=CSV --skip_leading_rows=1 --replace nyc_taxi.taxi_zones \
  data/taxi_zone_lookup_clean.csv \
  location_id:INTEGER,borough:STRING,zone:STRING,service_zone:STRING,is_airport:BOOLEAN,is_unknown:BOOLEAN
```

```sql
SELECT z.borough, COUNT(*) AS trips
FROM nyc_taxi.trips_clean t
JOIN nyc_taxi.taxi_zones z ON z.location_id = t.PULocationID
GROUP BY z.borough ORDER BY trips DESC;
```

Résultats mesurés le 2026-10-23 (macro exécutée dans Excel, puis `bq load`) :

| Fichier | Lignes lues | Gardées | Rejetées | Valeurs corrigées | Entrées du journal |
|---|---|---|---|---|---|
| `vba/sample/taxi_zone_lookup_dirty.csv` | 11 | 9 | 2 (ID `abc`, ID `4` en double) | 13 | 16 |
| `taxi_zone_lookup.csv` (TLC officiel) | 265 | 265 | 0 | 4 (`N/A` → `Unknown` sur 264 et 265) | 7 (4 corrections + 3 alertes « même zone » : `Corona` 56/57, `Governor's Island/Ellis Island/Liberty Island` 103/104/105) |

Dans BigQuery, `nyc_taxi.taxi_zones` contient 265 zones (2 inconnues, 3 aéroports).
La jointure avec `trips_clean` couvre les 3 253 304 trajets (0 sans zone) :
Manhattan 2 907 381, Queens 269 203, Brooklyn 55 453, Bronx 12 764,
Unknown 8 223, Staten Island 203, EWR 77.

Pour l'automatisation ou les tests, `CleanTaxiZoneFile(chemin)` fait le même
traitement sans boîte de dialogue et renvoie le résumé.

Seul le code `.bas` est versionné (texte lisible dans git), pas le classeur `.xlsm`.

## Sécurité & gouvernance des accès

### Analyse de risque

**Données traitées.** Uniquement le dataset public [NYC TLC Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
déjà anonymisé par la source : pas de nom, pas de plaque, pas de coordonnées
GPS (les lieux sont des zones TLC `PULocationID`/`DOLocationID`). Aucune donnée
personnelle, aucune donnée client. Une fuite des tables n'exposerait rien qui
ne soit déjà téléchargeable publiquement.

**Ce qui est réellement à protéger** : l'accès au projet GCP, pas les données.
Si le projet était mal configuré :
- **clé de service account commitée** sur GitHub public : n'importe qui
  pourrait agir sur le projet avec les droits de ce compte (avec `owner` :
  modifier l'IAM, créer des ressources, supprimer les datasets) ;
- **identité du pipeline trop privilégiée** : un bug ou une erreur de
  manipulation (mauvais `GCP_PROJECT_ID`, `WRITE_TRUNCATE` sur la mauvaise
  table, `bq rm`) peut toucher tout le projet au lieu d'un seul dataset ;
- **dataset partagé en `allAuthenticatedUsers`** : sans gravité ici (données
  publiques), mais chaque requête d'un tiers consommerait le quota du projet.

**Ce qui limite déjà le risque :**
- **BigQuery Sandbox** : aucun compte de facturation lié
  (`gcloud billing projects describe` → `billingEnabled: false`). Un
  identifiant volé ne peut pas générer de coût, car les quotas gratuits sont un
  plafond dur et non un seuil de facturation. Les tables expirent après 60 jours.
- **ADC (Application Default Credentials)** : le code ne lit aucune clé
  (`bigquery.Client(project=...)` seulement). Les credentials restent dans le
  profil gcloud de l'utilisateur, hors du repo.
- **`.gitignore`** : `.env`, `service-account-key.json` et `*.json` sont exclus.
- **Contrôle automatisé** (`scripts/check_secrets.py`, voir plus bas) : aucune
  credential dans les fichiers suivis ni dans l'historique git.

### Audit de l'existant (2026-09-24)

`python iam/iam_setup.py audit` (lecture seule) sur `mini-siem-taxi-pipeline` :

```
[audit] ADC credential    : authorized_user
[audit] pipeline identity : user:<compte propriétaire du projet>
[audit] project roles     : ['roles/owner']
[audit] nyc_taxi ACL      : ['OWNER'] (+ inherited via projectOwners/projectWriters special groups)
[audit] service accounts  : none
[audit] FINDING: pipeline identity holds project admin role(s) ['roles/owner']: ...
[audit] FINDING: pipeline identity is OWNER of dataset nyc_taxi: ...
```

**Constat : oui, le pipeline a plus de droits que nécessaire.** Il n'existe
aucun service account : le pipeline tourne via ADC avec le compte utilisateur
propriétaire du projet (`roles/owner`). Il pourrait donc modifier l'IAM,
supprimer n'importe quel dataset ou activer n'importe quelle API, alors qu'il
n'a besoin que de lancer des jobs BigQuery et de lire/écrire 3 tables de
`nyc_taxi`. Par ailleurs, `.env.example` pointait
`GOOGLE_APPLICATION_CREDENTIALS` vers un fichier de clé JSON, en contradiction
avec l'usage ADC. Cette ligne est retirée.

### Politique cible : moindre privilège

Détail et justification permission par permission : [`iam/iam_policy.yaml`](iam/iam_policy.yaml).

| Principal | Rôle | Portée | Pourquoi |
|---|---|---|---|
| SA `taxi-pipeline` | rôle custom `taxiPipelineJobRunner` : **`bigquery.jobs.create` seulement** ([`iam/custom_role_job_runner.yaml`](iam/custom_role_job_runner.yaml)) | projet | Lancer un job est la seule action BigQuery qui ne peut pas être limitée à un dataset. Plus étroit que `roles/bigquery.jobUser`. |
| SA `taxi-pipeline` | `roles/bigquery.dataEditor` (stocké `WRITER` dans l'ACL du dataset) | dataset `nyc_taxi` **uniquement** | Chargement `WRITE_TRUNCATE` + `CREATE OR REPLACE TABLE`. Rôle prédéfini le plus restreint qui le permet. Aucun accès aux autres datasets. |
| opérateur humain | `roles/iam.serviceAccountTokenCreator` | ce SA uniquement | Emprunter l'identité du SA (tokens courts), sans clé. |

Ce que le SA **ne peut pas** faire : modifier l'IAM, créer ou supprimer un
dataset, modifier l'ACL de `nyc_taxi`, lire un autre dataset, activer des API.
**Aucune clé JSON n'est créée** : l'accès passe par l'impersonation, ce qui
supprime le risque « clé commitée ».

Choix assumé : la portée dataset utilise le rôle prédéfini `dataEditor`
plutôt qu'un rôle custom, car la liste d'accès d'un dataset BigQuery (champ
`access`, client Python, `bq update`) n'accepte que des rôles prédéfinis.
`dataEditor` inclut aussi `tables.delete`, que le pipeline n'utilise pas :
c'est l'écart résiduel par rapport au strict minimum.

Le compte propriétaire garde `roles/owner` pour les tâches d'administration
ponctuelles (`bq mk`, `bq rm`, activation d'API) et pour Looker Studio, qui
interroge BigQuery avec les identifiants de la personne qui consulte le
rapport. La différence : le pipeline ne s'exécute plus avec ces droits.

### Appliquer la politique

```bash
# 1. Voir exactement ce qui sera fait (ne modifie rien)
python iam/iam_setup.py plan --operator <votre-email>

# 2. Appliquer (compte owner) : active iam + iamcredentials, crée le rôle
#    custom et le SA, pose les 3 bindings. Ré-exécutable (skip si existant).
python iam/iam_setup.py apply --operator <votre-email>

# 3. Faire tourner le pipeline sous l'identité du SA (aucun changement de code)
gcloud auth application-default login --impersonate-service-account=taxi-pipeline@<PROJECT_ID>.iam.gserviceaccount.com

# 4. Vérifier : doit afficher "OK: pipeline identity has no admin role" (exit 0)
python iam/iam_setup.py audit
```

Équivalent manuel : `plan` affiche les commandes `gcloud` une par une.
Pour revenir à l'ancien mode : `gcloud auth application-default login`
(sans `--impersonate-service-account`).

**Appliqué et vérifié le 2026-09-24** sur `mini-siem-taxi-pipeline` :
- état IAM relu après `apply` : rôle `taxiPipelineJobRunner` =
  `[bigquery.jobs.create]` ; au niveau projet, le SA n'a que ce rôle ; ACL
  `nyc_taxi` : `WRITER` pour le SA ; `serviceAccountTokenCreator` sur le SA
  pour l'opérateur ; `gcloud iam service-accounts keys list --managed-by=user`
  → 0 clé ;
- pipeline exécuté **sous l'identité du SA** (impersonation), code inchangé :
  chargement `yellow_tripdata_raw` (3 475 226 lignes), `sql/01`, `sql/02`
  (`trips_clean` : 3 253 304 lignes), `sql/03` (`daily_kpis` : 33 jours), tous OK ;
- tests négatifs sous le SA, tous refusés (`403 Forbidden`) : créer un autre
  dataset (API et `CREATE SCHEMA`), modifier l'ACL de `nyc_taxi` ;
- ADC basculé sur le SA, puis `python iam/iam_setup.py audit` (exit 0) :

```
[audit] ADC credential    : impersonated_service_account
[audit] pipeline identity : serviceAccount:taxi-pipeline@mini-siem-taxi-pipeline.iam.gserviceaccount.com
[audit] project roles     : ['projects/mini-siem-taxi-pipeline/roles/taxiPipelineJobRunner']
[audit] nyc_taxi ACL      : ['WRITER'] (+ inherited via projectOwners/projectWriters special groups)
[audit] service accounts  : ['taxi-pipeline@mini-siem-taxi-pipeline.iam.gserviceaccount.com']
[audit] OK: pipeline identity has no admin role
```

### Vérification anti-fuite de credentials

`scripts/check_secrets.py` scanne les fichiers suivis ou indexés par git, et
tout l'historique avec `--history`. Il détecte les clés JSON de service
account, les refresh tokens ADC, les clés privées PEM et les clés API
Google/AWS, ainsi que les fichiers interdits (`.env`,
`service-account-key.json`, `application_default_credentials.json`). Code de
sortie 1 si quelque chose est trouvé.

```bash
python scripts/check_secrets.py --history      # [ok] no credentials found (working tree + history)
python -m unittest discover -s tests -v        # 9 tests, dont un qui lance le scan --history sur ce repo
```

En hook git local (bloque le commit si une credential est indexée) :

```bash
printf '#!/bin/sh\npython scripts/check_secrets.py\n' > .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Nettoyage (important)

Pour éviter tout coût résiduel une fois le projet terminé :

```bash
bq rm -r -f <PROJECT_ID>:nyc_taxi   # supprime le dataset et toutes ses tables
```

Le dataset Sandbox expire de toute façon automatiquement après 60 jours
d'inactivité, mais autant le supprimer explicitement dès que le portfolio
n'en a plus besoin.
