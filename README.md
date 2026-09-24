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
