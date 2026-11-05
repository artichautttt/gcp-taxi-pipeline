#!/usr/bin/env Rscript
# KPI récurrents du pipeline NYC Taxi, calculés dans BigQuery (Sandbox) avec
# bigrquery, mis en forme avec dplyr et stringr, exportés en CSV agrégés.
#
#   Rscript analysis/kpi_bigquery.R --check-auth     # un appel de métadonnées, gratuit
#   Rscript analysis/kpi_bigquery.R --dry-run-only   # octets prévus par requête, rien n'est exécuté
#   Rscript analysis/kpi_bigquery.R                  # KPI + recoupement -> analysis/output/*.csv
#
# Variables d'environnement (lues aussi dans .env à la racine, ignoré par git) :
#   GCP_PROJECT_ID     projet BigQuery (facturation : Sandbox, désactivée)
#   TAXI_PIPELINE_SA   compte de service dont on emprunte l'identité
#   BQ_BRONZE_DATASET, BQ_SILVER_DATASET, BQ_GOLD_DATASET (défauts : ceux de dbt)
#
# Authentification : aucune clé. Un jeton d'accès court est demandé à
# `gcloud auth print-access-token --impersonate-service-account=...` ; il reste
# en mémoire, n'est jamais affiché ni écrit. gargle 1.7.0 ne lit pas les ADC de
# type impersonated_service_account, d'où ce passage par gcloud.
#
# Coût : chaque requête passe d'abord en dry-run (gratuit) et s'arrête si elle
# lirait plus de MAX_BYTES ; le job porte aussi maximumBytesBilled = MAX_BYTES,
# donc BigQuery refuse la requête plutôt que de facturer au-delà.

suppressPackageStartupMessages({
  library(dplyr)
  library(bigrquery)
  library(stringr)
})

MAX_BYTES <- 100 * 1024^2 # 100 Mio = 104 857 600 octets par requête

# Comptes publiés (README, docs/qualite.md) : le recoupement doit les retrouver.
PUBLISHED <- c(bronze_rows = 11198026, silver_trips = 10381226, quarantine_rows = 816800)

# --- configuration ------------------------------------------------------------

script_path <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
repo_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/")
env_file <- file.path(repo_root, ".env")
if (file.exists(env_file)) readRenviron(env_file)

env <- function(name, default = "") {
  value <- Sys.getenv(name, default)
  if (!nzchar(value)) stop("variable d'environnement manquante : ", name, call. = FALSE)
  value
}

args <- commandArgs(trailingOnly = TRUE)
mode <- if ("--check-auth" %in% args) "check-auth" else if ("--dry-run-only" %in% args) "dry-run" else "run"

project <- env("GCP_PROJECT_ID")
service_account <- env("TAXI_PIPELINE_SA")
if (!str_detect(service_account, "^[a-z0-9-]+@[a-z0-9-]+\\.iam\\.gserviceaccount\\.com$")) {
  stop("TAXI_PIPELINE_SA n'est pas une adresse de compte de service", call. = FALSE)
}
datasets <- list(
  bronze = env("BQ_BRONZE_DATASET", "nyc_taxi_bronze"),
  silver = env("BQ_SILVER_DATASET", "nyc_taxi_silver"),
  gold = env("BQ_GOLD_DATASET", "nyc_taxi_gold")
)
tbl <- function(layer, name) sprintf("`%s.%s.%s`", project, datasets[[layer]], name)
out_dir <- file.path(repo_root, "analysis", "output")

# --- authentification ---------------------------------------------------------

impersonated_token <- function(sa) {
  gcloud <- if (.Platform$OS.type == "windows") "gcloud.cmd" else "gcloud"
  # stderr jeté (avertissement d'impersonation de gcloud) ; stdout gardé en mémoire.
  out <- suppressWarnings(system2(
    gcloud, c("auth", "print-access-token", paste0("--impersonate-service-account=", sa)),
    stdout = TRUE, stderr = FALSE
  ))
  status <- attr(out, "status")
  if ((!is.null(status) && status != 0) || length(out) == 0) {
    # le message ne reprend jamais la sortie de gcloud
    stop("gcloud n'a pas fourni de jeton (code ", if (is.null(status)) "?" else status, ")", call. = FALSE)
  }
  httr::Token2.0$new(
    # application factice : le jeton vient de gcloud, httr ne fait aucun flux OAuth
    app = httr::oauth_app("gcloud-impersonation", key = "gcloud", secret = ""),
    endpoint = httr::oauth_endpoints("google"),
    params = list(as_header = TRUE),
    credentials = list(access_token = out[length(out)]),
    cache_path = FALSE # jamais de cache sur disque
  )
}

bq_auth(token = impersonated_token(service_account))

if (mode == "check-auth") {
  # tables.get : lecture de métadonnées, aucun job, aucun octet facturé
  meta <- bq_table_meta(bq_table(project, datasets$silver, "trips"), fields = "numRows")
  cat("auth ok : métadonnées lisibles,", datasets$silver, ".trips numRows =", meta$numRows, "\n")
  quit(status = 0)
}

# --- exécution plafonnée --------------------------------------------------------

run_log <- tibble(query = character(), dry_run_bytes = numeric(), bytes_billed = numeric(), rows = integer())

run_query <- function(name, sql) {
  dry_bytes <- as.numeric(bq_perform_query_dry_run(sql, billing = project))
  if (dry_bytes > MAX_BYTES) {
    stop(sprintf("%s : %s octets prévus, au-delà du plafond de %s", name, dry_bytes, MAX_BYTES), call. = FALSE)
  }
  if (mode == "dry-run") {
    cat(sprintf("%-28s dry-run %12.0f octets\n", name, dry_bytes))
    return(NULL)
  }
  job <- bq_perform_query(
    sql, billing = project,
    configuration = list(query = list(
      maximum_bytes_billed = format(MAX_BYTES, scientific = FALSE),
      use_query_cache = FALSE # octets mesurés reproductibles
    ))
  )
  bq_job_wait(job, quiet = TRUE)
  meta <- bq_job_meta(job)
  dest <- meta$configuration$query$destinationTable
  data <- bq_table_download(bq_table(dest$projectId, dest$datasetId, dest$tableId),
                            api = "json", quiet = TRUE) # tabledata.list, gratuit
  billed <- as.numeric(meta$statistics$query$totalBytesBilled)
  run_log <<- add_row(run_log, query = name, dry_run_bytes = dry_bytes,
                      bytes_billed = billed, rows = nrow(data))
  cat(sprintf("%-28s dry-run %12.0f octets | facturés %12.0f | %d lignes\n",
              name, dry_bytes, billed, nrow(data)))
  data
}

month_label <- function(yyyymm) {
  x <- as.character(yyyymm)
  str_c(str_sub(x, 1, 4), "-", str_sub(x, 5, 6))
}

write_out <- function(data, file) {
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  utils::write.table(data, file.path(out_dir, file), sep = ",", row.names = FALSE,
                     qmethod = "double", fileEncoding = "UTF-8", eol = "\n")
}

# --- KPI ------------------------------------------------------------------------

# K1 (CTE) : taux de rejet par mois et par règle, rapporté aux lignes Bronze du mois.
k1_sql <- sprintf("
with bronze_by_month as (
  select source_month, sum(row_count) as bronze_rows
  from %s
  group by source_month
),
rejected as (
  select source_month, rejection_reason, count(*) as rejected_rows
  from %s
  group by source_month, rejection_reason
)
select r.source_month, r.rejection_reason, r.rejected_rows, b.bronze_rows
from rejected as r
join bronze_by_month as b using (source_month)
order by r.source_month, r.rejected_rows desc",
  tbl("silver", "audit_trip_rows"), tbl("silver", "trips_quarantine"))

# K2 : volume, chiffre d'affaires et trajets en zone inconnue par mois.
k2_sql <- sprintf("
select pickup_month, count(*) as days, sum(trip_count) as trips,
       sum(revenue_usd) as revenue_usd, sum(trips_unknown_zone) as trips_unknown_zone
from %s
group by pickup_month
order by pickup_month", tbl("gold", "agg_daily_kpis"))

# K3 (CTE + fenêtre) : 10 premières zones de prise en charge par mois et leur part.
k3_sql <- sprintf("
with zone_month as (
  select a.pickup_month, a.zone_id, z.borough, z.zone_name, a.pickups,
         sum(a.pickups) over (partition by a.pickup_month) as month_pickups,
         rank() over (partition by a.pickup_month order by a.pickups desc) as pickup_rank
  from %s as a
  join %s as z using (zone_id)
)
select pickup_month, pickup_rank, zone_id, borough, zone_name, pickups, month_pickups
from zone_month
where pickup_rank <= 10
order by pickup_month, pickup_rank",
  tbl("gold", "agg_zone_monthly"), tbl("gold", "dim_zones"))

# K4 : zones en alerte du contrôle TLC, par mois et par borough.
k4_sql <- sprintf("
select month, coalesce(borough, 'non rattachée') as borough, count(*) as zones,
       countif(is_alert) as zones_in_alert,
       sum(tlc_pickups) as tlc_pickups, sum(bronze_pickups) as bronze_pickups
from %s
group by month, borough
order by month, borough", tbl("gold", "tlc_zone_reconciliation"))

# K5 (CTE) : profil par jour de la semaine (moyenne par jour calendaire observé).
k5_sql <- sprintf("
with days as (
  select k.pickup_month, d.weekday_name, extract(dayofweek from k.pickup_date) as weekday_num,
         k.trip_count, k.revenue_usd
  from %s as k
  join %s as d on k.pickup_date = d.date_day
)
select pickup_month, weekday_num, weekday_name, count(*) as days,
       sum(trip_count) as trips, sum(revenue_usd) as revenue_usd
from days
group by pickup_month, weekday_num, weekday_name
order by pickup_month, weekday_num",
  tbl("gold", "agg_daily_kpis"), tbl("gold", "dim_date"))

# Recoupement avec les comptes publiés (COUNT(*) sur une table : 0 octet lu).
check_sql <- sprintf("
select
  (select sum(row_count) from %1$s) as audit_rows,
  (select sum(row_count) from %1$s where row_outcome = 'kept') as audit_kept,
  (select count(*) from %2$s) as silver_trips,
  (select count(*) from %3$s) as quarantine_rows,
  (select sum(trip_count) from %4$s) as gold_daily_trips",
  tbl("silver", "audit_trip_rows"), tbl("silver", "trips"),
  tbl("silver", "trips_quarantine"), tbl("gold", "agg_daily_kpis"))

k1 <- run_query("K1_rejets_par_regle", k1_sql)
k2 <- run_query("K2_volume_mensuel", k2_sql)
k3 <- run_query("K3_top_zones", k3_sql)
k4 <- run_query("K4_alertes_tlc", k4_sql)
k5 <- run_query("K5_profil_jour_semaine", k5_sql)
chk <- run_query("recoupement", check_sql)

if (mode == "dry-run") quit(status = 0)

# --- mise en forme (dplyr, stringr) et exports agrégés ---------------------------

k1 <- k1 |>
  mutate(month = month_label(source_month),
         rule_family = case_when(
           str_starts(rejection_reason, "duplicate") ~ "doublon",
           str_starts(rejection_reason, "unknown_") ~ "code ou zone inconnu",
           str_starts(rejection_reason, "invalid_") ~ "montant ou distance invalide",
           .default = "horodatage ou durée"
         ),
         rejected_pct = round(100 * rejected_rows / bronze_rows, 4)) |>
  select(month, rejection_reason, rule_family, rejected_rows, bronze_rows, rejected_pct)

k2 <- k2 |>
  mutate(month = month_label(pickup_month),
         revenue_usd = round(revenue_usd, 2),
         trips_per_day = round(trips / days, 1),
         unknown_zone_pct = round(100 * trips_unknown_zone / trips, 3)) |>
  select(month, days, trips, trips_per_day, revenue_usd, trips_unknown_zone, unknown_zone_pct)

k3 <- k3 |>
  mutate(month = month_label(pickup_month),
         zone = str_c(zone_name, " (", borough, ")"),
         share_pct = round(100 * pickups / month_pickups, 2)) |>
  select(month, pickup_rank, zone_id, zone, pickups, share_pct)

k4 <- k4 |>
  mutate(month = month_label(month)) |>
  select(month, borough, zones, zones_in_alert, tlc_pickups, bronze_pickups)

k5 <- k5 |>
  mutate(month = month_label(pickup_month),
         trips_per_day = round(trips / days, 1),
         revenue_per_day_usd = round(revenue_usd / days, 2)) |>
  select(month, weekday_num, weekday_name, days, trips_per_day, revenue_per_day_usd)

# Bronze par les métadonnées des tables mensuelles (tables.get, gratuit).
bronze_tables <- bq_dataset_tables(bq_dataset(project, datasets$bronze))
bronze_ids <- vapply(bronze_tables, function(t) t$table, character(1))
bronze_ids <- sort(bronze_ids[str_detect(bronze_ids, "^yellow_tripdata_\\d{6}$")])
bronze_rows <- sum(vapply(bronze_ids, function(id) {
  as.numeric(bq_table_nrow(bq_table(project, datasets$bronze, id)))
}, numeric(1)))

reconciliation <- tibble(
  check = c("bronze (métadonnées, tables yellow_tripdata_AAAAMM)",
            "bronze (somme de audit_trip_rows)",
            "silver.trips (COUNT)",
            "silver.trips = kept de l'audit",
            "quarantaine (COUNT)",
            "silver + quarantaine",
            "gold.agg_daily_kpis (somme trip_count)"),
  measured = c(bronze_rows, chk$audit_rows, chk$silver_trips, chk$audit_kept,
               chk$quarantine_rows, chk$silver_trips + chk$quarantine_rows, chk$gold_daily_trips),
  published = PUBLISHED[c("bronze_rows", "bronze_rows", "silver_trips", "silver_trips",
                          "quarantine_rows", "bronze_rows", "silver_trips")]
) |>
  mutate(match = measured == published)

write_out(k1, "k1_rejets_par_regle.csv")
write_out(k2, "k2_volume_mensuel.csv")
write_out(k3, "k3_top_zones.csv")
write_out(k4, "k4_alertes_tlc.csv")
write_out(k5, "k5_profil_jour_semaine.csv")
write_out(reconciliation, "recoupement.csv")
write_out(run_log, "run_log.csv")

cat("\nmois Bronze :", str_c(str_remove(bronze_ids, "yellow_tripdata_"), collapse = ", "), "\n")
print(as.data.frame(reconciliation), row.names = FALSE)
cat(sprintf("\noctets facturés au total : %.0f (plafond %s par requête)\n",
            sum(run_log$bytes_billed), format(MAX_BYTES, scientific = FALSE)))
if (!all(reconciliation$match)) {
  stop("recoupement : un compte diffère des comptes publiés (voir analysis/output/recoupement.csv)", call. = FALSE)
}
cat("recoupement : tous les comptes égaux aux comptes publiés\n")
