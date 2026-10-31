# Règles de qualité

Chaque ligne Bronze de `yellow_tripdata_AAAAMM` finit à un seul endroit :
`nyc_taxi_silver.trips` si elle respecte toutes les règles de rejet, sinon
`nyc_taxi_silver.trips_quarantine` avec **une** raison, la première règle
enfreinte dans l'ordre ci-dessous. Les règles sont écrites dans
`dbt/models/silver/int_trips_classified.sql`.

Trois niveaux :
- **rejet** (16 règles) : la ligne va en quarantaine, telle que reçue ;
- **alerte** (3 indicateurs) : la ligne reste en Silver, avec un booléen
  `flag_*` compté dans le rapport de qualité ;
- **contrôle externe** (XML TLC) : comparaison par zone avec les comptes
  publiés par la TLC, non bloquante.

Les chiffres de ce document sont mesurés le 2026-10-31 sur les trois mois
chargés (runs du DAG `nyc_taxi_elt`), lus dans `nyc_taxi_silver.audit_trip_rows`
et `nyc_taxi_gold.tlc_zone_reconciliation`. Ils restent ici après l'expiration
des tables.

## Règles de rejet

Ordre d'évaluation. Les règles 1 à 7 et 15 viennent de l'ancien schéma en
étoile (8 règles) ; les règles 8 à 14 et 16 sont ajoutées (8 règles). Les
nouvelles règles de contenu passent après les anciennes, donc les comptes des
anciennes règles ne bougent pas.

| # | Motif (`rejection_reason`) | Condition | Justification |
|---|---|---|---|
| 1 | `missing_timestamp` | pickup ou dropoff nul | sans horodatage, ni durée, ni mois, ni partition |
| 2 | `pickup_outside_file_month` | mois du pickup ≠ mois du fichier | garde les partitions disjointes : le fichier de janvier contient quelques trajets de décembre, qui ne doivent pas écraser décembre à la relance |
| 3 | `dropoff_before_pickup` | dropoff < pickup | durée négative, physiquement impossible |
| 4 | `duration_over_6h` | durée > 6 h | au-delà, compteur oublié allumé (seuil repris de la chaîne historique `sql/02`) |
| 5 | `invalid_distance` | distance nulle, ≤ 0 ou > 200 mi | 0 mi = course non effectuée ; > 200 mi dépasse tout trajet plausible depuis New York (seuil historique) |
| 6 | `invalid_fare` | tarif nul, < 0 ou > 1 000 $ | un tarif négatif n'est pas une course facturée (le dictionnaire TLC ne le documente pas) ; > 1 000 $ aberrant (seuil historique) |
| 7 | `invalid_total` | total nul ou < tarif + pourboire | le total payé ne peut pas être inférieur à ses deux plus gros composants. Remplace le test « total = somme des composants », faux sur 2 564 195 lignes brutes de janvier à cause de la façon dont la TLC remplit le fichier (voir README) |
| 8 | `unknown_zone` | zone de pickup ou de dropoff nulle ou absente du référentiel (265 zones) | sans zone, pas de jointure avec `dim_zones` |
| 9 | `unknown_payment_type` | code de paiement absent du référentiel TLC (0 à 6) | code non documenté |
| 10 | `unknown_rate_code` | code tarifaire non nul et absent du référentiel | code non documenté ; un code **nul** est gardé et ramené à 99 (« Null/unknown » du dictionnaire TLC, courses Flex Fare) |
| 11 | `unknown_vendor` | fournisseur nul ou hors {1, 2, 6, 7} | seuls fournisseurs du dictionnaire TLC 2025 |
| 12 | `zero_duration` | dropoff = pickup | durée nulle avec une distance > 0 (règle 5 déjà passée) : enregistrement incohérent, vitesse infinie |
| 13 | `implausible_speed` | vitesse moyenne > 100 mph | aucune course urbaine n'atteint 100 mph de moyenne ; horodatage ou distance faux |
| 14 | `negative_component` | une surtaxe, taxe, pourboire, péage ou frais < 0 | un composant de paiement négatif contredit un tarif positif (règle 6 déjà passée) |
| 15 | `duplicate` | copie exacte d'une autre ligne (même `trip_id`) | une course chargée deux fois ; la première copie est gardée |
| 16 | `duplicate_business_key` | même fournisseur, horodatages, zones, distance et total qu'une autre ligne | doublon qui ne diffère que par un champ secondaire ; la première ligne est gardée |

Les deux règles de doublon passent en dernier : un doublon qui enfreint aussi
une autre règle est compté sous cette autre règle.

### Mesures par mois (2026-10-31)

| Motif | 2025-01 | 2025-02 | 2025-03 | Total | Part des lignes Bronze |
|---|---:|---:|---:|---:|---:|
| `invalid_fare` | 129 637 | 166 096 | 191 923 | 487 656 | 4,355 % |
| `invalid_distance` | 90 994 | 99 896 | 103 877 | 294 767 | 2,632 % |
| `zero_duration` | 1 232 | 4 403 | 21 428 | 27 063 | 0,242 % |
| `duration_over_6h` | 1 203 | 1 133 | 1 373 | 3 709 | 0,033 % |
| `implausible_speed` | 764 | 611 | 758 | 2 133 | 0,019 % |
| `invalid_total` | 225 | 268 | 284 | 777 | 0,007 % |
| `negative_component` | 101 | 90 | 120 | 311 | 0,003 % |
| `dropoff_before_pickup` | 124 | 93 | 81 | 298 | 0,003 % |
| `pickup_outside_file_month` | 22 | 31 | 33 | 86 | 0,001 % |
| les 7 autres règles | 0 | 0 | 0 | 0 | 0 % |
| **Quarantaine** | **224 302** | **272 621** | **319 877** | **816 800** | **7,29 %** |
| Lignes Bronze | 3 475 226 | 3 577 543 | 4 145 257 | 11 198 026 | |
| Lignes Silver (`trips`) | 3 250 924 | 3 304 922 | 3 825 380 | 10 381 226 | |
| Taux de rejet du mois | 6,45 % | 7,62 % | 7,72 % | 7,29 % | |

Règles à 0 sur les trois mois : `missing_timestamp`, `unknown_zone`,
`unknown_payment_type`, `unknown_rate_code`, `unknown_vendor`, `duplicate`,
`duplicate_business_key`. Elles restent comme garde-fous pour un fichier
futur. Leur motif fait partie des `accepted_values` testés, mais aucune n'a de
cas de test dédié avec une ligne fautive : limite connue.

`zero_duration` passe de 1 232 (janvier) à 21 428 (mars) : mesuré, non
expliqué par la TLC.

## Règles d'alerte (lignes gardées)

| Indicateur | Condition | Pourquoi une alerte et pas un rejet | 2025-01 | 2025-02 | 2025-03 |
|---|---|---|---:|---:|---:|
| `flag_unknown_zone` | zone 264 ou 265 (« Unknown » / « N/A » TLC) | la course a eu lieu et a été payée ; seule sa localisation manque | 25 485 | 22 419 | 24 987 |
| `flag_passenger_count` | passagers nul, 0 ou > 6 | saisi par le chauffeur, souvent absent ; n'invalide ni le trajet ni le montant | 436 475 | 649 069 | 747 163 |
| `flag_short_trip` | durée < 60 s | possible (course annulée facturée), mais à surveiller | 8 839 | 8 452 | 10 790 |

Ces comptes figurent dans `nyc_taxi_silver.quality_report`
(`outcome_kind = 'warning'`).

## Contrôles de réconciliation (bloquants)

`quality/quality_gate.py` fait échouer le run (et écrit quand même le
rapport, `gate_status = failed`) si une égalité n'est pas tenue :

| Contrôle | Égalité |
|---|---|
| Source = Bronze | lignes lues dans le Parquet = lignes de `yellow_tripdata_AAAAMM` (aussi vérifié à l'ingestion, `BronzeCountError`) |
| Bronze = audit | lignes Bronze = somme de `audit_trip_rows` du mois |
| Gardées = Silver | `kept` de l'audit = lignes du mois dans `silver.trips` |
| Rejets = quarantaine | pour chaque motif, audit = `trips_quarantine` |
| Silver + quarantaine = Bronze | les deux sorties couvrent toute l'entrée |
| Quarantaine du run | toutes les lignes de quarantaine du mois portent le `run_id` du run |

Les mêmes égalités existent en tests dbt (`dbt/tests/`), plus deux tests
qui revérifient les règles sur Silver (`silver_trips_respect_trip_rules`,
`silver_trips_total_covers_fare_and_tip`). Le 2026-10-31, les quatre runs
(janvier deux fois, février, mars) ont passé le contrôle : dbt PASS = 60,
WARN = 1 (contrôle XML ci-dessous), ERROR = 0, sur 61 tests.

## Contrôle externe : comptes publiés par la TLC (XML)

Source : NYC Open Data, jeu `c5iv-bn4s` (TLC, trajets par zone et par mois),
téléchargé en XML par `ingestion/tlc_zone_trips_xml.py`. Seuls les
pick-up Yellow Taxi servent. Modèle : `nyc_taxi_gold.tlc_zone_reconciliation`
(zone × mois).

**Alerte** (`is_alert`) si les deux conditions sont vraies :
- au moins `tlc_alert_min_trips` = **1 000** trajets d'un côté (TLC ou
  Bronze) ;
- écart Bronze − TLC supérieur à `tlc_alert_gap_pct` = **10 %** du compte
  TLC, ou zone absente côté TLC.

Le test `tlc_zone_reconciliation_alerts` a la sévérité `warn` : **il ne bloque
jamais le run.** La TLC publie ses comptes après son propre filtrage, que nous
ne connaissons pas ; un écart signale un sujet à regarder, pas une erreur du
pipeline.

**Justification des seuils** (mesure de l'audit, janvier 2025) :
- l'écart par zone est faible pour la plupart des zones : écart absolu médian
  de 1,59 % (médiane exacte, `percentile_cont`) sur les 103 zones d'au moins
  1 000 trajets que la TLC publie ;
- sous 1 000 trajets, quelques courses suffisent à faire un écart de plus de
  10 % : ces zones généreraient du bruit ;
- 10 % isole la queue de la distribution sans alerter sur l'écart habituel :
  26 de ces 103 zones le dépassent, plus les 2 zones absentes côté TLC, soit
  28 alertes.

**Résultats du 2026-10-31** :

| Mois | Zones comparées | Zones ≥ 1 000 trajets | Écart absolu médian (zones ≥ 1 000 publiées par la TLC) | Zones en alerte | Pick-ups TLC | Pick-ups Bronze du mois | Pick-ups Silver |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2025-01 | 261 | 105 | 1,59 % | 28 | 3 426 391 | 3 475 204 | 3 250 924 |
| 2025-02 | 257 | 121 | 2,31 % | 30 | 3 532 074 | 3 577 512 | 3 304 922 |
| 2025-03 | 260 | 145 | 3,45 % | 43 | 4 094 591 | 4 145 224 | 3 825 380 |

- Les zones 264 et 265 (« Unknown ») sont en alerte chaque mois : la TLC ne
  les publie pas, alors que le fichier en contient plus de 1 000 trajets. La
  zone 1 (EWR) est aussi absente côté TLC, mais sous le seuil (377, 314 et
  365 pick-ups Bronze).
- « Pick-ups Bronze du mois » exclut les trajets dont le pickup tombe hors du
  mois du fichier, d'où l'écart avec les lignes Bronze (22, 31 et 33 lignes).
- Le XML est révisé par la TLC : une relance ultérieure peut donner d'autres
  comptes côté TLC.

Requête de relecture :

```sql
select month, zone_id, zone_name, tlc_pickups, bronze_pickups, gap_bronze_pct
from `<PROJECT_ID>.nyc_taxi_gold.tlc_zone_reconciliation`
where is_alert
order by month, abs(gap_bronze) desc;
```

## Changer une règle

Voir [exploitation.md](exploitation.md#traiter-la-quarantaine) : modifier
`int_trips_classified.sql` et la liste `accepted_values` de `_silver.yml`,
documenter ici, relancer chaque mois concerné.
