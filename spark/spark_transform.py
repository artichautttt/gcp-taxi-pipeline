"""
Alternative transformation path for the NYC Yellow Taxi pipeline, using
PySpark DataFrame API in local mode (no cluster, no managed Spark service).

Reproduces, on the same local Parquet file used by ingestion/load_to_bigquery.py,
the same 3-stage logic already implemented in SQL against BigQuery:
  1. data quality checks   (sql/01_data_quality_checks.sql)
  2. cleaned trips table   (sql/02_transform_trips_clean.sql)
  3. daily KPI aggregates  (sql/03_analytics_daily_kpis.sql)

See README.md ("PySpark local") for why this exists and how to run it.
"""
import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "spark_output"

# Same English weekday names as BigQuery's FORMAT_DATE('%A', ...), indexed the
# way Spark's dayofweek() returns them (1 = Sunday ... 7 = Saturday) so the
# result doesn't depend on the JVM's default locale.
WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]


def build_spark_session(app_name: str = "nyc-taxi-transform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Default 1g driver heap OOMs when collecting ~3.25M rows via
        # toPandas() for the final local write (see run()).
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def run_quality_checks(raw: DataFrame) -> None:
    """Mirrors sql/01_data_quality_checks.sql: quantify anomalies, don't fix them here."""
    total_rows = raw.count()

    nulls = raw.select(
        F.count(F.when(F.col("tpep_pickup_datetime").isNull(), 1)).alias("null_pickup_datetime"),
        F.count(F.when(F.col("tpep_dropoff_datetime").isNull(), 1)).alias("null_dropoff_datetime"),
        F.count(F.when(F.col("trip_distance").isNull(), 1)).alias("null_trip_distance"),
        F.count(F.when(F.col("fare_amount").isNull(), 1)).alias("null_fare_amount"),
        F.count(F.when(F.col("PULocationID").isNull(), 1)).alias("null_pu_location"),
        F.count(F.when(F.col("DOLocationID").isNull(), 1)).alias("null_do_location"),
    ).first()

    distinct_rows = raw.distinct().count()

    # BigQuery's TIMESTAMP_DIFF(..., HOUR) truncates to whole hours, so this
    # must compare a truncated hour count too, to flag the exact same rows as
    # sql/01_data_quality_checks.sql (and filter the same ones in build_trips_clean).
    duration_hours_trunc = F.floor(
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
        / 3600.0
    )
    business = raw.select(
        F.count(F.when(F.col("trip_distance") < 0, 1)).alias("negative_distance"),
        F.count(F.when(F.col("trip_distance") > 200, 1)).alias("distance_over_200mi"),
        F.count(F.when(F.col("fare_amount") < 0, 1)).alias("negative_fare"),
        F.count(F.when(F.col("fare_amount") > 1000, 1)).alias("fare_over_1000usd"),
        F.count(F.when(F.col("passenger_count") == 0, 1)).alias("zero_passengers"),
        F.count(F.when(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"), 1)).alias(
            "dropoff_before_pickup"
        ),
        F.count(F.when(duration_hours_trunc > 6, 1)).alias("trip_over_6h"),
        F.count(F.when(~F.year("tpep_pickup_datetime").isin(2024, 2025), 1)).alias(
            "pickup_outside_expected_year"
        ),
    ).first()

    print(f"[quality] total_rows={total_rows:,} distinct_rows={distinct_rows:,} "
          f"duplicate_rows={total_rows - distinct_rows:,}")
    print(f"[quality] nulls: {nulls.asDict()}")
    print(f"[quality] business rules: {business.asDict()}")


def build_trips_clean(raw: DataFrame) -> DataFrame:
    """Mirrors sql/02_transform_trips_clean.sql: filter + derived columns."""
    duration_seconds = F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp(
        "tpep_pickup_datetime"
    )
    duration_min = duration_seconds / 60.0
    duration_hours_trunc = F.floor(duration_seconds / 3600.0)
    avg_speed_mph = F.when(duration_min > 0, F.col("trip_distance") / (duration_min / 60.0))

    clean = (
        raw.withColumn("trip_duration_min", duration_min)
        .withColumn("avg_speed_mph", avg_speed_mph)
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn(
            "pickup_weekday",
            F.element_at(F.array(*[F.lit(day) for day in WEEKDAYS]), F.dayofweek("tpep_pickup_datetime")),
        )
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        .filter(duration_hours_trunc <= 6)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") <= 200)
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("fare_amount") <= 1000)
        .filter(F.year("tpep_pickup_datetime").isin(2024, 2025))
    )

    return clean.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_duration_min",
        "passenger_count",
        "trip_distance",
        "avg_speed_mph",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "pickup_date",
        "pickup_hour",
        "pickup_weekday",
    )


def build_daily_kpis(clean: DataFrame) -> DataFrame:
    """Mirrors sql/03_analytics_daily_kpis.sql: daily aggregates for the dashboard."""
    tip_pct = F.when(F.col("fare_amount") != 0, F.col("tip_amount") / F.col("fare_amount"))

    return (
        clean.groupBy("pickup_date", "pickup_weekday")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount_usd"),
            F.round(F.sum("total_amount"), 2).alias("revenue_usd"),
            F.round(F.avg(tip_pct) * 100, 1).alias("avg_tip_pct"),
        )
        .orderBy("pickup_date")
    )


def run(source_file: Path, output_dir: Path) -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(f"[read] {source_file}")
        raw = spark.read.parquet(str(source_file))

        run_quality_checks(raw)

        print("[transform] building trips_clean")
        clean = build_trips_clean(raw).cache()

        print("[transform] building daily_kpis")
        daily_kpis = build_daily_kpis(clean)

        output_dir.mkdir(parents=True, exist_ok=True)
        clean_path = output_dir / "trips_clean.parquet"
        kpis_path = output_dir / "daily_kpis.parquet"

        # Collected to the driver and written with pandas/pyarrow rather than
        # DataFrame.write.parquet(): Spark's Hadoop-based writer needs
        # winutils.exe on Windows for local filesystem output, an extra native
        # dependency not worth pulling in for a local demo of this size.
        print(f"[write] {clean_path}")
        clean_pdf = clean.toPandas()
        clean_pdf.to_parquet(clean_path, index=False)
        print(f"[transform] trips_clean: {len(clean_pdf):,} rows kept")

        print(f"[write] {kpis_path}")
        daily_kpis.toPandas().to_parquet(kpis_path, index=False)

        print("[done] PySpark transformation complete")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform NYC taxi Parquet data with PySpark (local[*], no cluster)"
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=DATA_DIR / "yellow_tripdata_2025-01.parquet",
        help="Path to local parquet file (same one loaded into BigQuery)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Directory to write trips_clean.parquet and daily_kpis.parquet",
    )
    args = parser.parse_args()

    run(args.file, args.output_dir)
