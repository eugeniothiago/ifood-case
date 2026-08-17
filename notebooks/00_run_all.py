# Databricks notebook source
"""One-click, idempotent execution of the complete NYC Taxi pipeline."""

# COMMAND ----------

import os
import shutil
import sys

from pyspark.sql import functions as F

_CANDIDATE_ROOTS = [
    "/Workspace/Users/thiagoace1@gmail.com/ifood-case",
    "/Workspace/ifood-case",
    os.getcwd(),
]
for _root in _CANDIDATE_ROOTS:
    if os.path.isdir(os.path.join(_root, "src")):
        if _root not in sys.path:
            sys.path.insert(0, _root)
        break

from analysis.q1_monthly_avg_amount import answer_q1, answer_q1_with_optimization
from analysis.q2_avg_passengers_per_hour import answer_q2, answer_q2_with_optimization
from src.bronze import create_schemas, drop_table_if_exists, ingest_to_bronze
from src.config import DBFS_LANDING_PATH, PipelineConfig, taxi_file_urls
from src.delta_optimizations import run_all_optimizations
from src.gold import get_gold_summary, model_gold
from src.ingestion import download_taxi_data
from src.silver import get_silver_summary, transform_to_silver

USE_COMMUNITY_EDITION = True
config = (
    PipelineConfig.community_edition()
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)


def section_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

# COMMAND ----------

# Step 0: Create schemas, drop old tables, clear landing zone.
section_header("STEP 0 - CLEANUP")

# Create Hive databases (bronze, silver, gold) before any table operations.
# Community Edition does not auto-create schemas when using saveAsTable.
create_schemas(spark, config.all_schemas)
print(f"Ensured schemas exist: {config.all_schemas}")

for table_name in (config.gold_table, config.silver_table, config.bronze_table):
    drop_table_if_exists(spark, table_name)
    print(f"Dropped table if present: {table_name}")

# Clear DBFS landing zone.
try:
    dbutils.fs.rm(DBFS_LANDING_PATH, True)
except Exception:
    pass
try:
    dbutils.fs.mkdirs(DBFS_LANDING_PATH)
except Exception:
    pass
print(f"Cleared DBFS landing zone: {DBFS_LANDING_PATH}")
display(spark.createDataFrame([("cleanup", "completed")], ["step", "status"]))

# COMMAND ----------

# Step 1: download files and build Bronze.
section_header("STEP 1 - INGESTION + BRONZE")

# Strategy 1: dbutils.fs.cp from HTTP URL to DBFS.
# The JVM HTTP client resolves DNS independently from Python and can download
# from CloudFront URLs that fail with requests/wget/curl.
_ingestion_method = None
try:
    for month in config.months:
        filename = f"yellow_tripdata_{config.year:04d}-{month:02d}.parquet"
        dest = f"{DBFS_LANDING_PATH}/{filename}"
        downloaded = False
        for url in taxi_file_urls(config.year, month):
            try:
                dbutils.fs.cp(url, dest, True)
                downloaded = True
                break
            except Exception as url_err:
                print(f"  dbutils.fs.cp failed for {url}: {url_err}")
        if not downloaded:
            raise RuntimeError(f"All URLs failed for {filename}")
        print(f"  Downloaded {filename} via dbutils.fs.cp")

    file_uris = [
        f"{DBFS_LANDING_PATH}/yellow_tripdata_{config.year:04d}-{m:02d}.parquet"
        for m in config.months
    ]
    bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
    _ingestion_method = "dbutils.fs.cp -> DBFS"
except Exception as e1:
    print(f"  dbutils.fs.cp strategy failed: {e1}")
    print("  Falling back to local download (requests -> urllib -> wget -> curl)...")
    # Strategy 2: Python download to /tmp, then copy to DBFS via dbutils.fs.cp.
    try:
        local_paths = download_taxi_data(config.year, config.months, "/tmp/nyc_taxi/landing")
        # Copy local files to DBFS so Spark can read them.
        for local_path in local_paths:
            filename = os.path.basename(local_path)
            dbutils.fs.cp(f"file:{local_path}", f"{DBFS_LANDING_PATH}/{filename}", True)
        file_uris = [
            f"{DBFS_LANDING_PATH}/yellow_tripdata_{config.year:04d}-{m:02d}.parquet"
            for m in config.months
        ]
        bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
        _ingestion_method = "local download -> DBFS copy"
    except Exception as e2:
        raise RuntimeError(
            f"All ingestion methods failed:\n"
            f"  dbutils.fs.cp: {e1}\n"
            f"  local download: {e2}"
        )

print(f"Ingestion method: {_ingestion_method}")
bronze_count = spark.read.table(config.bronze_table).count()
print(f"\nBronze table '{config.bronze_table}': {bronze_count:,} rows")
display(spark.read.table(config.bronze_table).groupBy("VendorID").count().orderBy("VendorID"))

# COMMAND ----------

# Step 2: EDA on untouched Bronze.
section_header("STEP 2 - EXPLORATORY DATA ANALYSIS")
bronze_table_df = spark.read.table(config.bronze_table)
eda_findings = bronze_table_df.select(
    F.count(F.lit(1)).alias("bronze_rows"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
    F.sum(F.col("total_amount") < 0).alias("negative_amount_rows"),
    F.sum(F.col("passenger_count") <= 0).alias("non_positive_passenger_rows"),
)
display(eda_findings)
display(
    bronze_table_df.groupBy(
        F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month")
    )
    .count()
    .orderBy("pickup_month")
)
print("EDA complete.")

# COMMAND ----------

# Step 3: Silver + Data Quality.
section_header("STEP 3 - SILVER + DATA QUALITY")
silver_df = transform_to_silver(spark, config.bronze_table, config.silver_table)
silver_summary = get_silver_summary(spark, config.bronze_table, config.silver_table)
print(f"Bronze rows: {silver_summary['bronze_rows']:,}")
print(f"Silver rows: {silver_summary['silver_rows']:,}")
print(f"Rows rejected by DQ: {silver_summary['dropped_rows']:,}")
display(spark.read.table(config.silver_table).limit(20))

# COMMAND ----------

# Step 4: Gold.
section_header("STEP 4 - GOLD")
gold_df = model_gold(spark, config.silver_table, config.gold_table)
gold_count = spark.read.table(config.gold_table).count()
print(f"Gold table '{config.gold_table}': {gold_count:,} rows")
display(get_gold_summary(spark, config.gold_table))

# COMMAND ----------

# Step 5: Delta Optimizations.
section_header("STEP 5 - DELTA OPTIMIZATIONS")
run_all_optimizations(spark, config.gold_table)
display(spark.sql(f"DESCRIBE HISTORY {config.gold_table}").limit(10))

# COMMAND ----------

# Step 6: Analysis.
section_header("STEP 6 - ANALYSIS")
q1_standard = answer_q1(spark, config.gold_table)
q1_optimized = answer_q1_with_optimization(spark, config.gold_table)
q2_standard = answer_q2(spark, config.gold_table)
q2_optimized = answer_q2_with_optimization(spark, config.gold_table)
print("Q1 - monthly average total amount (standard)")
display(q1_standard)
print("Q1 - monthly average total amount (optimized)")
display(q1_optimized)
print("Q2 - May hourly average passengers (standard)")
display(q2_standard)
print("Q2 - May hourly average passengers (optimized)")
display(q2_optimized)

# COMMAND ----------

# Summary.
section_header("SUMMARY")
summary_df = spark.createDataFrame(
    [
        ("bronze", config.bronze_table, spark.read.table(config.bronze_table).count()),
        ("silver", config.silver_table, spark.read.table(config.silver_table).count()),
        ("gold", config.gold_table, spark.read.table(config.gold_table).count()),
    ],
    ["layer", "table_name", "row_count"],
)
display(summary_df)
print("PIPELINE STATUS: SUCCESS")