# Databricks notebook source
"""One-click, idempotent execution of the complete NYC Taxi pipeline."""

# COMMAND ----------

# Step 0: make the repository's reusable modules importable in Databricks.
# This supports both a Databricks Repo checkout and a workspace-uploaded notebook.
import os
import shutil
import sys
from pathlib import Path

# Try common workspace locations so imports work regardless of how the
# notebook was loaded into Databricks.
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

from pyspark.sql import functions as F

from analysis.q1_monthly_avg_amount import answer_q1, answer_q1_with_optimization
from analysis.q2_avg_passengers_per_hour import answer_q2, answer_q2_with_optimization
from src.bronze import ingest_to_bronze
from src.config import PipelineConfig
from src.delta_optimizations import run_all_optimizations
from src.gold import get_gold_summary, model_gold
from src.ingestion import download_taxi_data
from src.silver import get_silver_summary, transform_to_silver

# Community Edition uses two-level Hive Metastore table names and the local
# /tmp directory on the driver node as the landing zone (DBFS FUSE mounts
# like /dbfs/ do not support POSIX mkdir in Community Edition).
USE_COMMUNITY_EDITION = True
config = (
    PipelineConfig.community_edition(
        landing_path="/tmp/nyc_taxi/landing",
    )
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)


def section_header(title: str) -> None:
    """Print a consistent section marker in the Databricks run output."""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


# COMMAND ----------

# Step 0: cleanup makes reruns deterministic and safe after a failed execution.
section_header("STEP 0 — CLEANUP")
for table_name in (config.gold_table, config.silver_table, config.bronze_table):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Dropped table if present: {table_name}")

# Remove old landing files and recreate the directory.
# Use os operations which work on the local /tmp filesystem.
landing_path = config.landing_path
if os.path.exists(landing_path):
    shutil.rmtree(landing_path, ignore_errors=True)
os.makedirs(landing_path, exist_ok=True)
print(f"Cleared landing zone: {landing_path}")
display(spark.createDataFrame([("cleanup", "completed")], ["step", "status"]))

# COMMAND ----------

# Step 1: download the five monthly Parquet files and publish raw Bronze records.
section_header("STEP 1 — INGESTION + BRONZE")
landing_paths = download_taxi_data(config.year, config.months, config.landing_path)
# Convert local filesystem paths to file:// URIs so Spark can read them.
file_uris = []
for path in landing_paths:
    if path.startswith(("dbfs:", "file:", "s3:", "abfss:")):
        file_uris.append(path)
    else:
        file_uris.append(Path(path).resolve().as_uri())

for uri in file_uris:
    size_mb = os.path.getsize(uri.replace("file://", "")) / (1024 * 1024)
    print(f"  {os.path.basename(uri)}: {size_mb:.1f} MB")

bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
bronze_count = spark.read.table(config.bronze_table).count()
print(f"\nBronze table '{config.bronze_table}': {bronze_count:,} rows")
display(bronze_df.groupBy("VendorID").count().orderBy("VendorID"))

# COMMAND ----------

# Step 2: run a compact EDA on untouched Bronze before quality filtering.
section_header("STEP 2 — EXPLORATORY DATA ANALYSIS")
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
print("EDA complete: source coverage, anomalies, and monthly volume are displayed above.")

# COMMAND ----------

# Step 3: cast the five required fields, apply named DQ rules, and publish Silver.
section_header("STEP 3 — SILVER + DATA QUALITY")
silver_df = transform_to_silver(spark, config.bronze_table, config.silver_table)
silver_summary = get_silver_summary(spark, config.bronze_table, config.silver_table)
print(f"Bronze rows: {silver_summary['bronze_rows']:,}")
print(f"Silver rows: {silver_summary['silver_rows']:,}")
print(f"Rows rejected by DQ: {silver_summary['dropped_rows']:,}")
display(spark.read.table(config.silver_table).limit(20))

# COMMAND ----------

# Step 4: derive the DateType pickup_date and publish daily-partitioned Gold.
section_header("STEP 4 — GOLD")
gold_df = model_gold(spark, config.silver_table, config.gold_table)
gold_count = spark.read.table(config.gold_table).count()
print(f"Gold table '{config.gold_table}': {gold_count:,} rows")
display(get_gold_summary(spark, config.gold_table))

# COMMAND ----------

# Step 5: compact files, cluster by VendorID, and vacuum obsolete Delta files.
section_header("STEP 5 — DELTA OPTIMIZATIONS")
run_all_optimizations(spark, config.gold_table)
display(spark.sql(f"DESCRIBE HISTORY {config.gold_table}").limit(10))

# COMMAND ----------

# Step 6: answer both business questions with standard and optimized plans.
section_header("STEP 6 — ANALYSIS")
q1_standard = answer_q1(spark, config.gold_table)
q1_optimized = answer_q1_with_optimization(spark, config.gold_table)
q2_standard = answer_q2(spark, config.gold_table)
q2_optimized = answer_q2_with_optimization(spark, config.gold_table)
print("Q1 — monthly average total amount (standard)")
display(q1_standard)
print("Q1 — monthly average total amount (optimized)")
display(q1_optimized)
print("Q2 — May hourly average passengers (standard)")
display(q2_standard)
print("Q2 — May hourly average passengers (optimized)")
display(q2_optimized)

# COMMAND ----------

# Summary: persisted-table counts and a final status make the one-click run auditable.
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
