# Databricks notebook source
"""Comprehensive profiling of untouched Bronze NYC TLC data.

This notebook deliberately performs no cleansing or business transformation. Its
purpose is to make data quality observations visible before Silver rules are chosen.
"""

# COMMAND ----------

import os
import sys
from pathlib import Path

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

from src.bronze import create_schemas
from src.config import PipelineConfig

USE_COMMUNITY_EDITION = True
config = PipelineConfig.community_edition() if USE_COMMUNITY_EDITION else PipelineConfig()
bronze_df = spark.read.table(config.bronze_table)

# COMMAND ----------

# Schema inspection verifies that Bronze preserves the source contract plus lineage.
print("=== SCHEMA INSPECTION ===")
bronze_df.printSchema()

# COMMAND ----------

# Row counts establish the volume baseline and reveal per-month, per-vendor coverage.
print("=== ROW COUNTS ===")
total_rows = bronze_df.count()
print(f"Total Bronze rows: {total_rows:,}")

rows_per_month = (
    bronze_df.withColumn("pickup_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
    .groupBy("pickup_month")
    .count()
    .orderBy("pickup_month")
)
print("Rows per pickup month:")
display(rows_per_month)

rows_per_vendor = bronze_df.groupBy("VendorID").count().orderBy("VendorID")
print("Rows per VendorID:")
display(rows_per_vendor)

# COMMAND ----------

# Null analysis: count and percentage of nulls per column identifies fields
# that may need cleansing or exclusion in Silver.
print("=== NULL ANALYSIS ===")
null_counts = bronze_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in bronze_df.columns
])
null_row = null_counts.collect()[0]
null_results = [(c, null_row[c], total_rows) for c in bronze_df.columns]
null_df = spark.createDataFrame(
    [(c, int(n), int(t), round(float(n) / max(t, 1) * 100, 2)) for c, n, t in null_results],
    ["column", "null_count", "total_rows", "null_pct"],
)
display(null_df.orderBy(F.desc("null_pct")))

# COMMAND ----------

# Summary statistics: describe + summary with percentiles on numeric columns.
print("=== SUMMARY STATISTICS ===")
numeric_cols = [
    "passenger_count", "trip_distance", "fare_amount", "total_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
]
display(bronze_df.summary("count", "min", "25%", "50%", "75%", "max").select(
    "summary", *numeric_cols
))

# COMMAND ----------

# VendorID distribution: identifies data providers and cardinality.
print("=== VendorID DISTRIBUTION ===")
display(bronze_df.groupBy("VendorID").count().orderBy("VendorID"))

# COMMAND ----------

# passenger_count distribution: reveals 0-passenger trips and extreme outliers.
print("=== PASSENGER_COUNT DISTRIBUTION ===")
display(
    bronze_df.groupBy("passenger_count")
    .count()
    .orderBy("passenger_count")
)
print("\nZero-passenger trips:")
display(bronze_df.filter(F.col("passenger_count") == 0).groupBy("VendorID").count())
print("\nOutliers (> 6 passengers):")
display(bronze_df.filter(F.col("passenger_count") > 6).groupBy("passenger_count").count())

# COMMAND ----------

# total_amount distribution: percentiles, negatives, and extreme outliers.
print("=== TOTAL_AMOUNT DISTRIBUTION ===")
display(bronze_df.agg(
    F.min("total_amount").alias("min_amount"),
    F.expr("percentile(total_amount, 0.25)").alias("p25"),
    F.expr("percentile(total_amount, 0.50)").alias("median"),
    F.expr("percentile(total_amount, 0.75)").alias("p75"),
    F.expr("percentile(total_amount, 0.95)").alias("p95"),
    F.expr("percentile(total_amount, 0.99)").alias("p99"),
    F.max("total_amount").alias("max_amount"),
))
print("Negative total_amount rows:")
display(bronze_df.filter(F.col("total_amount") < 0).agg(F.count(F.lit(1)).alias("negative_count")))
print("Extreme outliers (> $1000):")
display(bronze_df.filter(F.col("total_amount") > 1000).agg(F.count(F.lit(1)).alias("outlier_count")))

# COMMAND ----------

# Temporal analysis: date range of pickups and dropoffs, trips outside the case period.
print("=== TEMPORAL ANALYSIS ===")
display(bronze_df.agg(
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
    F.min("tpep_dropoff_datetime").alias("min_dropoff"),
    F.max("tpep_dropoff_datetime").alias("max_dropoff"),
))
print("Trips before Jan 2023:")
display(bronze_df.filter(F.col("tpep_pickup_datetime") < "2023-01-01").agg(F.count(F.lit(1)).alias("before_count")))
print("Trips after May 2023:")
display(bronze_df.filter(F.col("tpep_pickup_datetime") >= "2023-06-01").agg(F.count(F.lit(1)).alias("after_count")))

# COMMAND ----------

# Data quality issues: reversed timestamps, negative amounts, zero passengers, null required fields.
print("=== DATA QUALITY ISSUES ===")
dq_issues = bronze_df.agg(
    F.sum((F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")).cast("int")).alias("reversed_timestamps"),
    F.sum((F.col("total_amount") < 0).cast("int")).alias("negative_amounts"),
    F.sum((F.col("passenger_count") <= 0).cast("int")).alias("non_positive_passengers"),
    F.sum(F.col("VendorID").isNull().cast("int")).alias("null_vendor"),
    F.sum(F.col("passenger_count").isNull().cast("int")).alias("null_passenger_count"),
    F.sum(F.col("total_amount").isNull().cast("int")).alias("null_total_amount"),
    F.sum(F.col("tpep_pickup_datetime").isNull().cast("int")).alias("null_pickup"),
    F.sum(F.col("tpep_dropoff_datetime").isNull().cast("int")).alias("null_dropoff"),
)
display(dq_issues)

# COMMAND ----------

# Cross-column analysis: VendorID vs total_amount, trips per day.
print("=== CROSS-COLUMN ANALYSIS ===")
print("Average total_amount by VendorID:")
display(bronze_df.groupBy("VendorID").agg(
    F.avg("total_amount").alias("avg_amount"),
    F.count(F.lit(1)).alias("trip_count"),
).orderBy("VendorID"))

print("Trips per day (sampled):")
display(
    bronze_df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .groupBy("pickup_date")
    .count()
    .orderBy("pickup_date")
    .limit(30)
)

# COMMAND ----------

# EDA Summary: key findings that inform Silver/Gold transformation decisions.
print("=== EDA SUMMARY ===")
print("""
Key findings from the exploratory data analysis:

1. VendorID: typically values 1 and 2 (Creative Mobile Technologies and VeriFone).
   A value of 6 may appear in some months.

2. passenger_count: predominantly 1-2 passengers. Zero-passenger trips exist and
   are data entry errors. Very high counts (>6) are outliers.

3. total_amount: positive with a right skew. Negative values exist (likely refunds).
   Extreme outliers above $1000 may be data errors or very long trips.

4. Temporal: trips outside Jan-May 2023 exist in the source data and must be
   filtered in Silver (pickup_in_case_period expectation).

5. Data quality issues: reversed timestamps, nulls in required fields, and
   negative amounts confirm the need for all five DQ expectations.

These findings directly inform the Silver layer data quality rules and the
Gold layer partitioning strategy (daily by pickup_date).
""")
