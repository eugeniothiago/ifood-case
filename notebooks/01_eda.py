# Databricks notebook source
"""Comprehensive profiling of untouched Bronze NYC TLC data.

This notebook deliberately performs no cleansing or business transformation. Its
purpose is to make data quality observations visible before Silver rules are chosen.
"""

# COMMAND ----------

from pyspark.sql import functions as F

from src.config import PipelineConfig

USE_COMMUNITY_EDITION = False
config = PipelineConfig.community_edition() if USE_COMMUNITY_EDITION else PipelineConfig()
bronze_df = spark.read.table(config.bronze_table)

# COMMAND ----------

# Schema inspection verifies that Bronze preserves the source contract plus lineage.
bronze_df.printSchema()
display(
    spark.createDataFrame(
        [(field.name, field.dataType.simpleString(), field.nullable) for field in bronze_df.schema],
        ["column_name", "data_type", "nullable"],
    )
)

# COMMAND ----------

# Counts by month and vendor reveal source coverage and whether any vendor dominates.
print(f"Total Bronze rows: {bronze_df.count():,}")
rows_per_month = bronze_df.groupBy(F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month")).count().orderBy("pickup_month")
rows_per_vendor = bronze_df.groupBy("VendorID").count().orderBy(F.desc("count"))
display(rows_per_month)
display(rows_per_vendor)

# COMMAND ----------

# Null rates identify fields requiring nullable handling or exclusion downstream.
total_rows = bronze_df.count()
null_rates = bronze_df.select(
    [
        F.sum(F.col(column).isNull().cast("long")).alias(f"{column}__nulls")
        for column in bronze_df.columns
    ]
).first()
null_rows = [
    (column, int(null_rates[f"{column}__nulls"] or 0), (int(null_rates[f"{column}__nulls"] or 0) / total_rows if total_rows else 0.0))
    for column in bronze_df.columns
]
null_analysis = spark.createDataFrame(null_rows, ["column_name", "null_count", "null_percentage"]).orderBy(F.desc("null_percentage"))
display(null_analysis)
print("Columns with high null rates (over 20%):")
display(null_analysis.filter(F.col("null_percentage") > 0.20))

# COMMAND ----------

# Numeric summaries expose skew, impossible values, and candidate outlier thresholds.
numeric_columns = [
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "Airport_fee",
]
display(bronze_df.select(numeric_columns).describe())
display(bronze_df.select(numeric_columns).summary("count", "min", "25%", "50%", "75%", "95%", "99%", "max"))

# COMMAND ----------

# Vendor frequencies establish whether vendor-specific behavior should be modeled.
vendor_distribution = bronze_df.groupBy("VendorID").agg(
    F.count(F.lit(1)).alias("trip_count"),
    F.round(F.avg("total_amount"), 2).alias("average_total_amount"),
).orderBy(F.desc("trip_count"))
display(vendor_distribution)

# COMMAND ----------

# Passenger anomalies are kept visible here; Silver can quarantine or document them.
passenger_distribution = bronze_df.groupBy("passenger_count").count().orderBy("passenger_count")
display(passenger_distribution)
print(f"Zero-passenger rows: {bronze_df.filter(F.col('passenger_count') == 0).count():,}")
display(bronze_df.filter(F.col("passenger_count") > 8).groupBy("passenger_count").count().orderBy(F.desc("passenger_count")))

# COMMAND ----------

# Amount percentiles and tail counts inform robust aggregations and outlier treatment.
amount_percentiles = bronze_df.select(
    F.min("total_amount").alias("min_total_amount"),
    F.max("total_amount").alias("max_total_amount"),
    F.expr("percentile_approx(total_amount, array(0.25, 0.50, 0.75, 0.95, 0.99), 10000)").alias("percentiles"),
)
display(amount_percentiles)
print(f"Negative total_amount rows: {bronze_df.filter(F.col('total_amount') < 0).count():,}")
print(f"Extreme total_amount rows (> $1,000): {bronze_df.filter(F.col('total_amount') > 1000).count():,}")
display(bronze_df.filter(F.col("total_amount") > 1000).select("VendorID", "total_amount", "tpep_pickup_datetime").orderBy(F.desc("total_amount")).limit(100))

# COMMAND ----------

# Date range checks detect late-arriving, future, and out-of-scope records.
date_range = bronze_df.select(
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
    F.min("tpep_dropoff_datetime").alias("min_dropoff"),
    F.max("tpep_dropoff_datetime").alias("max_dropoff"),
)
display(date_range)
print(f"Pickups outside Jan-May 2023: {bronze_df.filter((F.col('tpep_pickup_datetime') < '2023-01-01') | (F.col('tpep_pickup_datetime') >= '2023-06-01')).count():,}")
print(f"Future pickup dates: {bronze_df.filter(F.col('tpep_pickup_datetime') > F.current_timestamp()).count():,}")

# COMMAND ----------

# Explicit issue counts provide an auditable baseline for Silver expectations.
dq_issue_summary = spark.createDataFrame(
    [
        ("reversed_or_equal_timestamps", bronze_df.filter(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")).count()),
        ("negative_total_amount", bronze_df.filter(F.col("total_amount") < 0).count()),
        ("zero_passengers", bronze_df.filter(F.col("passenger_count") <= 0).count()),
        ("null_required_fields", bronze_df.filter(sum(F.col(c).isNull().cast("int") for c in ["VendorID", "passenger_count", "total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"]) > 0).count()),
    ],
    ["issue", "row_count"],
)
display(dq_issue_summary)

# COMMAND ----------

# Vendor-to-amount analysis checks whether mean values are driven by vendor mix.
display(
    bronze_df.groupBy("VendorID").agg(
        F.count("total_amount").alias("amount_observations"),
        F.round(F.avg("total_amount"), 2).alias("mean_total_amount"),
        F.round(F.expr("percentile_approx(total_amount, 0.50)"), 2).alias("median_total_amount"),
    ).orderBy("VendorID")
)

# COMMAND ----------

# Daily trip volumes reveal seasonality, gaps, and the appropriate daily partition key.
trips_per_day = bronze_df.groupBy(F.to_date("tpep_pickup_datetime").alias("pickup_date")).count().orderBy("pickup_date")
display(trips_per_day)
display(trips_per_day.select(F.min("count").alias("min_daily_trips"), F.max("count").alias("max_daily_trips"), F.avg("count").alias("avg_daily_trips")))

# COMMAND ----------

# EDA summary: these observations must be reviewed before applying Silver rules.
# The notebook intentionally does not assert hard-coded counts because source files
# can be refreshed. The displayed tables are the evidence used for the decisions.
display(
    spark.createDataFrame(
        [
            ("Bronze remains raw", "Preserve all 19 source columns and lineage for replay and audit."),
            ("Required fields are explicit", "Silver should validate the five case columns before publishing consumption data."),
            ("Invalid values are observable", "Use named quality predicates and quarantine rows instead of silently dropping them."),
            ("Partitioning is daily", "Gold should derive pickup_date and partition by day for date pruning."),
            ("Query optimization", "Use predicate pushdown on pickup_date and broadcast only genuinely small dimensions."),
            ("Delta operations", "Document OPTIMIZE/ZORDER/VACUUM and MERGE policies in the project README."),
        ],
        ["finding", "decision"],
    )
)
