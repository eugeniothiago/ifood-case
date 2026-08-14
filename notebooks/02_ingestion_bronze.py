# Databricks notebook source
"""Download the case inputs and append their raw records to Bronze."""

# COMMAND ----------

import os
import sys
from pathlib import Path

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

from src.bronze import ingest_to_bronze
from src.config import PipelineConfig, taxi_file_url
from src.ingestion import download_taxi_data
from src.schemas import RAW_SCHEMA

USE_COMMUNITY_EDITION = True
config = (
    PipelineConfig.community_edition(
        landing_path="/tmp/nyc_taxi/landing",
    )
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)

# COMMAND ----------

# Primary: download via Python (requests -> wget -> curl).
# Fallback: read directly from HTTP with Spark (JVM DNS resolver).
try:
    landing_paths = download_taxi_data(config.year, config.months, config.landing_path)
    file_uris = [
        path if path.startswith(("dbfs:", "file:", "s3:", "abfss:")) else Path(path).resolve().as_uri()
        for path in landing_paths
    ]
    bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
    print("Ingestion method: local download (requests/wget/curl)")
except Exception as download_error:
    print(f"  Local download failed: {download_error}")
    print("  Falling back to Spark direct HTTP read (JVM DNS resolver)...")
    http_urls = [taxi_file_url(config.year, m) for m in config.months]
    bronze_df = (
        spark.read.schema(RAW_SCHEMA)
        .parquet(*http_urls)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )
    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .saveAsTable(config.bronze_table)
    )
    print("Ingestion method: Spark direct HTTP read")

# COMMAND ----------

bronze_count = spark.read.table(config.bronze_table).count()
print(f"Bronze table: {config.bronze_table}")
print(f"Bronze rows: {bronze_count:,}")

# COMMAND ----------

bronze_df.printSchema()
