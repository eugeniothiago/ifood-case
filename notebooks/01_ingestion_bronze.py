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
from src.config import PipelineConfig, taxi_file_urls
from src.ingestion import download_taxi_data

USE_COMMUNITY_EDITION = True
config = (
    PipelineConfig.community_edition(
        landing_path="/tmp/nyc_taxi/landing",
    )
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)

# COMMAND ----------

# Strategy 1: dbutils.fs.cp (Databricks JVM HTTP client).
# The JVM HTTP client resolves DNS independently from the Python process
# and can download from CloudFront URLs that fail with requests/wget/curl.
# This is the most reliable download method in Databricks Community Edition.
# We try both primary and fallback CloudFront domains for each file.
_ingestion_method = None
try:
    for month in config.months:
        filename = f"yellow_tripdata_{config.year:04d}-{month:02d}.parquet"
        dest = f"{config.landing_path}/{filename}"
        downloaded = False
        for url in taxi_file_urls(config.year, month):
            try:
                dbutils.fs.cp(url, dest, True)  # True = overwrite
                print(f"  Downloaded {filename} via dbutils.fs.cp")
                downloaded = True
                break
            except Exception as url_err:
                print(f"  dbutils.fs.cp failed for {url}: {url_err}")
        if not downloaded:
            raise RuntimeError(f"All URLs failed for {filename}")

    file_uris = [
        f"{config.landing_path}/yellow_tripdata_{config.year:04d}-{m:02d}.parquet"
        for m in config.months
    ]
    bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
    _ingestion_method = "dbutils.fs.cp (JVM HTTP client)"
except Exception as e1:
    print(f"  dbutils.fs.cp strategy failed: {e1}")
    print("  Falling back to local download (requests -> urllib -> wget -> curl)...")
    # Strategy 2: local download via Python (requests -> urllib -> wget -> curl).
    try:
        landing_paths = download_taxi_data(config.year, config.months, config.landing_path)
        file_uris = [
            path if path.startswith(("dbfs:", "file:", "s3:", "abfss:")) else Path(path).resolve().as_uri()
            for path in landing_paths
        ]
        bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
        _ingestion_method = "local download (requests/urllib/wget/curl)"
    except Exception as e2:
        raise RuntimeError(
            f"All ingestion methods failed:\n"
            f"  dbutils.fs.cp: {e1}\n"
            f"  local download: {e2}"
        )

print(f"Ingestion method: {_ingestion_method}")

# COMMAND ----------

bronze_count = spark.read.table(config.bronze_table).count()
print(f"Bronze table: {config.bronze_table}")
print(f"Bronze rows: {bronze_count:,}")

# COMMAND ----------

bronze_df.printSchema()
