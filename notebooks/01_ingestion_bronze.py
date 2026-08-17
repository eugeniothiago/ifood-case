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

# Community Edition: landing zone MUST be under /Workspace/ for Spark access.
# For dbutils.fs.cp we use dbfs:/tmp/ (DBFS, Spark can read it).
USE_COMMUNITY_EDITION = True
WORKSPACE_LANDING = os.path.join(_root, "landing")
DBFS_LANDING = "dbfs:/tmp/nyc_taxi/landing"
config = (
    PipelineConfig.community_edition(
        landing_path=WORKSPACE_LANDING,
    )
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)

# COMMAND ----------

# Strategy 1: dbutils.fs.cp from HTTP URL to DBFS.
# dbutils.fs uses the JVM HTTP client which resolves DNS independently from
# Python and can download from CloudFront URLs that fail with requests/wget/curl.
# Destination is dbfs:/tmp/nyc_taxi/landing/ (Spark can read from DBFS).
_ingestion_method = None
try:
    for month in config.months:
        filename = f"yellow_tripdata_{config.year:04d}-{month:02d}.parquet"
        dest = f"{DBFS_LANDING}/{filename}"
        downloaded = False
        for url in taxi_file_urls(config.year, month):
            try:
                dbutils.fs.cp(url, dest, True)  # True = overwrite
                downloaded = True
                break
            except Exception as url_err:
                print(f"  dbutils.fs.cp failed for {url}: {url_err}")
        if not downloaded:
            raise RuntimeError(f"All URLs failed for {filename}")
        print(f"  Downloaded {filename} via dbutils.fs.cp -> {dest}")

    file_uris = [
        f"{DBFS_LANDING}/yellow_tripdata_{config.year:04d}-{m:02d}.parquet"
        for m in config.months
    ]
    bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
    _ingestion_method = "dbutils.fs.cp -> DBFS"
except Exception as e1:
    print(f"  dbutils.fs.cp strategy failed: {e1}")
    print("  Falling back to local download (requests -> urllib -> wget -> curl)...")
    print(f"  Landing zone: {WORKSPACE_LANDING}")
    # Strategy 2: Python download to /Workspace/ path (Spark can read it).
    try:
        landing_paths = download_taxi_data(config.year, config.months, WORKSPACE_LANDING)
        # Do NOT convert to file:// URI - pass /Workspace/ path directly.
        # Community Edition Spark can read /Workspace/ paths but not file:///tmp/.
        file_uris = list(landing_paths)
        bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
        _ingestion_method = "local download -> /Workspace/"
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