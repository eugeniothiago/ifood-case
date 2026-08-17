# Databricks notebook source
"""Download the case inputs and append their raw records to Bronze."""

# COMMAND ----------

import os
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

from src.bronze import create_schemas, drop_table_if_exists, ingest_to_bronze
from src.config import DBFS_LANDING_PATH, PipelineConfig, taxi_file_urls
from src.ingestion import download_taxi_data

USE_COMMUNITY_EDITION = True
config = (
    PipelineConfig.community_edition()
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)

# Ensure schemas exist before any table operation.
create_schemas(spark, config.all_schemas)
drop_table_if_exists(spark, config.bronze_table)

# Clear DBFS landing zone.
try:
    dbutils.fs.rm(DBFS_LANDING_PATH, True)
except Exception:
    pass
try:
    dbutils.fs.mkdirs(DBFS_LANDING_PATH)
except Exception:
    pass

# COMMAND ----------

# Strategy 1: dbutils.fs.cp from HTTP URL to DBFS.
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
    try:
        local_paths = download_taxi_data(config.year, config.months, "/tmp/nyc_taxi/landing")
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

# COMMAND ----------

bronze_count = spark.read.table(config.bronze_table).count()
print(f"Bronze table: {config.bronze_table}")
print(f"Bronze rows: {bronze_count:,}")

# COMMAND ----------

bronze_df.printSchema()