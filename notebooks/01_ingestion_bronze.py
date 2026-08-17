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
from src.config import PipelineConfig, taxi_file_urls

USE_COMMUNITY_EDITION = True
config = (
    PipelineConfig.community_edition()
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)

LANDING = os.path.join(_root, "landing")

create_schemas(spark, config.all_schemas)
drop_table_if_exists(spark, config.bronze_table)

try:
    dbutils.fs.rm(LANDING, True)
except Exception:
    pass
dbutils.fs.mkdirs(LANDING)

# COMMAND ----------

for month in config.months:
    filename = f"yellow_tripdata_{config.year:04d}-{month:02d}.parquet"
    dest = f"{LANDING}/{filename}"
    for url in taxi_file_urls(config.year, month):
        try:
            dbutils.fs.cp(url, dest, True)
            print(f"  Downloaded {filename}")
            break
        except Exception as url_err:
            print(f"  Failed {url}: {url_err}")
    else:
        raise RuntimeError(f"All URLs failed for {filename}")

file_uris = [
    f"{LANDING}/yellow_tripdata_{config.year:04d}-{m:02d}.parquet"
    for m in config.months
]
bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
print("Ingestion method: dbutils.fs.cp -> /Workspace/landing")

# COMMAND ----------

bronze_count = spark.read.table(config.bronze_table).count()
print(f"Bronze table: {config.bronze_table}")
print(f"Bronze rows: {bronze_count:,}")

# COMMAND ----------

bronze_df.printSchema()
