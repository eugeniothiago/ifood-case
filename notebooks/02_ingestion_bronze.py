# Databricks notebook source
"""Download the case inputs and append their raw records to Bronze."""

# COMMAND ----------

from pathlib import Path

from src.bronze import ingest_to_bronze
from src.config import PipelineConfig
from src.ingestion import download_taxi_data

# Set USE_COMMUNITY_EDITION to True when running in a two-level Hive Metastore workspace.
USE_COMMUNITY_EDITION = False
config = (
    PipelineConfig.community_edition()
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)

# COMMAND ----------

landing_paths = download_taxi_data(config.year, config.months, config.landing_path)
file_uris = [
    path if path.startswith(("dbfs:", "file:", "s3:", "abfss:")) else Path(path).resolve().as_uri()
    for path in landing_paths
]

# COMMAND ----------

bronze_df = ingest_to_bronze(spark, file_uris, config.bronze_table)
print(f"Bronze table: {config.bronze_table}")
print(f"Input files: {len(file_uris)}")
print(f"Bronze rows in this append: {bronze_df.count():,}")

# COMMAND ----------

for path in landing_paths:
    print(f"{path}: {Path(path).stat().st_size:,} bytes")

# COMMAND ----------

# The source schema is intentionally retained in Bronze; quality filtering belongs
# to Silver after the EDA notebook has profiled the untouched data.
bronze_df.printSchema()
