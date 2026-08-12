# Databricks notebook source
"""Build the typed, quality-filtered Silver Delta table."""

# COMMAND ----------

from src.config import PipelineConfig
from src.silver import get_silver_summary, transform_to_silver

# Set this to True for Databricks Community Edition's two-level Hive Metastore.
USE_COMMUNITY_EDITION = False
config = (
    PipelineConfig.community_edition()
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)
BRONZE_TABLE = config.bronze_table
SILVER_TABLE = config.silver_table

# COMMAND ----------

silver_df = transform_to_silver(spark, BRONZE_TABLE, SILVER_TABLE)
summary = get_silver_summary(spark, BRONZE_TABLE, SILVER_TABLE)
print(f"Bronze table: {BRONZE_TABLE}")
print(f"Silver table: {SILVER_TABLE}")
print(f"Bronze row count: {summary['bronze_rows']:,}")
print(f"Silver row count: {summary['silver_rows']:,}")
print(f"Rows dropped by data quality rules: {summary['dropped_rows']:,}")

# COMMAND ----------

# A small sample verifies the published Silver shape without collecting the table.
display(spark.read.table(SILVER_TABLE).limit(20))
