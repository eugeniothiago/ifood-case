# Databricks notebook source
"""Build, inspect, and maintain the daily-partitioned Gold Delta table."""

# COMMAND ----------

from src.config import PipelineConfig
from src.delta_optimizations import run_all_optimizations
from src.gold import get_gold_summary, model_gold

# Set this to True for Databricks Community Edition's two-level Hive Metastore.
USE_COMMUNITY_EDITION = False
config = (
    PipelineConfig.community_edition()
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)
SILVER_TABLE = config.silver_table
GOLD_TABLE = config.gold_table

# COMMAND ----------

gold_df = model_gold(spark, SILVER_TABLE, GOLD_TABLE)
print(f"Silver table: {SILVER_TABLE}")
print(f"Gold table: {GOLD_TABLE}")
print(f"Gold row count: {gold_df.count():,}")

# COMMAND ----------

# The physical and logical key is a DateType daily pickup_date, not a month string.
partition_distribution = get_gold_summary(spark, GOLD_TABLE)
display(partition_distribution)

# COMMAND ----------

# Each operation reports elapsed time and the final history count in the notebook log.
run_all_optimizations(spark, GOLD_TABLE)
