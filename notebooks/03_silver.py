# Databricks notebook source
"""Build the typed, quality-filtered Silver Delta table."""

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

from src.bronze import create_schemas
from src.config import PipelineConfig
from src.silver import get_silver_summary, transform_to_silver

USE_COMMUNITY_EDITION = True
config = (
create_schemas(spark, config.all_schemas)
    PipelineConfig.community_edition()
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)
BRONZE_TABLE = config.bronze_table
SILVER_TABLE = config.silver_table

# COMMAND ----------

silver_df = transform_to_silver(spark, BRONZE_TABLE, SILVER_TABLE)
silver_summary = get_silver_summary(spark, BRONZE_TABLE, SILVER_TABLE)

print(f"Bronze rows: {silver_summary['bronze_rows']:,}")
print(f"Silver rows: {silver_summary['silver_rows']:,}")
print(f"Rows rejected by DQ rules: {silver_summary['dropped_rows']:,}")

# COMMAND ----------

display(spark.read.table(SILVER_TABLE).limit(20))

# COMMAND ----------

spark.read.table(SILVER_TABLE).printSchema()