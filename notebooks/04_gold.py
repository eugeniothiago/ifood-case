# Databricks notebook source
"""Build, inspect, and maintain the daily-partitioned Gold Delta table."""

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
from src.delta_optimizations import run_all_optimizations
from src.gold import get_gold_summary, model_gold

USE_COMMUNITY_EDITION = True
config = (
create_schemas(spark, config.all_schemas)
    PipelineConfig.community_edition()
    if USE_COMMUNITY_EDITION
    else PipelineConfig()
)
SILVER_TABLE = config.silver_table
GOLD_TABLE = config.gold_table

# COMMAND ----------

gold_df = model_gold(spark, SILVER_TABLE, GOLD_TABLE)
gold_count = spark.read.table(GOLD_TABLE).count()
print(f"Gold table '{GOLD_TABLE}': {gold_count:,} rows")

# COMMAND ----------

display(get_gold_summary(spark, GOLD_TABLE))

# COMMAND ----------

# Delta optimizations: compact files, z-order by VendorID, vacuum old files.
run_all_optimizations(spark, GOLD_TABLE)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {GOLD_TABLE}").limit(10))