# Databricks notebook source
"""Standalone Databricks notebook for the two Gold-layer case analyses."""

# COMMAND ----------

# MAGIC %md
# MAGIC # NYC Taxi analyses
# MAGIC
# MAGIC Q1 and Q2 execute against an existing Gold table. The optimized variants
# MAGIC keep the same result contract while making Spark execution choices explicit.

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

from pyspark.sql import functions as F

from analysis.q1_monthly_avg_amount import answer_q1, answer_q1_with_optimization
from analysis.q2_avg_passengers_per_hour import answer_q2, answer_q2_with_optimization
from src.config import PipelineConfig

USE_COMMUNITY_EDITION = True
config = PipelineConfig.community_edition() if USE_COMMUNITY_EDITION else PipelineConfig()
GOLD_TABLE = config.gold_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 — Average total_amount per month
# MAGIC
# MAGIC Standard and optimized versions. The optimized version enables AQE
# MAGIC (Adaptive Query Execution) and uses a REPARTITION hint for controlled parallelism.

# COMMAND ----------

print("Q1 — Standard")
display(answer_q1(spark, GOLD_TABLE))

# COMMAND ----------

print("Q1 — Optimized (AQE + REPARTITION)")
display(answer_q1_with_optimization(spark, GOLD_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 — Average passenger_count per hour of day (May 2023)
# MAGIC
# MAGIC Standard and optimized versions. The optimized version demonstrates
# MAGIC partition pruning (skips ~120 of ~151 daily partitions), broadcast join
# MAGIC with an inline vendor dimension, and AQE.

# COMMAND ----------

print("Q2 — Standard")
display(answer_q2(spark, GOLD_TABLE))

# COMMAND ----------

print("Q2 — Optimized (partition pruning + broadcast join + AQE)")
display(answer_q2_with_optimization(spark, GOLD_TABLE))
