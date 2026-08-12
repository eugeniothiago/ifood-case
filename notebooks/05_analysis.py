# Databricks notebook source
"""Standalone Databricks notebook for the two Gold-layer case analyses."""

# COMMAND ----------

# MAGIC %md
# MAGIC # NYC Taxi analyses
# MAGIC
# MAGIC Q1 and Q2 execute against an existing Gold table. The optimized variants
# MAGIC keep the same result contract while making Spark execution choices explicit.

# COMMAND ----------

import sys
from pathlib import Path

from pyspark.sql import functions as F

for import_path in (Path.cwd() / "src", Path.cwd() / "analysis", Path.cwd()):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from analysis.q1_monthly_avg_amount import answer_q1, answer_q1_with_optimization
from analysis.q2_avg_passengers_per_hour import answer_q2, answer_q2_with_optimization
from src.config import PipelineConfig

USE_COMMUNITY_EDITION = True
config = PipelineConfig.community_edition() if USE_COMMUNITY_EDITION else PipelineConfig()
GOLD_TABLE = config.gold_table
print(f"Reading Gold table: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 — monthly average total amount
# MAGIC
# MAGIC Q1 derives `yyyy-MM`, then averages `total_amount` by month. The optimized
# MAGIC version enables AQE and makes the repartition hint and broadcast threshold visible.

# COMMAND ----------

q1_standard = answer_q1(spark, GOLD_TABLE).withColumn("query_variant", F.lit("standard"))
q1_optimized = answer_q1_with_optimization(spark, GOLD_TABLE).withColumn(
    "query_variant", F.lit("optimized")
)
q1_results = q1_standard.unionByName(q1_optimized).select(
    "pickup_month", "average_total_amount", "trip_count", "query_variant"
).orderBy("pickup_month", "query_variant")
print("Q1 — standard and optimized results")
display(q1_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 — May hourly average passengers
# MAGIC
# MAGIC Q2 applies timestamp bounds and the Gold `pickup_date` partition bounds.
# MAGIC This enables partition pruning and predicate pushdown. The optimized version
# MAGIC additionally broadcasts a tiny inline `VendorID` lookup and enables AQE.

# COMMAND ----------

q2_standard = answer_q2(spark, GOLD_TABLE).withColumn("query_variant", F.lit("standard"))
q2_optimized = answer_q2_with_optimization(spark, GOLD_TABLE).withColumn(
    "query_variant", F.lit("optimized")
)
q2_results = q2_standard.unionByName(q2_optimized).select(
    "pickup_hour", "average_passenger_count", "trip_count", "query_variant"
).orderBy("pickup_hour", "query_variant")
print("Q2 — standard and optimized results")
display(q2_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Physical plan inspection
# MAGIC
# MAGIC The optimized plan should expose the May partition filter, a broadcast hash
# MAGIC join, and an adaptive plan when those operators are supported by the runtime.

# COMMAND ----------

print("Q2 optimized physical plan")
q2_optimized.explain(mode="formatted")
