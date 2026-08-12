"""Monthly average taxi amount analysis over the Gold consumption table."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def answer_q1(spark: SparkSession, gold_table: str) -> DataFrame:
    """Return average total amount and trip count for each pickup month.

    The calculation uses the persisted Gold table and keeps the aggregation
    distributed until the caller displays or writes the result.
    """
    if not gold_table.strip():
        raise ValueError("gold_table cannot be empty")

    return (
        spark.read.table(gold_table)
        .withColumn(
            "pickup_month",
            F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM"),
        )
        .groupBy("pickup_month")
        .agg(
            F.avg("total_amount").alias("average_total_amount"),
            F.count(F.lit(1)).alias("trip_count"),
        )
        .orderBy("pickup_month")
    )


def answer_q1_with_optimization(spark: SparkSession, gold_table: str) -> DataFrame:
    """Return Q1 with explicit Spark optimization demonstrations.

    ``autoBroadcastJoinThreshold`` is set explicitly even though this query has
    no join; it documents the threshold that would allow Spark to broadcast a
    small dimension in a related query. AQE can then coalesce shuffle partitions
    and adapt join/exchange decisions from runtime statistics. The repartition
    hint (the SQL equivalent is ``/*+ REPARTITION(8) */``) makes the aggregation
    parallelism controlled and reproducible for this case-sized workload.
    """
    if not gold_table.strip():
        raise ValueError("gold_table cannot be empty")

    # Keep these settings local to the current Spark session. The threshold is
    # the 10 MiB broadcast default, made explicit for code-review visibility.
    spark.sql("SET spark.sql.autoBroadcastJoinThreshold = 10485760")
    # AQE adapts shuffle partition sizes and physical plans after stage statistics
    # are available, avoiding a one-size-fits-all static execution plan.
    spark.sql("SET spark.sql.adaptive.enabled = true")

    # DataFrame hint equivalent to the SQL hint: SELECT /*+ REPARTITION(8) */ ... FROM gold.
    return (
        spark.read.table(gold_table)
        .hint("REPARTITION", 8)
        .withColumn(
            "pickup_month",
            F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM"),
        )
        .groupBy("pickup_month")
        .agg(
            F.avg("total_amount").alias("average_total_amount"),
            F.count(F.lit(1)).alias("trip_count"),
        )
        .orderBy("pickup_month")
    )
