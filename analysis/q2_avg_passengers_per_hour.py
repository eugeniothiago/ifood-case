"""May hourly passenger analysis over the daily-partitioned Gold table."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql import functions as F

MAY_START = "2023-05-01"
JUNE_START = "2023-06-01"


def _may_filter() -> Column:
    """Build an exact May 2023 filter, including the physical partition key."""
    pickup_timestamp = F.col("tpep_pickup_datetime")
    # Gold is partitioned by pickup_date. Keeping these bounds on that native
    # DateType column enables partition pruning: Spark skips about 120 of about
    # 151 Jan-May daily partitions and reads only the 31 May partitions.
    partition_filter = (
        (F.col("pickup_date") >= F.to_date(F.lit(MAY_START)))
        & (F.col("pickup_date") < F.to_date(F.lit(JUNE_START)))
    )
    # The timestamp predicate preserves the question's exact timestamp boundary
    # and is also eligible for Delta statistics/data skipping.
    timestamp_filter = (pickup_timestamp >= F.lit(MAY_START)) & (
        pickup_timestamp < F.lit(JUNE_START)
    )
    return partition_filter & timestamp_filter


def answer_q2(spark: SparkSession, gold_table: str) -> DataFrame:
    """Return average passenger count and trip count for each May pickup hour."""
    if not gold_table.strip():
        raise ValueError("gold_table cannot be empty")

    return (
        spark.read.table(gold_table)
        .filter(_may_filter())
        .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
        .groupBy("pickup_hour")
        .agg(
            F.avg("passenger_count").alias("average_passenger_count"),
            F.count(F.lit(1)).alias("trip_count"),
        )
        .orderBy("pickup_hour")
    )


def answer_q2_with_optimization(spark: SparkSession, gold_table: str) -> DataFrame:
    """Return Q2 with predicate pushdown, broadcast, and AQE demonstrations.

    The May bounds are applied immediately after reading Gold so Spark can push
    predicates into the Delta scan and prune the daily ``pickup_date``
    partitions. The inline vendor lookup is deliberately tiny and is joined with
    a broadcast hint (the SQL equivalent is ``/*+ BROADCAST */``), avoiding a
    shuffle for this dimension. The lookup is a demonstration only; it does not
    change the requested all-vendor population because the join is left-sided.
    """
    if not gold_table.strip():
        raise ValueError("gold_table cannot be empty")

    # AQE can coalesce post-filter shuffle partitions when May has fewer rows
    # than the full five-month input and can adjust the physical plan at runtime.
    spark.sql("SET spark.sql.adaptive.enabled = true")

    # This small inline dimension is safe to broadcast. F.broadcast is the
    # DataFrame equivalent of: /*+ BROADCAST(vendor_lookup) */.
    vendor_lookup = spark.createDataFrame(
        [(1, "Creative Mobile Technologies"), (2, "VeriFone"), (6, "Other")],
        ["VendorID", "vendor_name"],
    ).select(
        F.col("VendorID").cast("long").alias("VendorID"),
        F.col("vendor_name"),
    )

    return (
        spark.read.table(gold_table)
        # Predicate pushdown and partition pruning happen before the join.
        .filter(_may_filter())
        .join(
            F.broadcast(vendor_lookup),
            on="VendorID",
            how="left",
        )
        .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
        .groupBy("pickup_hour")
        .agg(
            F.avg("passenger_count").alias("average_passenger_count"),
            F.count(F.lit(1)).alias("trip_count"),
        )
        .orderBy("pickup_hour")
    )
