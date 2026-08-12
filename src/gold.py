"""Gold consumption model for the NYC TLC Yellow Taxi case."""

from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import REQUIRED_COLUMNS

GOLD_PARTITION_COLUMN: Final[str] = "pickup_date"
GOLD_WRITE_MODE: Final[str] = "overwrite"


def model_gold(
    spark: SparkSession, silver_table: str, gold_table: str
) -> DataFrame:
    """Create the Gold table with a native daily ``pickup_date`` partition.

    Gold adds the date grain required by the consumption queries while retaining
    the five required fields. Daily partitioning is deliberate: both case
    questions filter or group by time, and the May-only hourly question can prune
    all non-May daily partitions. The Jan-May input has roughly 151 days, which
    is moderate cardinality—not the many tiny partitions that an overly granular
    key would create, and not the five coarse partitions produced by monthly
    partitioning. Monthly partitions would force a May query to scan one broad
    partition; VendorID would be an incorrect key because only two or three
    vendors are present, producing unbalanced partitions without time pruning.

    ``overwriteSchema=true`` keeps the published schema explicit, and
    ``partitionBy(pickup_date)`` stores a real Spark ``DateType`` partition key,
    preserving partition pruning instead of encoding dates as month strings.
    """
    if not silver_table.strip() or not gold_table.strip():
        raise ValueError("silver_table and gold_table cannot be empty")

    silver_df = spark.read.table(silver_table)
    gold_df = silver_df.select(
        *REQUIRED_COLUMNS,
        F.to_date(F.col("tpep_pickup_datetime")).alias(GOLD_PARTITION_COLUMN),
    )

    (
        gold_df.write.format("delta")
        .mode(GOLD_WRITE_MODE)
        .option("overwriteSchema", "true")
        .partitionBy(GOLD_PARTITION_COLUMN)
        .saveAsTable(gold_table)
    )
    return gold_df


def get_gold_summary(spark: SparkSession, gold_table: str) -> DataFrame:
    """Return rows per daily ``pickup_date`` partition for observability.

    The summary is intentionally a DataFrame so a Databricks notebook can
    ``display`` it, persist it, or apply additional checks without collecting
    all partition statistics to the driver. Grouping on the actual DateType
    column mirrors the physical partition key and makes missing days visible.
    """
    if not gold_table.strip():
        raise ValueError("gold_table cannot be empty")

    return (
        spark.read.table(gold_table)
        .groupBy(GOLD_PARTITION_COLUMN)
        .count()
        .orderBy(GOLD_PARTITION_COLUMN)
    )
