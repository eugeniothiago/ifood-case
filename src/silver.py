"""Silver transformation for the NYC TLC Yellow Taxi data.

Silver is the contract-enforcing layer: it keeps the five fields required by the
case, standardizes their types, and removes records that fail the shared quality
expectations. Bronze remains untouched so invalid source records can still be
replayed or audited.
"""

from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .data_quality import valid_rows
from .schemas import REQUIRED_COLUMNS

SILVER_WRITE_MODE: Final[str] = "overwrite"


def _cast_required_columns(bronze_df: DataFrame) -> DataFrame:
    """Cast the case contract fields before evaluating quality predicates.

    Explicit casts make the downstream Delta schema deterministic even when a
    source file or an upstream table was created with a wider/narrower type.
    Casting before validation also makes comparisons in the shared expectations
    type-safe. Invalid casts become nulls and are then rejected by the required
    non-null expectation.
    """
    return bronze_df.select(
        F.col("VendorID").cast("long").alias("VendorID"),
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
    )


def transform_to_silver(
    spark: SparkSession, bronze_table: str, silver_table: str
) -> DataFrame:
    """Build and overwrite the typed, quality-filtered Silver Delta table.

    The five named expectations in :mod:`src.data_quality` are applied together
    through ``valid_rows``: required fields must be present, passenger counts
    must be positive, amounts must be non-negative, dropoff must follow pickup,
    and pickup must fall within the January-May 2023 case period. Filtering here
    keeps Bronze lossless while making Silver safe for consumption. Only the
    five columns requested by the case are selected, limiting downstream schema
    coupling and storage.

    ``overwriteSchema=true`` is intentional. This layer is a deterministic
    rebuild from Bronze, so the published Delta schema should follow the
    explicit contract rather than accumulating accidental source columns.
    """
    if not bronze_table.strip() or not silver_table.strip():
        raise ValueError("bronze_table and silver_table cannot be empty")

    bronze_df = spark.read.table(bronze_table)
    # Materialize the explicit Silver contract: VendorID is bigint/long and the
    # other four fields use the required double/timestamp types.
    typed_df = _cast_required_columns(bronze_df)
    silver_df = valid_rows(typed_df).select(*REQUIRED_COLUMNS)

    (
        silver_df.write.format("delta")
        .mode(SILVER_WRITE_MODE)
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )
    return silver_df


def get_silver_summary(
    spark: SparkSession, bronze_table: str, silver_table: str
) -> dict[str, int]:
    """Return Bronze, Silver, and dropped-row counts for pipeline logging.

    Counts are read from the persisted tables rather than inferred from a
    transformation object. That makes the summary useful after a notebook
    restart and confirms what was actually published. ``dropped`` is the
    difference between the source Bronze snapshot and the resulting Silver
    table; it is not a count of distinct quality issues because one row may fail
    more than one expectation.
    """
    if not bronze_table.strip() or not silver_table.strip():
        raise ValueError("bronze_table and silver_table cannot be empty")

    bronze_count = spark.read.table(bronze_table).count()
    silver_count = spark.read.table(silver_table).count()
    return {
        "bronze_rows": int(bronze_count),
        "silver_rows": int(silver_count),
        "dropped_rows": int(bronze_count - silver_count),
    }
