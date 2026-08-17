"""Bronze Delta ingestion with explicit source schema and lineage."""

from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def create_schemas(spark: SparkSession, schemas: Sequence[str]) -> None:
    """Create Hive databases if they do not exist (needed for Community Edition).

    Deduplicates schema names to avoid redundant DDL calls.
    """
    seen: set[str] = set()
    for schema in schemas:
        if schema not in seen:
            seen.add(schema)
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")


def drop_table_if_exists(spark: SparkSession, table_name: str) -> None:
    """Safely drop a table, tolerating missing schema or table."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass


def ingest_to_bronze(
    spark: SparkSession, file_paths: Sequence[str], table_name: str
) -> DataFrame:
    """Read raw Parquet files and append them to a Delta Bronze table.

    Bronze preserves the source columns as-is. We do NOT force an explicit
    schema on read because NYC TLC Parquet files can have type differences
    across months (e.g. passenger_count is INT64 in some files, DOUBLE in
    others). Spark's mergeSchema handles this automatically.

    Uses _metadata.file_path (Spark 3.0+) instead of input_file_name() because
    Unity Catalog does not support input_file_name().
    """
    if not file_paths:
        raise ValueError("file_paths cannot be empty")
    if not table_name.strip():
        raise ValueError("table_name cannot be empty")

    # Read without forcing schema — let Spark infer and merge across files.
    raw_df = spark.read.option("mergeSchema", "true").parquet(*file_paths)

    bronze_df = (
        raw_df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )
    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
    return bronze_df