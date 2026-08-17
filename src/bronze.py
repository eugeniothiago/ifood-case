"""Bronze Delta ingestion with explicit source schema and lineage."""

from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .schemas import RAW_SCHEMA
from .data_quality import validate_schema


def ingest_to_bronze(
    spark: SparkSession, file_paths: Sequence[str], table_name: str
) -> DataFrame:
    """Read raw Parquet files and append them to a Delta Bronze table.

    Bronze preserves the source columns and does not silently discard malformed
    records. Lineage fields allow replay, audit, and later quarantine decisions.

    Uses _metadata.file_path (Spark 3.0+) instead of input_file_name() because
    Unity Catalog does not support input_file_name().
    """
    if not file_paths:
        raise ValueError("file_paths cannot be empty")
    if not table_name.strip():
        raise ValueError("table_name cannot be empty")

    raw_df = spark.read.schema(RAW_SCHEMA).parquet(*file_paths)
    if not validate_schema(raw_df, RAW_SCHEMA):
        raise ValueError("Bronze input schema does not match RAW_SCHEMA")

    # Use _metadata.file_path for lineage instead of F.input_file_name()
    # because Unity Catalog does not support input_file_name().
    bronze_df = (
        raw_df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )
    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .saveAsTable(table_name)
    )
    return bronze_df
