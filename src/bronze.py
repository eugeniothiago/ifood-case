"""Bronze Delta ingestion with explicit source schema and lineage."""

from functools import reduce
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import RAW_SCHEMA


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


def _read_and_normalize(spark: SparkSession, file_path: str) -> DataFrame:
    """Read one Parquet file and cast all RAW_SCHEMA columns to consistent types.

    NYC TLC monthly files can have type mismatches (e.g. passenger_count is
    INT64 in some months, DOUBLE in others) and case mismatches (e.g.
    Airport_fee vs airport_fee). Reading each file individually, casting to
    a shared schema, and dropping case-variant duplicates avoids merge errors.
    """
    df = spark.read.parquet(file_path)

    # Build a case-insensitive lookup of source column names.
    source_cols_lower = {c.lower(): c for c in df.columns}

    select_exprs = []
    used_source_cols: set[str] = set()

    for field in RAW_SCHEMA.fields:
        # Match case-insensitively so Airport_fee maps to airport_fee if present.
        src_col = source_cols_lower.get(field.name.lower())
        if src_col and src_col in df.columns:
            select_exprs.append(
                F.col(src_col).cast(field.dataType).alias(field.name)
            )
            used_source_cols.add(src_col)
        else:
            select_exprs.append(
                F.lit(None).cast(field.dataType).alias(field.name)
            )

    # Preserve any extra source columns not already consumed (case-insensitive).
    raw_names_lower = {f.name.lower() for f in RAW_SCHEMA.fields}
    for col_name in df.columns:
        if col_name not in used_source_cols and col_name.lower() not in raw_names_lower:
            select_exprs.append(F.col(col_name))

    df = df.select(*select_exprs)
    df = df.withColumn("_source_file", F.lit(file_path))
    return df


def ingest_to_bronze(
    spark: SparkSession, file_paths: Sequence[str], table_name: str
) -> DataFrame:
    """Read raw Parquet files and append them to a Delta Bronze table.

    Each file is read individually and cast to the shared RAW_SCHEMA to handle
    type and case differences across monthly TLC files. Files are then unioned
    by name and written to Delta as a single Bronze table with a consistent schema.
    """
    if not file_paths:
        raise ValueError("file_paths cannot be empty")
    if not table_name.strip():
        raise ValueError("table_name cannot be empty")

    dfs = [_read_and_normalize(spark, path) for path in file_paths]
    bronze_df = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs
    )
    bronze_df = (
        bronze_df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    return bronze_df