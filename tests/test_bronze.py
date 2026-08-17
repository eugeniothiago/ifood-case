"""Unit tests for Bronze ingestion helpers without a live Spark session."""

from unittest.mock import MagicMock

import pytest

from src.bronze import create_schemas, drop_table_if_exists, ingest_to_bronze


@pytest.mark.parametrize("file_paths", [[], ()])
def test_ingest_rejects_empty_file_paths(file_paths: object) -> None:
    """Bronze ingestion requires at least one source path."""
    with pytest.raises(ValueError):
        ingest_to_bronze(MagicMock(), file_paths, "bronze.table")  # type: ignore[arg-type]


def test_ingest_rejects_empty_table_name() -> None:
    """Bronze ingestion must reject an empty table identifier."""
    with pytest.raises(ValueError):
        ingest_to_bronze(MagicMock(), ["file.parquet"], "")


def test_ingest_rejects_whitespace_table_name() -> None:
    """Bronze ingestion must reject a whitespace-only identifier."""
    with pytest.raises(ValueError):
        ingest_to_bronze(MagicMock(), ["file.parquet"], "   ")


def test_create_schemas_executes_ddl() -> None:
    """create_schemas must issue CREATE DATABASE IF NOT EXISTS for each schema."""
    spark = MagicMock()
    create_schemas(spark, ["bronze", "silver", "gold"])
    assert spark.sql.call_count == 3
    spark.sql.assert_any_call("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql.assert_any_call("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql.assert_any_call("CREATE DATABASE IF NOT EXISTS gold")


def test_create_schemas_deduplicates() -> None:
    """create_schemas must not create the same schema twice."""
    spark = MagicMock()
    create_schemas(spark, ["bronze", "bronze", "silver"])
    assert spark.sql.call_count == 2


def test_drop_table_if_exists_swallows_errors() -> None:
    """drop_table_if_exists must not raise if the schema does not exist."""
    spark = MagicMock()
    spark.sql.side_effect = Exception("schema not found")
    drop_table_if_exists(spark, "bronze.yellow_tripdata")  # should not raise
    spark.sql.assert_called_once_with("DROP TABLE IF EXISTS bronze.yellow_tripdata")


def test_drop_table_if_exists_succeeds() -> None:
    """drop_table_if_exists must issue DROP TABLE IF EXISTS."""
    spark = MagicMock()
    drop_table_if_exists(spark, "bronze.yellow_tripdata")
    spark.sql.assert_called_once_with("DROP TABLE IF EXISTS bronze.yellow_tripdata")
