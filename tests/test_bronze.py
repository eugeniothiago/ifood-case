"""Unit tests for Bronze input validation without a live Spark session."""

from unittest.mock import MagicMock

import pytest

from src.bronze import ingest_to_bronze


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
