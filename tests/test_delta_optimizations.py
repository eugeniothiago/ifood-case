"""Unit tests for Delta maintenance validation and SQL generation."""

from unittest.mock import MagicMock

import pytest

from src.delta_optimizations import (
    DEFAULT_RETENTION_HOURS,
    describe_table_history,
    optimize_table,
    vacuum_table,
    zorder_table,
)


def test_optimize_rejects_empty_table_name() -> None:
    """OPTIMIZE must validate before touching Spark."""
    spark = MagicMock()
    with pytest.raises(ValueError):
        optimize_table(spark, "")
    spark.sql.assert_not_called()


def test_optimize_rejects_whitespace_table_name() -> None:
    """Whitespace-only identifiers must be rejected."""
    with pytest.raises(ValueError):
        optimize_table(MagicMock(), "   ")


def test_zorder_rejects_empty_column_list() -> None:
    """Z-ordering requires at least one column."""
    spark = MagicMock()
    with pytest.raises(ValueError):
        zorder_table(spark, "gold.table", [])
    spark.sql.assert_not_called()


def test_zorder_rejects_empty_column_string() -> None:
    """Z-ordering must reject blank column identifiers."""
    with pytest.raises(ValueError):
        zorder_table(MagicMock(), "gold.table", [""])


def test_vacuum_rejects_below_minimum_retention() -> None:
    """Retention below seven days must be rejected."""
    with pytest.raises(ValueError):
        vacuum_table(MagicMock(), "gold.table", 167)


def test_vacuum_rejects_zero_retention() -> None:
    """Zero retention must be rejected."""
    with pytest.raises(ValueError):
        vacuum_table(MagicMock(), "gold.table", 0)


def test_vacuum_rejects_negative_retention() -> None:
    """Negative retention must be rejected."""
    with pytest.raises(ValueError):
        vacuum_table(MagicMock(), "gold.table", -1)


def test_describe_history_rejects_empty_table_name() -> None:
    """History inspection must validate the table identifier."""
    spark = MagicMock()
    with pytest.raises(ValueError):
        describe_table_history(spark, "")
    spark.sql.assert_not_called()


def test_default_retention_hours() -> None:
    """The configured safe minimum retention must be 168 hours."""
    assert DEFAULT_RETENTION_HOURS == 168


def test_optimize_calls_spark_sql() -> None:
    """A valid OPTIMIZE request must issue the expected SQL."""
    spark = MagicMock()
    optimize_table(spark, "nyc_taxi.gold.yellow_tripdata")
    spark.sql.assert_called_once_with("OPTIMIZE nyc_taxi.gold.yellow_tripdata")
