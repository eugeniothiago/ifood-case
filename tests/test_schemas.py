"""Unit tests for the explicit source and consumption schemas."""

from pyspark.sql.types import DateType

from src.schemas import CONSUMPTION_SCHEMA, GOLD_SCHEMA, RAW_SCHEMA, REQUIRED_COLUMNS


def test_raw_schema_has_19_columns() -> None:
    """RAW_SCHEMA must preserve all 19 NYC TLC source columns."""
    assert len(RAW_SCHEMA.fields) == 19


def test_consumption_schema_has_5_required_columns() -> None:
    """Consumption output contains exactly the five case-required fields."""
    assert CONSUMPTION_SCHEMA.names == list(REQUIRED_COLUMNS)
    assert len(CONSUMPTION_SCHEMA.fields) == 5


def test_gold_schema_includes_pickup_date() -> None:
    """Gold adds a native DateType daily partition column."""
    assert "pickup_date" in GOLD_SCHEMA.names
    assert isinstance(GOLD_SCHEMA["pickup_date"].dataType, DateType)


def test_required_columns_match_consumption_schema() -> None:
    """The tuple contract and StructType field names must remain aligned."""
    assert tuple(CONSUMPTION_SCHEMA.names) == REQUIRED_COLUMNS
