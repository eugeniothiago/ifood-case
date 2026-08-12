"""Unit tests for schema validation and named data-quality expectations."""

from types import SimpleNamespace

from pyspark.sql.types import DoubleType, StructField, StructType

from src.data_quality import get_data_quality_expectations, validate_schema
from src.schemas import RAW_SCHEMA

EXPECTED_NAMES = {
    "required_columns_not_null",
    "passenger_count_positive",
    "total_amount_non_negative",
    "dropoff_after_pickup",
    "pickup_in_case_period",
}


def test_validate_schema_accepts_matching_schema() -> None:
    """A DataFrame-like object with the exact schema must pass validation."""
    assert validate_schema(SimpleNamespace(schema=RAW_SCHEMA)) is True


def test_validate_schema_rejects_wrong_type() -> None:
    """Changing one field type must fail exact schema validation."""
    wrong_schema = StructType(
        [
            StructField(
                field.name,
                DoubleType() if field.name == "VendorID" else field.dataType,
                field.nullable,
            )
            for field in RAW_SCHEMA.fields
        ]
    )
    assert validate_schema(SimpleNamespace(schema=wrong_schema)) is False


def test_expectations_are_named_and_callable() -> None:
    """Every contract expectation must be exposed as a callable predicate."""
    expectations = get_data_quality_expectations()
    assert len(expectations) == 5
    assert all(callable(expectation) for expectation in expectations.values())


def test_expectation_names() -> None:
    """Expectation names must match the versioned data contract exactly."""
    assert set(get_data_quality_expectations()) == EXPECTED_NAMES
