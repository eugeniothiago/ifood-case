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

from unittest.mock import MagicMock, patch

from src.data_quality import CASE_END_EXCLUSIVE, CASE_START, invalid_rows, valid_rows


def test_validate_schema_rejects_extra_column() -> None:
    """An extra field must fail exact schema validation."""
    extra_schema = StructType([*RAW_SCHEMA.fields, StructField("extra_field", DoubleType())])
    assert validate_schema(SimpleNamespace(schema=extra_schema)) is False


def test_validate_schema_rejects_missing_column() -> None:
    """A missing field must fail exact schema validation."""
    missing_schema = StructType(RAW_SCHEMA.fields[:-1])
    assert validate_schema(SimpleNamespace(schema=missing_schema)) is False


def test_validate_schema_rejects_reordered_fields() -> None:
    """Reordering fields must fail because StructType comparison is order-sensitive."""
    reordered_schema = StructType([RAW_SCHEMA.fields[1], RAW_SCHEMA.fields[0], *RAW_SCHEMA.fields[2:]])
    assert validate_schema(SimpleNamespace(schema=reordered_schema)) is False


def test_case_start_constant() -> None:
    """The quality contract starts on the first day of January 2023."""
    assert CASE_START == "2023-01-01"


def test_case_end_exclusive_constant() -> None:
    """The quality contract excludes timestamps on or after June 1, 2023."""
    assert CASE_END_EXCLUSIVE == "2023-06-01"


def test_invalid_rows_uses_all_predicates() -> None:
    """invalid_rows must evaluate and combine all five named expectations."""
    df = MagicMock()
    filtered = object()
    predicate_names = [
        "required_columns_not_null",
        "passenger_count_positive",
        "total_amount_non_negative",
        "dropoff_after_pickup",
        "pickup_in_case_period",
    ]
    predicates = {name: MagicMock(return_value=name) for name in predicate_names}
    with patch("src.data_quality.get_data_quality_expectations", return_value=predicates), patch(
        "src.data_quality._any_invalid", return_value=filtered
    ) as combine:
        df.filter.return_value = df
        result = invalid_rows(df)

    assert result is df
    assert combine.call_count == 1
    assert df.filter.call_args.args == (filtered,)
    for predicate in predicates.values():
        predicate.assert_called_once_with(df)


def test_valid_rows_negates_invalid() -> None:
    """valid_rows must filter with the negation of the combined invalid expression."""

    class NegatableExpression:
        def __invert__(self):
            return "negated-expression"

    df = MagicMock()
    predicates = {name: MagicMock(return_value=name) for name in ("one", "two", "three", "four", "five")}
    with patch("src.data_quality.get_data_quality_expectations", return_value=predicates), patch(
        "src.data_quality._any_invalid", return_value=NegatableExpression()
    ):
        result = valid_rows(df)

    assert result is df.filter.return_value
    df.filter.assert_called_once_with("negated-expression")
    for predicate in predicates.values():
        predicate.assert_called_once_with(df)
