"""Data quality rules shared by Bronze profiling and downstream layers."""

from functools import reduce
from operator import or_
from typing import Callable, Final

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .schemas import RAW_SCHEMA, REQUIRED_COLUMNS

CASE_START: Final[str] = "2023-01-01"
CASE_END_EXCLUSIVE: Final[str] = "2023-06-01"

Predicate = Callable[[DataFrame], Column]


def _any_invalid(predicates: list[Column]) -> Column:
    """Combine invalid-row predicates using a null-safe OR expression."""
    return reduce(or_, predicates, F.lit(False))


def required_columns_not_null(df: DataFrame) -> Column:
    """Return true for rows missing at least one required consumption field."""
    return _any_invalid([F.col(column).isNull() for column in REQUIRED_COLUMNS])


def passenger_count_positive(df: DataFrame) -> Column:
    """Return true for null or non-positive passenger counts."""
    del df
    return F.col("passenger_count").isNull() | (F.col("passenger_count") <= F.lit(0))


def total_amount_non_negative(df: DataFrame) -> Column:
    """Return true for null or negative total amounts."""
    del df
    return F.col("total_amount").isNull() | (F.col("total_amount") < F.lit(0))


def dropoff_after_pickup(df: DataFrame) -> Column:
    """Return true when timestamps are absent or dropoff is not after pickup."""
    del df
    return (
        F.col("tpep_pickup_datetime").isNull()
        | F.col("tpep_dropoff_datetime").isNull()
        | (F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"))
    )


def pickup_in_case_period(df: DataFrame) -> Column:
    """Return true for null or pickup timestamps outside the Jan-May case period."""
    del df
    pickup = F.col("tpep_pickup_datetime")
    return pickup.isNull() | (pickup < F.to_timestamp(F.lit(CASE_START))) | (
        pickup >= F.to_timestamp(F.lit(CASE_END_EXCLUSIVE))
    )


def get_data_quality_expectations() -> dict[str, Predicate]:
    """Return named DLT-style predicates, each true for invalid rows."""
    return {
        "required_columns_not_null": required_columns_not_null,
        "passenger_count_positive": passenger_count_positive,
        "total_amount_non_negative": total_amount_non_negative,
        "dropoff_after_pickup": dropoff_after_pickup,
        "pickup_in_case_period": pickup_in_case_period,
    }


def invalid_rows(df: DataFrame) -> DataFrame:
    """Return rows failing at least one named quality expectation."""
    predicates = [predicate(df) for predicate in get_data_quality_expectations().values()]
    return df.filter(_any_invalid(predicates))


def valid_rows(df: DataFrame) -> DataFrame:
    """Return rows passing every named quality expectation."""
    return df.filter(
        ~_any_invalid([predicate(df) for predicate in get_data_quality_expectations().values()])
    )


def validate_schema(df: DataFrame, expected_schema: StructType = RAW_SCHEMA) -> bool:
    """Return whether a DataFrame has exactly the expected field order and schema."""
    return df.schema == expected_schema
