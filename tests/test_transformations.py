"""Pure tests for the timestamp derivation rules used by the analyses."""

from datetime import date

from src.transformations import (
    pickup_date_from_timestamp,
    pickup_hour_from_timestamp,
    pickup_month_from_timestamp,
)


def test_pickup_date_derivation() -> None:
    """A timestamp maps to its calendar date, including a midnight boundary."""
    assert pickup_date_from_timestamp("2023-05-31T23:59:59") == date(2023, 5, 31)


def test_month_derivation() -> None:
    """Month derivation follows the Spark ``yyyy-MM`` format."""
    assert pickup_month_from_timestamp("2023-05-31T23:59:59") == "2023-05"


def test_hour_extraction() -> None:
    """Hour extraction returns the integer hour from the pickup timestamp."""
    assert pickup_hour_from_timestamp("2023-05-31T07:42:00") == 7
