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

from datetime import datetime

import pytest

from src.transformations import _as_datetime


def test_pickup_date_from_datetime_object() -> None:
    """A datetime object must produce its calendar date."""
    assert pickup_date_from_timestamp(datetime(2023, 3, 15, 10, 30)) == date(2023, 3, 15)


def test_pickup_date_from_date_object() -> None:
    """A date object must be returned unchanged by date derivation."""
    value = date(2023, 3, 15)
    assert pickup_date_from_timestamp(value) == value


def test_pickup_date_from_iso_with_z() -> None:
    """ISO timestamps with a UTC suffix must be supported."""
    assert pickup_date_from_timestamp("2023-03-15T10:30:00Z") == date(2023, 3, 15)


def test_pickup_date_midnight() -> None:
    """A midnight timestamp must remain on the same calendar date."""
    assert pickup_date_from_timestamp("2023-01-01T00:00:00") == date(2023, 1, 1)


def test_pickup_month_january() -> None:
    """January must be zero-padded in the month key."""
    assert pickup_month_from_timestamp("2023-01-15T12:00:00") == "2023-01"


def test_pickup_month_december() -> None:
    """December must be represented as the final two-digit month."""
    assert pickup_month_from_timestamp("2023-12-31T23:59:59") == "2023-12"


def test_pickup_hour_midnight() -> None:
    """Midnight must return hour zero."""
    assert pickup_hour_from_timestamp("2023-05-01T00:00:00") == 0


def test_pickup_hour_eleven_pm() -> None:
    """The last minute of the day must return hour 23."""
    assert pickup_hour_from_timestamp("2023-05-01T23:59:59") == 23


def test_invalid_type_raises_typeerror() -> None:
    """Integer timestamp inputs are unsupported."""
    with pytest.raises(TypeError):
        pickup_date_from_timestamp(42)  # type: ignore[arg-type]


def test_invalid_type_none_raises_typeerror() -> None:
    """None timestamp inputs are unsupported."""
    with pytest.raises(TypeError):
        pickup_month_from_timestamp(None)  # type: ignore[arg-type]


def test_as_datetime_rejects_list() -> None:
    """List timestamp inputs are unsupported by the normalizer."""
    with pytest.raises(TypeError):
        _as_datetime([])  # type: ignore[arg-type]
