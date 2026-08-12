"""Pure timestamp derivation rules mirrored by the Spark transformations."""

from datetime import date, datetime


def _as_datetime(value: datetime | date | str) -> datetime:
    """Normalize supported timestamp inputs for deterministic unit testing."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError("value must be a datetime, date, or ISO-8601 string")


def pickup_date_from_timestamp(value: datetime | date | str) -> date:
    """Return the calendar date used by Gold's ``pickup_date`` partition."""
    return _as_datetime(value).date()


def pickup_month_from_timestamp(value: datetime | date | str) -> str:
    """Return the ``yyyy-MM`` month key used by Q1."""
    return _as_datetime(value).strftime("%Y-%m")


def pickup_hour_from_timestamp(value: datetime | date | str) -> int:
    """Return the local pickup hour used by Q2."""
    return _as_datetime(value).hour
