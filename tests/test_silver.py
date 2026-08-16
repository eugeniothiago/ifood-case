"""Unit tests for Silver validation and write-mode configuration."""

from unittest.mock import MagicMock

import pytest

from src.silver import SILVER_WRITE_MODE, get_silver_summary, transform_to_silver


def test_transform_rejects_empty_bronze_table() -> None:
    """Silver transformation must reject a blank Bronze identifier."""
    with pytest.raises(ValueError):
        transform_to_silver(MagicMock(), "", "silver.table")


def test_transform_rejects_empty_silver_table() -> None:
    """Silver transformation must reject a blank Silver identifier."""
    with pytest.raises(ValueError):
        transform_to_silver(MagicMock(), "bronze.table", "")


def test_get_silver_summary_rejects_empty_bronze_table() -> None:
    """Silver summary must reject a blank Bronze identifier."""
    with pytest.raises(ValueError):
        get_silver_summary(MagicMock(), "", "silver.table")


def test_get_silver_summary_rejects_empty_silver_table() -> None:
    """Silver summary must reject a blank Silver identifier."""
    with pytest.raises(ValueError):
        get_silver_summary(MagicMock(), "bronze.table", "")


def test_silver_write_mode_is_overwrite() -> None:
    """Silver publication must use deterministic overwrite mode."""
    assert SILVER_WRITE_MODE == "overwrite"
